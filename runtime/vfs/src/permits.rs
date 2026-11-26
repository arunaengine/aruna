//! Fair namespace permits per bucket incarnation, per-instance file guards taken in ascending
//! order, and the pending mutation attempts every guard holder must respect.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::vfs::BucketScope;
use aruna_core::vfs_effects::{VfsEvent, VfsFailure};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use ulid::Ulid;

use crate::{Answer, VfsHandle};

#[derive(Default)]
pub(crate) struct Permits {
    namespaces: HashMap<BucketScope, Arc<Semaphore>>,
    held: HashMap<Ulid, OwnedSemaphorePermit>,
    /// Instance ids never repeat on one node, so they key the guards of every bucket.
    guards: HashMap<u64, Arc<Semaphore>>,
    guarded: HashMap<Ulid, Vec<OwnedSemaphorePermit>>,
    attempts: HashMap<Ulid, Attempt>,
}

struct Attempt {
    scope: BucketScope,
    instances: BTreeSet<u64>,
    done: Arc<Notify>,
}

impl Permits {
    pub(crate) fn take_permit(&mut self, permit_id: &Ulid) -> Option<OwnedSemaphorePermit> {
        self.held.remove(permit_id)
    }

    pub(crate) fn take_guard(&mut self, guard_id: &Ulid) -> Option<Vec<OwnedSemaphorePermit>> {
        self.guarded.remove(guard_id)
    }

    /// Whether an unfinished attempt of `scope` exists, which a vault fence must wait for.
    pub(crate) fn busy(&self, scope: &BucketScope) -> bool {
        self.attempts
            .values()
            .any(|attempt| attempt.scope == *scope)
    }

    /// Drops the guard of a reclaimed instance unless someone still holds or awaits it.
    pub(crate) fn forget(&mut self, instance_id: u64) {
        if let Some(guard) = self.guards.get(&instance_id)
            && Arc::strong_count(guard) == 1
            && guard.available_permits() == 1
        {
            self.guards.remove(&instance_id);
        }
    }

    /// The lowest unfinished attempt touching any of `instances`.
    fn pending(&self, instances: &[u64]) -> Option<Ulid> {
        self.attempts
            .iter()
            .filter(|(_, attempt)| instances.iter().any(|id| attempt.instances.contains(id)))
            .map(|(attempt_id, _)| *attempt_id)
            .min()
    }
}

impl VfsHandle {
    pub(crate) async fn acquire_permit(&self, scope: BucketScope) -> Answer {
        let semaphore = {
            let mut state = self.shared.state.lock();
            let namespaces = &mut state.permits.namespaces;
            let semaphore = namespaces.entry(scope).or_insert_with(fair_permit);
            Arc::clone(semaphore)
        };
        // Tokio semaphores queue waiters first in, first out.
        let permit = semaphore.acquire_owned().await;
        let permit = permit.map_err(|_| VfsFailure::NotFound)?;
        let permit_id = Ulid::generate();
        self.shared
            .state
            .lock()
            .permits
            .held
            .insert(permit_id, permit);
        Ok(VfsEvent::PermitAcquired { permit_id })
    }

    pub(crate) fn release_permit(&self, permit_id: Ulid) -> Answer {
        let released = self.shared.state.lock().permits.take_permit(&permit_id);
        match released {
            Some(_) => Ok(VfsEvent::Done),
            None => Err(VfsFailure::NotFound),
        }
    }

    /// Takes the guards of `instances` lowest id first, so two callers can never deadlock. A
    /// pending attempt on any of them yields its id instead, holding no guard.
    pub(crate) async fn acquire_guards(&self, _scope: BucketScope, instances: Vec<u64>) -> Answer {
        let mut ordered = instances;
        ordered.sort_unstable();
        ordered.dedup();
        let semaphores: Vec<Arc<Semaphore>> = {
            let mut state = self.shared.state.lock();
            if let Some(attempt_id) = state.permits.pending(&ordered) {
                return Ok(VfsEvent::PendingMutation { attempt_id });
            }
            let guards = &mut state.permits.guards;
            ordered
                .iter()
                .map(|id| Arc::clone(guards.entry(*id).or_insert_with(fair_permit)))
                .collect()
        };
        let mut permits = Vec::with_capacity(semaphores.len());
        for semaphore in semaphores {
            let permit = semaphore.acquire_owned().await;
            permits.push(permit.map_err(|_| VfsFailure::NotFound)?);
        }
        let mut state = self.shared.state.lock();
        // An attempt that began while this caller waited still wins; the guards drop here.
        if let Some(attempt_id) = state.permits.pending(&ordered) {
            drop(state);
            drop(permits);
            return Ok(VfsEvent::PendingMutation { attempt_id });
        }
        let guard_id = Ulid::generate();
        state.permits.guarded.insert(guard_id, permits);
        Ok(VfsEvent::GuardAcquired { guard_id })
    }

    pub(crate) fn release_guard(&self, guard_id: Ulid) -> Answer {
        let released = self.shared.state.lock().permits.take_guard(&guard_id);
        match released {
            Some(_) => Ok(VfsEvent::Done),
            None => Err(VfsFailure::NotFound),
        }
    }

    pub(crate) fn begin_attempt(
        &self,
        scope: BucketScope,
        attempt_id: Ulid,
        instances: Vec<u64>,
    ) -> Answer {
        let mut state = self.shared.state.lock();
        let attempt = state.permits.attempts.entry(attempt_id);
        let attempt = attempt.or_insert_with(|| Attempt {
            scope,
            instances: BTreeSet::new(),
            done: Arc::new(Notify::new()),
        });
        attempt.instances.extend(instances);
        Ok(VfsEvent::Done)
    }

    pub(crate) fn finish_attempt(&self, attempt_id: Ulid) -> Answer {
        let finished = self
            .shared
            .state
            .lock()
            .permits
            .attempts
            .remove(&attempt_id);
        if let Some(attempt) = finished {
            attempt.done.notify_waiters();
        }
        Ok(VfsEvent::Done)
    }

    pub(crate) async fn wait_attempt(&self, attempt_id: Ulid) -> Answer {
        let done = {
            let state = self.shared.state.lock();
            match state.permits.attempts.get(&attempt_id) {
                Some(attempt) => Arc::clone(&attempt.done),
                None => return Ok(VfsEvent::Done),
            }
        };
        let notified = done.notified();
        tokio::pin!(notified);
        // Registered before the recheck, so a finish in between still wakes this waiter.
        notified.as_mut().enable();
        if !self
            .shared
            .state
            .lock()
            .permits
            .attempts
            .contains_key(&attempt_id)
        {
            return Ok(VfsEvent::Done);
        }
        notified.await;
        Ok(VfsEvent::Done)
    }

    pub(crate) fn active_attempts(&self, scope: &BucketScope) -> Answer {
        let state = self.shared.state.lock();
        let mut ids: Vec<Ulid> = (state.permits.attempts.iter())
            .filter(|(_, attempt)| attempt.scope == *scope)
            .map(|(attempt_id, _)| *attempt_id)
            .collect();
        ids.sort_unstable();
        Ok(VfsEvent::Attempts { ids })
    }
}

fn fair_permit() -> Arc<Semaphore> {
    Arc::new(Semaphore::new(1))
}
