//! The vault-locked transition fence: it closes a bucket to new stage work only while no
//! plaintext stage, checkpoint, attempt or stage file of that bucket remains.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::vfs::BucketScope;
use aruna_core::vfs_effects::{VfsEvent, VfsFailure};
use std::collections::HashMap;
use std::sync::Arc;
use ulid::Ulid;

use crate::checkpoints::parse_name;
use crate::stages::volatile_id;
use crate::{Answer, Shared, State, VfsHandle};

#[derive(Default)]
pub(crate) struct Fences {
    closed: HashMap<BucketScope, FenceState>,
    ids: HashMap<Ulid, BucketScope>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FenceState {
    Held(Ulid),
    /// An unknown commit outcome keeps the bucket closed for good.
    Kept,
}

impl Fences {
    pub(crate) fn closed(&self, scope: &BucketScope) -> bool {
        self.closed.contains_key(scope)
    }

    fn open(&mut self, scope: &BucketScope, fence_id: Ulid) {
        if self.closed.get(scope) == Some(&FenceState::Held(fence_id)) {
            self.closed.remove(scope);
        }
        self.ids.remove(&fence_id);
    }
}

/// Opens the fence again unless the caller received it.
struct PendingFence {
    shared: Arc<Shared>,
    scope: BucketScope,
    fence_id: Ulid,
    armed: bool,
}

impl Drop for PendingFence {
    fn drop(&mut self) {
        if self.armed {
            self.shared
                .state
                .lock()
                .fences
                .open(&self.scope, self.fence_id);
        }
    }
}

/// Why plaintext work of `scope` still exists in memory, if it does.
fn busy_reason(state: &State, scope: &BucketScope) -> Option<&'static str> {
    let unpublished = (state.buckets.get(scope)).is_some_and(|bucket| {
        bucket
            .instances
            .values()
            .any(|instance| instance.unpublished())
    });
    if unpublished {
        return Some("a staged or unpublished file remains");
    }
    if state
        .checkpoints
        .values()
        .any(|checkpoint| checkpoint.scope == *scope)
    {
        return Some("a checkpoint remains");
    }
    if state.permits.busy(scope) {
        return Some("a mutation attempt is unfinished");
    }
    if state.permits.has_guards(scope) {
        return Some("a file guard is held");
    }
    if state
        .in_flight_writes
        .get(scope)
        .is_some_and(|count| *count > 0)
    {
        return Some("a stage write is in flight");
    }
    None
}

/// Counts one stage write of a bucket from its fence check until it settles, so a vault fence
/// refuses meanwhile; create it last under the state lock and drop it with the lock free.
pub(crate) struct WriteAdmission {
    shared: Arc<Shared>,
    scope: BucketScope,
}

impl WriteAdmission {
    /// Counts the write in; the caller checked the fence under the same lock.
    pub(crate) fn enter(shared: &Arc<Shared>, state: &mut State, scope: &BucketScope) -> Self {
        *state.in_flight_writes.entry(scope.clone()).or_insert(0) += 1;
        Self {
            shared: Arc::clone(shared),
            scope: scope.clone(),
        }
    }
}

impl Drop for WriteAdmission {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock();
        if let Some(count) = state.in_flight_writes.get_mut(&self.scope) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.in_flight_writes.remove(&self.scope);
            }
        }
    }
}

impl VfsHandle {
    pub(crate) async fn fence_vault(&self, scope: BucketScope) -> Answer {
        let fence_id = Ulid::generate();
        let (stages, volatiles) = {
            let mut state = self.shared.state.lock();
            if state.fences.closed(&scope) {
                return Err(VfsFailure::VaultFenced(
                    "the bucket is fenced already".to_string(),
                ));
            }
            if let Some(reason) = busy_reason(&state, &scope) {
                return Err(VfsFailure::VaultFenced(reason.to_string()));
            }
            // Closed before the folder scan, so nothing new is admitted while it runs.
            state
                .fences
                .closed
                .insert(scope.clone(), FenceState::Held(fence_id));
            state.fences.ids.insert(fence_id, scope.clone());
            let mut stages: HashMap<Ulid, bool> = HashMap::new();
            for ((stage_id, _), checkpoint) in &state.checkpoints {
                *stages.entry(*stage_id).or_default() |= checkpoint.scope == scope;
            }
            let mut volatiles: HashMap<u64, bool> = HashMap::new();
            for (owner, bucket) in &state.buckets {
                for instance_id in bucket.instances.keys() {
                    volatiles.insert(*instance_id, *owner == scope);
                }
            }
            (stages, volatiles)
        };
        let mut pending = PendingFence {
            shared: Arc::clone(&self.shared),
            scope: scope.clone(),
            fence_id,
            armed: true,
        };
        let root = self.shared.root.clone();
        let names = crate::blocking(move || {
            let mut names = Vec::new();
            for entry in std::fs::read_dir(&root)? {
                names.push(entry?.file_name().to_string_lossy().into_owned());
            }
            Ok(names)
        })
        .await
        .map_err(|_| {
            VfsFailure::VaultFenced("the stage folder could not be scanned".to_string())
        })?;
        for name in names {
            // A file this node cannot place in another bucket may hold plaintext of this one.
            let own = match (parse_name(&name), volatile_id(&name)) {
                (Some((stage_id, _, _)), _) => stages.get(&stage_id).copied().unwrap_or(true),
                (None, Some(instance_id)) => volatiles.get(&instance_id).copied().unwrap_or(true),
                (None, None) => false,
            };
            if own {
                return Err(VfsFailure::VaultFenced("a stage file remains".to_string()));
            }
        }
        let state = self.shared.state.lock();
        if let Some(reason) = busy_reason(&state, &scope) {
            drop(state);
            return Err(VfsFailure::VaultFenced(reason.to_string()));
        }
        pending.armed = false;
        Ok(VfsEvent::VaultFenced { fence_id })
    }

    pub(crate) fn release_fence(&self, fence_id: Ulid, keep_closed: bool) -> Answer {
        let mut state = self.shared.state.lock();
        let scope = state.fences.ids.get(&fence_id).cloned();
        let scope = scope.ok_or(VfsFailure::NotFound)?;
        match keep_closed {
            true => {
                state.fences.closed.insert(scope, FenceState::Kept);
                state.fences.ids.remove(&fence_id);
            }
            false => state.fences.open(&scope, fence_id),
        }
        Ok(VfsEvent::Done)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VfsConfig;

    #[tokio::test]
    async fn fence_waits_writes() {
        let folder = tempfile::tempdir().unwrap();
        let handle = VfsHandle::open(VfsConfig {
            stage_dir: folder.path().join("stage"),
            node_stage_bytes: 1 << 20,
            group_stage_bytes: 1 << 20,
        })
        .unwrap();
        let scope = BucketScope::new("bucket", 7);
        let admission = {
            let mut state = handle.shared.state.lock();
            WriteAdmission::enter(&handle.shared, &mut state, &scope)
        };
        let refused = handle.fence_vault(scope.clone()).await;
        assert!(matches!(refused, Err(VfsFailure::VaultFenced(_))));
        drop(admission);
        let granted = handle.fence_vault(scope).await;
        assert!(matches!(granted, Ok(VfsEvent::VaultFenced { .. })));
    }
}
