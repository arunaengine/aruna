//! Holds unlocked bucket keys in memory and admits plaintext reads under leases.
//! Each key lives in one allocation that its leases share, so a lock stops new reads at once
//! while admitted reads finish; the key is zeroed when the last of them ends.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::compute::SharedSecret;
use aruna_core::effects::BlobEffect;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::ArchiveKey;
use aruna_core::structs::storage::encryption::{
    BucketKeyError, BucketKeyRef, CopyTarget, KeyTicket, ReadLease, UnlockStatus, deadline_after,
    key_matches, seal_copies,
};
use aruna_core::structs::storage::key_audit::next_event_id;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use ulid::Ulid;

/// Buckets that may be unlocked at once on one node.
pub(super) const UNLOCKED_BUCKETS: usize = 1024;
/// Generations of one bucket that may be unlocked at once: the active one and a transition source.
const BUCKET_GENERATIONS: usize = 2;
/// A prepared key that is neither activated nor discarded in this time is dropped.
const PREPARED_TTL: Duration = Duration::from_secs(300);
/// Plaintext reads that may hold a lease at once; further admissions wait for a free slot.
const LEASE_SLOTS: usize = 256;

/// Archives pinned by leases with their pin counts, and archives claimed for deletion.
#[derive(Default)]
struct ArchiveUse {
    pins: HashMap<ArchiveKey, usize>,
    deleting: HashSet<ArchiveKey>,
}

type Pins = Arc<StdMutex<ArchiveUse>>;

/// One unlock session of a key generation.
struct Session {
    session_id: Ulid,
    sequence: Ulid,
    deadline_ms: Option<u64>,
    secret: SharedSecret,
    public_key: [u8; 32],
    active: bool,
    unlocked_at: SystemTime,
    prepared_at: Instant,
    /// The requested duration and maximum; they start counting at activation.
    bounds: (Option<Duration>, Option<Duration>),
    deadline: Option<Instant>,
    max_deadline: Option<Instant>,
}

impl Session {
    fn expired(&self, now: Instant) -> bool {
        self.deadline.is_some_and(|deadline| deadline <= now)
            || (!self.active && now.duration_since(self.prepared_at) >= PREPARED_TTL)
    }

    fn status(&self, key: BucketKeyRef, now: Instant) -> UnlockStatus {
        let left = |deadline: Option<Instant>| deadline.map(|at| at.saturating_duration_since(now));
        UnlockStatus {
            key,
            session_id: self.session_id,
            sequence: self.sequence,
            deadline_ms: self.deadline_ms,
            active: self.active,
            unlocked_at: self.unlocked_at,
            remaining: left(self.deadline),
            max_remaining: left(self.max_deadline),
        }
    }
}

/// Keeps an archive pinned while the lease that holds it lives. A pin without a key is the copy
/// lease of keyless work on an archive; cleanup never deletes a pinned archive.
pub struct ArchivePin {
    archive: ArchiveKey,
    pins: Pins,
}

impl Drop for ArchivePin {
    fn drop(&mut self) {
        let Ok(mut uses) = self.pins.lock() else {
            return;
        };
        if let Some(count) = uses.pins.get_mut(&self.archive) {
            *count -= 1;
            if *count == 0 {
                uses.pins.remove(&self.archive);
            }
        }
    }
}

/// Keeps an archive from being admitted or pinned while its backend copy is deleted.
pub(super) struct DeleteClaim {
    archive: ArchiveKey,
    pins: Pins,
}

impl Drop for DeleteClaim {
    fn drop(&mut self) {
        if let Ok(mut uses) = self.pins.lock() {
            uses.deleting.remove(&self.archive);
        }
    }
}

/// The adapter state behind a `ReadLease`: the shared key, the archive pin and the lease slot.
pub(super) struct LeaseGuard {
    secret: SharedSecret,
    _pin: ArchivePin,
    _slot: OwnedSemaphorePermit,
}

impl LeaseGuard {
    /// The key `lease` admitted. It stays usable after a lock until the lease ends.
    pub(super) fn secret(lease: &ReadLease) -> Option<&SharedSecret> {
        let guard = lease.guard().downcast_ref::<LeaseGuard>()?;
        Some(&guard.secret)
    }
}

/// Unlocked key generations, keyed by bucket id and generation. It never evicts an unlocked
/// key to make room; a full registry refuses the new unlock instead.
pub(super) struct UnlockRegistry {
    capacity: usize,
    sessions: HashMap<BucketKeyRef, Vec<Session>>,
    /// Sessions that reached their deadline before their timer ran, so it still records the lock.
    expired: HashMap<(BucketKeyRef, Ulid), Ulid>,
    pins: Pins,
    leases: Arc<Semaphore>,
}

/// Shows counts only, so no formatted handler carries a key.
impl std::fmt::Debug for UnlockRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnlockRegistry")
            .field("capacity", &self.capacity)
            .field("generations", &self.sessions.len())
            .finish_non_exhaustive()
    }
}

impl UnlockRegistry {
    pub(super) fn new(capacity: usize) -> Self {
        Self::with_leases(capacity, LEASE_SLOTS)
    }

    fn with_leases(capacity: usize, leases: usize) -> Self {
        Self {
            capacity,
            sessions: HashMap::new(),
            expired: HashMap::new(),
            pins: Arc::default(),
            leases: Arc::new(Semaphore::new(leases)),
        }
    }

    /// Holds a key checked against `public_key` without admitting reads. Without a duration
    /// the unlock lasts until the maximum, or until lock or restart when there is none.
    pub(super) fn prepare(
        &mut self,
        key: BucketKeyRef,
        public_key: &[u8; 32],
        private_key: SharedSecret,
        bounds: (Option<Duration>, Option<Duration>),
        now: (Instant, SystemTime),
    ) -> Result<KeyTicket, BucketKeyError> {
        let (duration, max) = bounds;
        self.purge(now.0);
        if !key_matches(private_key.bytes(), public_key) {
            return Err(BucketKeyError::WrongKey);
        }
        let duration = duration.or(max);
        if matches!((duration, max), (Some(duration), Some(max)) if duration > max) {
            return Err(BucketKeyError::InvalidDuration);
        }
        // A bound that no clock can reach is refused before anything is held.
        let reachable =
            |bound: Option<Duration>| bound.is_none_or(|bound| now.0.checked_add(bound).is_some());
        if !reachable(duration) || !reachable(max) {
            return Err(BucketKeyError::InvalidDuration);
        }
        let generations = self.generations(key.bucket_id);
        let known = self.sessions.contains_key(&key);
        if !known && generations == 0 && self.buckets() >= self.capacity {
            return Err(BucketKeyError::Capacity);
        }
        if !known && generations >= BUCKET_GENERATIONS {
            return Err(BucketKeyError::Capacity);
        }
        let sessions = self.sessions.entry(key).or_default();
        if sessions.iter().any(|session| !session.active) {
            return Err(BucketKeyError::Capacity);
        }
        let session_id = Ulid::generate();
        sessions.push(Session {
            session_id,
            sequence: Ulid::nil(),
            deadline_ms: None,
            secret: private_key,
            public_key: *public_key,
            active: false,
            unlocked_at: now.1,
            prepared_at: now.0,
            bounds: (duration, max),
            deadline: None,
            max_deadline: None,
        });
        Ok(KeyTicket { key, session_id })
    }

    /// Starts admitting reads with a prepared key; it replaces an older session of the generation.
    /// The session's bounds start now, after the audit intent was stored.
    pub(super) fn activate(
        &mut self,
        ticket: KeyTicket,
        now: (Instant, SystemTime),
    ) -> Result<UnlockStatus, BucketKeyError> {
        let (now, unlocked_at) = now;
        self.purge(now);
        let sessions = self
            .sessions
            .get_mut(&ticket.key)
            .ok_or(BucketKeyError::SessionMismatch)?;
        let index = sessions
            .iter()
            .position(|session| !session.active && session.session_id == ticket.session_id)
            .ok_or(BucketKeyError::SessionMismatch)?;
        let (duration, max) = sessions[index].bounds;
        let after = |bound: Option<Duration>| match bound {
            Some(bound) => now
                .checked_add(bound)
                .map(Some)
                .ok_or(BucketKeyError::InvalidDuration),
            None => Ok(None),
        };
        let (deadline, max_deadline) = (after(duration)?, after(max)?);
        let mut session = sessions.swap_remove(index);
        session.active = true;
        session.unlocked_at = unlocked_at;
        session.deadline = deadline;
        let at_ms = unlocked_at
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        session.sequence = next_event_id(at_ms);
        session.deadline_ms = duration.and_then(|duration| deadline_after(at_ms, duration));
        session.max_deadline = max_deadline;
        let status = session.status(ticket.key, now);
        sessions.clear();
        sessions.push(session);
        Ok(status)
    }

    /// Forgets the session of `ticket`, prepared or active.
    pub(super) fn discard(&mut self, ticket: KeyTicket) {
        if let Some(sessions) = self.sessions.get_mut(&ticket.key) {
            sessions.retain(|session| session.session_id != ticket.session_id);
            if sessions.is_empty() {
                self.sessions.remove(&ticket.key);
            }
        }
    }

    pub(super) fn status(&mut self, bucket_id: Ulid, now: Instant) -> Vec<UnlockStatus> {
        self.purge(now);
        let mut statuses: Vec<_> = self
            .sessions
            .iter()
            .filter(|(key, _)| key.bucket_id == bucket_id)
            .flat_map(|(key, sessions)| sessions.iter().map(|session| session.status(*key, now)))
            .collect();
        statuses.sort_by_key(|status| (status.key.generation, status.active));
        statuses
    }

    /// Moves the timed lock of an active session; it never passes the session maximum.
    pub(super) fn extend(
        &mut self,
        key: BucketKeyRef,
        session_id: Ulid,
        duration: Option<Duration>,
        now: (Instant, SystemTime),
    ) -> Result<UnlockStatus, BucketKeyError> {
        let (now, at) = now;
        self.purge(now);
        let session = self
            .sessions
            .get_mut(&key)
            .ok_or(BucketKeyError::Locked(key.bucket_id))?
            .iter_mut()
            .find(|session| session.active && session.session_id == session_id)
            .ok_or(BucketKeyError::SessionMismatch)?;
        let deadline = match duration {
            Some(duration) => Some(
                now.checked_add(duration)
                    .ok_or(BucketKeyError::InvalidDuration)?,
            ),
            None => session.max_deadline,
        };
        if let (Some(max), Some(deadline)) = (session.max_deadline, deadline)
            && deadline > max
        {
            return Err(BucketKeyError::InvalidDuration);
        }
        session.deadline = deadline;
        let at_ms = at
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        session.sequence = next_event_id(at_ms);
        session.deadline_ms = match duration {
            Some(duration) => deadline_after(at_ms, duration),
            None => session.bounds.1.and_then(|max| {
                let start = session
                    .unlocked_at
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                deadline_after(start, max)
            }),
        };
        Ok(session.status(key, now))
    }

    /// Locks every generation of a bucket, or only `only` when a timer names its session. A
    /// timer locks only a session past its deadline, checked here under the registry lock, so
    /// an old callback of an extended session locks nothing; an expired session is reported
    /// once, so its timed lock is recorded.
    pub(super) fn lock(
        &mut self,
        bucket_id: Ulid,
        only: Option<KeyTicket>,
        now: Instant,
    ) -> (Vec<KeyTicket>, Ulid) {
        self.purge(now);
        if let Some(ticket) = only {
            let expired = (ticket.key.bucket_id == bucket_id)
                .then(|| self.expired.remove(&(ticket.key, ticket.session_id)))
                .flatten();
            return expired.map_or((Vec::new(), Ulid::nil()), |sequence| {
                (vec![ticket], sequence)
            });
        }
        let mut locked = Vec::new();
        self.sessions.retain(|key, sessions| {
            if key.bucket_id != bucket_id {
                return true;
            }
            locked.extend(sessions.iter().map(|session| KeyTicket {
                key: *key,
                session_id: session.session_id,
            }));
            false
        });
        locked.sort_by_key(|ticket| ticket.key.generation);
        (
            locked,
            next_event_id(aruna_core::time::unix_timestamp_millis()),
        )
    }

    /// A lease for one plaintext read of `archive`, while its key generation is unlocked.
    pub(super) fn admit(
        &mut self,
        key: BucketKeyRef,
        archive: ArchiveKey,
        now: Instant,
        slot: OwnedSemaphorePermit,
    ) -> Result<ReadLease, BlobError> {
        self.purge(now);
        let session = self
            .sessions
            .get(&key)
            .and_then(|sessions| sessions.iter().find(|session| session.active))
            .ok_or(BucketKeyError::Locked(key.bucket_id))?;
        let guard = LeaseGuard {
            secret: session.secret.clone(),
            _pin: self.pin(archive.clone())?,
            _slot: slot,
        };
        Ok(ReadLease::new(
            key,
            archive,
            session.session_id,
            Arc::new(guard),
        ))
    }

    /// The handle and public key of the active session of `key`, for sealing inside the adapter.
    pub(super) fn unlocked_key(
        &mut self,
        key: BucketKeyRef,
        now: Instant,
    ) -> Result<(SharedSecret, [u8; 32]), BucketKeyError> {
        self.purge(now);
        let session = self
            .sessions
            .get(&key)
            .and_then(|sessions| sessions.iter().find(|session| session.active))
            .ok_or(BucketKeyError::Locked(key.bucket_id))?;
        Ok((session.secret.clone(), session.public_key))
    }

    /// Pins an archive without a key, so cleanup keeps it while keyless work uses it.
    /// An archive claimed for deletion is refused.
    pub(super) fn pin(&self, archive: ArchiveKey) -> Result<ArchivePin, BlobError> {
        let mut uses = self.pins.lock().map_err(|_| poisoned())?;
        if uses.deleting.contains(&archive) {
            return Err(BlobError::ReadError(
                "the archive is being deleted".to_string(),
            ));
        }
        *uses.pins.entry(archive.clone()).or_default() += 1;
        Ok(ArchivePin {
            archive,
            pins: Arc::clone(&self.pins),
        })
    }

    /// Fails closed: an unreadable pin table counts every archive as pinned.
    #[cfg(test)]
    fn is_pinned(&self, archive: &ArchiveKey) -> bool {
        self.pins
            .lock()
            .map_or(true, |uses| uses.pins.contains_key(archive))
    }

    /// Claims an unpinned archive for deletion. Hold the claim across the backend delete so no
    /// lease or pin can start meanwhile.
    pub(super) fn claim_delete(&self, archive: &ArchiveKey) -> Result<DeleteClaim, BlobError> {
        let mut uses = self.pins.lock().map_err(|_| poisoned())?;
        if uses.pins.contains_key(archive) || !uses.deleting.insert(archive.clone()) {
            return Err(BlobError::DeleteError("the archive is in use".to_string()));
        }
        Ok(DeleteClaim {
            archive: archive.clone(),
            pins: Arc::clone(&self.pins),
        })
    }

    pub(super) fn lease_slots(&self) -> Arc<Semaphore> {
        Arc::clone(&self.leases)
    }

    /// Forgets every session, for shutdown; admitted leases keep their own key until they end.
    pub(super) fn clear(&mut self) {
        self.sessions.clear();
        self.expired.clear();
    }

    /// Drops the session of `ticket` if it was prepared but never activated.
    fn drop_prepared(&mut self, ticket: KeyTicket) {
        if let Some(sessions) = self.sessions.get_mut(&ticket.key) {
            sessions.retain(|session| session.active || session.session_id != ticket.session_id);
            if sessions.is_empty() {
                self.sessions.remove(&ticket.key);
            }
        }
    }

    /// Removes sessions past their deadline; admitted leases keep their own key. An active
    /// session keeps only its id as evidence until its timer records the lock.
    fn purge(&mut self, now: Instant) {
        let expired = &mut self.expired;
        self.sessions.retain(|key, sessions| {
            sessions.retain(|session| {
                let ended = session.expired(now);
                if ended && session.active {
                    expired.insert(
                        (*key, session.session_id),
                        next_event_id(aruna_core::time::unix_timestamp_millis()),
                    );
                }
                !ended
            });
            !sessions.is_empty()
        });
    }

    fn buckets(&self) -> usize {
        let mut buckets: Vec<_> = self.sessions.keys().map(|key| key.bucket_id).collect();
        buckets.sort_unstable();
        buckets.dedup();
        buckets.len()
    }

    fn generations(&self, bucket_id: Ulid) -> usize {
        self.sessions
            .keys()
            .filter(|key| key.bucket_id == bucket_id)
            .count()
    }
}

fn poisoned() -> BlobError {
    BlobError::ReadError("unlock registry poisoned".to_string())
}

/// Drops a prepared key that is still not activated when it can no longer activate.
fn expire_prepared(registry: Weak<StdMutex<UnlockRegistry>>, ticket: KeyTicket) {
    tokio::spawn(async move {
        tokio::time::sleep(PREPARED_TTL).await;
        if let Some(registry) = registry.upgrade()
            && let Ok(mut registry) = registry.lock()
        {
            registry.drop_prepared(ticket);
        }
    });
}

impl super::BlobHandle {
    /// Forgets every unlocked key at shutdown; reads already admitted finish with their lease.
    pub fn clear_unlocks(&self) {
        if let Ok(mut registry) = self.handler.unlocks.lock() {
            registry.clear();
        }
    }
}

impl super::BlobHandler {
    /// Runs one unlock registry effect. Monotonic time decides every deadline.
    pub(super) fn unlock_effect(&self, effect: BlobEffect) -> BlobEvent {
        let Ok(mut registry) = self.unlocks.lock() else {
            return BlobEvent::Error(poisoned());
        };
        let now = Instant::now();
        let result = match effect {
            BlobEffect::PrepareKey {
                key,
                public_key,
                private_key,
                duration,
                max,
            } => registry
                .prepare(
                    key,
                    &public_key,
                    private_key,
                    (duration, max),
                    (now, SystemTime::now()),
                )
                .map(|ticket| {
                    expire_prepared(Arc::downgrade(&self.unlocks), ticket);
                    BlobEvent::KeyPrepared { ticket }
                }),
            BlobEffect::ActivateKey { ticket } => registry
                .activate(ticket, (now, SystemTime::now()))
                .map(|status| BlobEvent::KeyActivated { status }),
            BlobEffect::DiscardKey { ticket } => {
                registry.discard(ticket);
                Ok(BlobEvent::KeyDiscarded { ticket })
            }
            BlobEffect::ReadKeyStatus { bucket_id } => Ok(BlobEvent::KeyStatus {
                generations: registry.status(bucket_id, now),
            }),
            BlobEffect::ExtendKey {
                key,
                session_id,
                duration,
            } => registry
                .extend(key, session_id, duration, (now, SystemTime::now()))
                .map(|status| BlobEvent::KeyExtended { status }),
            BlobEffect::LockKey { bucket_id, session } => {
                let (locked, sequence) = registry.lock(bucket_id, session, now);
                let live = session
                    .filter(|_| locked.is_empty())
                    .and_then(|ticket| {
                        registry.status(bucket_id, now).into_iter().find(|status| {
                            status.active
                                && status.key == ticket.key
                                && status.session_id == ticket.session_id
                        })
                    })
                    .map(Box::new);
                Ok(BlobEvent::KeyLocked {
                    locked,
                    sequence,
                    live,
                })
            }
            _ => Err(BucketKeyError::Unsupported),
        };
        result.unwrap_or_else(|error| BlobEvent::Error(error.into()))
    }

    /// Admits a read once a lease slot is free; the slot stays with the lease until it ends.
    pub(super) async fn admit_read(&self, key: BucketKeyRef, archive: ArchiveKey) -> BlobEvent {
        let slots = match self.unlocks.lock() {
            Ok(registry) => registry.lease_slots(),
            Err(_) => return BlobEvent::Error(poisoned()),
        };
        let Ok(slot) = slots.acquire_owned().await else {
            return BlobEvent::Error(poisoned());
        };
        let admitted = match self.unlocks.lock() {
            Ok(mut registry) => registry.admit(key, archive, Instant::now(), slot),
            Err(_) => Err(poisoned()),
        };
        match admitted {
            Ok(lease) => BlobEvent::ReadAdmitted { lease },
            Err(error) => BlobEvent::Error(error),
        }
    }

    /// Claims an archive for deletion; see `UnlockRegistry::claim_delete`.
    pub(super) fn claim_delete(&self, archive: &ArchiveKey) -> Result<DeleteClaim, BlobError> {
        self.unlocks
            .lock()
            .map_err(|_| poisoned())?
            .claim_delete(archive)
    }

    /// Seals copies with an unlocked key; the registry lock is not held while sealing.
    pub(super) fn seal_unlocked(
        &self,
        key: BucketKeyRef,
        origin: (RealmId, NodeId),
        holders: &[CopyTarget],
    ) -> BlobEvent {
        let unlocked = match self.unlocks.lock() {
            Ok(mut registry) => registry.unlocked_key(key, Instant::now()),
            Err(_) => Err(BucketKeyError::Locked(key.bucket_id)),
        };
        let now_ms = aruna_core::time::unix_timestamp_millis();
        let sealed = unlocked.and_then(|(secret, public_key)| {
            seal_copies(key, &public_key, secret.bytes(), origin, holders, now_ms)
        });
        match sealed {
            Ok(copies) => BlobEvent::CopiesSealed { copies },
            Err(error) => BlobEvent::Error(error.into()),
        }
    }
}

#[cfg(test)]
#[path = "unlock_tests.rs"]
mod tests;
