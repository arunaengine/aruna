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
    BucketKeyError, BucketKeyRef, CopyTarget, KeyTicket, ReadLease, UnlockStatus, key_matches,
    seal_copies,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime};
use ulid::Ulid;

/// Buckets that may be unlocked at once on one node.
pub(super) const UNLOCKED_BUCKETS: usize = 1024;
/// Generations of one bucket that may be unlocked at once: the active one and a transition source.
const BUCKET_GENERATIONS: usize = 2;
/// A prepared key that is neither activated nor discarded in this time is dropped.
const PREPARED_TTL: Duration = Duration::from_secs(300);

/// Archives pinned by leases, with the number of leases that pin each.
type Pins = Arc<StdMutex<HashMap<ArchiveKey, usize>>>;

/// One unlock session of a key generation.
struct Session {
    session_id: Ulid,
    secret: SharedSecret,
    public_key: [u8; 32],
    active: bool,
    unlocked_at: SystemTime,
    prepared_at: Instant,
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
        let Ok(mut pins) = self.pins.lock() else {
            return;
        };
        if let Some(count) = pins.get_mut(&self.archive) {
            *count -= 1;
            if *count == 0 {
                pins.remove(&self.archive);
            }
        }
    }
}

/// The adapter state behind a `ReadLease`: it holds the shared key and the archive pin.
pub(super) struct LeaseGuard {
    _secret: SharedSecret,
    _pin: ArchivePin,
}

/// Unlocked key generations, keyed by bucket id and generation. It never evicts an unlocked
/// key to make room; a full registry refuses the new unlock instead.
pub(super) struct UnlockRegistry {
    capacity: usize,
    sessions: HashMap<BucketKeyRef, Vec<Session>>,
    pins: Pins,
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
        Self {
            capacity,
            sessions: HashMap::new(),
            pins: Arc::default(),
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
            secret: private_key,
            public_key: *public_key,
            active: false,
            unlocked_at: now.1,
            prepared_at: now.0,
            deadline: duration.map(|duration| now.0 + duration),
            max_deadline: max.map(|max| now.0 + max),
        });
        Ok(KeyTicket { key, session_id })
    }

    /// Starts admitting reads with a prepared key; it replaces an older session of the generation.
    pub(super) fn activate(
        &mut self,
        ticket: KeyTicket,
        now: Instant,
    ) -> Result<UnlockStatus, BucketKeyError> {
        self.purge(now);
        let sessions = self
            .sessions
            .get_mut(&ticket.key)
            .ok_or(BucketKeyError::SessionMismatch)?;
        let index = sessions
            .iter()
            .position(|session| !session.active && session.session_id == ticket.session_id)
            .ok_or(BucketKeyError::SessionMismatch)?;
        let mut session = sessions.swap_remove(index);
        session.active = true;
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
        now: Instant,
    ) -> Result<UnlockStatus, BucketKeyError> {
        self.purge(now);
        let session = self
            .sessions
            .get_mut(&key)
            .ok_or(BucketKeyError::Locked(key.bucket_id))?
            .iter_mut()
            .find(|session| session.active && session.session_id == session_id)
            .ok_or(BucketKeyError::SessionMismatch)?;
        let deadline = duration
            .map(|duration| now + duration)
            .or(session.max_deadline);
        if let (Some(max), Some(deadline)) = (session.max_deadline, deadline)
            && deadline > max
        {
            return Err(BucketKeyError::InvalidDuration);
        }
        session.deadline = deadline;
        Ok(session.status(key, now))
    }

    /// Locks every generation of a bucket, or only `only` when a timer names its session.
    pub(super) fn lock(&mut self, bucket_id: Ulid, only: Option<KeyTicket>) -> Vec<KeyTicket> {
        let mut locked = Vec::new();
        self.sessions.retain(|key, sessions| {
            if key.bucket_id != bucket_id {
                return true;
            }
            sessions.retain(|session| {
                let named = only.is_none_or(|ticket| {
                    ticket.key == *key && ticket.session_id == session.session_id
                });
                if named {
                    locked.push(KeyTicket {
                        key: *key,
                        session_id: session.session_id,
                    });
                }
                !named
            });
            !sessions.is_empty()
        });
        locked.sort_by_key(|ticket| ticket.key.generation);
        locked
    }

    /// A lease for one plaintext read of `archive`, while its key generation is unlocked.
    pub(super) fn admit(
        &mut self,
        key: BucketKeyRef,
        archive: ArchiveKey,
        now: Instant,
    ) -> Result<ReadLease, BucketKeyError> {
        self.purge(now);
        let session = self
            .sessions
            .get(&key)
            .and_then(|sessions| sessions.iter().find(|session| session.active))
            .ok_or(BucketKeyError::Locked(key.bucket_id))?;
        let guard = LeaseGuard {
            _secret: session.secret.clone(),
            _pin: self.pin(archive.clone()),
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
    pub(super) fn pin(&self, archive: ArchiveKey) -> ArchivePin {
        if let Ok(mut pins) = self.pins.lock() {
            *pins.entry(archive.clone()).or_default() += 1;
        }
        ArchivePin {
            archive,
            pins: Arc::clone(&self.pins),
        }
    }

    /// Fails closed: an unreadable pin table counts every archive as pinned.
    pub(super) fn is_pinned(&self, archive: &ArchiveKey) -> bool {
        self.pins
            .lock()
            .map_or(true, |pins| pins.contains_key(archive))
    }

    /// Removes sessions past their deadline; admitted leases keep their own key.
    fn purge(&mut self, now: Instant) {
        self.sessions.retain(|_, sessions| {
            sessions.retain(|session| !session.expired(now));
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

impl super::BlobHandler {
    /// Runs one unlock registry effect. Monotonic time decides every deadline.
    pub(super) fn unlock_effect(&self, effect: BlobEffect) -> BlobEvent {
        let Ok(mut registry) = self.unlocks.lock() else {
            return BlobEvent::Error(BlobError::ReadError("unlock registry poisoned".to_string()));
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
                .map(|ticket| BlobEvent::KeyPrepared { ticket }),
            BlobEffect::ActivateKey { ticket } => registry
                .activate(ticket, now)
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
                .extend(key, session_id, duration, now)
                .map(|status| BlobEvent::KeyExtended { status }),
            BlobEffect::LockKey { bucket_id, session } => Ok(BlobEvent::KeyLocked {
                locked: registry.lock(bucket_id, session),
            }),
            BlobEffect::AdmitRead { key, archive } => registry
                .admit(key, archive, now)
                .map(|lease| BlobEvent::ReadAdmitted { lease }),
            _ => Err(BucketKeyError::Unsupported),
        };
        result.unwrap_or_else(|error| BlobEvent::Error(error.into()))
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

    pub(super) fn archive_pinned(&self, archive: &ArchiveKey) -> bool {
        self.unlocks
            .lock()
            .map_or(true, |registry| registry.is_pinned(archive))
    }
}

#[cfg(test)]
#[path = "unlock_tests.rs"]
mod tests;
