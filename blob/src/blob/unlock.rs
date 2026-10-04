//! Holds unlocked bucket keys in memory and admits plaintext reads under leases.
//! Each key lives in one allocation that its leases share, so a lock stops new reads at once
//! while admitted reads finish; the key is zeroed when the last of them ends.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::SecretBytes;
use aruna_core::effects::BlobEffect;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::structs::storage::blob::ArchiveKey;
use aruna_core::structs::storage::encryption::{
    BucketKeyError, BucketKeyRef, KeyTicket, ReadLease, UnlockStatus, key_matches,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use ulid::Ulid;

/// Buckets that may be unlocked at once on one node.
pub(super) const UNLOCKED_BUCKETS: usize = 1024;
/// Generations of one bucket that may be unlocked at once: the active one and a transition source.
const BUCKET_GENERATIONS: usize = 2;
/// A prepared key that is neither activated nor discarded in this time is dropped.
const PREPARED_TTL: Duration = Duration::from_secs(300);

/// One unlock session of a key generation.
struct Session {
    session_id: Ulid,
    secret: Arc<SecretBytes>,
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

/// The adapter state behind a `ReadLease`: it holds the shared key.
pub(super) struct LeaseGuard {
    _secret: Arc<SecretBytes>,
}

/// Unlocked key generations, keyed by bucket id and generation. It never evicts an unlocked
/// key to make room; a full registry refuses the new unlock instead.
pub(super) struct UnlockRegistry {
    capacity: usize,
    sessions: HashMap<BucketKeyRef, Vec<Session>>,
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
        }
    }

    /// Holds a key checked against `public_key` without admitting reads. Without a duration
    /// the unlock lasts until the maximum, or until lock or restart when there is none.
    pub(super) fn prepare(
        &mut self,
        key: BucketKeyRef,
        public_key: &[u8; 32],
        private_key: SecretBytes,
        bounds: (Option<Duration>, Option<Duration>),
        now: (Instant, SystemTime),
    ) -> Result<KeyTicket, BucketKeyError> {
        let (duration, max) = bounds;
        self.purge(now.0);
        if !key_matches(&private_key, public_key) {
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
            secret: Arc::new(private_key),
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
            _secret: Arc::clone(&session.secret),
        };
        Ok(ReadLease::new(
            key,
            archive,
            session.session_id,
            Arc::new(guard),
        ))
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
}

#[cfg(test)]
#[path = "unlock_tests.rs"]
mod tests;
