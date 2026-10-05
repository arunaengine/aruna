//! The unlock registry under injected monotonic time: keys, bounds, locks and leases.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{LeaseGuard, UNLOCKED_BUCKETS, UnlockRegistry, expire_prepared};
use aruna_core::compute::{SecretBytes, SharedSecret};
use aruna_core::errors::BlobError;
use aruna_core::structs::storage::blob::{ArchiveKey, BackendRef};
use aruna_core::structs::storage::encryption::{
    BucketKeyError, BucketKeyRef, KeyTicket, ReadLease, public_key_of,
};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime};
use ulid::Ulid;

const MINUTE: Duration = Duration::from_secs(60);

fn private(seed: u8) -> SharedSecret {
    SharedSecret::new(SecretBytes::new(vec![seed; 32]))
}

fn reference(bucket: u8, generation: u64) -> BucketKeyRef {
    BucketKeyRef::new(Ulid::from_bytes([bucket; 16]), generation)
}

fn archive(seed: u8) -> ArchiveKey {
    ArchiveKey::new(Ulid::from_bytes([seed; 16]), BackendRef::node_default())
}

/// Admits a read with a free lease slot.
fn admit(
    registry: &mut UnlockRegistry,
    key: BucketKeyRef,
    archive: ArchiveKey,
    now: Instant,
) -> Result<ReadLease, BlobError> {
    let slot = registry.lease_slots().try_acquire_owned().unwrap();
    registry.admit(key, archive, now, slot)
}

/// Prepares and activates the key of `seed` for `key`.
fn unlock(
    registry: &mut UnlockRegistry,
    key: BucketKeyRef,
    seed: u8,
    bounds: (Option<Duration>, Option<Duration>),
    now: Instant,
) -> Result<KeyTicket, BucketKeyError> {
    let public = public_key_of(private(seed).bytes()).unwrap();
    let ticket = registry.prepare(
        key,
        &public,
        private(seed),
        bounds,
        (now, SystemTime::now()),
    )?;
    registry.activate(ticket, (now, SystemTime::now()))?;
    Ok(ticket)
}

#[test]
fn admits_after_activation() {
    let mut registry = UnlockRegistry::new(UNLOCKED_BUCKETS);
    let now = Instant::now();
    let key = reference(1, 1);
    let public = public_key_of(private(1).bytes()).unwrap();

    let wrong = registry.prepare(
        key,
        &public,
        private(2),
        (None, None),
        (now, SystemTime::now()),
    );
    assert_eq!(wrong, Err(BucketKeyError::WrongKey));
    let ticket = registry
        .prepare(
            key,
            &public,
            private(1),
            (None, None),
            (now, SystemTime::now()),
        )
        .unwrap();
    // A prepared key admits nothing until it is activated.
    assert!(admit(&mut registry, key, archive(1), now).is_err());
    assert!(!registry.status(key.bucket_id, now)[0].active);
    registry.activate(ticket, (now, SystemTime::now())).unwrap();
    let lease = admit(&mut registry, key, archive(1), now).unwrap();
    assert_eq!(lease.session_id, ticket.session_id);
    let other = reference(1, 2);
    assert_eq!(
        admit(&mut registry, other, archive(1), now).unwrap_err(),
        BlobError::BucketKey(BucketKeyError::Locked(other.bucket_id))
    );

    registry.discard(ticket);
    assert!(registry.status(key.bucket_id, now).is_empty());
    assert!(admit(&mut registry, key, archive(1), now).is_err());
}

#[test]
fn activation_starts_bounds() {
    let mut registry = UnlockRegistry::new(UNLOCKED_BUCKETS);
    let prepared_at = Instant::now();
    let key = reference(1, 1);
    let public = public_key_of(private(1).bytes()).unwrap();
    let bounds = (Some(MINUTE), Some(2 * MINUTE));
    let ticket = registry
        .prepare(
            key,
            &public,
            private(1),
            bounds,
            (prepared_at, SystemTime::now()),
        )
        .unwrap();
    // Audit work before activation does not shorten the unlock.
    let activated_at = prepared_at + Duration::from_secs(20);
    let status = registry
        .activate(ticket, (activated_at, SystemTime::now()))
        .unwrap();
    assert_eq!(
        (status.remaining, status.max_remaining),
        (Some(MINUTE), Some(2 * MINUTE))
    );
    let before = activated_at + Duration::from_secs(59);
    assert!(admit(&mut registry, key, archive(1), before).is_ok());
    assert!(admit(&mut registry, key, archive(1), activated_at + MINUTE).is_err());
}

#[test]
fn unreachable_bounds_refused() {
    let mut registry = UnlockRegistry::new(UNLOCKED_BUCKETS);
    let now = Instant::now();
    let key = reference(1, 1);
    let endless = Duration::MAX;
    let refused = unlock(&mut registry, key, 1, (Some(endless), None), now);
    assert_eq!(refused, Err(BucketKeyError::InvalidDuration));
    let ticket = unlock(&mut registry, key, 1, (None, None), now).unwrap();
    let extended = registry.extend(
        key,
        ticket.session_id,
        Some(endless),
        (now, SystemTime::now()),
    );
    assert_eq!(extended, Err(BucketKeyError::InvalidDuration));
    assert!(
        admit(&mut registry, key, archive(1), now).is_ok(),
        "the session stays as it was"
    );
}

#[test]
fn mutation_deadlines_match() {
    let mut registry = UnlockRegistry::new(UNLOCKED_BUCKETS);
    let key = reference(1, 1);
    let start = Instant::now();
    let wall = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
    let public = public_key_of(private(1).bytes()).unwrap();
    let ticket = registry
        .prepare(
            key,
            &public,
            private(1),
            (Some(MINUTE), Some(3 * MINUTE)),
            (start, wall),
        )
        .unwrap();
    let active = registry
        .activate(
            ticket,
            (
                start + Duration::from_secs(30),
                wall + Duration::from_secs(30),
            ),
        )
        .unwrap();
    assert_eq!(active.deadline_ms, Some(91_000));
    assert_eq!(active.remaining, Some(MINUTE));
    let extended = registry
        .extend(
            key,
            ticket.session_id,
            Some(MINUTE),
            (
                start + Duration::from_secs(60),
                wall + Duration::from_secs(60),
            ),
        )
        .unwrap();
    assert_eq!(extended.deadline_ms, Some(121_000));
    assert!(extended.sequence > active.sequence);
    let unlimited = registry
        .extend(
            key,
            ticket.session_id,
            None,
            (
                start + Duration::from_secs(70),
                wall + Duration::from_secs(70),
            ),
        )
        .unwrap();
    assert_eq!(unlimited.deadline_ms, Some(211_000));
    assert!(unlimited.sequence > extended.sequence);
}

#[test]
fn full_registry_refuses() {
    let mut registry = UnlockRegistry::new(1);
    let now = Instant::now();
    unlock(&mut registry, reference(1, 1), 1, (None, None), now).unwrap();
    // A source generation of the same bucket fits; a third generation or bucket does not.
    unlock(&mut registry, reference(1, 2), 2, (None, None), now).unwrap();
    let third = unlock(&mut registry, reference(1, 3), 3, (None, None), now);
    assert_eq!(third, Err(BucketKeyError::Capacity));
    let bucket = unlock(&mut registry, reference(2, 1), 4, (None, None), now);
    assert_eq!(bucket, Err(BucketKeyError::Capacity));
    // Nothing was evicted to make room.
    assert!(admit(&mut registry, reference(1, 1), archive(1), now).is_ok());
    assert!(admit(&mut registry, reference(1, 2), archive(1), now).is_ok());
}

#[test]
fn deadlines_close_admission() {
    let mut registry = UnlockRegistry::new(UNLOCKED_BUCKETS);
    let start = Instant::now();
    let key = reference(1, 1);
    let too_long = unlock(
        &mut registry,
        key,
        1,
        (Some(2 * MINUTE), Some(MINUTE)),
        start,
    );
    assert_eq!(too_long, Err(BucketKeyError::InvalidDuration));
    // Without a duration the unlock lasts until the maximum.
    let ticket = unlock(&mut registry, key, 1, (None, Some(MINUTE)), start).unwrap();
    let status = registry.status(key.bucket_id, start);
    assert_eq!(status[0].remaining, Some(MINUTE));

    let later = start + Duration::from_secs(30);
    let short = registry
        .extend(
            key,
            ticket.session_id,
            Some(Duration::from_secs(10)),
            (later, SystemTime::now()),
        )
        .unwrap();
    assert_eq!(short.remaining, Some(Duration::from_secs(10)));
    assert_eq!(short.max_remaining, Some(Duration::from_secs(30)));
    let beyond = registry.extend(
        key,
        ticket.session_id,
        Some(MINUTE),
        (later, SystemTime::now()),
    );
    assert_eq!(beyond, Err(BucketKeyError::InvalidDuration));
    let stranger = registry.extend(key, Ulid::generate(), None, (later, SystemTime::now()));
    assert_eq!(stranger, Err(BucketKeyError::SessionMismatch));
    // A session id is only valid with the generation it unlocked.
    let source = reference(1, 7);
    unlock(&mut registry, source, 7, (None, None), later).unwrap();
    let elsewhere = registry.extend(source, ticket.session_id, None, (later, SystemTime::now()));
    assert_eq!(elsewhere, Err(BucketKeyError::SessionMismatch));
    let source_session = registry.status(source.bucket_id, later)[1].session_id;
    let only = KeyTicket {
        key: source,
        session_id: source_session,
    };
    registry.discard(only);

    // A delayed timer cannot keep the key admitted past its deadline.
    let expired = later + Duration::from_secs(10);
    assert!(admit(&mut registry, key, archive(1), expired).is_err());
    assert!(registry.status(key.bucket_id, expired).is_empty());
    let locked = registry.extend(key, ticket.session_id, None, (expired, SystemTime::now()));
    assert_eq!(locked, Err(BucketKeyError::Locked(key.bucket_id)));
}

#[test]
fn lock_keeps_leases() {
    let mut registry = UnlockRegistry::new(UNLOCKED_BUCKETS);
    let now = Instant::now();
    let (active, source) = (reference(1, 2), reference(1, 1));
    let first = unlock(&mut registry, active, 1, (None, None), now).unwrap();
    unlock(&mut registry, source, 2, (None, None), now).unwrap();
    let lease = admit(&mut registry, active, archive(7), now).unwrap();

    // A stale timer names an older session and locks nothing.
    let fresh = unlock(&mut registry, active, 1, (None, None), now).unwrap();
    assert_ne!(fresh.session_id, first.session_id);
    assert!(
        registry
            .lock(active.bucket_id, Some(first), now)
            .0
            .is_empty()
    );
    assert!(admit(&mut registry, active, archive(7), now).is_ok());

    // A lock closes every generation of the bucket at once.
    let locked = registry.lock(active.bucket_id, None, now).0;
    assert_eq!(locked.len(), 2);
    assert!(admit(&mut registry, active, archive(7), now).is_err());
    assert!(admit(&mut registry, source, archive(7), now).is_err());
    // The admitted read keeps its key and its archive pin until it ends.
    let guard = lease.guard().downcast_ref::<LeaseGuard>().unwrap();
    assert_eq!(guard.secret, private(1));
    assert!(registry.is_pinned(&archive(7)));
    let pin = registry.pin(archive(8)).unwrap();
    drop(lease);
    assert!(!registry.is_pinned(&archive(7)));
    assert!(registry.is_pinned(&archive(8)));
    drop(pin);
    assert!(!registry.is_pinned(&archive(8)));
}

#[test]
fn leases_hold_slots() {
    let mut registry = UnlockRegistry::with_leases(UNLOCKED_BUCKETS, 1);
    let now = Instant::now();
    let key = reference(1, 1);
    unlock(&mut registry, key, 1, (None, None), now).unwrap();
    let lease = admit(&mut registry, key, archive(1), now).unwrap();
    // The only slot stays with the lease while its stream lives, even after a lock.
    registry.lock(key.bucket_id, None, now);
    assert!(registry.lease_slots().try_acquire_owned().is_err());
    drop(lease);
    assert!(registry.lease_slots().try_acquire_owned().is_ok());
}

#[test]
fn delete_claim_excludes() {
    let mut registry = UnlockRegistry::new(UNLOCKED_BUCKETS);
    let now = Instant::now();
    let key = reference(1, 1);
    unlock(&mut registry, key, 1, (None, None), now).unwrap();
    // A pinned archive cannot be claimed for deletion.
    let lease = admit(&mut registry, key, archive(1), now).unwrap();
    assert!(registry.claim_delete(&archive(1)).is_err());
    drop(lease);
    // While the claim is held, no read or keyless work can pin the archive.
    let claim = registry.claim_delete(&archive(1)).unwrap();
    assert!(registry.claim_delete(&archive(1)).is_err());
    assert!(admit(&mut registry, key, archive(1), now).is_err());
    assert!(registry.pin(archive(1)).is_err());
    assert!(registry.pin(archive(2)).is_ok());
    drop(claim);
    assert!(admit(&mut registry, key, archive(1), now).is_ok());
}

#[test]
fn clear_keeps_leases() {
    let mut registry = UnlockRegistry::new(UNLOCKED_BUCKETS);
    let now = Instant::now();
    let key = reference(1, 1);
    unlock(&mut registry, key, 1, (None, None), now).unwrap();
    let lease = admit(&mut registry, key, archive(1), now).unwrap();
    registry.clear();
    assert!(registry.status(key.bucket_id, now).is_empty());
    assert!(admit(&mut registry, key, archive(1), now).is_err());
    let guard = lease.guard().downcast_ref::<LeaseGuard>().unwrap();
    assert_eq!(guard.secret, private(1));
}

#[tokio::test(start_paused = true)]
async fn idle_prepared_dropped() {
    let registry = Arc::new(StdMutex::new(UnlockRegistry::new(UNLOCKED_BUCKETS)));
    let now = Instant::now();
    let public = public_key_of(private(1).bytes()).unwrap();
    let prepare = |key| {
        let bounds = (None, None);
        let mut guard = registry.lock().unwrap();
        guard.prepare(key, &public, private(1), bounds, (now, SystemTime::now()))
    };
    let (idle, used) = (reference(1, 1), reference(2, 1));
    let idle_ticket = prepare(idle).unwrap();
    let used_ticket = prepare(used).unwrap();
    registry
        .lock()
        .unwrap()
        .activate(used_ticket, (now, SystemTime::now()))
        .unwrap();
    expire_prepared(Arc::downgrade(&registry), idle_ticket);
    expire_prepared(Arc::downgrade(&registry), used_ticket);
    tokio::time::sleep(super::PREPARED_TTL).await;
    tokio::task::yield_now().await;
    let mut guard = registry.lock().unwrap();
    // The prepared key is gone without any later registry call; the activated one stays.
    assert!(!guard.sessions.contains_key(&idle));
    assert!(guard.status(used.bucket_id, now)[0].active);
}

#[test]
fn polled_expiry_locks() {
    let mut registry = UnlockRegistry::new(UNLOCKED_BUCKETS);
    let start = Instant::now();
    let key = reference(1, 1);
    let ticket = unlock(&mut registry, key, 1, (Some(MINUTE), None), start).unwrap();
    // Status polling after the deadline removes the key before the timer runs.
    let later = start + 2 * MINUTE;
    assert!(registry.status(key.bucket_id, later).is_empty());
    assert!(admit(&mut registry, key, archive(1), later).is_err());
    // The timer still learns that its session ended, so the timed lock is recorded once.
    assert_eq!(
        registry.lock(key.bucket_id, Some(ticket), later).0,
        vec![ticket]
    );
    assert!(
        registry
            .lock(key.bucket_id, Some(ticket), later)
            .0
            .is_empty()
    );
    // A stale timer of another session records nothing.
    let other = KeyTicket {
        key,
        session_id: Ulid::generate(),
    };
    assert!(
        registry
            .lock(key.bucket_id, Some(other), later)
            .0
            .is_empty()
    );
}

#[test]
fn expiry_per_session() {
    let mut registry = UnlockRegistry::new(UNLOCKED_BUCKETS);
    let start = Instant::now();
    let key = reference(1, 1);
    let first = unlock(&mut registry, key, 1, (Some(MINUTE), None), start).unwrap();
    // The first session expires unseen, then a second session of the generation expires too.
    let later = start + 2 * MINUTE;
    assert!(registry.status(key.bucket_id, later).is_empty());
    let second = unlock(&mut registry, key, 1, (Some(MINUTE), None), later).unwrap();
    let last = later + 2 * MINUTE;
    assert!(registry.status(key.bucket_id, last).is_empty());
    // Both delayed timers still record their own timed lock, in any order.
    assert_eq!(
        registry.lock(key.bucket_id, Some(second), last).0,
        vec![second]
    );
    assert_eq!(
        registry.lock(key.bucket_id, Some(first), last).0,
        vec![first]
    );
    // A third session unlocked after both expiries stays open whatever the old timers do.
    let third = unlock(&mut registry, key, 1, (None, None), last).unwrap();
    assert!(registry.lock(key.bucket_id, Some(first), last).0.is_empty());
    assert_eq!(
        registry.status(key.bucket_id, last)[0].session_id,
        third.session_id
    );
}

#[test]
fn timer_spares_extension() {
    let mut registry = UnlockRegistry::new(UNLOCKED_BUCKETS);
    let start = Instant::now();
    let key = reference(1, 1);
    let bounds = (Some(MINUTE), Some(3 * MINUTE));
    let ticket = unlock(&mut registry, key, 1, bounds, start).unwrap();
    // The session is extended to 90 seconds while its old timer callback already runs.
    let extended = start + Duration::from_secs(30);
    let session = ticket.session_id;
    registry
        .extend(key, session, Some(MINUTE), (extended, SystemTime::now()))
        .unwrap();
    // The old callback reaches the registry at the old deadline and locks nothing.
    let old = start + MINUTE;
    assert!(registry.lock(key.bucket_id, Some(ticket), old).0.is_empty());
    assert!(admit(&mut registry, key, archive(1), old).is_ok());
    // The rescheduled timer locks it at the new deadline.
    let new = extended + MINUTE;
    assert_eq!(
        registry.lock(key.bucket_id, Some(ticket), new).0,
        vec![ticket]
    );
    assert!(admit(&mut registry, key, archive(1), new).is_err());
}
