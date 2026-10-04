//! The unlock registry under injected monotonic time: keys, bounds, locks and leases.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{LeaseGuard, UNLOCKED_BUCKETS, UnlockRegistry};
use aruna_core::compute::SecretBytes;
use aruna_core::structs::storage::blob::{ArchiveKey, BackendRef};
use aruna_core::structs::storage::encryption::{
    BucketKeyError, BucketKeyRef, KeyTicket, public_key_of,
};
use std::time::{Duration, Instant, SystemTime};
use ulid::Ulid;

const MINUTE: Duration = Duration::from_secs(60);

fn private(seed: u8) -> SecretBytes {
    SecretBytes::new(vec![seed; 32])
}

fn reference(bucket: u8, generation: u64) -> BucketKeyRef {
    BucketKeyRef::new(Ulid::from_bytes([bucket; 16]), generation)
}

fn archive(seed: u8) -> ArchiveKey {
    ArchiveKey::new(Ulid::from_bytes([seed; 16]), BackendRef::node_default())
}

/// Prepares and activates the key of `seed` for `key`.
fn unlock(
    registry: &mut UnlockRegistry,
    key: BucketKeyRef,
    seed: u8,
    bounds: (Option<Duration>, Option<Duration>),
    now: Instant,
) -> Result<KeyTicket, BucketKeyError> {
    let public = public_key_of(&private(seed)).unwrap();
    let ticket = registry.prepare(
        key,
        &public,
        private(seed),
        bounds,
        (now, SystemTime::now()),
    )?;
    registry.activate(ticket, now)?;
    Ok(ticket)
}

#[test]
fn admits_after_activation() {
    let mut registry = UnlockRegistry::new(UNLOCKED_BUCKETS);
    let now = Instant::now();
    let key = reference(1, 1);
    let public = public_key_of(&private(1)).unwrap();

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
    assert!(registry.admit(key, archive(1), now).is_err());
    assert!(!registry.status(key.bucket_id, now)[0].active);
    registry.activate(ticket, now).unwrap();
    let lease = registry.admit(key, archive(1), now).unwrap();
    assert_eq!(lease.session_id, ticket.session_id);
    let other = reference(1, 2);
    assert_eq!(
        registry.admit(other, archive(1), now).unwrap_err(),
        BucketKeyError::Locked(other.bucket_id)
    );

    registry.discard(ticket);
    assert!(registry.status(key.bucket_id, now).is_empty());
    assert!(registry.admit(key, archive(1), now).is_err());
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
    assert!(registry.admit(reference(1, 1), archive(1), now).is_ok());
    assert!(registry.admit(reference(1, 2), archive(1), now).is_ok());
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
        .extend(key, ticket.session_id, Some(Duration::from_secs(10)), later)
        .unwrap();
    assert_eq!(short.remaining, Some(Duration::from_secs(10)));
    assert_eq!(short.max_remaining, Some(Duration::from_secs(30)));
    let beyond = registry.extend(key, ticket.session_id, Some(MINUTE), later);
    assert_eq!(beyond, Err(BucketKeyError::InvalidDuration));
    let stranger = registry.extend(key, Ulid::generate(), None, later);
    assert_eq!(stranger, Err(BucketKeyError::SessionMismatch));

    // A delayed timer cannot keep the key admitted past its deadline.
    let expired = later + Duration::from_secs(10);
    assert!(registry.admit(key, archive(1), expired).is_err());
    assert!(registry.status(key.bucket_id, expired).is_empty());
    let locked = registry.extend(key, ticket.session_id, None, expired);
    assert_eq!(locked, Err(BucketKeyError::Locked(key.bucket_id)));
}

#[test]
fn lock_keeps_leases() {
    let mut registry = UnlockRegistry::new(UNLOCKED_BUCKETS);
    let now = Instant::now();
    let (active, source) = (reference(1, 2), reference(1, 1));
    let first = unlock(&mut registry, active, 1, (None, None), now).unwrap();
    unlock(&mut registry, source, 2, (None, None), now).unwrap();
    let lease = registry.admit(active, archive(7), now).unwrap();

    // A stale timer names an older session and locks nothing.
    let fresh = unlock(&mut registry, active, 1, (None, None), now).unwrap();
    assert_ne!(fresh.session_id, first.session_id);
    assert!(registry.lock(active.bucket_id, Some(first)).is_empty());
    assert!(registry.admit(active, archive(7), now).is_ok());

    // A lock closes every generation of the bucket at once.
    let locked = registry.lock(active.bucket_id, None);
    assert_eq!(locked.len(), 2);
    assert!(registry.admit(active, archive(7), now).is_err());
    assert!(registry.admit(source, archive(7), now).is_err());
    // The admitted read keeps its key and its archive pin until it ends.
    let guard = lease.guard().downcast_ref::<LeaseGuard>().unwrap();
    assert_eq!(guard._secret.expose(), private(1).expose());
    assert!(registry.is_pinned(&archive(7)));
    let pin = registry.pin(archive(8));
    drop(lease);
    assert!(!registry.is_pinned(&archive(7)));
    assert!(registry.is_pinned(&archive(8)));
    drop(pin);
    assert!(!registry.is_pinned(&archive(8)));
}
