//! The restart scan rebuilds each session from audit records replayed in storage order, with
//! outcomes paired to their intents; records share one millisecond to rule out time order.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::bucket::key_rows::authority_rows;
use aruna_core::UserId;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{
    BucketKeyRef, EncryptionMode, GrantState, HolderOrigin,
};
use aruna_core::structs::storage::format::Compression;
use std::time::SystemTime;

const LOCKED: Ulid = Ulid::from_bytes([1; 16]);
const GROUP: Ulid = Ulid::from_bytes([3; 16]);
const NOW: u64 = 10_000;
/// Every record of a trail is written within this one millisecond.
const AT: u64 = 5_000;

fn user(seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
}

fn session(seed: u8) -> Option<Ulid> {
    Some(Ulid::from_bytes([seed; 16]))
}

fn page(values: Vec<(Vec<u8>, Vec<u8>)>, next: Option<Vec<u8>>) -> Event {
    Event::Storage(StorageEvent::IterResult {
        values: values
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect(),
        next_start_after: next.map(Into::into),
    })
}

fn rows(values: Vec<(Vec<u8>, Vec<u8>)>) -> Event {
    page(values, None)
}

fn settings(mode: EncryptionMode, bucket_id: Ulid) -> Vec<u8> {
    let settings = BucketEncryption {
        mode,
        bucket_id: Some(bucket_id),
        key_generation: 2,
        ..Default::default()
    };
    settings.to_bytes().unwrap()
}

/// The key record of `generation`; a node-managed key has a node vault copy.
fn key_row(bucket_id: Ulid, generation: u64, managed: bool) -> (Vec<u8>, Vec<u8>) {
    let key = BucketKeyRef::new(bucket_id, generation);
    let mut record = BucketKeyRecord::new(key, Ulid::from_bytes([6; 16]), [5; 32], 1);
    record.vault_entry = managed.then_some(Ulid::from_bytes([8; 16]));
    (record.key.key(), record.to_bytes().unwrap())
}

/// An audit trail written within one millisecond, with ids that grow in issue order as
/// `next_event_id` issues them.
#[derive(Default)]
struct Log {
    records: Vec<BucketAuditRecord>,
}

impl Log {
    fn add(
        &mut self,
        action: AuditAction,
        (generation, session_id): (u64, Option<Ulid>),
        (outcome, intent_id): (AuditOutcome, Option<Ulid>),
        deadline_ms: Option<u64>,
    ) -> Ulid {
        let record = BucketAuditRecord {
            event_id: Ulid::from_parts(AT, (1 << 64) + self.records.len() as u128),
            bucket_id: LOCKED,
            at_ms: AT,
            action,
            actor: None,
            node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
            generation: Some(generation),
            session_id,
            intent_id,
            sequence: None,
            deadline_ms,
            reason: None,
            outcome,
        };
        let event_id = record.event_id;
        self.records.push(record);
        event_id
    }

    fn intent(
        &mut self,
        action: AuditAction,
        at: (u64, Option<Ulid>),
        deadline: Option<u64>,
    ) -> Ulid {
        self.add(action, at, (AuditOutcome::Intent, None), deadline)
    }

    fn outcome(
        &mut self,
        action: AuditAction,
        at: (u64, Option<Ulid>),
        (outcome, intent): (AuditOutcome, Ulid),
        deadline: Option<u64>,
    ) {
        self.add(action, at, (outcome, Some(intent)), deadline);
    }

    fn applied(&mut self, action: AuditAction, at: (u64, Option<Ulid>), deadline: Option<u64>) {
        self.add(action, at, (AuditOutcome::Applied, None), deadline);
    }

    /// The trail as storage returns it: sorted by key, all in one millisecond.
    fn stored(&self) -> Vec<BucketAuditRecord> {
        let mut records = self.records.clone();
        records.sort_by_key(BucketAuditRecord::key);
        assert!(
            records
                .iter()
                .all(|record| record.event_id.timestamp_ms() == AT)
        );
        records
    }

    fn open(&self) -> Vec<u64> {
        replay(&self.stored(), NOW)
    }
}

/// Starts a scan of the bucket settings `buckets` and answers the settings page.
fn scanned(buckets: Vec<(Vec<u8>, Vec<u8>)>) -> (RestartScanOperation, Effects) {
    let mut operation = RestartScanOperation::new(NOW, RealmId::from_bytes([1; 32]));
    operation.start();
    let effects = operation.step(rows(buckets));
    (operation, effects)
}

fn scans(effects: &Effects, key_space: &str) -> bool {
    matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Iter { key_space: scanned, .. })] if scanned == key_space
    )
}

fn bucket_info() -> BucketInfo {
    BucketInfo {
        group_id: GROUP,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: user(1),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
    }
}

fn copy(user_id: UserId, generation: u64) -> (Vec<u8>, Vec<u8>) {
    let copy = SealedCopy {
        key: BucketKeyRef::new(LOCKED, generation),
        user_id,
        key_record: Ulid::from_bytes([9; 16]),
        key_id: "slot".to_string(),
        enc: [0; 32],
        ciphertext: vec![0; 48],
        created_at_ms: 1,
    };
    (copy.key(), copy.to_bytes().unwrap())
}

#[test]
fn finds_unlocked_generations() {
    let locked = (
        b"locked".to_vec(),
        settings(EncryptionMode::VaultLocked, LOCKED),
    );
    let (mut operation, effects) = scanned(vec![locked]);
    assert!(scans(&effects, BUCKET_KEY_KEYSPACE));
    let keys = vec![key_row(LOCKED, 1, false), key_row(LOCKED, 2, false)];
    let effects = operation.step(rows(keys));
    assert!(scans(&effects, BUCKET_AUDIT_KEYSPACE));
    // Generation 2 was locked after its unlock; generation 1 is still unlocked.
    let mut log = Log::default();
    log.applied(AuditAction::Unlock, (1, session(1)), None);
    log.applied(AuditAction::Unlock, (2, session(2)), None);
    log.applied(AuditAction::Lock, (2, session(2)), None);
    let stored: Vec<_> = (log.stored().iter())
        .map(|record| (record.key(), record.to_bytes().unwrap()))
        .collect();
    // The trail arrives in two storage pages.
    let (first, second) = stored.split_at(2);
    let next = first.last().map(|(key, _)| key.clone());
    let effects = operation.step(page(first.to_vec(), next));
    assert!(scans(&effects, BUCKET_AUDIT_KEYSPACE));
    let info = bucket_info();
    operation.step(rows(second.to_vec()));
    operation.step(Event::Storage(StorageEvent::ReadResult {
        key: Key::from(b"locked".to_vec()),
        value: Some(info.to_bytes().unwrap().into()),
    }));
    // Admin rights come from the current authorization documents: user(4) lost its role.
    let settings = BucketEncryption::from_bytes(&settings(EncryptionMode::VaultLocked, LOCKED));
    let values = authority_rows(&info, Some(&settings.unwrap()), &[user(5)]);
    operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
    let grant = BucketHolder {
        bucket_id: LOCKED,
        user_id: user(3),
        origin: HolderOrigin::Explicit,
        state: GrantState::Ready,
        granted_by: user(1),
        granted_at_ms: 1,
    };
    operation.step(rows(vec![(grant.key(), grant.to_bytes().unwrap())]));
    // A former admin keeps an old copy but is no holder any more and is not told.
    operation.step(rows(vec![copy(user(2), 1), copy(user(4), 1)]));
    let found = operation.finalize().unwrap();
    assert_eq!(
        found,
        [RestartedBucket {
            bucket: "locked".to_string(),
            bucket_id: LOCKED,
            group_id: GROUP,
            generations: vec![1],
            holders: vec![user(1), user(3), user(5)],
        }]
    );
}

#[test]
fn decrypting_bucket_included() {
    // A bucket in mode off still holds its source key while a decryption runs; a node-managed
    // bucket has only node vault keys, which startup opens itself.
    let managed_id = Ulid::from_bytes([2; 16]);
    let (mut operation, effects) = scanned(vec![
        (b"locked".to_vec(), settings(EncryptionMode::Off, LOCKED)),
        (
            b"managed".to_vec(),
            settings(EncryptionMode::NodeManaged, managed_id),
        ),
    ]);
    // The last listed bucket is scanned first: its only key has a node copy.
    assert!(scans(&effects, BUCKET_KEY_KEYSPACE));
    let effects = operation.step(rows(vec![key_row(managed_id, 1, true)]));
    assert!(scans(&effects, BUCKET_KEY_KEYSPACE));
    let effects = operation.step(rows(vec![key_row(LOCKED, 1, false)]));
    assert!(scans(&effects, BUCKET_AUDIT_KEYSPACE));
    let mut log = Log::default();
    log.applied(AuditAction::Unlock, (1, session(1)), None);
    let stored = (log.stored().iter())
        .map(|record| (record.key(), record.to_bytes().unwrap()))
        .collect();
    let effects = operation.step(rows(stored));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, .. })] if key_space == S3_BUCKET_KEYSPACE
    ));
}

#[test]
fn lost_outcome_counts() {
    // A crash after activation left only the synced intent: the key may have been in use.
    let mut log = Log::default();
    log.intent(AuditAction::Unlock, (1, session(1)), None);
    assert_eq!(log.open(), [1]);
    // A failed activation restores the state before its intent.
    let mut log = Log::default();
    let intent = log.intent(AuditAction::Unlock, (1, session(1)), None);
    let failed = (AuditOutcome::Failed, intent);
    log.outcome(AuditAction::Unlock, (1, session(1)), failed, None);
    assert!(log.open().is_empty());
}

#[test]
fn expired_sessions_skipped() {
    // A timed session that ended before this start was not unlocked at the restart.
    let mut log = Log::default();
    log.applied(AuditAction::Unlock, (1, session(1)), Some(NOW - 1));
    assert!(log.open().is_empty());
    // An extension of that session moves the deadline that counts.
    let intent = log.intent(AuditAction::Extend, (1, session(1)), Some(NOW + 1));
    let applied = (AuditOutcome::Applied, intent);
    log.outcome(AuditAction::Extend, (1, session(1)), applied, Some(NOW + 1));
    assert_eq!(log.open(), [1]);
}

#[test]
fn rejected_extension_restored() {
    // A refused extension without a duration restores the ended deadline: no false notice.
    let mut log = Log::default();
    log.applied(AuditAction::Unlock, (1, session(1)), Some(NOW - 1));
    let intent = log.intent(AuditAction::Extend, (1, session(1)), None);
    let failed = (AuditOutcome::Failed, intent);
    log.outcome(AuditAction::Extend, (1, session(1)), failed, None);
    assert!(log.open().is_empty());
    // An extension of another session never moves this session's deadline.
    let mut log = Log::default();
    log.applied(AuditAction::Unlock, (1, session(1)), Some(NOW - 1));
    log.intent(AuditAction::Extend, (1, session(2)), None);
    assert!(log.open().is_empty());
    // An extension whose outcome was lost still counts, as it may have applied.
    let mut log = Log::default();
    log.applied(AuditAction::Unlock, (1, session(1)), Some(NOW - 1));
    log.intent(AuditAction::Extend, (1, session(1)), None);
    assert_eq!(log.open(), [1]);
}

#[test]
fn failure_restores_own_intent() {
    // A delayed failed extension of session A never rolls back B's later unlock intent, even
    // when B's outcome was lost.
    let mut log = Log::default();
    log.applied(AuditAction::Unlock, (1, session(1)), Some(NOW - 1));
    let extend = log.intent(AuditAction::Extend, (1, session(1)), None);
    log.intent(AuditAction::Unlock, (1, session(2)), None);
    let failed = (AuditOutcome::Failed, extend);
    log.outcome(AuditAction::Extend, (1, session(1)), failed, None);
    assert_eq!(log.open(), [1]);
    // A failed unlock of A after B's intent leaves B; B failing too restores the state before A.
    let mut log = Log::default();
    let first = log.intent(AuditAction::Unlock, (1, session(1)), None);
    let second = log.intent(AuditAction::Unlock, (1, session(2)), None);
    let failed = (AuditOutcome::Failed, first);
    log.outcome(AuditAction::Unlock, (1, session(1)), failed, None);
    assert_eq!(log.open(), [1]);
    let failed = (AuditOutcome::Failed, second);
    log.outcome(AuditAction::Unlock, (1, session(2)), failed, None);
    assert!(log.open().is_empty());
}

#[test]
fn delayed_timer_keeps_newer() {
    // Session A's delayed timer records its lock after session B unlocked: B stays unlocked.
    let mut log = Log::default();
    log.applied(AuditAction::Unlock, (1, session(1)), None);
    log.applied(AuditAction::Unlock, (1, session(2)), None);
    log.applied(AuditAction::TimedLock, (1, session(1)), None);
    assert_eq!(log.open(), [1]);
    // B's own timed lock ends it.
    log.applied(AuditAction::TimedLock, (1, session(2)), None);
    assert!(log.open().is_empty());
    // A restart lock of the generation ends every session.
    let mut log = Log::default();
    log.applied(AuditAction::Unlock, (1, session(1)), None);
    log.applied(AuditAction::RestartLock, (1, None), None);
    assert!(log.open().is_empty());
}

#[test]
fn registry_order_replayed() {
    for (reversed, failed) in [(false, false), (true, false), (false, true), (true, true)] {
        let mut log = Log::default();
        log.applied(AuditAction::Unlock, (1, session(1)), None);
        let first = log.intent(AuditAction::Extend, (1, session(1)), Some(90_000));
        let second = log.intent(AuditAction::Extend, (1, session(1)), Some(30_000));
        let mut outcomes = [(second, 30_000, 1), (first, 90_000, 2)];
        if reversed {
            outcomes.reverse();
        }
        for (intent, deadline, sequence) in outcomes {
            let outcome = if failed && intent == second {
                AuditOutcome::Failed
            } else {
                AuditOutcome::Applied
            };
            log.outcome(
                AuditAction::Extend,
                (1, session(1)),
                (outcome, intent),
                (outcome == AuditOutcome::Applied).then_some(deadline),
            );
            log.records.last_mut().unwrap().sequence =
                (outcome == AuditOutcome::Applied).then_some(Ulid::from_parts(AT, sequence));
        }
        assert_eq!(replay(&log.stored(), 45_000), [1]);
        assert!(replay(&log.stored(), 90_000).is_empty());
    }
}

#[test]
fn overlapping_extensions_kept() {
    // E1 and E2 both extend session 1; E2 applies, then E1 fails: E2's deadline stays.
    let mut log = Log::default();
    log.applied(AuditAction::Unlock, (1, session(1)), Some(NOW - 1));
    let first = log.intent(AuditAction::Extend, (1, session(1)), Some(NOW - 1));
    let second = log.intent(AuditAction::Extend, (1, session(1)), Some(NOW + 100));
    let applied = (AuditOutcome::Applied, second);
    log.outcome(
        AuditAction::Extend,
        (1, session(1)),
        applied,
        Some(NOW + 100),
    );
    let failed = (AuditOutcome::Failed, first);
    log.outcome(AuditAction::Extend, (1, session(1)), failed, None);
    assert_eq!(log.open(), [1]);
    // E1 fails first, then E2 fails: the deadline from before both intents comes back.
    let mut log = Log::default();
    log.applied(AuditAction::Unlock, (1, session(1)), Some(NOW - 1));
    let first = log.intent(AuditAction::Extend, (1, session(1)), Some(NOW + 50));
    let second = log.intent(AuditAction::Extend, (1, session(1)), Some(NOW + 100));
    let failed = (AuditOutcome::Failed, first);
    log.outcome(AuditAction::Extend, (1, session(1)), failed, None);
    assert_eq!(log.open(), [1], "E2 still holds its deadline");
    let failed = (AuditOutcome::Failed, second);
    log.outcome(AuditAction::Extend, (1, session(1)), failed, None);
    assert!(log.open().is_empty());
    // E2 fails, then E1 applies: E1's own deadline counts.
    let mut log = Log::default();
    log.applied(AuditAction::Unlock, (1, session(1)), Some(NOW - 1));
    let first = log.intent(AuditAction::Extend, (1, session(1)), Some(NOW + 50));
    let second = log.intent(AuditAction::Extend, (1, session(1)), Some(NOW - 1));
    let failed = (AuditOutcome::Failed, second);
    log.outcome(AuditAction::Extend, (1, session(1)), failed, None);
    let applied = (AuditOutcome::Applied, first);
    log.outcome(
        AuditAction::Extend,
        (1, session(1)),
        applied,
        Some(NOW + 50),
    );
    assert_eq!(log.open(), [1]);
}
