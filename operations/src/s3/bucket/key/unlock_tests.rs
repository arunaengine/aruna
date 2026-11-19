//! Unlock: holder authority, the synced audit intent before activation and a discarded failed key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::bucket::key::extend::{ExtendBucketOperation, ExtendInput};
use crate::s3::bucket::key::test_rows::authority_rows;
use aruna_core::calls::ChildOutcome;
use aruna_core::compute::SecretBytes;
use aruna_core::handle::Handle;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{BucketEncryption, EncryptionMode, public_key_of};
use aruna_core::structs::storage::format::Compression;
use aruna_core::task::TaskEvent;
use std::time::SystemTime;

const BUCKET_ID: Ulid = Ulid::from_bytes([4; 16]);

fn user(seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
}

fn private() -> SharedSecret {
    SharedSecret::new(SecretBytes::new(vec![5; 32]))
}

fn input(caller: UserId) -> UnlockInput {
    UnlockInput {
        bucket: "bucket".to_string(),
        group_id: Ulid::from_bytes([3; 16]),
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        caller,
        key: BucketKeyRef::new(BUCKET_ID, 2),
        duration: Some(Duration::from_secs(60)),
        realm_id: RealmId::from_bytes([1; 32]),
        now_ms: 1_000,
    }
}

fn info() -> BucketInfo {
    BucketInfo {
        group_id: Ulid::from_bytes([3; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: user(1),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    }
}

fn event(event: Event) -> IoResult {
    IoResult::Event(Box::new(event))
}

fn blob(blob: BlobEvent) -> IoResult {
    event(Event::Blob(blob))
}

fn synced() -> IoResult {
    event(Event::Storage(StorageEvent::SyncAllFinished))
}

fn kept(stored: bool) -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<_, AuditError>(stored)))
}

fn asked(turn: &UnlockTurn) -> &Effect {
    match turn {
        Ok(Next::Call(IoCall::Effect(effect))) => effect,
        other => panic!("expected an effect call, got {other:?}"),
    }
}

/// The intent child of an unlock by `caller` with `admins`, up to the read of the sealed copy.
fn checked(caller: UserId, admins: &[UserId]) -> (UnlockIntent, IntentTurn) {
    let key = BucketKeyRef::new(BUCKET_ID, 2);
    let mut operation = UnlockIntent {
        input: input(caller),
        step: IntentStep::ReadBucket,
        implicit: false,
        max: None,
        private_key: Some(private()),
        record: None,
        ticket: None,
        intent: None,
    };
    assert_eq!(operation.transaction(), TransactionMode::Write);
    operation.first().unwrap();
    let settings = BucketEncryption {
        mode: EncryptionMode::VaultLocked,
        bucket_id: Some(BUCKET_ID),
        key_generation: 2,
        max_unlock_ms: Some(3_600_000),
        ..Default::default()
    };
    let rows = authority_rows(&info(), Some(&settings), admins);
    let values = rows.into_iter().map(|(_, value)| value).collect();
    operation.next(IoResult::Values(values)).unwrap();
    let public = public_key_of(private().bytes()).unwrap();
    let record = BucketKeyRecord::new(key, Ulid::from_bytes([6; 16]), public, 1);
    let answer = IoResult::Values(vec![Some(record.to_bytes().unwrap().into()), None]);
    let turn = operation.next(answer);
    (operation, turn)
}

/// The intent child up to its prepare step.
fn prepared(caller: UserId, admins: &[UserId]) -> (UnlockIntent, IntentTurn) {
    let (mut operation, turn) = checked(caller, admins);
    if !matches!(turn, Ok(Next::Call(IoCall::Page { .. }))) {
        return (operation, turn);
    }
    let copy = (Key::from(Vec::new()), Value::from(vec![1]));
    let turn = operation.next(IoResult::Page {
        rows: vec![copy],
        next: None,
    });
    (operation, turn)
}

fn ticket() -> KeyTicket {
    KeyTicket {
        key: BucketKeyRef::new(BUCKET_ID, 2),
        session_id: Ulid::from_bytes([8; 16]),
    }
}

fn intent_record() -> BucketAuditRecord {
    let (mut intent, _) = prepared(user(1), &[]);
    intent
        .next(blob(BlobEvent::KeyPrepared { ticket: ticket() }))
        .unwrap();
    let Ok(Next::Done(prepared)) = intent.next(IoResult::Done) else {
        panic!("the intent is written");
    };
    prepared.intent
}

/// An unlock whose intent committed and synced, waiting for the activation.
fn activating() -> UnlockBucketOperation {
    let mut operation = UnlockBucketOperation::new(input(user(1)), private());
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let prepared = PreparedUnlock {
        ticket: ticket(),
        intent: intent_record(),
        max: Some(Duration::from_secs(3_600)),
    };
    let committed = Ok::<_, UnlockError>(prepared);
    let turn = operation.next(IoResult::Child(ChildOutcome::new(committed)));
    // Reads see the key only after the intent committed and reached the disk.
    assert_eq!(asked(&turn), &Effect::Storage(StorageEffect::SyncAll));
    let turn = operation.next(synced());
    assert_eq!(
        asked(&turn),
        &Effect::Blob(BlobEffect::ActivateKey { ticket: ticket() })
    );
    operation
}

fn status(deadline_ms: u64, seconds: u64) -> UnlockStatus {
    UnlockStatus {
        key: ticket().key,
        session_id: ticket().session_id,
        sequence: Ulid::from_parts(1, 1),
        deadline_ms: Some(deadline_ms),
        active: true,
        unlocked_at: SystemTime::UNIX_EPOCH + Duration::from_secs(5),
        remaining: Some(Duration::from_secs(seconds)),
        max_remaining: Some(Duration::from_secs(3_600)),
    }
}

#[test]
fn intent_before_activation() {
    let (mut intent, turn) = prepared(user(1), &[]);
    assert!(matches!(
        &turn,
        Ok(Next::Call(IoCall::Effect(effect)))
            if matches!(effect.as_ref(), Effect::Blob(BlobEffect::PrepareKey { .. }))
    ));
    let turn = intent.next(blob(BlobEvent::KeyPrepared { ticket: ticket() }));
    let Ok(Next::Call(IoCall::Write(rows))) = &turn else {
        panic!("expected the intent, got {turn:?}");
    };
    assert_eq!(rows[0].0, BUCKET_AUDIT_KEYSPACE);
    let record = BucketAuditRecord::from_bytes(&rows[0].2).unwrap();
    assert_eq!(
        (record.outcome, record.deadline_ms),
        (AuditOutcome::Intent, Some(61_000))
    );

    let mut operation = activating();
    let active = status(65_000, 60);
    let turn = operation.next(blob(BlobEvent::KeyActivated {
        status: active.clone(),
    }));
    // The timed lock of this session is armed for the remaining time.
    let key = lock_timer(&ticket());
    let after = Duration::from_secs(60);
    let arm = TaskEffect::ShortenTimer {
        key: key.clone(),
        after,
    };
    assert_eq!(asked(&turn), &Effect::Task(arm));
    let armed = TaskEvent::TimerScheduled { key, after };
    operation.next(event(Event::Task(armed))).unwrap();
    // The applied record states the deadline of the session as activated.
    let applied = operation.outcome.clone().unwrap();
    assert_eq!(
        (applied.outcome, applied.deadline_ms),
        (AuditOutcome::Applied, Some(65_000))
    );
    assert_eq!(operation.next(kept(true)), Ok(Next::Done(active)));
}

struct TimerSender(tokio::sync::mpsc::Sender<aruna_core::task::TaskKey>);

#[async_trait::async_trait]
impl aruna_tasks::InboundTaskHandler for TimerSender {
    async fn handle_timer(&self, key: aruna_core::task::TaskKey) {
        self.0.send(key).await.unwrap();
    }
}

/// Runs the effect a turn asked for on the scheduler and returns its answer.
async fn on_scheduler<T: std::fmt::Debug, E: std::fmt::Debug>(
    scheduler: &aruna_tasks::TaskHandle,
    turn: Result<Next<T>, E>,
) -> IoResult {
    let Ok(Next::Call(IoCall::Effect(effect))) = turn else {
        panic!("expected a timer effect, got {turn:?}");
    };
    IoResult::Event(Box::new(scheduler.send_effect(*effect).await))
}

#[tokio::test(start_paused = true)]
async fn reversed_unlock_shorten() {
    let mut unlock = activating();
    let active = UnlockStatus {
        deadline_ms: Some(91_000),
        unlocked_at: SystemTime::UNIX_EPOCH + Duration::from_secs(1),
        ..status(91_000, 90)
    };
    let older = unlock.next(blob(BlobEvent::KeyActivated {
        status: active.clone(),
    }));
    let mut extension = ExtendBucketOperation::new(ExtendInput {
        bucket: "bucket".to_string(),
        group_id: Ulid::from_bytes([3; 16]),
        realm_id: RealmId::from_bytes([1; 32]),
        node_id: unlock.input.node_id,
        caller: user(1),
        generation: ticket().key.generation,
        session_id: ticket().session_id,
        duration: Some(Duration::from_secs(30)),
        now_ms: 1_000,
    });
    extension.first().unwrap();
    let settings = BucketEncryption {
        mode: EncryptionMode::VaultLocked,
        bucket_id: Some(BUCKET_ID),
        key_generation: ticket().key.generation,
        ..Default::default()
    };
    let rows = authority_rows(&info(), Some(&settings), &[]);
    let values = rows.into_iter().map(|(_, value)| value).collect();
    extension.next(IoResult::Values(values)).unwrap();
    extension
        .next(blob(BlobEvent::KeyStatus {
            generations: vec![active.clone()],
        }))
        .unwrap();
    let written = Ok::<(), StorageError>(());
    extension
        .next(IoResult::Child(ChildOutcome::new(written)))
        .unwrap();
    let newer = extension.next(blob(BlobEvent::KeyExtended {
        status: UnlockStatus {
            sequence: Ulid::from_parts(1, 2),
            deadline_ms: Some(31_000),
            remaining: Some(Duration::from_secs(30)),
            ..active
        },
    }));
    let scheduler = aruna_tasks::TaskHandle::new();
    let (sender, mut fired) = tokio::sync::mpsc::channel(2);
    scheduler
        .set_inbound_handler(std::sync::Arc::new(TimerSender(sender)))
        .await;
    let start = tokio::time::Instant::now();
    let answer = on_scheduler(&scheduler, newer).await;
    extension.next(answer).unwrap();
    let answer = on_scheduler(&scheduler, older).await;
    unlock.next(answer).unwrap();
    assert_eq!(fired.recv().await.unwrap(), lock_timer(&ticket()));
    assert_eq!(tokio::time::Instant::now() - start, Duration::from_secs(30));
    scheduler.shutdown(Duration::ZERO).await;
}

#[test]
fn delayed_unlock_unresolved() {
    let intent = intent_record();
    assert_eq!(intent.deadline_ms, Some(61_000));
    let mut operation = activating();
    operation.intent = Some(intent.clone());
    let active = UnlockStatus {
        sequence: Ulid::from_parts(31_000, 1),
        unlocked_at: SystemTime::UNIX_EPOCH + Duration::from_secs(31),
        ..status(91_000, 60)
    };
    operation
        .next(blob(BlobEvent::KeyActivated { status: active }))
        .unwrap();
    let armed = TaskEvent::TimerScheduled {
        key: lock_timer(&ticket()),
        after: Duration::from_secs(60),
    };
    operation.next(event(Event::Task(armed))).unwrap();
    let outcome = operation.outcome.clone().unwrap();
    assert_eq!(outcome.deadline_ms, Some(91_000));
    assert_eq!(
        crate::s3::bucket::key::restart::replay(std::slice::from_ref(&intent), 76_000),
        [2]
    );
    assert_eq!(
        crate::s3::bucket::key::restart::replay(&[intent, outcome], 76_000),
        [2]
    );
}

#[test]
fn failed_activation_discards() {
    let mut operation = activating();
    let refused = BlobError::BucketKey(BucketKeyError::Capacity);
    let turn = operation.next(blob(BlobEvent::Error(refused)));
    assert_eq!(
        asked(&turn),
        &Effect::Blob(BlobEffect::DiscardKey { ticket: ticket() })
    );
    operation
        .next(blob(BlobEvent::KeyDiscarded { ticket: ticket() }))
        .unwrap();
    let outcome = operation.outcome.clone().unwrap();
    assert_eq!(outcome.outcome, AuditOutcome::Failed);
    assert!(matches!(
        operation.next(kept(true)),
        Err(UnlockError::Blob(_))
    ));
}

#[test]
fn former_admin_refused() {
    // Admin rights decide implicit authority at the moment of the unlock (D30).
    let (_, turn) = prepared(user(2), &[]);
    assert!(matches!(turn, Err(UnlockError::NotHolder)));
    let (_, turn) = prepared(user(2), &[user(2)]);
    assert!(matches!(turn, Ok(Next::Call(IoCall::Effect(_)))));
}

#[test]
fn foreign_status_refused() {
    // An activation answer for another key generation is not this session's.
    let mut operation = activating();
    let other = UnlockStatus {
        key: BucketKeyRef::new(BUCKET_ID, 1),
        ..status(1, 1)
    };
    let turn = operation.next(blob(BlobEvent::KeyActivated { status: other }));
    assert!(matches!(turn, Err(UnlockError::InvalidStateEvent { .. })));
    let discard: IoCall = Effect::Blob(BlobEffect::DiscardKey { ticket: ticket() }).into();
    assert_eq!(operation.abort(), vec![discard]);
}

#[test]
fn uncommitted_intent_discards() {
    // A prepared key whose intent may be lost never activates.
    let (mut intent, _) = prepared(user(1), &[]);
    intent
        .next(blob(BlobEvent::KeyPrepared { ticket: ticket() }))
        .unwrap();
    let discard: IoCall = Effect::Blob(BlobEffect::DiscardKey { ticket: ticket() }).into();
    assert_eq!(intent.abort(), vec![discard]);
}

#[test]
fn unsynced_intent_discards() {
    let mut operation = UnlockBucketOperation::new(input(user(1)), private());
    operation.first().unwrap();
    let prepared = PreparedUnlock {
        ticket: ticket(),
        intent: intent_record(),
        max: None,
    };
    let committed = Ok::<_, UnlockError>(prepared);
    operation
        .next(IoResult::Child(ChildOutcome::new(committed)))
        .unwrap();
    // A failed sync ends the run, whose cleanup discards the key.
    let discard: IoCall = Effect::Blob(BlobEffect::DiscardKey { ticket: ticket() }).into();
    assert_eq!(operation.abort(), vec![discard]);
}

#[test]
fn copyless_holder_refused() {
    let (mut operation, turn) = checked(user(1), &[]);
    let Ok(Next::Call(IoCall::Page { space, prefix, .. })) = &turn else {
        panic!("expected the copy read, got {turn:?}");
    };
    assert_eq!(space, KEY_COPY_KEYSPACE);
    let key = BucketKeyRef::new(BUCKET_ID, 2);
    assert_eq!(prefix.as_ref(), &SealedCopy::user_prefix(key, user(1))[..]);
    let empty = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    assert!(matches!(operation.next(empty), Err(UnlockError::NoCopy)));
}
