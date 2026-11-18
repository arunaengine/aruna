//! Installs a checked bucket key in this node's unlock registry: prepare, then activate. A key
//! that cannot be activated is discarded, so none stays prepared. A timed key arms its lock timer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::audit_retry::{AuditError, AuditWrite, KeepAudit};
use crate::s3::bucket::key::lock::{answers_timer, lock_timer};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::SharedSecret;
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::encryption::{
    BucketKeyRef, KeyTicket, UnlockStatus, deadline_after,
};
use aruna_core::structs::storage::key_audit::{
    AuditAction, AuditOutcome, BucketAuditRecord, next_event_id,
};
use aruna_core::task::TaskEffect;
use aruna_core::{NodeId, UserId};
use std::time::Duration;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InstallState {
    PrepareKey,
    WriteIntent,
    ActivateKey,
    ArmTimer,
    DiscardKey,
    WriteOutcome,
}

#[derive(Debug, Error, PartialEq)]
pub enum InstallError {
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event in state {state}: expected {expected}, got {received}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: &'static str,
    },
    #[error("installing the key did not finish")]
    NotFinished,
}

impl From<IoError> for InstallError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

impl From<AuditError> for InstallError {
    fn from(error: AuditError) -> Self {
        match error {
            AuditError::Conversion(error) => Self::Conversion(error),
            AuditError::Storage(error) => Self::Storage(error),
            AuditError::NotAuditTimer => Self::NotFinished,
            AuditError::Unexpected(received) => Self::InvalidStateEvent {
                state: "WriteOutcome".to_string(),
                expected: "the audit retry timer's answer",
                received,
            },
        }
    }
}

type InstallTurn = Result<Next<UnlockStatus>, InstallError>;

/// The key and its unlock bounds: without a duration it lasts until `max`, or until lock or
/// restart when there is no maximum.
#[derive(Debug, PartialEq)]
pub struct InstallInput {
    pub key: BucketKeyRef,
    pub public_key: [u8; 32],
    pub private_key: SharedSecret,
    pub duration: Option<Duration>,
    pub max: Option<Duration>,
}

/// Who installs the key and when, for the synced intent and outcome records of the activation.
#[derive(Clone, Copy, Debug, PartialEq)]
struct InstallAudit {
    node_id: NodeId,
    actor: Option<UserId>,
    now_ms: u64,
}

#[derive(Debug, PartialEq)]
pub struct InstallKeyOperation {
    key: BucketKeyRef,
    input: Option<InstallInput>,
    bounds: (Option<Duration>, Option<Duration>),
    audit: Option<InstallAudit>,
    state: InstallState,
    ticket: Option<KeyTicket>,
    /// The session the prepared key opens, named in the audit records of the activation.
    session: Option<Ulid>,
    discarded: Option<KeyTicket>,
    failure: Option<InstallError>,
    activated: Option<UnlockStatus>,
    /// What the run ends with once its outcome record is settled.
    finished: Option<Result<UnlockStatus, InstallError>>,
    /// True once the intent is durable, so a failed activation records its outcome.
    intent_synced: bool,
    /// The event id of the stored intent, which the outcome names.
    intent_id: Option<Ulid>,
    outcome: Option<BucketAuditRecord>,
}

impl InstallKeyOperation {
    pub fn new(input: InstallInput) -> Self {
        Self {
            key: input.key,
            bounds: (input.duration, input.max),
            input: Some(input),
            audit: None,
            state: InstallState::PrepareKey,
            ticket: None,
            session: None,
            discarded: None,
            failure: None,
            activated: None,
            finished: None,
            intent_synced: false,
            intent_id: None,
            outcome: None,
        }
    }

    /// Records the activation: a synced intent before reads see the key, then its outcome.
    pub fn audited(mut self, node_id: NodeId, actor: Option<UserId>, now_ms: u64) -> Self {
        self.audit = Some(InstallAudit {
            node_id,
            actor,
            now_ms,
        });
        self
    }

    fn call(&mut self, state: InstallState, call: IoCall) -> InstallTurn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn refused(&self, received: Event) -> InstallTurn {
        match received {
            Event::Blob(BlobEvent::Error(error)) => Err(error.into()),
            other => Err(InstallError::InvalidStateEvent {
                state: format!("{:?}", self.state),
                expected: "the event of the current step",
                received: other.label(),
            }),
        }
    }

    /// Discards the prepared key before the failure ends the run.
    fn discard(&mut self, error: impl Into<InstallError>) -> InstallTurn {
        let Some(ticket) = self.ticket.take() else {
            return Err(error.into());
        };
        self.failure = Some(error.into());
        self.discarded = Some(ticket);
        let discard = Effect::Blob(BlobEffect::DiscardKey { ticket });
        self.call(InstallState::DiscardKey, discard.into())
    }

    fn record(&self, audit: InstallAudit, outcome: AuditOutcome) -> BucketAuditRecord {
        let (duration, max) = self.bounds;
        BucketAuditRecord {
            event_id: next_event_id(audit.now_ms),
            bucket_id: self.key.bucket_id,
            at_ms: audit.now_ms,
            action: AuditAction::Unlock,
            actor: audit.actor,
            node_id: audit.node_id,
            generation: Some(self.key.generation),
            session_id: self.session,
            intent_id: None,
            sequence: None,
            deadline_ms: duration
                .or(max)
                .and_then(|left| deadline_after(audit.now_ms, left)),
            reason: Some("initial activation".to_string()),
            outcome,
        }
    }

    fn activate(&mut self) -> InstallTurn {
        let ticket = self.ticket.ok_or(InstallError::NotFinished)?;
        let activate = Effect::Blob(BlobEffect::ActivateKey { ticket });
        self.call(InstallState::ActivateKey, activate.into())
    }

    fn prepared(&mut self, ticket: KeyTicket) -> InstallTurn {
        self.ticket = Some(ticket);
        self.session = Some(ticket.session_id);
        let Some(audit) = self.audit else {
            return self.activate();
        };
        let intent = self.record(audit, AuditOutcome::Intent);
        self.intent_id = Some(intent.event_id);
        // The intent must survive a crash before any read can use the key.
        match AuditWrite::new(&intent, true) {
            Ok(write) => self.call(InstallState::WriteIntent, IoCall::child(write)),
            Err(error) => self.discard(error),
        }
    }

    /// The registry closes admission at the deadline; the timer records the lock.
    fn arm_timer(&mut self, status: UnlockStatus) -> InstallTurn {
        let Some(after) = status.remaining else {
            return self.complete(Ok(status));
        };
        let ticket = KeyTicket {
            key: status.key,
            session_id: status.session_id,
        };
        self.activated = Some(status);
        let arm = Effect::Task(TaskEffect::ShortenTimer {
            key: lock_timer(&ticket),
            after,
        });
        self.call(InstallState::ArmTimer, arm.into())
    }

    /// Writes the outcome record when the activation is audited.
    fn complete(&mut self, result: Result<UnlockStatus, InstallError>) -> InstallTurn {
        let Some(audit) = self.audit else {
            return result.map(Next::Done);
        };
        let outcome = match result {
            Ok(_) => AuditOutcome::Applied,
            Err(_) => AuditOutcome::Failed,
        };
        let mut record = self.record(audit, outcome);
        if let Ok(status) = &result {
            record.deadline_ms = status.deadline_ms;
            record.sequence = Some(status.sequence);
        }
        self.finished = Some(result);
        record.intent_id = self.intent_id;
        self.outcome = Some(record);
        self.write_outcome()
    }

    /// A synced intent gets its failed outcome; otherwise nothing was recorded.
    fn key_discarded(&mut self) -> InstallTurn {
        let error = self.failure.take().unwrap_or(InstallError::NotFinished);
        match self.intent_synced {
            true => self.complete(Err(error)),
            false => Err(error),
        }
    }

    /// The key state is settled, so a lost outcome write moves to the record's retry timer.
    fn write_outcome(&mut self) -> InstallTurn {
        let record = self.outcome.clone().ok_or(InstallError::NotFinished)?;
        let keep = KeepAudit::new(vec![record]);
        self.call(InstallState::WriteOutcome, IoCall::child(keep))
    }

    /// The run ends with what the activation decided.
    fn finish(&mut self, kept: Result<bool, AuditError>) -> InstallTurn {
        kept?;
        self.outcome = None;
        let result = self.finished.take().ok_or(InstallError::NotFinished)?;
        result.map(Next::Done)
    }

    fn matches_ticket(&self, status: &UnlockStatus) -> bool {
        (self.ticket).is_some_and(|ticket| {
            (ticket.key, ticket.session_id) == (status.key, status.session_id)
        })
    }

    fn event_answered(&mut self, event: Event) -> InstallTurn {
        match (self.state, event) {
            (InstallState::PrepareKey, Event::Blob(BlobEvent::KeyPrepared { ticket }))
                if ticket.key == self.key =>
            {
                self.prepared(ticket)
            }
            (InstallState::ActivateKey, Event::Blob(BlobEvent::KeyActivated { status }))
                if self.matches_ticket(&status) =>
            {
                self.ticket = None;
                self.arm_timer(status)
            }
            (InstallState::ActivateKey, Event::Blob(BlobEvent::Error(error))) => {
                self.discard(error)
            }
            (InstallState::ArmTimer, Event::Task(event))
                if self.activated.as_ref().is_some_and(|status| {
                    let ticket = KeyTicket {
                        key: status.key,
                        session_id: status.session_id,
                    };
                    answers_timer(&event, &lock_timer(&ticket))
                }) =>
            {
                let status = self.activated.take().ok_or(InstallError::NotFinished)?;
                self.complete(Ok(status))
            }
            (InstallState::DiscardKey, Event::Blob(BlobEvent::KeyDiscarded { ticket }))
                if self.discarded == Some(ticket) =>
            {
                self.key_discarded()
            }
            (InstallState::DiscardKey, Event::Blob(BlobEvent::Error(_))) => self.key_discarded(),
            (_, received) => self.refused(received),
        }
    }
}

impl Operation for InstallKeyOperation {
    type Output = UnlockStatus;
    type Error = InstallError;

    /// The operation hands its only key handle to the adapter and keeps none.
    fn first(&mut self) -> InstallTurn {
        let input = self.input.take().ok_or(InstallError::NotFinished)?;
        let prepare = Effect::Blob(BlobEffect::PrepareKey {
            key: input.key,
            public_key: input.public_key,
            private_key: input.private_key,
            duration: input.duration,
            max: input.max,
        });
        self.call(InstallState::PrepareKey, prepare.into())
    }

    fn next(&mut self, result: IoResult) -> InstallTurn {
        match self.state {
            InstallState::WriteIntent => match result.child::<(), StorageError>()? {
                Ok(()) => {
                    self.intent_synced = true;
                    self.activate()
                }
                Err(error) => self.discard(error),
            },
            InstallState::WriteOutcome => self.finish(result.child::<bool, AuditError>()?),
            _ => self.event_answered(result.event()?),
        }
    }

    /// A failed run discards the prepared key it still owns.
    fn abort(&mut self) -> Vec<IoCall> {
        self.input = None;
        let discard = |ticket| Effect::Blob(BlobEffect::DiscardKey { ticket }).into();
        self.ticket.take().map(discard).into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::DriverContext;
    use crate::driver::drive;
    use crate::jobs::key_wake::wake_unlocked;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::compute::SecretBytes;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use aruna_core::operation::Runnable;
    use aruna_core::structs::execution::job::RoCrateLimits;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::encryption::{BucketKeyError, public_key_of};
    use std::time::SystemTime;
    use ulid::Ulid;

    fn event(event: Event) -> IoResult {
        IoResult::Event(Box::new(event))
    }

    fn blob(blob: BlobEvent) -> IoResult {
        event(Event::Blob(blob))
    }

    fn asked(turn: &InstallTurn) -> &Effect {
        match turn {
            Ok(Next::Call(IoCall::Effect(effect))) => effect,
            other => panic!("expected an effect call, got {other:?}"),
        }
    }

    /// Activates a key as the routes do, then resumes the work that waited for it.
    async fn activate_and_wake<O, K>(
        context: &DriverContext,
        operation: O,
        origin: (RealmId, NodeId),
    ) -> Result<UnlockStatus, O::Error>
    where
        O: Runnable<K, Output = UnlockStatus>,
    {
        let status = drive(operation, context).await?;
        let now_ms = aruna_core::time::unix_timestamp_millis();
        let limits = RoCrateLimits::default();
        wake_unlocked(context, status.key, now_ms, origin, &limits)
            .await
            .unwrap();
        Ok(status)
    }

    fn write_done(result: Result<(), StorageError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn kept(stored: bool) -> IoResult {
        IoResult::Child(ChildOutcome::new(Ok::<_, AuditError>(stored)))
    }

    /// The audit record the write child of `turn` stores.
    fn audit_record(turn: InstallTurn) -> BucketAuditRecord {
        let Ok(Next::Call(IoCall::Child(mut child))) = turn else {
            panic!("expected an audit write, got {turn:?}");
        };
        let effects = child.start();
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected an audit write, got {effects:?}");
        };
        BucketAuditRecord::from_bytes(&writes[0].2).unwrap()
    }

    fn input(duration: Option<Duration>, max: Option<Duration>) -> InstallInput {
        let private = SecretBytes::new(vec![4; 32]);
        InstallInput {
            key: BucketKeyRef::new(Ulid::from_bytes([1; 16]), 1),
            public_key: public_key_of(&private).unwrap(),
            private_key: SharedSecret::new(private),
            duration,
            max,
        }
    }

    fn ticket_of(operation: &InstallKeyOperation) -> KeyTicket {
        KeyTicket {
            key: operation.key,
            session_id: Ulid::from_bytes([2; 16]),
        }
    }

    fn prepared() -> (InstallKeyOperation, KeyTicket) {
        let mut operation = InstallKeyOperation::new(input(None, Some(Duration::from_secs(60))));
        let turn = operation.first();
        assert!(matches!(
            asked(&turn),
            Effect::Blob(BlobEffect::PrepareKey { .. })
        ));
        assert!(operation.input.is_none(), "the key handle is handed on");
        let ticket = ticket_of(&operation);
        let turn = operation.next(blob(BlobEvent::KeyPrepared { ticket }));
        assert_eq!(
            asked(&turn),
            &Effect::Blob(BlobEffect::ActivateKey { ticket })
        );
        (operation, ticket)
    }

    fn status(ticket: KeyTicket) -> UnlockStatus {
        UnlockStatus {
            key: ticket.key,
            session_id: ticket.session_id,
            sequence: ulid::Ulid::from_parts(1, 1),
            deadline_ms: Some(60_000),
            active: true,
            unlocked_at: SystemTime::UNIX_EPOCH,
            remaining: Some(Duration::from_secs(60)),
            max_remaining: Some(Duration::from_secs(60)),
        }
    }

    #[test]
    fn installs_prepared_key() {
        let (mut operation, ticket) = prepared();
        let turn = operation.next(blob(BlobEvent::KeyActivated {
            status: status(ticket),
        }));
        // A timed session arms its lock timer, so the timed lock is recorded.
        let key = lock_timer(&ticket);
        let arm = TaskEffect::ShortenTimer {
            key: key.clone(),
            after: Duration::from_secs(60),
        };
        assert_eq!(asked(&turn), &Effect::Task(arm));
        let after = Duration::from_secs(60);
        let armed = aruna_core::task::TaskEvent::TimerScheduled { key, after };
        assert_eq!(
            operation.next(event(Event::Task(armed))),
            Ok(Next::Done(status(ticket)))
        );

        // A status of another session is no answer to this activation.
        let (mut operation, mut ticket) = prepared();
        ticket.session_id = Ulid::from_bytes([3; 16]);
        let turn = operation.next(blob(BlobEvent::KeyActivated {
            status: status(ticket),
        }));
        assert!(matches!(turn, Err(InstallError::InvalidStateEvent { .. })));
    }

    #[test]
    fn delayed_install_recorded() {
        let node = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let mut operation = InstallKeyOperation::new(input(Some(Duration::from_secs(60)), None))
            .audited(node, None, 1_000);
        operation.first().unwrap();
        let ticket = ticket_of(&operation);
        let intent = audit_record(operation.next(blob(BlobEvent::KeyPrepared { ticket })));
        assert_eq!(intent.deadline_ms, Some(61_000));
        operation.next(write_done(Ok(()))).unwrap();
        let mut active = status(ticket);
        active.unlocked_at = SystemTime::UNIX_EPOCH + Duration::from_secs(31);
        active.deadline_ms = Some(91_000);
        operation
            .next(blob(BlobEvent::KeyActivated {
                status: active.clone(),
            }))
            .unwrap();
        let armed = aruna_core::task::TaskEvent::TimerScheduled {
            key: lock_timer(&ticket),
            after: Duration::from_secs(60),
        };
        let turn = operation.next(event(Event::Task(armed)));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let record = operation.outcome.clone().unwrap();
        assert_eq!(
            (record.deadline_ms, record.sequence),
            (Some(91_000), Some(active.sequence))
        );
        assert_eq!(
            crate::s3::bucket::key::restart::replay(std::slice::from_ref(&intent), 76_000),
            [1]
        );
        assert_eq!(
            crate::s3::bucket::key::restart::replay(&[intent, record], 76_000),
            [1]
        );
    }

    #[test]
    fn audited_install_syncs() {
        let node = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let mut operation = InstallKeyOperation::new(input(None, None)).audited(node, None, 1_000);
        operation.first().unwrap();
        let ticket = ticket_of(&operation);
        let intent = audit_record(operation.next(blob(BlobEvent::KeyPrepared { ticket })));
        assert_eq!(intent.outcome, AuditOutcome::Intent);
        // Reads see the key only after the intent reached the disk.
        let turn = operation.next(write_done(Ok(())));
        assert_eq!(
            asked(&turn),
            &Effect::Blob(BlobEffect::ActivateKey { ticket })
        );
        let mut open = status(ticket);
        (open.remaining, open.max_remaining) = (None, None);
        operation
            .next(blob(BlobEvent::KeyActivated {
                status: open.clone(),
            }))
            .unwrap();
        let outcome = operation.outcome.clone().unwrap();
        assert_eq!(outcome.outcome, AuditOutcome::Applied);
        assert_eq!(operation.next(kept(true)), Ok(Next::Done(open)));
    }

    #[test]
    fn lost_intent_discards() {
        // A lost intent write leaves the key prepared nowhere and records nothing.
        let node = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let mut lost = InstallKeyOperation::new(input(None, None)).audited(node, None, 1);
        lost.first().unwrap();
        let ticket = ticket_of(&lost);
        lost.next(blob(BlobEvent::KeyPrepared { ticket })).unwrap();
        let turn = lost.next(write_done(Err(StorageError::Timeout)));
        assert_eq!(
            asked(&turn),
            &Effect::Blob(BlobEffect::DiscardKey { ticket })
        );
        assert_eq!(
            lost.next(blob(BlobEvent::KeyDiscarded { ticket })),
            Err(InstallError::Storage(StorageError::Timeout))
        );
    }

    #[test]
    fn unstored_outcome_finishes() {
        // The key is active either way; the keeper already handed the record to its timer.
        let node = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let mut operation = InstallKeyOperation::new(input(None, None)).audited(node, None, 1_000);
        operation.first().unwrap();
        let ticket = ticket_of(&operation);
        operation
            .next(blob(BlobEvent::KeyPrepared { ticket }))
            .unwrap();
        operation.next(write_done(Ok(()))).unwrap();
        let mut open = status(ticket);
        (open.remaining, open.max_remaining) = (None, None);
        operation
            .next(blob(BlobEvent::KeyActivated {
                status: open.clone(),
            }))
            .unwrap();
        assert_eq!(operation.next(kept(false)), Ok(Next::Done(open)));
    }

    #[test]
    fn failed_activation_discards() {
        let refused = || BlobError::BucketKey(BucketKeyError::Capacity);
        let (mut operation, ticket) = prepared();
        let turn = operation.next(blob(BlobEvent::Error(refused())));
        assert_eq!(
            asked(&turn),
            &Effect::Blob(BlobEffect::DiscardKey { ticket })
        );
        assert_eq!(
            operation.next(blob(BlobEvent::KeyDiscarded { ticket })),
            Err(InstallError::Blob(refused()))
        );

        // An unexpected answer ends the run, whose cleanup discards the prepared key.
        let (mut stray, ticket) = prepared();
        let turn = stray.next(blob(BlobEvent::KeyDiscarded { ticket }));
        assert!(matches!(turn, Err(InstallError::InvalidStateEvent { .. })));
        let discard: IoCall = Effect::Blob(BlobEffect::DiscardKey { ticket }).into();
        assert_eq!(stray.abort(), vec![discard]);

        // A run stopped before activation discards the prepared key too.
        let (mut stopped, ticket) = prepared();
        let discard: IoCall = Effect::Blob(BlobEffect::DiscardKey { ticket }).into();
        assert_eq!(stopped.abort(), vec![discard]);
        assert!(stopped.abort().is_empty());
    }

    async fn enabled_context() -> (
        tempfile::TempDir,
        DriverContext,
        crate::s3::bucket::encryption::EnableResult,
    ) {
        use crate::driver::drive;
        use crate::s3::bucket::create::CreateBucketOperation;
        use crate::s3::bucket::encryption::{EnableEncryptionOperation, EnableInput};
        use aruna_blob::blob::BlobHandler;
        use aruna_core::UserId;
        use aruna_core::effects::StorageEffect;
        use aruna_core::node_vault::NodeVaultKey;
        use aruna_core::structs::identity::realm::RealmId;
        use aruna_core::structs::storage::blob::{Backend, BackendConfig, BucketInfo};
        use aruna_core::structs::storage::encryption::{BlockCipher, BlockKeys, EncryptionMode};
        use aruna_core::structs::storage::format::Compression;
        use aruna_core::structs::storage::holders::KeyLookup;
        use std::collections::{BTreeMap, HashMap};

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let storage = aruna_storage::FjallStorage::open(root).unwrap();
        storage.open_vault(NodeVaultKey::random());
        let net = aruna_net::NetHandle::new(crate::testing::local_net(), storage.clone())
            .await
            .unwrap();
        let config = BackendConfig {
            backend_type: Backend::FileSystem,
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100_000),
            multipart_bucket: Some("multipart".to_string()),
            root: format!("{root}/blobs"),
            service_config: HashMap::new(),
            timeouts: Default::default(),
        };
        let blob = BlobHandler::new(config, storage.clone(), net.clone())
            .await
            .unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net.clone()),
            blob_handle: Some(blob.clone()),
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let realm_id = RealmId::from_bytes([1; 32]);
        let creator = UserId::new(Ulid::from_bytes([5; 16]), realm_id);
        let group_id = Ulid::from_bytes([3; 16]);
        let info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: creator,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        // The authorization documents the enable reads its admins from.
        let rows = crate::s3::bucket::key::rows::authority_rows(&info, None, &[creator]);
        let documents = [realm_id.as_bytes().to_vec(), group_id.to_bytes().to_vec()];
        for (key, (_, value)) in documents.into_iter().zip(rows.into_iter().skip(2)) {
            let key_space = aruna_core::keyspaces::AUTH_KEYSPACE.to_string();
            let value = value.unwrap();
            let write = StorageEffect::Write {
                key_space,
                key: key.into(),
                value,
                txn_id: None,
            };
            storage.send_storage_effect(write).await;
        }
        drive(
            CreateBucketOperation::new("sealed".to_string(), info),
            &context,
        )
        .await
        .unwrap();
        let holder = SecretBytes::new(vec![6; 32]);
        let record = aruna_core::structs::identity::user::vault::UserKeyRecord {
            realm_id: creator.realm_id,
            user_id: creator,
            record_id: Ulid::from_bytes([7; 16]),
            key_id: "slot".to_string(),
            public_key: public_key_of(&holder).unwrap(),
            fingerprint: aruna_core::vault_format::key_fingerprint(
                &public_key_of(&holder).unwrap(),
            ),
            has_recovery: true,
            node_id: net.node_id(),
            placement: aruna_core::structs::placement::record::PlacementRef::NIL,
            created_at_ms: 1,
            signature: None,
        };
        let enabled = drive(
            EnableEncryptionOperation::new(EnableInput {
                bucket: "sealed".to_string(),
                group_id,
                realm_id,
                node_id: net.node_id(),
                caller: creator,
                mode: EncryptionMode::NodeManaged,
                cipher: BlockCipher::ChaCha20Poly1305,
                block_keys: BlockKeys::ContentDerived,
                max_unlock_ms: None,
                expected_generation: 0,
                lookups: BTreeMap::from([(creator, KeyLookup::Keys(vec![record]))]),
                now_ms: 1,
            }),
            &context,
        )
        .await
        .unwrap();
        (dir, context, enabled)
    }

    #[tokio::test]
    async fn install_wakes_parked() {
        use crate::jobs::store::key_wait::park_job;
        use crate::jobs::store::query::insert_job;
        use crate::jobs::store::state::{ClaimOutcome, claim_job, read_job_record};
        use aruna_core::keyspaces::KEY_COPY_KEYSPACE;
        use aruna_core::node_vault::{VaultEntry, VaultPurpose};
        use aruna_core::structs::execution::job::{
            JobId, JobPayload, JobRecord, JobState, KeyWait, due_index_key,
        };

        let (_dir, context, enabled) = enabled_context().await;
        let storage = &context.storage_handle;
        let node = context.net_handle.as_ref().unwrap().node_id();
        let realm = RealmId::from_bytes([1; 32]);
        let creator = UserId::new(Ulid::from_bytes([5; 16]), realm);
        let key = enabled.key.clone();
        let job_id = JobId::from_bytes([8; 16]);
        let payload = JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        };
        let job = JobRecord::new(job_id, payload, creator, node, 1_000, 1_000, None);
        insert_job(&context, &job).await.unwrap();
        let ClaimOutcome::Claimed(claimed) =
            claim_job(&context, job_id, node, 2_000).await.unwrap()
        else {
            panic!("job must be claimed")
        };
        let wait = KeyWait {
            node_id: node,
            bucket: "sealed".to_string(),
            group_id: None,
            key: key.key,
        };
        park_job(
            &context,
            job_id,
            claimed.claim.unwrap().claim_token,
            3_000,
            vec![wait],
        )
        .await
        .unwrap();
        assert!(!crate::jobs::runtime::key_unlocked(&context, key.key).await);
        assert_eq!(
            read_job_record(&context, job_id, None)
                .await
                .unwrap()
                .unwrap()
                .state,
            JobState::AwaitingKey
        );
        let install = InstallKeyOperation::new(InstallInput {
            key: key.key,
            public_key: key.public_key,
            private_key: enabled.private_key,
            duration: None,
            max: None,
        });
        let status = activate_and_wake(&context, install, (realm, node))
            .await
            .unwrap();
        assert!(status.active && status.remaining.is_none());
        let job = read_job_record(&context, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.state, JobState::Queued);
        let scheduled = StorageEffect::Read {
            key_space: aruna_core::keyspaces::SCHEDULE_INDEX_KEYSPACE.to_string(),
            key: due_index_key(job.due_at_ms, job_id),
            txn_id: None,
        };
        assert!(matches!(
            storage.send_storage_effect(scheduled).await,
            Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
        ));

        // The node copy opens to the installed key; the creator's copy is stored.
        let entry = VaultEntry::new(VaultPurpose::BucketKey, key.vault_entry.unwrap());
        let read = StorageEffect::VaultRead {
            entry,
            txn_id: None,
        };
        let Event::Storage(StorageEvent::VaultResult {
            secret: Some(secret),
            ..
        }) = storage.send_storage_effect(read).await
        else {
            panic!("no node copy");
        };
        assert_eq!(public_key_of(&secret), Some(key.public_key));
        let copy = enabled.holders.holder(creator).unwrap();
        assert_eq!(
            copy.state,
            aruna_core::structs::storage::holders::HolderState::Ready
        );
        let rows = StorageEffect::Iter {
            key_space: KEY_COPY_KEYSPACE.to_string(),
            prefix: Some(key.key.key().into()),
            start: None,
            limit: 8,
            txn_id: None,
        };
        let Event::Storage(StorageEvent::IterResult { values, .. }) =
            storage.send_storage_effect(rows).await
        else {
            panic!("no copies");
        };
        assert_eq!(values.len(), 1);
    }

    #[tokio::test]
    async fn unlock_resumes_rewrite() {
        use crate::blob::migration::queue::{ProcessTransitionsOperation, RECHECK};
        use crate::jobs::key_wake::read_row;
        use crate::s3::bucket::key::unlock::{UnlockBucketOperation, UnlockInput};
        use crate::tests::incoming::{RecordingTaskHandler, freeze_clock};
        use aruna_core::handle::Handle;
        use aruna_core::keyspaces::{
            BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, TRANSITION_KEYSPACE,
        };
        use aruna_core::stream::BackendStream;
        use aruna_core::structs::storage::blob::{BlobVersion, ResolvedBackend, VersionKey};
        use aruna_core::structs::storage::encryption::SealPlan;
        use aruna_core::structs::storage::format::Compression;
        use aruna_core::structs::storage::transition::{
            EncryptionTransition, TransitionKind, TransitionState, TransitionTarget,
        };
        use aruna_core::task::{TaskEffect, TaskKey};

        let (_dir, mut context, enabled) = enabled_context().await;
        let key = enabled.key.key;
        let plan = SealPlan::capture(&enabled.settings, &enabled.key)
            .unwrap()
            .unwrap();
        let blob = context.blob_handle.as_ref().unwrap();
        let stream = BackendStream(Box::pin(futures_util::stream::once(async {
            Ok(bytes::Bytes::from_static(b"waiting encrypted bytes"))
        })));
        let write = BlobEffect::Write {
            bucket: "sealed".to_string(),
            key: "data".to_string(),
            resolved: ResolvedBackend::node_default().with_encryption(Some(SealPlan {
                storage_generation: 0,
                ..plan
            })),
            created_by: UserId::default(),
            blob: stream,
            size: Some(23),
        };
        let Event::Blob(BlobEvent::WriteFinished { location }) = blob.send_blob_effect(write).await
        else {
            panic!("archive must be written")
        };
        let version_key = VersionKey::new("sealed", "data", Ulid::from_bytes([8; 16]));
        let version = BlobVersion::materialized(
            location.get_blake3().unwrap().try_into().unwrap(),
            location.backend.clone(),
            location.format.encoding(),
            SystemTime::UNIX_EPOCH,
            UserId::default(),
            None,
        );
        let transition = EncryptionTransition::new(
            TransitionKind::Reencode,
            Some(key),
            TransitionTarget {
                compression: Compression::Off,
                plan: Some(plan),
            },
            enabled.settings.storage_generation,
            1,
        );
        let writes = vec![
            (
                BLOB_LOCATIONS_KEYSPACE.to_string(),
                location.location_key().unwrap().to_bytes().into(),
                location.to_bytes().unwrap().into(),
            ),
            (
                BLOB_VERSIONS_KEYSPACE.to_string(),
                version_key.to_bytes().unwrap().into(),
                version.to_bytes().unwrap().into(),
            ),
            (
                TRANSITION_KEYSPACE.to_string(),
                b"sealed".to_vec().into(),
                transition.to_bytes().unwrap().into(),
            ),
        ];
        context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await;
        assert_eq!(
            crate::driver::drive(
                ProcessTransitionsOperation::new(context.node_and_realm()),
                &context
            )
            .await
            .unwrap(),
            Some(RECHECK)
        );
        let stored = read_row(&context, TRANSITION_KEYSPACE, b"sealed".to_vec())
            .await
            .unwrap()
            .unwrap();
        let parked = EncryptionTransition::from_bytes(&stored).unwrap();
        assert_eq!(
            (parked.state, parked.remaining),
            (TransitionState::AwaitingKey, 1)
        );
        assert!(parked.retry_at_ms.is_some());

        tokio::time::pause();
        let _clock = freeze_clock();
        let tasks = aruna_tasks::TaskHandle::new();
        let (sender, mut fired) = tokio::sync::mpsc::channel(8);
        tasks
            .set_inbound_handler(std::sync::Arc::new(RecordingTaskHandler { seen: sender }))
            .await;
        tasks
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key: TaskKey::MigrateCompression,
                after: RECHECK,
            }))
            .await;
        context.task_handle = Some(tasks.clone());
        let start = tokio::time::Instant::now();
        tokio::time::advance(Duration::from_secs(1)).await;
        let duration = Duration::from_secs(30);
        let node = context.net_handle.as_ref().unwrap().node_id();
        let realm = RealmId::from_bytes([1; 32]);
        let input = UnlockInput {
            bucket: "sealed".to_string(),
            group_id: Ulid::from_bytes([3; 16]),
            realm_id: realm,
            node_id: node,
            caller: UserId::new(Ulid::from_bytes([5; 16]), realm),
            key,
            duration: Some(duration),
            now_ms: aruna_core::time::unix_timestamp_millis(),
        };
        let unlock = UnlockBucketOperation::new(input, enabled.private_key);
        let status = activate_and_wake(&context, unlock, (realm, node))
            .await
            .unwrap();
        assert!(
            status
                .remaining
                .is_some_and(|remaining| remaining <= duration)
        );
        while fired.recv().await.unwrap() != TaskKey::MigrateCompression {}
        let resumed = read_row(&context, TRANSITION_KEYSPACE, b"sealed".to_vec())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            EncryptionTransition::from_bytes(&resumed)
                .unwrap()
                .retry_at_ms,
            None
        );
        assert_eq!(tokio::time::Instant::now() - start, Duration::from_secs(1));
        assert!(duration < RECHECK);
        crate::driver::drive(
            ProcessTransitionsOperation::new(context.node_and_realm()),
            &context,
        )
        .await
        .unwrap();
        let stored = read_row(&context, TRANSITION_KEYSPACE, b"sealed".to_vec())
            .await
            .unwrap()
            .unwrap();
        let progressed = EncryptionTransition::from_bytes(&stored).unwrap();
        assert_eq!(
            (progressed.done, progressed.remaining, progressed.failed),
            (1, 0, 0)
        );
        assert_eq!(progressed.state, TransitionState::Cleanup);
        tasks.shutdown(Duration::ZERO).await;
        tokio::time::resume();
    }
}
