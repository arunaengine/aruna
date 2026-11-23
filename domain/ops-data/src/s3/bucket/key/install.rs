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
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::encryption::{
    BucketKeyRef, KeyTicket, UnlockStatus, deadline_after,
};
use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, BucketAuditRecord};
use aruna_core::task::TaskEffect;
use aruna_core::user::id::UserId;
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

crate::s3::bucket::audit_retry::audit_failure!(InstallError, "WriteOutcome");

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
    ids: IdMint,
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
            ids: IdMint::default(),
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
        match received.into_blob() {
            Ok(BlobEvent::Error(error)) => Err(error.into()),
            Ok(other) => Err(InstallError::InvalidStateEvent {
                state: format!("{:?}", self.state),
                expected: "the event of the current step",
                received: other.label(),
            }),
            Err(other) => Err(InstallError::InvalidStateEvent {
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
        let discard = Effect::blob(BlobEffect::DiscardKey { ticket });
        self.call(InstallState::DiscardKey, discard.into())
    }

    fn record(&mut self, audit: InstallAudit, outcome: AuditOutcome) -> BucketAuditRecord {
        let (duration, max) = self.bounds;
        BucketAuditRecord {
            event_id: self.ids.mint(audit.now_ms),
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
        let activate = Effect::blob(BlobEffect::ActivateKey { ticket });
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
            (state, Event::Blob(blob)) => match (state, *blob) {
                (InstallState::PrepareKey, BlobEvent::KeyPrepared { ticket })
                    if ticket.key == self.key =>
                {
                    self.prepared(ticket)
                }
                (InstallState::ActivateKey, BlobEvent::KeyActivated { status })
                    if self.matches_ticket(&status) =>
                {
                    self.ticket = None;
                    self.arm_timer(status)
                }
                (InstallState::ActivateKey, BlobEvent::Error(error)) => self.discard(error),
                (InstallState::DiscardKey, BlobEvent::KeyDiscarded { ticket })
                    if self.discarded == Some(ticket) =>
                {
                    self.key_discarded()
                }
                (InstallState::DiscardKey, BlobEvent::Error(_)) => self.key_discarded(),
                (_, received) => self.refused(Event::blob(received)),
            },
            (_, received) => self.refused(received),
        }
    }
}

impl Operation for InstallKeyOperation {
    type Output = UnlockStatus;
    type Error = InstallError;

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }

    /// The operation hands its only key handle to the adapter and keeps none.
    fn first(&mut self) -> InstallTurn {
        let input = self.input.take().ok_or(InstallError::NotFinished)?;
        let prepare = Effect::blob(BlobEffect::PrepareKey {
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
        let discard = |ticket| Effect::blob(BlobEffect::DiscardKey { ticket }).into();
        self.ticket.take().map(discard).into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::compute::SecretBytes;
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::storage::encryption::{BucketKeyError, public_key_of};
    use std::time::SystemTime;
    use ulid::Ulid;

    fn event(event: Event) -> IoResult {
        IoResult::Event(Box::new(event))
    }

    fn blob(blob: BlobEvent) -> IoResult {
        event(Event::blob(blob))
    }

    fn asked(turn: &InstallTurn) -> &Effect {
        match turn {
            Ok(Next::Call(IoCall::Effect(effect))) => effect,
            other => panic!("expected an effect call, got {other:?}"),
        }
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
            (asked(&turn)).as_blob(),
            Some(BlobEffect::PrepareKey { .. })
        ));
        assert!(operation.input.is_none(), "the key handle is handed on");
        let ticket = ticket_of(&operation);
        let turn = operation.next(blob(BlobEvent::KeyPrepared { ticket }));
        assert_eq!(
            asked(&turn),
            &Effect::blob(BlobEffect::ActivateKey { ticket })
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
            &Effect::blob(BlobEffect::ActivateKey { ticket })
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
            &Effect::blob(BlobEffect::DiscardKey { ticket })
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
            &Effect::blob(BlobEffect::DiscardKey { ticket })
        );
        assert_eq!(
            operation.next(blob(BlobEvent::KeyDiscarded { ticket })),
            Err(InstallError::Blob(refused()))
        );

        // An unexpected answer ends the run, whose cleanup discards the prepared key.
        let (mut stray, ticket) = prepared();
        let turn = stray.next(blob(BlobEvent::KeyDiscarded { ticket }));
        assert!(matches!(turn, Err(InstallError::InvalidStateEvent { .. })));
        let discard: IoCall = Effect::blob(BlobEffect::DiscardKey { ticket }).into();
        assert_eq!(stray.abort(), vec![discard]);

        // A run stopped before activation discards the prepared key too.
        let (mut stopped, ticket) = prepared();
        let discard: IoCall = Effect::blob(BlobEffect::DiscardKey { ticket }).into();
        assert_eq!(stopped.abort(), vec![discard]);
        assert!(stopped.abort().is_empty());
    }
}
