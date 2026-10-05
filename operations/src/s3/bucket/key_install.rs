//! Installs a checked bucket key in this node's unlock registry: prepare, then activate. A key
//! that cannot be activated is discarded, so none stays prepared. A timed key arms its lock timer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::audit_retry::{AUDIT_ATTEMPTS, retry_effect, retry_timer};
use crate::s3::bucket::key_lock::{answers_timer, lock_timer};
use aruna_core::compute::SharedSecret;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::BUCKET_AUDIT_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::encryption::{
    BucketKeyRef, KeyTicket, UnlockStatus, deadline_after,
};
use aruna_core::structs::storage::key_audit::{
    AuditAction, AuditOutcome, BucketAuditRecord, next_event_id,
};
use aruna_core::task::TaskEffect;
use aruna_core::types::Effects;
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
use std::time::Duration;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InstallState {
    Init,
    PrepareKey,
    WriteIntent,
    SyncIntent,
    ActivateKey,
    ArmTimer,
    DiscardKey,
    WriteOutcome,
    ArmRetry,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum InstallError {
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
    #[error("installing the key did not finish")]
    NotFinished,
}

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
    /// True once the intent is durable, so a failed activation records its outcome.
    intent_synced: bool,
    /// The event id of the stored intent, which the outcome names.
    intent_id: Option<Ulid>,
    outcome: Option<BucketAuditRecord>,
    attempts: u32,
    output: Option<Result<UnlockStatus, InstallError>>,
}

impl InstallKeyOperation {
    pub fn new(input: InstallInput) -> Self {
        Self {
            key: input.key,
            bounds: (input.duration, input.max),
            input: Some(input),
            audit: None,
            state: InstallState::Init,
            ticket: None,
            session: None,
            discarded: None,
            failure: None,
            activated: None,
            intent_synced: false,
            intent_id: None,
            outcome: None,
            attempts: 0,
            output: None,
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

    /// A failure discards the prepared key this operation still owns.
    fn fail(&mut self, error: impl Into<InstallError>) -> Effects {
        self.output = Some(Err(error.into()));
        self.state = InstallState::Error;
        self.abort()
    }

    fn discard(&mut self, error: impl Into<InstallError>) -> Effects {
        let Some(ticket) = self.ticket.take() else {
            return self.fail(error);
        };
        self.failure = Some(error.into());
        self.state = InstallState::DiscardKey;
        self.discarded = Some(ticket);
        smallvec![Effect::Blob(BlobEffect::DiscardKey { ticket })]
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

    fn write(&mut self, record: &BucketAuditRecord) -> Effects {
        match record.to_bytes() {
            Ok(value) => smallvec![Effect::Storage(StorageEffect::Write {
                key_space: BUCKET_AUDIT_KEYSPACE.to_string(),
                key: record.key().into(),
                value: value.into(),
                txn_id: None,
            })],
            Err(error) => self.discard(error),
        }
    }

    fn activate(&mut self) -> Effects {
        let Some(ticket) = self.ticket else {
            return self.fail(InstallError::NotFinished);
        };
        self.state = InstallState::ActivateKey;
        smallvec![Effect::Blob(BlobEffect::ActivateKey { ticket })]
    }

    /// The registry closes admission at the deadline; the timer records the lock.
    fn arm_timer(&mut self, status: UnlockStatus) -> Effects {
        let Some(after) = status.remaining else {
            return self.complete(Ok(status));
        };
        let ticket = KeyTicket {
            key: status.key,
            session_id: status.session_id,
        };
        self.activated = Some(status);
        self.state = InstallState::ArmTimer;
        smallvec![Effect::Task(TaskEffect::ResetTimer {
            key: lock_timer(&ticket),
            after,
        })]
    }

    /// Writes the outcome record when the activation is audited.
    fn complete(&mut self, result: Result<UnlockStatus, InstallError>) -> Effects {
        let Some(audit) = self.audit else {
            self.state = InstallState::Finish;
            self.output = Some(result);
            return smallvec![];
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
        self.output = Some(result);
        record.intent_id = self.intent_id;
        self.outcome = Some(record);
        self.retry_outcome()
    }

    /// A synced intent gets its failed outcome; otherwise nothing was recorded.
    fn key_discarded(&mut self) -> Effects {
        let error = self.failure.take().unwrap_or(InstallError::NotFinished);
        match self.intent_synced {
            true => self.complete(Err(error)),
            false => self.fail(error),
        }
    }

    fn retry_outcome(&mut self) -> Effects {
        let Some(record) = self.outcome.clone() else {
            return self.fail(InstallError::NotFinished);
        };
        self.attempts += 1;
        self.state = InstallState::WriteOutcome;
        self.write(&record)
    }
}

impl Operation for InstallKeyOperation {
    type Output = UnlockStatus;
    type Error = InstallError;

    /// The operation hands its only key handle to the adapter and keeps none.
    fn start(&mut self) -> Effects {
        let Some(input) = self.input.take() else {
            return self.fail(InstallError::NotFinished);
        };
        self.state = InstallState::PrepareKey;
        smallvec![Effect::Blob(BlobEffect::PrepareKey {
            key: input.key,
            public_key: input.public_key,
            private_key: input.private_key,
            duration: input.duration,
            max: input.max,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let ticket = self.ticket;
        let discarded = self.discarded;
        match (self.state, event) {
            (InstallState::PrepareKey, Event::Blob(BlobEvent::KeyPrepared { ticket }))
                if ticket.key == self.key =>
            {
                self.ticket = Some(ticket);
                self.session = Some(ticket.session_id);
                let Some(audit) = self.audit else {
                    return self.activate();
                };
                let intent = self.record(audit, AuditOutcome::Intent);
                self.intent_id = Some(intent.event_id);
                self.state = InstallState::WriteIntent;
                self.write(&intent)
            }
            (InstallState::WriteIntent, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.state = InstallState::SyncIntent;
                smallvec![Effect::Storage(StorageEffect::SyncAll)]
            }
            // The intent must survive a crash before any read can use the key.
            (InstallState::SyncIntent, Event::Storage(StorageEvent::SyncAllFinished)) => {
                self.intent_synced = true;
                self.activate()
            }
            (
                InstallState::WriteIntent | InstallState::SyncIntent,
                Event::Storage(StorageEvent::Error { error }),
            ) => self.discard(error),
            (InstallState::ActivateKey, Event::Blob(BlobEvent::KeyActivated { status }))
                if ticket.is_some_and(|ticket| {
                    (ticket.key, ticket.session_id) == (status.key, status.session_id)
                }) =>
            {
                self.ticket = None;
                self.arm_timer(status)
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
                match self.activated.take() {
                    Some(status) => self.complete(Ok(status)),
                    None => self.fail(InstallError::NotFinished),
                }
            }
            (InstallState::ActivateKey, Event::Blob(BlobEvent::Error(error))) => {
                self.discard(error)
            }
            (InstallState::DiscardKey, Event::Blob(BlobEvent::KeyDiscarded { ticket }))
                if discarded == Some(ticket) =>
            {
                self.key_discarded()
            }
            (InstallState::DiscardKey, Event::Blob(BlobEvent::Error(_))) => self.key_discarded(),
            // The key state is settled; a failed outcome write is retried here first.
            (InstallState::WriteOutcome, Event::Storage(StorageEvent::Error { .. }))
                if self.attempts < AUDIT_ATTEMPTS =>
            {
                self.retry_outcome()
            }
            (InstallState::WriteOutcome, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.outcome = None;
                self.state = InstallState::Finish;
                smallvec![]
            }
            // A lasting outage hands the outcome to its own retry timer.
            (InstallState::WriteOutcome, Event::Storage(StorageEvent::Error { .. })) => {
                let Some(record) = self.outcome.as_ref() else {
                    return self.fail(InstallError::NotFinished);
                };
                self.state = InstallState::ArmRetry;
                smallvec![retry_effect(record)]
            }
            (InstallState::ArmRetry, Event::Task(event))
                if self
                    .outcome
                    .as_ref()
                    .is_some_and(|record| answers_timer(&event, &retry_timer(record))) =>
            {
                self.outcome = None;
                self.state = InstallState::Finish;
                smallvec![]
            }
            (InstallState::Finish | InstallState::Error, _) => smallvec![],
            (_, Event::Blob(BlobEvent::Error(error))) => self.fail(error),
            (state, received) => self.fail(InstallError::InvalidStateEvent {
                state: format!("{state:?}"),
                expected: "the event of the current step",
                received,
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, InstallState::Finish | InstallState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(InstallError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        self.input = None;
        match self.ticket.take() {
            Some(ticket) => smallvec![Effect::Blob(BlobEffect::DiscardKey { ticket })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::compute::SecretBytes;
    use aruna_core::structs::storage::encryption::{BucketKeyError, public_key_of};
    use std::time::SystemTime;
    use ulid::Ulid;

    fn prepared() -> (InstallKeyOperation, KeyTicket) {
        let private = SecretBytes::new(vec![4; 32]);
        let mut operation = InstallKeyOperation::new(InstallInput {
            key: BucketKeyRef::new(Ulid::from_bytes([1; 16]), 1),
            public_key: public_key_of(&private).unwrap(),
            private_key: SharedSecret::new(private),
            duration: None,
            max: Some(Duration::from_secs(60)),
        });
        let effects = operation.start();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::PrepareKey { .. })]
        ));
        assert!(operation.input.is_none(), "the key handle is handed on");
        let ticket = KeyTicket {
            key: operation.key,
            session_id: Ulid::from_bytes([2; 16]),
        };
        let effects = operation.step(Event::Blob(BlobEvent::KeyPrepared { ticket }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ActivateKey { ticket })]
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
        let effects = operation.step(Event::Blob(BlobEvent::KeyActivated {
            status: status(ticket),
        }));
        // A timed session arms its lock timer, so the timed lock is recorded.
        let key = lock_timer(&ticket);
        let arm = TaskEffect::ResetTimer {
            key: key.clone(),
            after: Duration::from_secs(60),
        };
        assert_eq!(effects.as_slice(), [Effect::Task(arm)]);
        let after = Duration::from_secs(60);
        let armed = aruna_core::task::TaskEvent::TimerScheduled { key, after };
        operation.step(Event::Task(armed));
        assert_eq!(operation.finalize(), Ok(status(ticket)));

        // A status of another session is no answer to this activation.
        let (mut operation, mut ticket) = prepared();
        ticket.session_id = Ulid::from_bytes([3; 16]);
        operation.step(Event::Blob(BlobEvent::KeyActivated {
            status: status(ticket),
        }));
        assert!(matches!(
            operation.finalize(),
            Err(InstallError::InvalidStateEvent { .. })
        ));
    }

    #[test]
    fn delayed_install_recorded() {
        let private = SecretBytes::new(vec![4; 32]);
        let node = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let mut operation = InstallKeyOperation::new(InstallInput {
            key: BucketKeyRef::new(Ulid::from_bytes([1; 16]), 1),
            public_key: public_key_of(&private).unwrap(),
            private_key: SharedSecret::new(private),
            duration: Some(Duration::from_secs(60)),
            max: None,
        })
        .audited(node, None, 1_000);
        operation.start();
        let ticket = KeyTicket {
            key: operation.key,
            session_id: Ulid::from_bytes([2; 16]),
        };
        operation.step(Event::Blob(BlobEvent::KeyPrepared { ticket }));
        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        }));
        operation.step(Event::Storage(StorageEvent::SyncAllFinished));
        let mut active = status(ticket);
        active.unlocked_at = SystemTime::UNIX_EPOCH + Duration::from_secs(31);
        active.deadline_ms = Some(91_000);
        operation.step(Event::Blob(BlobEvent::KeyActivated {
            status: active.clone(),
        }));
        let effects = operation.step(Event::Task(aruna_core::task::TaskEvent::TimerScheduled {
            key: lock_timer(&ticket),
            after: Duration::from_secs(60),
        }));
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("outcome missing")
        };
        let record = BucketAuditRecord::from_bytes(value).unwrap();
        assert_eq!(
            (record.deadline_ms, record.sequence),
            (Some(91_000), Some(active.sequence))
        );
        assert_eq!(
            crate::s3::bucket::key_restart::replay(&[record], 76_000),
            [1]
        );
    }

    #[test]
    fn audited_install_syncs() {
        let private = SecretBytes::new(vec![4; 32]);
        let node = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let mut operation = InstallKeyOperation::new(InstallInput {
            key: BucketKeyRef::new(Ulid::from_bytes([1; 16]), 1),
            public_key: public_key_of(&private).unwrap(),
            private_key: SharedSecret::new(private),
            duration: None,
            max: None,
        })
        .audited(node, None, 1_000);
        operation.start();
        let ticket = KeyTicket {
            key: operation.key,
            session_id: Ulid::from_bytes([2; 16]),
        };
        let effects = operation.step(Event::Blob(BlobEvent::KeyPrepared { ticket }));
        let record = |effects: &Effects| {
            let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
                panic!("expected an audit write, got {effects:?}");
            };
            BucketAuditRecord::from_bytes(value).unwrap()
        };
        assert_eq!(record(&effects).outcome, AuditOutcome::Intent);
        let written = || {
            Event::Storage(StorageEvent::WriteResult {
                key: Vec::new().into(),
            })
        };
        // Reads see the key only after the intent reached the disk.
        let effects = operation.step(written());
        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::SyncAll)]
        );
        let effects = operation.step(Event::Storage(StorageEvent::SyncAllFinished));
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ActivateKey { ticket })]
        );
        let mut open = status(ticket);
        (open.remaining, open.max_remaining) = (None, None);
        let effects = operation.step(Event::Blob(BlobEvent::KeyActivated {
            status: open.clone(),
        }));
        assert_eq!(record(&effects).outcome, AuditOutcome::Applied);
        operation.step(written());
        assert_eq!(operation.finalize(), Ok(open));

        // A lost intent write leaves the key prepared nowhere and records nothing.
        let (mut lost, ticket) = prepared();
        lost.audit = Some(InstallAudit {
            node_id: node,
            actor: None,
            now_ms: 1,
        });
        lost.state = InstallState::WriteIntent;
        let error = StorageError::Timeout;
        let effects = lost.step(Event::Storage(StorageEvent::Error { error }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::DiscardKey { ticket })]
        );
        let effects = lost.step(Event::Blob(BlobEvent::KeyDiscarded { ticket }));
        assert!(effects.is_empty());
        assert_eq!(
            lost.finalize(),
            Err(InstallError::Storage(StorageError::Timeout))
        );
    }

    #[test]
    fn failed_activation_discards() {
        let refused = || BlobError::BucketKey(BucketKeyError::Capacity);
        let (mut operation, ticket) = prepared();
        let effects = operation.step(Event::Blob(BlobEvent::Error(refused())));
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::DiscardKey { ticket })]
        );
        operation.step(Event::Blob(BlobEvent::KeyDiscarded { ticket }));
        assert_eq!(operation.finalize(), Err(InstallError::Blob(refused())));

        // An unexpected answer discards the prepared key this operation owns.
        let (mut stray, ticket) = prepared();
        let effects = stray.step(Event::Blob(BlobEvent::KeyDiscarded { ticket }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::DiscardKey { ticket })]
        );
        assert!(matches!(
            stray.finalize(),
            Err(InstallError::InvalidStateEvent { .. })
        ));

        // A run stopped before activation discards the prepared key too.
        let (mut stopped, ticket) = prepared();
        assert_eq!(
            stopped.abort().as_slice(),
            [Effect::Blob(BlobEffect::DiscardKey { ticket })]
        );
    }

    #[tokio::test]
    async fn enable_then_install() {
        use crate::driver::{DriverContext, drive};
        use crate::s3::bucket::create::CreateBucketOperation;
        use crate::s3::bucket::encryption::{EnableEncryptionOperation, EnableInput};
        use aruna_blob::blob::BlobHandler;
        use aruna_core::UserId;
        use aruna_core::effects::StorageEffect;
        use aruna_core::events::StorageEvent;
        use aruna_core::keyspaces::KEY_COPY_KEYSPACE;
        use aruna_core::node_vault::{NodeVaultKey, VaultEntry, VaultPurpose};
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
        let net = aruna_net::NetHandle::new(aruna_net::NetConfig::default(), storage.clone())
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
            task_handle: None,
            compute_handle: None,
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
        };
        // The authorization documents the enable reads its admins from.
        let rows = crate::s3::bucket::key_rows::authority_rows(&info, None, &[creator]);
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
        let key = enabled.key.clone();
        let install = InstallKeyOperation::new(InstallInput {
            key: key.key,
            public_key: key.public_key,
            private_key: enabled.private_key,
            duration: None,
            max: None,
        });
        let status = drive(install, &context).await.unwrap();
        assert!(status.active && status.remaining.is_none());

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
}
