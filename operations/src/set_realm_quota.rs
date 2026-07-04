use std::collections::BTreeSet;

use aruna_core::admin_document_reducer::{AdminDocumentReducerError, AdminDocumentReducerState};
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::ADMIN_DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, stale_admin_document_conflict_delete_entries,
};
use aruna_core::structs::{Actor, QuotaConfig, RealmConfigDocument};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};

#[derive(Debug, Clone, PartialEq)]
pub struct SetRealmQuotaConfig {
    pub actor: Actor,
    pub quota: QuotaConfig,
}

#[derive(Debug, PartialEq)]
pub struct SetRealmQuotaOperation {
    config: SetRealmQuotaConfig,
    txn_id: Option<TxnId>,
    state: SetRealmQuotaState,
    output: Option<Result<RealmConfigDocument, SetRealmQuotaError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum SetRealmQuotaState {
    Init,
    StartTransaction,
    ReadCurrent,
    WriteDocumentAndAdminState {
        document: RealmConfigDocument,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    },
    DeleteStaleAdminConflicts {
        document: RealmConfigDocument,
    },
    CommitTransaction {
        document: RealmConfigDocument,
    },
    ScheduleDocumentSyncOutboxDrain {
        document: RealmConfigDocument,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum SetRealmQuotaError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentReducerError(#[from] AdminDocumentReducerError),
    #[error("realm config document missing")]
    RealmConfigNotFound,
    #[error("invalid quota configuration: {reason}")]
    InvalidQuota { reason: String },
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl SetRealmQuotaOperation {
    pub fn new(config: SetRealmQuotaConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: SetRealmQuotaState::Init,
            output: None,
        }
    }

    fn document_ref(&self) -> DocumentSyncTarget {
        DocumentSyncTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn emit_read_current(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = SetRealmQuotaState::ReadCurrent;
        let document = self.document_ref();
        let target = self.admin_target();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    document.storage_keyspace().to_string(),
                    document.storage_key(),
                ),
                (
                    ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                    admin_document_reducer_state_key(&target),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn emit_write_document_and_admin_state(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<Effects, SetRealmQuotaError> {
        let Some(txn_id) = self.txn_id else {
            return Err(SetRealmQuotaError::MissingTransaction);
        };
        validate_quota(&self.config.quota)?;

        let Some(document_value) = document_value else {
            return Err(SetRealmQuotaError::RealmConfigNotFound);
        };
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;

        let target = self.admin_target();
        let previous_reducer_state = reducer_state_value
            .as_ref()
            .map(|value| {
                postcard::from_bytes::<AdminDocumentReducerState>(value.as_ref())
                    .map_err(ConversionError::from)
            })
            .transpose()?;
        if previous_reducer_state
            .as_ref()
            .is_some_and(|state| state.target != target)
        {
            return Err(AdminDocumentReducerError::TargetMismatch.into());
        }

        let mut reducer_state = previous_reducer_state
            .clone()
            .unwrap_or_else(|| AdminDocumentReducerState::new(target));
        let admin_event = reducer_state.apply_operation(
            &self.config.actor,
            AdminDocumentOperation::RealmConfigQuotaSet {
                quota: self.config.quota.clone(),
            },
        )?;
        document.quota = self.config.quota.clone();

        let stale_conflict_deletes = stale_admin_document_conflict_delete_entries(
            previous_reducer_state.as_ref(),
            Some(&reducer_state),
        );
        let document_target = self.document_ref();
        let mut writes = vec![
            (
                document_target.storage_keyspace().to_string(),
                document_target.storage_key(),
                document.to_bytes(&self.config.actor)?.into(),
            ),
            admin_document_reducer_state_write_entry(&reducer_state)?,
        ];
        let record = new_outbox_record_with_id(
            admin_event.event_id,
            self.config.actor.node_id,
            document_target,
            Vec::new(),
            DocumentSyncOutboxEvent::AdminOperation {
                event: Box::new(admin_event),
            },
        );
        writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        writes.extend(admin_document_conflict_write_entries(&reducer_state)?);

        self.output = Some(Ok(document.clone()));
        self.state = SetRealmQuotaState::WriteDocumentAndAdminState {
            document,
            stale_conflict_deletes,
        };

        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn emit_commit_transaction(&mut self, document: RealmConfigDocument) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(SetRealmQuotaError::MissingTransaction);
        };
        self.state = SetRealmQuotaState::CommitTransaction { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: SetRealmQuotaError) -> Effects {
        let cleanup = self.abort();
        self.state = SetRealmQuotaState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(SetRealmQuotaError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for SetRealmQuotaOperation {
    type Output = RealmConfigDocument;
    type Error = SetRealmQuotaError;

    fn start(&mut self) -> Effects {
        self.state = SetRealmQuotaState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            SetRealmQuotaState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            SetRealmQuotaState::ReadCurrent => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, document_value), (_, reducer_state_value)] = values.as_slice() else {
                        return self.unexpected_event(
                            "storage batch read result with realm config and reducer state",
                            format!("{values:?}"),
                        );
                    };
                    match self.emit_write_document_and_admin_state(
                        document_value.clone(),
                        reducer_state_value.clone(),
                    ) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch read result", format!("{other:?}")),
            },
            SetRealmQuotaState::WriteDocumentAndAdminState {
                document,
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(SetRealmQuotaError::MissingTransaction);
                    };
                    if !stale_conflict_deletes.is_empty() {
                        self.state = SetRealmQuotaState::DeleteStaleAdminConflicts { document };
                        return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                            deletes: stale_conflict_deletes,
                            txn_id: Some(txn_id),
                        })];
                    }
                    self.emit_commit_transaction(document)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch write result", format!("{other:?}")),
            },
            SetRealmQuotaState::DeleteStaleAdminConflicts { document } => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    self.emit_commit_transaction(document)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            SetRealmQuotaState::CommitTransaction { document } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = SetRealmQuotaState::ScheduleDocumentSyncOutboxDrain { document };
                    smallvec![schedule_outbox_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            SetRealmQuotaState::ScheduleDocumentSyncOutboxDrain { .. } => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = SetRealmQuotaState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document operation outbox drain; durable outbox remains retryable");
                    self.state = SetRealmQuotaState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            SetRealmQuotaState::Finish | SetRealmQuotaState::Error | SetRealmQuotaState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            SetRealmQuotaState::Finish | SetRealmQuotaState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .expect("set realm quota operation must set output")
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

fn validate_quota(quota: &QuotaConfig) -> Result<(), SetRealmQuotaError> {
    if !(1..=100).contains(&quota.warn_threshold_percent) {
        return Err(SetRealmQuotaError::InvalidQuota {
            reason: format!(
                "warn_threshold_percent must be between 1 and 100, got {}",
                quota.warn_threshold_percent
            ),
        });
    }
    if quota.grace_factor_percent < 100 {
        return Err(SetRealmQuotaError::InvalidQuota {
            reason: format!(
                "grace_factor_percent must be at least 100, got {}",
                quota.grace_factor_percent
            ),
        });
    }
    let mut seen_groups = BTreeSet::new();
    for over in &quota.group_overrides {
        if !seen_groups.insert(over.group_id) {
            return Err(SetRealmQuotaError::InvalidQuota {
                reason: format!("duplicate group override for group {}", over.group_id),
            });
        }
    }
    let mut seen_users = BTreeSet::new();
    for over in &quota.user_group_cap_overrides {
        if !seen_users.insert(over.user_id) {
            return Err(SetRealmQuotaError::InvalidQuota {
                reason: format!("duplicate user cap override for user {}", over.user_id),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::get_realm_config::GetRealmConfigOperation;
    use aruna_core::document::DocumentSyncTarget;
    use aruna_core::events::StorageEvent;
    use aruna_core::structs::{
        GroupQuotaOverride, RealmConfigDocument, RealmId, UserGroupCapOverride,
    };
    use aruna_core::types::UserId;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn test_ctx(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        }
    }

    fn actor(seed: u8, realm_id: RealmId) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([seed; 16]), realm_id),
            realm_id,
        }
    }

    async fn seed_config(ctx: &DriverContext, actor: &Actor, document: &RealmConfigDocument) {
        let target = DocumentSyncTarget::RealmConfig {
            realm_id: actor.realm_id,
        };
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: document.to_bytes(actor).unwrap().into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected event: {other:?}"),
        }
    }

    fn custom_quota() -> QuotaConfig {
        QuotaConfig {
            default_group_quota_bytes: Some(1_000_000),
            grace_factor_percent: 120,
            warn_threshold_percent: 90,
            group_overrides: vec![GroupQuotaOverride {
                group_id: Ulid::from_bytes([7; 16]),
                quota_bytes: Some(2_000_000),
                grace_factor_percent: Some(150),
            }],
            max_groups_per_user: Some(5),
            user_group_cap_overrides: vec![UserGroupCapOverride {
                user_id: UserId::local(Ulid::from_bytes([8; 16]), RealmId::from_bytes([1; 32])),
                max_groups: Some(10),
            }],
            max_devices_per_user: Some(4),
        }
    }

    #[tokio::test]
    async fn set_quota_round_trips_through_config_document() {
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([1; 32]);
        let actor = actor(1, realm_id);
        let document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        seed_config(&ctx, &actor, &document).await;

        let quota = custom_quota();
        let stored = drive(
            SetRealmQuotaOperation::new(SetRealmQuotaConfig {
                actor: actor.clone(),
                quota: quota.clone(),
            }),
            &ctx,
        )
        .await
        .unwrap();
        assert_eq!(stored.quota, quota);

        let reread = drive(GetRealmConfigOperation::new(realm_id), &ctx)
            .await
            .unwrap();
        assert_eq!(reread.quota, quota);
        assert_eq!(reread.quota.max_devices_per_user, Some(4));
    }

    #[tokio::test]
    async fn set_quota_fails_when_config_missing() {
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([2; 32]);
        let actor = actor(2, realm_id);

        let error = drive(
            SetRealmQuotaOperation::new(SetRealmQuotaConfig {
                actor,
                quota: QuotaConfig::default(),
            }),
            &ctx,
        )
        .await
        .unwrap_err();
        assert_eq!(error, SetRealmQuotaError::RealmConfigNotFound);
    }

    #[test]
    fn validate_quota_rejects_out_of_range_warn_threshold() {
        let too_low = QuotaConfig {
            warn_threshold_percent: 0,
            ..QuotaConfig::default()
        };
        assert!(matches!(
            validate_quota(&too_low),
            Err(SetRealmQuotaError::InvalidQuota { .. })
        ));
        let too_high = QuotaConfig {
            warn_threshold_percent: 101,
            ..QuotaConfig::default()
        };
        assert!(matches!(
            validate_quota(&too_high),
            Err(SetRealmQuotaError::InvalidQuota { .. })
        ));
    }

    #[test]
    fn validate_quota_rejects_low_grace_factor() {
        let quota = QuotaConfig {
            grace_factor_percent: 99,
            ..QuotaConfig::default()
        };
        assert!(matches!(
            validate_quota(&quota),
            Err(SetRealmQuotaError::InvalidQuota { .. })
        ));
    }

    #[test]
    fn validate_quota_rejects_duplicate_group_override() {
        let mut quota = QuotaConfig::default();
        let group_id = Ulid::from_bytes([3; 16]);
        quota.group_overrides = vec![
            GroupQuotaOverride {
                group_id,
                quota_bytes: Some(1),
                grace_factor_percent: None,
            },
            GroupQuotaOverride {
                group_id,
                quota_bytes: Some(2),
                grace_factor_percent: None,
            },
        ];
        assert!(matches!(
            validate_quota(&quota),
            Err(SetRealmQuotaError::InvalidQuota { .. })
        ));
    }

    #[test]
    fn validate_quota_rejects_duplicate_user_cap_override() {
        let mut quota = QuotaConfig::default();
        let user_id = UserId::local(Ulid::from_bytes([4; 16]), RealmId::from_bytes([1; 32]));
        quota.user_group_cap_overrides = vec![
            UserGroupCapOverride {
                user_id,
                max_groups: Some(1),
            },
            UserGroupCapOverride {
                user_id,
                max_groups: Some(2),
            },
        ];
        assert!(matches!(
            validate_quota(&quota),
            Err(SetRealmQuotaError::InvalidQuota { .. })
        ));
    }

    #[test]
    fn validate_quota_accepts_default() {
        assert!(validate_quota(&QuotaConfig::default()).is_ok());
    }
}
