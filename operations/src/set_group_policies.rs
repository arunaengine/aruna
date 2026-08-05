use aruna_core::admin_document_reducer::{
    AdminDocumentReducerError, AdminDocumentReducerState, GROUP_POLICIES_PATH,
};
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{ADMIN_DOCUMENT_STATE_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::request_policy::{RequestPolicy, policy_set_hash, validate_policy_set};
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, stale_admin_document_conflict_delete_entries,
};
use aruna_core::structs::{Actor, GroupAuthorizationDocument, PlacementRef, RealmConfigDocument};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, GroupId, Key, KeySpace, TxnId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::placement::placement_ref_for_target;

#[derive(Debug, Clone, PartialEq)]
pub struct SetGroupPoliciesConfig {
    pub actor: Actor,
    pub group_id: GroupId,
    pub policies: Vec<RequestPolicy>,
    /// When set, the write applies only if the stored set still hashes to it,
    /// compared inside the write transaction to close the check/use window.
    pub expected_hash: Option<[u8; 32]>,
}

/// Replaces a group's deny-only request policy set. Rides the group admin
/// document machinery, mirroring the realm-scoped set: the value replicates as
/// one last-writer-wins register carried on the group authorization document.
#[derive(Debug, PartialEq)]
pub struct SetGroupPoliciesOperation {
    config: SetGroupPoliciesConfig,
    txn_id: Option<TxnId>,
    state: SetGroupPoliciesState,
    output: Option<Result<GroupAuthorizationDocument, SetGroupPoliciesError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum SetGroupPoliciesState {
    Init,
    StartTransaction,
    ReadCurrent,
    WriteDocumentAndAdminState {
        document: GroupAuthorizationDocument,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    },
    DeleteStaleAdminConflicts {
        document: GroupAuthorizationDocument,
    },
    CommitTransaction {
        document: GroupAuthorizationDocument,
    },
    ScheduleDocumentSyncOutboxDrain {
        document: GroupAuthorizationDocument,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum SetGroupPoliciesError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentReducerError(#[from] AdminDocumentReducerError),
    #[error("group authorization document missing")]
    GroupAuthDocNotFound,
    #[error("stored policy set changed")]
    StaleHash,
    #[error("invalid policy set: {reason}")]
    InvalidPolicies { reason: String },
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl SetGroupPoliciesOperation {
    pub fn new(config: SetGroupPoliciesConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: SetGroupPoliciesState::Init,
            output: None,
        }
    }

    fn document_ref(&self) -> DocumentSyncTarget {
        DocumentSyncTarget::GroupAuthorization {
            group_id: self.config.group_id,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::Group {
            group_id: self.config.group_id,
        }
    }

    fn emit_read_current(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = SetGroupPoliciesState::ReadCurrent;
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
                (
                    REALM_CONFIG_KEYSPACE.to_string(),
                    ByteView::from(*self.config.actor.realm_id.as_bytes()),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn emit_write_document_and_admin_state(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
        realm_config_value: Option<Value>,
    ) -> Result<Effects, SetGroupPoliciesError> {
        let Some(txn_id) = self.txn_id else {
            return Err(SetGroupPoliciesError::MissingTransaction);
        };
        validate_policy_set(&self.config.policies)
            .map_err(|reason| SetGroupPoliciesError::InvalidPolicies { reason })?;

        let Some(document_value) = document_value else {
            return Err(SetGroupPoliciesError::GroupAuthDocNotFound);
        };
        let mut document = GroupAuthorizationDocument::from_bytes(&document_value)?;

        if let Some(expected) = self.config.expected_hash
            && policy_set_hash(&document.policies) != expected
        {
            return Err(SetGroupPoliciesError::StaleHash);
        }

        let target = self.admin_target();
        let previous_reducer_state = reducer_state_value
            .as_ref()
            .map(|value| {
                aruna_core::admin_document_reducer::decode_admin_document_reducer_state(
                    value.as_ref(),
                )
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
            AdminDocumentOperation::GroupPoliciesSet {
                policies: self.config.policies.clone(),
            },
        )?;
        apply_reducer_policies(&mut document, &reducer_state);

        let stale_conflict_deletes = stale_admin_document_conflict_delete_entries(
            previous_reducer_state.as_ref(),
            Some(&reducer_state),
        );
        let document_target = self.document_ref();
        let placement = realm_config_value
            .as_deref()
            .map(RealmConfigDocument::from_bytes)
            .transpose()?
            .map(|config| placement_ref_for_target(&config, &document_target, Default::default()))
            .unwrap_or(PlacementRef::NIL);
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
            placement,
            false,
        );
        writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        writes.extend(admin_document_conflict_write_entries(&reducer_state)?);

        self.output = Some(Ok(document.clone()));
        self.state = SetGroupPoliciesState::WriteDocumentAndAdminState {
            document,
            stale_conflict_deletes,
        };

        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn emit_commit_transaction(&mut self, document: GroupAuthorizationDocument) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(SetGroupPoliciesError::MissingTransaction);
        };
        self.state = SetGroupPoliciesState::CommitTransaction { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: SetGroupPoliciesError) -> Effects {
        let cleanup = self.abort();
        self.state = SetGroupPoliciesState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(SetGroupPoliciesError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for SetGroupPoliciesOperation {
    type Output = GroupAuthorizationDocument;
    type Error = SetGroupPoliciesError;

    fn start(&mut self) -> Effects {
        self.state = SetGroupPoliciesState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            SetGroupPoliciesState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            SetGroupPoliciesState::ReadCurrent => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [
                        (_, document_value),
                        (_, reducer_state_value),
                        (_, realm_config_value),
                    ] = values.as_slice()
                    else {
                        return self.unexpected_event(
                            "storage batch read result with group auth doc, admin state, and realm config",
                            format!("{values:?}"),
                        );
                    };
                    match self.emit_write_document_and_admin_state(
                        document_value.clone(),
                        reducer_state_value.clone(),
                        realm_config_value.clone(),
                    ) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch read result", format!("{other:?}")),
            },
            SetGroupPoliciesState::WriteDocumentAndAdminState {
                document,
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(SetGroupPoliciesError::MissingTransaction);
                    };
                    if !stale_conflict_deletes.is_empty() {
                        self.state = SetGroupPoliciesState::DeleteStaleAdminConflicts { document };
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
            SetGroupPoliciesState::DeleteStaleAdminConflicts { document } => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    self.emit_commit_transaction(document)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            SetGroupPoliciesState::CommitTransaction { document } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state =
                        SetGroupPoliciesState::ScheduleDocumentSyncOutboxDrain { document };
                    smallvec![schedule_outbox_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            SetGroupPoliciesState::ScheduleDocumentSyncOutboxDrain { .. } => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = SetGroupPoliciesState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document operation outbox drain; durable outbox remains retryable");
                    self.state = SetGroupPoliciesState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            SetGroupPoliciesState::Finish
            | SetGroupPoliciesState::Error
            | SetGroupPoliciesState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            SetGroupPoliciesState::Finish | SetGroupPoliciesState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .expect("set group policies operation must set output")
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

/// Overlays the reducer's materialized policy set onto the group auth document,
/// mirroring the replicated materialization in `net::irokle`.
fn apply_reducer_policies(
    document: &mut GroupAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
) {
    if !reducer_state.conflicts.contains_key(GROUP_POLICIES_PATH)
        && let Some(policies) = reducer_state.materialized_group_policies()
    {
        document.policies = policies;
    }
}

#[cfg(test)]
mod tests {
    use super::{SetGroupPoliciesConfig, SetGroupPoliciesError, SetGroupPoliciesOperation};
    use crate::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::driver::{DriverContext, drive};
    use crate::get_group::{GetGroupConfig, GetGroupOperation};
    use aruna_core::UserId;
    use aruna_core::request_policy::RequestPolicy;
    use aruna_core::structs::{Actor, RealmId};
    use aruna_storage::storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn policy(expression: &str) -> RequestPolicy {
        RequestPolicy {
            policy_id: Ulid::from_bytes([9u8; 16]),
            name: "no-writes".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: expression.to_string(),
            enabled: true,
        }
    }

    async fn setup_group() -> (tempfile::TempDir, DriverContext, Actor, Ulid) {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        };
        let realm_id = RealmId([23u8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([4u8; 16]), realm_id),
            realm_id,
        };
        let (group, _) = drive(
            CreateGroupOperation::new(CreateGroupConfig {
                actor: actor.clone(),
                display_name: "policy group".to_string(),
                owner_cap: None,
            }),
            &context,
        )
        .await
        .unwrap();
        (dir, context, actor, group.group_id)
    }

    #[tokio::test]
    async fn stores_group_policies() {
        // The set lands on the group auth doc and a later read returns it.
        let (_dir, context, actor, group_id) = setup_group().await;
        let policies = vec![policy("permission == 'write'")];
        let document = drive(
            SetGroupPoliciesOperation::new(SetGroupPoliciesConfig {
                actor: actor.clone(),
                group_id,
                policies: policies.clone(),
                expected_hash: None,
            }),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(document.policies, policies);

        let (_, auth_doc) = drive(
            GetGroupOperation::new(GetGroupConfig { group_id }),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(auth_doc.policies, policies);
    }

    #[tokio::test]
    async fn stale_hash_aborts() {
        // A concurrent write moves the stored hash; the second write must abort
        // on its stale expected_hash and leave the first set in place.
        use aruna_core::request_policy::policy_set_hash;
        let (_dir, context, actor, group_id) = setup_group().await;
        let empty_hash = policy_set_hash(&[]);
        let first = vec![policy("permission == 'write'")];
        drive(
            SetGroupPoliciesOperation::new(SetGroupPoliciesConfig {
                actor: actor.clone(),
                group_id,
                policies: first.clone(),
                expected_hash: Some(empty_hash),
            }),
            &context,
        )
        .await
        .unwrap();

        let stale = drive(
            SetGroupPoliciesOperation::new(SetGroupPoliciesConfig {
                actor: actor.clone(),
                group_id,
                policies: vec![policy("permission == 'read'")],
                expected_hash: Some(empty_hash),
            }),
            &context,
        )
        .await;
        assert!(matches!(stale, Err(SetGroupPoliciesError::StaleHash)));

        let (_, auth_doc) = drive(
            GetGroupOperation::new(GetGroupConfig { group_id }),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(auth_doc.policies, first);
    }

    #[tokio::test]
    async fn rejects_invalid_set() {
        // An uncompilable expression is refused at administration time.
        let (_dir, context, actor, group_id) = setup_group().await;
        let result = drive(
            SetGroupPoliciesOperation::new(SetGroupPoliciesConfig {
                actor,
                group_id,
                policies: vec![policy("path.startsWith(")],
                expected_hash: None,
            }),
            &context,
        )
        .await;
        assert!(matches!(
            result,
            Err(SetGroupPoliciesError::InvalidPolicies { .. })
        ));
    }
}
