//! Realm-admin replacement of the compute configuration.
//!
//! The directed location links the planner estimates transfers with and the
//! standing group compute quotas are replaced wholesale, through the same
//! admin-document path every other realm-config mutation uses, so a concurrent
//! change converges instead of one writer silently winning.

use aruna_core::admin_document_reducer::{
    AdminDocumentReducerError, AdminDocumentReducerState, REALM_CONFIG_COMPUTE_PATH,
};
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::ADMIN_DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, stale_admin_document_conflict_delete_entries,
};
use aruna_core::structs::{
    Actor, AuthContext, Permission, RealmComputeConfig, RealmConfigDocument, policy_admin_path,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::mutate_realm_placement::is_management;
use crate::placement::placement_ref_for_target;

#[derive(Debug, Clone, PartialEq)]
pub struct SetRealmComputeConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted credential stays
    /// restricted; it is never derived from `actor`.
    pub auth_context: AuthContext,
    /// The complete compute configuration; it replaces the stored one.
    pub compute: RealmComputeConfig,
}

#[derive(Debug, PartialEq)]
pub struct SetRealmComputeOperation {
    config: SetRealmComputeConfig,
    txn_id: Option<TxnId>,
    state: SetRealmComputeState,
    output: Option<Result<RealmConfigDocument, SetRealmComputeError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum SetRealmComputeState {
    Init,
    Auth,
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
pub enum SetRealmComputeError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentReducerError(#[from] AdminDocumentReducerError),
    #[error("realm config document missing")]
    RealmConfigNotFound,
    #[error("caller may not write the realm configuration")]
    Unauthorized,
    #[error("this node is not a realm management node")]
    NotManagementNode,
    #[error("invalid compute configuration: {reason}")]
    InvalidCompute { reason: String },
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl SetRealmComputeOperation {
    pub fn new(config: SetRealmComputeConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: SetRealmComputeState::Init,
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
        self.state = SetRealmComputeState::ReadCurrent;
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
    ) -> Result<Effects, SetRealmComputeError> {
        let Some(txn_id) = self.txn_id else {
            return Err(SetRealmComputeError::MissingTransaction);
        };
        self.config
            .compute
            .validate()
            .map_err(|error| SetRealmComputeError::InvalidCompute {
                reason: error.to_string(),
            })?;

        let Some(document_value) = document_value else {
            return Err(SetRealmComputeError::RealmConfigNotFound);
        };
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;
        if !is_management(&document, self.config.actor.node_id) {
            return Err(SetRealmComputeError::NotManagementNode);
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
            AdminDocumentOperation::RealmConfigComputeSet {
                compute: self.config.compute.clone(),
            },
        )?;
        // Derive the stored configuration from the reducer's materialized state
        // so the local write agrees with the replicated overlay: a conflicted
        // compute path leaves the previously agreed configuration in place.
        apply_reducer_compute(&mut document, &reducer_state);

        let stale_conflict_deletes = stale_admin_document_conflict_delete_entries(
            previous_reducer_state.as_ref(),
            Some(&reducer_state),
        );
        let document_target = self.document_ref();
        let placement = placement_ref_for_target(&document, &document_target, Default::default());
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
        self.state = SetRealmComputeState::WriteDocumentAndAdminState {
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
            return self.fail(SetRealmComputeError::MissingTransaction);
        };
        self.state = SetRealmComputeState::CommitTransaction { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: SetRealmComputeError) -> Effects {
        let cleanup = self.abort();
        self.state = SetRealmComputeState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(SetRealmComputeError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for SetRealmComputeOperation {
    type Output = RealmConfigDocument;
    type Error = SetRealmComputeError;

    fn start(&mut self) -> Effects {
        if self.config.auth_context.realm_id != self.config.actor.realm_id {
            return self.fail(SetRealmComputeError::Unauthorized);
        }
        self.state = SetRealmComputeState::Auth;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: policy_admin_path(self.config.actor.realm_id),
                required_permission: Permission::WRITE,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            SetRealmComputeState::Auth => match event {
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) => {
                    match allowed {
                        Ok(true) => {
                            self.state = SetRealmComputeState::StartTransaction;
                            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                                read: false
                            })]
                        }
                        Ok(false) => self.fail(SetRealmComputeError::Unauthorized),
                        Err(error) => {
                            warn!(error = %error, "Realm compute authorization check failed");
                            self.fail(SetRealmComputeError::Unauthorized)
                        }
                    }
                }
                other => self.unexpected_event("authorization result", format!("{other:?}")),
            },
            SetRealmComputeState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            SetRealmComputeState::ReadCurrent => match event {
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
            SetRealmComputeState::WriteDocumentAndAdminState {
                document,
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(SetRealmComputeError::MissingTransaction);
                    };
                    if !stale_conflict_deletes.is_empty() {
                        self.state = SetRealmComputeState::DeleteStaleAdminConflicts { document };
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
            SetRealmComputeState::DeleteStaleAdminConflicts { document } => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    self.emit_commit_transaction(document)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            SetRealmComputeState::CommitTransaction { document } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = SetRealmComputeState::ScheduleDocumentSyncOutboxDrain { document };
                    smallvec![schedule_outbox_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            SetRealmComputeState::ScheduleDocumentSyncOutboxDrain { .. } => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = SetRealmComputeState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document operation outbox drain; durable outbox remains retryable");
                    self.state = SetRealmComputeState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            SetRealmComputeState::Finish
            | SetRealmComputeState::Error
            | SetRealmComputeState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            SetRealmComputeState::Finish | SetRealmComputeState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(SetRealmComputeError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

/// Overlays the reducer's materialized compute configuration onto the document,
/// mirroring the replicated materialization in `net::irokle`.
fn apply_reducer_compute(
    document: &mut RealmConfigDocument,
    reducer_state: &AdminDocumentReducerState,
) {
    if !reducer_state
        .conflicts
        .contains_key(REALM_CONFIG_COMPUTE_PATH)
        && let Some(compute) = reducer_state.materialized_realm_compute()
    {
        document.compute = compute;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::get_realm_config::GetRealmConfigOperation;
    use aruna_core::compute_quota::ComputeQuota;
    use aruna_core::document::DocumentSyncTarget;
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::structs::{
        GroupComputeQuota, LocationLink, RealmAuthorizationDocument, RealmId, RealmNodeKind,
    };
    use aruna_core::types::UserId;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn context(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    fn actor(realm_id: RealmId) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([1u8; 16]), realm_id),
            realm_id,
        }
    }

    fn auth(actor: &Actor) -> AuthContext {
        AuthContext {
            user_id: actor.user_id,
            realm_id: actor.realm_id,
            path_restrictions: None,
        }
    }

    fn compute_config(actor: &Actor, compute: RealmComputeConfig) -> SetRealmComputeConfig {
        SetRealmComputeConfig {
            actor: actor.clone(),
            auth_context: auth(actor),
            compute,
        }
    }

    /// The realm admin role only grants what the operation checks, so the
    /// permission sub-operation decides on stored rules like production does.
    async fn seed_realm_admin(ctx: &DriverContext, actor: &Actor) {
        let mut document = RealmAuthorizationDocument::new_default_realm_doc(actor.realm_id);
        for role in document.roles.values_mut() {
            role.assigned_users.insert(actor.user_id);
        }
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: AUTH_KEYSPACE.to_string(),
                key: (*actor.realm_id.as_bytes()).into(),
                value: document.to_bytes(actor).unwrap().into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected event: {other:?}"),
        }
    }

    fn management_config(realm_id: RealmId, actor: &Actor) -> RealmConfigDocument {
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.ensure_node(actor.node_id, RealmNodeKind::Management);
        document
    }

    async fn seed(ctx: &DriverContext, actor: &Actor, document: &RealmConfigDocument) {
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

    fn configured() -> RealmComputeConfig {
        RealmComputeConfig {
            links: vec![LocationLink {
                from: "eu-west".to_string(),
                to: "us-east".to_string(),
                bandwidth_bytes_per_sec: 125_000_000,
            }],
            group_quotas: vec![GroupComputeQuota {
                group_id: Ulid::from_bytes([7u8; 16]),
                quota: ComputeQuota {
                    max_jobs: Some(4),
                    ..ComputeQuota::default()
                },
            }],
            ..RealmComputeConfig::default()
        }
    }

    #[tokio::test]
    async fn stores_compute_config() {
        // The mutation must survive as the document a later read resolves, or
        // the planner and the quota gate would keep deciding on stale facts.
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let actor = actor(realm_id);
        seed(&ctx, &actor, &management_config(realm_id, &actor)).await;
        seed_realm_admin(&ctx, &actor).await;

        let stored = drive(
            SetRealmComputeOperation::new(compute_config(&actor, configured())),
            &ctx,
        )
        .await
        .expect("compute configuration stores");
        assert_eq!(stored.compute, configured());

        let reread = drive(GetRealmConfigOperation::new(realm_id), &ctx)
            .await
            .expect("config reads");
        assert_eq!(reread.compute.links, configured().links);
        assert_eq!(
            reread.compute.effective_quota(&Ulid::from_bytes([7u8; 16])),
            Ok(ComputeQuota {
                max_jobs: Some(4),
                ..ComputeQuota::default()
            })
        );
    }

    #[tokio::test]
    async fn refuses_invalid_links() {
        // A zero bandwidth would make one transfer estimate infinite, so the
        // mutation is refused instead of clamped.
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let actor = actor(realm_id);
        seed(&ctx, &actor, &management_config(realm_id, &actor)).await;
        seed_realm_admin(&ctx, &actor).await;

        let invalid = RealmComputeConfig {
            links: vec![LocationLink {
                from: "eu-west".to_string(),
                to: "us-east".to_string(),
                bandwidth_bytes_per_sec: 0,
            }],
            ..RealmComputeConfig::default()
        };
        let error = drive(
            SetRealmComputeOperation::new(compute_config(&actor, invalid)),
            &ctx,
        )
        .await
        .expect_err("a zero bandwidth link is refused");
        assert!(matches!(error, SetRealmComputeError::InvalidCompute { .. }));
    }

    #[tokio::test]
    async fn refuses_unauthorized_caller() {
        // Nothing is written when the caller holds no realm-config write.
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let actor = actor(realm_id);
        seed(&ctx, &actor, &management_config(realm_id, &actor)).await;
        let mut document = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        document.roles.clear();
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: AUTH_KEYSPACE.to_string(),
                key: (*realm_id.as_bytes()).into(),
                value: document.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected event: {other:?}"),
        }

        let error = drive(
            SetRealmComputeOperation::new(compute_config(&actor, configured())),
            &ctx,
        )
        .await
        .expect_err("an unauthorized caller is refused");
        assert_eq!(error, SetRealmComputeError::Unauthorized);

        let reread = drive(GetRealmConfigOperation::new(realm_id), &ctx)
            .await
            .expect("config reads");
        assert_eq!(reread.compute, RealmComputeConfig::default());
    }

    #[tokio::test]
    async fn refuses_server_node() {
        // A realm admin token still may not write the config on a server node:
        // peers reject a realm-config event whose origin is not management.
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let actor = actor(realm_id);
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.ensure_node(actor.node_id, RealmNodeKind::Server);
        seed(&ctx, &actor, &document).await;
        seed_realm_admin(&ctx, &actor).await;

        let error = drive(
            SetRealmComputeOperation::new(compute_config(&actor, configured())),
            &ctx,
        )
        .await
        .expect_err("a server node is refused");
        assert_eq!(error, SetRealmComputeError::NotManagementNode);

        let reread = drive(GetRealmConfigOperation::new(realm_id), &ctx)
            .await
            .expect("config reads");
        assert_eq!(reread.compute, RealmComputeConfig::default());
    }

    #[test]
    fn start_checks_permission() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let actor = actor(realm_id);
        let mut operation =
            SetRealmComputeOperation::new(compute_config(&actor, RealmComputeConfig::default()));
        let effects = operation.start();
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        let emitted = format!("{effects:?}");
        assert!(emitted.contains(&policy_admin_path(realm_id)));
        assert!(emitted.contains("WRITE"));
    }

    #[test]
    fn denied_is_terminal() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let actor = actor(realm_id);
        let mut operation =
            SetRealmComputeOperation::new(compute_config(&actor, RealmComputeConfig::default()));
        operation.start();
        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(false) },
        ));
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize(),
            Err(SetRealmComputeError::Unauthorized)
        );
    }

    #[test]
    fn allowed_starts_transaction() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let actor = actor(realm_id);
        let mut operation =
            SetRealmComputeOperation::new(compute_config(&actor, RealmComputeConfig::default()));
        operation.start();
        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));
        assert_eq!(
            effects.as_slice(),
            &[Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        );
    }
}
