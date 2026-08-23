use aruna_core::admin_document_reducer::{
    AdminDocumentReducerError, AdminDocumentReducerState, REALM_CONFIG_POLICIES_PATH,
};
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::ADMIN_DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::request_policy::{RequestPolicy, policy_set_hash, validate_policy_set};
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, stale_admin_document_conflict_delete_entries,
};
use aruna_core::structs::{Actor, AuthContext, Permission, RealmConfigDocument, policy_admin_path};
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
pub struct SetRealmPoliciesConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted credential stays
    /// restricted; it is never derived from `actor`.
    pub auth_context: AuthContext,
    pub policies: Vec<RequestPolicy>,
    /// When set, the write applies only if the stored set still hashes to it,
    /// compared inside the write transaction to close the check/use window.
    pub expected_hash: Option<[u8; 32]>,
}

/// Replaces the realm's deny-only request policy set. Rides the same admin
/// document machinery as the other realm config settings, so the set
/// replicates realm-wide and merges last-writer-wins as one value.
#[derive(Debug, PartialEq)]
pub struct SetRealmPoliciesOperation {
    config: SetRealmPoliciesConfig,
    txn_id: Option<TxnId>,
    state: SetRealmPoliciesState,
    output: Option<Result<RealmConfigDocument, SetRealmPoliciesError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum SetRealmPoliciesState {
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
pub enum SetRealmPoliciesError {
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
    #[error("stored policy set changed")]
    StaleHash,
    #[error("invalid policy set: {reason}")]
    InvalidPolicies { reason: String },
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

impl SetRealmPoliciesOperation {
    pub fn new(config: SetRealmPoliciesConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: SetRealmPoliciesState::Init,
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
        self.state = SetRealmPoliciesState::ReadCurrent;
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

    fn emit_write_state(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<Effects, SetRealmPoliciesError> {
        let Some(txn_id) = self.txn_id else {
            return Err(SetRealmPoliciesError::MissingTransaction);
        };
        validate_policy_set(&self.config.policies)
            .map_err(|reason| SetRealmPoliciesError::InvalidPolicies { reason })?;

        let Some(document_value) = document_value else {
            return Err(SetRealmPoliciesError::RealmConfigNotFound);
        };
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;
        if !is_management(&document, self.config.actor.node_id) {
            return Err(SetRealmPoliciesError::NotManagementNode);
        }

        if let Some(expected) = self.config.expected_hash
            && policy_set_hash(&document.request_policies) != expected
        {
            return Err(SetRealmPoliciesError::StaleHash);
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
            AdminDocumentOperation::RealmConfigPoliciesSet {
                policies: self.config.policies.clone(),
            },
        )?;
        // Mirror the replicated overlay: a conflicted policies path leaves the
        // last agreed set in place instead of clobbering it with the input.
        apply_reducer_policies(&mut document, &reducer_state);

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
        self.state = SetRealmPoliciesState::WriteDocumentAndAdminState {
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
            return self.fail(SetRealmPoliciesError::MissingTransaction);
        };
        self.state = SetRealmPoliciesState::CommitTransaction { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: SetRealmPoliciesError) -> Effects {
        let cleanup = self.abort();
        self.state = SetRealmPoliciesState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(SetRealmPoliciesError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for SetRealmPoliciesOperation {
    type Output = RealmConfigDocument;
    type Error = SetRealmPoliciesError;

    fn start(&mut self) -> Effects {
        if self.config.auth_context.realm_id != self.config.actor.realm_id {
            return self.fail(SetRealmPoliciesError::Unauthorized);
        }
        self.state = SetRealmPoliciesState::Auth;
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
            SetRealmPoliciesState::Auth => match event {
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) => {
                    match allowed {
                        Ok(true) => {
                            self.state = SetRealmPoliciesState::StartTransaction;
                            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                                read: false
                            })]
                        }
                        Ok(false) => self.fail(SetRealmPoliciesError::Unauthorized),
                        Err(error) => {
                            warn!(error = %error, "Realm policy authorization check failed");
                            self.fail(SetRealmPoliciesError::Unauthorized)
                        }
                    }
                }
                other => self.unexpected_event("authorization result", format!("{other:?}")),
            },
            SetRealmPoliciesState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            SetRealmPoliciesState::ReadCurrent => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, document_value), (_, reducer_state_value)] = values.as_slice() else {
                        return self.unexpected_event(
                            "storage batch read result with realm config and reducer state",
                            format!("{values:?}"),
                        );
                    };
                    match self.emit_write_state(document_value.clone(), reducer_state_value.clone())
                    {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch read result", format!("{other:?}")),
            },
            SetRealmPoliciesState::WriteDocumentAndAdminState {
                document,
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(SetRealmPoliciesError::MissingTransaction);
                    };
                    if !stale_conflict_deletes.is_empty() {
                        self.state = SetRealmPoliciesState::DeleteStaleAdminConflicts { document };
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
            SetRealmPoliciesState::DeleteStaleAdminConflicts { document } => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    self.emit_commit_transaction(document)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            SetRealmPoliciesState::CommitTransaction { document } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state =
                        SetRealmPoliciesState::ScheduleDocumentSyncOutboxDrain { document };
                    smallvec![schedule_outbox_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            SetRealmPoliciesState::ScheduleDocumentSyncOutboxDrain { .. } => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = SetRealmPoliciesState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document operation outbox drain; durable outbox remains retryable");
                    self.state = SetRealmPoliciesState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            SetRealmPoliciesState::Finish
            | SetRealmPoliciesState::Error
            | SetRealmPoliciesState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            SetRealmPoliciesState::Finish | SetRealmPoliciesState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(SetRealmPoliciesError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

/// Overlays the reducer's materialized policy set onto the config document,
/// mirroring the replicated materialization in `net::irokle`.
fn apply_reducer_policies(
    document: &mut RealmConfigDocument,
    reducer_state: &AdminDocumentReducerState,
) {
    if !reducer_state
        .conflicts
        .contains_key(REALM_CONFIG_POLICIES_PATH)
        && let Some(policies) = reducer_state.materialized_realm_policies()
    {
        document.request_policies = policies;
    }
}

#[cfg(test)]
mod tests {
    use super::{SetRealmPoliciesConfig, SetRealmPoliciesError, SetRealmPoliciesOperation};
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};
    use crate::get_realm_config::GetRealmConfigOperation;
    use aruna_core::UserId;
    use aruna_core::document::DocumentSyncTarget;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::request_policy::RequestPolicy;
    use aruna_core::structs::{
        Actor, AuthContext, RealmAuthorizationDocument, RealmConfigDocument, RealmId,
        RealmNodeKind, policy_admin_path,
    };
    use aruna_storage::storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn actor(realm_id: RealmId) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([4u8; 16]), realm_id),
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

    fn policies_config(
        actor: &Actor,
        policies: Vec<RequestPolicy>,
        expected_hash: Option<[u8; 32]>,
    ) -> SetRealmPoliciesConfig {
        SetRealmPoliciesConfig {
            actor: actor.clone(),
            auth_context: auth(actor),
            policies,
            expected_hash,
        }
    }

    /// Realm creation leaves the admin role unassigned, so the fixture claims it
    /// for the actor the permission sub-operation then decides on.
    async fn seed_realm_admin(context: &DriverContext, actor: &Actor) {
        let mut document = RealmAuthorizationDocument::new_default_realm_doc(actor.realm_id);
        for role in document.roles.values_mut() {
            role.assigned_users.insert(actor.user_id);
        }
        match context
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

    fn policy(expression: &str) -> RequestPolicy {
        RequestPolicy {
            policy_id: Ulid::from_bytes([7u8; 16]),
            name: "no-writes".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: expression.to_string(),
            enabled: true,
        }
    }

    async fn setup_realm() -> (tempfile::TempDir, DriverContext, Actor) {
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
        let realm_id = RealmId([21u8; 32]);
        let actor = actor(realm_id);
        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: actor.clone(),
                realm_description: "policy realm".to_string(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            &context,
        )
        .await
        .unwrap();
        seed_realm_admin(&context, &actor).await;
        (dir, context, actor)
    }

    /// Replaces the stored realm config with one that ranks the actor's node as
    /// a plain server, which realm creation never produces.
    async fn seed_server_config(context: &DriverContext, actor: &Actor) {
        let mut document = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
        document.ensure_node(actor.node_id, RealmNodeKind::Server);
        let target = DocumentSyncTarget::RealmConfig {
            realm_id: actor.realm_id,
        };
        match context
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

    #[tokio::test]
    async fn refuses_server_node() {
        // A realm admin token still may not write the config on a server node:
        // peers reject a realm-config event whose origin is not management.
        let (_dir, context, actor) = setup_realm().await;
        seed_server_config(&context, &actor).await;

        let error = drive(
            SetRealmPoliciesOperation::new(policies_config(
                &actor,
                vec![policy("permission == 'write'")],
                None,
            )),
            &context,
        )
        .await
        .expect_err("a server node is refused");
        assert!(matches!(error, SetRealmPoliciesError::NotManagementNode));

        let read = drive(GetRealmConfigOperation::new(actor.realm_id), &context)
            .await
            .unwrap();
        assert!(read.request_policies.is_empty());
    }

    #[tokio::test]
    async fn stores_policy_set() {
        // The set lands on the stored realm config and a later read returns it.
        let (_dir, context, actor) = setup_realm().await;
        let policies = vec![policy("permission == 'write'")];
        let document = drive(
            SetRealmPoliciesOperation::new(policies_config(&actor, policies.clone(), None)),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(document.request_policies, policies);

        let read = drive(GetRealmConfigOperation::new(actor.realm_id), &context)
            .await
            .unwrap();
        assert_eq!(read.request_policies, policies);
    }

    #[tokio::test]
    async fn stale_hash_aborts() {
        // A concurrent write moves the stored hash; the second write must abort
        // on its stale expected_hash and leave the first set in place.
        use aruna_core::request_policy::policy_set_hash;
        let (_dir, context, actor) = setup_realm().await;
        let empty_hash = policy_set_hash(&[]);
        let first = vec![policy("permission == 'write'")];
        drive(
            SetRealmPoliciesOperation::new(policies_config(
                &actor,
                first.clone(),
                Some(empty_hash),
            )),
            &context,
        )
        .await
        .unwrap();

        let stale = drive(
            SetRealmPoliciesOperation::new(policies_config(
                &actor,
                vec![policy("permission == 'read'")],
                Some(empty_hash),
            )),
            &context,
        )
        .await;
        assert!(matches!(stale, Err(SetRealmPoliciesError::StaleHash)));

        let read = drive(GetRealmConfigOperation::new(actor.realm_id), &context)
            .await
            .unwrap();
        assert_eq!(read.request_policies, first);
    }

    #[tokio::test]
    async fn rejects_invalid_set() {
        // An uncompilable expression is refused at administration time.
        let (_dir, context, actor) = setup_realm().await;
        let result = drive(
            SetRealmPoliciesOperation::new(policies_config(
                &actor,
                vec![policy("path.startsWith(")],
                None,
            )),
            &context,
        )
        .await;
        assert!(matches!(
            result,
            Err(SetRealmPoliciesError::InvalidPolicies { .. })
        ));
    }

    #[tokio::test]
    async fn refuses_unauthorized_caller() {
        // A realm member without the admin role writes nothing.
        let (_dir, context, actor) = setup_realm().await;
        let outsider = Actor {
            user_id: UserId::local(Ulid::from_bytes([9u8; 16]), actor.realm_id),
            ..actor.clone()
        };
        let error = drive(
            SetRealmPoliciesOperation::new(policies_config(
                &outsider,
                vec![policy("permission == 'write'")],
                None,
            )),
            &context,
        )
        .await
        .expect_err("an unauthorized caller is refused");
        assert_eq!(error, SetRealmPoliciesError::Unauthorized);

        let read = drive(GetRealmConfigOperation::new(actor.realm_id), &context)
            .await
            .unwrap();
        assert!(read.request_policies.is_empty());
    }

    #[test]
    fn start_checks_permission() {
        let realm_id = RealmId([21u8; 32]);
        let actor = actor(realm_id);
        let mut operation =
            SetRealmPoliciesOperation::new(policies_config(&actor, Vec::new(), None));
        let effects = operation.start();
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        let emitted = format!("{effects:?}");
        assert!(emitted.contains(&policy_admin_path(realm_id)));
        assert!(emitted.contains("WRITE"));
    }

    #[test]
    fn denied_is_terminal() {
        let realm_id = RealmId([21u8; 32]);
        let actor = actor(realm_id);
        let mut operation =
            SetRealmPoliciesOperation::new(policies_config(&actor, Vec::new(), None));
        operation.start();
        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(false) },
        ));
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize(),
            Err(SetRealmPoliciesError::Unauthorized)
        );
    }

    #[test]
    fn allowed_starts_transaction() {
        let realm_id = RealmId([21u8; 32]);
        let actor = actor(realm_id);
        let mut operation =
            SetRealmPoliciesOperation::new(policies_config(&actor, Vec::new(), None));
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
