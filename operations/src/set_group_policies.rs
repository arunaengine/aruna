use aruna_core::admin_document_reducer::{
    AdminDocumentReducerError, AdminDocumentReducerState, GROUP_POLICIES_PATH,
};
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{ADMIN_DOCUMENT_STATE_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::request_policy::{RequestPolicy, policy_set_hash, validate_policy_set};
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, stale_admin_document_conflict_delete_entries,
};
use aruna_core::structs::{
    Actor, AuthContext, GroupAuthorizationDocument, Permission, PlacementRef, RealmConfigDocument,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, GroupId, Key, KeySpace, TxnId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::placement::placement_ref_for_target;

#[derive(Debug, Clone, PartialEq)]
pub struct SetGroupPoliciesConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted credential stays
    /// restricted; it is never derived from `actor`.
    pub auth_context: AuthContext,
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
    /// Bucket the authorization row publishes onto, read inside the write
    /// transaction.
    fence: crate::placement::fence::WriteFence,
    state: SetGroupPoliciesState,
    output: Option<Result<GroupAuthorizationDocument, SetGroupPoliciesError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum SetGroupPoliciesState {
    Init,
    Auth,
    StartTransaction,
    ReadCurrent,
    WriteDocumentAndAdminState {
        document: GroupAuthorizationDocument,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    },
    DeleteStaleAdminConflicts {
        document: GroupAuthorizationDocument,
    },
    ReadBucketFence {
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
    #[error("caller may not write the group configuration")]
    Unauthorized,
    #[error("stored policy set changed")]
    StaleHash,
    #[error("invalid policy set: {reason}")]
    InvalidPolicies { reason: String },
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("the group's bucket cut over to a new holder set; retry the change")]
    PlacementFenced,
    #[error("operation did not finish")]
    NotFinished,
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
            fence: Default::default(),
            state: SetGroupPoliciesState::Init,
            output: None,
        }
    }

    fn document_ref(&self) -> DocumentSyncTarget {
        DocumentSyncTarget::GroupAuthorization {
            group_id: self.config.group_id,
        }
    }

    fn admin_config_path(&self) -> String {
        format!(
            "/{}/g/{}/admin/config",
            self.config.actor.realm_id, self.config.group_id
        )
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

    fn emit_write_state(
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
        let realm_config = realm_config_value
            .as_deref()
            .map(RealmConfigDocument::from_bytes)
            .transpose()?;
        let placement = realm_config
            .as_ref()
            .map(|config| placement_ref_for_target(config, &document_target, Default::default()))
            .unwrap_or(PlacementRef::NIL);
        let realm_id = self.config.actor.realm_id;
        if let Some(config) = realm_config.as_ref() {
            self.fence.add(realm_id, config, [placement]);
        }
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
        )
        .fenced_at(self.fence.generation(&realm_id, &placement));
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

    /// Takes the bucket's fence inside the transaction before committing, so a
    /// departing holder's close rejects or conflicts this write.
    fn emit_commit_transaction(&mut self, document: GroupAuthorizationDocument) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(SetGroupPoliciesError::MissingTransaction);
        };
        if self.fence.is_empty() {
            return self.emit_commit(document);
        }
        self.state = SetGroupPoliciesState::ReadBucketFence { document };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: self.fence.reads(),
            txn_id: Some(txn_id),
        })]
    }

    fn emit_commit(&mut self, document: GroupAuthorizationDocument) -> Effects {
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
        if self.config.auth_context.realm_id != self.config.actor.realm_id {
            return self.fail(SetGroupPoliciesError::Unauthorized);
        }
        self.state = SetGroupPoliciesState::Auth;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: self.admin_config_path(),
                required_permission: Permission::WRITE,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            SetGroupPoliciesState::Auth => match event {
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) => {
                    match allowed {
                        Ok(true) => {
                            self.state = SetGroupPoliciesState::StartTransaction;
                            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                                read: false
                            })]
                        }
                        Ok(false) => self.fail(SetGroupPoliciesError::Unauthorized),
                        Err(error) => {
                            warn!(error = %error, "Group policy authorization check failed");
                            match error {
                                AuthorizationError::StorageError(error) => {
                                    self.fail(SetGroupPoliciesError::StorageError(error))
                                }
                                _ => self.fail(SetGroupPoliciesError::Unauthorized),
                            }
                        }
                    }
                }
                other => self.unexpected_event("authorization result", format!("{other:?}")),
            },
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
                    match self.emit_write_state(
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
            SetGroupPoliciesState::ReadBucketFence { document } => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    if self.fence.admits(&values) {
                        self.emit_commit(document)
                    } else {
                        self.fail(SetGroupPoliciesError::PlacementFenced)
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("bucket fence read result", format!("{other:?}")),
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
            .unwrap_or(Err(SetGroupPoliciesError::NotFinished))
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
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::request_policy::RequestPolicy;
    use aruna_core::structs::{Actor, AuthContext, RealmAuthorizationDocument, RealmId};
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

    fn group_config(
        actor: &Actor,
        group_id: Ulid,
        policies: Vec<RequestPolicy>,
        expected_hash: Option<[u8; 32]>,
    ) -> SetGroupPoliciesConfig {
        SetGroupPoliciesConfig {
            actor: actor.clone(),
            auth_context: auth(actor),
            group_id,
            policies,
            expected_hash,
        }
    }

    /// The group creator holds the group admin role, but the permission
    /// sub-operation also needs the realm authorization document to exist.
    async fn seed_realm_doc(context: &DriverContext, actor: &Actor) {
        let document = RealmAuthorizationDocument::new_default_realm_doc(actor.realm_id);
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
        let actor = actor(realm_id);
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
        seed_realm_doc(&context, &actor).await;
        (dir, context, actor, group.group_id)
    }

    #[tokio::test]
    async fn stores_group_policies() {
        // The set lands on the group auth doc and a later read returns it.
        let (_dir, context, actor, group_id) = setup_group().await;
        let policies = vec![policy("permission == 'write'")];
        let document = drive(
            SetGroupPoliciesOperation::new(group_config(&actor, group_id, policies.clone(), None)),
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
            SetGroupPoliciesOperation::new(group_config(
                &actor,
                group_id,
                first.clone(),
                Some(empty_hash),
            )),
            &context,
        )
        .await
        .unwrap();

        let stale = drive(
            SetGroupPoliciesOperation::new(group_config(
                &actor,
                group_id,
                vec![policy("permission == 'read'")],
                Some(empty_hash),
            )),
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
            SetGroupPoliciesOperation::new(group_config(
                &actor,
                group_id,
                vec![policy("path.startsWith(")],
                None,
            )),
            &context,
        )
        .await;
        assert!(matches!(
            result,
            Err(SetGroupPoliciesError::InvalidPolicies { .. })
        ));
    }

    #[tokio::test]
    async fn refuses_unauthorized_caller() {
        // A realm member outside the group's admin role writes nothing.
        let (_dir, context, actor, group_id) = setup_group().await;
        let outsider = Actor {
            user_id: UserId::local(Ulid::from_bytes([8u8; 16]), actor.realm_id),
            ..actor.clone()
        };
        let error = drive(
            SetGroupPoliciesOperation::new(group_config(
                &outsider,
                group_id,
                vec![policy("permission == 'write'")],
                None,
            )),
            &context,
        )
        .await
        .expect_err("an unauthorized caller is refused");
        assert_eq!(error, SetGroupPoliciesError::Unauthorized);

        let (_, auth_doc) = drive(
            GetGroupOperation::new(GetGroupConfig { group_id }),
            &context,
        )
        .await
        .unwrap();
        assert!(auth_doc.policies.is_empty());
    }

    #[test]
    fn start_checks_permission() {
        let realm_id = RealmId([23u8; 32]);
        let group_id = Ulid::from_bytes([5u8; 16]);
        let actor = actor(realm_id);
        let mut operation =
            SetGroupPoliciesOperation::new(group_config(&actor, group_id, Vec::new(), None));
        let effects = operation.start();
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        let emitted = format!("{effects:?}");
        assert!(emitted.contains(&format!("/{realm_id}/g/{group_id}/admin/config")));
        assert!(emitted.contains("WRITE"));
    }

    #[test]
    fn denied_is_terminal() {
        let realm_id = RealmId([23u8; 32]);
        let actor = actor(realm_id);
        let mut operation = SetGroupPoliciesOperation::new(group_config(
            &actor,
            Ulid::from_bytes([5u8; 16]),
            Vec::new(),
            None,
        ));
        operation.start();
        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(false) },
        ));
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize(),
            Err(SetGroupPoliciesError::Unauthorized)
        );
    }

    #[test]
    fn allowed_starts_transaction() {
        let realm_id = RealmId([23u8; 32]);
        let actor = actor(realm_id);
        let mut operation = SetGroupPoliciesOperation::new(group_config(
            &actor,
            Ulid::from_bytes([5u8; 16]),
            Vec::new(),
            None,
        ));
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
