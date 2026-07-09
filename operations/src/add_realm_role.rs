use aruna_core::admin_document_reducer::{AdminDocumentReducerError, AdminDocumentReducerState};
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentRoleDefinition, AdminDocumentTarget,
};
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{ADMIN_DOCUMENT_STATE_KEYSPACE, AUTH_KEYSPACE};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, stale_admin_document_conflict_delete_entries,
};
use aruna_core::structs::{
    Actor, AuthContext, Permission, RealmAuthorizationDocument, RealmId, Role,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, UserId};
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::HashSet;
use thiserror::Error;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::replicate_documents::replicate_documents_effect;

#[derive(Clone, Debug, PartialEq)]
pub struct AddRealmRoleConfig {
    pub actor: Actor,
    pub realm_id: RealmId,
    pub role: Role,
}

#[derive(PartialEq)]
pub struct AddRealmRoleOperation {
    input: AddRealmRoleConfig,
    state: AddRealmRoleState,
    output: Option<Result<RealmAuthorizationDocument, AddRealmRoleError>>,
}

impl std::fmt::Debug for AddRealmRoleOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddRealmRoleOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AddRealmRoleState {
    Init,
    Auth,
    StartTransaction,
    GetAuthDocAndAdminState {
        txn_id: TxnId,
    },
    WriteRealmAuthDocAndAdminState {
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
        stale_conflict_delete_keys: Vec<(KeySpace, Vec<u8>)>,
    },
    DeleteStaleAdminConflicts {
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
    },
    CommitTransaction {
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
    },
    ScheduleAdminDocumentOutboxDrain {
        auth_doc: RealmAuthorizationDocument,
    },
    AnnounceAuthDoc {
        auth_doc: RealmAuthorizationDocument,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum AddRealmRoleError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentReducerError(#[from] AdminDocumentReducerError),
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("Unauthorized")]
    Unauthorized,
    #[error("No realm authorization document found")]
    RealmAuthDocNotFound,
    #[error(transparent)]
    CheckPermissionsError(#[from] AuthorizationError),
    #[error("Adding role to realm did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: AddRealmRoleState,
        expected: &'static str,
        got: String,
    },
}

impl AddRealmRoleOperation {
    pub fn new(input: AddRealmRoleConfig) -> Self {
        AddRealmRoleOperation {
            input,
            state: AddRealmRoleState::Init,
            output: None,
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                AddRealmRoleState::StartTransaction,
                "Event::Storage(StorageEvent::TransactionStarted)",
                got,
            );
        };
        match self.emit_get_auth_doc(txn_id) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn auth_context(&self) -> AuthContext {
        AuthContext {
            user_id: self.input.actor.user_id,
            realm_id: self.input.actor.realm_id,
            path_restrictions: None,
        }
    }

    fn handle_authorization(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return self.unexpected_event(
                AddRealmRoleState::Auth,
                "Event::SubOperation(SubOperationEvent::AuthorizationResult)",
                got,
            );
        };

        match self.emit_start_transaction(allowed) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_start_transaction(
        &mut self,
        auth_result: Result<bool, AuthorizationError>,
    ) -> Result<Effects, AddRealmRoleError> {
        if auth_result? {
            self.state = AddRealmRoleState::StartTransaction;
            Ok(smallvec![Effect::Storage(
                StorageEffect::StartTransaction { read: false }
            )])
        } else {
            Err(AddRealmRoleError::Unauthorized)
        }
    }

    fn emit_get_auth_doc(&mut self, txn_id: TxnId) -> Result<Effects, AddRealmRoleError> {
        self.state = AddRealmRoleState::GetAuthDocAndAdminState { txn_id };
        let target = AdminDocumentTarget::Realm {
            realm_id: self.input.realm_id,
        };
        let auth_key = (*self.input.realm_id.as_bytes()).into();
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (AUTH_KEYSPACE.to_string(), auth_key),
                (
                    ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                    admin_document_reducer_state_key(&target),
                ),
            ],
            txn_id: Some(txn_id),
        })])
    }

    fn handle_get_auth_doc_and_admin_state(&mut self, event: Event, txn_id: TxnId) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::BatchReadResult)",
                got,
            );
        };
        let [(_, auth_doc_value), (_, reducer_state_value)] = values.as_slice() else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::BatchReadResult) with auth doc and admin state values",
                got,
            );
        };

        match self.emit_write_realm_auth_doc_and_admin_state(
            txn_id,
            auth_doc_value.clone(),
            reducer_state_value.clone(),
        ) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_write_realm_auth_doc_and_admin_state(
        &mut self,
        txn_id: TxnId,
        auth_doc: Option<ByteView>,
        reducer_state_value: Option<ByteView>,
    ) -> Result<Effects, AddRealmRoleError> {
        let mut auth_doc = RealmAuthorizationDocument::from_bytes(
            &auth_doc.ok_or_else(|| AddRealmRoleError::RealmAuthDocNotFound)?,
        )?;
        let target = AdminDocumentTarget::Realm {
            realm_id: self.input.realm_id,
        };
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
        let admin_events = apply_admin_reducer_updates(&mut reducer_state, &self.input)?;
        materialize_realm_role(&mut auth_doc, &self.input.role, &reducer_state);

        let stale_conflict_delete_keys: Vec<_> = stale_admin_document_conflict_delete_entries(
            previous_reducer_state.as_ref(),
            Some(&reducer_state),
        )
        .into_iter()
        .map(|(key_space, key)| (key_space, key.as_ref().to_vec()))
        .collect();

        let mut writes = vec![
            (
                AUTH_KEYSPACE.to_string(),
                (*auth_doc.realm_id.as_bytes()).into(),
                auth_doc.to_bytes(&self.input.actor)?.into(),
            ),
            admin_document_reducer_state_write_entry(&reducer_state)?,
        ];
        let document_target = DocumentSyncTarget::RealmAuthorization {
            realm_id: self.input.realm_id,
        };
        for event in &admin_events {
            let record = new_outbox_record_with_id(
                event.event_id,
                self.input.actor.node_id,
                document_target.clone(),
                Vec::new(),
                DocumentSyncOutboxEvent::AdminOperation {
                    event: Box::new(event.clone()),
                },
                false,
            );
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
        writes.extend(admin_document_conflict_write_entries(&reducer_state)?);

        self.state = AddRealmRoleState::WriteRealmAuthDocAndAdminState {
            txn_id,
            auth_doc,
            admin_outbox_written: !admin_events.is_empty(),
            stale_conflict_delete_keys,
        };

        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_write_realm_auth_doc_and_admin_state(
        &mut self,
        event: Event,
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
        stale_conflict_delete_keys: Vec<(KeySpace, Vec<u8>)>,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::BatchWriteResult)",
                got,
            );
        };

        if !stale_conflict_delete_keys.is_empty() {
            self.state = AddRealmRoleState::DeleteStaleAdminConflicts {
                txn_id,
                auth_doc,
                admin_outbox_written,
            };
            let deletes = stale_conflict_delete_keys
                .into_iter()
                .map(|(key_space, key)| (key_space, Key::from(key)))
                .collect();
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes,
                txn_id: Some(txn_id),
            })];
        }

        self.emit_commit_transaction(txn_id, auth_doc, admin_outbox_written)
    }

    fn handle_delete_stale_admin_conflicts(
        &mut self,
        event: Event,
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::BatchDeleteResult)",
                got,
            );
        };

        self.emit_commit_transaction(txn_id, auth_doc, admin_outbox_written)
    }

    fn emit_commit_transaction(
        &mut self,
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
    ) -> Effects {
        self.state = AddRealmRoleState::CommitTransaction {
            txn_id,
            auth_doc,
            admin_outbox_written,
        };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit_transaction(
        &mut self,
        event: Event,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::TransactionCommitted)",
                got,
            );
        };
        if admin_outbox_written {
            self.state = AddRealmRoleState::ScheduleAdminDocumentOutboxDrain { auth_doc };
            return smallvec![schedule_outbox_drain_effect()];
        }

        self.emit_announce_auth_doc(auth_doc)
    }

    fn handle_schedule_admin_document_outbox_drain(
        &mut self,
        event: Event,
        auth_doc: RealmAuthorizationDocument,
    ) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. })
            | Event::Task(TaskEvent::Error { .. }) => {
                self.state = AddRealmRoleState::Finish;
                self.output = Some(Ok(auth_doc));
                smallvec![]
            }
            other => self.unexpected_event(
                self.state.clone(),
                "admin document outbox drain timer schedule",
                format!("{other:?}"),
            ),
        }
    }

    fn emit_announce_auth_doc(&mut self, auth_doc: RealmAuthorizationDocument) -> Effects {
        self.state = AddRealmRoleState::AnnounceAuthDoc {
            auth_doc: auth_doc.clone(),
        };
        let document = DocumentSyncTarget::RealmAuthorization {
            realm_id: auth_doc.realm_id,
        };
        smallvec![replicate_documents_effect(
            self.input.actor.realm_id,
            self.input.actor.node_id,
            vec![document],
        )]
    }

    fn handle_announce_auth_doc(
        &mut self,
        event: Event,
        auth_doc: RealmAuthorizationDocument,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::SubOperation(SubOperationEvent::DocumentSyncResult)",
                got,
            );
        };
        if let Err(error) = result {
            return self.fail(AddRealmRoleError::TopicAnnouncement(error));
        }
        self.state = AddRealmRoleState::Finish;
        self.output = Some(Ok(auth_doc));
        smallvec![]
    }

    fn fail(&mut self, err: AddRealmRoleError) -> Effects {
        self.state = AddRealmRoleState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn fail_with_cleanup(&mut self, err: AddRealmRoleError, cleanup_effects: Effects) -> Effects {
        self.state = AddRealmRoleState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: AddRealmRoleState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            AddRealmRoleError::UnexpectedEvent {
                state,
                expected,
                got,
            },
            cleanup_effects,
        )
    }

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }

        Ok(event)
    }
}

impl Operation for AddRealmRoleOperation {
    type Output = RealmAuthorizationDocument;

    type Error = AddRealmRoleError;

    fn start(&mut self) -> Effects {
        self.state = AddRealmRoleState::Auth;

        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.auth_context(),
                path: format!(
                    "/{}/admin/roles/{}",
                    self.input.realm_id, self.input.role.role_id
                ),
                required_permission: Permission::WRITE,
            }),
            |result| Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: result
            }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state.clone() {
            AddRealmRoleState::Auth => self.handle_authorization(event),
            AddRealmRoleState::StartTransaction => self.handle_start_transaction(event),
            AddRealmRoleState::GetAuthDocAndAdminState { txn_id } => {
                self.handle_get_auth_doc_and_admin_state(event, txn_id)
            }
            AddRealmRoleState::WriteRealmAuthDocAndAdminState {
                txn_id,
                auth_doc,
                admin_outbox_written,
                stale_conflict_delete_keys,
            } => self.handle_write_realm_auth_doc_and_admin_state(
                event,
                txn_id,
                auth_doc,
                admin_outbox_written,
                stale_conflict_delete_keys,
            ),
            AddRealmRoleState::DeleteStaleAdminConflicts {
                txn_id,
                auth_doc,
                admin_outbox_written,
            } => self.handle_delete_stale_admin_conflicts(
                event,
                txn_id,
                auth_doc,
                admin_outbox_written,
            ),
            AddRealmRoleState::CommitTransaction {
                auth_doc,
                admin_outbox_written,
                ..
            } => self.handle_commit_transaction(event, auth_doc, admin_outbox_written),
            AddRealmRoleState::ScheduleAdminDocumentOutboxDrain { auth_doc } => {
                self.handle_schedule_admin_document_outbox_drain(event, auth_doc)
            }
            AddRealmRoleState::AnnounceAuthDoc { auth_doc } => {
                self.handle_announce_auth_doc(event, auth_doc)
            }
            AddRealmRoleState::Init | AddRealmRoleState::Finish | AddRealmRoleState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            AddRealmRoleState::Finish | AddRealmRoleState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or_else(|| AddRealmRoleError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            AddRealmRoleState::GetAuthDocAndAdminState { txn_id }
            | AddRealmRoleState::WriteRealmAuthDocAndAdminState { txn_id, .. }
            | AddRealmRoleState::DeleteStaleAdminConflicts { txn_id, .. }
            | AddRealmRoleState::CommitTransaction { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

fn apply_admin_reducer_updates(
    state: &mut AdminDocumentReducerState,
    input: &AddRealmRoleConfig,
) -> Result<Vec<AdminDocumentEvent>, AdminDocumentReducerError> {
    let mut admin_events = Vec::new();
    let event = state.apply_operation(
        &input.actor,
        AdminDocumentOperation::RealmRoleCreated {
            role: AdminDocumentRoleDefinition::from(&input.role),
        },
    )?;
    admin_events.push(event);

    for user_id in sorted_user_ids(&input.role.assigned_users) {
        let event = state.apply_operation(
            &input.actor,
            AdminDocumentOperation::RealmRoleUserAssignmentAdded {
                role_id: input.role.role_id,
                user_id,
            },
        )?;
        admin_events.push(event);
    }

    Ok(admin_events)
}

fn materialize_realm_role(
    auth_doc: &mut RealmAuthorizationDocument,
    role: &Role,
    reducer_state: &AdminDocumentReducerState,
) {
    if !reducer_state
        .materialized_realm_roles()
        .contains(&role.role_id)
    {
        auth_doc.roles.remove(&role.role_id);
        return;
    }

    auth_doc.roles.insert(role.role_id, role.clone());

    let materialized_assignments = reducer_state.materialized_realm_role_user_assignments();
    let Some(auth_role) = auth_doc.roles.get_mut(&role.role_id) else {
        return;
    };
    for user_id in sorted_user_ids(&role.assigned_users) {
        if materialized_assignments
            .get(&role.role_id)
            .is_some_and(|users| users.contains(&user_id))
        {
            auth_role.assigned_users.insert(user_id);
        } else {
            auth_role.assigned_users.remove(&user_id);
        }
    }
}

fn sorted_user_ids(user_ids: &HashSet<UserId>) -> Vec<UserId> {
    let mut user_ids: Vec<_> = user_ids.iter().copied().collect();
    user_ids.sort();
    user_ids
}

#[cfg(test)]
pub mod test {
    use std::collections::{HashMap, HashSet};

    use crate::add_realm_role::{AddRealmRoleConfig, AddRealmRoleOperation, AddRealmRoleState};
    use crate::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::admin_document_reducer::{
        AdminDocumentConflict, AdminDocumentConflictValue, AdminDocumentReducerState,
    };
    use aruna_core::admin_documents::{
        AdminDocumentDot, AdminDocumentOperation, AdminDocumentRoleDefinition, AdminDocumentTarget,
    };
    use aruna_core::document::{
        DocumentSyncOutboxEvent, DocumentSyncOutboxRecord, DocumentSyncTarget,
    };
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        ADMIN_DOCUMENT_CONFLICT_KEYSPACE, ADMIN_DOCUMENT_STATE_KEYSPACE, AUTH_KEYSPACE,
        DOCUMENT_SYNC_OUTBOX_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::storage_entries::{
        admin_document_reducer_conflict_key, admin_document_reducer_state_key,
    };
    use aruna_core::structs::{Actor, Permission, RealmAuthorizationDocument, Role};
    use aruna_core::task::{TaskEvent, TaskKey};
    use aruna_core::types::TxnId;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[test]
    pub fn writes_realm_auth_doc_reducer_state_and_conflicts_in_one_transaction() {
        let realm_id = aruna_core::structs::RealmId([0u8; 32]);
        let actor_user_id = UserId::local(Ulid::from_bytes([1u8; 16]), realm_id);
        let assigned_user_id = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id);
        let retained_conflict_user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let actor = Actor {
            node_id,
            user_id: actor_user_id,
            realm_id,
        };
        let role_id = Ulid::from_bytes([4u8; 16]);
        let retained_conflict_role_id = Ulid::from_bytes([5u8; 16]);
        let input = AddRealmRoleConfig {
            actor: actor.clone(),
            realm_id,
            role: Role {
                role_id,
                name: "test_role".to_string(),
                permissions: HashMap::from([(
                    format!("/{realm_id}/admin/roles/{role_id}"),
                    Permission::WRITE,
                )]),
                assigned_users: HashSet::from([assigned_user_id]),
            },
        };
        let target = AdminDocumentTarget::Realm { realm_id };
        let stale_conflict_path =
            format!("realm.roles.{role_id}.assigned_users.{assigned_user_id}");
        let retained_conflict_path = format!(
            "realm.roles.{retained_conflict_role_id}.assigned_users.{retained_conflict_user_id}"
        );
        let stale_add_dot = conflict_dot(2, 1);
        let stale_remove_dot = conflict_dot(3, 1);
        let retained_add_dot = conflict_dot(4, 1);
        let retained_remove_dot = conflict_dot(5, 1);
        let mut previous_state = AdminDocumentReducerState::new(target.clone());
        for dot in [
            stale_add_dot,
            stale_remove_dot,
            retained_add_dot,
            retained_remove_dot,
        ] {
            previous_state
                .clock
                .advance(dot.origin_node_id, dot.origin_seq);
        }
        previous_state.conflicts.insert(
            stale_conflict_path.clone(),
            AdminDocumentConflict {
                path: stale_conflict_path.clone(),
                values: vec![
                    AdminDocumentConflictValue {
                        value: Some(assigned_user_id.to_string()),
                        dot: stale_add_dot,
                    },
                    AdminDocumentConflictValue {
                        value: None,
                        dot: stale_remove_dot,
                    },
                ],
            },
        );
        previous_state.conflicts.insert(
            retained_conflict_path.clone(),
            AdminDocumentConflict {
                path: retained_conflict_path.clone(),
                values: vec![
                    AdminDocumentConflictValue {
                        value: Some(retained_conflict_user_id.to_string()),
                        dot: retained_add_dot,
                    },
                    AdminDocumentConflictValue {
                        value: None,
                        dot: retained_remove_dot,
                    },
                ],
            },
        );
        let auth_doc = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        let stale_conflict_key = admin_document_reducer_conflict_key(&target, &stale_conflict_path);
        let mut expected_auth_doc = auth_doc.clone();
        expected_auth_doc.roles.insert(role_id, input.role.clone());

        let mut operation = AddRealmRoleOperation::new(input.clone());
        assert!(matches!(
            operation.start().first(),
            Some(Effect::SubOperation(_))
        ));
        let effects = operation.step(Event::SubOperation(
            aruna_core::events::SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));
        assert_eq!(
            effects.first().unwrap(),
            &Effect::Storage(StorageEffect::StartTransaction { read: false })
        );
        let txn_id = TxnId::new();
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert_eq!(
            effects.first().unwrap(),
            &Effect::Storage(StorageEffect::BatchRead {
                reads: vec![
                    (AUTH_KEYSPACE.to_string(), (*realm_id.as_bytes()).into()),
                    (
                        ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                        admin_document_reducer_state_key(&target),
                    ),
                ],
                txn_id: Some(txn_id),
            })
        );

        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    (*realm_id.as_bytes()).into(),
                    Some(auth_doc.to_bytes(&actor).unwrap().into()),
                ),
                (
                    admin_document_reducer_state_key(&target),
                    Some(postcard::to_allocvec(&previous_state).unwrap().into()),
                ),
            ],
        }));
        let write_effect = effects.first().unwrap();
        match write_effect {
            Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: effect_txn_id,
            }) => {
                assert_eq!(effect_txn_id, &Some(txn_id));
                let auth_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == AUTH_KEYSPACE)
                    .expect("realm auth doc write is included");
                let reducer_state_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == ADMIN_DOCUMENT_STATE_KEYSPACE)
                    .expect("admin reducer state write is included");
                let outbox_records: Vec<DocumentSyncOutboxRecord> = writes
                    .iter()
                    .filter(|(keyspace, _, _)| keyspace == DOCUMENT_SYNC_OUTBOX_KEYSPACE)
                    .map(|(_, _, value)| postcard::from_bytes(value.as_ref()).unwrap())
                    .collect();
                let _retained_conflict_write = writes
                    .iter()
                    .find(|(keyspace, key, _)| {
                        keyspace == ADMIN_DOCUMENT_CONFLICT_KEYSPACE
                            && key
                                == &admin_document_reducer_conflict_key(
                                    &target,
                                    &retained_conflict_path,
                                )
                    })
                    .expect("retained conflict write is included");

                let stored_auth_doc =
                    RealmAuthorizationDocument::from_bytes(auth_write.2.as_ref()).unwrap();
                let reducer_state: AdminDocumentReducerState =
                    postcard::from_bytes(reducer_state_write.2.as_ref()).unwrap();

                assert_eq!(stored_auth_doc.roles.get(&role_id).unwrap(), &input.role);
                assert!(outbox_records.iter().any(|record| {
                    record.target == (DocumentSyncTarget::RealmAuthorization { realm_id })
                        && matches!(
                            &record.event,
                            DocumentSyncOutboxEvent::AdminOperation { event }
                                if event.target == target
                                    && matches!(
                                        &event.op,
                                        AdminDocumentOperation::RealmRoleCreated { role }
                                            if role == &AdminDocumentRoleDefinition::from(&input.role)
                                    )
                        )
                }));
                assert!(
                    reducer_state
                        .materialized_realm_role_user_assignments()
                        .get(&role_id)
                        .is_some_and(|users| users.contains(&assigned_user_id))
                );
                assert!(!reducer_state.conflicts.contains_key(&stale_conflict_path));
                assert!(
                    reducer_state
                        .conflicts
                        .contains_key(&retained_conflict_path)
                );
                assert!(!writes.iter().any(|(keyspace, key, _)| {
                    keyspace == ADMIN_DOCUMENT_CONFLICT_KEYSPACE && key == &stale_conflict_key
                }));
            }
            other => panic!("unexpected realm role write effect: {other:?}"),
        }

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert_eq!(
            effects.first().unwrap(),
            &Effect::Storage(StorageEffect::BatchDelete {
                deletes: vec![(
                    ADMIN_DOCUMENT_CONFLICT_KEYSPACE.to_string(),
                    stale_conflict_key,
                )],
                txn_id: Some(txn_id),
            })
        );
        let effects = operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        assert_eq!(
            effects.first().unwrap(),
            &Effect::Storage(StorageEffect::CommitTransaction { txn_id })
        );
        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(matches!(effects.first(), Some(Effect::Task(_))));
        assert_eq!(
            operation.state,
            AddRealmRoleState::ScheduleAdminDocumentOutboxDrain {
                auth_doc: expected_auth_doc.clone(),
            }
        );

        let effects = operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainDocumentSyncOutbox,
            after: std::time::Duration::ZERO,
        }));
        assert!(effects.is_empty());
        assert_eq!(operation.state, AddRealmRoleState::Finish);
        assert_eq!(operation.finalize().unwrap(), expected_auth_doc);
    }

    #[test]
    pub fn outbox_drain_scheduling_error_finishes_without_direct_auth_doc() {
        let realm_id = aruna_core::structs::RealmId([6u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([7u8; 16]), realm_id);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
            user_id,
            realm_id,
        };
        let role = Role {
            role_id: Ulid::from_bytes([9u8; 16]),
            name: "test_role".to_string(),
            permissions: HashMap::from([(format!("/{realm_id}/admin"), Permission::WRITE)]),
            assigned_users: HashSet::from([user_id]),
        };
        let auth_doc = RealmAuthorizationDocument {
            realm_id,
            roles: HashMap::from([(role.role_id, role.clone())]),
            operation_restrictions: HashMap::new(),
        };
        let mut operation = AddRealmRoleOperation::new(AddRealmRoleConfig {
            actor,
            realm_id,
            role,
        });
        operation.state = AddRealmRoleState::ScheduleAdminDocumentOutboxDrain {
            auth_doc: auth_doc.clone(),
        };

        let effects = operation.step(Event::Task(TaskEvent::Error {
            key: Some(TaskKey::DrainDocumentSyncOutbox),
            message: "schedule failed".to_string(),
        }));

        assert!(effects.is_empty());
        assert_eq!(operation.state, AddRealmRoleState::Finish);
        assert_eq!(operation.finalize().unwrap(), auth_doc);
    }

    fn conflict_dot(seed: u8, origin_seq: u64) -> AdminDocumentDot {
        AdminDocumentDot {
            event_id: Ulid::from_bytes([seed; 16]),
            origin_node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
            origin_seq,
        }
    }

    #[tokio::test]
    pub async fn test_add_role() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let task_handle = TaskHandle::new();

        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle.clone()),
            metadata_handle: None,
            task_handle: Some(task_handle),
            blob_handle: None,
        };

        let realm_id = aruna_core::structs::RealmId([0u8; 32]);
        let user_id = UserId::local(Ulid::new(), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let realm_config = CreateRealmConfig {
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            realm_description: "A realm description".to_string(),
            oidc_providers: Vec::new(),
        };
        let realm_operation = CreateRealmOperation::new(realm_config.clone());
        let (_realm, _realm_auth_doc) = drive(realm_operation, &context).await.unwrap();
        drive(
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                actor: realm_config.actor.clone(),
            }),
            &context,
        )
        .await
        .unwrap();

        let add_role_input = AddRealmRoleConfig {
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            realm_id,
            role: Role {
                role_id: Ulid::new(),
                name: "test_role".to_string(),
                permissions: HashMap::from([(
                    format!("{}/admin/create_group/*", realm_id),
                    Permission::WRITE,
                )]),
                assigned_users: HashSet::from([user_id]),
            },
        };

        let add_role_operation = AddRealmRoleOperation::new(add_role_input.clone());
        let auth_doc = drive(add_role_operation, &context).await.unwrap();

        assert_eq!(
            auth_doc.roles.get(&add_role_input.role.role_id).unwrap(),
            &add_role_input.role
        );

        net_handle.shutdown().await;
    }
}
