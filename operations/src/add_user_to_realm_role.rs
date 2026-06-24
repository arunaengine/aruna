use aruna_core::admin_document_reducer::{AdminDocumentReducerError, AdminDocumentReducerState};
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
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
use aruna_core::structs::{Actor, AuthContext, Permission, RealmAuthorizationDocument, RealmId};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Key, KeySpace, RoleId, TxnId, UserId};
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::HashSet;
use thiserror::Error;
use ulid::Ulid;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::replicate_documents::replicate_documents_effect;
use aruna_core::types::Effects;

#[derive(Clone, Debug, PartialEq)]
pub struct AddUserToRealmRolesInput {
    pub actor: Actor,
    pub realm_id: RealmId,
    pub user_id: UserId,
    pub role_ids: HashSet<RoleId>,
}

#[derive(PartialEq)]
pub struct AddUserToRealmRolesOperation {
    input: AddUserToRealmRolesInput,
    state: AddUserToRealmRolesState,
    output: Option<Result<RealmAuthorizationDocument, AddUserToRealmRolesError>>,
}

impl std::fmt::Debug for AddUserToRealmRolesOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddUserToRealmRolesOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AddUserToRealmRolesState {
    Init,
    Auth,
    StartTransaction,
    ReadAuthDocAndAdminState {
        txn_id: TxnId,
    },
    WriteAuthDocAndAdminState {
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
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
pub enum AddUserToRealmRolesError {
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
    #[error("Role not found")]
    RoleNotFound,
    #[error("Authorization document not found")]
    AuthDocNotFound,
    #[error(transparent)]
    CheckPermissionsError(#[from] AuthorizationError),
    #[error("Adding user to realm  did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: AddUserToRealmRolesState,
        expected: &'static str,
        got: String,
    },
}

impl AddUserToRealmRolesOperation {
    pub fn new(input: AddUserToRealmRolesInput) -> Self {
        AddUserToRealmRolesOperation {
            input,
            state: AddUserToRealmRolesState::Init,
            output: None,
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                AddUserToRealmRolesState::StartTransaction,
                "Event::Storage(StorageEvent::TransactionStarted)",
                got,
            );
        };
        match self.emit_read_auth_doc_and_admin_state(txn_id) {
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
                AddUserToRealmRolesState::Auth,
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
    ) -> Result<Effects, AddUserToRealmRolesError> {
        if auth_result? {
            self.state = AddUserToRealmRolesState::StartTransaction;
            Ok(smallvec![Effect::Storage(
                StorageEffect::StartTransaction { read: false }
            )])
        } else {
            Err(AddUserToRealmRolesError::Unauthorized)
        }
    }

    fn emit_read_auth_doc_and_admin_state(
        &mut self,
        txn_id: TxnId,
    ) -> Result<Effects, AddUserToRealmRolesError> {
        self.state = AddUserToRealmRolesState::ReadAuthDocAndAdminState { txn_id };
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

    fn handle_read_auth_doc_and_admin_state(&mut self, event: Event, txn_id: TxnId) -> Effects {
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

        match self.emit_write_auth_doc_and_admin_state(
            txn_id,
            auth_doc_value.clone(),
            reducer_state_value.clone(),
        ) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_write_auth_doc_and_admin_state(
        &mut self,
        txn_id: TxnId,
        auth_doc: Option<ByteView>,
        reducer_state_value: Option<ByteView>,
    ) -> Result<Effects, AddUserToRealmRolesError> {
        let mut auth_doc = RealmAuthorizationDocument::from_bytes(
            &auth_doc.ok_or_else(|| AddUserToRealmRolesError::AuthDocNotFound)?,
        )?;
        let role_ids = sorted_role_ids(&self.input.role_ids);
        for role_id in &role_ids {
            if !auth_doc.roles.contains_key(role_id) {
                return Err(AddUserToRealmRolesError::RoleNotFound);
            }
        }

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
        let admin_events = apply_admin_reducer_updates(&mut reducer_state, &self.input, &role_ids)?;

        let materialized_assignments = reducer_state.materialized_realm_role_user_assignments();
        for role_id in role_ids {
            let role = auth_doc
                .roles
                .get_mut(&role_id)
                .ok_or_else(|| AddUserToRealmRolesError::RoleNotFound)?;
            if materialized_assignments
                .get(&role_id)
                .is_some_and(|users| users.contains(&self.input.user_id))
            {
                role.assigned_users.insert(self.input.user_id);
            } else {
                role.assigned_users.remove(&self.input.user_id);
            }
        }

        let key = (*auth_doc.realm_id.as_bytes()).into();
        let value = auth_doc.to_bytes(&self.input.actor)?.into();
        let stale_conflict_deletes = stale_admin_document_conflict_delete_entries(
            previous_reducer_state.as_ref(),
            Some(&reducer_state),
        );
        let mut writes = vec![
            (AUTH_KEYSPACE.to_string(), key, value),
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
            );
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
        writes.extend(admin_document_conflict_write_entries(&reducer_state)?);

        self.state = AddUserToRealmRolesState::WriteAuthDocAndAdminState {
            txn_id,
            auth_doc,
            admin_outbox_written: !admin_events.is_empty(),
            stale_conflict_deletes,
        };

        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_write_auth_doc_and_admin_state(
        &mut self,
        event: Event,
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::BatchWriteResult)",
                got,
            );
        };

        if !stale_conflict_deletes.is_empty() {
            self.state = AddUserToRealmRolesState::DeleteStaleAdminConflicts {
                txn_id,
                auth_doc,
                admin_outbox_written,
            };
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes: stale_conflict_deletes,
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
        self.state = AddUserToRealmRolesState::CommitTransaction {
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
            self.state = AddUserToRealmRolesState::ScheduleAdminDocumentOutboxDrain { auth_doc };
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
            Event::Task(TaskEvent::TimerScheduled { .. }) => self.emit_announce_auth_doc(auth_doc),
            Event::Task(TaskEvent::Error { message, .. }) => {
                self.fail(AddUserToRealmRolesError::TopicAnnouncement(format!(
                    "admin document outbox drain scheduling failed: {message}"
                )))
            }
            other => self.unexpected_event(
                self.state.clone(),
                "admin document outbox drain timer schedule",
                format!("{other:?}"),
            ),
        }
    }

    fn emit_announce_auth_doc(&mut self, auth_doc: RealmAuthorizationDocument) -> Effects {
        self.state = AddUserToRealmRolesState::AnnounceAuthDoc {
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
            return self.fail(AddUserToRealmRolesError::TopicAnnouncement(error));
        }
        self.state = AddUserToRealmRolesState::Finish;
        self.output = Some(Ok(auth_doc));
        smallvec![]
    }

    fn fail(&mut self, err: AddUserToRealmRolesError) -> Effects {
        self.state = AddUserToRealmRolesState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn fail_with_cleanup(
        &mut self,
        err: AddUserToRealmRolesError,
        cleanup_effects: Effects,
    ) -> Effects {
        self.state = AddUserToRealmRolesState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: AddUserToRealmRolesState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            AddUserToRealmRolesError::UnexpectedEvent {
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

impl Operation for AddUserToRealmRolesOperation {
    type Output = RealmAuthorizationDocument;

    type Error = AddUserToRealmRolesError;

    fn start(&mut self) -> Effects {
        self.state = AddUserToRealmRolesState::Auth;

        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.auth_context(),
                path: format!(
                    "/{}/admin/roles/{}",
                    self.input.realm_id, self.input.user_id
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
            AddUserToRealmRolesState::Auth => self.handle_authorization(event),
            AddUserToRealmRolesState::StartTransaction => self.handle_start_transaction(event),
            AddUserToRealmRolesState::ReadAuthDocAndAdminState { txn_id } => {
                self.handle_read_auth_doc_and_admin_state(event, txn_id)
            }
            AddUserToRealmRolesState::WriteAuthDocAndAdminState {
                txn_id,
                auth_doc,
                admin_outbox_written,
                stale_conflict_deletes,
            } => self.handle_write_auth_doc_and_admin_state(
                event,
                txn_id,
                auth_doc,
                admin_outbox_written,
                stale_conflict_deletes,
            ),
            AddUserToRealmRolesState::DeleteStaleAdminConflicts {
                txn_id,
                auth_doc,
                admin_outbox_written,
            } => self.handle_delete_stale_admin_conflicts(
                event,
                txn_id,
                auth_doc,
                admin_outbox_written,
            ),
            AddUserToRealmRolesState::CommitTransaction {
                auth_doc,
                admin_outbox_written,
                ..
            } => self.handle_commit_transaction(event, auth_doc, admin_outbox_written),
            AddUserToRealmRolesState::ScheduleAdminDocumentOutboxDrain { auth_doc } => {
                self.handle_schedule_admin_document_outbox_drain(event, auth_doc)
            }
            AddUserToRealmRolesState::AnnounceAuthDoc { auth_doc } => {
                self.handle_announce_auth_doc(event, auth_doc)
            }
            AddUserToRealmRolesState::Init
            | AddUserToRealmRolesState::Finish
            | AddUserToRealmRolesState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            AddUserToRealmRolesState::Finish | AddUserToRealmRolesState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or_else(|| AddUserToRealmRolesError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            AddUserToRealmRolesState::ReadAuthDocAndAdminState { txn_id }
            | AddUserToRealmRolesState::WriteAuthDocAndAdminState { txn_id, .. }
            | AddUserToRealmRolesState::DeleteStaleAdminConflicts { txn_id, .. }
            | AddUserToRealmRolesState::CommitTransaction { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }

            _ => smallvec![],
        }
    }
}

fn sorted_role_ids(role_ids: &HashSet<RoleId>) -> Vec<RoleId> {
    let mut role_ids: Vec<_> = role_ids.iter().copied().collect();
    role_ids.sort();
    role_ids
}

fn apply_admin_reducer_updates(
    state: &mut AdminDocumentReducerState,
    input: &AddUserToRealmRolesInput,
    role_ids: &[RoleId],
) -> Result<Vec<AdminDocumentEvent>, AdminDocumentReducerError> {
    let mut admin_events = Vec::new();
    for role_id in role_ids {
        if should_seed_realm_role(state, *role_id) {
            apply_admin_reducer_operation(
                state,
                &input.actor,
                AdminDocumentOperation::RealmRoleAdded { role_id: *role_id },
            )?;
        }
        let event = apply_admin_reducer_operation(
            state,
            &input.actor,
            AdminDocumentOperation::RealmRoleUserAssignmentAdded {
                role_id: *role_id,
                user_id: input.user_id,
            },
        )?;
        admin_events.push(event);
    }

    Ok(admin_events)
}

fn should_seed_realm_role(state: &AdminDocumentReducerState, role_id: RoleId) -> bool {
    !state.materialized_realm_roles().contains(&role_id)
        && !state
            .conflicts
            .contains_key(&format!("realm.roles.{role_id}"))
}

fn apply_admin_reducer_operation(
    state: &mut AdminDocumentReducerState,
    actor: &Actor,
    op: AdminDocumentOperation,
) -> Result<AdminDocumentEvent, AdminDocumentReducerError> {
    let observed = state.clock.clone();
    let event = AdminDocumentEvent {
        event_id: Ulid::new(),
        target: state.target.clone(),
        origin_node_id: actor.node_id,
        origin_seq: observed.sequence_for(&actor.node_id) + 1,
        observed,
        actor: actor.clone(),
        op,
    };
    state.apply(&event)?;
    Ok(event)
}

#[cfg(test)]
pub mod test {
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

    use crate::add_user_to_realm_role::{AddUserToRealmRolesInput, AddUserToRealmRolesOperation};
    use crate::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::admin_document_reducer::{
        AdminDocumentAttributeVersion, AdminDocumentConflict, AdminDocumentConflictValue,
        AdminDocumentReducerState,
    };
    use aruna_core::admin_documents::{
        AdminDocumentClock, AdminDocumentDot, AdminDocumentOperation, AdminDocumentTarget,
    };
    use aruna_core::document::{
        DocumentSyncOutboxEvent, DocumentSyncOutboxRecord, DocumentSyncTarget,
    };
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::keyspaces::{
        ADMIN_DOCUMENT_CONFLICT_KEYSPACE, ADMIN_DOCUMENT_STATE_KEYSPACE, AUTH_KEYSPACE,
        DOCUMENT_SYNC_OUTBOX_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::storage_entries::{
        admin_document_reducer_conflict_key, admin_document_reducer_state_key,
    };
    use aruna_core::structs::{Actor, Permission, RealmAuthorizationDocument, RealmId, Role};
    use aruna_core::task::{TaskEvent, TaskKey};
    use aruna_core::types::{RoleId, TxnId};
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn node(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn dot(seed: u8) -> AdminDocumentDot {
        AdminDocumentDot {
            event_id: Ulid::from_bytes([seed; 16]),
            origin_node_id: node(seed),
            origin_seq: u64::from(seed),
        }
    }

    fn assignment_path(role_id: RoleId, user_id: UserId) -> String {
        format!("realm.roles.{role_id}.assigned_users.{user_id}")
    }

    fn assignment_conflict(
        role_id: RoleId,
        user_id: UserId,
        add_seed: u8,
        remove_seed: u8,
    ) -> AdminDocumentConflict {
        AdminDocumentConflict {
            path: assignment_path(role_id, user_id),
            values: vec![
                AdminDocumentConflictValue {
                    value: Some(user_id.to_string()),
                    dot: dot(add_seed),
                },
                AdminDocumentConflictValue {
                    value: None,
                    dot: dot(remove_seed),
                },
            ],
        }
    }

    fn reducer_state_with_assignment_conflicts(
        realm_id: RealmId,
        role_id: RoleId,
        assigned_user_id: UserId,
        remaining_conflict_user_id: UserId,
    ) -> AdminDocumentReducerState {
        let role_dot = dot(3);
        let mut clock = AdminDocumentClock::default();
        for dot in [role_dot, dot(11), dot(12), dot(13), dot(14)] {
            clock.advance(dot.origin_node_id, dot.origin_seq);
        }

        AdminDocumentReducerState {
            target: AdminDocumentTarget::Realm { realm_id },
            clock,
            applied_event_ids: BTreeSet::new(),
            user_attributes: BTreeMap::new(),
            conflicts: BTreeMap::from([
                (
                    assignment_path(role_id, assigned_user_id),
                    assignment_conflict(role_id, assigned_user_id, 11, 12),
                ),
                (
                    assignment_path(role_id, remaining_conflict_user_id),
                    assignment_conflict(role_id, remaining_conflict_user_id, 13, 14),
                ),
            ]),
            user_name: None,
            user_subject_ids: BTreeMap::from([(
                format!("realm.roles.{role_id}"),
                AdminDocumentAttributeVersion {
                    value: Some(role_id.to_string()),
                    dot: role_dot,
                },
            )]),
        }
    }

    #[test]
    fn writes_reducer_state_and_conflicts_with_realm_auth_doc_transaction() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let owner_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let assigned_user_id = UserId::local(Ulid::from_bytes([4u8; 16]), realm_id);
        let remaining_conflict_user_id = UserId::local(Ulid::from_bytes([5u8; 16]), realm_id);
        let role_id = Ulid::from_bytes([6u8; 16]);
        let second_role_id = Ulid::from_bytes([7u8; 16]);
        let actor = Actor {
            node_id: node(8),
            user_id: owner_id,
            realm_id,
        };
        let auth_doc = RealmAuthorizationDocument {
            realm_id,
            roles: HashMap::from([
                (
                    role_id,
                    Role {
                        role_id,
                        name: "realm_user".to_string(),
                        permissions: HashMap::from([("/test".to_string(), Permission::READ)]),
                        assigned_users: HashSet::new(),
                    },
                ),
                (
                    second_role_id,
                    Role {
                        role_id: second_role_id,
                        name: "realm_reader".to_string(),
                        permissions: HashMap::from([("/other".to_string(), Permission::READ)]),
                        assigned_users: HashSet::new(),
                    },
                ),
            ]),
            operation_restrictions: HashMap::new(),
        };
        let previous_state = reducer_state_with_assignment_conflicts(
            realm_id,
            role_id,
            assigned_user_id,
            remaining_conflict_user_id,
        );
        let input = AddUserToRealmRolesInput {
            actor: actor.clone(),
            realm_id,
            user_id: assigned_user_id,
            role_ids: HashSet::from([role_id, second_role_id]),
        };
        let target = AdminDocumentTarget::Realm { realm_id };
        let mut operation = AddUserToRealmRolesOperation::new(input);

        operation.start();
        operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));
        let txn_id = TxnId::new();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));

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
        let (updated_auth_doc, reducer_state, outbox_records) = match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchWrite { writes, txn_id: id }) => {
                assert_eq!(*id, Some(txn_id));
                let auth_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == AUTH_KEYSPACE)
                    .expect("auth doc write is included");
                let reducer_state_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == ADMIN_DOCUMENT_STATE_KEYSPACE)
                    .expect("reducer state write is included");
                let outbox_records: Vec<DocumentSyncOutboxRecord> = writes
                    .iter()
                    .filter(|(keyspace, _, _)| keyspace == DOCUMENT_SYNC_OUTBOX_KEYSPACE)
                    .map(|(_, _, value)| postcard::from_bytes(value).unwrap())
                    .collect();
                let conflict_writes: Vec<_> = writes
                    .iter()
                    .filter(|(keyspace, _, _)| keyspace == ADMIN_DOCUMENT_CONFLICT_KEYSPACE)
                    .collect();
                assert_eq!(conflict_writes.len(), 1);
                let conflict: AdminDocumentConflict =
                    postcard::from_bytes(conflict_writes[0].2.as_ref()).unwrap();
                assert_eq!(
                    conflict.path,
                    assignment_path(role_id, remaining_conflict_user_id)
                );

                (
                    RealmAuthorizationDocument::from_bytes(auth_write.2.as_ref()).unwrap(),
                    postcard::from_bytes::<AdminDocumentReducerState>(
                        reducer_state_write.2.as_ref(),
                    )
                    .unwrap(),
                    outbox_records,
                )
            }
            other => panic!("unexpected write effect: {other:?}"),
        };
        assert_eq!(outbox_records.len(), 2);
        for expected_role_id in [role_id, second_role_id] {
            assert!(outbox_records.iter().any(|record| {
                record.target == (DocumentSyncTarget::RealmAuthorization { realm_id })
                    && matches!(
                        &record.event,
                        DocumentSyncOutboxEvent::AdminOperation { event }
                            if event.target == target
                                && matches!(
                                    &event.op,
                                    AdminDocumentOperation::RealmRoleUserAssignmentAdded {
                                        role_id: event_role_id,
                                        user_id: event_user_id,
                                    } if *event_role_id == expected_role_id
                                        && *event_user_id == assigned_user_id
                                )
                    )
            }));
        }
        assert!(
            updated_auth_doc
                .roles
                .get(&role_id)
                .unwrap()
                .assigned_users
                .contains(&assigned_user_id)
        );
        assert!(
            updated_auth_doc
                .roles
                .get(&second_role_id)
                .unwrap()
                .assigned_users
                .contains(&assigned_user_id)
        );
        assert_eq!(
            reducer_state.materialized_realm_role_user_assignments(),
            BTreeMap::from([
                (role_id, BTreeSet::from([assigned_user_id])),
                (second_role_id, BTreeSet::from([assigned_user_id])),
            ])
        );
        assert!(
            !reducer_state
                .conflicts
                .contains_key(&assignment_path(role_id, assigned_user_id))
        );
        assert!(
            reducer_state
                .conflicts
                .contains_key(&assignment_path(role_id, remaining_conflict_user_id))
        );

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchDelete { deletes, .. }) => {
                assert_eq!(
                    deletes,
                    &vec![(
                        ADMIN_DOCUMENT_CONFLICT_KEYSPACE.to_string(),
                        admin_document_reducer_conflict_key(
                            &target,
                            &assignment_path(role_id, assigned_user_id),
                        ),
                    )]
                );
            }
            other => panic!("unexpected conflict delete effect: {other:?}"),
        };

        let effects = operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::CommitTransaction { .. }))
        ));
        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(matches!(effects.first(), Some(Effect::Task(_))));
        let effects = operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainDocumentSyncOutbox,
            after: std::time::Duration::ZERO,
        }));
        assert!(matches!(effects.first(), Some(Effect::SubOperation(_))));
    }

    #[tokio::test]
    pub async fn test_add_user() {
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
        let (_realm, realm_auth_doc) = drive(realm_operation, &context).await.unwrap();
        drive(
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                actor: realm_config.actor.clone(),
            }),
            &context,
        )
        .await
        .unwrap();

        let admin_role = realm_auth_doc
            .roles
            .iter()
            .filter_map(|(id, r)| {
                if r.name == "realm_admin" {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();

        let add_user_input = AddUserToRealmRolesInput {
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            realm_id,
            user_id: UserId::local(Ulid::new(), realm_id),
            role_ids: admin_role,
        };

        let add_user_operation = AddUserToRealmRolesOperation::new(add_user_input.clone());
        let auth_doc = drive(add_user_operation, &context).await.unwrap();

        assert!(
            auth_doc
                .roles
                .iter()
                .any(|(_, role)| role.assigned_users.contains(&add_user_input.user_id))
        );

        assert!(
            auth_doc
                .roles
                .iter()
                .find(|(_, v)| v.name == "realm_admin")
                .unwrap()
                .1
                .assigned_users
                .contains(&add_user_input.user_id)
        );

        net_handle.shutdown().await;
    }
}
