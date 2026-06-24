use aruna_core::admin_document_reducer::{AdminDocumentReducerError, AdminDocumentReducerState};
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{ADMIN_DOCUMENT_STATE_KEYSPACE, AUTH_KEYSPACE};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, stale_admin_document_conflict_delete_entries,
};
use aruna_core::structs::{Actor, AuthContext, GroupAuthorizationDocument, Permission};
use aruna_core::types::{Effects, GroupId, Key, KeySpace, RoleId, TxnId, UserId};
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::HashSet;
use thiserror::Error;
use ulid::Ulid;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::replicate_documents::replicate_documents_effect;

#[derive(Clone, Debug, PartialEq)]
pub struct AddUserToGroupInput {
    pub actor: Actor,
    pub group_id: GroupId,
    pub user_id: UserId,
    pub role_ids: HashSet<RoleId>,
}

#[derive(PartialEq)]
pub struct AddUserToGroupOperation {
    input: AddUserToGroupInput,
    state: AddUserToGroupState,
    output: Option<Result<GroupAuthorizationDocument, AddUserToGroupError>>,
}

impl std::fmt::Debug for AddUserToGroupOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddUserToGroupOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AddUserToGroupState {
    Init,
    Auth,
    StartTransaction,
    ReadAuthDocAndAdminState {
        txn_id: TxnId,
    },
    WriteAuthDocAndAdminState {
        txn_id: TxnId,
        auth_doc: GroupAuthorizationDocument,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    },
    DeleteStaleAdminConflicts {
        txn_id: TxnId,
        auth_doc: GroupAuthorizationDocument,
    },
    CommitTransaction {
        txn_id: TxnId,
        auth_doc: GroupAuthorizationDocument,
    },
    AnnounceAuthDoc {
        auth_doc: GroupAuthorizationDocument,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum AddUserToGroupError {
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
    #[error("Adding user to group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: AddUserToGroupState,
        expected: &'static str,
        got: String,
    },
}

impl AddUserToGroupOperation {
    pub fn new(input: AddUserToGroupInput) -> Self {
        AddUserToGroupOperation {
            input,
            state: AddUserToGroupState::Init,
            output: None,
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                AddUserToGroupState::StartTransaction,
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
                AddUserToGroupState::Auth,
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
    ) -> Result<Effects, AddUserToGroupError> {
        if auth_result? {
            self.state = AddUserToGroupState::StartTransaction;
            Ok(smallvec![Effect::Storage(
                StorageEffect::StartTransaction { read: false }
            )])
        } else {
            Err(AddUserToGroupError::Unauthorized)
        }
    }

    fn emit_read_auth_doc_and_admin_state(
        &mut self,
        txn_id: TxnId,
    ) -> Result<Effects, AddUserToGroupError> {
        self.state = AddUserToGroupState::ReadAuthDocAndAdminState { txn_id };
        let target = AdminDocumentTarget::Group {
            group_id: self.input.group_id,
        };
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    AUTH_KEYSPACE.to_string(),
                    ByteView::from(self.input.group_id.to_bytes()),
                ),
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
    ) -> Result<Effects, AddUserToGroupError> {
        let mut auth_doc = GroupAuthorizationDocument::from_bytes(
            &auth_doc.ok_or_else(|| AddUserToGroupError::AuthDocNotFound)?,
        )?;
        let role_ids = sorted_role_ids(&self.input.role_ids);
        for role_id in &role_ids {
            if !auth_doc.roles.contains_key(role_id) {
                return Err(AddUserToGroupError::RoleNotFound);
            }
        }

        let target = AdminDocumentTarget::Group {
            group_id: self.input.group_id,
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
        apply_admin_reducer_updates(&mut reducer_state, &self.input, &role_ids)?;

        let materialized_assignments = reducer_state.materialized_group_role_user_assignments();
        for role_id in role_ids {
            let role = auth_doc
                .roles
                .get_mut(&role_id)
                .ok_or_else(|| AddUserToGroupError::RoleNotFound)?;
            if materialized_assignments
                .get(&role_id)
                .is_some_and(|users| users.contains(&self.input.user_id))
            {
                role.assigned_users.insert(self.input.user_id);
            } else {
                role.assigned_users.remove(&self.input.user_id);
            }
        }

        let key = auth_doc.group_id.to_bytes().into();
        let value = auth_doc.to_bytes(&self.input.actor)?.into();
        let stale_conflict_deletes = stale_admin_document_conflict_delete_entries(
            previous_reducer_state.as_ref(),
            Some(&reducer_state),
        );
        let mut writes = vec![
            (AUTH_KEYSPACE.to_string(), key, value),
            admin_document_reducer_state_write_entry(&reducer_state)?,
        ];
        writes.extend(admin_document_conflict_write_entries(&reducer_state)?);

        self.state = AddUserToGroupState::WriteAuthDocAndAdminState {
            txn_id,
            auth_doc,
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
        auth_doc: GroupAuthorizationDocument,
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
            self.state = AddUserToGroupState::DeleteStaleAdminConflicts { txn_id, auth_doc };
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes: stale_conflict_deletes,
                txn_id: Some(txn_id),
            })];
        }

        self.emit_commit_transaction(txn_id, auth_doc)
    }

    fn handle_delete_stale_admin_conflicts(
        &mut self,
        event: Event,
        txn_id: TxnId,
        auth_doc: GroupAuthorizationDocument,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::BatchDeleteResult)",
                got,
            );
        };

        self.emit_commit_transaction(txn_id, auth_doc)
    }

    fn emit_commit_transaction(
        &mut self,
        txn_id: TxnId,
        auth_doc: GroupAuthorizationDocument,
    ) -> Effects {
        self.state = AddUserToGroupState::CommitTransaction { txn_id, auth_doc };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit_transaction(
        &mut self,
        event: Event,
        auth_doc: GroupAuthorizationDocument,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::TransactionCommitted)",
                got,
            );
        };
        self.state = AddUserToGroupState::AnnounceAuthDoc {
            auth_doc: auth_doc.clone(),
        };
        let document = DocumentSyncTarget::GroupAuthorization {
            group_id: auth_doc.group_id,
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
        auth_doc: GroupAuthorizationDocument,
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
            return self.fail(AddUserToGroupError::TopicAnnouncement(error));
        }
        self.state = AddUserToGroupState::Finish;
        self.output = Some(Ok(auth_doc));
        smallvec![]
    }

    fn fail(&mut self, err: AddUserToGroupError) -> Effects {
        self.state = AddUserToGroupState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn fail_with_cleanup(&mut self, err: AddUserToGroupError, cleanup_effects: Effects) -> Effects {
        self.state = AddUserToGroupState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: AddUserToGroupState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            AddUserToGroupError::UnexpectedEvent {
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

impl Operation for AddUserToGroupOperation {
    type Output = GroupAuthorizationDocument;

    type Error = AddUserToGroupError;

    fn start(&mut self) -> Effects {
        self.state = AddUserToGroupState::Auth;

        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.auth_context(),
                path: format!(
                    "/{}/g/{}/admin/users/{}",
                    self.input.actor.realm_id, self.input.group_id, self.input.user_id
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
            AddUserToGroupState::Auth => self.handle_authorization(event),
            AddUserToGroupState::StartTransaction => self.handle_start_transaction(event),
            AddUserToGroupState::ReadAuthDocAndAdminState { txn_id } => {
                self.handle_read_auth_doc_and_admin_state(event, txn_id)
            }
            AddUserToGroupState::WriteAuthDocAndAdminState {
                txn_id,
                auth_doc,
                stale_conflict_deletes,
            } => self.handle_write_auth_doc_and_admin_state(
                event,
                txn_id,
                auth_doc,
                stale_conflict_deletes,
            ),
            AddUserToGroupState::DeleteStaleAdminConflicts { txn_id, auth_doc } => {
                self.handle_delete_stale_admin_conflicts(event, txn_id, auth_doc)
            }
            AddUserToGroupState::CommitTransaction { auth_doc, .. } => {
                self.handle_commit_transaction(event, auth_doc)
            }
            AddUserToGroupState::AnnounceAuthDoc { auth_doc } => {
                self.handle_announce_auth_doc(event, auth_doc)
            }
            AddUserToGroupState::Init
            | AddUserToGroupState::Finish
            | AddUserToGroupState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            AddUserToGroupState::Finish | AddUserToGroupState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or_else(|| AddUserToGroupError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            AddUserToGroupState::ReadAuthDocAndAdminState { txn_id }
            | AddUserToGroupState::WriteAuthDocAndAdminState { txn_id, .. }
            | AddUserToGroupState::DeleteStaleAdminConflicts { txn_id, .. }
            | AddUserToGroupState::CommitTransaction { txn_id, .. } => {
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
    input: &AddUserToGroupInput,
    role_ids: &[RoleId],
) -> Result<(), AdminDocumentReducerError> {
    for role_id in role_ids {
        if should_seed_group_role(state, *role_id) {
            apply_admin_reducer_operation(
                state,
                &input.actor,
                AdminDocumentOperation::GroupRoleAdded { role_id: *role_id },
            )?;
        }
        apply_admin_reducer_operation(
            state,
            &input.actor,
            AdminDocumentOperation::GroupRoleUserAssignmentAdded {
                role_id: *role_id,
                user_id: input.user_id,
            },
        )?;
    }

    Ok(())
}

fn should_seed_group_role(state: &AdminDocumentReducerState, role_id: RoleId) -> bool {
    !state.materialized_group_roles().contains(&role_id)
        && !state
            .conflicts
            .contains_key(&format!("group.roles.{role_id}"))
}

fn apply_admin_reducer_operation(
    state: &mut AdminDocumentReducerState,
    actor: &Actor,
    op: AdminDocumentOperation,
) -> Result<(), AdminDocumentReducerError> {
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
    Ok(())
}

#[cfg(test)]
pub mod test {
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

    use aruna_core::UserId;
    use aruna_core::admin_document_reducer::{
        AdminDocumentAttributeVersion, AdminDocumentConflict, AdminDocumentConflictValue,
        AdminDocumentReducerState,
    };
    use aruna_core::admin_documents::{AdminDocumentClock, AdminDocumentDot, AdminDocumentTarget};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::operation::Operation;
    use aruna_core::storage_entries::{
        admin_document_reducer_conflict_key, admin_document_reducer_state_key,
    };
    use aruna_core::structs::{Actor, GroupAuthorizationDocument, Permission, RealmId, Role};
    use aruna_core::types::{RoleId, TxnId};
    use aruna_core::{
        ADMIN_DOCUMENT_CONFLICT_KEYSPACE, ADMIN_DOCUMENT_STATE_KEYSPACE, AUTH_KEYSPACE,
    };
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    use crate::add_user_to_group::{AddUserToGroupInput, AddUserToGroupOperation};
    use crate::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};

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
        format!("group.roles.{role_id}.assigned_users.{user_id}")
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
        group_id: Ulid,
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
            target: AdminDocumentTarget::Group { group_id },
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
                format!("group.roles.{role_id}"),
                AdminDocumentAttributeVersion {
                    value: Some(role_id.to_string()),
                    dot: role_dot,
                },
            )]),
        }
    }

    #[test]
    fn writes_reducer_state_and_conflicts_with_auth_doc_update_transaction() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let owner_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let assigned_user_id = UserId::local(Ulid::from_bytes([4u8; 16]), realm_id);
        let remaining_conflict_user_id = UserId::local(Ulid::from_bytes([5u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([6u8; 16]);
        let role_id = Ulid::from_bytes([7u8; 16]);
        let actor = Actor {
            node_id: node(8),
            user_id: owner_id,
            realm_id,
        };
        let auth_doc = GroupAuthorizationDocument {
            group_id,
            roles: HashMap::from([(
                role_id,
                Role {
                    role_id,
                    name: "user".to_string(),
                    permissions: HashMap::from([("/test".to_string(), Permission::READ)]),
                    assigned_users: HashSet::new(),
                },
            )]),
        };
        let previous_state = reducer_state_with_assignment_conflicts(
            group_id,
            role_id,
            assigned_user_id,
            remaining_conflict_user_id,
        );
        let input = AddUserToGroupInput {
            actor: actor.clone(),
            group_id,
            user_id: assigned_user_id,
            role_ids: HashSet::from([role_id]),
        };
        let target = AdminDocumentTarget::Group { group_id };
        let mut operation = AddUserToGroupOperation::new(input);

        operation.start();
        operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));
        let txn_id = TxnId::new();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));

        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    group_id.to_bytes().into(),
                    Some(auth_doc.to_bytes(&actor).unwrap().into()),
                ),
                (
                    admin_document_reducer_state_key(&target),
                    Some(postcard::to_allocvec(&previous_state).unwrap().into()),
                ),
            ],
        }));
        let (updated_auth_doc, reducer_state) = match effects.first().unwrap() {
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
                    GroupAuthorizationDocument::from_bytes(auth_write.2.as_ref()).unwrap(),
                    postcard::from_bytes::<AdminDocumentReducerState>(
                        reducer_state_write.2.as_ref(),
                    )
                    .unwrap(),
                )
            }
            other => panic!("unexpected write effect: {other:?}"),
        };
        assert!(
            updated_auth_doc
                .roles
                .get(&role_id)
                .unwrap()
                .assigned_users
                .contains(&assigned_user_id)
        );
        assert_eq!(
            reducer_state.materialized_group_role_user_assignments(),
            BTreeMap::from([(role_id, BTreeSet::from([assigned_user_id]))])
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
            realm_description: "Test realm".to_string(),
            oidc_providers: Vec::new(),
        };
        let realm_operation = CreateRealmOperation::new(realm_config);
        let _ = drive(realm_operation, &context).await.unwrap();

        let group_config = CreateGroupConfig {
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            display_name: "Test group".to_string(),
        };
        let group_operation = CreateGroupOperation::new(group_config.clone());
        let (group, auth_doc) = drive(group_operation, &context).await.unwrap();

        let reader_writer_roles = auth_doc
            .roles
            .iter()
            .filter_map(|(k, v)| if v.name == "user" { Some(*k) } else { None })
            .collect();

        let add_user_input = AddUserToGroupInput {
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            group_id: group.group_id,
            user_id: UserId::local(Ulid::new(), realm_id),
            role_ids: reader_writer_roles,
        };

        let add_user_operation = AddUserToGroupOperation::new(add_user_input.clone());
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
                .find(|(_, v)| v.name == "user")
                .unwrap()
                .1
                .assigned_users
                .contains(&add_user_input.user_id)
        );

        net_handle.shutdown().await;
    }
}
