use aruna_core::admin_document_reducer::{AdminDocumentReducerError, AdminDocumentReducerState};
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentRoleDefinition, AdminDocumentTarget,
};
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{ADMIN_DOCUMENT_STATE_KEYSPACE, AUTH_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, stale_admin_document_conflict_delete_entries,
};
use aruna_core::structs::{Actor, AuthContext, Group, GroupAuthorizationDocument, RealmId, Role};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, GroupId, Key, KeySpace, TxnId, UserId};
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use std::collections::HashSet;
use thiserror::Error;
use ulid::Ulid;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::replicate_documents::replicate_documents_effect;
use aruna_core::structs::Permission;

#[derive(Clone, Debug, PartialEq)]
pub struct AddGroupRoleConfig {
    pub auth_context: AuthContext,
    pub actor: Actor,
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub role: Role,
}

#[derive(PartialEq)]
pub struct AddGroupRoleOperation {
    input: AddGroupRoleConfig,
    state: AddGroupRoleState,
    output: Option<Result<(Group, GroupAuthorizationDocument), AddGroupRoleError>>,
}

impl std::fmt::Debug for AddGroupRoleOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddRoleOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AddGroupRoleState {
    Init,
    Auth,
    StartTransaction,
    GetGroup {
        txn_id: TxnId,
    },
    GetAuthDocAndAdminState {
        txn_id: TxnId,
        group: Group,
    },
    WriteGroupAuthDocAndAdminState {
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        stale_conflict_delete_keys: Vec<(KeySpace, Vec<u8>)>,
    },
    DeleteStaleAdminConflicts {
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
    },
    CommitTransaction {
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
    },
    ScheduleAdminDocumentOutboxDrain {
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    },
    AnnounceGroupDoc {
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    },
    AnnounceAuthDoc {
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum AddGroupRoleError {
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
    #[error("No group found")]
    GroupNotFound,
    #[error(transparent)]
    CheckPermissionsError(#[from] AuthorizationError),
    #[error("Adding role to group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: AddGroupRoleState,
        expected: &'static str,
        got: String,
    },
}

impl AddGroupRoleOperation {
    pub fn new(input: AddGroupRoleConfig) -> Self {
        AddGroupRoleOperation {
            input,
            state: AddGroupRoleState::Init,
            output: None,
        }
    }

    fn handle_authorization(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return self.unexpected_event(
                AddGroupRoleState::Auth,
                "Event::SubOperation(SuboperationEvent::AuthorizationResult)",
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
    ) -> Result<Effects, AddGroupRoleError> {
        if auth_result? {
            self.state = AddGroupRoleState::StartTransaction;
            Ok(smallvec![Effect::Storage(
                StorageEffect::StartTransaction { read: false }
            )])
        } else {
            Err(AddGroupRoleError::Unauthorized)
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                AddGroupRoleState::StartTransaction,
                "Event::Storage(StorageEvent::TransactionStarted)",
                got,
            );
        };
        match self.emit_get_group(txn_id) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_get_group(&mut self, txn_id: TxnId) -> Result<Effects, AddGroupRoleError> {
        self.state = AddGroupRoleState::GetGroup { txn_id };
        let key = self.input.group_id.to_bytes().into();
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: GROUP_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_get_group(&mut self, event: Event, txn_id: TxnId) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::ReadResult)",
                got,
            );
        };

        match self.emit_get_auth_doc_and_admin_state(value, txn_id) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_get_auth_doc_and_admin_state(
        &mut self,
        group: Option<ByteView>,
        txn_id: TxnId,
    ) -> Result<Effects, AddGroupRoleError> {
        let group = Group::from_bytes(&group.ok_or_else(|| AddGroupRoleError::GroupNotFound)?)?;

        self.state = AddGroupRoleState::GetAuthDocAndAdminState { txn_id, group };

        let target = AdminDocumentTarget::Group {
            group_id: self.input.group_id,
        };
        let key = self.input.group_id.to_bytes().into();
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (AUTH_KEYSPACE.to_string(), key),
                (
                    ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                    admin_document_reducer_state_key(&target),
                ),
            ],
            txn_id: Some(txn_id),
        })])
    }

    fn handle_get_auth_doc_and_admin_state(
        &mut self,
        event: Event,
        txn_id: TxnId,
        group: Group,
    ) -> Effects {
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

        match self.emit_write_group_auth_doc_and_admin_state(
            txn_id,
            group,
            auth_doc_value.clone(),
            reducer_state_value.clone(),
        ) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_write_group_auth_doc_and_admin_state(
        &mut self,
        txn_id: TxnId,
        mut group: Group,
        auth_doc: Option<ByteView>,
        reducer_state_value: Option<ByteView>,
    ) -> Result<Effects, AddGroupRoleError> {
        let mut auth_doc = GroupAuthorizationDocument::from_bytes(
            &auth_doc.ok_or_else(|| AddGroupRoleError::GroupNotFound)?,
        )?;
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
        let admin_events = apply_admin_reducer_updates(&mut reducer_state, &self.input)?;
        materialize_group_role(&mut group, &mut auth_doc, &self.input.role, &reducer_state);

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
                auth_doc.group_id.to_bytes().into(),
                auth_doc.to_bytes(&self.input.actor)?.into(),
            ),
            (
                GROUP_KEYSPACE.to_string(),
                group.group_id.to_bytes().into(),
                group.to_bytes(&self.input.actor)?.into(),
            ),
            admin_document_reducer_state_write_entry(&reducer_state)?,
        ];
        let document_target = DocumentSyncTarget::GroupAuthorization {
            group_id: self.input.group_id,
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

        self.state = AddGroupRoleState::WriteGroupAuthDocAndAdminState {
            txn_id,
            group,
            auth_doc,
            admin_outbox_written: !admin_events.is_empty(),
            stale_conflict_delete_keys,
        };

        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_write_group_auth_doc_and_admin_state(
        &mut self,
        event: Event,
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
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
            self.state = AddGroupRoleState::DeleteStaleAdminConflicts {
                txn_id,
                group,
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

        self.emit_commit_transaction(txn_id, group, auth_doc, admin_outbox_written)
    }

    fn handle_delete_stale_admin_conflicts(
        &mut self,
        event: Event,
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
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

        self.emit_commit_transaction(txn_id, group, auth_doc, admin_outbox_written)
    }

    fn emit_commit_transaction(
        &mut self,
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
    ) -> Effects {
        self.state = AddGroupRoleState::CommitTransaction {
            txn_id,
            group,
            auth_doc,
            admin_outbox_written,
        };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit_transaction(
        &mut self,
        event: Event,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
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
            self.state = AddGroupRoleState::ScheduleAdminDocumentOutboxDrain { group, auth_doc };
            return smallvec![schedule_outbox_drain_effect()];
        }

        self.emit_announce_group_doc(group, auth_doc)
    }

    fn handle_schedule_admin_document_outbox_drain(
        &mut self,
        event: Event,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    ) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. })
            | Event::Task(TaskEvent::Error { .. }) => self.emit_announce_group_doc(group, auth_doc),
            other => self.unexpected_event(
                self.state.clone(),
                "admin document outbox drain timer schedule",
                format!("{other:?}"),
            ),
        }
    }

    fn emit_announce_group_doc(
        &mut self,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    ) -> Effects {
        self.state = AddGroupRoleState::AnnounceGroupDoc {
            group: group.clone(),
            auth_doc: auth_doc.clone(),
        };
        let document = DocumentSyncTarget::Group {
            group_id: group.group_id,
        };
        smallvec![replicate_documents_effect(
            self.input.actor.realm_id,
            self.input.actor.node_id,
            vec![document],
        )]
    }

    fn handle_announce_group_doc(
        &mut self,
        event: Event,
        group: Group,
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
            return self.fail(AddGroupRoleError::TopicAnnouncement(error));
        }
        self.state = AddGroupRoleState::AnnounceAuthDoc {
            group: group.clone(),
            auth_doc: auth_doc.clone(),
        };
        let document = DocumentSyncTarget::GroupAuthorization {
            group_id: group.group_id,
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
        group: Group,
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
            return self.fail(AddGroupRoleError::TopicAnnouncement(error));
        }
        self.state = AddGroupRoleState::Finish;
        self.output = Some(Ok((group, auth_doc)));
        smallvec![]
    }

    fn fail(&mut self, err: AddGroupRoleError) -> Effects {
        self.state = AddGroupRoleState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn fail_with_cleanup(&mut self, err: AddGroupRoleError, cleanup_effects: Effects) -> Effects {
        self.state = AddGroupRoleState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: AddGroupRoleState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            AddGroupRoleError::UnexpectedEvent {
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

impl Operation for AddGroupRoleOperation {
    type Output = (Group, GroupAuthorizationDocument);

    type Error = AddGroupRoleError;

    fn start(&mut self) -> Effects {
        self.state = AddGroupRoleState::Auth;

        let auth_config = CheckPermissionsConfig {
            auth_context: self.input.auth_context.clone(),
            path: format!(
                "/{}/g/{}/admin",
                self.input.realm_id,
                self.input.group_id.to_string()
            ),
            required_permission: Permission::WRITE,
        };
        let auth_operation =
            boxed_suboperation(CheckPermissionsOperation::new(auth_config), |result| {
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed: result })
            });

        smallvec![Effect::SubOperation(auth_operation)]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state.clone() {
            AddGroupRoleState::Auth => self.handle_authorization(event),
            AddGroupRoleState::StartTransaction => self.handle_start_transaction(event),
            AddGroupRoleState::GetGroup { txn_id } => self.handle_get_group(event, txn_id),
            AddGroupRoleState::GetAuthDocAndAdminState { txn_id, group } => {
                self.handle_get_auth_doc_and_admin_state(event, txn_id, group)
            }
            AddGroupRoleState::WriteGroupAuthDocAndAdminState {
                txn_id,
                group,
                auth_doc,
                admin_outbox_written,
                stale_conflict_delete_keys,
            } => self.handle_write_group_auth_doc_and_admin_state(
                event,
                txn_id,
                group,
                auth_doc,
                admin_outbox_written,
                stale_conflict_delete_keys,
            ),
            AddGroupRoleState::DeleteStaleAdminConflicts {
                txn_id,
                group,
                auth_doc,
                admin_outbox_written,
            } => self.handle_delete_stale_admin_conflicts(
                event,
                txn_id,
                group,
                auth_doc,
                admin_outbox_written,
            ),
            AddGroupRoleState::CommitTransaction {
                group,
                auth_doc,
                admin_outbox_written,
                ..
            } => self.handle_commit_transaction(event, group, auth_doc, admin_outbox_written),
            AddGroupRoleState::ScheduleAdminDocumentOutboxDrain { group, auth_doc } => {
                self.handle_schedule_admin_document_outbox_drain(event, group, auth_doc)
            }
            AddGroupRoleState::AnnounceGroupDoc { group, auth_doc } => {
                self.handle_announce_group_doc(event, group, auth_doc)
            }
            AddGroupRoleState::AnnounceAuthDoc { group, auth_doc } => {
                self.handle_announce_auth_doc(event, group, auth_doc)
            }
            AddGroupRoleState::Init | AddGroupRoleState::Finish | AddGroupRoleState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            AddGroupRoleState::Finish | AddGroupRoleState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or_else(|| AddGroupRoleError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            AddGroupRoleState::GetGroup { txn_id }
            | AddGroupRoleState::GetAuthDocAndAdminState { txn_id, .. }
            | AddGroupRoleState::WriteGroupAuthDocAndAdminState { txn_id, .. }
            | AddGroupRoleState::DeleteStaleAdminConflicts { txn_id, .. }
            | AddGroupRoleState::CommitTransaction { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }

            _ => smallvec![],
        }
    }
}

fn apply_admin_reducer_updates(
    state: &mut AdminDocumentReducerState,
    input: &AddGroupRoleConfig,
) -> Result<Vec<AdminDocumentEvent>, AdminDocumentReducerError> {
    let mut admin_events = Vec::new();
    let event = apply_admin_reducer_operation(
        state,
        &input.actor,
        AdminDocumentOperation::GroupRoleCreated {
            role: AdminDocumentRoleDefinition::from(&input.role),
        },
    )?;
    admin_events.push(event);

    for user_id in sorted_user_ids(&input.role.assigned_users) {
        let event = apply_admin_reducer_operation(
            state,
            &input.actor,
            AdminDocumentOperation::GroupRoleUserAssignmentAdded {
                role_id: input.role.role_id,
                user_id,
            },
        )?;
        admin_events.push(event);
    }

    Ok(admin_events)
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

fn materialize_group_role(
    group: &mut Group,
    auth_doc: &mut GroupAuthorizationDocument,
    role: &Role,
    reducer_state: &AdminDocumentReducerState,
) {
    if !reducer_state
        .materialized_group_roles()
        .contains(&role.role_id)
    {
        group.roles.remove(&role.role_id);
        auth_doc.roles.remove(&role.role_id);
        return;
    }

    group.roles.insert(role.role_id);
    auth_doc.roles.insert(role.role_id, role.clone());

    let materialized_assignments = reducer_state.materialized_group_role_user_assignments();
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

    use crate::add_group_role::{AddGroupRoleConfig, AddGroupRoleOperation, AddGroupRoleState};
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
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
    use aruna_core::operation::Operation;
    use aruna_core::storage_entries::{
        admin_document_reducer_conflict_key, admin_document_reducer_state_key,
    };
    use aruna_core::structs::{Actor, Group, GroupAuthorizationDocument, Permission, Role};
    use aruna_core::task::{TaskEvent, TaskKey};
    use aruna_core::types::TxnId;
    use aruna_core::{
        ADMIN_DOCUMENT_CONFLICT_KEYSPACE, ADMIN_DOCUMENT_STATE_KEYSPACE,
        DOCUMENT_SYNC_OUTBOX_KEYSPACE,
    };
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_add_role() {
        //
        // Inputs
        //
        let realm_id = aruna_core::structs::RealmId([0u8; 32]);
        let user_id = UserId::local(Ulid::new(), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let group_id = Ulid::new();
        let auth_doc =
            GroupAuthorizationDocument::new_default_group_doc(user_id, realm_id, group_id);
        let group = Group {
            display_name: "test".to_string(),
            group_id,
            realm_id,
            roles: auth_doc.roles.keys().copied().collect(),
        };
        let auth_context = aruna_core::structs::AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        };
        let actor = Actor {
            node_id,
            user_id,
            realm_id,
        };
        let role_id = Ulid::new();
        let add_role_input = AddGroupRoleConfig {
            auth_context: auth_context.clone(),
            actor: actor.clone(),
            realm_id,
            group_id,
            role: Role {
                role_id,
                name: "test_role".to_string(),
                permissions: HashMap::from([(
                    format!(
                        "{}/g/{}/meta/{}",
                        realm_id,
                        group_id.to_string(),
                        Ulid::new(),
                    ),
                    Permission::READ,
                )]),
                assigned_users: HashSet::from([user_id]),
            },
        };
        let target = AdminDocumentTarget::Group { group_id };
        let conflict_path = format!("group.roles.{role_id}.assigned_users.{user_id}");
        let add_dot = AdminDocumentDot {
            event_id: Ulid::from_bytes([2u8; 16]),
            origin_node_id: iroh::SecretKey::from_bytes(&[2u8; 32]).public(),
            origin_seq: 1,
        };
        let remove_dot = AdminDocumentDot {
            event_id: Ulid::from_bytes([3u8; 16]),
            origin_node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            origin_seq: 1,
        };
        let mut previous_state = AdminDocumentReducerState::new(target.clone());
        previous_state.clock.advance(add_dot.origin_node_id, 1);
        previous_state.clock.advance(remove_dot.origin_node_id, 1);
        previous_state.conflicts.insert(
            conflict_path.clone(),
            AdminDocumentConflict {
                path: conflict_path.clone(),
                values: vec![
                    AdminDocumentConflictValue {
                        value: Some(user_id.to_string()),
                        dot: add_dot,
                    },
                    AdminDocumentConflictValue {
                        value: None,
                        dot: remove_dot,
                    },
                ],
            },
        );
        let stale_conflict_delete_keys = vec![(
            ADMIN_DOCUMENT_CONFLICT_KEYSPACE.to_string(),
            admin_document_reducer_conflict_key(&target, &conflict_path)
                .as_ref()
                .to_vec(),
        )];

        //
        // Steps
        //
        let mut add_role_operation = AddGroupRoleOperation::new(add_role_input.clone());
        assert_eq!(add_role_operation.state, AddGroupRoleState::Init);

        let effects = add_role_operation.start();
        let auth_effect = effects.first().unwrap();
        assert!(matches!(auth_effect, Effect::SubOperation(_)));
        assert_eq!(add_role_operation.state, AddGroupRoleState::Auth);
        let effects = add_role_operation.step(Event::SubOperation(
            aruna_core::events::SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));

        let txn_effect = effects.first().unwrap();
        assert_eq!(
            txn_effect,
            &Effect::Storage(aruna_core::effects::StorageEffect::StartTransaction { read: false })
        );
        assert_eq!(
            add_role_operation.state,
            AddGroupRoleState::StartTransaction
        );

        let txn_id = TxnId::new();
        let effects = add_role_operation.step(Event::Storage(
            aruna_core::events::StorageEvent::TransactionStarted { txn_id },
        ));
        let get_group_effect = effects.first().unwrap();
        assert_eq!(
            get_group_effect,
            &Effect::Storage(aruna_core::effects::StorageEffect::Read {
                key_space: GROUP_KEYSPACE.to_string(),
                key: group_id.to_bytes().into(),
                txn_id: Some(txn_id)
            })
        );
        assert_eq!(
            add_role_operation.state,
            AddGroupRoleState::GetGroup { txn_id }
        );

        let effects = add_role_operation.step(Event::Storage(
            aruna_core::events::StorageEvent::ReadResult {
                key: group_id.to_bytes().into(),
                value: Some(group.to_bytes(&actor).unwrap().into()),
            },
        ));
        let get_auth_doc_and_admin_state_effect = effects.first().unwrap();
        assert_eq!(
            get_auth_doc_and_admin_state_effect,
            &Effect::Storage(aruna_core::effects::StorageEffect::BatchRead {
                reads: vec![
                    (AUTH_KEYSPACE.to_string(), group_id.to_bytes().into()),
                    (
                        ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                        admin_document_reducer_state_key(&target),
                    ),
                ],
                txn_id: Some(txn_id)
            })
        );
        assert_eq!(
            add_role_operation.state,
            AddGroupRoleState::GetAuthDocAndAdminState {
                txn_id,
                group: group.clone()
            }
        );

        let effects = add_role_operation.step(Event::Storage(
            aruna_core::events::StorageEvent::BatchReadResult {
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
            },
        ));
        let write_group_auth_doc_and_admin_state_effect = effects.first().unwrap();
        let mut mutated_group = group.clone();
        mutated_group.roles.insert(add_role_input.role.role_id);
        let mut mutated_auth_doc = auth_doc.clone();
        mutated_auth_doc
            .roles
            .insert(add_role_input.role.role_id, add_role_input.role.clone());
        match write_group_auth_doc_and_admin_state_effect {
            Effect::Storage(aruna_core::effects::StorageEffect::BatchWrite {
                writes,
                txn_id: effect_txn_id,
            }) => {
                assert_eq!(effect_txn_id, &Some(txn_id));
                let auth_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == AUTH_KEYSPACE)
                    .expect("auth doc write is included");
                let group_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == GROUP_KEYSPACE)
                    .expect("group doc write is included");
                let reducer_state_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == ADMIN_DOCUMENT_STATE_KEYSPACE)
                    .expect("reducer state write is included");
                let outbox_records: Vec<DocumentSyncOutboxRecord> = writes
                    .iter()
                    .filter(|(keyspace, _, _)| keyspace == DOCUMENT_SYNC_OUTBOX_KEYSPACE)
                    .map(|(_, _, value)| postcard::from_bytes(value.as_ref()).unwrap())
                    .collect();

                assert_eq!(auth_write.1, group_id.to_bytes().into());
                assert_eq!(group_write.1, group_id.to_bytes().into());
                let stored_auth_doc =
                    GroupAuthorizationDocument::from_bytes(auth_write.2.as_ref()).unwrap();
                let stored_group_doc = Group::from_bytes(group_write.2.as_ref()).unwrap();
                let reducer_state: aruna_core::admin_document_reducer::AdminDocumentReducerState =
                    postcard::from_bytes(reducer_state_write.2.as_ref()).unwrap();
                assert_eq!(stored_auth_doc, mutated_auth_doc);
                assert_eq!(stored_group_doc, mutated_group);
                assert!(
                    reducer_state
                        .materialized_group_roles()
                        .contains(&add_role_input.role.role_id)
                );
                assert!(!reducer_state.conflicts.contains_key(&conflict_path));
                assert!(outbox_records.iter().any(|record| {
                    record.target == (DocumentSyncTarget::GroupAuthorization { group_id })
                        && matches!(
                            &record.event,
                            DocumentSyncOutboxEvent::AdminOperation { event }
                                if event.target == target
                                    && matches!(
                                        &event.op,
                                        AdminDocumentOperation::GroupRoleCreated { role }
                                            if role == &AdminDocumentRoleDefinition::from(&add_role_input.role)
                                    )
                        )
                }));
            }
            other => panic!("unexpected group role write effect: {other:?}"),
        }
        assert_eq!(
            add_role_operation.state,
            AddGroupRoleState::WriteGroupAuthDocAndAdminState {
                txn_id,
                group: mutated_group.clone(),
                auth_doc: mutated_auth_doc.clone(),
                admin_outbox_written: true,
                stale_conflict_delete_keys: stale_conflict_delete_keys.clone(),
            }
        );

        assert!(mutated_group.roles.contains(&add_role_input.role.role_id));
        assert_eq!(
            mutated_auth_doc
                .roles
                .get(&add_role_input.role.role_id)
                .unwrap(),
            &add_role_input.role
        );

        let effects = add_role_operation.step(Event::Storage(
            aruna_core::events::StorageEvent::BatchWriteResult {
                entries: Vec::new(),
            },
        ));
        assert_eq!(
            effects.first().unwrap(),
            &Effect::Storage(StorageEffect::BatchDelete {
                deletes: vec![(
                    ADMIN_DOCUMENT_CONFLICT_KEYSPACE.to_string(),
                    admin_document_reducer_conflict_key(&target, &conflict_path),
                )],
                txn_id: Some(txn_id),
            })
        );

        let effects = add_role_operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        let commit_transaction_effect = effects.first().unwrap();
        match commit_transaction_effect {
            Effect::Storage(aruna_core::effects::StorageEffect::CommitTransaction {
                txn_id: effect_txn_id,
            }) => {
                assert_eq!(effect_txn_id, &txn_id);
            }
            other => panic!("unexpected create role effect: {other:?}"),
        }
        assert_eq!(
            add_role_operation.state,
            AddGroupRoleState::CommitTransaction {
                txn_id,
                group: mutated_group.clone(),
                auth_doc: mutated_auth_doc.clone(),
                admin_outbox_written: true,
            }
        );

        let effects = add_role_operation.step(Event::Storage(
            aruna_core::events::StorageEvent::TransactionCommitted { txn_id },
        ));
        assert!(matches!(effects.first(), Some(Effect::Task(_))));
        assert_eq!(
            add_role_operation.state,
            AddGroupRoleState::ScheduleAdminDocumentOutboxDrain {
                group: mutated_group.clone(),
                auth_doc: mutated_auth_doc.clone(),
            }
        );

        let effects = add_role_operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainDocumentSyncOutbox,
            after: std::time::Duration::ZERO,
        }));
        let announce_group_doc = effects.first().unwrap();
        assert!(matches!(announce_group_doc, Effect::SubOperation(_)));
        assert_eq!(
            add_role_operation.state,
            AddGroupRoleState::AnnounceGroupDoc {
                group: mutated_group.clone(),
                auth_doc: mutated_auth_doc.clone(),
            }
        );

        let effects =
            add_role_operation.step(Event::SubOperation(SubOperationEvent::DocumentSyncResult {
                result: Ok(()),
            }));
        let announce_auth_doc = effects.first().unwrap();
        assert!(matches!(announce_auth_doc, Effect::SubOperation(_)));
        assert_eq!(
            add_role_operation.state,
            AddGroupRoleState::AnnounceAuthDoc {
                group: mutated_group,
                auth_doc: mutated_auth_doc,
            }
        );

        let effects = add_role_operation.step(Event::SubOperation(
            aruna_core::events::SubOperationEvent::DocumentSyncResult { result: Ok(()) },
        ));
        assert!(effects.is_empty());
        assert_eq!(add_role_operation.state, AddGroupRoleState::Finish);
    }
}
