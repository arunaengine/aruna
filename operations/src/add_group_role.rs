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
use aruna_core::util::unix_timestamp_millis;
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use std::collections::HashSet;
use thiserror::Error;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::notifications::emit::emit_notifications_effect;
use crate::notifications::routing::{RoutingContext, route_resource_event};
use crate::replicate_documents::replicate_documents_effect;
use aruna_core::structs::Permission;
use aruna_core::structs::ResourceEvent;

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
        new_members: Vec<UserId>,
    },
    DeleteStaleAdminConflicts {
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        new_members: Vec<UserId>,
    },
    CommitTransaction {
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        new_members: Vec<UserId>,
    },
    ScheduleAdminDocumentOutboxDrain {
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        new_members: Vec<UserId>,
    },
    AnnounceGroupDoc {
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        new_members: Vec<UserId>,
    },
    AnnounceAuthDoc {
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        new_members: Vec<UserId>,
    },
    EmitNotifications {
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
    #[error("Invalid public role")]
    InvalidPublicRole,
    #[error("Invalid assigned user")]
    InvalidAssignedUser,
    #[error("Reserved role name")]
    ReservedRoleName,
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

const RESERVED_GROUP_ROLE_NAMES: &[&str] = &["admin", "user"];

fn is_reserved_group_role_name(name: &str) -> bool {
    RESERVED_GROUP_ROLE_NAMES.contains(&name.trim())
}

impl AddGroupRoleOperation {
    pub fn new(input: AddGroupRoleConfig) -> Self {
        AddGroupRoleOperation {
            input,
            state: AddGroupRoleState::Init,
            output: None,
        }
    }

    fn validate_role(&self) -> Result<(), AddGroupRoleError> {
        if is_reserved_group_role_name(&self.input.role.name) {
            return Err(AddGroupRoleError::ReservedRoleName);
        }

        if self
            .input
            .role
            .assigned_users
            .iter()
            .any(|user| user.is_nil() && !user.is_nil_in(self.input.realm_id))
        {
            return Err(AddGroupRoleError::InvalidAssignedUser);
        }

        if self.input.role.is_public(self.input.realm_id)
            && self
                .input
                .role
                .permissions
                .values()
                .any(|permission| permission != &Permission::READ)
        {
            return Err(AddGroupRoleError::InvalidPublicRole);
        }

        Ok(())
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
        let admin_events = apply_admin_reducer_updates(&mut reducer_state, &self.input)?;
        let members_before = group_members(&auth_doc);
        materialize_group_role(&mut group, &mut auth_doc, &self.input.role, &reducer_state);
        let new_members = newly_materialized_members(&members_before, &auth_doc, &self.input.role);

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
                false,
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
            new_members,
        };

        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_write_group_auth_doc_and_admin_state(
        &mut self,
        event: Event,
        state: AddGroupRoleState,
    ) -> Effects {
        let AddGroupRoleState::WriteGroupAuthDocAndAdminState {
            txn_id,
            group,
            auth_doc,
            admin_outbox_written,
            stale_conflict_delete_keys,
            new_members,
        } = state
        else {
            unreachable!("handler only accepts WriteGroupAuthDocAndAdminState");
        };

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
                new_members,
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

        self.emit_commit_transaction(txn_id, group, auth_doc, admin_outbox_written, new_members)
    }

    fn handle_delete_stale_admin_conflicts(
        &mut self,
        event: Event,
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        new_members: Vec<UserId>,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::BatchDeleteResult)",
                got,
            );
        };

        self.emit_commit_transaction(txn_id, group, auth_doc, admin_outbox_written, new_members)
    }

    fn emit_commit_transaction(
        &mut self,
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        new_members: Vec<UserId>,
    ) -> Effects {
        self.state = AddGroupRoleState::CommitTransaction {
            txn_id,
            group,
            auth_doc,
            admin_outbox_written,
            new_members,
        };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit_transaction(
        &mut self,
        event: Event,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        new_members: Vec<UserId>,
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
            self.state = AddGroupRoleState::ScheduleAdminDocumentOutboxDrain {
                group,
                auth_doc,
                new_members,
            };
            return smallvec![schedule_outbox_drain_effect()];
        }

        self.emit_announce_group_doc(group, auth_doc, new_members)
    }

    fn handle_schedule_admin_document_outbox_drain(
        &mut self,
        event: Event,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        new_members: Vec<UserId>,
    ) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. })
            | Event::Task(TaskEvent::Error { .. }) => {
                self.emit_membership_notifications_or_finish(group, auth_doc, new_members)
            }
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
        new_members: Vec<UserId>,
    ) -> Effects {
        self.state = AddGroupRoleState::AnnounceGroupDoc {
            group: group.clone(),
            auth_doc: auth_doc.clone(),
            new_members,
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
        new_members: Vec<UserId>,
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
            new_members,
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
        new_members: Vec<UserId>,
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
        self.emit_membership_notifications_or_finish(group, auth_doc, new_members)
    }

    fn emit_membership_notifications_or_finish(
        &mut self,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        new_members: Vec<UserId>,
    ) -> Effects {
        let records = new_members
            .into_iter()
            .flat_map(|affected_user| {
                route_resource_event(
                    &ResourceEvent::GroupMemberAdded {
                        group_id: self.input.group_id,
                        affected_user,
                        actor_user_id: self.input.actor.user_id,
                    },
                    RoutingContext {
                        group_auth: Some(&auth_doc),
                        realm_auth: None,
                    },
                    unix_timestamp_millis(),
                )
            })
            .collect::<Vec<_>>();
        if !records.is_empty() {
            self.state = AddGroupRoleState::EmitNotifications { group, auth_doc };
            return smallvec![emit_notifications_effect(records)];
        }

        self.state = AddGroupRoleState::Finish;
        self.output = Some(Ok((group, auth_doc)));
        smallvec![]
    }

    fn handle_emit_notifications(
        &mut self,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    ) -> Effects {
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
        if let Err(error) = self.validate_role() {
            return self.fail(error);
        }

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
            state @ AddGroupRoleState::WriteGroupAuthDocAndAdminState { .. } => {
                self.handle_write_group_auth_doc_and_admin_state(event, state)
            }
            AddGroupRoleState::DeleteStaleAdminConflicts {
                txn_id,
                group,
                auth_doc,
                admin_outbox_written,
                new_members,
            } => self.handle_delete_stale_admin_conflicts(
                event,
                txn_id,
                group,
                auth_doc,
                admin_outbox_written,
                new_members,
            ),
            AddGroupRoleState::CommitTransaction {
                group,
                auth_doc,
                admin_outbox_written,
                new_members,
                ..
            } => self.handle_commit_transaction(
                event,
                group,
                auth_doc,
                admin_outbox_written,
                new_members,
            ),
            AddGroupRoleState::ScheduleAdminDocumentOutboxDrain {
                group,
                auth_doc,
                new_members,
            } => self.handle_schedule_admin_document_outbox_drain(
                event,
                group,
                auth_doc,
                new_members,
            ),
            AddGroupRoleState::AnnounceGroupDoc {
                group,
                auth_doc,
                new_members,
            } => self.handle_announce_group_doc(event, group, auth_doc, new_members),
            AddGroupRoleState::AnnounceAuthDoc {
                group,
                auth_doc,
                new_members,
            } => self.handle_announce_auth_doc(event, group, auth_doc, new_members),
            AddGroupRoleState::EmitNotifications { group, auth_doc } => {
                self.handle_emit_notifications(group, auth_doc)
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
    let event = state.apply_operation(
        &input.actor,
        AdminDocumentOperation::GroupRoleCreated {
            role: AdminDocumentRoleDefinition::from(&input.role),
        },
    )?;
    admin_events.push(event);

    for user_id in sorted_user_ids(&input.role.assigned_users) {
        let event = state.apply_operation(
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

fn group_members(auth_doc: &GroupAuthorizationDocument) -> HashSet<UserId> {
    auth_doc
        .roles
        .values()
        .flat_map(|role| role.assigned_users.iter().copied())
        .filter(|user| !user.is_nil())
        .collect()
}

fn newly_materialized_members(
    members_before: &HashSet<UserId>,
    auth_doc: &GroupAuthorizationDocument,
    role: &Role,
) -> Vec<UserId> {
    sorted_user_ids(&role.assigned_users)
        .into_iter()
        .filter(|user| {
            !user.is_nil()
                && !members_before.contains(user)
                && auth_doc
                    .roles
                    .values()
                    .any(|role| role.assigned_users.contains(user))
        })
        .collect()
}

fn sorted_user_ids(user_ids: &HashSet<UserId>) -> Vec<UserId> {
    let mut user_ids: Vec<_> = user_ids.iter().copied().collect();
    user_ids.sort();
    user_ids
}

#[cfg(test)]
pub mod test {
    use std::collections::{HashMap, HashSet};

    use crate::add_group_role::{
        AddGroupRoleConfig, AddGroupRoleError, AddGroupRoleOperation, AddGroupRoleState,
    };
    use crate::add_user_to_group::{AddUserToGroupInput, AddUserToGroupOperation};
    use crate::create_group::{CreateGroupConfig, CreateGroupOperation};
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
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, NOTIFICATION_OUTBOX_KEYSPACE};
    use aruna_core::operation::Operation;
    use aruna_core::storage_entries::{
        admin_document_reducer_conflict_key, admin_document_reducer_state_key,
    };
    use aruna_core::structs::{
        Actor, AuthContext, Group, GroupAuthorizationDocument, NotificationOutboxRecord,
        NotificationRecord, Permission, RealmId, Role,
    };
    use aruna_core::task::{TaskEvent, TaskKey};
    use aruna_core::types::{RoleId, TxnId};
    use aruna_core::{
        ADMIN_DOCUMENT_CONFLICT_KEYSPACE, ADMIN_DOCUMENT_STATE_KEYSPACE,
        DOCUMENT_SYNC_OUTBOX_KEYSPACE,
    };
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    fn node(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    async fn test_context() -> (DriverContext, TempDir) {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
        };
        (context, tempdir)
    }

    async fn setup_group(context: &DriverContext) -> (Actor, Group, GroupAuthorizationDocument) {
        let realm_id = RealmId([0u8; 32]);
        let actor = Actor {
            node_id: node(1),
            user_id: UserId::local(Ulid::new(), realm_id),
            realm_id,
        };
        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: actor.clone(),
                realm_description: "Test realm".to_string(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            context,
        )
        .await
        .unwrap();
        let (group, auth_doc) = drive(
            CreateGroupOperation::new(CreateGroupConfig {
                actor: actor.clone(),
                display_name: "Test group".to_string(),
                owner_cap: None,
            }),
            context,
        )
        .await
        .unwrap();
        (actor, group, auth_doc)
    }

    fn auth_context(actor: &Actor) -> AuthContext {
        AuthContext {
            user_id: actor.user_id,
            realm_id: actor.realm_id,
            path_restrictions: None,
        }
    }

    fn role_ids_by_name(auth_doc: &GroupAuthorizationDocument, name: &str) -> HashSet<RoleId> {
        auth_doc
            .roles
            .iter()
            .filter_map(|(id, role)| (role.name == name).then_some(*id))
            .collect()
    }

    async fn read_notification_outbox(context: &DriverContext) -> Vec<NotificationRecord> {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1024,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(_, value)| {
                    postcard::from_bytes::<NotificationOutboxRecord>(&value)
                        .unwrap()
                        .record
                })
                .collect(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    #[test]
    fn rejects_reserved_role_names() {
        let realm_id = aruna_core::structs::RealmId([1u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
            user_id,
            realm_id,
        };

        for name in ["admin", "user", " admin "] {
            let mut operation = AddGroupRoleOperation::new(AddGroupRoleConfig {
                auth_context: aruna_core::structs::AuthContext {
                    user_id,
                    realm_id,
                    path_restrictions: None,
                },
                actor: actor.clone(),
                realm_id,
                group_id,
                role: Role {
                    role_id: Ulid::new(),
                    name: name.to_string(),
                    permissions: HashMap::from([(
                        format!("/{realm_id}/g/{group_id}/data/**"),
                        Permission::READ,
                    )]),
                    assigned_users: HashSet::new(),
                },
            });

            assert!(operation.start().is_empty());
            assert_eq!(
                operation.finalize(),
                Err(AddGroupRoleError::ReservedRoleName)
            );
        }
    }

    #[test]
    fn rejects_public_roles_with_write_or_deny_permissions() {
        let realm_id = aruna_core::structs::RealmId([1u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
            user_id,
            realm_id,
        };

        for permission in [Permission::WRITE, Permission::DENY] {
            let mut operation = AddGroupRoleOperation::new(AddGroupRoleConfig {
                auth_context: aruna_core::structs::AuthContext {
                    user_id,
                    realm_id,
                    path_restrictions: None,
                },
                actor: actor.clone(),
                realm_id,
                group_id,
                role: Role {
                    role_id: Ulid::new(),
                    name: "public".to_string(),
                    permissions: HashMap::from([(
                        format!("/{realm_id}/g/{group_id}/data/**"),
                        permission,
                    )]),
                    assigned_users: HashSet::from([UserId::nil(realm_id)]),
                },
            });

            assert!(operation.start().is_empty());
            assert_eq!(
                operation.finalize(),
                Err(AddGroupRoleError::InvalidPublicRole)
            );
        }
    }

    #[test]
    fn rejects_foreign_nil_assigned_users() {
        let realm_id = aruna_core::structs::RealmId([1u8; 32]);
        let other_realm_id = aruna_core::structs::RealmId([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([4u8; 16]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[5u8; 32]).public(),
            user_id,
            realm_id,
        };
        let mut operation = AddGroupRoleOperation::new(AddGroupRoleConfig {
            auth_context: aruna_core::structs::AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
            },
            actor,
            realm_id,
            group_id,
            role: Role {
                role_id: Ulid::new(),
                name: "foreign-nil".to_string(),
                permissions: HashMap::from([(
                    format!("/{realm_id}/g/{group_id}/data/**"),
                    Permission::READ,
                )]),
                assigned_users: HashSet::from([UserId::nil(other_realm_id)]),
            },
        });

        assert!(operation.start().is_empty());
        assert_eq!(
            operation.finalize(),
            Err(AddGroupRoleError::InvalidAssignedUser)
        );
    }

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
            owner: user_id,
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
                new_members: Vec::new(),
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
                new_members: Vec::new(),
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
                new_members: Vec::new(),
            }
        );

        let effects = add_role_operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainDocumentSyncOutbox,
            after: std::time::Duration::ZERO,
        }));
        assert!(effects.is_empty());
        assert_eq!(add_role_operation.state, AddGroupRoleState::Finish);
        assert_eq!(
            add_role_operation.finalize().unwrap(),
            (mutated_group, mutated_auth_doc)
        );
    }

    #[tokio::test]
    async fn assigned_users_emit_membership_notifications_for_new_members() {
        let (context, _tempdir) = test_context().await;
        let (actor, group, auth_doc) = setup_group(&context).await;
        let second_admin = UserId::local(Ulid::new(), actor.realm_id);
        drive(
            AddUserToGroupOperation::new(AddUserToGroupInput {
                actor: actor.clone(),
                group_id: group.group_id,
                user_id: second_admin,
                role_ids: role_ids_by_name(&auth_doc, "admin"),
            }),
            &context,
        )
        .await
        .unwrap();

        let member = UserId::local(Ulid::new(), actor.realm_id);
        drive(
            AddGroupRoleOperation::new(AddGroupRoleConfig {
                auth_context: auth_context(&actor),
                actor: actor.clone(),
                realm_id: actor.realm_id,
                group_id: group.group_id,
                role: Role {
                    role_id: Ulid::new(),
                    name: "custom_member".to_string(),
                    permissions: HashMap::from([(
                        format!("/{}/g/{}/data/**", actor.realm_id, group.group_id),
                        Permission::READ,
                    )]),
                    assigned_users: HashSet::from([member]),
                },
            }),
            &context,
        )
        .await
        .unwrap();

        let pairs: HashSet<(UserId, &'static str)> = read_notification_outbox(&context)
            .await
            .iter()
            .map(|record| (record.recipient, record.kind.name()))
            .collect();
        assert_eq!(
            pairs,
            HashSet::from([
                (second_admin, "added_to_group"),
                (member, "added_to_group"),
                (second_admin, "group_member_added"),
            ])
        );
    }

    #[tokio::test]
    async fn public_role_nil_assignment_emits_no_membership_notifications() {
        let (context, _tempdir) = test_context().await;
        let (actor, group, _auth_doc) = setup_group(&context).await;

        drive(
            AddGroupRoleOperation::new(AddGroupRoleConfig {
                auth_context: auth_context(&actor),
                actor: actor.clone(),
                realm_id: actor.realm_id,
                group_id: group.group_id,
                role: Role {
                    role_id: Ulid::new(),
                    name: "public_viewer".to_string(),
                    permissions: HashMap::from([(
                        format!("/{}/g/{}/data/**", actor.realm_id, group.group_id),
                        Permission::READ,
                    )]),
                    assigned_users: HashSet::from([UserId::nil(actor.realm_id)]),
                },
            }),
            &context,
        )
        .await
        .unwrap();

        assert!(read_notification_outbox(&context).await.is_empty());
    }
}
