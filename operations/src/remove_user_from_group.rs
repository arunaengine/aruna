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
use aruna_core::structs::{
    Actor, AuthContext, GroupAuthorizationDocument, Permission, ResourceEvent,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, GroupId, KeySpace, RoleId, TxnId, UserId};
use aruna_core::util::unix_timestamp_millis;
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::HashSet;
use thiserror::Error;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::notifications::emit::emit_notifications_effect;
use crate::notifications::routing::{RoutingContext, route_resource_event};

#[derive(Clone, Debug, PartialEq)]
pub struct RemoveUserFromGroupInput {
    pub actor: Actor,
    pub group_id: GroupId,
    pub user_id: UserId,
    /// None removes the user from every role of the group.
    pub role_ids: Option<HashSet<RoleId>>,
}

#[derive(PartialEq)]
pub struct RemoveUserFromGroupOperation {
    input: RemoveUserFromGroupInput,
    state: RemoveUserFromGroupState,
    output: Option<Result<GroupAuthorizationDocument, RemoveUserFromGroupError>>,
}

impl std::fmt::Debug for RemoveUserFromGroupOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoveUserFromGroupOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RemoveUserFromGroupState {
    Init,
    Auth,
    StartTransaction,
    ReadAuthDocAndAdminState {
        txn_id: TxnId,
    },
    WriteAuthDocAndAdminState {
        txn_id: TxnId,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        stale_conflict_delete_keys: Vec<(KeySpace, Vec<u8>)>,
        was_member: bool,
    },
    DeleteStaleAdminConflicts {
        txn_id: TxnId,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        was_member: bool,
    },
    CommitTransaction {
        txn_id: TxnId,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        was_member: bool,
    },
    ScheduleAdminDocumentOutboxDrain {
        auth_doc: GroupAuthorizationDocument,
        was_member: bool,
    },
    EmitNotifications {
        auth_doc: GroupAuthorizationDocument,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RemoveUserFromGroupError {
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
    #[error("Invalid user id")]
    InvalidUserId,
    #[error("Authorization document not found")]
    AuthDocNotFound,
    #[error("cannot remove the last admin of a group")]
    LastAdmin,
    #[error(transparent)]
    CheckPermissionsError(#[from] AuthorizationError),
    #[error("Removing user from group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: RemoveUserFromGroupState,
        expected: &'static str,
        got: String,
    },
}

impl RemoveUserFromGroupOperation {
    pub fn new(input: RemoveUserFromGroupInput) -> Self {
        RemoveUserFromGroupOperation {
            input,
            state: RemoveUserFromGroupState::Init,
            output: None,
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                RemoveUserFromGroupState::StartTransaction,
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
                RemoveUserFromGroupState::Auth,
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
    ) -> Result<Effects, RemoveUserFromGroupError> {
        if auth_result? {
            self.state = RemoveUserFromGroupState::StartTransaction;
            Ok(smallvec![Effect::Storage(
                StorageEffect::StartTransaction { read: false }
            )])
        } else {
            Err(RemoveUserFromGroupError::Unauthorized)
        }
    }

    fn emit_read_auth_doc_and_admin_state(
        &mut self,
        txn_id: TxnId,
    ) -> Result<Effects, RemoveUserFromGroupError> {
        self.state = RemoveUserFromGroupState::ReadAuthDocAndAdminState { txn_id };
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
    ) -> Result<Effects, RemoveUserFromGroupError> {
        let mut auth_doc = GroupAuthorizationDocument::from_bytes(
            &auth_doc.ok_or_else(|| RemoveUserFromGroupError::AuthDocNotFound)?,
        )?;

        let was_member = auth_doc
            .roles
            .values()
            .any(|role| role.assigned_users.contains(&self.input.user_id));

        let guarded_admin_roles: Vec<RoleId> = auth_doc
            .roles
            .iter()
            .filter_map(|(role_id, role)| {
                (role.name == "admin" && role.assigned_users.contains(&self.input.user_id))
                    .then_some(*role_id)
            })
            .collect();

        let role_ids = match &self.input.role_ids {
            Some(role_ids) => {
                let mut sorted_role_ids: Vec<_> = role_ids.iter().copied().collect();
                sorted_role_ids.sort();
                for role_id in &sorted_role_ids {
                    let role = auth_doc
                        .roles
                        .get_mut(role_id)
                        .ok_or_else(|| RemoveUserFromGroupError::RoleNotFound)?;
                    role.assigned_users.remove(&self.input.user_id);
                }
                sorted_role_ids
            }
            None => {
                let mut sorted_role_ids = Vec::new();
                for (role_id, role) in auth_doc.roles.iter_mut() {
                    if role.assigned_users.remove(&self.input.user_id) {
                        sorted_role_ids.push(*role_id);
                    }
                }
                sorted_role_ids.sort();
                sorted_role_ids
            }
        };

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
        let admin_events = apply_admin_reducer_updates(
            &mut reducer_state,
            &self.input.actor,
            self.input.user_id,
            &role_ids,
        )?;

        let materialized_assignments = reducer_state.materialized_group_role_user_assignments();
        for role_id in &role_ids {
            let role = auth_doc
                .roles
                .get_mut(role_id)
                .ok_or_else(|| RemoveUserFromGroupError::RoleNotFound)?;
            if materialized_assignments
                .get(role_id)
                .is_some_and(|users| users.contains(&self.input.user_id))
            {
                role.assigned_users.insert(self.input.user_id);
            } else {
                role.assigned_users.remove(&self.input.user_id);
            }
        }

        // A group must always retain at least one admin, otherwise nobody
        // could manage it anymore. This also applies to self-leave.
        if guarded_admin_roles.iter().any(|role_id| {
            auth_doc
                .roles
                .get(role_id)
                .is_some_and(|role| role.assigned_users.is_empty())
        }) {
            return Err(RemoveUserFromGroupError::LastAdmin);
        }

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

        self.state = RemoveUserFromGroupState::WriteAuthDocAndAdminState {
            txn_id,
            auth_doc,
            admin_outbox_written: !admin_events.is_empty(),
            stale_conflict_delete_keys,
            was_member,
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
        admin_outbox_written: bool,
        stale_conflict_delete_keys: Vec<(KeySpace, Vec<u8>)>,
        was_member: bool,
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
            self.state = RemoveUserFromGroupState::DeleteStaleAdminConflicts {
                txn_id,
                auth_doc,
                admin_outbox_written,
                was_member,
            };
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes: stale_conflict_delete_keys
                    .into_iter()
                    .map(|(key_space, key)| (key_space, ByteView::from(key)))
                    .collect(),
                txn_id: Some(txn_id),
            })];
        }

        self.emit_commit_transaction(txn_id, auth_doc, admin_outbox_written, was_member)
    }

    fn handle_delete_stale_admin_conflicts(
        &mut self,
        event: Event,
        txn_id: TxnId,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        was_member: bool,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::BatchDeleteResult)",
                got,
            );
        };

        self.emit_commit_transaction(txn_id, auth_doc, admin_outbox_written, was_member)
    }

    fn emit_commit_transaction(
        &mut self,
        txn_id: TxnId,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        was_member: bool,
    ) -> Effects {
        self.state = RemoveUserFromGroupState::CommitTransaction {
            txn_id,
            auth_doc,
            admin_outbox_written,
            was_member,
        };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit_transaction(
        &mut self,
        event: Event,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        was_member: bool,
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
            self.state = RemoveUserFromGroupState::ScheduleAdminDocumentOutboxDrain {
                auth_doc,
                was_member,
            };
            return smallvec![schedule_outbox_drain_effect()];
        }

        self.emit_removed_notifications_or_finish(auth_doc, was_member)
    }

    fn handle_schedule_admin_document_outbox_drain(
        &mut self,
        event: Event,
        auth_doc: GroupAuthorizationDocument,
        was_member: bool,
    ) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. })
            | Event::Task(TaskEvent::Error { .. }) => {
                self.emit_removed_notifications_or_finish(auth_doc, was_member)
            }
            other => self.unexpected_event(
                self.state.clone(),
                "admin document outbox drain timer schedule",
                format!("{other:?}"),
            ),
        }
    }

    fn emit_removed_notifications_or_finish(
        &mut self,
        auth_doc: GroupAuthorizationDocument,
        was_member: bool,
    ) -> Effects {
        let still_member = auth_doc
            .roles
            .values()
            .any(|role| role.assigned_users.contains(&self.input.user_id));
        if was_member && !still_member {
            let records = route_resource_event(
                &ResourceEvent::GroupMemberRemoved {
                    group_id: self.input.group_id,
                    affected_user: self.input.user_id,
                    actor_user_id: self.input.actor.user_id,
                },
                RoutingContext {
                    group_auth: Some(&auth_doc),
                    realm_auth: None,
                },
                unix_timestamp_millis(),
            );
            if !records.is_empty() {
                self.state = RemoveUserFromGroupState::EmitNotifications { auth_doc };
                return smallvec![emit_notifications_effect(records)];
            }
        }

        self.state = RemoveUserFromGroupState::Finish;
        self.output = Some(Ok(auth_doc));
        smallvec![]
    }

    fn handle_emit_notifications(&mut self, auth_doc: GroupAuthorizationDocument) -> Effects {
        self.state = RemoveUserFromGroupState::Finish;
        self.output = Some(Ok(auth_doc));
        smallvec![]
    }

    fn fail(&mut self, err: RemoveUserFromGroupError) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(err, cleanup_effects)
    }

    fn fail_with_cleanup(
        &mut self,
        err: RemoveUserFromGroupError,
        cleanup_effects: Effects,
    ) -> Effects {
        self.state = RemoveUserFromGroupState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: RemoveUserFromGroupState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            RemoveUserFromGroupError::UnexpectedEvent {
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

impl Operation for RemoveUserFromGroupOperation {
    type Output = GroupAuthorizationDocument;

    type Error = RemoveUserFromGroupError;

    fn start(&mut self) -> Effects {
        if self.input.user_id.is_nil() {
            return self.fail(RemoveUserFromGroupError::InvalidUserId);
        }

        // Self-leave needs no admin permission; the last-admin guard still applies.
        if self.input.actor.user_id == self.input.user_id {
            self.state = RemoveUserFromGroupState::StartTransaction;
            return smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })];
        }

        self.state = RemoveUserFromGroupState::Auth;

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
            RemoveUserFromGroupState::Auth => self.handle_authorization(event),
            RemoveUserFromGroupState::StartTransaction => self.handle_start_transaction(event),
            RemoveUserFromGroupState::ReadAuthDocAndAdminState { txn_id } => {
                self.handle_read_auth_doc_and_admin_state(event, txn_id)
            }
            RemoveUserFromGroupState::WriteAuthDocAndAdminState {
                txn_id,
                auth_doc,
                admin_outbox_written,
                stale_conflict_delete_keys,
                was_member,
            } => self.handle_write_auth_doc_and_admin_state(
                event,
                txn_id,
                auth_doc,
                admin_outbox_written,
                stale_conflict_delete_keys,
                was_member,
            ),
            RemoveUserFromGroupState::DeleteStaleAdminConflicts {
                txn_id,
                auth_doc,
                admin_outbox_written,
                was_member,
            } => self.handle_delete_stale_admin_conflicts(
                event,
                txn_id,
                auth_doc,
                admin_outbox_written,
                was_member,
            ),
            RemoveUserFromGroupState::CommitTransaction {
                auth_doc,
                admin_outbox_written,
                was_member,
                ..
            } => self.handle_commit_transaction(event, auth_doc, admin_outbox_written, was_member),
            RemoveUserFromGroupState::ScheduleAdminDocumentOutboxDrain {
                auth_doc,
                was_member,
            } => self.handle_schedule_admin_document_outbox_drain(event, auth_doc, was_member),
            RemoveUserFromGroupState::EmitNotifications { auth_doc } => {
                self.handle_emit_notifications(auth_doc)
            }
            RemoveUserFromGroupState::Init
            | RemoveUserFromGroupState::Finish
            | RemoveUserFromGroupState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RemoveUserFromGroupState::Finish | RemoveUserFromGroupState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or_else(|| RemoveUserFromGroupError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            RemoveUserFromGroupState::ReadAuthDocAndAdminState { txn_id }
            | RemoveUserFromGroupState::WriteAuthDocAndAdminState { txn_id, .. }
            | RemoveUserFromGroupState::DeleteStaleAdminConflicts { txn_id, .. }
            | RemoveUserFromGroupState::CommitTransaction { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }

            _ => smallvec![],
        }
    }
}

fn apply_admin_reducer_updates(
    state: &mut AdminDocumentReducerState,
    actor: &Actor,
    user_id: UserId,
    role_ids: &[RoleId],
) -> Result<Vec<AdminDocumentEvent>, AdminDocumentReducerError> {
    let mut admin_events = Vec::new();
    for role_id in role_ids {
        if should_seed_group_role(state, *role_id) {
            let event = state.apply_operation(
                actor,
                AdminDocumentOperation::GroupRoleAdded { role_id: *role_id },
            )?;
            admin_events.push(event);
        }
        let event = state.apply_operation(
            actor,
            AdminDocumentOperation::GroupRoleUserAssignmentRemoved {
                role_id: *role_id,
                user_id,
            },
        )?;
        admin_events.push(event);
    }

    Ok(admin_events)
}

fn should_seed_group_role(state: &AdminDocumentReducerState, role_id: RoleId) -> bool {
    !state.materialized_group_roles().contains(&role_id)
        && !state
            .conflicts
            .contains_key(&format!("group.roles.{role_id}"))
}

#[cfg(test)]
pub mod test {
    use std::collections::{HashMap, HashSet};

    use aruna_core::UserId;
    use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
    use aruna_core::document::{
        DocumentSyncOutboxEvent, DocumentSyncOutboxRecord, DocumentSyncTarget,
    };
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, NOTIFICATION_OUTBOX_KEYSPACE};
    use aruna_core::operation::Operation;
    use aruna_core::storage_entries::admin_document_reducer_state_key;
    use aruna_core::structs::{
        Actor, Group, GroupAuthorizationDocument, NotificationOutboxRecord, NotificationRecord,
        RealmId,
    };
    use aruna_core::task::{TaskEvent, TaskKey};
    use aruna_core::types::{RoleId, TxnId};
    use aruna_core::{DOCUMENT_SYNC_OUTBOX_KEYSPACE, structs::Permission, structs::Role};
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    use crate::add_user_to_group::{AddUserToGroupInput, AddUserToGroupOperation};
    use crate::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};
    use crate::get_group::{GetGroupConfig, GetGroupOperation};
    use crate::remove_user_from_group::{
        RemoveUserFromGroupError, RemoveUserFromGroupInput, RemoveUserFromGroupOperation,
    };

    async fn test_context() -> (DriverContext, NetHandle, TempDir) {
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
        (context, net_handle, random_path)
    }

    async fn setup_group(context: &DriverContext) -> (Actor, Group, GroupAuthorizationDocument) {
        let realm_id = RealmId([0u8; 32]);
        let user_id = UserId::local(Ulid::new(), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let actor = Actor {
            node_id,
            user_id,
            realm_id,
        };

        let realm_operation = CreateRealmOperation::new(CreateRealmConfig {
            actor: actor.clone(),
            realm_description: "Test realm".to_string(),
            oidc_providers: Vec::new(),
        });
        drive(realm_operation, context).await.unwrap();

        let group_operation = CreateGroupOperation::new(CreateGroupConfig {
            actor: actor.clone(),
            display_name: "Test group".to_string(),
            owner_cap: None,
        });
        let (group, auth_doc) = drive(group_operation, context).await.unwrap();
        (actor, group, auth_doc)
    }

    fn role_ids_by_name(auth_doc: &GroupAuthorizationDocument, name: &str) -> HashSet<RoleId> {
        auth_doc
            .roles
            .iter()
            .filter_map(|(k, v)| (v.name == name).then_some(*k))
            .collect()
    }

    #[test]
    fn rejects_nil_user_id_as_removal_target() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let owner_id = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let role_id = Ulid::from_bytes([4u8; 16]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[5u8; 32]).public(),
            user_id: owner_id,
            realm_id,
        };
        let mut operation = RemoveUserFromGroupOperation::new(RemoveUserFromGroupInput {
            actor,
            group_id,
            user_id: UserId::nil(realm_id),
            role_ids: Some(HashSet::from([role_id])),
        });

        assert!(operation.start().is_empty());
        assert_eq!(
            operation.finalize(),
            Err(RemoveUserFromGroupError::InvalidUserId)
        );
    }

    #[test]
    fn queues_admin_operation_outbox_events_for_member_removal() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let owner_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let member_id = UserId::local(Ulid::from_bytes([4u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([5u8; 16]);
        let role_id = Ulid::from_bytes([6u8; 16]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
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
                    assigned_users: HashSet::from([member_id]),
                },
            )]),
        };
        let mut operation = RemoveUserFromGroupOperation::new(RemoveUserFromGroupInput {
            actor: actor.clone(),
            group_id,
            user_id: member_id,
            role_ids: Some(HashSet::from([role_id])),
        });

        let effects = operation
            .emit_write_auth_doc_and_admin_state(
                TxnId::new(),
                Some(auth_doc.to_bytes(&actor).unwrap().into()),
                None,
            )
            .unwrap();

        let (stored_auth_doc, outbox_records) = match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchWrite { writes, .. }) => {
                let auth_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == AUTH_KEYSPACE)
                    .expect("auth doc write is included");
                let outbox_records: Vec<DocumentSyncOutboxRecord> = writes
                    .iter()
                    .filter(|(keyspace, _, _)| keyspace == DOCUMENT_SYNC_OUTBOX_KEYSPACE)
                    .map(|(_, _, value)| postcard::from_bytes(value.as_ref()).unwrap())
                    .collect();
                (
                    GroupAuthorizationDocument::from_bytes(auth_write.2.as_ref()).unwrap(),
                    outbox_records,
                )
            }
            other => panic!("unexpected write effect: {other:?}"),
        };

        assert!(
            !stored_auth_doc.roles[&role_id]
                .assigned_users
                .contains(&member_id)
        );
        assert_eq!(outbox_records.len(), 2);
        assert!(outbox_records.iter().all(|record| {
            record.target == (DocumentSyncTarget::GroupAuthorization { group_id })
        }));
        let events: Vec<_> = outbox_records
            .iter()
            .map(|record| match &record.event {
                DocumentSyncOutboxEvent::AdminOperation { event } => event.as_ref(),
                other => panic!("unexpected outbox event: {other:?}"),
            })
            .collect();
        assert_eq!(events[0].target, AdminDocumentTarget::Group { group_id });
        assert!(matches!(
            &events[0].op,
            AdminDocumentOperation::GroupRoleAdded { role_id: event_role_id }
                if *event_role_id == role_id
        ));
        assert!(matches!(
            &events[1].op,
            AdminDocumentOperation::GroupRoleUserAssignmentRemoved {
                role_id: event_role_id,
                user_id: event_user_id,
            } if *event_role_id == role_id && *event_user_id == member_id
        ));
    }

    #[tokio::test]
    pub async fn test_remove_user_from_all_roles() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, auth_doc) = setup_group(&context).await;

        let member_id = UserId::local(Ulid::new(), actor.realm_id);
        let add_input = AddUserToGroupInput {
            actor: actor.clone(),
            group_id: group.group_id,
            user_id: member_id,
            role_ids: role_ids_by_name(&auth_doc, "user"),
        };
        let auth_doc = drive(AddUserToGroupOperation::new(add_input), &context)
            .await
            .unwrap();
        assert!(
            auth_doc
                .roles
                .values()
                .any(|role| role.assigned_users.contains(&member_id))
        );

        let remove_input = RemoveUserFromGroupInput {
            actor,
            group_id: group.group_id,
            user_id: member_id,
            role_ids: None,
        };
        let auth_doc = drive(RemoveUserFromGroupOperation::new(remove_input), &context)
            .await
            .unwrap();
        assert!(
            auth_doc
                .roles
                .values()
                .all(|role| !role.assigned_users.contains(&member_id))
        );

        net_handle.shutdown().await;
    }

    #[tokio::test]
    pub async fn test_last_admin_guard() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, _auth_doc) = setup_group(&context).await;

        let remove_input = RemoveUserFromGroupInput {
            actor: actor.clone(),
            group_id: group.group_id,
            user_id: actor.user_id,
            role_ids: None,
        };
        let result = drive(RemoveUserFromGroupOperation::new(remove_input), &context).await;
        assert_eq!(result.unwrap_err(), RemoveUserFromGroupError::LastAdmin);

        let (_, auth_doc) = drive(
            GetGroupOperation::new(GetGroupConfig {
                group_id: group.group_id,
            }),
            &context,
        )
        .await
        .unwrap();
        assert!(
            auth_doc
                .roles
                .values()
                .filter(|role| role.name == "admin")
                .all(|role| role.assigned_users.contains(&actor.user_id))
        );

        net_handle.shutdown().await;
    }

    #[tokio::test]
    pub async fn test_self_leave() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, auth_doc) = setup_group(&context).await;

        let member_id = UserId::local(Ulid::new(), actor.realm_id);
        let add_input = AddUserToGroupInput {
            actor: actor.clone(),
            group_id: group.group_id,
            user_id: member_id,
            role_ids: role_ids_by_name(&auth_doc, "user"),
        };
        drive(AddUserToGroupOperation::new(add_input), &context)
            .await
            .unwrap();

        let member_actor = Actor {
            node_id: actor.node_id,
            user_id: member_id,
            realm_id: actor.realm_id,
        };
        let remove_input = RemoveUserFromGroupInput {
            actor: member_actor,
            group_id: group.group_id,
            user_id: member_id,
            role_ids: None,
        };
        let auth_doc = drive(RemoveUserFromGroupOperation::new(remove_input), &context)
            .await
            .unwrap();
        assert!(
            auth_doc
                .roles
                .values()
                .all(|role| !role.assigned_users.contains(&member_id))
        );

        net_handle.shutdown().await;
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

    #[tokio::test]
    async fn remove_user_emits_removed_notification() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, auth_doc) = setup_group(&context).await;

        let member = UserId::local(Ulid::new(), actor.realm_id);
        drive(
            AddUserToGroupOperation::new(AddUserToGroupInput {
                actor: actor.clone(),
                group_id: group.group_id,
                user_id: member,
                role_ids: role_ids_by_name(&auth_doc, "user"),
            }),
            &context,
        )
        .await
        .unwrap();

        drive(
            RemoveUserFromGroupOperation::new(RemoveUserFromGroupInput {
                actor: actor.clone(),
                group_id: group.group_id,
                user_id: member,
                role_ids: None,
            }),
            &context,
        )
        .await
        .unwrap();

        let removed: Vec<_> = read_notification_outbox(&context)
            .await
            .into_iter()
            .filter(|record| record.kind.name() == "removed_from_group")
            .collect();
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].recipient, member);

        net_handle.shutdown().await;
    }

    #[test]
    fn host_ok_independent_of_emit() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let owner_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let member_id = UserId::local(Ulid::from_bytes([4u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([5u8; 16]);
        let role_id = Ulid::from_bytes([6u8; 16]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
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
                    assigned_users: HashSet::from([member_id]),
                },
            )]),
        };
        let target = AdminDocumentTarget::Group { group_id };

        let mut operation = RemoveUserFromGroupOperation::new(RemoveUserFromGroupInput {
            actor: actor.clone(),
            group_id,
            user_id: member_id,
            role_ids: Some(HashSet::from([role_id])),
        });
        operation.start();
        operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));
        let txn_id = TxnId::new();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    group_id.to_bytes().into(),
                    Some(auth_doc.to_bytes(&actor).unwrap().into()),
                ),
                (admin_document_reducer_state_key(&target), None),
            ],
        }));
        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        let effects = operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainDocumentSyncOutbox,
            after: std::time::Duration::ZERO,
        }));
        assert!(matches!(effects.first(), Some(Effect::SubOperation(_))));
        assert!(!operation.is_complete());

        let effects = operation.step(Event::Search());
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        let result = operation.finalize().unwrap();
        assert!(!result.roles[&role_id].assigned_users.contains(&member_id));
    }

    #[tokio::test]
    async fn removing_non_member_emits_nothing() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, _auth_doc) = setup_group(&context).await;

        let stranger = UserId::local(Ulid::new(), actor.realm_id);
        drive(
            RemoveUserFromGroupOperation::new(RemoveUserFromGroupInput {
                actor: actor.clone(),
                group_id: group.group_id,
                user_id: stranger,
                role_ids: None,
            }),
            &context,
        )
        .await
        .unwrap();

        assert!(read_notification_outbox(&context).await.is_empty());

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn revoking_one_role_while_still_member_emits_nothing() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, auth_doc) = setup_group(&context).await;

        let member = UserId::local(Ulid::new(), actor.realm_id);
        let mut both_roles = role_ids_by_name(&auth_doc, "user");
        both_roles.extend(role_ids_by_name(&auth_doc, "admin"));
        drive(
            AddUserToGroupOperation::new(AddUserToGroupInput {
                actor: actor.clone(),
                group_id: group.group_id,
                user_id: member,
                role_ids: both_roles,
            }),
            &context,
        )
        .await
        .unwrap();

        drive(
            RemoveUserFromGroupOperation::new(RemoveUserFromGroupInput {
                actor: actor.clone(),
                group_id: group.group_id,
                user_id: member,
                role_ids: Some(role_ids_by_name(&auth_doc, "user")),
            }),
            &context,
        )
        .await
        .unwrap();

        assert!(
            read_notification_outbox(&context)
                .await
                .into_iter()
                .all(|record| record.kind.name() != "removed_from_group")
        );

        net_handle.shutdown().await;
    }
}
