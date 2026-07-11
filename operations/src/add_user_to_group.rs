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
use aruna_core::types::{Effects, GroupId, Key, KeySpace, RoleId, TxnId, UserId};
use aruna_core::util::unix_timestamp_millis;
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::HashSet;
use thiserror::Error;
use ulid::Ulid;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::notifications::emit::emit_notifications_effect;
use crate::notifications::routing::{RoutingContext, route_resource_event};
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
        admin_outbox_written: bool,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
        newly_added: bool,
    },
    DeleteStaleAdminConflicts {
        txn_id: TxnId,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        newly_added: bool,
    },
    CommitTransaction {
        txn_id: TxnId,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        newly_added: bool,
    },
    ScheduleAdminDocumentOutboxDrain {
        auth_doc: GroupAuthorizationDocument,
        newly_added: bool,
    },
    AnnounceAuthDoc {
        auth_doc: GroupAuthorizationDocument,
        newly_added: bool,
    },
    EmitNotifications {
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
    #[error("Invalid user id")]
    InvalidUserId,
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
        let admin_events = apply_admin_reducer_updates(&mut reducer_state, &self.input, &role_ids)?;

        let was_member = auth_doc
            .roles
            .values()
            .any(|role| role.assigned_users.contains(&self.input.user_id));

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
        let newly_added = !was_member
            && auth_doc
                .roles
                .values()
                .any(|role| role.assigned_users.contains(&self.input.user_id));

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

        self.state = AddUserToGroupState::WriteAuthDocAndAdminState {
            txn_id,
            auth_doc,
            admin_outbox_written: !admin_events.is_empty(),
            stale_conflict_deletes,
            newly_added,
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
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
        newly_added: bool,
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
            self.state = AddUserToGroupState::DeleteStaleAdminConflicts {
                txn_id,
                auth_doc,
                admin_outbox_written,
                newly_added,
            };
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes: stale_conflict_deletes,
                txn_id: Some(txn_id),
            })];
        }

        self.emit_commit_transaction(txn_id, auth_doc, admin_outbox_written, newly_added)
    }

    fn handle_delete_stale_admin_conflicts(
        &mut self,
        event: Event,
        txn_id: TxnId,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        newly_added: bool,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::BatchDeleteResult)",
                got,
            );
        };

        self.emit_commit_transaction(txn_id, auth_doc, admin_outbox_written, newly_added)
    }

    fn emit_commit_transaction(
        &mut self,
        txn_id: TxnId,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        newly_added: bool,
    ) -> Effects {
        self.state = AddUserToGroupState::CommitTransaction {
            txn_id,
            auth_doc,
            admin_outbox_written,
            newly_added,
        };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit_transaction(
        &mut self,
        event: Event,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        newly_added: bool,
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
            self.state = AddUserToGroupState::ScheduleAdminDocumentOutboxDrain {
                auth_doc,
                newly_added,
            };
            return smallvec![schedule_outbox_drain_effect()];
        }

        self.emit_announce_auth_doc(auth_doc, newly_added)
    }

    fn handle_schedule_admin_document_outbox_drain(
        &mut self,
        event: Event,
        auth_doc: GroupAuthorizationDocument,
        newly_added: bool,
    ) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. })
            | Event::Task(TaskEvent::Error { .. }) => {
                self.emit_membership_notifications_or_finish(auth_doc, newly_added)
            }
            other => self.unexpected_event(
                self.state.clone(),
                "admin document outbox drain timer schedule",
                format!("{other:?}"),
            ),
        }
    }

    fn emit_announce_auth_doc(
        &mut self,
        auth_doc: GroupAuthorizationDocument,
        newly_added: bool,
    ) -> Effects {
        self.state = AddUserToGroupState::AnnounceAuthDoc {
            auth_doc: auth_doc.clone(),
            newly_added,
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
        newly_added: bool,
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
        self.emit_membership_notifications_or_finish(auth_doc, newly_added)
    }

    fn emit_membership_notifications_or_finish(
        &mut self,
        auth_doc: GroupAuthorizationDocument,
        newly_added: bool,
    ) -> Effects {
        if newly_added {
            let records = route_resource_event(
                &ResourceEvent::GroupMemberAdded {
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
                self.state = AddUserToGroupState::EmitNotifications { auth_doc };
                return smallvec![emit_notifications_effect(records)];
            }
        }

        self.state = AddUserToGroupState::Finish;
        self.output = Some(Ok(auth_doc));
        smallvec![]
    }

    fn handle_emit_notifications(&mut self, auth_doc: GroupAuthorizationDocument) -> Effects {
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
        if self.input.user_id.is_nil() {
            return self.fail(AddUserToGroupError::InvalidUserId);
        }

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
                admin_outbox_written,
                stale_conflict_deletes,
                newly_added,
            } => self.handle_write_auth_doc_and_admin_state(
                event,
                txn_id,
                auth_doc,
                admin_outbox_written,
                stale_conflict_deletes,
                newly_added,
            ),
            AddUserToGroupState::DeleteStaleAdminConflicts {
                txn_id,
                auth_doc,
                admin_outbox_written,
                newly_added,
            } => self.handle_delete_stale_admin_conflicts(
                event,
                txn_id,
                auth_doc,
                admin_outbox_written,
                newly_added,
            ),
            AddUserToGroupState::CommitTransaction {
                auth_doc,
                admin_outbox_written,
                newly_added,
                ..
            } => self.handle_commit_transaction(event, auth_doc, admin_outbox_written, newly_added),
            AddUserToGroupState::ScheduleAdminDocumentOutboxDrain {
                auth_doc,
                newly_added,
            } => self.handle_schedule_admin_document_outbox_drain(event, auth_doc, newly_added),
            AddUserToGroupState::AnnounceAuthDoc {
                auth_doc,
                newly_added,
            } => self.handle_announce_auth_doc(event, auth_doc, newly_added),
            AddUserToGroupState::EmitNotifications { auth_doc } => {
                self.handle_emit_notifications(auth_doc)
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
) -> Result<Vec<AdminDocumentEvent>, AdminDocumentReducerError> {
    let mut admin_events = Vec::new();
    for role_id in role_ids {
        if should_seed_group_role(state, *role_id) {
            let event = apply_admin_reducer_operation(
                state,
                &input.actor,
                AdminDocumentOperation::GroupRoleAdded { role_id: *role_id },
            )?;
            admin_events.push(event);
        }
        let event = apply_admin_reducer_operation(
            state,
            &input.actor,
            AdminDocumentOperation::GroupRoleUserAssignmentAdded {
                role_id: *role_id,
                user_id: input.user_id,
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

fn apply_admin_reducer_operation(
    state: &mut AdminDocumentReducerState,
    actor: &Actor,
    op: AdminDocumentOperation,
) -> Result<AdminDocumentEvent, AdminDocumentReducerError> {
    let observed = state.clock.clone();
    let event = AdminDocumentEvent {
        event_id: Ulid::r#gen(),
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
    use aruna_core::keyspaces::NOTIFICATION_OUTBOX_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::storage_entries::{
        admin_document_reducer_conflict_key, admin_document_reducer_state_key,
    };
    use aruna_core::structs::{
        Actor, Group, GroupAuthorizationDocument, NotificationOutboxRecord, NotificationRecord,
        Permission, RealmId, Role,
    };
    use aruna_core::task::TaskEvent;
    use aruna_core::task::TaskKey;
    use aruna_core::types::{RoleId, TxnId};
    use aruna_core::{
        ADMIN_DOCUMENT_CONFLICT_KEYSPACE, ADMIN_DOCUMENT_STATE_KEYSPACE, AUTH_KEYSPACE,
        DOCUMENT_SYNC_OUTBOX_KEYSPACE,
    };
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    use crate::add_user_to_group::{
        AddUserToGroupError, AddUserToGroupInput, AddUserToGroupOperation, AddUserToGroupState,
    };
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
            equivalent_value_dots: BTreeMap::new(),
        }
    }

    fn reducer_state_with_role_conflict(
        group_id: Ulid,
        role_id: RoleId,
    ) -> AdminDocumentReducerState {
        let path = format!("group.roles.{role_id}");
        AdminDocumentReducerState {
            target: AdminDocumentTarget::Group { group_id },
            clock: AdminDocumentClock::default(),
            applied_event_ids: BTreeSet::new(),
            user_attributes: BTreeMap::new(),
            conflicts: BTreeMap::from([(
                path.clone(),
                AdminDocumentConflict {
                    path,
                    values: vec![
                        AdminDocumentConflictValue {
                            value: Some(role_id.to_string()),
                            dot: dot(21),
                        },
                        AdminDocumentConflictValue {
                            value: None,
                            dot: dot(22),
                        },
                    ],
                },
            )]),
            user_name: None,
            user_subject_ids: BTreeMap::new(),
            equivalent_value_dots: BTreeMap::new(),
        }
    }

    #[test]
    fn rejects_nil_user_id_as_group_member() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let owner_id = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let role_id = Ulid::from_bytes([4u8; 16]);
        let actor = Actor {
            node_id: node(5),
            user_id: owner_id,
            realm_id,
        };
        let mut operation = AddUserToGroupOperation::new(AddUserToGroupInput {
            actor,
            group_id,
            user_id: UserId::nil(realm_id),
            role_ids: HashSet::from([role_id]),
        });

        assert!(operation.start().is_empty());
        assert_eq!(
            operation.finalize(),
            Err(AddUserToGroupError::InvalidUserId)
        );
    }

    #[test]
    fn seeds_missing_group_role_before_assignment_outbox_event() {
        let realm_id = RealmId::from_bytes([12u8; 32]);
        let owner_id = UserId::local(Ulid::from_bytes([13u8; 16]), realm_id);
        let assigned_user_id = UserId::local(Ulid::from_bytes([14u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([15u8; 16]);
        let role_id = Ulid::from_bytes([16u8; 16]);
        let actor = Actor {
            node_id: node(17),
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
        let input = AddUserToGroupInput {
            actor: actor.clone(),
            group_id,
            user_id: assigned_user_id,
            role_ids: HashSet::from([role_id]),
        };
        let target = AdminDocumentTarget::Group { group_id };
        let mut operation = AddUserToGroupOperation::new(input);

        let effects = operation
            .emit_write_auth_doc_and_admin_state(
                TxnId::r#gen(),
                Some(auth_doc.to_bytes(&actor).unwrap().into()),
                None,
            )
            .unwrap();
        let outbox_records: Vec<DocumentSyncOutboxRecord> = match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchWrite { writes, .. }) => writes
                .iter()
                .filter(|(keyspace, _, _)| keyspace == DOCUMENT_SYNC_OUTBOX_KEYSPACE)
                .map(|(_, _, value)| postcard::from_bytes(value).unwrap())
                .collect(),
            other => panic!("unexpected write effect: {other:?}"),
        };
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
        assert_eq!(events[0].target, target);
        assert_eq!(events[0].origin_seq, 1);
        assert!(matches!(
            &events[0].op,
            AdminDocumentOperation::GroupRoleAdded { role_id: event_role_id }
                if *event_role_id == role_id
        ));
        assert_eq!(events[1].target, target);
        assert_eq!(events[1].origin_seq, 2);
        assert_eq!(events[1].observed.sequence_for(&actor.node_id), 1);
        assert!(matches!(
            &events[1].op,
            AdminDocumentOperation::GroupRoleUserAssignmentAdded {
                role_id: event_role_id,
                user_id: event_user_id,
            } if *event_role_id == role_id && *event_user_id == assigned_user_id
        ));
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
        let txn_id = TxnId::r#gen();
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
                    GroupAuthorizationDocument::from_bytes(auth_write.2.as_ref()).unwrap(),
                    postcard::from_bytes::<AdminDocumentReducerState>(
                        reducer_state_write.2.as_ref(),
                    )
                    .unwrap(),
                    outbox_records,
                )
            }
            other => panic!("unexpected write effect: {other:?}"),
        };
        assert_eq!(outbox_records.len(), 1);
        assert_eq!(
            outbox_records[0].target,
            DocumentSyncTarget::GroupAuthorization { group_id }
        );
        assert!(matches!(
            &outbox_records[0].event,
            DocumentSyncOutboxEvent::AdminOperation { event }
                if event.target == target
                    && matches!(
                        &event.op,
                        AdminDocumentOperation::GroupRoleUserAssignmentAdded {
                            role_id: event_role_id,
                            user_id: event_user_id,
                        } if *event_role_id == role_id && *event_user_id == assigned_user_id
                    )
        ));
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
        assert!(!operation.is_complete());
        let effects = operation.step(Event::SubOperation(SubOperationEvent::NotificationsEmitted));
        assert!(effects.is_empty());
        assert!(operation.is_complete());
    }

    #[test]
    fn unresolved_role_conflict_does_not_emit_membership_notification() {
        let realm_id = RealmId::from_bytes([31u8; 32]);
        let actor = Actor {
            node_id: node(32),
            user_id: UserId::local(Ulid::from_bytes([33u8; 16]), realm_id),
            realm_id,
        };
        let member_id = UserId::local(Ulid::from_bytes([34u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([35u8; 16]);
        let role_id = Ulid::from_bytes([36u8; 16]);
        let auth_doc = seeded_user_role_doc(group_id, role_id);
        let reducer_state = reducer_state_with_role_conflict(group_id, role_id);
        let mut operation = AddUserToGroupOperation::new(AddUserToGroupInput {
            actor: actor.clone(),
            group_id,
            user_id: member_id,
            role_ids: HashSet::from([role_id]),
        });
        let txn_id = TxnId::r#gen();

        operation
            .emit_write_auth_doc_and_admin_state(
                txn_id,
                Some(auth_doc.to_bytes(&actor).unwrap().into()),
                Some(postcard::to_allocvec(&reducer_state).unwrap().into()),
            )
            .unwrap();
        assert!(matches!(
            &operation.state,
            AddUserToGroupState::WriteAuthDocAndAdminState {
                newly_added: false,
                ..
            }
        ));

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { .. })]
        ));
        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(matches!(effects.as_slice(), [Effect::Task(_)]));
        let effects = operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainDocumentSyncOutbox,
            after: std::time::Duration::ZERO,
        }));

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        let result = operation.finalize().unwrap();
        assert!(!result.roles[&role_id].assigned_users.contains(&member_id));
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
        let user_id = UserId::local(Ulid::r#gen(), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();

        let realm_config = CreateRealmConfig {
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            realm_description: "Test realm".to_string(),
            oidc_providers: Vec::new(),
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
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
            owner_cap: None,
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
            user_id: UserId::local(Ulid::r#gen(), realm_id),
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

    async fn add_context() -> (DriverContext, NetHandle, TempDir) {
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
        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle.clone()),
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            blob_handle: None,
        };
        (context, net_handle, random_path)
    }

    async fn setup_group(context: &DriverContext) -> (Actor, Group, GroupAuthorizationDocument) {
        let realm_id = RealmId([0u8; 32]);
        let actor = Actor {
            node_id: node(1),
            user_id: UserId::local(Ulid::r#gen(), realm_id),
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

    fn seeded_user_role_doc(group_id: Ulid, role_id: Ulid) -> GroupAuthorizationDocument {
        GroupAuthorizationDocument {
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
        }
    }

    #[tokio::test]
    async fn add_user_emits_membership_notifications() {
        let (context, net_handle, _tmp) = add_context().await;
        let (actor, group, auth_doc) = setup_group(&context).await;

        let second_admin = UserId::local(Ulid::r#gen(), actor.realm_id);
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

        let member = UserId::local(Ulid::r#gen(), actor.realm_id);
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

        net_handle.shutdown().await;
    }

    #[test]
    fn host_result_is_ok_even_when_emit_fails() {
        let realm_id = RealmId::from_bytes([12u8; 32]);
        let member_id = UserId::local(Ulid::from_bytes([14u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([15u8; 16]);
        let role_id = Ulid::from_bytes([16u8; 16]);
        let actor = Actor {
            node_id: node(17),
            user_id: UserId::local(Ulid::from_bytes([13u8; 16]), realm_id),
            realm_id,
        };
        let auth_doc = seeded_user_role_doc(group_id, role_id);
        let target = AdminDocumentTarget::Group { group_id };

        let mut operation = AddUserToGroupOperation::new(AddUserToGroupInput {
            actor: actor.clone(),
            group_id,
            user_id: member_id,
            role_ids: HashSet::from([role_id]),
        });
        operation.start();
        operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));
        let txn_id = TxnId::r#gen();
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
        assert!(result.roles[&role_id].assigned_users.contains(&member_id));
    }

    #[test]
    fn emit_state_is_skipped_when_no_recipients() {
        let realm_id = RealmId::from_bytes([12u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([13u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([15u8; 16]);
        let role_id = Ulid::from_bytes([16u8; 16]);
        let actor = Actor {
            node_id: node(17),
            user_id,
            realm_id,
        };
        let auth_doc = seeded_user_role_doc(group_id, role_id);
        let target = AdminDocumentTarget::Group { group_id };

        let mut operation = AddUserToGroupOperation::new(AddUserToGroupInput {
            actor: actor.clone(),
            group_id,
            user_id,
            role_ids: HashSet::from([role_id]),
        });
        operation.start();
        operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));
        let txn_id = TxnId::r#gen();
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
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert!(operation.finalize().is_ok());
    }

    #[tokio::test]
    async fn readding_existing_member_emits_nothing() {
        let (context, net_handle, _tmp) = add_context().await;
        let (actor, group, auth_doc) = setup_group(&context).await;
        let member = UserId::local(Ulid::r#gen(), actor.realm_id);

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
        let after_first = read_notification_outbox(&context).await;
        assert_eq!(after_first.len(), 1);
        assert_eq!(after_first[0].recipient, member);

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
        assert_eq!(read_notification_outbox(&context).await.len(), 1);

        drive(
            AddUserToGroupOperation::new(AddUserToGroupInput {
                actor: actor.clone(),
                group_id: group.group_id,
                user_id: member,
                role_ids: role_ids_by_name(&auth_doc, "viewer"),
            }),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(read_notification_outbox(&context).await.len(), 1);

        net_handle.shutdown().await;
    }
}
