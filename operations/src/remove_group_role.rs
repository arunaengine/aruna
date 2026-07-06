use aruna_core::admin_document_reducer::{AdminDocumentReducerError, AdminDocumentReducerState};
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
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
use aruna_core::structs::{Actor, AuthContext, Group, GroupAuthorizationDocument, RealmId};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, GroupId, KeySpace, RoleId, TxnId};
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use aruna_core::structs::Permission;

#[derive(Clone, Debug, PartialEq)]
pub struct RemoveGroupRoleConfig {
    pub auth_context: AuthContext,
    pub actor: Actor,
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub role_id: RoleId,
}

#[derive(PartialEq)]
pub struct RemoveGroupRoleOperation {
    input: RemoveGroupRoleConfig,
    state: RemoveGroupRoleState,
    output: Option<Result<(Group, GroupAuthorizationDocument), RemoveGroupRoleError>>,
}

impl std::fmt::Debug for RemoveGroupRoleOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoveGroupRoleOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RemoveGroupRoleState {
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
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RemoveGroupRoleError {
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
    #[error("Authorization document not found")]
    AuthDocNotFound,
    #[error("Role not found")]
    RoleNotFound,
    #[error("the admin role of a group cannot be deleted")]
    AdminRoleUndeletable,
    #[error(transparent)]
    CheckPermissionsError(#[from] AuthorizationError),
    #[error("Removing role from group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: RemoveGroupRoleState,
        expected: &'static str,
        got: String,
    },
}

impl RemoveGroupRoleOperation {
    pub fn new(input: RemoveGroupRoleConfig) -> Self {
        RemoveGroupRoleOperation {
            input,
            state: RemoveGroupRoleState::Init,
            output: None,
        }
    }

    fn handle_authorization(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return self.unexpected_event(
                RemoveGroupRoleState::Auth,
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
    ) -> Result<Effects, RemoveGroupRoleError> {
        if auth_result? {
            self.state = RemoveGroupRoleState::StartTransaction;
            Ok(smallvec![Effect::Storage(
                StorageEffect::StartTransaction { read: false }
            )])
        } else {
            Err(RemoveGroupRoleError::Unauthorized)
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                RemoveGroupRoleState::StartTransaction,
                "Event::Storage(StorageEvent::TransactionStarted)",
                got,
            );
        };
        match self.emit_get_group(txn_id) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_get_group(&mut self, txn_id: TxnId) -> Result<Effects, RemoveGroupRoleError> {
        self.state = RemoveGroupRoleState::GetGroup { txn_id };
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
    ) -> Result<Effects, RemoveGroupRoleError> {
        let group = Group::from_bytes(&group.ok_or_else(|| RemoveGroupRoleError::GroupNotFound)?)?;

        self.state = RemoveGroupRoleState::GetAuthDocAndAdminState { txn_id, group };

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
    ) -> Result<Effects, RemoveGroupRoleError> {
        let mut auth_doc = GroupAuthorizationDocument::from_bytes(
            &auth_doc.ok_or_else(|| RemoveGroupRoleError::AuthDocNotFound)?,
        )?;
        let role = auth_doc
            .roles
            .get(&self.input.role_id)
            .ok_or_else(|| RemoveGroupRoleError::RoleNotFound)?;
        // The admin role is the only guaranteed management entry point of a group.
        if role.name == "admin" {
            return Err(RemoveGroupRoleError::AdminRoleUndeletable);
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
        let admin_events = apply_admin_reducer_updates(&mut reducer_state, &self.input)?;
        materialize_removed_group_role(
            &mut group,
            &mut auth_doc,
            self.input.role_id,
            &reducer_state,
        );

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

        self.state = RemoveGroupRoleState::WriteGroupAuthDocAndAdminState {
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
            self.state = RemoveGroupRoleState::DeleteStaleAdminConflicts {
                txn_id,
                group,
                auth_doc,
                admin_outbox_written,
            };
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes: stale_conflict_delete_keys
                    .into_iter()
                    .map(|(key_space, key)| (key_space, ByteView::from(key)))
                    .collect(),
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
        self.state = RemoveGroupRoleState::CommitTransaction {
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
            self.state = RemoveGroupRoleState::ScheduleAdminDocumentOutboxDrain { group, auth_doc };
            return smallvec![schedule_outbox_drain_effect()];
        }

        self.state = RemoveGroupRoleState::Finish;
        self.output = Some(Ok((group, auth_doc)));
        smallvec![]
    }

    fn handle_schedule_admin_document_outbox_drain(
        &mut self,
        event: Event,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    ) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. })
            | Event::Task(TaskEvent::Error { .. }) => {
                self.state = RemoveGroupRoleState::Finish;
                self.output = Some(Ok((group, auth_doc)));
                smallvec![]
            }
            other => self.unexpected_event(
                self.state.clone(),
                "admin document outbox drain timer schedule",
                format!("{other:?}"),
            ),
        }
    }

    fn fail(&mut self, err: RemoveGroupRoleError) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(err, cleanup_effects)
    }

    fn fail_with_cleanup(
        &mut self,
        err: RemoveGroupRoleError,
        cleanup_effects: Effects,
    ) -> Effects {
        self.state = RemoveGroupRoleState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: RemoveGroupRoleState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            RemoveGroupRoleError::UnexpectedEvent {
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

impl Operation for RemoveGroupRoleOperation {
    type Output = (Group, GroupAuthorizationDocument);

    type Error = RemoveGroupRoleError;

    fn start(&mut self) -> Effects {
        self.state = RemoveGroupRoleState::Auth;

        let auth_config = CheckPermissionsConfig {
            auth_context: self.input.auth_context.clone(),
            path: format!("/{}/g/{}/admin", self.input.realm_id, self.input.group_id),
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
            RemoveGroupRoleState::Auth => self.handle_authorization(event),
            RemoveGroupRoleState::StartTransaction => self.handle_start_transaction(event),
            RemoveGroupRoleState::GetGroup { txn_id } => self.handle_get_group(event, txn_id),
            RemoveGroupRoleState::GetAuthDocAndAdminState { txn_id, group } => {
                self.handle_get_auth_doc_and_admin_state(event, txn_id, group)
            }
            RemoveGroupRoleState::WriteGroupAuthDocAndAdminState {
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
            RemoveGroupRoleState::DeleteStaleAdminConflicts {
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
            RemoveGroupRoleState::CommitTransaction {
                group,
                auth_doc,
                admin_outbox_written,
                ..
            } => self.handle_commit_transaction(event, group, auth_doc, admin_outbox_written),
            RemoveGroupRoleState::ScheduleAdminDocumentOutboxDrain { group, auth_doc } => {
                self.handle_schedule_admin_document_outbox_drain(event, group, auth_doc)
            }
            RemoveGroupRoleState::Init
            | RemoveGroupRoleState::Finish
            | RemoveGroupRoleState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RemoveGroupRoleState::Finish | RemoveGroupRoleState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or_else(|| RemoveGroupRoleError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            RemoveGroupRoleState::GetGroup { txn_id }
            | RemoveGroupRoleState::GetAuthDocAndAdminState { txn_id, .. }
            | RemoveGroupRoleState::WriteGroupAuthDocAndAdminState { txn_id, .. }
            | RemoveGroupRoleState::DeleteStaleAdminConflicts { txn_id, .. }
            | RemoveGroupRoleState::CommitTransaction { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }

            _ => smallvec![],
        }
    }
}

fn apply_admin_reducer_updates(
    state: &mut AdminDocumentReducerState,
    input: &RemoveGroupRoleConfig,
) -> Result<Vec<AdminDocumentEvent>, AdminDocumentReducerError> {
    let event = state.apply_operation(
        &input.actor,
        AdminDocumentOperation::GroupRoleRemoved {
            role_id: input.role_id,
        },
    )?;
    Ok(vec![event])
}

fn materialize_removed_group_role(
    group: &mut Group,
    auth_doc: &mut GroupAuthorizationDocument,
    role_id: RoleId,
    reducer_state: &AdminDocumentReducerState,
) {
    if reducer_state.materialized_group_roles().contains(&role_id) {
        group.roles.insert(role_id);
    } else {
        group.roles.remove(&role_id);
        auth_doc.roles.remove(&role_id);
    }
}

#[cfg(test)]
pub mod test {
    use std::collections::{HashMap, HashSet};

    use aruna_core::DOCUMENT_SYNC_OUTBOX_KEYSPACE;
    use aruna_core::UserId;
    use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
    use aruna_core::document::{
        DocumentSyncOutboxEvent, DocumentSyncOutboxRecord, DocumentSyncTarget,
    };
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
    use aruna_core::structs::{
        Actor, AuthContext, Group, GroupAuthorizationDocument, Permission, RealmId, Role,
    };
    use aruna_core::types::TxnId;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    use crate::add_group_role::{AddGroupRoleConfig, AddGroupRoleOperation};
    use crate::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};
    use crate::remove_group_role::{
        RemoveGroupRoleConfig, RemoveGroupRoleError, RemoveGroupRoleOperation,
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
            node_location: None,
            node_weight: None,
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

    fn auth_context(actor: &Actor) -> AuthContext {
        AuthContext {
            user_id: actor.user_id,
            realm_id: actor.realm_id,
            path_restrictions: None,
        }
    }

    #[test]
    fn queues_admin_operation_outbox_event_for_role_removal() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([4u8; 16]);
        let role_id = Ulid::from_bytes([5u8; 16]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[6u8; 32]).public(),
            user_id,
            realm_id,
        };
        let role = Role {
            role_id,
            name: "custom_role".to_string(),
            permissions: HashMap::from([("/test".to_string(), Permission::READ)]),
            assigned_users: HashSet::from([user_id]),
        };
        let group = Group {
            display_name: "Test group".to_string(),
            group_id,
            realm_id,
            roles: HashSet::from([role_id]),
            owner: user_id,
        };
        let auth_doc = GroupAuthorizationDocument {
            group_id,
            roles: HashMap::from([(role_id, role)]),
        };
        let mut operation = RemoveGroupRoleOperation::new(RemoveGroupRoleConfig {
            auth_context: auth_context(&actor),
            actor: actor.clone(),
            realm_id,
            group_id,
            role_id,
        });

        let effects = operation
            .emit_write_group_auth_doc_and_admin_state(
                TxnId::new(),
                group,
                Some(auth_doc.to_bytes(&actor).unwrap().into()),
                None,
            )
            .unwrap();

        let (stored_group, stored_auth_doc, outbox_records) = match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchWrite { writes, .. }) => {
                let group_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == GROUP_KEYSPACE)
                    .expect("group doc write is included");
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
                    Group::from_bytes(group_write.2.as_ref()).unwrap(),
                    GroupAuthorizationDocument::from_bytes(auth_write.2.as_ref()).unwrap(),
                    outbox_records,
                )
            }
            other => panic!("unexpected write effect: {other:?}"),
        };

        assert!(!stored_group.roles.contains(&role_id));
        assert!(!stored_auth_doc.roles.contains_key(&role_id));
        assert_eq!(outbox_records.len(), 1);
        assert_eq!(
            outbox_records[0].target,
            DocumentSyncTarget::GroupAuthorization { group_id }
        );
        let event = match &outbox_records[0].event {
            DocumentSyncOutboxEvent::AdminOperation { event } => event.as_ref(),
            other => panic!("unexpected outbox event: {other:?}"),
        };
        assert_eq!(event.target, AdminDocumentTarget::Group { group_id });
        assert!(matches!(
            &event.op,
            AdminDocumentOperation::GroupRoleRemoved { role_id: event_role_id }
                if *event_role_id == role_id
        ));
    }

    #[tokio::test]
    pub async fn test_remove_role() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, _auth_doc) = setup_group(&context).await;

        let role = Role {
            role_id: Ulid::new(),
            name: "custom_role".to_string(),
            permissions: HashMap::from([(
                format!("/{}/g/{}/meta/**", actor.realm_id, group.group_id),
                Permission::READ,
            )]),
            assigned_users: HashSet::new(),
        };
        let add_config = AddGroupRoleConfig {
            auth_context: auth_context(&actor),
            actor: actor.clone(),
            realm_id: actor.realm_id,
            group_id: group.group_id,
            role: role.clone(),
        };
        let (group, auth_doc) = drive(AddGroupRoleOperation::new(add_config), &context)
            .await
            .unwrap();
        assert!(group.roles.contains(&role.role_id));
        assert!(auth_doc.roles.contains_key(&role.role_id));

        let remove_config = RemoveGroupRoleConfig {
            auth_context: auth_context(&actor),
            actor: actor.clone(),
            realm_id: actor.realm_id,
            group_id: group.group_id,
            role_id: role.role_id,
        };
        let (group, auth_doc) = drive(RemoveGroupRoleOperation::new(remove_config), &context)
            .await
            .unwrap();
        assert!(!group.roles.contains(&role.role_id));
        assert!(!auth_doc.roles.contains_key(&role.role_id));

        net_handle.shutdown().await;
    }

    #[tokio::test]
    pub async fn test_admin_role_undeletable() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, auth_doc) = setup_group(&context).await;
        let admin_role_id = *auth_doc
            .roles
            .iter()
            .find(|(_, role)| role.name == "admin")
            .unwrap()
            .0;

        let remove_config = RemoveGroupRoleConfig {
            auth_context: auth_context(&actor),
            actor: actor.clone(),
            realm_id: actor.realm_id,
            group_id: group.group_id,
            role_id: admin_role_id,
        };
        let result = drive(RemoveGroupRoleOperation::new(remove_config), &context).await;
        assert_eq!(
            result.unwrap_err(),
            RemoveGroupRoleError::AdminRoleUndeletable
        );

        net_handle.shutdown().await;
    }
}
