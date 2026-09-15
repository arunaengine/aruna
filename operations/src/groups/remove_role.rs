use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    DOCUMENT_STATE_KEYSPACE, AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::placement::placement_record::PlacementRef;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, GroupId, KeySpace, RoleId, TxnId};
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::placement::target_placement_ref;
use crate::sync::document_outbox::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::structs::identity::auth::Permission;

#[derive(Clone, Debug, PartialEq)]
pub struct RemoveGroupConfig {
    pub auth_context: AuthContext,
    pub actor: Actor,
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub role_id: RoleId,
}

#[derive(PartialEq)]
pub struct RemoveGroupOperation {
    input: RemoveGroupConfig,
    /// Bucket the authorization rows publish onto, read inside the write
    /// transaction.
    fence: crate::placement::fence::WriteFence,
    state: RemoveGroupState,
    output: Option<Result<(Group, GroupAuthorizationDocument), RemoveGroupError>>,
}

impl std::fmt::Debug for RemoveGroupOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoveGroupOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RemoveGroupState {
    Init,
    Auth,
    StartTransaction,
    GetGroup {
        txn_id: TxnId,
    },
    #[serde(rename = "GetAuthDocAndAdminState")]
    GetAdminState {
        txn_id: TxnId,
        group: Group,
    },
    #[serde(rename = "WriteGroupAuthDocAndAdminState")]
    WriteDocState {
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        #[serde(rename = "stale_conflict_delete_keys")]
        conflict_delete_keys: Vec<(KeySpace, Vec<u8>)>,
    },
    #[serde(rename = "DeleteStaleAdminConflicts")]
    DeleteAdminConflicts {
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
    },
    ReadBucketFence {
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
    #[serde(rename = "ScheduleAdminDocumentOutboxDrain")]
    ScheduleDocumentDrain {
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RemoveGroupError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentError(#[from] AdminDocumentError),
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
    #[error("the group's bucket cut over to a new holder set; retry the change")]
    PlacementFenced,
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("Unauthorized")]
    Unauthorized,
    #[error("No group found")]
    GroupNotFound,
    #[error("Authorization document not found")]
    DocNotFound,
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
        state: RemoveGroupState,
        expected: &'static str,
        got: String,
    },
}

impl RemoveGroupOperation {
    pub fn new(input: RemoveGroupConfig) -> Self {
        RemoveGroupOperation {
            input,
            fence: Default::default(),
            state: RemoveGroupState::Init,
            output: None,
        }
    }

    fn handle_authorization(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return self.unexpected_event(
                RemoveGroupState::Auth,
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
    ) -> Result<Effects, RemoveGroupError> {
        if auth_result? {
            self.state = RemoveGroupState::StartTransaction;
            Ok(smallvec![Effect::Storage(
                StorageEffect::StartTransaction { read: false }
            )])
        } else {
            Err(RemoveGroupError::Unauthorized)
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                RemoveGroupState::StartTransaction,
                "Event::Storage(StorageEvent::TransactionStarted)",
                got,
            );
        };
        match self.emit_get_group(txn_id) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_get_group(&mut self, txn_id: TxnId) -> Result<Effects, RemoveGroupError> {
        self.state = RemoveGroupState::GetGroup { txn_id };
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

        match self.emit_auth_read(value, txn_id) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_auth_read(
        &mut self,
        group: Option<ByteView>,
        txn_id: TxnId,
    ) -> Result<Effects, RemoveGroupError> {
        let group = Group::from_bytes(&group.ok_or_else(|| RemoveGroupError::GroupNotFound)?)?;

        self.state = RemoveGroupState::GetAdminState { txn_id, group };

        let target = AdminDocumentTarget::Group {
            group_id: self.input.group_id,
        };
        let key = self.input.group_id.to_bytes().into();
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (AUTH_KEYSPACE.to_string(), key),
                (
                    DOCUMENT_STATE_KEYSPACE.to_string(),
                    reducer_state_key(&target),
                ),
                (
                    REALM_CONFIG_KEYSPACE.to_string(),
                    ByteView::from(*self.input.actor.realm_id.as_bytes()),
                ),
            ],
            txn_id: Some(txn_id),
        })])
    }

    fn handle_auth_read(&mut self, event: Event, txn_id: TxnId, group: Group) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::BatchReadResult)",
                got,
            );
        };
        let [
            (_, auth_doc_value),
            (_, reducer_state_value),
            (_, realm_config_value),
        ] = values.as_slice()
        else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::BatchReadResult) with auth doc, admin state, and realm config values",
                got,
            );
        };

        match self.emit_document_write(
            txn_id,
            group,
            auth_doc_value.clone(),
            reducer_state_value.clone(),
            realm_config_value.clone(),
        ) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_document_write(
        &mut self,
        txn_id: TxnId,
        mut group: Group,
        auth_doc: Option<ByteView>,
        reducer_state_value: Option<ByteView>,
        realm_config_value: Option<ByteView>,
    ) -> Result<Effects, RemoveGroupError> {
        let mut auth_doc =
            super::parse_auth_record(auth_doc)?.ok_or(RemoveGroupError::DocNotFound)?;
        let role = auth_doc
            .roles
            .get(&self.input.role_id)
            .ok_or_else(|| RemoveGroupError::RoleNotFound)?;
        // The admin role is the only guaranteed management entry point of a group.
        if role.name == "admin" {
            return Err(RemoveGroupError::AdminRoleUndeletable);
        }
        let target = AdminDocumentTarget::Group {
            group_id: self.input.group_id,
        };
        let previous_reducer_state = reducer_state_value
            .as_ref()
            .map(|value| {
                aruna_core::reducer::decode_reducer_state(value.as_ref())
                    .map_err(ConversionError::from)
            })
            .transpose()?;
        if previous_reducer_state
            .as_ref()
            .is_some_and(|state| state.target != target)
        {
            return Err(AdminDocumentError::TargetMismatch.into());
        }

        let mut reducer_state = previous_reducer_state
            .clone()
            .unwrap_or_else(|| AdminDocumentState::new(target));
        let admin_events = apply_reducer_updates(&mut reducer_state, &self.input)?;
        materialize_role_removal(
            &mut group,
            &mut auth_doc,
            self.input.role_id,
            &reducer_state,
        );

        let conflict_delete_keys: Vec<_> =
            stale_conflict_deletes(previous_reducer_state.as_ref(), Some(&reducer_state))
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
            reducer_state_entry(&reducer_state)?,
        ];
        let document_target = DocumentTarget::GroupAuthorization {
            group_id: self.input.group_id,
        };
        let realm_config = realm_config_value
            .as_deref()
            .map(RealmConfigDocument::from_bytes)
            .transpose()?;
        let placement = realm_config
            .as_ref()
            .map(|config| target_placement_ref(config, &document_target, Default::default()))
            .unwrap_or(PlacementRef::NIL);
        let realm_id = self.input.actor.realm_id;
        if let Some(config) = realm_config.as_ref() {
            self.fence.add(realm_id, config, [placement]);
        }
        let generation = self.fence.generation(&realm_id, &placement);
        for event in &admin_events {
            let record = new_identified_record(
                event.event_id,
                self.input.actor.node_id,
                document_target.clone(),
                Vec::new(),
                DocumentOutboxEvent::admin(event.clone()),
                placement,
                false,
            )
            .fenced_at(generation);
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
        writes.extend(conflict_write_entries(&reducer_state)?);

        self.state = RemoveGroupState::WriteDocState {
            txn_id,
            group,
            auth_doc,
            admin_outbox_written: !admin_events.is_empty(),
            conflict_delete_keys,
        };

        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_document_write(
        &mut self,
        event: Event,
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
        conflict_delete_keys: Vec<(KeySpace, Vec<u8>)>,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::BatchWriteResult)",
                got,
            );
        };

        if !conflict_delete_keys.is_empty() {
            self.state = RemoveGroupState::DeleteAdminConflicts {
                txn_id,
                group,
                auth_doc,
                admin_outbox_written,
            };
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes: conflict_delete_keys
                    .into_iter()
                    .map(|(key_space, key)| (key_space, ByteView::from(key)))
                    .collect(),
                txn_id: Some(txn_id),
            })];
        }

        self.emit_commit_transaction(txn_id, group, auth_doc, admin_outbox_written)
    }

    fn handle_conflict_deletes(
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

    /// Takes the bucket's fence inside the transaction before committing, so a
    /// departing holder's close rejects or conflicts this write.
    fn emit_commit_transaction(
        &mut self,
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
    ) -> Effects {
        if self.fence.is_empty() {
            return self.emit_commit(txn_id, group, auth_doc, admin_outbox_written);
        }
        self.state = RemoveGroupState::ReadBucketFence {
            txn_id,
            group,
            auth_doc,
            admin_outbox_written,
        };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: self.fence.reads(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_bucket_fence(
        &mut self,
        event: Event,
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::BatchReadResult)",
                got,
            );
        };
        if !self.fence.admits(&values) {
            return self.fail(RemoveGroupError::PlacementFenced);
        }
        self.emit_commit(txn_id, group, auth_doc, admin_outbox_written)
    }

    fn emit_commit(
        &mut self,
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
        admin_outbox_written: bool,
    ) -> Effects {
        self.state = RemoveGroupState::CommitTransaction {
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
            self.state = RemoveGroupState::ScheduleDocumentDrain { group, auth_doc };
            return smallvec![schedule_drain_effect()];
        }

        self.state = RemoveGroupState::Finish;
        self.output = Some(Ok((group, auth_doc)));
        smallvec![]
    }

    fn handle_drain_schedule(
        &mut self,
        event: Event,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    ) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. })
            | Event::Task(TaskEvent::Error { .. }) => {
                self.state = RemoveGroupState::Finish;
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

    fn fail(&mut self, err: RemoveGroupError) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(err, cleanup_effects)
    }

    fn fail_with_cleanup(&mut self, err: RemoveGroupError, cleanup_effects: Effects) -> Effects {
        self.state = RemoveGroupState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: RemoveGroupState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            RemoveGroupError::UnexpectedEvent {
                state,
                expected,
                got,
            },
            cleanup_effects,
        )
    }

    fn catch_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }

        Ok(event)
    }
}

impl Operation for RemoveGroupOperation {
    type Output = (Group, GroupAuthorizationDocument);

    type Error = RemoveGroupError;

    fn start(&mut self) -> Effects {
        self.state = RemoveGroupState::Auth;

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
        let event = match self.catch_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state.clone() {
            RemoveGroupState::Auth => self.handle_authorization(event),
            RemoveGroupState::StartTransaction => self.handle_start_transaction(event),
            RemoveGroupState::GetGroup { txn_id } => self.handle_get_group(event, txn_id),
            RemoveGroupState::GetAdminState { txn_id, group } => {
                self.handle_auth_read(event, txn_id, group)
            }
            RemoveGroupState::WriteDocState {
                txn_id,
                group,
                auth_doc,
                admin_outbox_written,
                conflict_delete_keys,
            } => self.handle_document_write(
                event,
                txn_id,
                group,
                auth_doc,
                admin_outbox_written,
                conflict_delete_keys,
            ),
            RemoveGroupState::DeleteAdminConflicts {
                txn_id,
                group,
                auth_doc,
                admin_outbox_written,
            } => self.handle_conflict_deletes(event, txn_id, group, auth_doc, admin_outbox_written),
            RemoveGroupState::ReadBucketFence {
                txn_id,
                group,
                auth_doc,
                admin_outbox_written,
            } => self.handle_bucket_fence(event, txn_id, group, auth_doc, admin_outbox_written),
            RemoveGroupState::CommitTransaction {
                group,
                auth_doc,
                admin_outbox_written,
                ..
            } => self.handle_commit_transaction(event, group, auth_doc, admin_outbox_written),
            RemoveGroupState::ScheduleDocumentDrain { group, auth_doc } => {
                self.handle_drain_schedule(event, group, auth_doc)
            }
            RemoveGroupState::Init | RemoveGroupState::Finish | RemoveGroupState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RemoveGroupState::Finish | RemoveGroupState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or_else(|| RemoveGroupError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            RemoveGroupState::GetGroup { txn_id }
            | RemoveGroupState::GetAdminState { txn_id, .. }
            | RemoveGroupState::WriteDocState { txn_id, .. }
            | RemoveGroupState::DeleteAdminConflicts { txn_id, .. }
            | RemoveGroupState::ReadBucketFence { txn_id, .. }
            | RemoveGroupState::CommitTransaction { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }

            _ => smallvec![],
        }
    }
}

fn apply_reducer_updates(
    state: &mut AdminDocumentState,
    input: &RemoveGroupConfig,
) -> Result<Vec<AdminDocumentEvent>, AdminDocumentError> {
    let event = state.apply_operation(
        &input.actor,
        AdminDocumentOperation::GroupRoleRemoved {
            role_id: input.role_id,
        },
    )?;
    Ok(vec![event])
}

fn materialize_role_removal(
    group: &mut Group,
    auth_doc: &mut GroupAuthorizationDocument,
    role_id: RoleId,
    reducer_state: &AdminDocumentState,
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

    use aruna_core::SYNC_OUTBOX_KEYSPACE;
    use aruna_core::UserId;
    use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
    use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord, DocumentTarget};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
    use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission, Role};
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::types::TxnId;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    use crate::driver::{DriverContext, drive};
    use crate::groups::add_role::{AddRoleConfig, AddRoleOperation};
    use crate::groups::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::groups::remove_role::{RemoveGroupConfig, RemoveGroupError, RemoveGroupOperation};
    use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};

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
            compute_handle: None,
            blob_handle: None,
        };
        (context, net_handle, random_path)
    }

    async fn setup_group(context: &DriverContext) -> (Actor, Group, GroupAuthorizationDocument) {
        let realm_id = RealmId([0u8; 32]);
        let user_id = UserId::local(Ulid::generate(), realm_id);
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
            node_labels: Default::default(),
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
            session: None,
        }
    }

    #[test]
    fn queues_removal_outbox() {
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
            policies: Vec::new(),
            roles: HashMap::from([(role_id, role)]),
        };
        let mut operation = RemoveGroupOperation::new(RemoveGroupConfig {
            auth_context: auth_context(&actor),
            actor: actor.clone(),
            realm_id,
            group_id,
            role_id,
        });

        let effects = operation
            .emit_document_write(
                TxnId::generate(),
                group,
                Some(auth_doc.to_bytes(&actor).unwrap().into()),
                None,
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
                let outbox_records: Vec<DocumentOutboxRecord> = writes
                    .iter()
                    .filter(|(keyspace, _, _)| keyspace == SYNC_OUTBOX_KEYSPACE)
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
            DocumentTarget::GroupAuthorization { group_id }
        );
        let event = match &outbox_records[0].event {
            DocumentOutboxEvent::AdminOperation { event, .. } => event.as_ref(),
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
            role_id: Ulid::generate(),
            name: "custom_role".to_string(),
            permissions: HashMap::from([(
                format!("/{}/g/{}/meta/**", actor.realm_id, group.group_id),
                Permission::READ,
            )]),
            assigned_users: HashSet::new(),
        };
        let add_config = AddRoleConfig {
            auth_context: auth_context(&actor),
            actor: actor.clone(),
            realm_id: actor.realm_id,
            group_id: group.group_id,
            role: role.clone(),
        };
        let (group, auth_doc) = drive(AddRoleOperation::new(add_config), &context)
            .await
            .unwrap();
        assert!(group.roles.contains(&role.role_id));
        assert!(auth_doc.roles.contains_key(&role.role_id));

        let remove_config = RemoveGroupConfig {
            auth_context: auth_context(&actor),
            actor: actor.clone(),
            realm_id: actor.realm_id,
            group_id: group.group_id,
            role_id: role.role_id,
        };
        let (group, auth_doc) = drive(RemoveGroupOperation::new(remove_config), &context)
            .await
            .unwrap();
        assert!(!group.roles.contains(&role.role_id));
        assert!(!auth_doc.roles.contains_key(&role.role_id));

        net_handle.shutdown().await;
    }

    #[tokio::test]
    pub async fn protects_admin_role() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, auth_doc) = setup_group(&context).await;
        let admin_role_id = *auth_doc
            .roles
            .iter()
            .find(|(_, role)| role.name == "admin")
            .unwrap()
            .0;

        let remove_config = RemoveGroupConfig {
            auth_context: auth_context(&actor),
            actor: actor.clone(),
            realm_id: actor.realm_id,
            group_id: group.group_id,
            role_id: admin_role_id,
        };
        let result = drive(RemoveGroupOperation::new(remove_config), &context).await;
        assert_eq!(result.unwrap_err(), RemoveGroupError::AdminRoleUndeletable);

        net_handle.shutdown().await;
    }
}
