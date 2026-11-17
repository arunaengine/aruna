//! Removes one role from a group and from its authorization document.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, DOCUMENT_STATE_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode, boxed_retry};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState};
use aruna_core::rows::{done, read_row};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::types::{GroupId, Key, KeySpace, RoleId};
use byteview::ByteView;
use thiserror::Error;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::epoch_due::DueMarks;
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::placement::fence::WriteFence;
use aruna_core::placement::target_placement_ref;
use aruna_core::structs::identity::auth::Permission;

#[derive(Clone, Debug, PartialEq)]
pub struct RemoveGroupConfig {
    pub auth_context: AuthContext,
    pub actor: Actor,
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub role_id: RoleId,
}

/// The group and its authorization document after the removal.
type RemovedRole = (Group, GroupAuthorizationDocument);

/// Checks the caller, then removes the role in a write transaction that reruns after a lost
/// conflict, since background writes touch the same rows.
#[derive(Debug, PartialEq)]
pub struct RemoveGroupOperation {
    input: RemoveGroupConfig,
    guard: Option<aruna_core::plugin_installs::InstallGuard>,
    authorized: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum RemoveGroupError {
    #[error("the plugin install changed meanwhile; this change is stale")]
    Superseded,
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
    #[error("Unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for RemoveGroupError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "RemoveGroup".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl RemoveGroupOperation {
    pub fn new(input: RemoveGroupConfig) -> Self {
        RemoveGroupOperation {
            input,
            guard: None,
            authorized: false,
        }
    }

    /// Removes the role only while the install record is still at the guard's revision.
    pub fn guarded(mut self, guard: aruna_core::plugin_installs::InstallGuard) -> Self {
        self.guard = Some(guard);
        self
    }

    fn change(&self) -> IoCall {
        let input = self.input.clone();
        let guard = self.guard;
        let seed = self.input.role_id.to_bytes();
        let build = move || RoleRemoval::new(input.clone(), guard);
        let child = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        IoCall::Child(child)
    }
}

impl Operation for RemoveGroupOperation {
    type Output = RemovedRole;
    type Error = RemoveGroupError;

    fn first(&mut self) -> Result<Next<RemovedRole>, RemoveGroupError> {
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: self.input.auth_context.clone(),
                path: format!("/{}/g/{}/admin", self.input.realm_id, self.input.group_id),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RemovedRole>, RemoveGroupError> {
        if self.authorized {
            return result
                .child::<RemovedRole, RemoveGroupError>()?
                .map(Next::Done);
        }
        if !result.child::<bool, AuthorizationError>()?? {
            return Err(RemoveGroupError::Unauthorized);
        }
        self.authorized = true;
        Ok(Next::Call(self.change()))
    }
}

/// What the removal transaction waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Group,
    Documents,
    Write,
    Delete,
    Fence,
    Marks,
}

/// Reads the group and its documents, writes the removal, takes the bucket fence and marks the
/// group's key epochs due, in one write transaction.
#[derive(Debug, PartialEq)]
struct RoleRemoval {
    input: RemoveGroupConfig,
    guard: Option<aruna_core::plugin_installs::InstallGuard>,
    stage: Stage,
    group: Option<Group>,
    /// Bucket the authorization rows publish onto, read inside the write transaction.
    fence: WriteFence,
    written: Option<RemovedRole>,
    outbox: bool,
    deletes: Vec<(KeySpace, Key)>,
    marks: DueMarks,
}

impl RoleRemoval {
    fn new(
        input: RemoveGroupConfig,
        guard: Option<aruna_core::plugin_installs::InstallGuard>,
    ) -> Self {
        let marks = DueMarks::new(
            Some(input.group_id),
            aruna_core::time::unix_timestamp_millis(),
        );
        Self {
            input,
            guard,
            stage: Stage::Group,
            group: None,
            fence: Default::default(),
            written: None,
            outbox: false,
            deletes: Vec::new(),
            marks,
        }
    }

    fn read_documents(&mut self, result: IoResult) -> Result<Next<RemovedRole>, RemoveGroupError> {
        let [group] = aruna_core::rows::values::<1>(result)?;
        let group = Group::from_bytes(&group.ok_or(RemoveGroupError::GroupNotFound)?)?;
        self.group = Some(group);
        self.stage = Stage::Documents;
        let target = AdminDocumentTarget::Group {
            group_id: self.input.group_id,
        };
        let key: Key = self.input.group_id.to_bytes().into();
        Ok(Next::Call(IoCall::Read(vec![
            (AUTH_KEYSPACE.to_string(), key),
            (
                DOCUMENT_STATE_KEYSPACE.to_string(),
                reducer_state_key(&target),
            ),
            (
                REALM_CONFIG_KEYSPACE.to_string(),
                ByteView::from(*self.input.actor.realm_id.as_bytes()),
            ),
        ])))
    }

    fn emit_document_write(
        &mut self,
        mut group: Group,
        auth_doc: Option<ByteView>,
        reducer_state_value: Option<ByteView>,
        realm_config_value: Option<ByteView>,
    ) -> Result<Next<RemovedRole>, RemoveGroupError> {
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

        let conflict_delete_keys =
            stale_conflict_deletes(previous_reducer_state.as_ref(), Some(&reducer_state));
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
        if (self.guard).is_some_and(|guard| !guard.admits(realm_config.as_ref())) {
            return Err(RemoveGroupError::Superseded);
        }
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

        self.written = Some((group, auth_doc));
        self.outbox = !admin_events.is_empty();
        self.deletes = conflict_delete_keys;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// Takes the bucket's fence inside the transaction before committing, so a departing
    /// holder's close rejects or conflicts this write.
    fn after_writes(&mut self) -> Next<RemovedRole> {
        if self.fence.is_empty() {
            return self.marks();
        }
        self.stage = Stage::Fence;
        Next::Call(IoCall::Read(self.fence.reads()))
    }

    fn marks(&mut self) -> Next<RemovedRole> {
        self.stage = Stage::Marks;
        Next::Call(self.marks.first())
    }

    /// Wakes the outbox drain after the commit when the removal queued admin events; a failed
    /// timer leaves the durable outbox retryable.
    fn finish(&mut self) -> Result<Next<RemovedRole>, RemoveGroupError> {
        let output = self.written.take().ok_or(RemoveGroupError::NotFinished)?;
        if !self.outbox {
            return Ok(Next::Done(output));
        }
        let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        Ok(Next::Then { output, calls })
    }
}

impl Operation for RoleRemoval {
    type Output = RemovedRole;
    type Error = RemoveGroupError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn conflict_retries() -> u32 {
        u32::from(super::CONFLICT_RETRIES)
    }

    fn first(&mut self) -> Result<Next<RemovedRole>, RemoveGroupError> {
        let key = self.input.group_id.to_bytes().to_vec();
        Ok(Next::Call(read_row(GROUP_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RemovedRole>, RemoveGroupError> {
        match self.stage {
            Stage::Group => self.read_documents(result),
            Stage::Documents => {
                let [auth_doc, reducer, realm_config] = aruna_core::rows::values::<3>(result)?;
                let group = self.group.take().ok_or(RemoveGroupError::GroupNotFound)?;
                self.emit_document_write(group, auth_doc, reducer, realm_config)
            }
            Stage::Write if !self.deletes.is_empty() => {
                done(result)?;
                self.stage = Stage::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            Stage::Write | Stage::Delete => {
                done(result)?;
                Ok(self.after_writes())
            }
            Stage::Fence => {
                let IoResult::Values(values) = result else {
                    return Err(RemoveGroupError::NotFinished);
                };
                let keys = self.fence.reads().into_iter().map(|(_, key)| key);
                let rows: Vec<_> = keys.zip(values).collect();
                if !self.fence.admits(&rows) {
                    return Err(RemoveGroupError::PlacementFenced);
                }
                Ok(self.marks())
            }
            Stage::Marks => match self.marks.next(result, true) {
                Ok(Some(call)) => Ok(Next::Call(call)),
                Ok(None) => self.finish(),
                Err(error) => Err(StorageError::WriteError(error.to_string()).into()),
            },
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
    use aruna_core::calls::{IoCall, IoError, IoResult, Next};
    use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord, DocumentTarget};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
    use aruna_core::operation::{Operation, TransactionMode};
    use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission, Role};
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    use crate::driver::{DriverContext, drive};
    use crate::groups::add_role::{AddRoleConfig, AddRoleOperation};
    use crate::groups::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::groups::remove_role::{
        RemoveGroupConfig, RemoveGroupError, RemoveGroupOperation, RoleRemoval,
    };
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
            graph_handle: None,
            task_handle: Some(task_handle),
            compute_handle: None,
            outbound_handle: None,
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
    fn retries_commit_conflict() {
        // A lost commit reruns the whole removal, rereading every row, up to the retry bound.
        let conflict = RemoveGroupError::from(IoError::Conflict);
        assert!(RoleRemoval::is_conflict(&conflict));
        assert_eq!(
            RoleRemoval::conflict_retries(),
            u32::from(crate::groups::CONFLICT_RETRIES)
        );
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([10u8; 16]), realm_id),
            realm_id,
        };
        let config = RemoveGroupConfig {
            auth_context: auth_context(&actor),
            actor,
            realm_id,
            group_id: Ulid::from_bytes([8u8; 16]),
            role_id: Ulid::from_bytes([11u8; 16]),
        };
        let mut removal = RoleRemoval::new(config.clone(), None);
        assert_eq!(removal.transaction(), TransactionMode::Write);
        assert!(matches!(removal.first(), Ok(Next::Call(IoCall::Read(_)))));
        let missing = removal.next(IoResult::Values(vec![None]));
        assert_eq!(missing, Err(RemoveGroupError::GroupNotFound));
        let mut operation = RemoveGroupOperation::new(config);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
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
        let mut operation = RoleRemoval::new(
            RemoveGroupConfig {
                auth_context: auth_context(&actor),
                actor: actor.clone(),
                realm_id,
                group_id,
                role_id,
            },
            None,
        );

        let turn = operation.emit_document_write(
            group,
            Some(auth_doc.to_bytes(&actor).unwrap().into()),
            None,
            None,
        );

        let (stored_group, stored_auth_doc, outbox_records) = match turn {
            Ok(Next::Call(IoCall::Write(writes))) => {
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
            other => panic!("unexpected write call: {other:?}"),
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
