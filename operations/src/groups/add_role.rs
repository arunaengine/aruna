//! Adds a role to a group and stores it in the group and its authorization document.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget, AdminRoleDefinition,
};
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::Effect;
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
use aruna_core::structs::identity::auth::{Actor, AuthContext, Role};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{GroupId, Key, KeySpace};
use byteview::ByteView;
use std::collections::HashSet;
use thiserror::Error;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::epoch_due::DueMarks;
use crate::notifications::emit::emit_notifications_effect;
use crate::notifications::routing::{RoutingContext, route_resource_event};
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::placement::fence::WriteFence;
use aruna_core::placement::target_placement_ref;
use aruna_core::structs::execution::notification::ResourceEvent;
use aruna_core::structs::identity::auth::Permission;

#[derive(Clone, Debug, PartialEq)]
pub struct AddRoleConfig {
    pub auth_context: AuthContext,
    pub actor: Actor,
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub role: Role,
}

/// The group and its authorization document after the change, whether admin events reached the
/// outbox, and the users the role newly made members.
type AddedRole = (Group, GroupAuthorizationDocument, bool, Vec<UserId>);

/// What the role creation waits for after the caller check.
#[derive(Debug, PartialEq)]
enum Step {
    Check,
    Write,
    AnnounceGroup,
    AnnounceAuthorization,
}

/// Checks the caller, adds the role in a write transaction that reruns after a lost conflict,
/// then wakes the outbox drain or announces both documents, and notifies new members.
#[derive(Debug, PartialEq)]
pub struct AddRoleOperation {
    input: AddRoleConfig,
    guard: Option<aruna_core::plugin_installs::InstallGuard>,
    step: Step,
    added: Option<AddedRole>,
}

#[derive(Debug, Error, PartialEq)]
pub enum AddRoleError {
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
    #[error("Unauthorized")]
    Unauthorized,
    #[error("No group found")]
    GroupNotFound,
    #[error("the group's bucket cut over to a new holder set; retry the change")]
    PlacementFenced,
    #[error("Invalid public role")]
    InvalidPublicRole,
    #[error("Invalid assigned user")]
    InvalidAssignedUser,
    #[error("Role path escapes the group subtree")]
    UnconfinedRolePath,
    #[error("Reserved role name")]
    ReservedRoleName,
    #[error(transparent)]
    CheckPermissionsError(#[from] AuthorizationError),
    #[error("Adding role to group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for AddRoleError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "AddRole".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

const GROUP_ROLE_NAMES: &[&str] = &["admin", "user"];

fn reserved_role_name(name: &str) -> bool {
    GROUP_ROLE_NAMES.contains(&name.trim())
}

impl AddRoleConfig {
    fn validate(&self) -> Result<(), AddRoleError> {
        if reserved_role_name(&self.role.name) {
            return Err(AddRoleError::ReservedRoleName);
        }

        if self
            .role
            .assigned_users
            .iter()
            .any(|user| user.is_nil() && !user.is_nil_in(self.realm_id))
        {
            return Err(AddRoleError::InvalidAssignedUser);
        }

        if self.role.is_public(self.realm_id)
            && self
                .role
                .permissions
                .values()
                .any(|permission| permission != &Permission::READ)
        {
            return Err(AddRoleError::InvalidPublicRole);
        }

        let subtree_root =
            aruna_core::permission_path::role_subtree_root(self.realm_id, self.group_id);
        if self
            .role
            .permissions
            .keys()
            .any(|pattern| !aruna_core::permission_path::role_path_confined(pattern, &subtree_root))
        {
            return Err(AddRoleError::UnconfinedRolePath);
        }

        Ok(())
    }
}

impl AddRoleOperation {
    /// Applies the role only while the install record is still at the guard's revision.
    pub fn guarded(mut self, guard: aruna_core::plugin_installs::InstallGuard) -> Self {
        self.guard = Some(guard);
        self
    }

    pub fn new(input: AddRoleConfig) -> Self {
        AddRoleOperation {
            input,
            guard: None,
            step: Step::Check,
            added: None,
        }
    }

    fn change(&mut self) -> Next<(Group, GroupAuthorizationDocument)> {
        self.step = Step::Write;
        let input = self.input.clone();
        let guard = self.guard;
        let seed = self.input.role.role_id.to_bytes();
        let build = move || RoleAddition::new(input.clone(), guard);
        let child = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        Next::Call(IoCall::Child(child))
    }

    fn announce(
        &mut self,
        document: DocumentTarget,
        step: Step,
    ) -> Result<Next<(Group, GroupAuthorizationDocument)>, AddRoleError> {
        let announce = crate::realm::announce_call(&self.input.actor, document);
        self.step = step;
        Ok(Next::Call(announce.ok_or(AddRoleError::NotFinished)?))
    }

    fn written(
        &mut self,
        result: IoResult,
    ) -> Result<Next<(Group, GroupAuthorizationDocument)>, AddRoleError> {
        let added = result.child::<AddedRole, AddRoleError>()??;
        if added.2 {
            // A failed drain timer leaves the durable outbox retryable.
            let drain = IoCall::Effect(Box::new(schedule_drain_effect()));
            return Ok(self.finish(added, Some(drain)));
        }
        let document = DocumentTarget::Group {
            group_id: added.0.group_id,
        };
        self.added = Some(added);
        self.announce(document, Step::AnnounceGroup)
    }

    fn announced(&mut self, result: IoResult) -> Result<(), AddRoleError> {
        let outcome =
            crate::realm::announced(result).map_err(|got| AddRoleError::UnexpectedEvent {
                state: format!("{:?}", self.step),
                expected: "Event::SubOperation(SubOperationEvent::DocumentSyncResult)",
                got,
            })?;
        outcome.map_err(AddRoleError::TopicAnnouncement)
    }

    /// The group and its document, with the drain wake and the new members' notices made after
    /// it; their failures are only logged, as the change is already durable.
    fn finish(
        &self,
        added: AddedRole,
        drain: Option<IoCall>,
    ) -> Next<(Group, GroupAuthorizationDocument)> {
        let (group, auth_doc, _, new_members) = added;
        let mut calls: Vec<IoCall> = drain.into_iter().collect();
        calls.extend(self.added_notice(&auth_doc, &new_members));
        let output = (group, auth_doc);
        if calls.is_empty() {
            return Next::Done(output);
        }
        Next::Then { output, calls }
    }

    /// The notices to the members the role newly added.
    fn added_notice(
        &self,
        auth_doc: &GroupAuthorizationDocument,
        new_members: &[UserId],
    ) -> Option<IoCall> {
        let records = (new_members.iter().copied())
            .flat_map(|affected_user| {
                route_resource_event(
                    &ResourceEvent::GroupMemberAdded {
                        group_id: self.input.group_id,
                        affected_user,
                        actor_user_id: self.input.actor.user_id,
                    },
                    RoutingContext {
                        group_auth: Some(auth_doc),
                        realm_auth: None,
                        realm_id: self.input.actor.realm_id,
                    },
                    unix_timestamp_millis(),
                )
            })
            .collect::<Vec<_>>();
        if records.is_empty() {
            return None;
        }
        match emit_notifications_effect(records) {
            Effect::SubOperation(child) => Some(IoCall::Child(child)),
            _ => None,
        }
    }
}

impl Operation for AddRoleOperation {
    type Output = (Group, GroupAuthorizationDocument);
    type Error = AddRoleError;

    fn first(&mut self) -> Result<Next<(Group, GroupAuthorizationDocument)>, AddRoleError> {
        self.input.validate()?;
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: self.input.auth_context.clone(),
                path: format!("/{}/g/{}/admin", self.input.realm_id, self.input.group_id),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<(Group, GroupAuthorizationDocument)>, AddRoleError> {
        match self.step {
            Step::Check => {
                if !result.child::<bool, AuthorizationError>()?? {
                    return Err(AddRoleError::Unauthorized);
                }
                Ok(self.change())
            }
            Step::Write => self.written(result),
            Step::AnnounceGroup => {
                self.announced(result)?;
                let group_id = self.input.group_id;
                let document = DocumentTarget::GroupAuthorization { group_id };
                self.announce(document, Step::AnnounceAuthorization)
            }
            Step::AnnounceAuthorization => {
                self.announced(result)?;
                let added = self.added.take().ok_or(AddRoleError::NotFinished)?;
                Ok(self.finish(added, None))
            }
        }
    }
}

/// What the role transaction waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Group,
    Documents,
    Write,
    Delete,
    Fence,
    Marks,
}

/// Reads the group and its documents, writes the role, takes the bucket fence, and marks the
/// group's key epochs due when the role narrows access, in one write transaction.
#[derive(Debug, PartialEq)]
struct RoleAddition {
    input: AddRoleConfig,
    guard: Option<aruna_core::plugin_installs::InstallGuard>,
    stage: Stage,
    group: Option<Group>,
    /// Bucket the authorization rows publish onto, read inside the write transaction.
    fence: WriteFence,
    narrowed: bool,
    written: Option<AddedRole>,
    deletes: Vec<(KeySpace, Key)>,
    marks: DueMarks,
}

impl RoleAddition {
    fn new(input: AddRoleConfig, guard: Option<aruna_core::plugin_installs::InstallGuard>) -> Self {
        let marks = DueMarks::new(Some(input.group_id), unix_timestamp_millis());
        Self {
            input,
            guard,
            stage: Stage::Group,
            group: None,
            fence: Default::default(),
            narrowed: false,
            written: None,
            deletes: Vec::new(),
            marks,
        }
    }

    fn read_documents(&mut self, result: IoResult) -> Result<Next<AddedRole>, AddRoleError> {
        let [group] = aruna_core::rows::values::<1>(result)?;
        let group = Group::from_bytes(&group.ok_or(AddRoleError::GroupNotFound)?)?;
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
    ) -> Result<Next<AddedRole>, AddRoleError> {
        let mut auth_doc =
            super::parse_auth_record(auth_doc)?.ok_or(AddRoleError::GroupNotFound)?;
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
        let members_before = group_members(&auth_doc);
        let roles_before = auth_doc.roles.clone();
        materialize_group_role(&mut group, &mut auth_doc, &self.input.role, &reducer_state);
        self.narrowed = aruna_core::admin_documents::roles_narrowed(&roles_before, &auth_doc.roles);
        let new_members = newly_materialized_members(&members_before, &auth_doc, &self.input.role);

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
            return Err(AddRoleError::Superseded);
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

        self.written = Some((group, auth_doc, !admin_events.is_empty(), new_members));
        self.deletes = conflict_delete_keys;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// Takes the bucket's fence inside the transaction before committing, so a departing
    /// holder's close rejects or conflicts this write.
    fn after_writes(&mut self) -> Result<Next<AddedRole>, AddRoleError> {
        if self.fence.is_empty() {
            return self.after_fence();
        }
        self.stage = Stage::Fence;
        Ok(Next::Call(IoCall::Read(self.fence.reads())))
    }

    /// A DENY rule or a replaced role can narrow existing READ scopes.
    fn after_fence(&mut self) -> Result<Next<AddedRole>, AddRoleError> {
        let deny = (self.input.role.permissions.values()).any(|p| *p == Permission::DENY);
        if self.narrowed || deny {
            self.stage = Stage::Marks;
            return Ok(Next::Call(self.marks.first()));
        }
        self.finish()
    }

    fn finish(&mut self) -> Result<Next<AddedRole>, AddRoleError> {
        self.written
            .take()
            .map(Next::Done)
            .ok_or(AddRoleError::NotFinished)
    }
}

impl Operation for RoleAddition {
    type Output = AddedRole;
    type Error = AddRoleError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn conflict_retries() -> u32 {
        u32::from(super::CONFLICT_RETRIES)
    }

    fn first(&mut self) -> Result<Next<AddedRole>, AddRoleError> {
        let key = self.input.group_id.to_bytes().to_vec();
        Ok(Next::Call(read_row(GROUP_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AddedRole>, AddRoleError> {
        match self.stage {
            Stage::Group => self.read_documents(result),
            Stage::Documents => {
                let [auth_doc, reducer, realm_config] = aruna_core::rows::values::<3>(result)?;
                let group = self.group.take().ok_or(AddRoleError::GroupNotFound)?;
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
                self.after_writes()
            }
            Stage::Fence => {
                let IoResult::Values(values) = result else {
                    return Err(AddRoleError::NotFinished);
                };
                let keys = self.fence.reads().into_iter().map(|(_, key)| key);
                let rows: Vec<_> = keys.zip(values).collect();
                if !self.fence.admits(&rows) {
                    return Err(AddRoleError::PlacementFenced);
                }
                self.after_fence()
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
    input: &AddRoleConfig,
) -> Result<Vec<AdminDocumentEvent>, AdminDocumentError> {
    let mut admin_events = Vec::new();
    let event = state.apply_operation(
        &input.actor,
        AdminDocumentOperation::GroupRoleCreated {
            role: AdminRoleDefinition::from(&input.role),
        },
    )?;
    admin_events.push(event);

    for user_id in crate::sorted_user_ids(&input.role.assigned_users) {
        let event = state.apply_operation(
            &input.actor,
            AdminDocumentOperation::GroupAssignmentAdded {
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
    reducer_state: &AdminDocumentState,
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

    let materialized_assignments = reducer_state.materialized_group_assignments();
    let Some(auth_role) = auth_doc.roles.get_mut(&role.role_id) else {
        return;
    };
    for user_id in crate::sorted_user_ids(&role.assigned_users) {
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
    crate::sorted_user_ids(&role.assigned_users)
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

#[cfg(test)]
pub mod test {
    use std::collections::{HashMap, HashSet};

    use crate::driver::{DriverContext, drive};
    use crate::groups::add_member::{AddUserInput, AddUserOperation};
    use crate::groups::add_role::{AddRoleConfig, AddRoleError, AddRoleOperation, RoleAddition};
    use crate::groups::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_core::UserId;
    use aruna_core::admin_documents::{
        AdminDocumentDot, AdminDocumentOperation, AdminDocumentTarget, AdminRoleDefinition,
    };
    use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
    use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord, DocumentTarget};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, GROUP_KEYSPACE, NOTIFICATION_OUTBOX_KEYSPACE, REALM_CONFIG_KEYSPACE,
    };
    use aruna_core::operation::{Operation, TransactionMode};
    use aruna_core::reducer::{AdminConflict, AdminConflictValue, AdminDocumentState};
    use aruna_core::storage_entries::{reducer_conflict_key, reducer_state_key};
    use aruna_core::structs::execution::notification::{
        NotificationOutboxRecord, NotificationRecord,
    };
    use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission, Role};
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::types::RoleId;
    use aruna_core::{DOCUMENT_CONFLICT_KEYSPACE, DOCUMENT_STATE_KEYSPACE, SYNC_OUTBOX_KEYSPACE};
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
            graph_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
            outbound_handle: None,
        };
        (context, tempdir)
    }

    async fn setup_group(context: &DriverContext) -> (Actor, Group, GroupAuthorizationDocument) {
        let realm_id = RealmId([0u8; 32]);
        let actor = Actor {
            node_id: node(1),
            user_id: UserId::local(Ulid::generate(), realm_id),
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
            session: None,
        }
    }

    fn named_role_ids(auth_doc: &GroupAuthorizationDocument, name: &str) -> HashSet<RoleId> {
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
    fn retries_commit_conflict() {
        // A lost commit reruns the whole change, rereading every row, up to the retry bound.
        let conflict = AddRoleError::from(IoError::Conflict);
        assert!(RoleAddition::is_conflict(&conflict));
        assert_eq!(
            RoleAddition::conflict_retries(),
            u32::from(crate::groups::CONFLICT_RETRIES)
        );
    }

    #[test]
    fn rejects_reserved_names() {
        let realm_id = aruna_core::structs::identity::realm::RealmId([1u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
            user_id,
            realm_id,
        };

        for name in ["admin", "user", " admin "] {
            let mut operation = AddRoleOperation::new(AddRoleConfig {
                auth_context: aruna_core::structs::identity::auth::AuthContext {
                    user_id,
                    realm_id,
                    path_restrictions: None,
                    session: None,
                },
                actor: actor.clone(),
                realm_id,
                group_id,
                role: Role {
                    role_id: Ulid::generate(),
                    name: name.to_string(),
                    permissions: HashMap::from([(
                        format!("/{realm_id}/g/{group_id}/data/**"),
                        Permission::READ,
                    )]),
                    assigned_users: HashSet::new(),
                },
            });

            assert_eq!(operation.first(), Err(AddRoleError::ReservedRoleName));
        }
    }

    #[test]
    fn rejects_public_writes() {
        let realm_id = aruna_core::structs::identity::realm::RealmId([1u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
            user_id,
            realm_id,
        };

        for permission in [Permission::WRITE, Permission::DENY] {
            let mut operation = AddRoleOperation::new(AddRoleConfig {
                auth_context: aruna_core::structs::identity::auth::AuthContext {
                    user_id,
                    realm_id,
                    path_restrictions: None,
                    session: None,
                },
                actor: actor.clone(),
                realm_id,
                group_id,
                role: Role {
                    role_id: Ulid::generate(),
                    name: "public".to_string(),
                    permissions: HashMap::from([(
                        format!("/{realm_id}/g/{group_id}/data/**"),
                        permission,
                    )]),
                    assigned_users: HashSet::from([UserId::nil(realm_id)]),
                },
            });

            assert_eq!(operation.first(), Err(AddRoleError::InvalidPublicRole));
        }
    }

    #[test]
    fn rejects_foreign_nil() {
        let realm_id = aruna_core::structs::identity::realm::RealmId([1u8; 32]);
        let other_realm_id = aruna_core::structs::identity::realm::RealmId([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([4u8; 16]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[5u8; 32]).public(),
            user_id,
            realm_id,
        };
        let mut operation = AddRoleOperation::new(AddRoleConfig {
            auth_context: aruna_core::structs::identity::auth::AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            actor,
            realm_id,
            group_id,
            role: Role {
                role_id: Ulid::generate(),
                name: "foreign-nil".to_string(),
                permissions: HashMap::from([(
                    format!("/{realm_id}/g/{group_id}/data/**"),
                    Permission::READ,
                )]),
                assigned_users: HashSet::from([UserId::nil(other_realm_id)]),
            },
        });

        assert_eq!(operation.first(), Err(AddRoleError::InvalidAssignedUser));
    }

    #[test]
    fn rejects_unconfined_paths() {
        // Patterns outside the group subtree (other group, realm admin, wildcard
        // group, unrooted) must be refused before any effect is emitted.
        let realm_id = aruna_core::structs::identity::realm::RealmId([1u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let other_group = Ulid::from_bytes([9u8; 16]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
            user_id,
            realm_id,
        };

        for pattern in [
            format!("/{realm_id}/g/{other_group}/data/**"),
            format!("/{realm_id}/admin/**"),
            format!("/{realm_id}/g/*/data/**"),
            format!("{realm_id}/g/{group_id}/data/**"),
        ] {
            let mut operation = AddRoleOperation::new(AddRoleConfig {
                auth_context: aruna_core::structs::identity::auth::AuthContext {
                    user_id,
                    realm_id,
                    path_restrictions: None,
                    session: None,
                },
                actor: actor.clone(),
                realm_id,
                group_id,
                role: Role {
                    role_id: Ulid::generate(),
                    name: "escaping".to_string(),
                    permissions: HashMap::from([(pattern, Permission::READ)]),
                    assigned_users: HashSet::from([user_id]),
                },
            });

            assert_eq!(operation.first(), Err(AddRoleError::UnconfinedRolePath));
        }
    }

    #[tokio::test]
    pub async fn test_add_role() {
        let realm_id = aruna_core::structs::identity::realm::RealmId([0u8; 32]);
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let group_id = Ulid::generate();
        let auth_doc = GroupAuthorizationDocument::default_group_doc(user_id, realm_id, group_id);
        let group = Group {
            display_name: "test".to_string(),
            group_id,
            realm_id,
            roles: auth_doc.roles.keys().copied().collect(),
            owner: user_id,
        };
        let auth_context = aruna_core::structs::identity::auth::AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let actor = Actor {
            node_id,
            user_id,
            realm_id,
        };
        let role_id = Ulid::generate();
        let add_role_input = AddRoleConfig {
            auth_context: auth_context.clone(),
            actor: actor.clone(),
            realm_id,
            group_id,
            role: Role {
                role_id,
                name: "test_role".to_string(),
                permissions: HashMap::from([(
                    format!(
                        "/{}/g/{}/meta/{}",
                        realm_id,
                        group_id.to_string(),
                        Ulid::generate(),
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
        let mut previous_state = AdminDocumentState::new(target.clone());
        previous_state.clock.advance(add_dot.origin_node_id, 1);
        previous_state.clock.advance(remove_dot.origin_node_id, 1);
        previous_state.conflicts.insert(
            conflict_path.clone(),
            AdminConflict {
                path: conflict_path.clone(),
                values: vec![
                    AdminConflictValue {
                        value: Some(user_id.to_string()),
                        dot: add_dot,
                    },
                    AdminConflictValue {
                        value: None,
                        dot: remove_dot,
                    },
                ],
            },
        );
        let conflict_delete_keys = vec![(
            DOCUMENT_CONFLICT_KEYSPACE.to_string(),
            reducer_conflict_key(&target, &conflict_path)
                .as_ref()
                .to_vec(),
        )];

        let mut checked = AddRoleOperation::new(add_role_input.clone());
        assert!(matches!(checked.first(), Ok(Next::Call(IoCall::Child(_)))));
        let allowed = ChildOutcome::new(Ok::<bool, aruna_core::errors::AuthorizationError>(true));
        assert!(matches!(
            checked.next(IoResult::Child(allowed)),
            Ok(Next::Call(IoCall::Child(_)))
        ));

        let mut add_role_operation = RoleAddition::new(add_role_input.clone(), None);
        assert_eq!(add_role_operation.transaction(), TransactionMode::Write);
        assert_eq!(
            add_role_operation.first(),
            Ok(Next::Call(IoCall::Read(vec![(
                GROUP_KEYSPACE.to_string(),
                group_id.to_bytes().to_vec().into(),
            )])))
        );
        let documents = add_role_operation.next(IoResult::Values(vec![Some(
            group.to_bytes(&actor).unwrap().into(),
        )]));
        assert_eq!(
            documents,
            Ok(Next::Call(IoCall::Read(vec![
                (AUTH_KEYSPACE.to_string(), group_id.to_bytes().into()),
                (
                    DOCUMENT_STATE_KEYSPACE.to_string(),
                    reducer_state_key(&target),
                ),
                (
                    REALM_CONFIG_KEYSPACE.to_string(),
                    realm_id.as_bytes().to_vec().into(),
                ),
            ])))
        );

        let turn = add_role_operation.next(IoResult::Values(vec![
            Some(auth_doc.to_bytes(&actor).unwrap().into()),
            Some(postcard::to_allocvec(&previous_state).unwrap().into()),
            None,
        ]));

        let mut mutated_group = group.clone();
        mutated_group.roles.insert(add_role_input.role.role_id);
        let mut mutated_auth_doc = auth_doc.clone();
        mutated_auth_doc
            .roles
            .insert(add_role_input.role.role_id, add_role_input.role.clone());
        match turn {
            Ok(Next::Call(IoCall::Write(writes))) => {
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
                    .find(|(keyspace, _, _)| keyspace == DOCUMENT_STATE_KEYSPACE)
                    .expect("reducer state write is included");
                let outbox_records: Vec<DocumentOutboxRecord> = writes
                    .iter()
                    .filter(|(keyspace, _, _)| keyspace == SYNC_OUTBOX_KEYSPACE)
                    .map(|(_, _, value)| postcard::from_bytes(value.as_ref()).unwrap())
                    .collect();

                assert_eq!(auth_write.1, group_id.to_bytes().into());
                assert_eq!(group_write.1, group_id.to_bytes().into());
                let stored_auth_doc =
                    GroupAuthorizationDocument::from_bytes(auth_write.2.as_ref()).unwrap();
                let stored_group_doc = Group::from_bytes(group_write.2.as_ref()).unwrap();
                let reducer_state: aruna_core::reducer::AdminDocumentState =
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
                    record.target == (DocumentTarget::GroupAuthorization { group_id })
                        && matches!(
                            &record.event,
                            DocumentOutboxEvent::AdminOperation { event, .. }
                                if event.target == target
                                    && matches!(
                                        &event.op,
                                        AdminDocumentOperation::GroupRoleCreated { role }
                                            if role == &AdminRoleDefinition::from(&add_role_input.role)
                                    )
                        )
                }));
            }
            other => panic!("unexpected group role write call: {other:?}"),
        }
        assert!(mutated_group.roles.contains(&add_role_input.role.role_id));
        assert_eq!(
            mutated_auth_doc
                .roles
                .get(&add_role_input.role.role_id)
                .unwrap(),
            &add_role_input.role
        );

        let deletes = conflict_delete_keys
            .into_iter()
            .map(|(space, key)| (space, key.into()))
            .collect();
        assert_eq!(
            add_role_operation.next(IoResult::Done),
            Ok(Next::Call(IoCall::Delete(deletes)))
        );
        assert_eq!(
            add_role_operation.next(IoResult::Done),
            Ok(Next::Done((
                mutated_group.clone(),
                mutated_auth_doc.clone(),
                true,
                Vec::new()
            )))
        );

        // With admin events queued, only the drain wake follows; no member is new.
        checked.step = crate::groups::add_role::Step::Write;
        let written = ChildOutcome::new(Ok::<_, AddRoleError>((
            mutated_group.clone(),
            mutated_auth_doc.clone(),
            true,
            Vec::<aruna_core::UserId>::new(),
        )));
        let Ok(Next::Then { output, calls }) = checked.next(IoResult::Child(written)) else {
            panic!("expected the change with the drain wake");
        };
        assert_eq!(output, (mutated_group, mutated_auth_doc));
        assert!(matches!(calls.as_slice(), [IoCall::Effect(_)]));
    }

    #[tokio::test]
    async fn notifies_new_members() {
        let (context, _tempdir) = test_context().await;
        let (actor, group, auth_doc) = setup_group(&context).await;
        let second_admin = UserId::local(Ulid::generate(), actor.realm_id);
        drive(
            AddUserOperation::new(AddUserInput {
                actor: actor.clone(),
                group_id: group.group_id,
                user_id: second_admin,
                role_ids: named_role_ids(&auth_doc, "admin"),
            }),
            &context,
        )
        .await
        .unwrap();

        let member = UserId::local(Ulid::generate(), actor.realm_id);
        drive(
            AddRoleOperation::new(AddRoleConfig {
                auth_context: auth_context(&actor),
                actor: actor.clone(),
                realm_id: actor.realm_id,
                group_id: group.group_id,
                role: Role {
                    role_id: Ulid::generate(),
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
    async fn public_nil_silent() {
        let (context, _tempdir) = test_context().await;
        let (actor, group, _auth_doc) = setup_group(&context).await;

        drive(
            AddRoleOperation::new(AddRoleConfig {
                auth_context: auth_context(&actor),
                actor: actor.clone(),
                realm_id: actor.realm_id,
                group_id: group.group_id,
                role: Role {
                    role_id: Ulid::generate(),
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

    /// A realm whose buckets are activated at generation one, so an add-role
    /// resolves a generation and takes the group bucket's fence.
    fn activated_config(
        actor: &Actor,
    ) -> aruna_core::structs::identity::realm::RealmConfigDocument {
        let mut config = aruna_core::structs::identity::realm::RealmConfigDocument::new(
            actor.realm_id,
            Vec::new(),
            3,
        );
        config.ensure_node(
            actor.node_id,
            aruna_core::structs::identity::realm::RealmNodeKind::Server,
        );
        config
            .strategies
            .push(aruna_core::structs::placement::record::PlacementStrategy {
                strategy_id: Ulid::from_bytes([5; 16]),
                name: "default".to_string(),
                replica_count: Some(1),
                distinct_locations: false,
                affinity: Vec::new(),
                shard_count: 16,
            });
        config.default_strategy_id = Some(config.strategies[0].strategy_id);
        config.snapshot_candidate_map();
        config
    }

    fn role(actor: &Actor, group_id: Ulid) -> Role {
        Role {
            role_id: Ulid::from_bytes([6; 16]),
            name: "viewer".to_string(),
            permissions: HashMap::from([(
                format!("/{}/g/{group_id}/data/**", actor.realm_id),
                Permission::READ,
            )]),
            assigned_users: HashSet::new(),
        }
    }

    /// Runs a role change to the bucket fence read and answers it with `closed`.
    fn step_to_fence(
        operation: &mut RoleAddition,
        actor: &Actor,
        group: &Group,
        auth_doc: &GroupAuthorizationDocument,
        closed: Option<u64>,
    ) -> Result<Next<(Group, GroupAuthorizationDocument, bool, Vec<UserId>)>, AddRoleError> {
        let config = activated_config(actor);
        operation.first().unwrap();
        let group_row = Some(group.to_bytes(actor).unwrap().into());
        operation.next(IoResult::Values(vec![group_row])).unwrap();
        let rows = vec![
            Some(auth_doc.to_bytes(actor).unwrap().into()),
            None,
            Some(config.to_bytes(actor).unwrap().into()),
        ];
        operation.next(IoResult::Values(rows)).unwrap();
        let Ok(Next::Call(IoCall::Read(reads))) = operation.next(IoResult::Done) else {
            panic!("the transaction reads the bucket fence");
        };
        let values = reads
            .iter()
            .map(|_| {
                closed.map(|generation| {
                    aruna_core::types::Value::from(generation.to_be_bytes().to_vec())
                })
            })
            .collect();
        operation.next(IoResult::Values(values))
    }

    fn fixture() -> (Actor, Group, GroupAuthorizationDocument) {
        let realm_id = RealmId::from_bytes([2; 32]);
        let actor = Actor {
            node_id: node(3),
            user_id: UserId::local(Ulid::from_bytes([4; 16]), realm_id),
            realm_id,
        };
        let auth_doc = GroupAuthorizationDocument::default_group_doc(
            actor.user_id,
            realm_id,
            Ulid::from_bytes([9; 16]),
        );
        let group = Group {
            display_name: "Test group".to_string(),
            group_id: auth_doc.group_id,
            realm_id,
            roles: auth_doc.roles.keys().copied().collect(),
            owner: actor.user_id,
        };
        (actor, group, auth_doc)
    }

    #[test]
    fn role_takes_fence() {
        // An admitted role change stamps the generation it resolved at onto
        // every admin row, so the drain can bound the predecessor set.
        let (actor, group, auth_doc) = fixture();
        let mut operation = RoleAddition::new(
            AddRoleConfig {
                auth_context: auth_context(&actor),
                actor: actor.clone(),
                realm_id: actor.realm_id,
                group_id: group.group_id,
                role: role(&actor, group.group_id),
            },
            None,
        );

        let turn = step_to_fence(&mut operation, &actor, &group, &auth_doc, None);
        assert!(matches!(turn, Ok(Next::Done(_))));
    }

    #[test]
    fn replaced_role_marks() {
        // Replacing an existing READ role with a narrower path marks epochs due before commit.
        let (actor, group, mut auth_doc) = fixture();
        let mut old = role(&actor, group.group_id);
        old.permissions = HashMap::from([(
            format!("/{}/g/{}/**", actor.realm_id, group.group_id),
            Permission::READ,
        )]);
        auth_doc.roles.insert(old.role_id, old);
        let mut operation = RoleAddition::new(
            AddRoleConfig {
                auth_context: auth_context(&actor),
                actor: actor.clone(),
                realm_id: actor.realm_id,
                group_id: group.group_id,
                role: role(&actor, group.group_id),
            },
            None,
        );

        let turn = step_to_fence(&mut operation, &actor, &group, &auth_doc, None);
        assert!(
            matches!(turn, Ok(Next::Call(IoCall::Page { .. }))),
            "a narrowing replacement marks epochs due first, got {turn:?}"
        );
    }

    #[test]
    fn fence_rejects_role() {
        // The departing holder closed generation one: the role change must not
        // commit a row the drained bucket can no longer publish.
        let (actor, group, auth_doc) = fixture();
        let mut operation = RoleAddition::new(
            AddRoleConfig {
                auth_context: auth_context(&actor),
                actor: actor.clone(),
                realm_id: actor.realm_id,
                group_id: group.group_id,
                role: role(&actor, group.group_id),
            },
            None,
        );

        let turn = step_to_fence(&mut operation, &actor, &group, &auth_doc, Some(1));
        assert_eq!(turn, Err(AddRoleError::PlacementFenced));
    }
}
