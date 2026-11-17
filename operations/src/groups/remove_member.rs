//! Removes a user from some or all roles of a group's authorization document.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::Effect;
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::{AUTH_KEYSPACE, DOCUMENT_STATE_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode, boxed_retry};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState};
use aruna_core::rows::done;
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::execution::notification::ResourceEvent;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{GroupId, Key, KeySpace, RoleId};
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

#[derive(Clone, Debug, PartialEq)]
pub struct RemoveFromInput {
    pub actor: Actor,
    pub group_id: GroupId,
    pub user_id: UserId,
    /// None removes the user from every role of the group.
    pub role_ids: Option<HashSet<RoleId>>,
}

/// Checks the caller unless the user leaves on their own, then removes the assignments in a write
/// transaction that reruns after a lost conflict, since background writes touch the same rows.
#[derive(Debug, PartialEq)]
pub struct RemoveFromOperation {
    input: RemoveFromInput,
    authorized: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum RemoveFromError {
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
    #[error("Role not found")]
    RoleNotFound,
    #[error("Invalid user id")]
    InvalidUserId,
    #[error("Authorization document not found")]
    DocNotFound,
    #[error("cannot remove the last admin of a group")]
    LastAdmin,
    #[error(transparent)]
    CheckPermissionsError(#[from] AuthorizationError),
    #[error("Removing user from group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for RemoveFromError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "RemoveFrom".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl RemoveFromOperation {
    pub fn new(input: RemoveFromInput) -> Self {
        RemoveFromOperation {
            input,
            authorized: false,
        }
    }

    fn change(&mut self) -> Next<GroupAuthorizationDocument> {
        self.authorized = true;
        let input = self.input.clone();
        let seed = self.input.user_id.to_bytes();
        let build = move || MemberRemoval::new(input.clone());
        let child = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        Next::Call(IoCall::Child(child))
    }
}

impl Operation for RemoveFromOperation {
    type Output = GroupAuthorizationDocument;
    type Error = RemoveFromError;

    fn first(&mut self) -> Result<Next<GroupAuthorizationDocument>, RemoveFromError> {
        if self.input.user_id.is_nil() {
            return Err(RemoveFromError::InvalidUserId);
        }
        // Self-leave needs no admin permission; the last-admin guard still applies.
        if self.input.actor.user_id == self.input.user_id {
            return Ok(self.change());
        }
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: AuthContext::from(&self.input.actor),
                path: format!(
                    "/{}/g/{}/admin/users/{}",
                    self.input.actor.realm_id, self.input.group_id, self.input.user_id
                ),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<GroupAuthorizationDocument>, RemoveFromError> {
        if self.authorized {
            return result
                .child::<GroupAuthorizationDocument, RemoveFromError>()?
                .map(Next::Done);
        }
        if !result.child::<bool, AuthorizationError>()?? {
            return Err(RemoveFromError::Unauthorized);
        }
        Ok(self.change())
    }
}

/// What the member removal transaction waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Read,
    Write,
    Delete,
    Fence,
    Marks,
}

/// Reads the group's documents, removes the assignments, takes the bucket fence and marks the
/// group's key epochs due, in one write transaction.
#[derive(Debug, PartialEq)]
struct MemberRemoval {
    input: RemoveFromInput,
    stage: Stage,
    /// Bucket the authorization rows publish onto, read inside the write transaction.
    fence: WriteFence,
    written: Option<GroupAuthorizationDocument>,
    outbox: bool,
    was_member: bool,
    deletes: Vec<(KeySpace, Key)>,
    marks: DueMarks,
}

impl MemberRemoval {
    fn new(input: RemoveFromInput) -> Self {
        let marks = DueMarks::new(Some(input.group_id), unix_timestamp_millis());
        Self {
            input,
            stage: Stage::Read,
            fence: Default::default(),
            written: None,
            outbox: false,
            was_member: false,
            deletes: Vec::new(),
            marks,
        }
    }

    fn emit_auth_write(
        &mut self,
        auth_doc: Option<ByteView>,
        reducer_state_value: Option<ByteView>,
        realm_config_value: Option<ByteView>,
    ) -> Result<Next<GroupAuthorizationDocument>, RemoveFromError> {
        let mut auth_doc =
            super::parse_auth_record(auth_doc)?.ok_or(RemoveFromError::DocNotFound)?;

        let (was_member, guarded_admin_roles, role_ids) =
            remove_assignments(&mut auth_doc, &self.input)?;

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
        let admin_events = apply_reducer_updates(
            &mut reducer_state,
            &self.input.actor,
            self.input.user_id,
            &role_ids,
        )?;

        let materialized_assignments = reducer_state.materialized_group_assignments();
        for role_id in &role_ids {
            let role = auth_doc
                .roles
                .get_mut(role_id)
                .ok_or_else(|| RemoveFromError::RoleNotFound)?;
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
            return Err(RemoveFromError::LastAdmin);
        }

        let conflict_delete_keys =
            stale_conflict_deletes(previous_reducer_state.as_ref(), Some(&reducer_state));
        let mut writes = vec![
            (
                AUTH_KEYSPACE.to_string(),
                auth_doc.group_id.to_bytes().into(),
                auth_doc.to_bytes(&self.input.actor)?.into(),
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

        self.written = Some(auth_doc);
        self.outbox = !admin_events.is_empty();
        self.was_member = was_member;
        self.deletes = conflict_delete_keys;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// Takes the bucket's fence inside the transaction before committing, so a departing
    /// holder's close rejects or conflicts this write.
    fn after_writes(&mut self) -> Next<GroupAuthorizationDocument> {
        if self.fence.is_empty() {
            return self.marks();
        }
        self.stage = Stage::Fence;
        Next::Call(IoCall::Read(self.fence.reads()))
    }

    fn marks(&mut self) -> Next<GroupAuthorizationDocument> {
        self.stage = Stage::Marks;
        Next::Call(self.marks.first())
    }

    /// The notice to the removed user when the change ended their membership.
    fn removed_notice(&self, auth_doc: &GroupAuthorizationDocument) -> Option<IoCall> {
        let still_member = auth_doc
            .roles
            .values()
            .any(|role| role.assigned_users.contains(&self.input.user_id));
        if self.was_member && !still_member {
            let records = route_resource_event(
                &ResourceEvent::GroupMemberRemoved {
                    group_id: self.input.group_id,
                    affected_user: self.input.user_id,
                    actor_user_id: self.input.actor.user_id,
                },
                RoutingContext {
                    group_auth: Some(auth_doc),
                    realm_auth: None,
                    realm_id: self.input.actor.realm_id,
                },
                unix_timestamp_millis(),
            );
            if let (false, Effect::SubOperation(child)) =
                (records.is_empty(), emit_notifications_effect(records))
            {
                return Some(IoCall::Child(child));
            }
        }
        None
    }

    /// After the commit, wakes the outbox drain when admin events were queued and notifies a
    /// removed member; failures there are only logged, as the change itself is durable.
    fn finish(&mut self) -> Result<Next<GroupAuthorizationDocument>, RemoveFromError> {
        let output = self.written.take().ok_or(RemoveFromError::NotFinished)?;
        let mut calls = Vec::new();
        if self.outbox {
            calls.push(IoCall::Effect(Box::new(schedule_drain_effect())));
        }
        calls.extend(self.removed_notice(&output));
        if calls.is_empty() {
            return Ok(Next::Done(output));
        }
        Ok(Next::Then { output, calls })
    }
}

impl Operation for MemberRemoval {
    type Output = GroupAuthorizationDocument;
    type Error = RemoveFromError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn conflict_retries() -> u32 {
        u32::from(super::CONFLICT_RETRIES)
    }

    fn first(&mut self) -> Result<Next<GroupAuthorizationDocument>, RemoveFromError> {
        let target = AdminDocumentTarget::Group {
            group_id: self.input.group_id,
        };
        Ok(Next::Call(IoCall::Read(vec![
            (
                AUTH_KEYSPACE.to_string(),
                ByteView::from(self.input.group_id.to_bytes()),
            ),
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

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<GroupAuthorizationDocument>, RemoveFromError> {
        match self.stage {
            Stage::Read => {
                let [auth_doc, reducer, realm_config] = aruna_core::rows::values::<3>(result)?;
                self.emit_auth_write(auth_doc, reducer, realm_config)
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
                    return Err(RemoveFromError::NotFinished);
                };
                let keys = self.fence.reads().into_iter().map(|(_, key)| key);
                let rows: Vec<_> = keys.zip(values).collect();
                if !self.fence.admits(&rows) {
                    return Err(RemoveFromError::PlacementFenced);
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

fn remove_assignments(
    auth_doc: &mut GroupAuthorizationDocument,
    input: &RemoveFromInput,
) -> Result<(bool, Vec<RoleId>, Vec<RoleId>), RemoveFromError> {
    let was_member = auth_doc
        .roles
        .values()
        .any(|role| role.assigned_users.contains(&input.user_id));
    let guarded_admin_roles = auth_doc
        .roles
        .iter()
        .filter_map(|(role_id, role)| {
            (role.name == "admin" && role.assigned_users.contains(&input.user_id))
                .then_some(*role_id)
        })
        .collect();
    let role_ids = match &input.role_ids {
        Some(role_ids) => {
            let mut sorted_role_ids: Vec<_> = role_ids.iter().copied().collect();
            sorted_role_ids.sort();
            for role_id in &sorted_role_ids {
                let role = auth_doc
                    .roles
                    .get_mut(role_id)
                    .ok_or(RemoveFromError::RoleNotFound)?;
                role.assigned_users.remove(&input.user_id);
            }
            sorted_role_ids
        }
        None => {
            let mut sorted_role_ids = Vec::new();
            for (role_id, role) in &mut auth_doc.roles {
                if role.assigned_users.remove(&input.user_id) {
                    sorted_role_ids.push(*role_id);
                }
            }
            sorted_role_ids.sort();
            sorted_role_ids
        }
    };
    Ok((was_member, guarded_admin_roles, role_ids))
}

fn apply_reducer_updates(
    state: &mut AdminDocumentState,
    actor: &Actor,
    user_id: UserId,
    role_ids: &[RoleId],
) -> Result<Vec<AdminDocumentEvent>, AdminDocumentError> {
    let mut admin_events = Vec::new();
    for role_id in role_ids {
        if should_seed_role(state, *role_id) {
            let event = state.apply_operation(
                actor,
                AdminDocumentOperation::GroupRoleAdded { role_id: *role_id },
            )?;
            admin_events.push(event);
        }
        let event = state.apply_operation(
            actor,
            AdminDocumentOperation::GroupAssignmentRemoved {
                role_id: *role_id,
                user_id,
            },
        )?;
        admin_events.push(event);
    }

    Ok(admin_events)
}

fn should_seed_role(state: &AdminDocumentState, role_id: RoleId) -> bool {
    !state.materialized_group_roles().contains(&role_id)
        && !state
            .conflicts
            .contains_key(&format!("group.roles.{role_id}"))
}

#[cfg(test)]
pub mod test {
    use std::collections::{HashMap, HashSet};

    use aruna_core::SYNC_OUTBOX_KEYSPACE;
    use aruna_core::UserId;
    use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
    use aruna_core::calls::{IoCall, IoError, IoResult, Next};
    use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord, DocumentTarget};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, NOTIFICATION_OUTBOX_KEYSPACE};
    use aruna_core::operation::{Operation, TransactionMode};
    use aruna_core::structs::execution::notification::{
        NotificationOutboxRecord, NotificationRecord,
    };
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::auth::{Permission, Role};
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::types::RoleId;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    use crate::document_repository::{GetGroupConfig, GetGroupOperation};
    use crate::driver::{DriverContext, drive};
    use crate::groups::add_member::{AddUserInput, AddUserOperation};
    use crate::groups::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::groups::remove_member::{
        MemberRemoval, RemoveFromError, RemoveFromInput, RemoveFromOperation,
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

    fn named_role_ids(auth_doc: &GroupAuthorizationDocument, name: &str) -> HashSet<RoleId> {
        auth_doc
            .roles
            .iter()
            .filter_map(|(k, v)| (v.name == name).then_some(*k))
            .collect()
    }

    #[test]
    fn rejects_nil_target() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let owner_id = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let role_id = Ulid::from_bytes([4u8; 16]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[5u8; 32]).public(),
            user_id: owner_id,
            realm_id,
        };
        let mut operation = RemoveFromOperation::new(RemoveFromInput {
            actor,
            group_id,
            user_id: UserId::nil(realm_id),
            role_ids: Some(HashSet::from([role_id])),
        });

        assert_eq!(operation.first(), Err(RemoveFromError::InvalidUserId));
    }

    #[test]
    fn retries_commit_conflict() {
        // A lost commit reruns the whole removal, rereading every row, up to the retry bound.
        let conflict = RemoveFromError::from(IoError::Conflict);
        assert!(MemberRemoval::is_conflict(&conflict));
        assert_eq!(
            MemberRemoval::conflict_retries(),
            u32::from(crate::groups::CONFLICT_RETRIES)
        );
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([9u8; 16]), realm_id),
            realm_id,
        };
        let input = RemoveFromInput {
            actor,
            group_id: Ulid::from_bytes([7u8; 16]),
            user_id: UserId::local(Ulid::from_bytes([10u8; 16]), realm_id),
            role_ids: None,
        };
        assert_eq!(
            MemberRemoval::new(input).transaction(),
            TransactionMode::Write
        );
    }

    #[test]
    fn queues_removal_outbox() {
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
            policies: Vec::new(),
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
        let mut operation = MemberRemoval::new(RemoveFromInput {
            actor: actor.clone(),
            group_id,
            user_id: member_id,
            role_ids: Some(HashSet::from([role_id])),
        });

        let turn =
            operation.emit_auth_write(Some(auth_doc.to_bytes(&actor).unwrap().into()), None, None);

        let (stored_auth_doc, outbox_records) = match turn {
            Ok(Next::Call(IoCall::Write(writes))) => {
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
                    GroupAuthorizationDocument::from_bytes(auth_write.2.as_ref()).unwrap(),
                    outbox_records,
                )
            }
            other => panic!("unexpected write call: {other:?}"),
        };

        assert!(
            !stored_auth_doc.roles[&role_id]
                .assigned_users
                .contains(&member_id)
        );
        assert_eq!(outbox_records.len(), 2);
        assert!(
            outbox_records.iter().all(|record| {
                record.target == (DocumentTarget::GroupAuthorization { group_id })
            })
        );
        let events: Vec<_> = outbox_records
            .iter()
            .map(|record| match &record.event {
                DocumentOutboxEvent::AdminOperation { event, .. } => event.as_ref(),
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
            AdminDocumentOperation::GroupAssignmentRemoved {
                role_id: event_role_id,
                user_id: event_user_id,
            } if *event_role_id == role_id && *event_user_id == member_id
        ));
    }

    #[tokio::test]
    pub async fn removes_all_roles() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, auth_doc) = setup_group(&context).await;

        let member_id = UserId::local(Ulid::generate(), actor.realm_id);
        let add_input = AddUserInput {
            actor: actor.clone(),
            group_id: group.group_id,
            user_id: member_id,
            role_ids: named_role_ids(&auth_doc, "user"),
        };
        let auth_doc = drive(AddUserOperation::new(add_input), &context)
            .await
            .unwrap();
        assert!(
            auth_doc
                .roles
                .values()
                .any(|role| role.assigned_users.contains(&member_id))
        );

        let remove_input = RemoveFromInput {
            actor,
            group_id: group.group_id,
            user_id: member_id,
            role_ids: None,
        };
        let auth_doc = drive(RemoveFromOperation::new(remove_input), &context)
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
    pub async fn protects_last_admin() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, _auth_doc) = setup_group(&context).await;

        let remove_input = RemoveFromInput {
            actor: actor.clone(),
            group_id: group.group_id,
            user_id: actor.user_id,
            role_ids: None,
        };
        let result = drive(RemoveFromOperation::new(remove_input), &context).await;
        assert_eq!(result.unwrap_err(), RemoveFromError::LastAdmin);

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

        let member_id = UserId::local(Ulid::generate(), actor.realm_id);
        let add_input = AddUserInput {
            actor: actor.clone(),
            group_id: group.group_id,
            user_id: member_id,
            role_ids: named_role_ids(&auth_doc, "user"),
        };
        drive(AddUserOperation::new(add_input), &context)
            .await
            .unwrap();

        let member_actor = Actor {
            node_id: actor.node_id,
            user_id: member_id,
            realm_id: actor.realm_id,
        };
        let remove_input = RemoveFromInput {
            actor: member_actor,
            group_id: group.group_id,
            user_id: member_id,
            role_ids: None,
        };
        let auth_doc = drive(RemoveFromOperation::new(remove_input), &context)
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
    async fn emits_removed_notice() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, auth_doc) = setup_group(&context).await;

        let member = UserId::local(Ulid::generate(), actor.realm_id);
        drive(
            AddUserOperation::new(AddUserInput {
                actor: actor.clone(),
                group_id: group.group_id,
                user_id: member,
                role_ids: named_role_ids(&auth_doc, "user"),
            }),
            &context,
        )
        .await
        .unwrap();

        drive(
            RemoveFromOperation::new(RemoveFromInput {
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
    fn emit_failure_succeeds() {
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
            policies: Vec::new(),
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
        let mut operation = MemberRemoval::new(RemoveFromInput {
            actor: actor.clone(),
            group_id,
            user_id: member_id,
            role_ids: Some(HashSet::from([role_id])),
        });
        operation.first().unwrap();
        let rows = vec![Some(auth_doc.to_bytes(&actor).unwrap().into()), None, None];
        assert!(matches!(
            operation.next(IoResult::Values(rows)),
            Ok(Next::Call(IoCall::Write(_)))
        ));
        assert!(matches!(
            operation.next(IoResult::Done),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        let empty = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        // The drain timer and the removal notice run after the commit; their failure is only
        // logged, so the removal is the outcome.
        let Ok(Next::Then { output, calls }) = operation.next(empty) else {
            panic!("expected the removal with the calls after commit");
        };
        assert!(matches!(
            calls.as_slice(),
            [IoCall::Effect(_), IoCall::Child(_)]
        ));
        assert!(!output.roles[&role_id].assigned_users.contains(&member_id));
    }

    #[tokio::test]
    async fn non_member_silent() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, _auth_doc) = setup_group(&context).await;

        let stranger = UserId::local(Ulid::generate(), actor.realm_id);
        drive(
            RemoveFromOperation::new(RemoveFromInput {
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
    async fn role_revoke_silent() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, auth_doc) = setup_group(&context).await;

        let member = UserId::local(Ulid::generate(), actor.realm_id);
        let mut both_roles = named_role_ids(&auth_doc, "user");
        both_roles.extend(named_role_ids(&auth_doc, "admin"));
        drive(
            AddUserOperation::new(AddUserInput {
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
            RemoveFromOperation::new(RemoveFromInput {
                actor: actor.clone(),
                group_id: group.group_id,
                user_id: member,
                role_ids: Some(named_role_ids(&auth_doc, "user")),
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
