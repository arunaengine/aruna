//! Adds a user with its roles to a group's authorization document.
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
pub struct AddUserInput {
    pub actor: Actor,
    pub group_id: GroupId,
    pub user_id: UserId,
    pub role_ids: HashSet<RoleId>,
}

/// The written document, whether admin events reached the outbox, and whether the user is a new
/// member.
type AddedMember = (GroupAuthorizationDocument, bool, bool);

/// What the member addition waits for after the caller check.
#[derive(Debug, PartialEq)]
enum Step {
    Check,
    Write,
    Announce,
}

/// Checks the caller, adds the member in a write transaction that reruns after a lost conflict,
/// then wakes the outbox drain or announces the document, and notifies a new member.
#[derive(Debug, PartialEq)]
pub struct AddUserOperation {
    input: AddUserInput,
    step: Step,
    added: Option<(GroupAuthorizationDocument, bool)>,
}

#[derive(Debug, Error, PartialEq)]
pub enum AddUserError {
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
    #[error(transparent)]
    CheckPermissionsError(#[from] AuthorizationError),
    #[error("Adding user to group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for AddUserError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "AddUser".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl AddUserOperation {
    pub fn new(input: AddUserInput) -> Self {
        AddUserOperation {
            input,
            step: Step::Check,
            added: None,
        }
    }

    fn change(&mut self) -> Next<GroupAuthorizationDocument> {
        self.step = Step::Write;
        let input = self.input.clone();
        let seed = self.input.user_id.to_bytes();
        let build = move || MemberAddition::new(input.clone());
        let child = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        Next::Call(IoCall::Child(child))
    }

    fn written(
        &mut self,
        result: IoResult,
    ) -> Result<Next<GroupAuthorizationDocument>, AddUserError> {
        let (auth_doc, outbox, newly_added) = result.child::<AddedMember, AddUserError>()??;
        if outbox {
            // A failed drain timer leaves the durable outbox retryable.
            let drain = IoCall::Effect(Box::new(schedule_drain_effect()));
            return Ok(self.finish(auth_doc, newly_added, Some(drain)));
        }
        let document = DocumentTarget::GroupAuthorization {
            group_id: auth_doc.group_id,
        };
        let announce = crate::realm::announce_call(&self.input.actor, document);
        let announce = announce.ok_or(AddUserError::NotFinished)?;
        self.added = Some((auth_doc, newly_added));
        self.step = Step::Announce;
        Ok(Next::Call(announce))
    }

    fn announced(
        &mut self,
        result: IoResult,
    ) -> Result<Next<GroupAuthorizationDocument>, AddUserError> {
        let outcome =
            crate::realm::announced(result).map_err(|got| AddUserError::UnexpectedEvent {
                state: "Announce".to_string(),
                expected: "Event::SubOperation(SubOperationEvent::DocumentSyncResult)",
                got,
            })?;
        outcome.map_err(AddUserError::TopicAnnouncement)?;
        let (auth_doc, newly_added) = self.added.take().ok_or(AddUserError::NotFinished)?;
        Ok(self.finish(auth_doc, newly_added, None))
    }

    /// The document, with the drain wake and a new member's notice made after it; their failures
    /// are only logged, as the change is already durable.
    fn finish(
        &self,
        auth_doc: GroupAuthorizationDocument,
        newly_added: bool,
        drain: Option<IoCall>,
    ) -> Next<GroupAuthorizationDocument> {
        let mut calls: Vec<IoCall> = drain.into_iter().collect();
        calls.extend(self.added_notice(&auth_doc, newly_added));
        if calls.is_empty() {
            return Next::Done(auth_doc);
        }
        Next::Then {
            output: auth_doc,
            calls,
        }
    }

    /// The notice to a member the change newly added.
    fn added_notice(
        &self,
        auth_doc: &GroupAuthorizationDocument,
        newly_added: bool,
    ) -> Option<IoCall> {
        if newly_added {
            let records = route_resource_event(
                &ResourceEvent::GroupMemberAdded {
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
}

impl Operation for AddUserOperation {
    type Output = GroupAuthorizationDocument;
    type Error = AddUserError;

    fn first(&mut self) -> Result<Next<GroupAuthorizationDocument>, AddUserError> {
        if self.input.user_id.is_nil() {
            return Err(AddUserError::InvalidUserId);
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

    fn next(&mut self, result: IoResult) -> Result<Next<GroupAuthorizationDocument>, AddUserError> {
        match self.step {
            Step::Check => {
                if !result.child::<bool, AuthorizationError>()?? {
                    return Err(AddUserError::Unauthorized);
                }
                Ok(self.change())
            }
            Step::Write => self.written(result),
            Step::Announce => self.announced(result),
        }
    }
}

/// What the member addition transaction waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Read,
    Write,
    Delete,
    Fence,
    Marks,
}

/// Reads the group's documents, writes the assignments, takes the bucket fence, and marks the
/// group's key epochs due when a DENY role is assigned, in one write transaction.
#[derive(Debug, PartialEq)]
struct MemberAddition {
    input: AddUserInput,
    stage: Stage,
    /// Bucket the authorization rows publish onto, read inside the write transaction.
    fence: WriteFence,
    deny: bool,
    written: Option<AddedMember>,
    deletes: Vec<(KeySpace, Key)>,
    marks: DueMarks,
}

impl MemberAddition {
    fn new(input: AddUserInput) -> Self {
        let marks = DueMarks::new(Some(input.group_id), unix_timestamp_millis());
        Self {
            input,
            stage: Stage::Read,
            fence: Default::default(),
            deny: false,
            written: None,
            deletes: Vec::new(),
            marks,
        }
    }

    fn emit_auth_write(
        &mut self,
        auth_doc: Option<ByteView>,
        reducer_state_value: Option<ByteView>,
        realm_config_value: Option<ByteView>,
    ) -> Result<Next<AddedMember>, AddUserError> {
        let mut auth_doc = super::parse_auth_record(auth_doc)?.ok_or(AddUserError::DocNotFound)?;
        let role_ids = sorted_role_ids(&self.input.role_ids);
        for role_id in &role_ids {
            if !auth_doc.roles.contains_key(role_id) {
                return Err(AddUserError::RoleNotFound);
            }
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
        let admin_events = apply_reducer_updates(&mut reducer_state, &self.input, &role_ids)?;

        let was_member = auth_doc
            .roles
            .values()
            .any(|role| role.assigned_users.contains(&self.input.user_id));

        let materialized_assignments = reducer_state.materialized_group_assignments();
        for role_id in role_ids {
            let role = auth_doc
                .roles
                .get_mut(&role_id)
                .ok_or_else(|| AddUserError::RoleNotFound)?;
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
        let stale_conflict_deletes =
            stale_conflict_deletes(previous_reducer_state.as_ref(), Some(&reducer_state));
        let mut writes = vec![
            (AUTH_KEYSPACE.to_string(), key, value),
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

        // An assigned DENY rule can narrow the member's existing READ scopes.
        self.deny = self.input.role_ids.iter().any(|id| {
            auth_doc
                .roles
                .get(id)
                .is_some_and(|r| r.permissions.values().any(|p| *p == Permission::DENY))
        });
        self.written = Some((auth_doc, !admin_events.is_empty(), newly_added));
        self.deletes = stale_conflict_deletes;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// Takes the bucket's fence inside the transaction before committing, so a departing
    /// holder's close rejects or conflicts this write.
    fn after_writes(&mut self) -> Result<Next<AddedMember>, AddUserError> {
        if self.fence.is_empty() {
            return self.after_fence();
        }
        self.stage = Stage::Fence;
        Ok(Next::Call(IoCall::Read(self.fence.reads())))
    }

    fn after_fence(&mut self) -> Result<Next<AddedMember>, AddUserError> {
        if self.deny {
            self.stage = Stage::Marks;
            return Ok(Next::Call(self.marks.first()));
        }
        self.finish()
    }

    fn finish(&mut self) -> Result<Next<AddedMember>, AddUserError> {
        self.written
            .take()
            .map(Next::Done)
            .ok_or(AddUserError::NotFinished)
    }
}

impl Operation for MemberAddition {
    type Output = AddedMember;
    type Error = AddUserError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn conflict_retries() -> u32 {
        u32::from(super::CONFLICT_RETRIES)
    }

    fn first(&mut self) -> Result<Next<AddedMember>, AddUserError> {
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

    fn next(&mut self, result: IoResult) -> Result<Next<AddedMember>, AddUserError> {
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
                self.after_writes()
            }
            Stage::Fence => {
                let IoResult::Values(values) = result else {
                    return Err(AddUserError::NotFinished);
                };
                let keys = self.fence.reads().into_iter().map(|(_, key)| key);
                let rows: Vec<_> = keys.zip(values).collect();
                if !self.fence.admits(&rows) {
                    return Err(AddUserError::PlacementFenced);
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

fn sorted_role_ids(role_ids: &HashSet<RoleId>) -> Vec<RoleId> {
    let mut role_ids: Vec<_> = role_ids.iter().copied().collect();
    role_ids.sort();
    role_ids
}

fn apply_reducer_updates(
    state: &mut AdminDocumentState,
    input: &AddUserInput,
    role_ids: &[RoleId],
) -> Result<Vec<AdminDocumentEvent>, AdminDocumentError> {
    let mut admin_events = Vec::new();
    for role_id in role_ids {
        if should_seed_role(state, *role_id) {
            let event = state.apply_operation(
                &input.actor,
                AdminDocumentOperation::GroupRoleAdded { role_id: *role_id },
            )?;
            admin_events.push(event);
        }
        let event = state.apply_operation(
            &input.actor,
            AdminDocumentOperation::GroupAssignmentAdded {
                role_id: *role_id,
                user_id: input.user_id,
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
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

    use aruna_core::UserId;
    use aruna_core::admin_documents::{
        AdminDocumentClock, AdminDocumentDot, AdminDocumentOperation, AdminDocumentTarget,
    };
    use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
    use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord, DocumentTarget};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::NOTIFICATION_OUTBOX_KEYSPACE;
    use aruna_core::operation::{Operation, TransactionMode};
    use aruna_core::reducer::{
        AdminAttributeVersion, AdminConflict, AdminConflictValue, AdminDocumentState,
    };
    use aruna_core::storage_entries::reducer_conflict_key;
    use aruna_core::structs::execution::notification::{
        NotificationOutboxRecord, NotificationRecord,
    };
    use aruna_core::structs::identity::auth::{Actor, Permission, Role};
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::types::RoleId;
    use aruna_core::{
        AUTH_KEYSPACE, DOCUMENT_CONFLICT_KEYSPACE, DOCUMENT_STATE_KEYSPACE, SYNC_OUTBOX_KEYSPACE,
    };
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    use crate::driver::{DriverContext, drive};
    use crate::groups::add_member::{
        AddUserError, AddUserInput, AddUserOperation, MemberAddition, Step,
    };
    use crate::groups::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};

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
    ) -> AdminConflict {
        AdminConflict {
            path: assignment_path(role_id, user_id),
            values: vec![
                AdminConflictValue {
                    value: Some(user_id.to_string()),
                    dot: dot(add_seed),
                },
                AdminConflictValue {
                    value: None,
                    dot: dot(remove_seed),
                },
            ],
        }
    }

    fn assignment_conflict_state(
        group_id: Ulid,
        role_id: RoleId,
        assigned_user_id: UserId,
        conflict_user_id: UserId,
    ) -> AdminDocumentState {
        let role_dot = dot(3);
        let mut clock = AdminDocumentClock::default();
        for dot in [role_dot, dot(11), dot(12), dot(13), dot(14)] {
            clock.advance(dot.origin_node_id, dot.origin_seq);
        }

        AdminDocumentState {
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
                    assignment_path(role_id, conflict_user_id),
                    assignment_conflict(role_id, conflict_user_id, 13, 14),
                ),
            ]),
            user_name: None,
            user_subject_ids: BTreeMap::from([(
                format!("group.roles.{role_id}"),
                AdminAttributeVersion {
                    value: Some(role_id.to_string()),
                    dot: role_dot,
                },
            )]),
            equivalent_value_dots: BTreeMap::new(),
            revocation_floor: 0,
            revocation_next_expiry: None,
        }
    }

    fn role_conflict_state(group_id: Ulid, role_id: RoleId) -> AdminDocumentState {
        let path = format!("group.roles.{role_id}");
        AdminDocumentState {
            target: AdminDocumentTarget::Group { group_id },
            clock: AdminDocumentClock::default(),
            applied_event_ids: BTreeSet::new(),
            user_attributes: BTreeMap::new(),
            conflicts: BTreeMap::from([(
                path.clone(),
                AdminConflict {
                    path,
                    values: vec![
                        AdminConflictValue {
                            value: Some(role_id.to_string()),
                            dot: dot(21),
                        },
                        AdminConflictValue {
                            value: None,
                            dot: dot(22),
                        },
                    ],
                },
            )]),
            user_name: None,
            user_subject_ids: BTreeMap::new(),
            equivalent_value_dots: BTreeMap::new(),
            revocation_floor: 0,
            revocation_next_expiry: None,
        }
    }

    #[test]
    fn rejects_nil_member() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let owner_id = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let role_id = Ulid::from_bytes([4u8; 16]);
        let actor = Actor {
            node_id: node(5),
            user_id: owner_id,
            realm_id,
        };
        let mut operation = AddUserOperation::new(AddUserInput {
            actor,
            group_id,
            user_id: UserId::nil(realm_id),
            role_ids: HashSet::from([role_id]),
        });

        assert_eq!(operation.first(), Err(AddUserError::InvalidUserId));
    }

    #[test]
    fn retries_commit_conflict() {
        // A lost commit reruns the whole addition, rereading every row, up to the retry bound.
        let conflict = AddUserError::from(IoError::Conflict);
        assert!(MemberAddition::is_conflict(&conflict));
        assert_eq!(
            MemberAddition::conflict_retries(),
            u32::from(crate::groups::CONFLICT_RETRIES)
        );
    }

    #[test]
    fn seeds_missing_role() {
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
            policies: Vec::new(),
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
        let input = AddUserInput {
            actor: actor.clone(),
            group_id,
            user_id: assigned_user_id,
            role_ids: HashSet::from([role_id]),
        };
        let target = AdminDocumentTarget::Group { group_id };
        let mut operation = MemberAddition::new(input);

        let turn =
            operation.emit_auth_write(Some(auth_doc.to_bytes(&actor).unwrap().into()), None, None);
        let outbox_records: Vec<DocumentOutboxRecord> = match turn {
            Ok(Next::Call(IoCall::Write(writes))) => writes
                .iter()
                .filter(|(keyspace, _, _)| keyspace == SYNC_OUTBOX_KEYSPACE)
                .map(|(_, _, value)| postcard::from_bytes(value).unwrap())
                .collect(),
            other => panic!("unexpected write call: {other:?}"),
        };
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
            AdminDocumentOperation::GroupAssignmentAdded {
                role_id: event_role_id,
                user_id: event_user_id,
            } if *event_role_id == role_id && *event_user_id == assigned_user_id
        ));
    }

    #[test]
    fn writes_reducer_conflicts() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let owner_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let assigned_user_id = UserId::local(Ulid::from_bytes([4u8; 16]), realm_id);
        let conflict_user_id = UserId::local(Ulid::from_bytes([5u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([6u8; 16]);
        let role_id = Ulid::from_bytes([7u8; 16]);
        let actor = Actor {
            node_id: node(8),
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
                    assigned_users: HashSet::new(),
                },
            )]),
        };
        let previous_state =
            assignment_conflict_state(group_id, role_id, assigned_user_id, conflict_user_id);
        let input = AddUserInput {
            actor: actor.clone(),
            group_id,
            user_id: assigned_user_id,
            role_ids: HashSet::from([role_id]),
        };
        let target = AdminDocumentTarget::Group { group_id };
        let document_target = DocumentTarget::GroupAuthorization { group_id };
        let mut realm_config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        realm_config.seed_default_placement();
        for seed in 20..24u8 {
            realm_config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        let expected_placement = aruna_core::placement::target_placement_ref(
            &realm_config,
            &document_target,
            Default::default(),
        );
        assert_ne!(expected_placement, PlacementRef::NIL);
        let mut operation = MemberAddition::new(input);
        assert_eq!(operation.transaction(), TransactionMode::Write);

        assert!(
            matches!(operation.first(), Ok(Next::Call(IoCall::Read(reads))) if reads.len() == 3)
        );
        let turn = operation.next(IoResult::Values(vec![
            Some(auth_doc.to_bytes(&actor).unwrap().into()),
            Some(postcard::to_allocvec(&previous_state).unwrap().into()),
            Some(realm_config.to_bytes(&actor).unwrap().into()),
        ]));
        let (updated_auth_doc, reducer_state, outbox_records) = match turn {
            Ok(Next::Call(IoCall::Write(writes))) => {
                let auth_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == AUTH_KEYSPACE)
                    .expect("auth doc write is included");
                let reducer_state_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == DOCUMENT_STATE_KEYSPACE)
                    .expect("reducer state write is included");
                let outbox_records: Vec<DocumentOutboxRecord> = writes
                    .iter()
                    .filter(|(keyspace, _, _)| keyspace == SYNC_OUTBOX_KEYSPACE)
                    .map(|(_, _, value)| postcard::from_bytes(value).unwrap())
                    .collect();
                let conflict_writes: Vec<_> = writes
                    .iter()
                    .filter(|(keyspace, _, _)| keyspace == DOCUMENT_CONFLICT_KEYSPACE)
                    .collect();
                assert_eq!(conflict_writes.len(), 1);
                let conflict: AdminConflict =
                    postcard::from_bytes(conflict_writes[0].2.as_ref()).unwrap();
                assert_eq!(conflict.path, assignment_path(role_id, conflict_user_id));

                (
                    GroupAuthorizationDocument::from_bytes(auth_write.2.as_ref()).unwrap(),
                    postcard::from_bytes::<AdminDocumentState>(reducer_state_write.2.as_ref())
                        .unwrap(),
                    outbox_records,
                )
            }
            other => panic!("unexpected write call: {other:?}"),
        };
        assert_eq!(outbox_records.len(), 1);
        assert_eq!(outbox_records[0].target, document_target);
        assert_eq!(outbox_records[0].placement, expected_placement);
        assert!(matches!(
            &outbox_records[0].event,
            DocumentOutboxEvent::AdminOperation { event, .. }
                if event.target == target
                    && matches!(
                        &event.op,
                        AdminDocumentOperation::GroupAssignmentAdded {
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
            reducer_state.materialized_group_assignments(),
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
                .contains_key(&assignment_path(role_id, conflict_user_id))
        );

        let deletes = vec![(
            DOCUMENT_CONFLICT_KEYSPACE.to_string(),
            reducer_conflict_key(&target, &assignment_path(role_id, assigned_user_id)),
        )];
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Call(IoCall::Delete(deletes)))
        );
        // The group's bucket fence is read inside the transaction before the commit.
        assert!(matches!(
            operation.next(IoResult::Done),
            Ok(Next::Call(IoCall::Read(reads))) if !reads.is_empty()
        ));

        // With admin events queued, the drain wake and the new member's notice follow.
        let mut checked = AddUserOperation::new(operation.input.clone());
        checked.step = Step::Write;
        let written = ChildOutcome::new(Ok::<_, AddUserError>((updated_auth_doc, true, true)));
        let Ok(Next::Then { calls, .. }) = checked.next(IoResult::Child(written)) else {
            panic!("expected the document with the calls after the write");
        };
        assert!(matches!(
            calls.as_slice(),
            [IoCall::Effect(_), IoCall::Child(_)]
        ));
    }

    #[test]
    fn conflict_skips_notification() {
        let realm_id = RealmId::from_bytes([31u8; 32]);
        let actor = Actor {
            node_id: node(32),
            user_id: UserId::local(Ulid::from_bytes([33u8; 16]), realm_id),
            realm_id,
        };
        let member_id = UserId::local(Ulid::from_bytes([34u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([35u8; 16]);
        let role_id = Ulid::from_bytes([36u8; 16]);
        let auth_doc = seeded_user_role(group_id, role_id);
        let reducer_state = role_conflict_state(group_id, role_id);
        let input = AddUserInput {
            actor: actor.clone(),
            group_id,
            user_id: member_id,
            role_ids: HashSet::from([role_id]),
        };
        let mut operation = MemberAddition::new(input.clone());
        let turn = operation.emit_auth_write(
            Some(auth_doc.to_bytes(&actor).unwrap().into()),
            Some(postcard::to_allocvec(&reducer_state).unwrap().into()),
            None,
        );
        assert!(matches!(turn, Ok(Next::Call(IoCall::Write(_)))));
        let Ok(Next::Done((result, outbox, newly_added))) = operation.next(IoResult::Done) else {
            panic!("expected the written document");
        };
        assert!(!newly_added);
        assert!(!result.roles[&role_id].assigned_users.contains(&member_id));

        // A user who is no new member gets no notice; only the drain wake follows.
        let mut checked = AddUserOperation::new(input);
        checked.step = Step::Write;
        let written = ChildOutcome::new(Ok::<_, AddUserError>((result, outbox, newly_added)));
        let Ok(Next::Then { calls, .. }) = checked.next(IoResult::Child(written)) else {
            panic!("expected the drain wake after the write");
        };
        assert!(matches!(calls.as_slice(), [IoCall::Effect(_)]));
    }

    #[test]
    fn deny_assignment_marks() {
        let realm_id = RealmId::from_bytes([41u8; 32]);
        let actor = Actor {
            node_id: node(42),
            user_id: UserId::local(Ulid::from_bytes([43u8; 16]), realm_id),
            realm_id,
        };
        let member_id = UserId::local(Ulid::from_bytes([44u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([45u8; 16]);
        let role_id = Ulid::from_bytes([46u8; 16]);
        let mut auth_doc = seeded_user_role(group_id, role_id);
        let role = auth_doc.roles.get_mut(&role_id).unwrap();
        role.permissions
            .insert("/test".to_string(), Permission::DENY);
        let mut operation = MemberAddition::new(AddUserInput {
            actor: actor.clone(),
            group_id,
            user_id: member_id,
            role_ids: HashSet::from([role_id]),
        });
        let doc = Some(auth_doc.to_bytes(&actor).unwrap().into());
        operation.emit_auth_write(doc, None, None).unwrap();

        // The existing DENY role's assignment marks epochs due inside its transaction.
        assert!(matches!(
            operation.next(IoResult::Done),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
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
            graph_handle: None,
            task_handle: Some(task_handle),
            compute_handle: None,
            outbound_handle: None,
            blob_handle: None,
        };

        let realm_id = aruna_core::structs::identity::realm::RealmId([0u8; 32]);
        let user_id = UserId::local(Ulid::generate(), realm_id);
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

        let add_user_input = AddUserInput {
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            group_id: group.group_id,
            user_id: UserId::local(Ulid::generate(), realm_id),
            role_ids: reader_writer_roles,
        };

        let add_user_operation = AddUserOperation::new(add_user_input.clone());
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
            graph_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
            outbound_handle: None,
            blob_handle: None,
        };
        (context, net_handle, random_path)
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

    fn seeded_user_role(group_id: Ulid, role_id: Ulid) -> GroupAuthorizationDocument {
        GroupAuthorizationDocument {
            group_id,
            policies: Vec::new(),
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
    async fn emits_membership_notifications() {
        let (context, net_handle, _tmp) = add_context().await;
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

    /// The calls after the write of a new member `user_id`, added by the actor seeded with 13.
    fn calls_after_write(user_seed: u8) -> Vec<IoCall> {
        let realm_id = RealmId::from_bytes([12u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([user_seed; 16]), realm_id);
        let group_id = Ulid::from_bytes([15u8; 16]);
        let role_id = Ulid::from_bytes([16u8; 16]);
        let actor = Actor {
            node_id: node(17),
            user_id: UserId::local(Ulid::from_bytes([13u8; 16]), realm_id),
            realm_id,
        };
        let mut auth_doc = seeded_user_role(group_id, role_id);
        let role = auth_doc.roles.get_mut(&role_id).unwrap();
        role.assigned_users.insert(user_id);
        let mut operation = AddUserOperation::new(AddUserInput {
            actor,
            group_id,
            user_id,
            role_ids: HashSet::from([role_id]),
        });
        operation.step = Step::Write;
        let written = ChildOutcome::new(Ok::<_, AddUserError>((auth_doc, true, true)));
        match operation.next(IoResult::Child(written)) {
            Ok(Next::Then { output, calls }) => {
                assert!(output.roles[&role_id].assigned_users.contains(&user_id));
                calls
            }
            other => panic!("expected the document with calls after it, got {other:?}"),
        }
    }

    #[test]
    fn notice_after_drain() {
        // The notice runs after the commit like the drain wake, so its failure never fails the
        // addition; an actor adding themselves gets no notice.
        assert!(matches!(
            calls_after_write(14).as_slice(),
            [IoCall::Effect(_), IoCall::Child(_)]
        ));
        assert!(matches!(
            calls_after_write(13).as_slice(),
            [IoCall::Effect(_)]
        ));
    }

    #[tokio::test]
    async fn existing_member_silent() {
        let (context, net_handle, _tmp) = add_context().await;
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
        let after_first = read_notification_outbox(&context).await;
        assert_eq!(after_first.len(), 1);
        assert_eq!(after_first[0].recipient, member);

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
        assert_eq!(read_notification_outbox(&context).await.len(), 1);

        drive(
            AddUserOperation::new(AddUserInput {
                actor: actor.clone(),
                group_id: group.group_id,
                user_id: member,
                role_ids: named_role_ids(&auth_doc, "viewer"),
            }),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(read_notification_outbox(&context).await.len(), 1);

        net_handle.shutdown().await;
    }
}
