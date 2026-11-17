//! Assigns a set of roles to one user in the realm authorization document.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::keyspaces::{AUTH_KEYSPACE, DOCUMENT_STATE_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState};
use aruna_core::rows::done;
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId};
use aruna_core::types::{Key, KeySpace, RoleId};
use byteview::ByteView;
use std::collections::HashSet;
use thiserror::Error;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::epoch_due::DueMarks;
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};

#[derive(Clone, Debug, PartialEq)]
pub struct AssignRolesInput {
    pub actor: Actor,
    pub realm_id: RealmId,
    pub user_id: UserId,
    pub role_ids: HashSet<RoleId>,
}

#[derive(Debug, Error, PartialEq)]
pub enum AssignRolesError {
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
    #[error("Role not found")]
    RoleNotFound,
    #[error("Invalid user id")]
    InvalidUserId,
    #[error("Authorization document not found")]
    DocNotFound,
    #[error(transparent)]
    CheckPermissionsError(#[from] AuthorizationError),
    #[error("Adding user to realm  did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for AssignRolesError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "AssignRoles".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

/// The written document, and whether admin events reached the outbox.
type AssignedRoles = (RealmAuthorizationDocument, bool);

/// What the assignment waits for after the caller check.
#[derive(Debug, PartialEq)]
enum Step {
    Check,
    Write,
    Announce,
}

/// Checks the caller, assigns the roles in one write transaction, then wakes the outbox drain or,
/// when nothing reached the outbox, announces the document and waits for the announcement.
#[derive(Debug, PartialEq)]
pub struct AssignRolesOperation {
    input: AssignRolesInput,
    step: Step,
    document: Option<RealmAuthorizationDocument>,
}

impl AssignRolesOperation {
    pub fn new(input: AssignRolesInput) -> Self {
        AssignRolesOperation {
            input,
            step: Step::Check,
            document: None,
        }
    }

    fn written(
        &mut self,
        result: IoResult,
    ) -> Result<Next<RealmAuthorizationDocument>, AssignRolesError> {
        let (document, outbox) = result.child::<AssignedRoles, AssignRolesError>()??;
        if outbox {
            // A failed drain timer leaves the durable outbox retryable.
            let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
            return Ok(Next::Then {
                output: document,
                calls,
            });
        }
        let announce = super::announce_call(&self.input.actor, document.realm_id);
        let announce = announce.ok_or(AssignRolesError::NotFinished)?;
        self.document = Some(document);
        self.step = Step::Announce;
        Ok(Next::Call(announce))
    }

    fn announced(
        &mut self,
        result: IoResult,
    ) -> Result<Next<RealmAuthorizationDocument>, AssignRolesError> {
        let outcome =
            super::announced(result).map_err(|got| AssignRolesError::UnexpectedEvent {
                state: "Announce".to_string(),
                expected: "Event::SubOperation(SubOperationEvent::DocumentSyncResult)",
                got,
            })?;
        outcome.map_err(AssignRolesError::TopicAnnouncement)?;
        self.document
            .take()
            .map(Next::Done)
            .ok_or(AssignRolesError::NotFinished)
    }
}

impl Operation for AssignRolesOperation {
    type Output = RealmAuthorizationDocument;
    type Error = AssignRolesError;

    fn first(&mut self) -> Result<Next<RealmAuthorizationDocument>, AssignRolesError> {
        if self.input.user_id.is_nil() {
            return Err(AssignRolesError::InvalidUserId);
        }
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: AuthContext::from(&self.input.actor),
                path: format!(
                    "/{}/admin/roles/{}",
                    self.input.realm_id, self.input.user_id
                ),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<RealmAuthorizationDocument>, AssignRolesError> {
        match self.step {
            Step::Check => {
                if !result.child::<bool, AuthorizationError>()?? {
                    return Err(AssignRolesError::Unauthorized);
                }
                self.step = Step::Write;
                Ok(Next::Call(IoCall::child(RoleWrite::new(
                    self.input.clone(),
                ))))
            }
            Step::Write => self.written(result),
            Step::Announce => self.announced(result),
        }
    }
}

/// What the assignment transaction waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Read,
    Write,
    Delete,
    Marks,
}

/// Reads the realm authorization and its admin state, writes the assignment, and marks key epochs
/// due when it narrowed access, in one write transaction.
#[derive(Debug, PartialEq)]
struct RoleWrite {
    input: AssignRolesInput,
    stage: Stage,
    narrowed: bool,
    written: Option<AssignedRoles>,
    deletes: Vec<(KeySpace, Key)>,
    marks: DueMarks,
}

impl RoleWrite {
    fn new(input: AssignRolesInput) -> Self {
        Self {
            input,
            stage: Stage::Read,
            narrowed: false,
            written: None,
            deletes: Vec::new(),
            marks: DueMarks::new(None, aruna_core::time::unix_timestamp_millis()),
        }
    }

    fn emit_auth_write(
        &mut self,
        auth_doc: Option<ByteView>,
        reducer_state_value: Option<ByteView>,
    ) -> Result<Next<AssignedRoles>, AssignRolesError> {
        let mut auth_doc = RealmAuthorizationDocument::from_bytes(
            &auth_doc.ok_or_else(|| AssignRolesError::DocNotFound)?,
        )?;
        let role_ids = sorted_role_ids(&self.input.role_ids);
        for role_id in &role_ids {
            if !auth_doc.roles.contains_key(role_id) {
                return Err(AssignRolesError::RoleNotFound);
            }
        }

        let target = AdminDocumentTarget::Realm {
            realm_id: self.input.realm_id,
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

        let roles_before = auth_doc.roles.clone();
        let materialized_assignments = reducer_state.materialized_realm_assignments();
        for role_id in role_ids {
            let role = auth_doc
                .roles
                .get_mut(&role_id)
                .ok_or_else(|| AssignRolesError::RoleNotFound)?;
            if materialized_assignments
                .get(&role_id)
                .is_some_and(|users| users.contains(&self.input.user_id))
            {
                role.assigned_users.insert(self.input.user_id);
            } else {
                role.assigned_users.remove(&self.input.user_id);
            }
        }
        self.narrowed = aruna_core::admin_documents::roles_narrowed(&roles_before, &auth_doc.roles);

        let key = (*auth_doc.realm_id.as_bytes()).into();
        let value = auth_doc.to_bytes(&self.input.actor)?.into();
        let stale_conflict_deletes =
            stale_conflict_deletes(previous_reducer_state.as_ref(), Some(&reducer_state));
        let mut writes = vec![
            (AUTH_KEYSPACE.to_string(), key, value),
            reducer_state_entry(&reducer_state)?,
        ];
        let document_target = DocumentTarget::RealmAuthorization {
            realm_id: self.input.realm_id,
        };
        for event in &admin_events {
            let record = new_identified_record(
                event.event_id,
                self.input.actor.node_id,
                document_target.clone(),
                Vec::new(),
                DocumentOutboxEvent::admin(event.clone()),
                // No realm config in reach here; the stage-2 topic flip resolves
                // the real ref for this target.
                aruna_core::structs::placement::record::PlacementRef::NIL,
                false,
            );
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
        writes.extend(conflict_write_entries(&reducer_state)?);

        self.written = Some((auth_doc, !admin_events.is_empty()));
        self.deletes = stale_conflict_deletes;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn after_writes(&mut self) -> Result<Next<AssignedRoles>, AssignRolesError> {
        if self.narrowed {
            self.stage = Stage::Marks;
            return Ok(Next::Call(self.marks.first()));
        }
        self.finish()
    }

    fn finish(&mut self) -> Result<Next<AssignedRoles>, AssignRolesError> {
        self.written
            .take()
            .map(Next::Done)
            .ok_or(AssignRolesError::NotFinished)
    }
}

impl Operation for RoleWrite {
    type Output = AssignedRoles;
    type Error = AssignRolesError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<AssignedRoles>, AssignRolesError> {
        let target = AdminDocumentTarget::Realm {
            realm_id: self.input.realm_id,
        };
        let auth_key = (*self.input.realm_id.as_bytes()).into();
        Ok(Next::Call(IoCall::Read(vec![
            (AUTH_KEYSPACE.to_string(), auth_key),
            (
                DOCUMENT_STATE_KEYSPACE.to_string(),
                reducer_state_key(&target),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AssignedRoles>, AssignRolesError> {
        match self.stage {
            Stage::Read => {
                let [auth_doc, reducer] = aruna_core::rows::values::<2>(result)?;
                self.emit_auth_write(auth_doc, reducer)
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
    input: &AssignRolesInput,
    role_ids: &[RoleId],
) -> Result<Vec<AdminDocumentEvent>, AdminDocumentError> {
    let mut admin_events = Vec::new();
    for role_id in role_ids {
        if should_seed_role(state, *role_id) {
            let event = state.apply_operation(
                &input.actor,
                AdminDocumentOperation::RealmRoleAdded { role_id: *role_id },
            )?;
            admin_events.push(event);
        }
        let event = state.apply_operation(
            &input.actor,
            AdminDocumentOperation::RealmAssignmentAdded {
                role_id: *role_id,
                user_id: input.user_id,
            },
        )?;
        admin_events.push(event);
    }

    Ok(admin_events)
}

pub(crate) fn should_seed_role(state: &AdminDocumentState, role_id: RoleId) -> bool {
    !state.materialized_realm_roles().contains(&role_id)
        && !state
            .conflicts
            .contains_key(&format!("realm.roles.{role_id}"))
}

#[cfg(test)]
pub mod test {
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

    use crate::driver::{DriverContext, drive};
    use crate::realm::assign_role::{
        AssignRolesError, AssignRolesInput, AssignRolesOperation, RoleWrite,
    };
    use crate::realm::claim_admin::{ClaimInitialInput, ClaimInitialOperation};
    use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_core::UserId;
    use aruna_core::admin_documents::{
        AdminDocumentClock, AdminDocumentDot, AdminDocumentOperation, AdminDocumentTarget,
    };
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord, DocumentTarget};
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, DOCUMENT_CONFLICT_KEYSPACE, DOCUMENT_STATE_KEYSPACE, SYNC_OUTBOX_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::reducer::{
        AdminAttributeVersion, AdminConflict, AdminConflictValue, AdminDocumentState,
    };
    use aruna_core::storage_entries::{reducer_conflict_key, reducer_state_key};
    use aruna_core::structs::identity::auth::{Actor, Permission, Role};
    use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId};
    use aruna_core::types::RoleId;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

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
        format!("realm.roles.{role_id}.assigned_users.{user_id}")
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

    fn conflicted_reducer_state(
        realm_id: RealmId,
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
            target: AdminDocumentTarget::Realm { realm_id },
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
                format!("realm.roles.{role_id}"),
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

    #[test]
    fn rejects_nil_assignee() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let owner_id = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id);
        let role_id = Ulid::from_bytes([3u8; 16]);
        let actor = Actor {
            node_id: node(4),
            user_id: owner_id,
            realm_id,
        };
        let mut operation = AssignRolesOperation::new(AssignRolesInput {
            actor,
            realm_id,
            user_id: UserId::nil(realm_id),
            role_ids: HashSet::from([role_id]),
        });

        assert_eq!(operation.first(), Err(AssignRolesError::InvalidUserId));
    }

    #[test]
    fn seeds_missing_role() {
        let realm_id = RealmId::from_bytes([12u8; 32]);
        let owner_id = UserId::local(Ulid::from_bytes([13u8; 16]), realm_id);
        let assigned_user_id = UserId::local(Ulid::from_bytes([14u8; 16]), realm_id);
        let role_id = Ulid::from_bytes([15u8; 16]);
        let actor = Actor {
            node_id: node(16),
            user_id: owner_id,
            realm_id,
        };
        let auth_doc = RealmAuthorizationDocument {
            realm_id,
            roles: HashMap::from([(
                role_id,
                Role {
                    role_id,
                    name: "realm_user".to_string(),
                    permissions: HashMap::from([("/test".to_string(), Permission::READ)]),
                    assigned_users: HashSet::new(),
                },
            )]),
            operation_restrictions: HashMap::new(),
        };
        let input = AssignRolesInput {
            actor: actor.clone(),
            realm_id,
            user_id: assigned_user_id,
            role_ids: HashSet::from([role_id]),
        };
        let target = AdminDocumentTarget::Realm { realm_id };
        let mut operation = RoleWrite::new(input);

        let turn = operation.emit_auth_write(Some(auth_doc.to_bytes(&actor).unwrap().into()), None);
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
                record.target == (DocumentTarget::RealmAuthorization { realm_id })
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
            AdminDocumentOperation::RealmRoleAdded { role_id: event_role_id }
                if *event_role_id == role_id
        ));
        assert_eq!(events[1].target, target);
        assert_eq!(events[1].origin_seq, 2);
        assert_eq!(events[1].observed.sequence_for(&actor.node_id), 1);
        assert!(matches!(
            &events[1].op,
            AdminDocumentOperation::RealmAssignmentAdded {
                role_id: event_role_id,
                user_id: event_user_id,
            } if *event_role_id == role_id && *event_user_id == assigned_user_id
        ));
    }

    #[test]
    fn writes_assignment_atomically() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let owner_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let assigned_user_id = UserId::local(Ulid::from_bytes([4u8; 16]), realm_id);
        let conflict_user_id = UserId::local(Ulid::from_bytes([5u8; 16]), realm_id);
        let role_id = Ulid::from_bytes([6u8; 16]);
        let second_role_id = Ulid::from_bytes([7u8; 16]);
        let actor = Actor {
            node_id: node(8),
            user_id: owner_id,
            realm_id,
        };
        let auth_doc = RealmAuthorizationDocument {
            realm_id,
            roles: HashMap::from([
                (
                    role_id,
                    Role {
                        role_id,
                        name: "realm_user".to_string(),
                        permissions: HashMap::from([("/test".to_string(), Permission::READ)]),
                        assigned_users: HashSet::new(),
                    },
                ),
                (
                    second_role_id,
                    Role {
                        role_id: second_role_id,
                        name: "realm_reader".to_string(),
                        permissions: HashMap::from([("/other".to_string(), Permission::READ)]),
                        assigned_users: HashSet::new(),
                    },
                ),
            ]),
            operation_restrictions: HashMap::new(),
        };
        let previous_state =
            conflicted_reducer_state(realm_id, role_id, assigned_user_id, conflict_user_id);
        let input = AssignRolesInput {
            actor: actor.clone(),
            realm_id,
            user_id: assigned_user_id,
            role_ids: HashSet::from([role_id, second_role_id]),
        };
        let target = AdminDocumentTarget::Realm { realm_id };
        let mut operation = RoleWrite::new(input);

        assert!(
            matches!(operation.first(), Ok(Next::Call(IoCall::Read(reads))) if reads.len() == 2)
        );
        let turn = operation.next(IoResult::Values(vec![
            Some(auth_doc.to_bytes(&actor).unwrap().into()),
            Some(postcard::to_allocvec(&previous_state).unwrap().into()),
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
                    RealmAuthorizationDocument::from_bytes(auth_write.2.as_ref()).unwrap(),
                    postcard::from_bytes::<AdminDocumentState>(reducer_state_write.2.as_ref())
                        .unwrap(),
                    outbox_records,
                )
            }
            other => panic!("unexpected write call: {other:?}"),
        };
        assert_eq!(outbox_records.len(), 3);
        assert!(outbox_records.iter().any(|record| {
            record.target == (DocumentTarget::RealmAuthorization { realm_id })
                && matches!(
                    &record.event,
                    DocumentOutboxEvent::AdminOperation { event, .. }
                        if event.target == target
                            && matches!(
                                &event.op,
                                AdminDocumentOperation::RealmRoleAdded { role_id: event_role_id }
                                    if *event_role_id == second_role_id
                            )
                )
        }));
        for expected_role_id in [role_id, second_role_id] {
            assert!(outbox_records.iter().any(|record| {
                record.target == (DocumentTarget::RealmAuthorization { realm_id })
                    && matches!(
                        &record.event,
                        DocumentOutboxEvent::AdminOperation { event, .. }
                            if event.target == target
                                && matches!(
                                    &event.op,
                                    AdminDocumentOperation::RealmAssignmentAdded {
                                        role_id: event_role_id,
                                        user_id: event_user_id,
                                    } if *event_role_id == expected_role_id
                                        && *event_user_id == assigned_user_id
                                )
                    )
            }));
        }
        assert!(
            updated_auth_doc
                .roles
                .get(&role_id)
                .unwrap()
                .assigned_users
                .contains(&assigned_user_id)
        );
        assert!(
            updated_auth_doc
                .roles
                .get(&second_role_id)
                .unwrap()
                .assigned_users
                .contains(&assigned_user_id)
        );
        assert_eq!(
            reducer_state.materialized_realm_assignments(),
            BTreeMap::from([
                (role_id, BTreeSet::from([assigned_user_id])),
                (second_role_id, BTreeSet::from([assigned_user_id])),
            ])
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
        // The assignment narrows nothing, so it ends without epoch marks and with the outbox.
        let Ok(Next::Done((document, outbox))) = operation.next(IoResult::Done) else {
            panic!("expected the written document");
        };
        assert_eq!(document, updated_auth_doc);
        assert!(outbox);
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
            realm_description: "A realm description".to_string(),
            oidc_providers: Vec::new(),
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        };
        let realm_operation = CreateRealmOperation::new(realm_config.clone());
        let (_realm, realm_auth_doc) = drive(realm_operation, &context).await.unwrap();
        drive(
            ClaimInitialOperation::new(ClaimInitialInput {
                actor: realm_config.actor.clone(),
            }),
            &context,
        )
        .await
        .unwrap();

        let admin_role = realm_auth_doc
            .roles
            .iter()
            .filter_map(|(id, r)| {
                if r.name == "realm_admin" {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();

        let add_user_input = AssignRolesInput {
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            realm_id,
            user_id: UserId::local(Ulid::generate(), realm_id),
            role_ids: admin_role,
        };

        let add_user_operation = AssignRolesOperation::new(add_user_input.clone());
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
                .find(|(_, v)| v.name == "realm_admin")
                .unwrap()
                .1
                .assigned_users
                .contains(&add_user_input.user_id)
        );

        net_handle.shutdown().await;
    }
}
