//! Adds a role definition to the realm authorization document and replicates the change.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget, AdminRoleDefinition,
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
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission, Role};
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId};
use aruna_core::types::{Key, KeySpace};
use byteview::ByteView;
use thiserror::Error;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::epoch_due::DueMarks;
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};

#[derive(Clone, Debug, PartialEq)]
pub struct RealmRoleConfig {
    pub actor: Actor,
    pub realm_id: RealmId,
    pub role: Role,
}

#[derive(Debug, Error, PartialEq)]
pub enum RealmRoleError {
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
    #[error("No realm authorization document found")]
    RealmDocMissing,
    #[error("Invalid public role")]
    InvalidPublicRole,
    #[error("Invalid assigned user")]
    InvalidAssignedUser,
    #[error("Reserved role name")]
    ReservedRoleName,
    #[error(transparent)]
    CheckPermissionsError(#[from] AuthorizationError),
    #[error("Adding role to realm did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for RealmRoleError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "RealmRole".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

const REALM_ROLE_NAMES: &[&str] = &["realm_admin"];

fn reserved_role_name(name: &str) -> bool {
    REALM_ROLE_NAMES.contains(&name.trim())
}

impl RealmRoleConfig {
    fn validate(&self) -> Result<(), RealmRoleError> {
        if reserved_role_name(&self.role.name) {
            return Err(RealmRoleError::ReservedRoleName);
        }

        if self
            .role
            .assigned_users
            .iter()
            .any(|user| user.is_nil() && !user.is_nil_in(self.realm_id))
        {
            return Err(RealmRoleError::InvalidAssignedUser);
        }

        if self.role.is_public(self.realm_id)
            && self
                .role
                .permissions
                .values()
                .any(|permission| permission != &Permission::READ)
        {
            return Err(RealmRoleError::InvalidPublicRole);
        }

        Ok(())
    }
}

/// The written document, and whether admin events reached the outbox.
type WrittenRole = (RealmAuthorizationDocument, bool);

/// What the role creation waits for after the caller check.
#[derive(Debug, PartialEq)]
enum Step {
    Check,
    Write,
    Announce,
}

/// Checks the caller, writes the role in one write transaction, then wakes the outbox drain or,
/// when nothing reached the outbox, announces the document and waits for the announcement.
#[derive(Debug, PartialEq)]
pub struct RealmRoleOperation {
    input: RealmRoleConfig,
    step: Step,
    document: Option<RealmAuthorizationDocument>,
}

impl RealmRoleOperation {
    pub fn new(input: RealmRoleConfig) -> Self {
        RealmRoleOperation {
            input,
            step: Step::Check,
            document: None,
        }
    }

    fn written(
        &mut self,
        result: IoResult,
    ) -> Result<Next<RealmAuthorizationDocument>, RealmRoleError> {
        let (document, outbox) = result.child::<WrittenRole, RealmRoleError>()??;
        if outbox {
            // A failed drain timer leaves the durable outbox retryable.
            let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
            return Ok(Next::Then {
                output: document,
                calls,
            });
        }
        let announce = super::announce_call(
            &self.input.actor,
            DocumentTarget::RealmAuthorization {
                realm_id: document.realm_id,
            },
        );
        let announce = announce.ok_or(RealmRoleError::NotFinished)?;
        self.document = Some(document);
        self.step = Step::Announce;
        Ok(Next::Call(announce))
    }

    fn announced(
        &mut self,
        result: IoResult,
    ) -> Result<Next<RealmAuthorizationDocument>, RealmRoleError> {
        let outcome = super::announced(result).map_err(|got| RealmRoleError::UnexpectedEvent {
            state: "Announce".to_string(),
            expected: "Event::SubOperation(SubOperationEvent::DocumentSyncResult)",
            got,
        })?;
        outcome.map_err(RealmRoleError::TopicAnnouncement)?;
        self.document
            .take()
            .map(Next::Done)
            .ok_or(RealmRoleError::NotFinished)
    }
}

impl Operation for RealmRoleOperation {
    type Output = RealmAuthorizationDocument;
    type Error = RealmRoleError;

    fn first(&mut self) -> Result<Next<RealmAuthorizationDocument>, RealmRoleError> {
        self.input.validate()?;
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: AuthContext::from(&self.input.actor),
                path: format!(
                    "/{}/admin/roles/{}",
                    self.input.realm_id, self.input.role.role_id
                ),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<RealmAuthorizationDocument>, RealmRoleError> {
        match self.step {
            Step::Check => {
                if !result.child::<bool, AuthorizationError>()?? {
                    return Err(RealmRoleError::Unauthorized);
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

/// What the role transaction waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Read,
    Write,
    Delete,
    Marks,
}

/// Reads the realm authorization and its admin state, writes the role, and marks key epochs due
/// when it narrowed access, in one write transaction.
#[derive(Debug, PartialEq)]
struct RoleWrite {
    input: RealmRoleConfig,
    stage: Stage,
    narrowed: bool,
    written: Option<WrittenRole>,
    deletes: Vec<(KeySpace, Key)>,
    marks: DueMarks,
}

impl RoleWrite {
    fn new(input: RealmRoleConfig) -> Self {
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
    ) -> Result<Next<WrittenRole>, RealmRoleError> {
        let mut auth_doc = RealmAuthorizationDocument::from_bytes(
            &auth_doc.ok_or_else(|| RealmRoleError::RealmDocMissing)?,
        )?;
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
        let admin_events = apply_reducer_updates(&mut reducer_state, &self.input)?;
        let roles_before = auth_doc.roles.clone();
        materialize_realm_role(&mut auth_doc, &self.input.role, &reducer_state);
        self.narrowed = aruna_core::admin_documents::roles_narrowed(&roles_before, &auth_doc.roles);

        let conflict_delete_keys =
            stale_conflict_deletes(previous_reducer_state.as_ref(), Some(&reducer_state));

        let mut writes = vec![
            (
                AUTH_KEYSPACE.to_string(),
                (*auth_doc.realm_id.as_bytes()).into(),
                auth_doc.to_bytes(&self.input.actor)?.into(),
            ),
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
        self.deletes = conflict_delete_keys;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn after_writes(&mut self) -> Result<Next<WrittenRole>, RealmRoleError> {
        if self.narrowed {
            self.stage = Stage::Marks;
            return Ok(Next::Call(self.marks.first()));
        }
        self.finish()
    }

    fn finish(&mut self) -> Result<Next<WrittenRole>, RealmRoleError> {
        self.written
            .take()
            .map(Next::Done)
            .ok_or(RealmRoleError::NotFinished)
    }
}

impl Operation for RoleWrite {
    type Output = WrittenRole;
    type Error = RealmRoleError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<WrittenRole>, RealmRoleError> {
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

    fn next(&mut self, result: IoResult) -> Result<Next<WrittenRole>, RealmRoleError> {
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

fn apply_reducer_updates(
    state: &mut AdminDocumentState,
    input: &RealmRoleConfig,
) -> Result<Vec<AdminDocumentEvent>, AdminDocumentError> {
    let mut admin_events = Vec::new();
    let event = state.apply_operation(
        &input.actor,
        AdminDocumentOperation::RealmRoleCreated {
            role: AdminRoleDefinition::from(&input.role),
        },
    )?;
    admin_events.push(event);

    for user_id in crate::sorted_user_ids(&input.role.assigned_users) {
        let event = state.apply_operation(
            &input.actor,
            AdminDocumentOperation::RealmAssignmentAdded {
                role_id: input.role.role_id,
                user_id,
            },
        )?;
        admin_events.push(event);
    }

    Ok(admin_events)
}

fn materialize_realm_role(
    auth_doc: &mut RealmAuthorizationDocument,
    role: &Role,
    reducer_state: &AdminDocumentState,
) {
    if !reducer_state
        .materialized_realm_roles()
        .contains(&role.role_id)
    {
        auth_doc.roles.remove(&role.role_id);
        return;
    }

    auth_doc.roles.insert(role.role_id, role.clone());

    let materialized_assignments = reducer_state.materialized_realm_assignments();
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

#[cfg(test)]
pub mod test {
    use std::collections::{HashMap, HashSet};

    use crate::driver::{DriverContext, drive};
    use crate::realm::add_role::{
        RealmRoleConfig, RealmRoleError, RealmRoleOperation, RoleWrite, Step,
    };
    use crate::realm::claim_admin::{ClaimInitialInput, ClaimInitialOperation};
    use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_core::UserId;
    use aruna_core::admin_documents::{
        AdminDocumentDot, AdminDocumentOperation, AdminDocumentTarget, AdminRoleDefinition,
    };
    use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
    use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord, DocumentTarget};
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, DOCUMENT_CONFLICT_KEYSPACE, DOCUMENT_STATE_KEYSPACE, SYNC_OUTBOX_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::reducer::{AdminConflict, AdminConflictValue, AdminDocumentState};
    use aruna_core::storage_entries::{reducer_conflict_key, reducer_state_key};
    use aruna_core::structs::identity::auth::{Actor, Permission, Role};
    use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[test]
    fn rejects_reserved_names() {
        let realm_id = aruna_core::structs::identity::realm::RealmId([1u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            user_id,
            realm_id,
        };

        for name in ["realm_admin", " realm_admin "] {
            let mut operation = RealmRoleOperation::new(RealmRoleConfig {
                actor: actor.clone(),
                realm_id,
                role: Role {
                    role_id: Ulid::generate(),
                    name: name.to_string(),
                    permissions: HashMap::from([(
                        format!("/{realm_id}/data/**"),
                        Permission::READ,
                    )]),
                    assigned_users: HashSet::new(),
                },
            });

            assert_eq!(operation.first(), Err(RealmRoleError::ReservedRoleName));
        }
    }

    #[test]
    fn rejects_public_permissions() {
        let realm_id = aruna_core::structs::identity::realm::RealmId([1u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            user_id,
            realm_id,
        };

        for permission in [Permission::WRITE, Permission::DENY] {
            let mut operation = RealmRoleOperation::new(RealmRoleConfig {
                actor: actor.clone(),
                realm_id,
                role: Role {
                    role_id: Ulid::generate(),
                    name: "public".to_string(),
                    permissions: HashMap::from([(format!("/{realm_id}/data/**"), permission)]),
                    assigned_users: HashSet::from([UserId::nil(realm_id)]),
                },
            });

            assert_eq!(operation.first(), Err(RealmRoleError::InvalidPublicRole));
        }
    }

    #[test]
    fn rejects_foreign_nil() {
        let realm_id = aruna_core::structs::identity::realm::RealmId([1u8; 32]);
        let other_realm_id = aruna_core::structs::identity::realm::RealmId([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
            user_id,
            realm_id,
        };
        let mut operation = RealmRoleOperation::new(RealmRoleConfig {
            actor,
            realm_id,
            role: Role {
                role_id: Ulid::generate(),
                name: "foreign-nil".to_string(),
                permissions: HashMap::from([(format!("/{realm_id}/data/**"), Permission::READ)]),
                assigned_users: HashSet::from([UserId::nil(other_realm_id)]),
            },
        });

        assert_eq!(operation.first(), Err(RealmRoleError::InvalidAssignedUser));
    }

    #[test]
    fn deny_role_marks() {
        // An assigned DENY role narrows READ scopes, so epochs are marked due before commit.
        let realm_id = aruna_core::structs::identity::realm::RealmId([1u8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([2u8; 16]), realm_id),
            realm_id,
        };
        let mut operation = RealmRoleOperation::new(RealmRoleConfig {
            actor: actor.clone(),
            realm_id,
            role: Role {
                role_id: Ulid::from_bytes([4u8; 16]),
                name: "blocked".to_string(),
                permissions: HashMap::from([(format!("/{realm_id}/data/**"), Permission::DENY)]),
                assigned_users: HashSet::from([UserId::local(
                    Ulid::from_bytes([5u8; 16]),
                    realm_id,
                )]),
            },
        });
        let mut operation = RoleWrite::new(operation.input.clone());
        let auth_doc = RealmAuthorizationDocument::default_realm_doc(realm_id);
        operation.first().unwrap();
        let read = IoResult::Values(vec![Some(auth_doc.to_bytes(&actor).unwrap().into()), None]);
        assert!(matches!(
            operation.next(read),
            Ok(Next::Call(IoCall::Write(_)))
        ));
        let marking = operation.next(IoResult::Done);
        assert!(
            matches!(marking, Ok(Next::Call(IoCall::Page { .. }))),
            "got {marking:?}"
        );
    }

    #[test]
    pub fn writes_role_atomically() {
        let realm_id = aruna_core::structs::identity::realm::RealmId([0u8; 32]);
        let actor_user_id = UserId::local(Ulid::from_bytes([1u8; 16]), realm_id);
        let assigned_user_id = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id);
        let conflict_user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let actor = Actor {
            node_id,
            user_id: actor_user_id,
            realm_id,
        };
        let role_id = Ulid::from_bytes([4u8; 16]);
        let conflict_role_id = Ulid::from_bytes([5u8; 16]);
        let input = RealmRoleConfig {
            actor: actor.clone(),
            realm_id,
            role: Role {
                role_id,
                name: "test_role".to_string(),
                permissions: HashMap::from([(
                    format!("/{realm_id}/admin/roles/{role_id}"),
                    Permission::WRITE,
                )]),
                assigned_users: HashSet::from([assigned_user_id]),
            },
        };
        let target = AdminDocumentTarget::Realm { realm_id };
        let stale_conflict_path =
            format!("realm.roles.{role_id}.assigned_users.{assigned_user_id}");
        let retained_conflict_path =
            format!("realm.roles.{conflict_role_id}.assigned_users.{conflict_user_id}");
        let stale_add_dot = conflict_dot(2, 1);
        let stale_remove_dot = conflict_dot(3, 1);
        let retained_add_dot = conflict_dot(4, 1);
        let retained_remove_dot = conflict_dot(5, 1);
        let mut previous_state = AdminDocumentState::new(target.clone());
        for dot in [
            stale_add_dot,
            stale_remove_dot,
            retained_add_dot,
            retained_remove_dot,
        ] {
            previous_state
                .clock
                .advance(dot.origin_node_id, dot.origin_seq);
        }
        previous_state.conflicts.insert(
            stale_conflict_path.clone(),
            AdminConflict {
                path: stale_conflict_path.clone(),
                values: vec![
                    AdminConflictValue {
                        value: Some(assigned_user_id.to_string()),
                        dot: stale_add_dot,
                    },
                    AdminConflictValue {
                        value: None,
                        dot: stale_remove_dot,
                    },
                ],
            },
        );
        previous_state.conflicts.insert(
            retained_conflict_path.clone(),
            AdminConflict {
                path: retained_conflict_path.clone(),
                values: vec![
                    AdminConflictValue {
                        value: Some(conflict_user_id.to_string()),
                        dot: retained_add_dot,
                    },
                    AdminConflictValue {
                        value: None,
                        dot: retained_remove_dot,
                    },
                ],
            },
        );
        let auth_doc = RealmAuthorizationDocument::default_realm_doc(realm_id);
        let stale_conflict_key = reducer_conflict_key(&target, &stale_conflict_path);
        let mut expected_auth_doc = auth_doc.clone();
        expected_auth_doc.roles.insert(role_id, input.role.clone());

        let mut checked = RealmRoleOperation::new(input.clone());
        assert!(matches!(checked.first(), Ok(Next::Call(IoCall::Child(_)))));
        let allowed = IoResult::Child(ChildOutcome::new(Ok::<
            bool,
            aruna_core::errors::AuthorizationError,
        >(true)));
        assert!(matches!(
            checked.next(allowed),
            Ok(Next::Call(IoCall::Child(_)))
        ));

        let mut operation = RoleWrite::new(input.clone());
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Read(vec![
                (AUTH_KEYSPACE.to_string(), (*realm_id.as_bytes()).into()),
                (
                    DOCUMENT_STATE_KEYSPACE.to_string(),
                    reducer_state_key(&target),
                ),
            ])))
        );

        let turn = operation.next(IoResult::Values(vec![
            Some(auth_doc.to_bytes(&actor).unwrap().into()),
            Some(postcard::to_allocvec(&previous_state).unwrap().into()),
        ]));
        match turn {
            Ok(Next::Call(IoCall::Write(writes))) => {
                let auth_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == AUTH_KEYSPACE)
                    .expect("realm auth doc write is included");
                let reducer_state_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == DOCUMENT_STATE_KEYSPACE)
                    .expect("admin reducer state write is included");
                let outbox_records: Vec<DocumentOutboxRecord> = writes
                    .iter()
                    .filter(|(keyspace, _, _)| keyspace == SYNC_OUTBOX_KEYSPACE)
                    .map(|(_, _, value)| postcard::from_bytes(value.as_ref()).unwrap())
                    .collect();
                let _retained_conflict_write = writes
                    .iter()
                    .find(|(keyspace, key, _)| {
                        keyspace == DOCUMENT_CONFLICT_KEYSPACE
                            && key == &reducer_conflict_key(&target, &retained_conflict_path)
                    })
                    .expect("retained conflict write is included");

                let stored_auth_doc =
                    RealmAuthorizationDocument::from_bytes(auth_write.2.as_ref()).unwrap();
                let reducer_state: AdminDocumentState =
                    postcard::from_bytes(reducer_state_write.2.as_ref()).unwrap();

                assert_eq!(stored_auth_doc.roles.get(&role_id).unwrap(), &input.role);
                assert!(outbox_records.iter().any(|record| {
                    record.target == (DocumentTarget::RealmAuthorization { realm_id })
                        && matches!(
                            &record.event,
                            DocumentOutboxEvent::AdminOperation { event, .. }
                                if event.target == target
                                    && matches!(
                                        &event.op,
                                        AdminDocumentOperation::RealmRoleCreated { role }
                                            if role == &AdminRoleDefinition::from(&input.role)
                                    )
                        )
                }));
                assert!(
                    reducer_state
                        .materialized_realm_assignments()
                        .get(&role_id)
                        .is_some_and(|users| users.contains(&assigned_user_id))
                );
                assert!(!reducer_state.conflicts.contains_key(&stale_conflict_path));
                assert!(
                    reducer_state
                        .conflicts
                        .contains_key(&retained_conflict_path)
                );
                assert!(!writes.iter().any(|(keyspace, key, _)| {
                    keyspace == DOCUMENT_CONFLICT_KEYSPACE && key == &stale_conflict_key
                }));
            }
            other => panic!("unexpected realm role write call: {other:?}"),
        }

        let deletes = vec![(DOCUMENT_CONFLICT_KEYSPACE.to_string(), stale_conflict_key)];
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Call(IoCall::Delete(deletes)))
        );
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Done((expected_auth_doc, true)))
        );
    }

    #[test]
    pub fn outbox_drain_after() {
        // With admin events in the outbox the drain timer runs after the write; its failure is
        // only logged, so the document is the outcome either way.
        let realm_id = aruna_core::structs::identity::realm::RealmId([6u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([7u8; 16]), realm_id);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
            user_id,
            realm_id,
        };
        let role = Role {
            role_id: Ulid::from_bytes([9u8; 16]),
            name: "test_role".to_string(),
            permissions: HashMap::from([(format!("/{realm_id}/admin"), Permission::WRITE)]),
            assigned_users: HashSet::from([user_id]),
        };
        let auth_doc = RealmAuthorizationDocument {
            realm_id,
            roles: HashMap::from([(role.role_id, role.clone())]),
            operation_restrictions: HashMap::new(),
        };
        let mut operation = RealmRoleOperation::new(RealmRoleConfig {
            actor,
            realm_id,
            role,
        });
        operation.step = Step::Write;
        let written = ChildOutcome::new(Ok::<_, RealmRoleError>((auth_doc.clone(), true)));
        let Ok(Next::Then { output, calls }) = operation.next(IoResult::Child(written)) else {
            panic!("expected the document with the drain timer");
        };
        assert_eq!(output, auth_doc);
        assert!(matches!(calls.as_slice(), [IoCall::Effect(_)]));
    }

    fn conflict_dot(seed: u8, origin_seq: u64) -> AdminDocumentDot {
        AdminDocumentDot {
            event_id: Ulid::from_bytes([seed; 16]),
            origin_node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
            origin_seq,
        }
    }

    #[tokio::test]
    pub async fn test_add_role() {
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
        let (_realm, _realm_auth_doc) = drive(realm_operation, &context).await.unwrap();
        drive(
            ClaimInitialOperation::new(ClaimInitialInput {
                actor: realm_config.actor.clone(),
            }),
            &context,
        )
        .await
        .unwrap();

        let add_role_input = RealmRoleConfig {
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            realm_id,
            role: Role {
                role_id: Ulid::generate(),
                name: "test_role".to_string(),
                permissions: HashMap::from([(
                    format!("{}/admin/create_group/*", realm_id),
                    Permission::WRITE,
                )]),
                assigned_users: HashSet::from([user_id]),
            },
        };

        let add_role_operation = RealmRoleOperation::new(add_role_input.clone());
        let auth_doc = drive(add_role_operation, &context).await.unwrap();

        assert_eq!(
            auth_doc.roles.get(&add_role_input.role.role_id).unwrap(),
            &add_role_input.role
        );

        net_handle.shutdown().await;
    }
}
