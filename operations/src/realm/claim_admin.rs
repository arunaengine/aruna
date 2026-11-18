//! Claims the first admin role of a realm while its authorization document has no admin yet.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget, AdminRoleDefinition,
};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentOutboxEvent;
use aruna_core::document::DocumentTarget;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{AUTH_KEYSPACE, DOCUMENT_STATE_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState};
use aruna_core::rows::{done, values};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, Role};
use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
use aruna_core::types::{Key, KeySpace, RoleId};
use byteview::ByteView;
use thiserror::Error;

use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};

#[derive(Clone, Debug, PartialEq)]
pub struct ClaimInitialInput {
    pub actor: Actor,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ClaimInitialResult {
    Claimed(RealmAuthorizationDocument),
    AlreadyClaimed,
}

/// What the claim transaction waits for.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Stage {
    Read,
    Write,
    Delete,
}

/// Claims the realm admin role in one write transaction while no one holds it; a claimed role
/// answers without a write. The caller reruns it after a lost commit.
#[derive(Debug, PartialEq)]
pub struct ClaimInitialOperation {
    input: ClaimInitialInput,
    stage: Stage,
    claimed: Option<RealmAuthorizationDocument>,
    deletes: Vec<(KeySpace, Key)>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ClaimInitialError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentError(#[from] AdminDocumentError),
    #[error("authorization document not found")]
    DocNotFound,
    #[error("realm_admin role not found")]
    AdminRoleMissing,
    #[error("initial realm administrator has already been claimed")]
    AdministratorAlreadyClaimed,
    #[error("claiming initial realm admin did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for ClaimInitialError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "ClaimInitial".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

type ClaimTurn = Result<Next<ClaimInitialResult>, ClaimInitialError>;

impl ClaimInitialOperation {
    pub fn new(input: ClaimInitialInput) -> Self {
        Self {
            input,
            stage: Stage::Read,
            claimed: None,
            deletes: Vec::new(),
        }
    }

    fn emit_auth_write(
        &mut self,
        auth_doc: Option<ByteView>,
        reducer_state_value: Option<ByteView>,
    ) -> ClaimTurn {
        let mut auth_doc = RealmAuthorizationDocument::from_bytes(
            &auth_doc.ok_or(ClaimInitialError::DocNotFound)?,
        )?;
        let role = auth_doc
            .roles
            .values()
            .find(|role| role.name == "realm_admin")
            .cloned()
            .ok_or(ClaimInitialError::AdminRoleMissing)?;

        if !role.assigned_users.is_empty() {
            return Ok(Next::Done(ClaimInitialResult::AlreadyClaimed));
        }

        let target = AdminDocumentTarget::Realm {
            realm_id: self.input.actor.realm_id,
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
        if previous_reducer_state
            .as_ref()
            .is_some_and(|state| has_admin_assignment(state, role.role_id))
        {
            return Ok(Next::Done(ClaimInitialResult::AlreadyClaimed));
        }
        if previous_reducer_state
            .as_ref()
            .is_some_and(|state| has_admin_conflict(state, role.role_id))
        {
            return Ok(Next::Done(ClaimInitialResult::AlreadyClaimed));
        }

        let mut reducer_state = previous_reducer_state
            .clone()
            .unwrap_or_else(|| AdminDocumentState::new(target));
        let admin_events = apply_reducer_updates(&mut reducer_state, &self.input.actor, &role)?;
        materialize_admin_assignment(
            &mut auth_doc,
            &reducer_state,
            role.role_id,
            &self.input.actor,
        )?;

        let stale_conflict_deletes =
            stale_conflict_deletes(previous_reducer_state.as_ref(), Some(&reducer_state));
        let mut writes = vec![
            (
                AUTH_KEYSPACE.to_string(),
                ByteView::from(*auth_doc.realm_id.as_bytes()),
                auth_doc.to_bytes(&self.input.actor)?.into(),
            ),
            reducer_state_entry(&reducer_state)?,
        ];
        let document_target = DocumentTarget::RealmAuthorization {
            realm_id: self.input.actor.realm_id,
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

        self.claimed = Some(auth_doc);
        self.deletes = stale_conflict_deletes;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// The claim always queues admin events; a failed drain timer leaves the outbox retryable.
    fn finish(&mut self) -> ClaimTurn {
        let auth_doc = self.claimed.take().ok_or(ClaimInitialError::NotFinished)?;
        let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        Ok(Next::Then {
            output: ClaimInitialResult::Claimed(auth_doc),
            calls,
        })
    }
}

impl Operation for ClaimInitialOperation {
    type Output = ClaimInitialResult;
    type Error = ClaimInitialError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> ClaimTurn {
        let target = AdminDocumentTarget::Realm {
            realm_id: self.input.actor.realm_id,
        };
        let auth_key = ByteView::from(*self.input.actor.realm_id.as_bytes());
        Ok(Next::Call(IoCall::Read(vec![
            (AUTH_KEYSPACE.to_string(), auth_key),
            (
                DOCUMENT_STATE_KEYSPACE.to_string(),
                reducer_state_key(&target),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> ClaimTurn {
        match self.stage {
            Stage::Read => {
                let [auth_doc, reducer] = values::<2>(result)?;
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
                self.finish()
            }
        }
    }
}

fn has_admin_assignment(state: &AdminDocumentState, role_id: RoleId) -> bool {
    state
        .materialized_realm_assignments()
        .get(&role_id)
        .is_some_and(|users| !users.is_empty())
}

fn has_admin_conflict(state: &AdminDocumentState, role_id: RoleId) -> bool {
    state.conflicts.contains_key(&realm_role_path(role_id))
}

fn apply_reducer_updates(
    state: &mut AdminDocumentState,
    actor: &Actor,
    role: &Role,
) -> Result<Vec<AdminDocumentEvent>, AdminDocumentError> {
    let mut admin_events = Vec::new();
    if should_seed_admin(state, role.role_id) {
        let event = state.apply_operation(
            actor,
            AdminDocumentOperation::RealmRoleCreated {
                role: AdminRoleDefinition::from(role),
            },
        )?;
        admin_events.push(event);
    }
    let event = state.apply_operation(
        actor,
        AdminDocumentOperation::RealmAssignmentAdded {
            role_id: role.role_id,
            user_id: actor.user_id,
        },
    )?;
    admin_events.push(event);

    Ok(admin_events)
}

fn should_seed_admin(state: &AdminDocumentState, role_id: RoleId) -> bool {
    !state.materialized_realm_roles().contains(&role_id)
        && !state.conflicts.contains_key(&realm_role_path(role_id))
}

fn realm_role_path(role_id: RoleId) -> String {
    format!("realm.roles.{role_id}")
}

fn materialize_admin_assignment(
    auth_doc: &mut RealmAuthorizationDocument,
    reducer_state: &AdminDocumentState,
    role_id: RoleId,
    actor: &Actor,
) -> Result<(), ClaimInitialError> {
    let materialized_assignments = reducer_state.materialized_realm_assignments();
    let role = auth_doc
        .roles
        .get_mut(&role_id)
        .ok_or(ClaimInitialError::AdminRoleMissing)?;
    if materialized_assignments
        .get(&role_id)
        .is_some_and(|users| users.contains(&actor.user_id))
    {
        role.assigned_users.insert(actor.user_id);
    } else {
        role.assigned_users.remove(&actor.user_id);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ClaimInitialError, ClaimInitialInput, ClaimInitialOperation, ClaimInitialResult};
    use crate::driver::{DriverContext, drive};
    use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_core::UserId;
    use aruna_core::admin_documents::{
        AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget, AdminRoleDefinition,
    };
    use aruna_core::calls::{IoCall, IoError, IoResult, Next};
    use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord};
    use aruna_core::errors::StorageError;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::keyspaces::{DOCUMENT_STATE_KEYSPACE, SYNC_OUTBOX_KEYSPACE};
    use aruna_core::operation::{Operation, TransactionMode};
    use aruna_core::reducer::AdminDocumentState;
    use aruna_core::structs::identity::auth::{Actor, Role};
    use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId};
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use ed25519_dalek::SigningKey;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    async fn setup_context() -> (DriverContext, NetHandle, RealmId, iroh::PublicKey, TempDir) {
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

        let realm_signing_key: SigningKey = generate_signing_key();
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();

        let context = DriverContext {
            storage_handle,
            blob_handle: None,
            net_handle: Some(net_handle.clone()),
            metadata_handle: None,
            graph_handle: None,
            task_handle: Some(task_handle),
            compute_handle: None,
            outbound_handle: None,
        };

        let realm_config = CreateRealmConfig {
            actor: Actor {
                node_id,
                user_id: UserId::nil(realm_id),
                realm_id,
            },
            realm_description: "Realm".to_string(),
            oidc_providers: Vec::new(),
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        };
        drive(CreateRealmOperation::new(realm_config), &context)
            .await
            .unwrap();

        (context, net_handle, realm_id, node_id, random_path)
    }

    fn actor(realm_id: RealmId, node_seed: u8, user_seed: u8) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[node_seed; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([user_seed; 16]), realm_id),
            realm_id,
        }
    }

    fn auth_and_role(realm_id: RealmId) -> (RealmAuthorizationDocument, Role) {
        let auth_doc = RealmAuthorizationDocument::default_realm_doc(realm_id);
        let role = auth_doc
            .roles
            .values()
            .find(|role| role.name == "realm_admin")
            .unwrap()
            .clone();
        (auth_doc, role)
    }

    fn realm_admin_event(
        event_seed: u8,
        actor: Actor,
        op: AdminDocumentOperation,
    ) -> AdminDocumentEvent {
        AdminDocumentEvent {
            event_id: Ulid::from_bytes([event_seed; 16]),
            target: AdminDocumentTarget::Realm {
                realm_id: actor.realm_id,
            },
            origin_node_id: actor.node_id,
            origin_seq: 1,
            observed: Default::default(),
            actor,
            op,
        }
    }

    #[test]
    fn writes_admin_state() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let actor = actor(realm_id, 3, 4);
        let (auth_doc, role) = auth_and_role(realm_id);
        let mut operation = ClaimInitialOperation::new(ClaimInitialInput {
            actor: actor.clone(),
        });
        let turn = operation
            .emit_auth_write(Some(auth_doc.to_bytes(&actor).unwrap().into()), None)
            .unwrap();
        let outbox_records = match turn {
            Next::Call(IoCall::Write(writes)) => {
                assert!(
                    writes
                        .iter()
                        .any(|(keyspace, _, _)| keyspace == DOCUMENT_STATE_KEYSPACE)
                );
                writes
                    .iter()
                    .filter(|(keyspace, _, _)| keyspace == SYNC_OUTBOX_KEYSPACE)
                    .map(|(_, _, value)| postcard::from_bytes(value.as_ref()).unwrap())
                    .collect::<Vec<DocumentOutboxRecord>>()
            }
            _ => panic!("expected the admin state writes"),
        };
        assert!(outbox_records.iter().any(|record| matches!(
            &record.event,
            DocumentOutboxEvent::AdminOperation { event, .. }
                if matches!(&event.op, AdminDocumentOperation::RealmRoleCreated { role: event_role }
                    if event_role == &AdminRoleDefinition::from(&role))
        )));
        assert!(outbox_records.iter().any(|record| matches!(
            &record.event,
            DocumentOutboxEvent::AdminOperation { event, .. }
                if matches!(&event.op, AdminDocumentOperation::RealmAssignmentAdded { role_id, user_id }
                    if *role_id == role.role_id && *user_id == actor.user_id)
        )));
    }

    #[test]
    fn claimed_state_aborts() {
        let realm_id = RealmId::from_bytes([11u8; 32]);
        let actor = actor(realm_id, 12, 13);
        let (auth_doc, role) = auth_and_role(realm_id);
        let mut previous_state = AdminDocumentState::new(AdminDocumentTarget::Realm { realm_id });
        super::apply_reducer_updates(&mut previous_state, &actor, &role).unwrap();
        let mut operation = ClaimInitialOperation::new(ClaimInitialInput {
            actor: actor.clone(),
        });
        let turn = operation.emit_auth_write(
            Some(auth_doc.to_bytes(&actor).unwrap().into()),
            Some(postcard::to_allocvec(&previous_state).unwrap().into()),
        );
        assert_eq!(turn, Ok(Next::Done(ClaimInitialResult::AlreadyClaimed)));
    }

    #[test]
    fn role_conflict_aborts() {
        let realm_id = RealmId::from_bytes([21u8; 32]);
        let claiming_actor = actor(realm_id, 22, 23);
        let (auth_doc, role) = auth_and_role(realm_id);
        let mut previous_state = AdminDocumentState::new(AdminDocumentTarget::Realm { realm_id });
        let mut first_role = AdminRoleDefinition::from(&role);
        first_role.name = "first conflicted realm admin".to_string();
        let mut second_role = AdminRoleDefinition::from(&role);
        second_role.name = "second conflicted realm admin".to_string();
        previous_state
            .apply(&realm_admin_event(
                24,
                actor(realm_id, 25, 26),
                AdminDocumentOperation::RealmRoleCreated { role: first_role },
            ))
            .unwrap();
        previous_state
            .apply(&realm_admin_event(
                27,
                actor(realm_id, 28, 29),
                AdminDocumentOperation::RealmRoleCreated { role: second_role },
            ))
            .unwrap();

        let mut operation = ClaimInitialOperation::new(ClaimInitialInput {
            actor: claiming_actor.clone(),
        });
        let turn = operation.emit_auth_write(
            Some(auth_doc.to_bytes(&claiming_actor).unwrap().into()),
            Some(postcard::to_allocvec(&previous_state).unwrap().into()),
        );

        assert_eq!(turn, Ok(Next::Done(ClaimInitialResult::AlreadyClaimed)));
    }

    #[test]
    fn commit_conflict_preserved() {
        // A lost commit reaches the caller as a conflict, which reruns the claim.
        let conflict = ClaimInitialError::from(IoError::Conflict);
        assert_eq!(
            conflict,
            ClaimInitialError::StorageError(StorageError::TransactionConflict)
        );
        assert!(ClaimInitialOperation::is_conflict(&conflict));
    }

    #[test]
    fn claim_wakes_drain() {
        // The claimed document comes out with the drain wake after the commit.
        let realm_id = RealmId::from_bytes([15u8; 32]);
        let actor = actor(realm_id, 16, 17);
        let (auth_doc, role) = auth_and_role(realm_id);
        let mut operation = ClaimInitialOperation::new(ClaimInitialInput {
            actor: actor.clone(),
        });
        assert_eq!(operation.transaction(), TransactionMode::Write);
        operation.first().unwrap();
        let read = IoResult::Values(vec![Some(auth_doc.to_bytes(&actor).unwrap().into()), None]);
        assert!(matches!(
            operation.next(read),
            Ok(Next::Call(IoCall::Write(_)))
        ));
        let Ok(Next::Then { output, calls }) = operation.next(IoResult::Done) else {
            panic!("expected the claimed document with the drain wake");
        };
        let ClaimInitialResult::Claimed(claimed) = output else {
            panic!("expected a claim");
        };
        assert!(
            claimed.roles[&role.role_id]
                .assigned_users
                .contains(&actor.user_id)
        );
        assert!(matches!(calls.as_slice(), [IoCall::Effect(_)]));
    }

    #[tokio::test]
    async fn claims_admin_once() {
        let (context, net_handle, realm_id, node_id, _temp_dir) = setup_context().await;
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let result = drive(
            ClaimInitialOperation::new(ClaimInitialInput {
                actor: Actor {
                    node_id,
                    user_id,
                    realm_id,
                },
            }),
            &context,
        )
        .await
        .unwrap();

        match result {
            ClaimInitialResult::Claimed(auth_doc) => {
                assert!(auth_doc.roles.values().any(|role| {
                    role.name == "realm_admin" && role.assigned_users.contains(&user_id)
                }));
            }
            other => panic!("unexpected claim result: {other:?}"),
        }

        let second = drive(
            ClaimInitialOperation::new(ClaimInitialInput {
                actor: Actor {
                    node_id,
                    user_id: UserId::local(Ulid::generate(), realm_id),
                    realm_id,
                },
            }),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(second, ClaimInitialResult::AlreadyClaimed);

        net_handle.shutdown().await;
    }

    // The committed claim must hold after a restart, even with no saved claim flag.
    #[tokio::test]
    async fn claim_survives_reopen() {
        let (context, net_handle, realm_id, node_id, temp_dir) = setup_context().await;
        let admin_id = UserId::local(Ulid::generate(), realm_id);
        let claimed = drive(
            ClaimInitialOperation::new(ClaimInitialInput {
                actor: Actor {
                    node_id,
                    user_id: admin_id,
                    realm_id,
                },
            }),
            &context,
        )
        .await
        .unwrap();
        assert!(matches!(claimed, ClaimInitialResult::Claimed(_)));

        net_handle.shutdown().await;
        drop(net_handle);
        let storage_handle = context.storage_handle.clone();
        drop(context);
        storage_handle.close().await;

        let reopened = DriverContext {
            storage_handle: storage::FjallStorage::open(temp_dir.path().to_str().unwrap()).unwrap(),
            blob_handle: None,
            net_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
            outbound_handle: None,
        };
        for user_id in [admin_id, UserId::local(Ulid::generate(), realm_id)] {
            let result = drive(
                ClaimInitialOperation::new(ClaimInitialInput {
                    actor: Actor {
                        node_id,
                        user_id,
                        realm_id,
                    },
                }),
                &reopened,
            )
            .await
            .unwrap();
            assert_eq!(result, ClaimInitialResult::AlreadyClaimed);
        }
    }
}
