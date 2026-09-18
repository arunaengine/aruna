//! Claims the first admin role of a realm while its authorization document has no admin yet.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget, AdminRoleDefinition,
};
use aruna_core::document::DocumentOutboxEvent;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{AUTH_KEYSPACE, DOCUMENT_STATE_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, Role};
use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, RoleId, TxnId};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

use crate::sync::document_outbox::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use crate::sync::replicate_documents::replicate_documents_effect;

#[derive(Clone, Debug, PartialEq)]
pub struct ClaimInitialInput {
    pub actor: Actor,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ClaimInitialResult {
    Claimed(RealmAuthorizationDocument),
    AlreadyClaimed,
}

#[derive(PartialEq)]
pub struct ClaimInitialOperation {
    input: ClaimInitialInput,
    state: ClaimInitialState,
    output: Option<Result<ClaimInitialResult, ClaimInitialError>>,
}

impl std::fmt::Debug for ClaimInitialOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClaimInitialOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ClaimInitialState {
    Init,
    StartTransaction,
    ReadAdminState {
        txn_id: TxnId,
    },
    WriteAdminState {
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    },
    DeleteAdminConflicts {
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
    },
    CommitTransaction {
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
    },
    ScheduleDocumentDrain {
        auth_doc: RealmAuthorizationDocument,
    },
    AbortTransaction,
    AnnounceAuthDoc {
        auth_doc: RealmAuthorizationDocument,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ClaimInitialError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentError(#[from] AdminDocumentError),
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
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
        state: ClaimInitialState,
        expected: &'static str,
        got: String,
    },
}

impl ClaimInitialOperation {
    pub fn new(input: ClaimInitialInput) -> Self {
        Self {
            input,
            state: ClaimInitialState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ClaimInitialError) -> Effects {
        let cleanup_effects = self.abort();
        self.state = ClaimInitialState::Error;
        self.output = Some(Err(error));
        cleanup_effects
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(ClaimInitialError::UnexpectedEvent {
            state: self.state.clone(),
            expected,
            got,
        })
    }

    fn emit_auth_read(&mut self, txn_id: TxnId) -> Effects {
        self.state = ClaimInitialState::ReadAdminState { txn_id };
        let target = AdminDocumentTarget::Realm {
            realm_id: self.input.actor.realm_id,
        };
        let auth_key = ByteView::from(*self.input.actor.realm_id.as_bytes());
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (AUTH_KEYSPACE.to_string(), auth_key),
                (
                    DOCUMENT_STATE_KEYSPACE.to_string(),
                    reducer_state_key(&target),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn emit_auth_write(
        &mut self,
        txn_id: TxnId,
        auth_doc: Option<ByteView>,
        reducer_state_value: Option<ByteView>,
    ) -> Result<Effects, ClaimInitialError> {
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
            return Ok(self.abort_already_claimed(txn_id));
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
            return Ok(self.abort_already_claimed(txn_id));
        }
        if previous_reducer_state
            .as_ref()
            .is_some_and(|state| has_admin_conflict(state, role.role_id))
        {
            return Ok(self.abort_already_claimed(txn_id));
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

        self.state = ClaimInitialState::WriteAdminState {
            txn_id,
            auth_doc: auth_doc.clone(),
            admin_outbox_written: !admin_events.is_empty(),
            stale_conflict_deletes,
        };

        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn abort_already_claimed(&mut self, txn_id: TxnId) -> Effects {
        self.state = ClaimInitialState::AbortTransaction;
        self.output = Some(Ok(ClaimInitialResult::AlreadyClaimed));
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    fn emit_commit_transaction(
        &mut self,
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
    ) -> Effects {
        self.state = ClaimInitialState::CommitTransaction {
            txn_id,
            auth_doc,
            admin_outbox_written,
        };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn emit_auth_announce(&mut self, auth_doc: RealmAuthorizationDocument) -> Effects {
        self.state = ClaimInitialState::AnnounceAuthDoc {
            auth_doc: auth_doc.clone(),
        };
        let document = DocumentTarget::RealmAuthorization {
            realm_id: auth_doc.realm_id,
        };
        smallvec![replicate_documents_effect(
            self.input.actor.realm_id,
            self.input.actor.node_id,
            vec![document],
        )]
    }

    fn handle_schedule(&mut self, event: Event, auth_doc: RealmAuthorizationDocument) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. })
            | Event::Task(TaskEvent::Error { .. }) => {
                self.state = ClaimInitialState::Finish;
                self.output = Some(Ok(ClaimInitialResult::Claimed(auth_doc)));
                smallvec![]
            }
            other => self.unexpected_event(
                "admin document outbox drain timer schedule",
                format!("{other:?}"),
            ),
        }
    }
}

impl Operation for ClaimInitialOperation {
    type Output = ClaimInitialResult;
    type Error = ClaimInitialError;

    fn start(&mut self) -> Effects {
        self.state = ClaimInitialState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error })
                if matches!(self.state, ClaimInitialState::CommitTransaction { .. }) =>
            {
                self.state = ClaimInitialState::Error;
                self.output = Some(Err(error.into()));
                return smallvec![];
            }
            Event::Storage(StorageEvent::Error { error }) => return self.fail(error.into()),
            other => other,
        };

        match self.state.clone() {
            ClaimInitialState::StartTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self
                        .unexpected_event("Event::Storage(StorageEvent::TransactionStarted)", got);
                };
                self.emit_auth_read(txn_id)
            }
            ClaimInitialState::ReadAdminState { txn_id } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
                    return self
                        .unexpected_event("Event::Storage(StorageEvent::BatchReadResult)", got);
                };
                let [(_, auth_doc_value), (_, reducer_state_value)] = values.as_slice() else {
                    return self.unexpected_event(
                        "Event::Storage(StorageEvent::BatchReadResult) with auth doc and admin state values",
                        got,
                    );
                };

                match self.emit_auth_write(
                    txn_id,
                    auth_doc_value.clone(),
                    reducer_state_value.clone(),
                ) {
                    Ok(effects) => effects,
                    Err(error) => self.fail(error),
                }
            }
            ClaimInitialState::WriteAdminState {
                txn_id,
                auth_doc,
                admin_outbox_written,
                stale_conflict_deletes,
            } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
                    return self
                        .unexpected_event("Event::Storage(StorageEvent::BatchWriteResult)", got);
                };

                if !stale_conflict_deletes.is_empty() {
                    self.state = ClaimInitialState::DeleteAdminConflicts {
                        txn_id,
                        auth_doc,
                        admin_outbox_written,
                    };
                    return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                        deletes: stale_conflict_deletes,
                        txn_id: Some(txn_id),
                    })];
                }

                self.emit_commit_transaction(txn_id, auth_doc, admin_outbox_written)
            }
            ClaimInitialState::DeleteAdminConflicts {
                txn_id,
                auth_doc,
                admin_outbox_written,
            } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
                    return self
                        .unexpected_event("Event::Storage(StorageEvent::BatchDeleteResult)", got);
                };

                self.emit_commit_transaction(txn_id, auth_doc, admin_outbox_written)
            }
            ClaimInitialState::CommitTransaction {
                auth_doc,
                admin_outbox_written,
                ..
            } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.unexpected_event(
                        "Event::Storage(StorageEvent::TransactionCommitted)",
                        got,
                    );
                };

                if admin_outbox_written {
                    self.state = ClaimInitialState::ScheduleDocumentDrain { auth_doc };
                    return smallvec![schedule_drain_effect()];
                }

                self.emit_auth_announce(auth_doc)
            }
            ClaimInitialState::ScheduleDocumentDrain { auth_doc } => {
                self.handle_schedule(event, auth_doc)
            }
            ClaimInitialState::AbortTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionAborted { .. }) = event else {
                    return self
                        .unexpected_event("Event::Storage(StorageEvent::TransactionAborted)", got);
                };

                self.state = ClaimInitialState::Finish;
                smallvec![]
            }
            ClaimInitialState::AnnounceAuthDoc { auth_doc } => {
                let got = format!("{event:?}");
                let Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) = event
                else {
                    return self.unexpected_event(
                        "Event::SubOperation(SubOperationEvent::DocumentSyncResult)",
                        got,
                    );
                };

                if let Err(error) = result {
                    return self.fail(ClaimInitialError::TopicAnnouncement(error));
                }

                self.state = ClaimInitialState::Finish;
                self.output = Some(Ok(ClaimInitialResult::Claimed(auth_doc.clone())));
                smallvec![]
            }
            ClaimInitialState::Finish | ClaimInitialState::Error | ClaimInitialState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ClaimInitialState::Finish | ClaimInitialState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ClaimInitialError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            ClaimInitialState::ReadAdminState { txn_id }
            | ClaimInitialState::WriteAdminState { txn_id, .. }
            | ClaimInitialState::DeleteAdminConflicts { txn_id, .. }
            | ClaimInitialState::CommitTransaction { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
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
    use super::{
        ClaimInitialError, ClaimInitialInput, ClaimInitialOperation, ClaimInitialResult,
        ClaimInitialState,
    };
    use crate::driver::{DriverContext, drive};
    use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_core::UserId;
    use aruna_core::admin_documents::{
        AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget, AdminRoleDefinition,
    };
    use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keys::generate_signing_key;
    use aruna_core::keyspaces::{DOCUMENT_STATE_KEYSPACE, SYNC_OUTBOX_KEYSPACE};
    use aruna_core::operation::Operation;
    use aruna_core::reducer::AdminDocumentState;
    use aruna_core::structs::identity::auth::{Actor, Role};
    use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId};
    use aruna_core::task::{TaskEvent, TaskKey};
    use aruna_core::types::TxnId;
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
            task_handle: Some(task_handle),
            compute_handle: None,
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
        let txn_id = TxnId::generate();
        let effects = operation
            .emit_auth_write(
                txn_id,
                Some(auth_doc.to_bytes(&actor).unwrap().into()),
                None,
            )
            .unwrap();
        let outbox_records = match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: effect_txn_id,
            }) => {
                assert_eq!(*effect_txn_id, Some(txn_id));
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
            other => panic!("unexpected write effect: {other:?}"),
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
        let txn_id = TxnId::generate();
        let effects = operation
            .emit_auth_write(
                txn_id,
                Some(auth_doc.to_bytes(&actor).unwrap().into()),
                Some(postcard::to_allocvec(&previous_state).unwrap().into()),
            )
            .unwrap();
        assert_eq!(
            effects.first().unwrap(),
            &Effect::Storage(StorageEffect::AbortTransaction { txn_id })
        );
        assert_eq!(
            operation.output,
            Some(Ok(ClaimInitialResult::AlreadyClaimed))
        );
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
        let txn_id = TxnId::generate();
        let effects = operation
            .emit_auth_write(
                txn_id,
                Some(auth_doc.to_bytes(&claiming_actor).unwrap().into()),
                Some(postcard::to_allocvec(&previous_state).unwrap().into()),
            )
            .unwrap();

        assert_eq!(
            effects.as_slice(),
            &[Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        );
        assert_eq!(
            operation.output,
            Some(Ok(ClaimInitialResult::AlreadyClaimed))
        );
    }

    #[test]
    fn commit_conflict_preserved() {
        let realm_id = RealmId::from_bytes([31u8; 32]);
        let actor = actor(realm_id, 32, 33);
        let (auth_doc, _) = auth_and_role(realm_id);
        let txn_id = TxnId::generate();
        let mut operation = ClaimInitialOperation::new(ClaimInitialInput { actor });
        operation.state = ClaimInitialState::CommitTransaction {
            txn_id,
            auth_doc,
            admin_outbox_written: false,
        };

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }));

        assert!(effects.is_empty());
        assert_eq!(operation.state, ClaimInitialState::Error);
        assert_eq!(
            operation.output,
            Some(Err(ClaimInitialError::StorageError(
                StorageError::TransactionConflict
            )))
        );
    }

    #[test]
    fn schedule_error_finishes() {
        let realm_id = RealmId::from_bytes([15u8; 32]);
        let actor = actor(realm_id, 16, 17);
        let (auth_doc, _) = auth_and_role(realm_id);
        let mut operation = ClaimInitialOperation::new(ClaimInitialInput { actor });
        operation.state = ClaimInitialState::ScheduleDocumentDrain {
            auth_doc: auth_doc.clone(),
        };

        operation.step(Event::Task(TaskEvent::Error {
            key: Some(TaskKey::DrainSyncOutbox),
            message: "schedule failed".to_string(),
        }));
        assert_eq!(operation.state, ClaimInitialState::Finish);
        assert_eq!(
            operation.output,
            Some(Ok(ClaimInitialResult::Claimed(auth_doc)))
        );
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
}
