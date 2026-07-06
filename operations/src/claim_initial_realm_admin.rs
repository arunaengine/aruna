use aruna_core::admin_document_reducer::{AdminDocumentReducerError, AdminDocumentReducerState};
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentRoleDefinition, AdminDocumentTarget,
};
use aruna_core::document::DocumentSyncOutboxEvent;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{ADMIN_DOCUMENT_STATE_KEYSPACE, AUTH_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, stale_admin_document_conflict_delete_entries,
};
use aruna_core::structs::{Actor, RealmAuthorizationDocument, Role};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, RoleId, TxnId};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::replicate_documents::replicate_documents_effect;

#[derive(Clone, Debug, PartialEq)]
pub struct ClaimInitialRealmAdminInput {
    pub actor: Actor,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ClaimInitialRealmAdminResult {
    Claimed(RealmAuthorizationDocument),
    AlreadyClaimed,
}

#[derive(PartialEq)]
pub struct ClaimInitialRealmAdminOperation {
    input: ClaimInitialRealmAdminInput,
    state: ClaimInitialRealmAdminState,
    output: Option<Result<ClaimInitialRealmAdminResult, ClaimInitialRealmAdminError>>,
}

impl std::fmt::Debug for ClaimInitialRealmAdminOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClaimInitialRealmAdminOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ClaimInitialRealmAdminState {
    Init,
    StartTransaction,
    ReadAuthDocAndAdminState {
        txn_id: TxnId,
    },
    WriteAuthDocAndAdminState {
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    },
    DeleteStaleAdminConflicts {
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
    },
    CommitTransaction {
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
    },
    ScheduleAdminDocumentOutboxDrain {
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
pub enum ClaimInitialRealmAdminError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentReducerError(#[from] AdminDocumentReducerError),
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
    #[error("authorization document not found")]
    AuthDocNotFound,
    #[error("realm_admin role not found")]
    RealmAdminRoleNotFound,
    #[error("claiming initial realm admin did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: ClaimInitialRealmAdminState,
        expected: &'static str,
        got: String,
    },
}

impl ClaimInitialRealmAdminOperation {
    pub fn new(input: ClaimInitialRealmAdminInput) -> Self {
        Self {
            input,
            state: ClaimInitialRealmAdminState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ClaimInitialRealmAdminError) -> Effects {
        let cleanup_effects = self.abort();
        self.state = ClaimInitialRealmAdminState::Error;
        self.output = Some(Err(error));
        cleanup_effects
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(ClaimInitialRealmAdminError::UnexpectedEvent {
            state: self.state.clone(),
            expected,
            got,
        })
    }

    fn emit_read_auth_doc_and_admin_state(&mut self, txn_id: TxnId) -> Effects {
        self.state = ClaimInitialRealmAdminState::ReadAuthDocAndAdminState { txn_id };
        let target = AdminDocumentTarget::Realm {
            realm_id: self.input.actor.realm_id,
        };
        let auth_key = ByteView::from(*self.input.actor.realm_id.as_bytes());
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (AUTH_KEYSPACE.to_string(), auth_key),
                (
                    ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                    admin_document_reducer_state_key(&target),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn emit_write_auth_doc_and_admin_state(
        &mut self,
        txn_id: TxnId,
        auth_doc: Option<ByteView>,
        reducer_state_value: Option<ByteView>,
    ) -> Result<Effects, ClaimInitialRealmAdminError> {
        let mut auth_doc = RealmAuthorizationDocument::from_bytes(
            &auth_doc.ok_or(ClaimInitialRealmAdminError::AuthDocNotFound)?,
        )?;
        let role = auth_doc
            .roles
            .values()
            .find(|role| role.name == "realm_admin")
            .cloned()
            .ok_or(ClaimInitialRealmAdminError::RealmAdminRoleNotFound)?;

        if !role.assigned_users.is_empty() {
            return Ok(self.abort_already_claimed(txn_id));
        }

        let target = AdminDocumentTarget::Realm {
            realm_id: self.input.actor.realm_id,
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
        if previous_reducer_state
            .as_ref()
            .is_some_and(|state| reducer_has_realm_admin_assignment(state, role.role_id))
        {
            return Ok(self.abort_already_claimed(txn_id));
        }
        if previous_reducer_state
            .as_ref()
            .is_some_and(|state| reducer_has_realm_admin_role_conflict(state, role.role_id))
        {
            return Ok(self.abort_already_claimed(txn_id));
        }

        let mut reducer_state = previous_reducer_state
            .clone()
            .unwrap_or_else(|| AdminDocumentReducerState::new(target));
        let admin_events =
            apply_admin_reducer_updates(&mut reducer_state, &self.input.actor, &role)?;
        materialize_realm_admin_assignment(
            &mut auth_doc,
            &reducer_state,
            role.role_id,
            &self.input.actor,
        )?;

        let stale_conflict_deletes = stale_admin_document_conflict_delete_entries(
            previous_reducer_state.as_ref(),
            Some(&reducer_state),
        );
        let mut writes = vec![
            (
                AUTH_KEYSPACE.to_string(),
                ByteView::from(*auth_doc.realm_id.as_bytes()),
                auth_doc.to_bytes(&self.input.actor)?.into(),
            ),
            admin_document_reducer_state_write_entry(&reducer_state)?,
        ];
        let document_target = DocumentSyncTarget::RealmAuthorization {
            realm_id: self.input.actor.realm_id,
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
                // No realm config in reach here; the stage-2 topic flip resolves
                // the real ref for this target.
                aruna_core::structs::PlacementRef::NIL,
                false,
            );
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
        writes.extend(admin_document_conflict_write_entries(&reducer_state)?);

        self.state = ClaimInitialRealmAdminState::WriteAuthDocAndAdminState {
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
        self.state = ClaimInitialRealmAdminState::AbortTransaction;
        self.output = Some(Ok(ClaimInitialRealmAdminResult::AlreadyClaimed));
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    fn emit_commit_transaction(
        &mut self,
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
    ) -> Effects {
        self.state = ClaimInitialRealmAdminState::CommitTransaction {
            txn_id,
            auth_doc,
            admin_outbox_written,
        };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn emit_announce_auth_doc(&mut self, auth_doc: RealmAuthorizationDocument) -> Effects {
        self.state = ClaimInitialRealmAdminState::AnnounceAuthDoc {
            auth_doc: auth_doc.clone(),
        };
        let document = DocumentSyncTarget::RealmAuthorization {
            realm_id: auth_doc.realm_id,
        };
        smallvec![replicate_documents_effect(
            self.input.actor.realm_id,
            self.input.actor.node_id,
            vec![document],
        )]
    }
}

impl Operation for ClaimInitialRealmAdminOperation {
    type Output = ClaimInitialRealmAdminResult;
    type Error = ClaimInitialRealmAdminError;

    fn start(&mut self) -> Effects {
        self.state = ClaimInitialRealmAdminState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error })
                if matches!(
                    self.state,
                    ClaimInitialRealmAdminState::CommitTransaction { .. }
                ) =>
            {
                self.state = ClaimInitialRealmAdminState::Error;
                self.output = Some(Err(error.into()));
                return smallvec![];
            }
            Event::Storage(StorageEvent::Error { error }) => return self.fail(error.into()),
            other => other,
        };

        match self.state.clone() {
            ClaimInitialRealmAdminState::StartTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self
                        .unexpected_event("Event::Storage(StorageEvent::TransactionStarted)", got);
                };
                self.emit_read_auth_doc_and_admin_state(txn_id)
            }
            ClaimInitialRealmAdminState::ReadAuthDocAndAdminState { txn_id } => {
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

                match self.emit_write_auth_doc_and_admin_state(
                    txn_id,
                    auth_doc_value.clone(),
                    reducer_state_value.clone(),
                ) {
                    Ok(effects) => effects,
                    Err(error) => self.fail(error),
                }
            }
            ClaimInitialRealmAdminState::WriteAuthDocAndAdminState {
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
                    self.state = ClaimInitialRealmAdminState::DeleteStaleAdminConflicts {
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
            ClaimInitialRealmAdminState::DeleteStaleAdminConflicts {
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
            ClaimInitialRealmAdminState::CommitTransaction {
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
                    self.state =
                        ClaimInitialRealmAdminState::ScheduleAdminDocumentOutboxDrain { auth_doc };
                    return smallvec![schedule_outbox_drain_effect()];
                }

                self.emit_announce_auth_doc(auth_doc)
            }
            ClaimInitialRealmAdminState::ScheduleAdminDocumentOutboxDrain { auth_doc } => {
                match event {
                    Event::Task(TaskEvent::TimerScheduled { .. })
                    | Event::Task(TaskEvent::Error { .. }) => {
                        self.state = ClaimInitialRealmAdminState::Finish;
                        self.output = Some(Ok(ClaimInitialRealmAdminResult::Claimed(auth_doc)));
                        smallvec![]
                    }
                    other => self.unexpected_event(
                        "admin document outbox drain timer schedule",
                        format!("{other:?}"),
                    ),
                }
            }
            ClaimInitialRealmAdminState::AbortTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionAborted { .. }) = event else {
                    return self
                        .unexpected_event("Event::Storage(StorageEvent::TransactionAborted)", got);
                };

                self.state = ClaimInitialRealmAdminState::Finish;
                smallvec![]
            }
            ClaimInitialRealmAdminState::AnnounceAuthDoc { auth_doc } => {
                let got = format!("{event:?}");
                let Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) = event
                else {
                    return self.unexpected_event(
                        "Event::SubOperation(SubOperationEvent::DocumentSyncResult)",
                        got,
                    );
                };

                if let Err(error) = result {
                    return self.fail(ClaimInitialRealmAdminError::TopicAnnouncement(error));
                }

                self.state = ClaimInitialRealmAdminState::Finish;
                self.output = Some(Ok(ClaimInitialRealmAdminResult::Claimed(auth_doc.clone())));
                smallvec![]
            }
            ClaimInitialRealmAdminState::Finish
            | ClaimInitialRealmAdminState::Error
            | ClaimInitialRealmAdminState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ClaimInitialRealmAdminState::Finish | ClaimInitialRealmAdminState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or(ClaimInitialRealmAdminError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            ClaimInitialRealmAdminState::ReadAuthDocAndAdminState { txn_id }
            | ClaimInitialRealmAdminState::WriteAuthDocAndAdminState { txn_id, .. }
            | ClaimInitialRealmAdminState::DeleteStaleAdminConflicts { txn_id, .. }
            | ClaimInitialRealmAdminState::CommitTransaction { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

fn reducer_has_realm_admin_assignment(state: &AdminDocumentReducerState, role_id: RoleId) -> bool {
    state
        .materialized_realm_role_user_assignments()
        .get(&role_id)
        .is_some_and(|users| !users.is_empty())
}

fn reducer_has_realm_admin_role_conflict(
    state: &AdminDocumentReducerState,
    role_id: RoleId,
) -> bool {
    state.conflicts.contains_key(&realm_role_path(role_id))
}

fn apply_admin_reducer_updates(
    state: &mut AdminDocumentReducerState,
    actor: &Actor,
    role: &Role,
) -> Result<Vec<AdminDocumentEvent>, AdminDocumentReducerError> {
    let mut admin_events = Vec::new();
    if should_seed_realm_admin_role(state, role.role_id) {
        let event = apply_admin_reducer_operation(
            state,
            actor,
            AdminDocumentOperation::RealmRoleCreated {
                role: AdminDocumentRoleDefinition::from(role),
            },
        )?;
        admin_events.push(event);
    }
    let event = apply_admin_reducer_operation(
        state,
        actor,
        AdminDocumentOperation::RealmRoleUserAssignmentAdded {
            role_id: role.role_id,
            user_id: actor.user_id,
        },
    )?;
    admin_events.push(event);

    Ok(admin_events)
}

fn should_seed_realm_admin_role(state: &AdminDocumentReducerState, role_id: RoleId) -> bool {
    !state.materialized_realm_roles().contains(&role_id)
        && !state.conflicts.contains_key(&realm_role_path(role_id))
}

fn realm_role_path(role_id: RoleId) -> String {
    format!("realm.roles.{role_id}")
}

fn apply_admin_reducer_operation(
    state: &mut AdminDocumentReducerState,
    actor: &Actor,
    op: AdminDocumentOperation,
) -> Result<AdminDocumentEvent, AdminDocumentReducerError> {
    let observed = state.clock.clone();
    let event = AdminDocumentEvent {
        event_id: Ulid::new(),
        target: state.target.clone(),
        origin_node_id: actor.node_id,
        origin_seq: observed.sequence_for(&actor.node_id) + 1,
        observed,
        actor: actor.clone(),
        op,
    };
    state.apply(&event)?;
    Ok(event)
}

fn materialize_realm_admin_assignment(
    auth_doc: &mut RealmAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    role_id: RoleId,
    actor: &Actor,
) -> Result<(), ClaimInitialRealmAdminError> {
    let materialized_assignments = reducer_state.materialized_realm_role_user_assignments();
    let role = auth_doc
        .roles
        .get_mut(&role_id)
        .ok_or(ClaimInitialRealmAdminError::RealmAdminRoleNotFound)?;
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
        ClaimInitialRealmAdminError, ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
        ClaimInitialRealmAdminResult, ClaimInitialRealmAdminState,
    };
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::admin_document_reducer::AdminDocumentReducerState;
    use aruna_core::admin_documents::{
        AdminDocumentEvent, AdminDocumentOperation, AdminDocumentRoleDefinition,
        AdminDocumentTarget,
    };
    use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncOutboxRecord};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{ADMIN_DOCUMENT_STATE_KEYSPACE, DOCUMENT_SYNC_OUTBOX_KEYSPACE};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{Actor, RealmAuthorizationDocument, RealmId, Role};
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

        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let realm_signing_key: SigningKey = SigningKey::generate(&mut csprng);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();

        let context = DriverContext {
            storage_handle,
            blob_handle: None,
            net_handle: Some(net_handle.clone()),
            metadata_handle: None,
            task_handle: Some(task_handle),
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

    fn auth_doc_and_admin_role(realm_id: RealmId) -> (RealmAuthorizationDocument, Role) {
        let auth_doc = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
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
    fn writes_reducer_state_and_admin_outbox() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let actor = actor(realm_id, 3, 4);
        let (auth_doc, role) = auth_doc_and_admin_role(realm_id);
        let mut operation = ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
            actor: actor.clone(),
        });
        let txn_id = TxnId::new();
        let effects = operation
            .emit_write_auth_doc_and_admin_state(
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
                        .any(|(keyspace, _, _)| keyspace == ADMIN_DOCUMENT_STATE_KEYSPACE)
                );
                writes
                    .iter()
                    .filter(|(keyspace, _, _)| keyspace == DOCUMENT_SYNC_OUTBOX_KEYSPACE)
                    .map(|(_, _, value)| postcard::from_bytes(value.as_ref()).unwrap())
                    .collect::<Vec<DocumentSyncOutboxRecord>>()
            }
            other => panic!("unexpected write effect: {other:?}"),
        };
        assert!(outbox_records.iter().any(|record| matches!(
            &record.event,
            DocumentSyncOutboxEvent::AdminOperation { event }
                if matches!(&event.op, AdminDocumentOperation::RealmRoleCreated { role: event_role }
                    if event_role == &AdminDocumentRoleDefinition::from(&role))
        )));
        assert!(outbox_records.iter().any(|record| matches!(
            &record.event,
            DocumentSyncOutboxEvent::AdminOperation { event }
                if matches!(&event.op, AdminDocumentOperation::RealmRoleUserAssignmentAdded { role_id, user_id }
                    if *role_id == role.role_id && *user_id == actor.user_id)
        )));
    }

    #[test]
    fn reducer_state_already_claimed_aborts() {
        let realm_id = RealmId::from_bytes([11u8; 32]);
        let actor = actor(realm_id, 12, 13);
        let (auth_doc, role) = auth_doc_and_admin_role(realm_id);
        let mut previous_state =
            AdminDocumentReducerState::new(AdminDocumentTarget::Realm { realm_id });
        super::apply_admin_reducer_updates(&mut previous_state, &actor, &role).unwrap();
        let mut operation = ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
            actor: actor.clone(),
        });
        let txn_id = TxnId::new();
        let effects = operation
            .emit_write_auth_doc_and_admin_state(
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
            Some(Ok(ClaimInitialRealmAdminResult::AlreadyClaimed))
        );
    }

    #[test]
    fn reducer_role_body_conflict_aborts_without_admin_outbox() {
        let realm_id = RealmId::from_bytes([21u8; 32]);
        let claiming_actor = actor(realm_id, 22, 23);
        let (auth_doc, role) = auth_doc_and_admin_role(realm_id);
        let mut previous_state =
            AdminDocumentReducerState::new(AdminDocumentTarget::Realm { realm_id });
        let mut first_role = AdminDocumentRoleDefinition::from(&role);
        first_role.name = "first conflicted realm admin".to_string();
        let mut second_role = AdminDocumentRoleDefinition::from(&role);
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

        let mut operation = ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
            actor: claiming_actor.clone(),
        });
        let txn_id = TxnId::new();
        let effects = operation
            .emit_write_auth_doc_and_admin_state(
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
            Some(Ok(ClaimInitialRealmAdminResult::AlreadyClaimed))
        );
    }

    #[test]
    fn commit_storage_conflict_preserves_original_error_without_abort() {
        let realm_id = RealmId::from_bytes([31u8; 32]);
        let actor = actor(realm_id, 32, 33);
        let (auth_doc, _) = auth_doc_and_admin_role(realm_id);
        let txn_id = TxnId::new();
        let mut operation =
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput { actor });
        operation.state = ClaimInitialRealmAdminState::CommitTransaction {
            txn_id,
            auth_doc,
            admin_outbox_written: false,
        };

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }));

        assert!(effects.is_empty());
        assert_eq!(operation.state, ClaimInitialRealmAdminState::Error);
        assert_eq!(
            operation.output,
            Some(Err(ClaimInitialRealmAdminError::StorageError(
                StorageError::TransactionConflict
            )))
        );
    }

    #[test]
    fn scheduling_error_finishes_without_direct_announcement() {
        let realm_id = RealmId::from_bytes([15u8; 32]);
        let actor = actor(realm_id, 16, 17);
        let (auth_doc, _) = auth_doc_and_admin_role(realm_id);
        let mut operation =
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput { actor });
        operation.state = ClaimInitialRealmAdminState::ScheduleAdminDocumentOutboxDrain {
            auth_doc: auth_doc.clone(),
        };

        operation.step(Event::Task(TaskEvent::Error {
            key: Some(TaskKey::DrainDocumentSyncOutbox),
            message: "schedule failed".to_string(),
        }));
        assert_eq!(operation.state, ClaimInitialRealmAdminState::Finish);
        assert_eq!(
            operation.output,
            Some(Ok(ClaimInitialRealmAdminResult::Claimed(auth_doc)))
        );
    }

    #[tokio::test]
    async fn claims_initial_realm_admin_once() {
        let (context, net_handle, realm_id, node_id, _temp_dir) = setup_context().await;
        let user_id = UserId::local(Ulid::new(), realm_id);
        let result = drive(
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
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
            ClaimInitialRealmAdminResult::Claimed(auth_doc) => {
                assert!(auth_doc.roles.values().any(|role| {
                    role.name == "realm_admin" && role.assigned_users.contains(&user_id)
                }));
            }
            other => panic!("unexpected claim result: {other:?}"),
        }

        let second = drive(
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                actor: Actor {
                    node_id,
                    user_id: UserId::local(Ulid::new(), realm_id),
                    realm_id,
                },
            }),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(second, ClaimInitialRealmAdminResult::AlreadyClaimed);

        net_handle.shutdown().await;
    }
}
