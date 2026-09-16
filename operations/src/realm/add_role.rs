use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget, AdminRoleDefinition,
};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{AUTH_KEYSPACE, DOCUMENT_STATE_KEYSPACE};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission, Role};
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::sync::document_outbox::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use crate::sync::replicate_documents::replicate_documents_effect;

#[derive(Clone, Debug, PartialEq)]
pub struct RealmRoleConfig {
    pub actor: Actor,
    pub realm_id: RealmId,
    pub role: Role,
}

#[derive(PartialEq)]
pub struct RealmRoleOperation {
    input: RealmRoleConfig,
    state: RealmRoleState,
    output: Option<Result<RealmAuthorizationDocument, RealmRoleError>>,
}

impl std::fmt::Debug for RealmRoleOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealmRoleOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RealmRoleState {
    Init,
    Auth,
    StartTransaction,
    GetAdminState {
        txn_id: TxnId,
    },
    WriteAuthState {
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
        conflict_delete_keys: Vec<(KeySpace, Vec<u8>)>,
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
    AnnounceAuthDoc {
        auth_doc: RealmAuthorizationDocument,
    },
    Finish,
    Error,
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
    #[error("No transaction found")]
    NoTransactionFound,
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
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: RealmRoleState,
        expected: &'static str,
        got: String,
    },
}

const REALM_ROLE_NAMES: &[&str] = &["realm_admin"];

fn reserved_role_name(name: &str) -> bool {
    REALM_ROLE_NAMES.contains(&name.trim())
}

impl RealmRoleOperation {
    pub fn new(input: RealmRoleConfig) -> Self {
        RealmRoleOperation {
            input,
            state: RealmRoleState::Init,
            output: None,
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                RealmRoleState::StartTransaction,
                "Event::Storage(StorageEvent::TransactionStarted)",
                got,
            );
        };
        match self.emit_auth_read(txn_id) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn auth_context(&self) -> AuthContext {
        AuthContext {
            user_id: self.input.actor.user_id,
            realm_id: self.input.actor.realm_id,
            path_restrictions: None,
            session: None,
        }
    }

    fn validate_role(&self) -> Result<(), RealmRoleError> {
        if reserved_role_name(&self.input.role.name) {
            return Err(RealmRoleError::ReservedRoleName);
        }

        if self
            .input
            .role
            .assigned_users
            .iter()
            .any(|user| user.is_nil() && !user.is_nil_in(self.input.realm_id))
        {
            return Err(RealmRoleError::InvalidAssignedUser);
        }

        if self.input.role.is_public(self.input.realm_id)
            && self
                .input
                .role
                .permissions
                .values()
                .any(|permission| permission != &Permission::READ)
        {
            return Err(RealmRoleError::InvalidPublicRole);
        }

        Ok(())
    }

    fn handle_authorization(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return self.unexpected_event(
                RealmRoleState::Auth,
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
    ) -> Result<Effects, RealmRoleError> {
        if auth_result? {
            self.state = RealmRoleState::StartTransaction;
            Ok(smallvec![Effect::Storage(
                StorageEffect::StartTransaction { read: false }
            )])
        } else {
            Err(RealmRoleError::Unauthorized)
        }
    }

    fn emit_auth_read(&mut self, txn_id: TxnId) -> Result<Effects, RealmRoleError> {
        self.state = RealmRoleState::GetAdminState { txn_id };
        let target = AdminDocumentTarget::Realm {
            realm_id: self.input.realm_id,
        };
        let auth_key = (*self.input.realm_id.as_bytes()).into();
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (AUTH_KEYSPACE.to_string(), auth_key),
                (
                    DOCUMENT_STATE_KEYSPACE.to_string(),
                    reducer_state_key(&target),
                ),
            ],
            txn_id: Some(txn_id),
        })])
    }

    fn handle_auth_read(&mut self, event: Event, txn_id: TxnId) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::BatchReadResult)",
                got,
            );
        };
        let [(_, auth_doc_value), (_, reducer_state_value)] = values.as_slice() else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::BatchReadResult) with auth doc and admin state values",
                got,
            );
        };

        match self.emit_auth_write(txn_id, auth_doc_value.clone(), reducer_state_value.clone()) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_auth_write(
        &mut self,
        txn_id: TxnId,
        auth_doc: Option<ByteView>,
        reducer_state_value: Option<ByteView>,
    ) -> Result<Effects, RealmRoleError> {
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
        materialize_realm_role(&mut auth_doc, &self.input.role, &reducer_state);

        let conflict_delete_keys: Vec<_> =
            stale_conflict_deletes(previous_reducer_state.as_ref(), Some(&reducer_state))
                .into_iter()
                .map(|(key_space, key)| (key_space, key.as_ref().to_vec()))
                .collect();

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

        self.state = RealmRoleState::WriteAuthState {
            txn_id,
            auth_doc,
            admin_outbox_written: !admin_events.is_empty(),
            conflict_delete_keys,
        };

        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_auth_write(
        &mut self,
        event: Event,
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
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
            self.state = RealmRoleState::DeleteAdminConflicts {
                txn_id,
                auth_doc,
                admin_outbox_written,
            };
            let deletes = conflict_delete_keys
                .into_iter()
                .map(|(key_space, key)| (key_space, Key::from(key)))
                .collect();
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes,
                txn_id: Some(txn_id),
            })];
        }

        self.emit_commit_transaction(txn_id, auth_doc, admin_outbox_written)
    }

    fn delete_stale_conflicts(
        &mut self,
        event: Event,
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
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

        self.emit_commit_transaction(txn_id, auth_doc, admin_outbox_written)
    }

    fn emit_commit_transaction(
        &mut self,
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
        admin_outbox_written: bool,
    ) -> Effects {
        self.state = RealmRoleState::CommitTransaction {
            txn_id,
            auth_doc,
            admin_outbox_written,
        };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit_transaction(
        &mut self,
        event: Event,
        auth_doc: RealmAuthorizationDocument,
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
            self.state = RealmRoleState::ScheduleDocumentDrain { auth_doc };
            return smallvec![schedule_drain_effect()];
        }

        self.emit_auth_announce(auth_doc)
    }

    fn schedule_outbox_drain(
        &mut self,
        event: Event,
        auth_doc: RealmAuthorizationDocument,
    ) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. })
            | Event::Task(TaskEvent::Error { .. }) => {
                self.state = RealmRoleState::Finish;
                self.output = Some(Ok(auth_doc));
                smallvec![]
            }
            other => self.unexpected_event(
                self.state.clone(),
                "admin document outbox drain timer schedule",
                format!("{other:?}"),
            ),
        }
    }

    fn emit_auth_announce(&mut self, auth_doc: RealmAuthorizationDocument) -> Effects {
        self.state = RealmRoleState::AnnounceAuthDoc {
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

    fn handle_auth_announce(
        &mut self,
        event: Event,
        auth_doc: RealmAuthorizationDocument,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::SubOperation(SubOperationEvent::DocumentSyncResult)",
                got,
            );
        };
        if let Err(error) = result {
            return self.fail(RealmRoleError::TopicAnnouncement(error));
        }
        self.state = RealmRoleState::Finish;
        self.output = Some(Ok(auth_doc));
        smallvec![]
    }

    fn fail(&mut self, err: RealmRoleError) -> Effects {
        self.state = RealmRoleState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn fail_with_cleanup(&mut self, err: RealmRoleError, cleanup_effects: Effects) -> Effects {
        self.state = RealmRoleState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: RealmRoleState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            RealmRoleError::UnexpectedEvent {
                state,
                expected,
                got,
            },
            cleanup_effects,
        )
    }

    fn fail_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }

        Ok(event)
    }
}

impl Operation for RealmRoleOperation {
    type Output = RealmAuthorizationDocument;

    type Error = RealmRoleError;

    fn start(&mut self) -> Effects {
        if let Err(error) = self.validate_role() {
            return self.fail(error);
        }

        self.state = RealmRoleState::Auth;

        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.auth_context(),
                path: format!(
                    "/{}/admin/roles/{}",
                    self.input.realm_id, self.input.role.role_id
                ),
                required_permission: Permission::WRITE,
            }),
            |result| Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: result
            }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state.clone() {
            RealmRoleState::Auth => self.handle_authorization(event),
            RealmRoleState::StartTransaction => self.handle_start_transaction(event),
            RealmRoleState::GetAdminState { txn_id } => self.handle_auth_read(event, txn_id),
            RealmRoleState::WriteAuthState {
                txn_id,
                auth_doc,
                admin_outbox_written,
                conflict_delete_keys,
            } => self.handle_auth_write(
                event,
                txn_id,
                auth_doc,
                admin_outbox_written,
                conflict_delete_keys,
            ),
            RealmRoleState::DeleteAdminConflicts {
                txn_id,
                auth_doc,
                admin_outbox_written,
            } => self.delete_stale_conflicts(event, txn_id, auth_doc, admin_outbox_written),
            RealmRoleState::CommitTransaction {
                auth_doc,
                admin_outbox_written,
                ..
            } => self.handle_commit_transaction(event, auth_doc, admin_outbox_written),
            RealmRoleState::ScheduleDocumentDrain { auth_doc } => {
                self.schedule_outbox_drain(event, auth_doc)
            }
            RealmRoleState::AnnounceAuthDoc { auth_doc } => {
                self.handle_auth_announce(event, auth_doc)
            }
            RealmRoleState::Init | RealmRoleState::Finish | RealmRoleState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RealmRoleState::Finish | RealmRoleState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or_else(|| RealmRoleError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            RealmRoleState::GetAdminState { txn_id }
            | RealmRoleState::WriteAuthState { txn_id, .. }
            | RealmRoleState::DeleteAdminConflicts { txn_id, .. }
            | RealmRoleState::CommitTransaction { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
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
        RealmRoleConfig, RealmRoleError, RealmRoleOperation, RealmRoleState,
    };
    use crate::realm::claim_admin::{ClaimInitialInput, ClaimInitialOperation};
    use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_core::UserId;
    use aruna_core::admin_documents::{
        AdminDocumentDot, AdminDocumentOperation, AdminDocumentTarget, AdminRoleDefinition,
    };
    use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord, DocumentTarget};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, DOCUMENT_CONFLICT_KEYSPACE, DOCUMENT_STATE_KEYSPACE, SYNC_OUTBOX_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::reducer::{AdminConflict, AdminConflictValue, AdminDocumentState};
    use aruna_core::storage_entries::{reducer_conflict_key, reducer_state_key};
    use aruna_core::structs::identity::auth::{Actor, Permission, Role};
    use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
    use aruna_core::task::{TaskEvent, TaskKey};
    use aruna_core::types::TxnId;
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

            assert!(operation.start().is_empty());
            assert_eq!(operation.finalize(), Err(RealmRoleError::ReservedRoleName));
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

            assert!(operation.start().is_empty());
            assert_eq!(operation.finalize(), Err(RealmRoleError::InvalidPublicRole));
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

        assert!(operation.start().is_empty());
        assert_eq!(
            operation.finalize(),
            Err(RealmRoleError::InvalidAssignedUser)
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

        let mut operation = RealmRoleOperation::new(input.clone());
        assert!(matches!(
            operation.start().first(),
            Some(Effect::SubOperation(_))
        ));
        let effects = operation.step(Event::SubOperation(
            aruna_core::events::SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));
        assert_eq!(
            effects.first().unwrap(),
            &Effect::Storage(StorageEffect::StartTransaction { read: false })
        );
        let txn_id = TxnId::generate();
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert_eq!(
            effects.first().unwrap(),
            &Effect::Storage(StorageEffect::BatchRead {
                reads: vec![
                    (AUTH_KEYSPACE.to_string(), (*realm_id.as_bytes()).into()),
                    (
                        DOCUMENT_STATE_KEYSPACE.to_string(),
                        reducer_state_key(&target),
                    ),
                ],
                txn_id: Some(txn_id),
            })
        );

        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    (*realm_id.as_bytes()).into(),
                    Some(auth_doc.to_bytes(&actor).unwrap().into()),
                ),
                (
                    reducer_state_key(&target),
                    Some(postcard::to_allocvec(&previous_state).unwrap().into()),
                ),
            ],
        }));
        let write_effect = effects.first().unwrap();
        match write_effect {
            Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: effect_txn_id,
            }) => {
                assert_eq!(effect_txn_id, &Some(txn_id));
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
            other => panic!("unexpected realm role write effect: {other:?}"),
        }

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert_eq!(
            effects.first().unwrap(),
            &Effect::Storage(StorageEffect::BatchDelete {
                deletes: vec![(DOCUMENT_CONFLICT_KEYSPACE.to_string(), stale_conflict_key,)],
                txn_id: Some(txn_id),
            })
        );
        let effects = operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        assert_eq!(
            effects.first().unwrap(),
            &Effect::Storage(StorageEffect::CommitTransaction { txn_id })
        );
        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(matches!(effects.first(), Some(Effect::Task(_))));
        assert_eq!(
            operation.state,
            RealmRoleState::ScheduleDocumentDrain {
                auth_doc: expected_auth_doc.clone(),
            }
        );

        let effects = operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainSyncOutbox,
            after: std::time::Duration::ZERO,
        }));
        assert!(effects.is_empty());
        assert_eq!(operation.state, RealmRoleState::Finish);
        assert_eq!(operation.finalize().unwrap(), expected_auth_doc);
    }

    #[test]
    pub fn outbox_error_finishes() {
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
        operation.state = RealmRoleState::ScheduleDocumentDrain {
            auth_doc: auth_doc.clone(),
        };

        let effects = operation.step(Event::Task(TaskEvent::Error {
            key: Some(TaskKey::DrainSyncOutbox),
            message: "schedule failed".to_string(),
        }));

        assert!(effects.is_empty());
        assert_eq!(operation.state, RealmRoleState::Finish);
        assert_eq!(operation.finalize().unwrap(), auth_doc);
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
            task_handle: Some(task_handle),
            compute_handle: None,
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
