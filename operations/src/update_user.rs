use aruna_core::admin_document_reducer::{AdminDocumentReducerError, AdminDocumentReducerState};
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::document::{
    DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent, DocumentSyncRevision,
    DocumentSyncTarget,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, document_sync_revision_key,
    document_sync_revision_write_entry, stale_admin_document_conflict_delete_entries,
};
use aruna_core::structs::{
    Actor, AuthContext, Permission, PlacementRef, RealmConfigDocument, RealmId, User,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, UserId};
use aruna_core::user_update_validation::{
    UserAttributeValidationError, validate_user_attribute_count, validate_user_attribute_key,
    validate_user_attribute_value,
};
use aruna_core::{ADMIN_DOCUMENT_STATE_KEYSPACE, DOCUMENT_SYNC_REVISION_KEYSPACE, USER_KEYSPACE};
use byteview::ByteView;
use chrono::Utc;
use smallvec::smallvec;
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use ulid::Ulid;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::placement::placement_ref_for_target;
use crate::replicate_documents::replicate_documents_effect;

const MAX_USER_NAME_LEN: usize = 256;

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateUserInput {
    pub actor: Actor,
    pub auth_context: AuthContext,
    pub self_realm_id: RealmId,
    pub user_id: String,
    pub name: Option<String>,
    pub set_attributes: HashMap<String, String>,
    pub remove_attributes: Vec<String>,
}

#[derive(Debug, PartialEq)]
pub struct UpdateUserOperation {
    input: UpdateUserInput,
    target_user_id: Option<UserId>,
    state: UpdateUserState,
    output: Option<Result<User, UpdateUserError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum UpdateUserState {
    Init,
    Auth,
    StartTransaction,
    ReadUserAdminStateAndDocumentRevision {
        txn_id: TxnId,
    },
    WriteUserAdminStateAndDocumentRevision {
        txn_id: TxnId,
        user: User,
        admin_outbox_written: bool,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    },
    DeleteStaleAdminConflicts {
        txn_id: TxnId,
        user: User,
        admin_outbox_written: bool,
    },
    CommitTransaction {
        txn_id: TxnId,
        user: User,
        admin_outbox_written: bool,
    },
    ScheduleAdminDocumentOutboxDrain {
        user: User,
    },
    AnnounceUser {
        user: User,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum UpdateUserError {
    #[error("Unauthorized")]
    Unauthorized,
    #[error("User not found")]
    UserNotFound,
    #[error("stored user id does not match requested user id")]
    UserIdMismatch,
    #[error("user name must be non-empty and at most {MAX_USER_NAME_LEN} bytes")]
    InvalidUserName,
    #[error("invalid user attribute key: {0}")]
    InvalidAttributeKey(String),
    #[error("invalid user attribute value for key: {0}")]
    InvalidAttributeValue(String),
    #[error("too many user attributes")]
    TooManyAttributes,
    #[error(transparent)]
    AuthorizationError(#[from] AuthorizationError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentReducerError(#[from] AdminDocumentReducerError),
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
    #[error("update user did not finish")]
    NotFinished,
}

impl From<UserAttributeValidationError> for UpdateUserError {
    fn from(error: UserAttributeValidationError) -> Self {
        match error {
            UserAttributeValidationError::InvalidKey(key) => Self::InvalidAttributeKey(key),
            UserAttributeValidationError::InvalidValue(key) => Self::InvalidAttributeValue(key),
            UserAttributeValidationError::TooManyAttributes => Self::TooManyAttributes,
        }
    }
}

impl UpdateUserOperation {
    pub fn new(input: UpdateUserInput) -> Self {
        Self {
            input,
            target_user_id: None,
            state: UpdateUserState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: UpdateUserError) -> Effects {
        let cleanup = self.abort();
        self.state = UpdateUserState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }
        Ok(event)
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(UpdateUserError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn start_auth(&mut self) -> Result<Effects, UpdateUserError> {
        let target_user_id = UserId::from_string(&self.input.user_id)?;
        if target_user_id.realm_id != self.input.self_realm_id
            || self.input.auth_context.realm_id != self.input.self_realm_id
            || self.input.actor.realm_id != self.input.self_realm_id
            || self.input.actor.user_id != self.input.auth_context.user_id
        {
            return Err(UpdateUserError::Unauthorized);
        }
        self.target_user_id = Some(target_user_id);

        if self.input.auth_context.user_id == target_user_id {
            if self.input.auth_context.path_restrictions.is_some() {
                return Err(UpdateUserError::Unauthorized);
            }
            self.state = UpdateUserState::StartTransaction;
            return Ok(smallvec![Effect::Storage(
                StorageEffect::StartTransaction { read: false }
            )]);
        }

        self.state = UpdateUserState::Auth;
        Ok(smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.input.auth_context.clone(),
                path: format!("/{}/admin/u/{}", self.input.self_realm_id, target_user_id),
                required_permission: Permission::WRITE,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))])
    }

    fn handle_auth_result(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return self.unexpected_event(
                "Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed })",
                got,
            );
        };

        match allowed {
            Ok(true) => {
                self.state = UpdateUserState::StartTransaction;
                smallvec![Effect::Storage(StorageEffect::StartTransaction {
                    read: false,
                })]
            }
            Ok(false) => self.fail(UpdateUserError::Unauthorized),
            Err(error) => self.fail(error.into()),
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                "Event::Storage(StorageEvent::TransactionStarted { txn_id })",
                got,
            );
        };
        let Some(target_user_id) = self.target_user_id else {
            return self.fail(UpdateUserError::UserNotFound);
        };
        let admin_target = AdminDocumentTarget::User {
            user_id: target_user_id,
        };
        let document_target = DocumentSyncTarget::User {
            user_id: target_user_id,
        };
        self.state = UpdateUserState::ReadUserAdminStateAndDocumentRevision { txn_id };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    USER_KEYSPACE.to_string(),
                    ByteView::from(target_user_id.to_bytes()),
                ),
                (
                    ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                    admin_document_reducer_state_key(&admin_target),
                ),
                (
                    DOCUMENT_SYNC_REVISION_KEYSPACE.to_string(),
                    document_sync_revision_key(&document_target),
                ),
                (
                    REALM_CONFIG_KEYSPACE.to_string(),
                    ByteView::from(*self.input.actor.realm_id.as_bytes()),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn handle_read_user_admin_state_and_document_revision(
        &mut self,
        event: Event,
        txn_id: TxnId,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected_event("Event::Storage(StorageEvent::BatchReadResult)", got);
        };
        let [
            (_, user_value),
            (_, reducer_state_value),
            (_, revision_value),
            (_, realm_config_value),
        ] = values.as_slice()
        else {
            return self.unexpected_event(
                "Event::Storage(StorageEvent::BatchReadResult) with user, admin state, document revision, and realm config values",
                got,
            );
        };

        match self.emit_write_user(
            txn_id,
            user_value.clone(),
            reducer_state_value.clone(),
            revision_value.clone(),
            realm_config_value.clone(),
        ) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn emit_write_user(
        &mut self,
        txn_id: TxnId,
        user_value: Option<ByteView>,
        reducer_state_value: Option<ByteView>,
        revision_value: Option<ByteView>,
        realm_config_value: Option<ByteView>,
    ) -> Result<Effects, UpdateUserError> {
        let current = user_value.ok_or(UpdateUserError::UserNotFound)?;
        let mut user = User::from_bytes(&current)?;
        if Some(user.user_id) != self.target_user_id {
            return Err(UpdateUserError::UserIdMismatch);
        }

        apply_updates(&mut user, &self.input)?;
        let admin_target = AdminDocumentTarget::User {
            user_id: user.user_id,
        };
        let previous_reducer_state = reducer_state_value
            .as_ref()
            .map(|value| {
                aruna_core::admin_document_reducer::decode_admin_document_reducer_state(
                    value.as_ref(),
                )
                .map_err(ConversionError::from)
            })
            .transpose()?;
        if previous_reducer_state
            .as_ref()
            .is_some_and(|state| state.target != admin_target)
        {
            return Err(AdminDocumentReducerError::TargetMismatch.into());
        }
        let mut reducer_state = previous_reducer_state
            .clone()
            .unwrap_or_else(|| AdminDocumentReducerState::new(admin_target));
        let admin_events = apply_admin_reducer_updates(&mut reducer_state, &self.input)?;
        let previous_document_revision = revision_value
            .as_ref()
            .map(|value| {
                postcard::from_bytes::<DocumentSyncChange>(value.as_ref())
                    .map_err(ConversionError::from)
            })
            .transpose()?;
        let document_target = DocumentSyncTarget::User {
            user_id: user.user_id,
        };
        let placement = realm_config_value
            .as_deref()
            .map(RealmConfigDocument::from_bytes)
            .transpose()?
            .map(|config| placement_ref_for_target(&config, &document_target, Default::default()))
            .unwrap_or(PlacementRef::NIL);
        let document_revision = local_user_document_sync_change(
            previous_document_revision.as_ref(),
            &self.input.actor,
            placement,
        );

        let bytes = user.reconcile_bytes(Some(&current), &self.input.actor)?;
        let stale_conflict_deletes = stale_admin_document_conflict_delete_entries(
            previous_reducer_state.as_ref(),
            Some(&reducer_state),
        );
        let mut writes = vec![
            (
                USER_KEYSPACE.to_string(),
                ByteView::from(user.user_id.to_bytes()),
                ByteView::from(bytes),
            ),
            admin_document_reducer_state_write_entry(&reducer_state)?,
        ];
        writes.push(document_sync_revision_write_entry(
            &document_target,
            &document_revision,
        )?);
        for event in &admin_events {
            let record = new_outbox_record_with_id(
                event.event_id,
                self.input.actor.node_id,
                document_target.clone(),
                Vec::new(),
                DocumentSyncOutboxEvent::AdminOperation {
                    event: Box::new(event.clone()),
                },
                false,
            );
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
        writes.extend(admin_document_conflict_write_entries(&reducer_state)?);

        self.state = UpdateUserState::WriteUserAdminStateAndDocumentRevision {
            txn_id,
            user: user.clone(),
            admin_outbox_written: !admin_events.is_empty(),
            stale_conflict_deletes,
        };
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_write_user(
        &mut self,
        event: Event,
        txn_id: TxnId,
        user: User,
        admin_outbox_written: bool,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.unexpected_event("Event::Storage(StorageEvent::BatchWriteResult)", got);
        };
        if !stale_conflict_deletes.is_empty() {
            self.state = UpdateUserState::DeleteStaleAdminConflicts {
                txn_id,
                user,
                admin_outbox_written,
            };
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes: stale_conflict_deletes,
                txn_id: Some(txn_id),
            })];
        }

        self.emit_commit_transaction(txn_id, user, admin_outbox_written)
    }

    fn handle_delete_stale_admin_conflicts(
        &mut self,
        event: Event,
        txn_id: TxnId,
        user: User,
        admin_outbox_written: bool,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.unexpected_event("Event::Storage(StorageEvent::BatchDeleteResult)", got);
        };

        self.emit_commit_transaction(txn_id, user, admin_outbox_written)
    }

    fn emit_commit_transaction(
        &mut self,
        txn_id: TxnId,
        user: User,
        admin_outbox_written: bool,
    ) -> Effects {
        self.state = UpdateUserState::CommitTransaction {
            txn_id,
            user,
            admin_outbox_written,
        };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit_transaction(
        &mut self,
        event: Event,
        user: User,
        admin_outbox_written: bool,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                "Event::Storage(StorageEvent::TransactionCommitted { .. })",
                got,
            );
        };
        if admin_outbox_written {
            self.state = UpdateUserState::ScheduleAdminDocumentOutboxDrain { user };
            return smallvec![schedule_outbox_drain_effect()];
        }

        self.emit_announce_user(user)
    }

    fn handle_schedule_admin_document_outbox_drain(&mut self, event: Event, user: User) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. })
            | Event::Task(TaskEvent::Error { .. }) => {
                self.state = UpdateUserState::Finish;
                self.output = Some(Ok(user));
                smallvec![]
            }
            other => self.unexpected_event(
                "admin document outbox drain timer schedule",
                format!("{other:?}"),
            ),
        }
    }

    fn emit_announce_user(&mut self, user: User) -> Effects {
        let user_id = user.user_id;
        self.state = UpdateUserState::AnnounceUser { user };
        let document = DocumentSyncTarget::User { user_id };
        smallvec![replicate_documents_effect(
            self.input.actor.realm_id,
            self.input.actor.node_id,
            vec![document],
        )]
    }

    fn handle_announce_user(&mut self, event: Event, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) = event else {
            return self.unexpected_event(
                "Event::SubOperation(SubOperationEvent::DocumentSyncResult { result })",
                got,
            );
        };

        match result {
            Ok(()) => {
                self.state = UpdateUserState::Finish;
                self.output = Some(Ok(user));
                smallvec![]
            }
            Err(error) => self.fail(UpdateUserError::TopicAnnouncement(error)),
        }
    }
}

impl Operation for UpdateUserOperation {
    type Output = User;
    type Error = UpdateUserError;

    fn start(&mut self) -> Effects {
        match self.start_auth() {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state.clone() {
            UpdateUserState::Auth => self.handle_auth_result(event),
            UpdateUserState::StartTransaction => self.handle_start_transaction(event),
            UpdateUserState::ReadUserAdminStateAndDocumentRevision { txn_id } => {
                self.handle_read_user_admin_state_and_document_revision(event, txn_id)
            }
            UpdateUserState::WriteUserAdminStateAndDocumentRevision {
                txn_id,
                user,
                admin_outbox_written,
                stale_conflict_deletes,
            } => self.handle_write_user(
                event,
                txn_id,
                user,
                admin_outbox_written,
                stale_conflict_deletes,
            ),
            UpdateUserState::DeleteStaleAdminConflicts {
                txn_id,
                user,
                admin_outbox_written,
            } => {
                self.handle_delete_stale_admin_conflicts(event, txn_id, user, admin_outbox_written)
            }
            UpdateUserState::CommitTransaction {
                user,
                admin_outbox_written,
                ..
            } => self.handle_commit_transaction(event, user, admin_outbox_written),
            UpdateUserState::ScheduleAdminDocumentOutboxDrain { user } => {
                self.handle_schedule_admin_document_outbox_drain(event, user)
            }
            UpdateUserState::AnnounceUser { user } => self.handle_announce_user(event, user),
            UpdateUserState::Init | UpdateUserState::Finish | UpdateUserState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, UpdateUserState::Finish | UpdateUserState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(UpdateUserError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            UpdateUserState::ReadUserAdminStateAndDocumentRevision { txn_id }
            | UpdateUserState::WriteUserAdminStateAndDocumentRevision { txn_id, .. }
            | UpdateUserState::DeleteStaleAdminConflicts { txn_id, .. }
            | UpdateUserState::CommitTransaction { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

fn current_timestamp_ms() -> u64 {
    u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default()
}

fn local_user_document_sync_change(
    previous_change: Option<&DocumentSyncChange>,
    actor: &Actor,
    placement: PlacementRef,
) -> DocumentSyncChange {
    let updated_at_ms = current_timestamp_ms();
    let minimum_generation = previous_change
        .map(|change| change.current.generation.saturating_add(1))
        .unwrap_or_default();
    DocumentSyncChange {
        base: previous_change.map(|change| change.current),
        current: DocumentSyncRevision {
            generation: updated_at_ms.max(minimum_generation),
            event_id: Ulid::new(),
            actor: actor.node_id,
            updated_at_ms,
        },
        kind: DocumentSyncChangeKind::Upsert,
        placement,
    }
}

fn apply_admin_reducer_updates(
    state: &mut AdminDocumentReducerState,
    input: &UpdateUserInput,
) -> Result<Vec<AdminDocumentEvent>, AdminDocumentReducerError> {
    let mut events = Vec::new();
    for operation in admin_document_operations(input) {
        events.push(state.apply_operation(&input.actor, operation)?);
    }

    Ok(events)
}

fn admin_document_operations(input: &UpdateUserInput) -> Vec<AdminDocumentOperation> {
    let mut operations = Vec::new();

    if let Some(name) = input.name.as_ref() {
        operations.push(AdminDocumentOperation::UserNameSet {
            name: name.trim().to_string(),
        });
    }

    let mut remove_keys = input.remove_attributes.clone();
    remove_keys.sort();
    remove_keys.dedup();
    for key in remove_keys {
        if !input.set_attributes.contains_key(&key) {
            operations.push(AdminDocumentOperation::UserAttributeRemoved { key });
        }
    }

    let mut set_attributes: Vec<_> = input.set_attributes.iter().collect();
    set_attributes.sort_by_key(|(left, _)| *left);
    for (key, value) in set_attributes {
        operations.push(AdminDocumentOperation::UserAttributeSet {
            key: key.clone(),
            value: value.clone(),
        });
    }

    operations
}

fn apply_updates(user: &mut User, input: &UpdateUserInput) -> Result<(), UpdateUserError> {
    if let Some(name) = input.name.as_ref() {
        let trimmed = name.trim();
        if trimmed.is_empty() || trimmed.len() > MAX_USER_NAME_LEN {
            return Err(UpdateUserError::InvalidUserName);
        }
        user.name = trimmed.to_string();
    }

    let mut removals = HashSet::new();
    for key in &input.remove_attributes {
        validate_user_attribute_key(key)?;
        removals.insert(key.clone());
    }
    for key in removals {
        user.attributes.remove(&key);
    }

    for (key, value) in &input.set_attributes {
        validate_user_attribute_key(key)?;
        validate_user_attribute_value(key, value)?;
        user.attributes.insert(key.clone(), value.clone());
    }

    validate_user_attribute_count(user.attributes.len())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{UpdateUserError, UpdateUserInput, UpdateUserOperation};
    use aruna_core::admin_document_reducer::{
        AdminDocumentConflict, AdminDocumentConflictValue, AdminDocumentReducerState,
    };
    use aruna_core::admin_documents::{AdminDocumentClock, AdminDocumentDot, AdminDocumentTarget};
    use aruna_core::document::{
        DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent,
        DocumentSyncOutboxRecord, DocumentSyncRevision, DocumentSyncTarget,
    };
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::storage_entries::{
        admin_document_reducer_conflict_key, admin_document_reducer_state_key,
        document_sync_revision_key,
    };
    use aruna_core::structs::{Actor, AuthContext, PlacementRef, RealmId, User};
    use aruna_core::task::{TaskEvent, TaskKey};
    use aruna_core::types::{TxnId, UserId};
    use aruna_core::{
        ADMIN_DOCUMENT_CONFLICT_KEYSPACE, ADMIN_DOCUMENT_STATE_KEYSPACE,
        DOCUMENT_SYNC_OUTBOX_KEYSPACE, DOCUMENT_SYNC_REVISION_KEYSPACE, USER_KEYSPACE,
    };
    use byteview::ByteView;
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use ulid::Ulid;

    fn actor(realm_id: RealmId, user_id: UserId) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
            user_id,
            realm_id,
        }
    }

    fn auth_context(realm_id: RealmId, user_id: UserId) -> AuthContext {
        AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        }
    }

    fn input(realm_id: RealmId, caller_id: UserId, user_id: UserId) -> UpdateUserInput {
        UpdateUserInput {
            actor: actor(realm_id, caller_id),
            auth_context: auth_context(realm_id, caller_id),
            self_realm_id: realm_id,
            user_id: user_id.to_string(),
            name: Some("Alice Updated".to_string()),
            set_attributes: HashMap::from([
                ("orcid".to_string(), "0000-0002-1825-0097".to_string()),
                ("department".to_string(), "biology".to_string()),
            ]),
            remove_attributes: vec!["old".to_string()],
        }
    }

    fn stored_user(user_id: UserId) -> User {
        User {
            user_id,
            name: "Alice".to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: HashMap::from([
                ("old".to_string(), "remove-me".to_string()),
                ("department".to_string(), "physics".to_string()),
            ]),
        }
    }

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

    fn document_revision(seed: u8, generation: u64) -> DocumentSyncRevision {
        DocumentSyncRevision {
            generation,
            event_id: Ulid::from_bytes([seed; 16]),
            actor: node(seed),
            updated_at_ms: generation,
        }
    }

    fn conflict(path: &str, first_seed: u8, second_seed: u8) -> AdminDocumentConflict {
        AdminDocumentConflict {
            path: path.to_string(),
            values: vec![
                AdminDocumentConflictValue {
                    value: Some(format!("value-{first_seed}")),
                    dot: dot(first_seed),
                },
                AdminDocumentConflictValue {
                    value: Some(format!("value-{second_seed}")),
                    dot: dot(second_seed),
                },
            ],
        }
    }

    fn reducer_state_with_conflicts(user_id: UserId) -> AdminDocumentReducerState {
        let name_first = dot(11);
        let name_second = dot(12);
        let title_first = dot(13);
        let title_second = dot(14);
        let mut clock = AdminDocumentClock::default();
        for dot in [name_first, name_second, title_first, title_second] {
            clock.advance(dot.origin_node_id, dot.origin_seq);
        }

        AdminDocumentReducerState {
            target: AdminDocumentTarget::User { user_id },
            clock,
            applied_event_ids: BTreeSet::from([
                name_first.event_id,
                name_second.event_id,
                title_first.event_id,
                title_second.event_id,
            ]),
            user_attributes: BTreeMap::new(),
            conflicts: BTreeMap::from([
                ("user.name".to_string(), conflict("user.name", 11, 12)),
                (
                    "user.attributes.title".to_string(),
                    conflict("user.attributes.title", 13, 14),
                ),
            ]),
            user_name: None,
            user_subject_ids: BTreeMap::new(),
            equivalent_value_dots: BTreeMap::new(),
        }
    }

    #[test]
    fn updates_user_attributes_and_queues_admin_operations() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let original = stored_user(user_id);
        let mut operation = UpdateUserOperation::new(input(realm_id, user_id, user_id));

        assert!(matches!(
            operation.start().first(),
            Some(Effect::Storage(StorageEffect::StartTransaction {
                read: false
            }))
        ));

        let txn_id = TxnId::new();
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let target = AdminDocumentTarget::User { user_id };
        let document = DocumentSyncTarget::User { user_id };
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchRead { reads, txn_id: id }) => {
                assert_eq!(*id, Some(txn_id));
                assert_eq!(reads.len(), 4);
                assert_eq!(reads[0].0, USER_KEYSPACE);
                assert_eq!(reads[0].1.as_ref(), user_id.to_bytes().as_slice());
                assert_eq!(reads[1].0, ADMIN_DOCUMENT_STATE_KEYSPACE);
                assert_eq!(reads[1].1, admin_document_reducer_state_key(&target));
                assert_eq!(reads[2].0, DOCUMENT_SYNC_REVISION_KEYSPACE);
                assert_eq!(reads[2].1, document_sync_revision_key(&document));
                assert_eq!(reads[3].0, REALM_CONFIG_KEYSPACE);
            }
            other => panic!("unexpected read effect: {other:?}"),
        }

        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    user_id.to_bytes().into(),
                    Some(original.to_bytes(&actor(realm_id, user_id)).unwrap().into()),
                ),
                (admin_document_reducer_state_key(&target), None),
                (document_sync_revision_key(&document), None),
                (ByteView::from(*realm_id.as_bytes()), None),
            ],
        }));
        let (updated, reducer_state) = match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchWrite { writes, txn_id: id }) => {
                assert_eq!(*id, Some(txn_id));
                let user_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == USER_KEYSPACE)
                    .expect("user write is included");
                let reducer_state_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == ADMIN_DOCUMENT_STATE_KEYSPACE)
                    .expect("reducer state write is included");
                assert!(
                    writes
                        .iter()
                        .all(|(keyspace, _, _)| keyspace != ADMIN_DOCUMENT_CONFLICT_KEYSPACE)
                );
                let outbox_records: Vec<DocumentSyncOutboxRecord> = writes
                    .iter()
                    .filter(|(keyspace, _, _)| keyspace == DOCUMENT_SYNC_OUTBOX_KEYSPACE)
                    .map(|(_, _, value)| postcard::from_bytes(value).unwrap())
                    .collect();
                assert_eq!(outbox_records.len(), 4);
                assert!(
                    outbox_records
                        .iter()
                        .all(|record| record.target == document)
                );
                assert!(outbox_records.iter().any(|record| matches!(
                    &record.event,
                    DocumentSyncOutboxEvent::AdminOperation {
                        event
                    } if matches!(
                        &event.op,
                        aruna_core::admin_documents::AdminDocumentOperation::UserNameSet { .. }
                    )
                )));
                (
                    User::from_bytes(user_write.2.as_ref()).unwrap(),
                    postcard::from_bytes::<AdminDocumentReducerState>(
                        reducer_state_write.2.as_ref(),
                    )
                    .unwrap(),
                )
            }
            other => panic!("unexpected update effect: {other:?}"),
        };
        assert_eq!(updated.name, "Alice Updated");
        assert_eq!(
            updated.attributes.get("orcid").map(String::as_str),
            Some("0000-0002-1825-0097")
        );
        assert_eq!(
            updated.attributes.get("department").map(String::as_str),
            Some("biology")
        );
        assert!(!updated.attributes.contains_key("old"));
        assert_eq!(
            reducer_state.materialized_user_name(),
            Some(updated.name.clone())
        );
        assert_eq!(
            reducer_state
                .materialized_user_attributes()
                .get("department")
                .map(String::as_str),
            Some("biology")
        );

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::CommitTransaction { .. }))
        ));

        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(matches!(effects.first(), Some(Effect::Task(_))));

        let effects = operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainDocumentSyncOutbox,
            after: std::time::Duration::ZERO,
        }));
        assert!(effects.is_empty());
        assert_eq!(operation.finalize().unwrap(), updated);
    }

    #[test]
    fn writes_document_sync_revision_sidecar_with_user_update() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let original = stored_user(user_id);
        let request = input(realm_id, user_id, user_id);
        let expected_actor = request.actor.clone();
        let mut operation = UpdateUserOperation::new(request);
        let admin_target = AdminDocumentTarget::User { user_id };
        let document_target = DocumentSyncTarget::User { user_id };
        let previous_revision = DocumentSyncChange {
            base: None,
            current: document_revision(21, 42),
            kind: DocumentSyncChangeKind::Upsert,
            placement: PlacementRef::NIL,
        };

        operation.start();
        let txn_id = TxnId::new();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));

        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    user_id.to_bytes().into(),
                    Some(original.to_bytes(&actor(realm_id, user_id)).unwrap().into()),
                ),
                (admin_document_reducer_state_key(&admin_target), None),
                (
                    document_sync_revision_key(&document_target),
                    Some(postcard::to_allocvec(&previous_revision).unwrap().into()),
                ),
                (ByteView::from(*realm_id.as_bytes()), None),
            ],
        }));

        let [
            Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: Some(write_txn_id),
            }),
        ] = effects.as_slice()
        else {
            panic!("expected user update batch write, got {effects:?}");
        };
        assert_eq!(*write_txn_id, txn_id);
        let user_write = writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == USER_KEYSPACE)
            .expect("user write is included");
        let updated = User::from_bytes(user_write.2.as_ref()).unwrap();
        assert_eq!(updated.name, "Alice Updated");

        let (revision_key, revision): (_, DocumentSyncChange) = writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == DOCUMENT_SYNC_REVISION_KEYSPACE)
            .map(|(_, key, value)| {
                (
                    key,
                    postcard::from_bytes(value).expect("revision sidecar decodes"),
                )
            })
            .expect("revision sidecar write exists");
        assert_eq!(revision_key, &document_sync_revision_key(&document_target));
        assert_eq!(revision.base, Some(previous_revision.current));
        assert_eq!(revision.current.actor, expected_actor.node_id);
        assert!(revision.current.generation > previous_revision.current.generation);
        assert!(revision.current.updated_at_ms > 0);
        assert_eq!(revision.kind, DocumentSyncChangeKind::Upsert);
    }

    #[test]
    fn writes_reducer_state_and_conflicts_with_user_update_transaction() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let original = stored_user(user_id);
        let previous_state = reducer_state_with_conflicts(user_id);
        let target = AdminDocumentTarget::User { user_id };
        let document = DocumentSyncTarget::User { user_id };
        let mut operation = UpdateUserOperation::new(input(realm_id, user_id, user_id));

        operation.start();
        let txn_id = TxnId::new();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));

        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    user_id.to_bytes().into(),
                    Some(original.to_bytes(&actor(realm_id, user_id)).unwrap().into()),
                ),
                (
                    admin_document_reducer_state_key(&target),
                    Some(postcard::to_allocvec(&previous_state).unwrap().into()),
                ),
                (document_sync_revision_key(&document), None),
                (ByteView::from(*realm_id.as_bytes()), None),
            ],
        }));

        let (updated, reducer_state) = match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchWrite { writes, txn_id: id }) => {
                assert_eq!(*id, Some(txn_id));
                let user_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == USER_KEYSPACE)
                    .expect("user write is included");
                let reducer_state_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == ADMIN_DOCUMENT_STATE_KEYSPACE)
                    .expect("reducer state write is included");
                let conflict_writes: Vec<_> = writes
                    .iter()
                    .filter(|(keyspace, _, _)| keyspace == ADMIN_DOCUMENT_CONFLICT_KEYSPACE)
                    .collect();
                assert_eq!(conflict_writes.len(), 1);
                let conflict: AdminDocumentConflict =
                    postcard::from_bytes(conflict_writes[0].2.as_ref()).unwrap();
                assert_eq!(conflict.path, "user.attributes.title");

                (
                    User::from_bytes(user_write.2.as_ref()).unwrap(),
                    postcard::from_bytes::<AdminDocumentReducerState>(
                        reducer_state_write.2.as_ref(),
                    )
                    .unwrap(),
                )
            }
            other => panic!("unexpected update effect: {other:?}"),
        };

        assert_eq!(updated.name, "Alice Updated");
        assert_eq!(reducer_state.materialized_user_name(), Some(updated.name));
        assert_eq!(reducer_state.conflicts.len(), 1);
        assert!(
            reducer_state
                .conflicts
                .contains_key("user.attributes.title")
        );
        assert!(!reducer_state.conflicts.contains_key("user.name"));

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        let deletes = match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchDelete {
                deletes,
                txn_id: id,
            }) => {
                assert_eq!(*id, Some(txn_id));
                assert_eq!(
                    deletes,
                    &vec![(
                        ADMIN_DOCUMENT_CONFLICT_KEYSPACE.to_string(),
                        admin_document_reducer_conflict_key(&target, "user.name"),
                    )]
                );
                deletes.clone()
            }
            other => panic!("unexpected conflict delete effect: {other:?}"),
        };

        let effects = operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: deletes,
        }));
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::CommitTransaction { .. }))
        ));
    }

    #[test]
    fn invalid_attribute_key_uses_update_user_error() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let mut input = input(realm_id, user_id, user_id);
        input.remove_attributes = vec!["display name".to_string()];
        input.set_attributes.clear();
        let mut user = stored_user(user_id);

        assert_eq!(
            super::apply_updates(&mut user, &input),
            Err(UpdateUserError::InvalidAttributeKey(
                "display name".to_string()
            ))
        );
    }

    #[test]
    fn unauthorized_update_fails() {
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let caller_id = UserId::local(Ulid::from_bytes([4u8; 16]), realm_id);
        let user_id = UserId::local(Ulid::from_bytes([5u8; 16]), realm_id);
        let mut operation = UpdateUserOperation::new(input(realm_id, caller_id, user_id));
        operation.start();

        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(false) },
        ));

        assert!(effects.is_empty());
        assert!(operation.finalize().is_err());
    }

    #[test]
    fn scoped_self_update_fails() {
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([7u8; 16]), realm_id);
        let mut input = input(realm_id, user_id, user_id);
        input.auth_context.path_restrictions = Some(Vec::new());
        let mut operation = UpdateUserOperation::new(input);

        let effects = operation.start();

        assert!(effects.is_empty());
        assert!(operation.finalize().is_err());
    }
}
