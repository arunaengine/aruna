//! Updates a user's fields and attributes and publishes the change for sync.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{
    DocumentChange, DocumentChangeKind, DocumentOutboxEvent, DocumentSyncRevision, DocumentTarget,
};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::keyspaces::{FEDERATION_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState};
use aruna_core::rows::done;
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
    sync_revision_entry, sync_revision_key,
};
use aruna_core::storage_entries::{subject_index_key, subject_index_value};
use aruna_core::structs::identity::admin_audit::AdminAuditRecord;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::identity::user::User;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::time::unix_timestamp_millis as current_timestamp_ms;
use aruna_core::types::{Key, KeySpace};
use aruna_core::user::validation::{
    UserAttributeError, counted_attributes, is_reserved_attribute, validate_attribute_count,
    validate_attribute_key, validate_attribute_value,
};
use aruna_core::{
    DOCUMENT_STATE_KEYSPACE, SUBJECT_CLAIMS_KEYSPACE, SUBJECT_INDEX_KEYSPACE,
    SYNC_REVISION_KEYSPACE, USER_KEYSPACE,
};
use byteview::ByteView;
use std::collections::{BTreeSet, HashMap, HashSet};
use thiserror::Error;
use ulid::Ulid;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::epoch_due::DueMarks;
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use crate::users::merge::{Coordinator, coordinator};
use aruna_core::placement::fence::WriteFence;
use aruna_core::placement::target_placement_ref;

const MAX_USER_LEN: usize = 256;

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateUserInput {
    pub actor: Actor,
    pub auth_context: AuthContext,
    pub self_realm_id: RealmId,
    pub user_id: String,
    pub name: Option<String>,
    pub set_attributes: HashMap<String, String>,
    pub remove_attributes: Vec<String>,
    /// Set by operations that authorized the change themselves; only they may touch reserved keys.
    pub system: bool,
    /// Links or unlinks a login of another realm, with its ownership claims in the same commit.
    pub alias: Option<AliasChange>,
    /// Attaches a new sign-in subject key with its index entry; only system callers do.
    pub subject: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AliasChange {
    /// Links a login confirmed for its session issued at `issued_at`; every transaction attempt
    /// rechecks the account, the realm's admission and cutoffs at `now`.
    Add {
        alias: UserId,
        issued_at: u64,
        now: u64,
    },
    Remove(UserId),
}

impl AliasChange {
    fn alias(&self) -> UserId {
        match self {
            Self::Add { alias, .. } | Self::Remove(alias) => *alias,
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum UpdateUserError {
    #[error("the plugin install changed meanwhile; this change is stale")]
    Superseded,
    #[error("Unauthorized")]
    Unauthorized,
    #[error("User not found")]
    UserNotFound,
    #[error("stored user id does not match requested user id")]
    UserIdMismatch,
    #[error("user name must be non-empty and at most {MAX_USER_LEN} bytes")]
    InvalidUserName,
    #[error("invalid user attribute key: {0}")]
    InvalidAttributeKey(String),
    #[error("invalid user attribute value for key: {0}")]
    InvalidAttributeValue(String),
    #[error("too many user attributes")]
    TooManyAttributes,
    #[error("the linked login is not a login of another realm")]
    InvalidAlias,
    #[error("the login is already linked to another account")]
    AliasClaimed,
    #[error("the account already links a login of that realm")]
    AliasRealmTaken,
    #[error("the account has no such linked login")]
    AliasMissing,
    #[error("the account is inactive or a service account")]
    AliasInactive,
    #[error("the login's realm is no longer admitted")]
    AliasNotAdmitted,
    #[error("the login was cut off after it was confirmed")]
    AliasCutOff,
    #[error("the sign-in already belongs to another account")]
    SubjectClaimed,
    #[error("this node does not run account merges")]
    NotCoordinator,
    #[error(transparent)]
    AuthorizationError(#[from] AuthorizationError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentError(#[from] AdminDocumentError),
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
    #[error("the user's bucket cut over to a new holder set; retry the update")]
    PlacementFenced,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
    #[error("update user did not finish")]
    NotFinished,
}

impl From<UserAttributeError> for UpdateUserError {
    fn from(error: UserAttributeError) -> Self {
        match error {
            UserAttributeError::InvalidKey(key) => Self::InvalidAttributeKey(key),
            UserAttributeError::InvalidValue(key) => Self::InvalidAttributeValue(key),
            UserAttributeError::TooManyAttributes => Self::TooManyAttributes,
        }
    }
}

impl From<IoError> for UpdateUserError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "UpdateUser".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

/// The updated user, and whether admin events reached the outbox.
pub(crate) type UserWritten = (User, bool);

/// What the update waits for.
#[derive(Debug, PartialEq)]
enum Step {
    Check,
    Write,
    Announce,
}

/// Checks the caller unless it updates itself or the change is a system one, writes the user in
/// one write transaction, then wakes the outbox drain or announces the user document.
#[derive(Debug, PartialEq)]
pub struct UpdateUserOperation {
    input: UpdateUserInput,
    audit: Option<AdminAuditRecord>,
    guard: Option<aruna_core::plugin_installs::InstallGuard>,
    step: Step,
    user: Option<User>,
}

impl UpdateUserOperation {
    pub fn new(input: UpdateUserInput) -> Self {
        Self {
            input,
            audit: None,
            guard: None,
            step: Step::Check,
            user: None,
        }
    }

    /// Changes the user only while the install record is still at the guard's revision.
    pub fn guarded(mut self, guard: Option<aruna_core::plugin_installs::InstallGuard>) -> Self {
        self.guard = guard;
        self
    }

    /// Commits the audit rows of `record` in the same transaction as the user change.
    pub fn audited(mut self, record: AdminAuditRecord) -> Self {
        self.audit = Some(record);
        self
    }

    fn target(&self) -> Result<UserId, UpdateUserError> {
        let target_user_id = UserId::from_string(&self.input.user_id)?;
        if target_user_id.realm_id != self.input.self_realm_id
            || self.input.auth_context.realm_id != self.input.self_realm_id
            || self.input.actor.realm_id != self.input.self_realm_id
            || self.input.actor.user_id != self.input.auth_context.user_id
        {
            return Err(UpdateUserError::Unauthorized);
        }
        Ok(target_user_id)
    }

    fn write(&mut self) -> Result<Next<User>, UpdateUserError> {
        let target = self.target()?;
        let mut write = UserWrite::new(self.input.clone(), target);
        write.audit = self.audit.clone();
        write.guard = self.guard;
        self.step = Step::Write;
        Ok(Next::Call(IoCall::child(write)))
    }

    fn written(&mut self, result: IoResult) -> Result<Next<User>, UpdateUserError> {
        let (user, outbox) = result.child::<UserWritten, UpdateUserError>()??;
        if outbox {
            // A failed drain timer leaves the durable outbox retryable.
            let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
            return Ok(Next::Then {
                output: user,
                calls,
            });
        }
        let document = DocumentTarget::User {
            user_id: user.user_id,
        };
        let announce = crate::realm::announce_call(&self.input.actor, document);
        let announce = announce.ok_or(UpdateUserError::NotFinished)?;
        self.user = Some(user);
        self.step = Step::Announce;
        Ok(Next::Call(announce))
    }
}

impl Operation for UpdateUserOperation {
    type Output = User;
    type Error = UpdateUserError;

    fn first(&mut self) -> Result<Next<User>, UpdateUserError> {
        let target = self.target()?;
        if self.input.system {
            return self.write();
        }
        if self.input.auth_context.user_id == target {
            if self.input.auth_context.path_restrictions.is_some() {
                return Err(UpdateUserError::Unauthorized);
            }
            return self.write();
        }
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: self.input.auth_context.clone(),
                path: format!("/{}/admin/u/{}", self.input.self_realm_id, target),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<User>, UpdateUserError> {
        match self.step {
            Step::Check => {
                if !result.child::<bool, AuthorizationError>()?? {
                    return Err(UpdateUserError::Unauthorized);
                }
                self.write()
            }
            Step::Write => self.written(result),
            Step::Announce => {
                let outcome = crate::realm::announced(result).map_err(|got| {
                    UpdateUserError::UnexpectedEvent {
                        state: "AnnounceUser".to_string(),
                        expected: "Event::SubOperation(SubOperationEvent::DocumentSyncResult)",
                        got,
                    }
                })?;
                outcome.map_err(UpdateUserError::TopicAnnouncement)?;
                self.user
                    .take()
                    .map(Next::Done)
                    .ok_or(UpdateUserError::NotFinished)
            }
        }
    }
}

/// What the user transaction waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Read,
    Write,
    Delete,
    Fence,
    Marks,
}

/// Reads the user, its admin state, sync revision, realm config and claims, writes the change,
/// takes the bucket fence, and marks key epochs due for a deactivation, in one write transaction.
#[derive(Debug, PartialEq)]
pub(crate) struct UserWrite {
    input: UpdateUserInput,
    target_user_id: UserId,
    audit: Option<AdminAuditRecord>,
    guard: Option<aruna_core::plugin_installs::InstallGuard>,
    stage: Stage,
    /// Bucket the user's rows publish onto, read inside the write transaction.
    fence: WriteFence,
    deactivating: bool,
    written: Option<UserWritten>,
    deletes: Vec<(KeySpace, Key)>,
    marks: DueMarks,
}

impl UserWrite {
    pub(crate) fn new(input: UpdateUserInput, target_user_id: UserId) -> Self {
        Self {
            input,
            target_user_id,
            audit: None,
            guard: None,
            stage: Stage::Read,
            fence: Default::default(),
            deactivating: false,
            written: None,
            deletes: Vec::new(),
            marks: DueMarks::new(None, current_timestamp_ms()),
        }
    }

    fn reads(&self) -> Result<Next<UserWritten>, UpdateUserError> {
        let target_user_id = self.target_user_id;
        let admin_target = AdminDocumentTarget::User {
            user_id: target_user_id,
        };
        let document_target = DocumentTarget::User {
            user_id: target_user_id,
        };
        Ok(Next::Call(IoCall::Read(
            vec![
                (
                    USER_KEYSPACE.to_string(),
                    ByteView::from(target_user_id.to_bytes()),
                ),
                (
                    DOCUMENT_STATE_KEYSPACE.to_string(),
                    reducer_state_key(&admin_target),
                ),
                (
                    SYNC_REVISION_KEYSPACE.to_string(),
                    sync_revision_key(&document_target),
                ),
                (
                    REALM_CONFIG_KEYSPACE.to_string(),
                    ByteView::from(*self.input.actor.realm_id.as_bytes()),
                ),
            ]
            .into_iter()
            .chain(self.input.alias.map(|change| {
                let key = aruna_core::link::alias_claims_key(&change.alias());
                (FEDERATION_KEYSPACE.to_string(), ByteView::from(key))
            }))
            .chain(self.input.subject.iter().flat_map(|subject| {
                let key = subject_index_key(subject);
                let realm_id = self.input.actor.realm_id;
                let config = AdminDocumentTarget::RealmConfig { realm_id };
                [
                    (SUBJECT_CLAIMS_KEYSPACE.to_string(), key.clone()),
                    (SUBJECT_INDEX_KEYSPACE.to_string(), key),
                    (
                        DOCUMENT_STATE_KEYSPACE.to_string(),
                        reducer_state_key(&config),
                    ),
                ]
            }))
            .collect(),
        )))
    }

    fn emit_write_user(
        &mut self,
        user_value: Option<ByteView>,
        reducer_state_value: Option<ByteView>,
        revision_value: Option<ByteView>,
        realm_config_value: Option<ByteView>,
        extra: &[Option<ByteView>],
    ) -> Result<Next<UserWritten>, UpdateUserError> {
        let (alias_rows, subject_rows) = extra.split_at(usize::from(self.input.alias.is_some()));
        let claims_value = alias_rows.first().cloned().flatten();
        let current = user_value.ok_or(UpdateUserError::UserNotFound)?;
        let mut user = User::from_bytes(&current)?;
        if user.user_id != self.target_user_id {
            return Err(UpdateUserError::UserIdMismatch);
        }

        let realm_config = realm_config_value
            .as_deref()
            .map(RealmConfigDocument::from_bytes)
            .transpose()?;
        if (self.guard).is_some_and(|guard| !guard.admits(realm_config.as_ref())) {
            return Err(UpdateUserError::Superseded);
        }
        if let Some(AliasChange::Add {
            alias,
            issued_at,
            now,
        }) = self.input.alias
        {
            check_link(&user, realm_config.as_ref(), &alias, issued_at, now)?;
        }
        let was_deactivated = user.is_deactivated();
        apply_updates(&mut user, &self.input)?;
        let claims = self
            .input
            .alias
            .map(|change| apply_alias(&mut user, change, claims_value.as_deref()))
            .transpose()?;
        // An active user turning inactive ends READ; a repeat leaves the status unchanged.
        self.deactivating = !was_deactivated && user.is_deactivated();
        let admin_target = AdminDocumentTarget::User {
            user_id: user.user_id,
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
            .is_some_and(|state| state.target != admin_target)
        {
            return Err(AdminDocumentError::TargetMismatch.into());
        }
        let mut reducer_state = previous_reducer_state
            .clone()
            .unwrap_or_else(|| AdminDocumentState::new(admin_target));
        let mut admin_events = apply_reducer_updates(&mut reducer_state, &self.input)?;
        let subject_claims = match self.input.subject.as_ref() {
            Some(_) if !self.input.system => return Err(UpdateUserError::Unauthorized),
            Some(subject) => {
                let (rows, coordinator_row) = subject_rows.split_at(subject_rows.len().min(2));
                let state = (coordinator_row.first().and_then(|value| value.as_deref()))
                    .map(|bytes| {
                        aruna_core::reducer::decode_reducer_state(bytes)
                            .map_err(ConversionError::from)
                    })
                    .transpose()?;
                // Like a merge, an attach commits only on the pinned coordinator, so the two
                // never commit on two nodes at once.
                let pinned = Coordinator::Pinned(self.input.actor.node_id);
                if realm_config
                    .as_ref()
                    .map(|config| coordinator(config, state.as_ref()))
                    != Some(pinned)
                {
                    return Err(UpdateUserError::NotCoordinator);
                }
                attach_subject(&mut user, subject, rows)?
            }
            None => None,
        };
        if let (Some(subject), Some(_)) = (self.input.subject.as_ref(), subject_claims.as_ref()) {
            admin_events.push(reducer_state.apply_operation(
                &self.input.actor,
                AdminDocumentOperation::SubjectIdAdded {
                    subject_id: subject.clone(),
                },
            )?);
        }
        let previous_document_revision = revision_value
            .as_ref()
            .map(|value| {
                postcard::from_bytes::<DocumentChange>(value.as_ref())
                    .map_err(ConversionError::from)
            })
            .transpose()?;
        let document_target = DocumentTarget::User {
            user_id: user.user_id,
        };
        let placement = realm_config
            .as_ref()
            .map(|config| target_placement_ref(config, &document_target, Default::default()))
            .unwrap_or(PlacementRef::NIL);
        let realm_id = self.input.actor.realm_id;
        if let Some(config) = realm_config.as_ref() {
            self.fence.add(realm_id, config, [placement]);
        }
        let generation = self.fence.generation(&realm_id, &placement);
        let document_revision = local_sync_change(
            previous_document_revision.as_ref(),
            &self.input.actor,
            placement,
        );

        let bytes = user.reconcile_bytes(Some(&current), &self.input.actor)?;
        let mut stale_conflict_deletes =
            stale_conflict_deletes(previous_reducer_state.as_ref(), Some(&reducer_state));
        let mut writes = vec![
            (
                USER_KEYSPACE.to_string(),
                ByteView::from(user.user_id.to_bytes()),
                ByteView::from(bytes),
            ),
            reducer_state_entry(&reducer_state)?,
        ];
        writes.push(sync_revision_entry(&document_target, &document_revision)?);
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
        if let (Some(subject), Some(claims)) = (self.input.subject.as_ref(), subject_claims) {
            let key = subject_index_key(subject);
            let value = postcard::to_allocvec(&claims).map_err(ConversionError::from)?;
            writes.push((
                SUBJECT_CLAIMS_KEYSPACE.to_string(),
                key.clone(),
                value.into(),
            ));
            writes.push((
                SUBJECT_INDEX_KEYSPACE.to_string(),
                key,
                subject_index_value(user.user_id),
            ));
        }
        if let (Some(change), Some(claims)) = (self.input.alias, claims) {
            let key = ByteView::from(aruna_core::link::alias_claims_key(&change.alias()));
            if claims.is_empty() {
                stale_conflict_deletes.push((FEDERATION_KEYSPACE.to_string(), key));
            } else {
                let value = postcard::to_allocvec(&claims).map_err(ConversionError::from)?;
                writes.push((FEDERATION_KEYSPACE.to_string(), key, ByteView::from(value)));
            }
        }
        if let Some(audit) = &self.audit {
            writes.extend(audit.entries()?);
        }

        self.written = Some((user, !admin_events.is_empty()));
        self.deletes = stale_conflict_deletes;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// Takes the bucket's fence inside the transaction before committing, so a departing
    /// holder's close rejects or conflicts this write.
    fn after_writes(&mut self) -> Result<Next<UserWritten>, UpdateUserError> {
        if self.fence.is_empty() {
            return self.after_fence();
        }
        self.stage = Stage::Fence;
        Ok(Next::Call(IoCall::Read(self.fence.reads())))
    }

    fn after_fence(&mut self) -> Result<Next<UserWritten>, UpdateUserError> {
        if self.deactivating {
            self.stage = Stage::Marks;
            return Ok(Next::Call(self.marks.first()));
        }
        self.finish()
    }

    fn finish(&mut self) -> Result<Next<UserWritten>, UpdateUserError> {
        self.written
            .take()
            .map(Next::Done)
            .ok_or(UpdateUserError::NotFinished)
    }
}

impl Operation for UserWrite {
    type Output = UserWritten;
    type Error = UpdateUserError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<UserWritten>, UpdateUserError> {
        self.reads()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<UserWritten>, UpdateUserError> {
        match self.stage {
            Stage::Read => {
                let IoResult::Values(values) = result else {
                    return Err(UpdateUserError::NotFinished);
                };
                let mut values = values.into_iter();
                let mut take = || values.next().flatten();
                let (user, reducer, revision, config) = (take(), take(), take(), take());
                let extra: Vec<_> = values.collect();
                self.emit_write_user(user, reducer, revision, config, &extra)
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
                    return Err(UpdateUserError::NotFinished);
                };
                let keys = self.fence.reads().into_iter().map(|(_, key)| key);
                let rows: Vec<_> = keys.zip(values).collect();
                if !self.fence.admits(&rows) {
                    return Err(UpdateUserError::PlacementFenced);
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

pub(crate) fn local_sync_change(
    previous_change: Option<&DocumentChange>,
    actor: &Actor,
    placement: PlacementRef,
) -> DocumentChange {
    let updated_at_ms = current_timestamp_ms();
    let minimum_generation = previous_change
        .map(|change| change.current.generation.saturating_add(1))
        .unwrap_or_default();
    DocumentChange {
        base: previous_change.map(|change| change.current),
        current: DocumentSyncRevision {
            generation: updated_at_ms.max(minimum_generation),
            event_id: Ulid::generate(),
            actor: actor.node_id,
            updated_at_ms,
        },
        kind: DocumentChangeKind::Upsert,
        placement,
    }
}

fn apply_reducer_updates(
    state: &mut AdminDocumentState,
    input: &UpdateUserInput,
) -> Result<Vec<AdminDocumentEvent>, AdminDocumentError> {
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

    match input.alias {
        Some(AliasChange::Add { alias, .. }) => {
            operations.push(AdminDocumentOperation::UserAliasAdded { alias });
        }
        Some(AliasChange::Remove(alias)) => {
            operations.push(AdminDocumentOperation::UserAliasRemoved { alias });
        }
        None => {}
    }

    operations
}

/// Refuses a link when the account turned inactive, the login's realm is no longer admitted,
/// or the login was cut off after the confirmed session.
fn check_link(
    user: &User,
    config: Option<&RealmConfigDocument>,
    alias: &UserId,
    issued_at: u64,
    now: u64,
) -> Result<(), UpdateUserError> {
    if user.is_deactivated() || user.service_group().is_some() {
        return Err(UpdateUserError::AliasInactive);
    }
    let admitted = config
        .and_then(|config| config.federation.as_ref())
        .is_some_and(|settings| settings.accepted_realms.admits(&alias.realm_id));
    if !admitted {
        return Err(UpdateUserError::AliasNotAdmitted);
    }
    if config
        .and_then(|config| config.user_cutoff(alias, now))
        .is_some_and(|cutoff| issued_at < cutoff)
    {
        return Err(UpdateUserError::AliasCutOff);
    }
    Ok(())
}

/// Applies a link or unlink to the account and returns the login's new ownership claims. A
/// login another account claims, or a second login of the same realm, is refused.
fn apply_alias(
    user: &mut User,
    change: AliasChange,
    claims: Option<&[u8]>,
) -> Result<BTreeSet<UserId>, UpdateUserError> {
    let alias = change.alias();
    if alias.realm_id == user.user_id.realm_id || alias.is_nil() {
        return Err(UpdateUserError::InvalidAlias);
    }
    let mut claims = claims
        .map(postcard::from_bytes::<BTreeSet<UserId>>)
        .transpose()
        .map_err(ConversionError::from)?
        .unwrap_or_default();
    match change {
        AliasChange::Add { .. } => {
            if claims.iter().any(|owner| *owner != user.user_id) {
                return Err(UpdateUserError::AliasClaimed);
            }
            let same_realm = user
                .alias_user_ids
                .iter()
                .any(|linked| *linked != alias && linked.realm_id == alias.realm_id);
            if same_realm {
                return Err(UpdateUserError::AliasRealmTaken);
            }
            user.alias_user_ids.insert(alias);
            claims.insert(user.user_id);
        }
        AliasChange::Remove(_) => {
            if !user.alias_user_ids.remove(&alias) {
                return Err(UpdateUserError::AliasMissing);
            }
            claims.remove(&user.user_id);
        }
    }
    Ok(claims)
}

/// Adds `subject` to the account and returns its new claims, or `None` when it already belongs
/// to the account. A subject any other account claims is refused.
fn attach_subject(
    user: &mut User,
    subject: &str,
    rows: &[Option<ByteView>],
) -> Result<Option<BTreeSet<UserId>>, UpdateUserError> {
    let [claims, index] = rows else {
        return Err(UpdateUserError::SubjectClaimed);
    };
    let claims = match (claims, index) {
        (Some(bytes), _) => {
            postcard::from_bytes::<BTreeSet<UserId>>(bytes).map_err(ConversionError::from)?
        }
        (None, Some(bytes)) => BTreeSet::from([UserId::from_storage_key(bytes)?]),
        (None, None) => BTreeSet::new(),
    };
    // A repeat leaves the account unchanged, whatever stale claim another node's merge left.
    if user.subject_ids.iter().any(|known| known == subject) {
        return Ok(None);
    }
    if claims.iter().any(|owner| *owner != user.user_id) {
        return Err(UpdateUserError::SubjectClaimed);
    }
    user.subject_ids.push(subject.to_string());
    Ok(Some(BTreeSet::from([user.user_id])))
}

fn apply_updates(user: &mut User, input: &UpdateUserInput) -> Result<(), UpdateUserError> {
    if let Some(name) = input.name.as_ref() {
        let trimmed = name.trim();
        if trimmed.is_empty() || trimmed.len() > MAX_USER_LEN {
            return Err(UpdateUserError::InvalidUserName);
        }
        user.name = trimmed.to_string();
    }

    let reserved = input
        .remove_attributes
        .iter()
        .chain(input.set_attributes.keys())
        .find(|key| is_reserved_attribute(key));
    if let Some(key) = reserved
        && !input.system
    {
        return Err(UpdateUserError::InvalidAttributeKey(key.clone()));
    }
    let mut removals = HashSet::new();
    for key in &input.remove_attributes {
        validate_attribute_key(key)?;
        removals.insert(key.clone());
    }
    for key in removals {
        user.attributes.remove(&key);
    }

    for (key, value) in &input.set_attributes {
        validate_attribute_key(key)?;
        validate_attribute_value(key, value)?;
        user.attributes.insert(key.clone(), value.clone());
    }

    validate_attribute_count(counted_attributes(user.attributes.keys()))?;

    Ok(())
}

#[cfg(test)]
mod pure_tests {
    use super::UserWritten;
    use super::{Step, UpdateUserError, UpdateUserInput, UpdateUserOperation, UserWrite};
    use aruna_core::UserId;
    use aruna_core::admin_documents::{AdminDocumentClock, AdminDocumentDot, AdminDocumentTarget};
    use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
    use aruna_core::document::{
        DocumentChange, DocumentChangeKind, DocumentOutboxEvent, DocumentOutboxRecord,
        DocumentSyncRevision, DocumentTarget,
    };
    use aruna_core::errors::AuthorizationError;
    use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
    use aruna_core::operation::{Operation, TransactionMode};
    use aruna_core::reducer::{AdminConflict, AdminConflictValue, AdminDocumentState};
    use aruna_core::storage_entries::{reducer_conflict_key, reducer_state_key, sync_revision_key};
    use aruna_core::structs::identity::auth::{Actor, AuthContext};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::identity::user::User;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::types::{Key, KeySpace, Value};
    use aruna_core::{
        DOCUMENT_CONFLICT_KEYSPACE, DOCUMENT_STATE_KEYSPACE, SYNC_OUTBOX_KEYSPACE,
        SYNC_REVISION_KEYSPACE, USER_KEYSPACE,
    };
    use byteview::ByteView;
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use ulid::Ulid;

    type Turn = Result<Next<UserWritten>, UpdateUserError>;

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
            session: None,
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
            system: false,
            alias: None,
            subject: None,
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

    fn conflict(path: &str, first_seed: u8, second_seed: u8) -> AdminConflict {
        AdminConflict {
            path: path.to_string(),
            values: vec![
                AdminConflictValue {
                    value: Some(format!("value-{first_seed}")),
                    dot: dot(first_seed),
                },
                AdminConflictValue {
                    value: Some(format!("value-{second_seed}")),
                    dot: dot(second_seed),
                },
            ],
        }
    }

    fn conflict_state(user_id: UserId) -> AdminDocumentState {
        let name_first = dot(11);
        let name_second = dot(12);
        let title_first = dot(13);
        let title_second = dot(14);
        let mut clock = AdminDocumentClock::default();
        for dot in [name_first, name_second, title_first, title_second] {
            clock.advance(dot.origin_node_id, dot.origin_seq);
        }

        AdminDocumentState {
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
            revocation_floor: 0,
            revocation_next_expiry: None,
        }
    }

    /// The user transaction of `input`, as the update runs it after its checks.
    fn write(input: UpdateUserInput) -> UserWrite {
        let target = UserId::from_string(&input.user_id).unwrap();
        UserWrite::new(input, target)
    }

    /// The transaction of `input` answered with the stored `user` and these other rows.
    fn read_user(
        input: UpdateUserInput,
        user: &User,
        rows: Vec<Option<Value>>,
    ) -> (UserWrite, Turn) {
        let mut operation = write(input);
        operation.first().unwrap();
        let owner = actor(user.user_id.realm_id, user.user_id);
        let mut values = vec![Some(user.to_bytes(&owner).unwrap().into())];
        values.extend(rows);
        let turn = operation.next(IoResult::Values(values));
        (operation, turn)
    }

    fn writes(turn: Turn) -> Vec<(KeySpace, Key, Value)> {
        match turn {
            Ok(Next::Call(IoCall::Write(writes))) => writes,
            other => panic!("expected the user write, got {other:?}"),
        }
    }

    fn row<'a>(writes: &'a [(KeySpace, Key, Value)], space: &str) -> &'a Value {
        &writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == space)
            .unwrap()
            .2
    }

    #[test]
    fn stale_claim_ignored() {
        // A merge elsewhere may leave the merged account's claim here for a while.
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let owner = UserId::local(Ulid::from_bytes([4u8; 16]), realm_id);
        let merged = UserId::local(Ulid::from_bytes([5u8; 16]), realm_id);
        let mut user = stored_user(owner);
        user.subject_ids.push("subject".to_string());
        let claims = postcard::to_allocvec(&BTreeSet::from([owner, merged])).unwrap();
        let rows = [Some(ByteView::from(claims)), None];
        assert_eq!(super::attach_subject(&mut user, "subject", &rows), Ok(None));
        assert_eq!(
            super::attach_subject(&mut user, "other", &rows),
            Err(UpdateUserError::SubjectClaimed)
        );
    }

    #[test]
    fn attach_needs_coordinator() {
        // An attach commits only on the pinned merge coordinator, whatever routed it here.
        use aruna_core::admin_documents::AdminDocumentOperation;
        use aruna_core::storage_entries::reducer_state_entry;
        use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([4u8; 16]), realm_id);
        let local = actor(realm_id, user_id);
        let other = node(5);
        let mut realm = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        realm.ensure_node(local.node_id, RealmNodeKind::Management);
        realm.ensure_node(other, RealmNodeKind::Management);
        let realm_bytes = ByteView::from(realm.to_bytes(&local).unwrap());
        let pinned = |node_id| {
            let config = AdminDocumentTarget::RealmConfig { realm_id };
            let mut state = AdminDocumentState::new(config);
            let pin = AdminDocumentOperation::MergeCoordinatorSet { node_id };
            state.apply_operation(&local, pin).unwrap();
            reducer_state_entry(&state).unwrap().2
        };
        for (coordinator, refused) in [(other, true), (local.node_id, false)] {
            let request = UpdateUserInput {
                name: None,
                set_attributes: HashMap::new(),
                remove_attributes: Vec::new(),
                system: true,
                subject: Some("subject".to_string()),
                ..input(realm_id, user_id, user_id)
            };
            let rows = vec![
                None,
                None,
                Some(realm_bytes.clone()),
                None,
                None,
                Some(pinned(coordinator)),
            ];
            let (_, turn) = read_user(request, &stored_user(user_id), rows);
            if refused {
                assert_eq!(turn, Err(UpdateUserError::NotCoordinator));
            } else {
                assert!(matches!(turn, Ok(Next::Call(IoCall::Write(_)))));
            }
        }
    }

    #[test]
    fn updates_user_attributes() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let original = stored_user(user_id);
        // A caller updating itself needs no permission check.
        let mut checked = UpdateUserOperation::new(input(realm_id, user_id, user_id));
        assert!(matches!(checked.first(), Ok(Next::Call(IoCall::Child(_)))));

        let mut operation = write(input(realm_id, user_id, user_id));
        assert_eq!(operation.transaction(), TransactionMode::Write);
        let target = AdminDocumentTarget::User { user_id };
        let document = DocumentTarget::User { user_id };
        let Ok(Next::Call(IoCall::Read(reads))) = operation.first() else {
            panic!("expected the user reads");
        };
        assert_eq!(reads.len(), 4);
        assert_eq!(reads[0].0, USER_KEYSPACE);
        assert_eq!(reads[0].1.as_ref(), user_id.to_bytes().as_slice());
        assert_eq!(reads[1].0, DOCUMENT_STATE_KEYSPACE);
        assert_eq!(reads[1].1, reducer_state_key(&target));
        assert_eq!(reads[2].0, SYNC_REVISION_KEYSPACE);
        assert_eq!(reads[2].1, sync_revision_key(&document));
        assert_eq!(reads[3].0, REALM_CONFIG_KEYSPACE);

        let values = vec![
            Some(original.to_bytes(&actor(realm_id, user_id)).unwrap().into()),
            None,
            None,
            None,
        ];
        let writes = writes(operation.next(IoResult::Values(values)));
        assert!(
            writes
                .iter()
                .all(|(keyspace, _, _)| keyspace != DOCUMENT_CONFLICT_KEYSPACE)
        );
        let outbox_records: Vec<DocumentOutboxRecord> = writes
            .iter()
            .filter(|(keyspace, _, _)| keyspace == SYNC_OUTBOX_KEYSPACE)
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
            DocumentOutboxEvent::AdminOperation { event, .. } if matches!(
                &event.op,
                aruna_core::admin_documents::AdminDocumentOperation::UserNameSet { .. }
            )
        )));
        let updated = User::from_bytes(row(&writes, USER_KEYSPACE)).unwrap();
        let reducer_state: AdminDocumentState =
            postcard::from_bytes(row(&writes, DOCUMENT_STATE_KEYSPACE)).unwrap();
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
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Done((updated.clone(), true)))
        );

        // With admin events queued, the outbox drain wakes after the commit.
        checked.step = Step::Write;
        let written = ChildOutcome::new(Ok::<_, UpdateUserError>((updated.clone(), true)));
        let Ok(Next::Then { output, calls }) = checked.next(IoResult::Child(written)) else {
            panic!("expected the user with the drain wake");
        };
        assert_eq!(output, updated);
        assert!(matches!(calls.as_slice(), [IoCall::Effect(_)]));
    }

    #[test]
    fn writes_sync_sidecar() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let request = input(realm_id, user_id, user_id);
        let expected_actor = request.actor.clone();
        let document_target = DocumentTarget::User { user_id };
        let previous_revision = DocumentChange {
            base: None,
            current: document_revision(21, 42),
            kind: DocumentChangeKind::Upsert,
            placement: PlacementRef::NIL,
        };
        let previous = Some(postcard::to_allocvec(&previous_revision).unwrap().into());
        let (_, turn) = read_user(request, &stored_user(user_id), vec![None, previous, None]);
        let writes = writes(turn);
        let updated = User::from_bytes(row(&writes, USER_KEYSPACE)).unwrap();
        assert_eq!(updated.name, "Alice Updated");

        let (revision_key, revision): (_, DocumentChange) = writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == SYNC_REVISION_KEYSPACE)
            .map(|(_, key, value)| {
                (
                    key,
                    postcard::from_bytes(value).expect("revision sidecar decodes"),
                )
            })
            .expect("revision sidecar write exists");
        assert_eq!(revision_key, &sync_revision_key(&document_target));
        assert_eq!(revision.base, Some(previous_revision.current));
        assert_eq!(revision.current.actor, expected_actor.node_id);
        assert!(revision.current.generation > previous_revision.current.generation);
        assert!(revision.current.updated_at_ms > 0);
        assert_eq!(revision.kind, DocumentChangeKind::Upsert);
    }

    #[test]
    fn audit_shares_transaction() {
        use aruna_core::keyspaces::ADMIN_AUDIT_KEYSPACE;
        use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord};
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let original = stored_user(user_id);
        let mut record =
            AdminAuditRecord::new(AdminAuditAction::AccountDeactivated, user_id, user_id);
        record.group_id = Some(Ulid::from_bytes([4u8; 16]));
        let mut operation = write(input(realm_id, user_id, user_id));
        operation.audit = Some(record.clone());
        operation.first().unwrap();
        let values = vec![
            Some(original.to_bytes(&actor(realm_id, user_id)).unwrap().into()),
            None,
            None,
            None,
        ];
        let writes = writes(operation.next(IoResult::Values(values)));
        assert!(writes.iter().any(|row| row.0 == USER_KEYSPACE));
        let audit: Vec<_> = writes
            .iter()
            .filter(|row| row.0 == ADMIN_AUDIT_KEYSPACE)
            .collect();
        assert_eq!(audit.len(), 2, "one realm row and one group row");
        assert_eq!(AdminAuditRecord::from_bytes(&audit[0].2).unwrap(), record);
    }

    #[test]
    fn writes_reducer_conflicts() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let previous_state = conflict_state(user_id);
        let target = AdminDocumentTarget::User { user_id };
        let previous = Some(postcard::to_allocvec(&previous_state).unwrap().into());
        let request = input(realm_id, user_id, user_id);
        let (mut operation, turn) =
            read_user(request, &stored_user(user_id), vec![previous, None, None]);
        let writes = writes(turn);
        let conflict_writes: Vec<_> = writes
            .iter()
            .filter(|(keyspace, _, _)| keyspace == DOCUMENT_CONFLICT_KEYSPACE)
            .collect();
        assert_eq!(conflict_writes.len(), 1);
        let conflict: AdminConflict = postcard::from_bytes(conflict_writes[0].2.as_ref()).unwrap();
        assert_eq!(conflict.path, "user.attributes.title");
        let updated = User::from_bytes(row(&writes, USER_KEYSPACE)).unwrap();
        let reducer_state: AdminDocumentState =
            postcard::from_bytes(row(&writes, DOCUMENT_STATE_KEYSPACE)).unwrap();

        assert_eq!(updated.name, "Alice Updated");
        assert_eq!(reducer_state.materialized_user_name(), Some(updated.name));
        assert_eq!(reducer_state.conflicts.len(), 1);
        assert!(
            reducer_state
                .conflicts
                .contains_key("user.attributes.title")
        );
        assert!(!reducer_state.conflicts.contains_key("user.name"));

        let deletes = vec![(
            DOCUMENT_CONFLICT_KEYSPACE.to_string(),
            reducer_conflict_key(&target, "user.name"),
        )];
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Call(IoCall::Delete(deletes)))
        );
        assert!(matches!(operation.next(IoResult::Done), Ok(Next::Done(_))));
    }

    #[test]
    fn rejects_attribute_key() {
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
    fn deactivation_marks_due() {
        // Turning inactive marks encrypted buckets due inside the user transaction; a repeat
        // does not.
        use aruna_core::user::validation::DEACTIVATED_ATTRIBUTE;
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let run = |stored: User| {
            let mut request = input(realm_id, user_id, user_id);
            request.set_attributes =
                HashMap::from([(DEACTIVATED_ATTRIBUTE.to_string(), "true".to_string())]);
            request.remove_attributes.clear();
            request.system = true;
            let (mut operation, turn) = read_user(request, &stored, vec![None, None, None]);
            assert!(matches!(turn, Ok(Next::Call(IoCall::Write(_)))));
            operation.next(IoResult::Done)
        };

        assert!(matches!(
            run(stored_user(user_id)),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        let mut inactive = stored_user(user_id);
        inactive
            .attributes
            .insert(DEACTIVATED_ATTRIBUTE.to_string(), "true".to_string());
        assert!(matches!(run(inactive), Ok(Next::Done(_))));
    }

    #[test]
    fn reserved_needs_system() {
        // Account status and service ownership change only through their own operations.
        use aruna_core::user::validation::DEACTIVATED_ATTRIBUTE;
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let mut input = input(realm_id, user_id, user_id);
        input.remove_attributes = vec![DEACTIVATED_ATTRIBUTE.to_string()];
        input.set_attributes.clear();
        let mut user = stored_user(user_id);
        user.attributes
            .insert(DEACTIVATED_ATTRIBUTE.to_string(), "true".to_string());

        assert_eq!(
            super::apply_updates(&mut user, &input),
            Err(UpdateUserError::InvalidAttributeKey(
                DEACTIVATED_ATTRIBUTE.to_string()
            ))
        );
        input.system = true;
        assert_eq!(super::apply_updates(&mut user, &input), Ok(()));
        assert!(!user.is_deactivated());
    }

    #[test]
    fn unauthorized_update_fails() {
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let caller_id = UserId::local(Ulid::from_bytes([4u8; 16]), realm_id);
        let user_id = UserId::local(Ulid::from_bytes([5u8; 16]), realm_id);
        let mut operation = UpdateUserOperation::new(input(realm_id, caller_id, user_id));
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let denied = ChildOutcome::new(Ok::<bool, AuthorizationError>(false));
        assert_eq!(
            operation.next(IoResult::Child(denied)),
            Err(UpdateUserError::Unauthorized)
        );
    }

    #[test]
    fn rejects_scoped_update() {
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([7u8; 16]), realm_id);
        let mut input = input(realm_id, user_id, user_id);
        input.auth_context.path_restrictions = Some(Vec::new());
        let mut operation = UpdateUserOperation::new(input);
        assert_eq!(operation.first(), Err(UpdateUserError::Unauthorized));
    }

    /// A realm whose buckets are activated at generation one, so an update
    /// resolves a generation and takes the user bucket's fence.
    fn activated_config(
        realm_id: RealmId,
        actor: &Actor,
    ) -> aruna_core::structs::identity::realm::RealmConfigDocument {
        let mut config =
            aruna_core::structs::identity::realm::RealmConfigDocument::new(realm_id, Vec::new(), 3);
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

    /// Runs a user transaction to the bucket fence read and answers it with `closed`.
    fn step_to_fence(realm_id: RealmId, user_id: UserId, closed: Option<u64>) -> Turn {
        let caller = actor(realm_id, user_id);
        let config = activated_config(realm_id, &caller);
        let rows = vec![None, None, Some(config.to_bytes(&caller).unwrap().into())];
        let request = input(realm_id, user_id, user_id);
        let (mut operation, turn) = read_user(request, &stored_user(user_id), rows);
        assert!(matches!(turn, Ok(Next::Call(IoCall::Write(_)))));
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

    #[test]
    fn update_takes_fence() {
        // An admitted user update commits only after taking the bucket's fence.
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        assert!(matches!(
            step_to_fence(realm_id, user_id, None),
            Ok(Next::Done(_))
        ));
    }

    #[test]
    fn fence_rejects_user() {
        // The departing holder closed generation one: the update must not
        // commit a row the drained bucket can no longer publish.
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        assert_eq!(
            step_to_fence(realm_id, user_id, Some(1)),
            Err(UpdateUserError::PlacementFenced)
        );
    }
}
