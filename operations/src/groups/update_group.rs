//! Renames a group, leaving its id and every permission path untouched.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::{DOCUMENT_STATE_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode, boxed_retry};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, DISPLAY_NAME_PATH};
use aruna_core::rows::{done, values};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::group::Group;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::dataset_location::DatasetLocation;
use aruna_core::types::{GroupId, Key, KeySpace, Value};
use byteview::ByteView;
use thiserror::Error;
use tracing::warn;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::placement::target_placement_ref;

pub const MAX_NAME_LEN: usize = 256;

/// The one group-name rule: trimmed, non-empty and at most
/// `MAX_NAME_LEN` bytes. `None` means the name is refused.
pub fn normalize_group_name(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    (!trimmed.is_empty() && trimmed.len() <= MAX_NAME_LEN).then(|| trimmed.to_string())
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateGroupConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted (delegated)
    /// credential stays restricted; it is never derived from `actor`.
    pub auth_context: AuthContext,
    pub group_id: GroupId,
    pub change: GroupChange,
}

/// What an update changes: the display name or the default dataset location.
#[derive(Debug, Clone, PartialEq)]
pub enum GroupChange {
    Name(String),
    Location(Option<DatasetLocation>),
}

/// Renames a group after creation. Only the label changes: the group id and
/// every permission path, bucket and dataset stay as they are.
#[derive(Debug, PartialEq)]
pub struct UpdateGroupOperation {
    config: UpdateGroupConfig,
    state: UpdateGroupState,
}

/// Which check or write the update waits for.
#[derive(Debug, Clone, Copy, PartialEq)]
enum UpdateGroupState {
    AuthGroupAdmin,
    AuthRealmAdmin,
    Write,
}

#[derive(Debug, Error, PartialEq)]
pub enum UpdateGroupError {
    #[error("Unauthorized")]
    Unauthorized,
    #[error("group not found")]
    GroupNotFound,
    #[error("group name must be non-empty and at most {MAX_NAME_LEN} bytes")]
    InvalidDisplayName,
    #[error("the dataset location has an invalid bucket name or prefix")]
    InvalidLocation,
    #[error("stored group id does not match the requested group id")]
    GroupIdMismatch,
    #[error("the group's bucket cut over to a new holder set; retry the rename")]
    PlacementFenced,
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentError(#[from] AdminDocumentError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
    #[error("update group did not finish")]
    NotFinished,
}

impl From<IoError> for UpdateGroupError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "UpdateGroup".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

type UpdateTurn = Result<Next<Group>, UpdateGroupError>;

/// The admin event the change applies; an invalid name or location is refused.
fn admin_change(config: &UpdateGroupConfig) -> Result<AdminDocumentOperation, UpdateGroupError> {
    match &config.change {
        GroupChange::Name(name) => normalize_group_name(name)
            .map(|display_name| AdminDocumentOperation::DisplayNameSet { display_name })
            .ok_or(UpdateGroupError::InvalidDisplayName),
        GroupChange::Location(Some(location)) if !location.valid() => {
            Err(UpdateGroupError::InvalidLocation)
        }
        GroupChange::Location(location) => Ok(AdminDocumentOperation::GroupLocationSet {
            location: location.clone(),
        }),
    }
}

impl UpdateGroupOperation {
    pub fn new(config: UpdateGroupConfig) -> Self {
        Self {
            config,
            state: UpdateGroupState::AuthGroupAdmin,
        }
    }

    fn check_permission(&self, path: String) -> Next<Group> {
        Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path,
                required_permission: Permission::WRITE,
            },
        )))
    }

    /// The write reruns from its read after a lost commit, which also rereads the realm config.
    fn write(&mut self) -> Next<Group> {
        self.state = UpdateGroupState::Write;
        let config = self.config.clone();
        let seed = self.config.actor.user_id.to_bytes();
        let build = move || GroupUpdate::new(config.clone());
        let child = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        Next::Call(IoCall::Child(child))
    }

    fn handle_auth(&mut self, allowed: Result<bool, AuthorizationError>) -> UpdateTurn {
        match allowed {
            Ok(true) => Ok(self.write()),
            // Realm admins may rename a group they are not a member of, so a refused
            // group-admin check falls back to the realm group administration path.
            Ok(false) if self.state == UpdateGroupState::AuthGroupAdmin => {
                self.state = UpdateGroupState::AuthRealmAdmin;
                let path = format!("/{}/admin/groups", self.config.actor.realm_id);
                Ok(self.check_permission(path))
            }
            Ok(false) => Err(UpdateGroupError::Unauthorized),
            Err(error) => {
                warn!(error = %error, "Group rename authorization check failed");
                match error {
                    AuthorizationError::StorageError(error) => Err(error.into()),
                    _ => Err(UpdateGroupError::Unauthorized),
                }
            }
        }
    }
}

impl Operation for UpdateGroupOperation {
    type Output = Group;
    type Error = UpdateGroupError;

    fn first(&mut self) -> UpdateTurn {
        if self.config.auth_context.realm_id != self.config.actor.realm_id
            || self.config.auth_context.user_id != self.config.actor.user_id
        {
            return Err(UpdateGroupError::Unauthorized);
        }
        admin_change(&self.config)?;
        Ok(self.check_permission(format!(
            "/{}/g/{}/admin",
            self.config.actor.realm_id, self.config.group_id
        )))
    }

    fn next(&mut self, result: IoResult) -> UpdateTurn {
        match self.state {
            UpdateGroupState::AuthGroupAdmin | UpdateGroupState::AuthRealmAdmin => {
                let allowed = result.child::<bool, AuthorizationError>()?;
                self.handle_auth(allowed)
            }
            UpdateGroupState::Write => result.child::<Group, UpdateGroupError>()?.map(Next::Done),
        }
    }
}

/// What the rename transaction waits for.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Stage {
    Read,
    Write,
    Delete,
    Fence,
}

/// Reads the group, its admin state and the realm config, then stores the change and its queued
/// admin event, and takes the bucket fence, in one write transaction.
#[derive(Debug, PartialEq)]
struct GroupUpdate {
    config: UpdateGroupConfig,
    /// Bucket the group rows publish onto, read inside the write transaction.
    fence: aruna_core::placement::fence::WriteFence,
    stage: Stage,
    group: Option<Group>,
    deletes: Vec<(KeySpace, Key)>,
}

impl GroupUpdate {
    fn new(config: UpdateGroupConfig) -> Self {
        Self {
            config,
            fence: Default::default(),
            stage: Stage::Read,
            group: None,
            deletes: Vec::new(),
        }
    }

    fn document_ref(&self) -> DocumentTarget {
        DocumentTarget::GroupAuthorization {
            group_id: self.config.group_id,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::Group {
            group_id: self.config.group_id,
        }
    }

    fn emit_write_state(
        &mut self,
        group_value: Option<Value>,
        reducer_state_value: Option<Value>,
        realm_config_value: Option<Value>,
    ) -> UpdateTurn {
        let change = admin_change(&self.config)?;
        let group_value = group_value.ok_or(UpdateGroupError::GroupNotFound)?;
        let mut group = Group::from_bytes(&group_value)?;
        if group.group_id != self.config.group_id || group.realm_id != self.config.actor.realm_id {
            return Err(UpdateGroupError::GroupIdMismatch);
        }

        let target = self.admin_target();
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
        let admin_event = reducer_state.apply_operation(&self.config.actor, change.clone())?;
        if let AdminDocumentOperation::DisplayNameSet { display_name } = change {
            group.display_name = display_name;
        }
        overlay_reducer_name(&mut group, &reducer_state);

        let stale_conflict_deletes =
            stale_conflict_deletes(previous_reducer_state.as_ref(), Some(&reducer_state));
        let document_target = self.document_ref();
        let realm_config = realm_config_value
            .as_deref()
            .map(RealmConfigDocument::from_bytes)
            .transpose()?;
        let placement = realm_config
            .as_ref()
            .map(|config| target_placement_ref(config, &document_target, Default::default()))
            .unwrap_or(PlacementRef::NIL);
        let realm_id = self.config.actor.realm_id;
        if let Some(config) = realm_config.as_ref() {
            self.fence.add(realm_id, config, [placement]);
        }
        let mut writes = vec![
            (
                GROUP_KEYSPACE.to_string(),
                ByteView::from(group.group_id.to_bytes()),
                group.to_bytes(&self.config.actor)?.into(),
            ),
            reducer_state_entry(&reducer_state)?,
        ];
        let record = new_identified_record(
            admin_event.event_id,
            self.config.actor.node_id,
            document_target,
            Vec::new(),
            DocumentOutboxEvent::admin(admin_event),
            placement,
            false,
        )
        .fenced_at(self.fence.generation(&realm_id, &placement));
        writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        writes.extend(conflict_write_entries(&reducer_state)?);

        self.group = Some(group);
        self.deletes = stale_conflict_deletes;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// Takes the bucket's fence inside the transaction before committing, so a departing holder's
    /// close rejects or conflicts this write.
    fn fence_or_finish(&mut self) -> UpdateTurn {
        if self.fence.is_empty() {
            return self.finish();
        }
        self.stage = Stage::Fence;
        Ok(Next::Call(IoCall::Read(self.fence.reads())))
    }

    /// A failed drain timer leaves the durable outbox retryable.
    fn finish(&mut self) -> UpdateTurn {
        let output = self.group.take().ok_or(UpdateGroupError::NotFinished)?;
        let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        Ok(Next::Then { output, calls })
    }
}

impl Operation for GroupUpdate {
    type Output = Group;
    type Error = UpdateGroupError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn conflict_retries() -> u32 {
        super::CONFLICT_RETRIES as u32
    }

    fn first(&mut self) -> UpdateTurn {
        let target = self.admin_target();
        Ok(Next::Call(IoCall::Read(vec![
            (
                GROUP_KEYSPACE.to_string(),
                ByteView::from(self.config.group_id.to_bytes()),
            ),
            (
                DOCUMENT_STATE_KEYSPACE.to_string(),
                reducer_state_key(&target),
            ),
            (
                REALM_CONFIG_KEYSPACE.to_string(),
                ByteView::from(*self.config.actor.realm_id.as_bytes()),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> UpdateTurn {
        match self.stage {
            Stage::Read => {
                let [group, reducer, config] = values::<3>(result)?;
                self.emit_write_state(group, reducer, config)
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
                self.fence_or_finish()
            }
            Stage::Fence => {
                let IoResult::Values(values) = result else {
                    return Err(UpdateGroupError::NotFinished);
                };
                let keys = self.fence.reads().into_iter().map(|(_, key)| key);
                let rows: Vec<_> = keys.zip(values).collect();
                if !self.fence.admits(&rows) {
                    return Err(UpdateGroupError::PlacementFenced);
                }
                self.finish()
            }
        }
    }
}

/// Keeps the stored name when the reducer withheld the field on a conflict, so
/// a concurrent rename never blanks the group label (decision Q2).
fn overlay_reducer_name(group: &mut Group, reducer_state: &AdminDocumentState) {
    if !reducer_state.conflicts.contains_key(DISPLAY_NAME_PATH)
        && let Some(display_name) = reducer_state.materialized_group_name()
    {
        group.display_name = display_name;
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{GroupUpdate, UpdateGroupConfig, UpdateGroupError, UpdateGroupOperation};
    use aruna_core::UserId;
    use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
    use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
    use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord, DocumentTarget};
    use aruna_core::errors::{AuthorizationError, StorageError};
    use aruna_core::keyspaces::{
        DOCUMENT_STATE_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, SYNC_OUTBOX_KEYSPACE,
    };
    use aruna_core::operation::{Operation, TransactionMode};
    use aruna_core::reducer::AdminDocumentState;
    use aruna_core::storage_entries::reducer_state_key;
    use aruna_core::structs::identity::auth::{Actor, AuthContext};
    use aruna_core::structs::identity::group::Group;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::types::GroupId;
    use byteview::ByteView;
    use ulid::Ulid;

    fn realm() -> RealmId {
        RealmId::from_bytes([2u8; 32])
    }

    fn caller() -> UserId {
        UserId::local(Ulid::from_bytes([3u8; 16]), realm())
    }

    fn group() -> GroupId {
        Ulid::from_bytes([4u8; 16])
    }

    fn actor() -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
            user_id: caller(),
            realm_id: realm(),
        }
    }

    fn config(display_name: &str) -> UpdateGroupConfig {
        UpdateGroupConfig {
            actor: actor(),
            auth_context: AuthContext {
                user_id: caller(),
                realm_id: realm(),
                path_restrictions: None,
                session: None,
            },
            group_id: group(),
            change: super::GroupChange::Name(display_name.to_string()),
        }
    }

    fn stored_group() -> Group {
        Group {
            display_name: "Engineering".to_string(),
            group_id: group(),
            realm_id: realm(),
            roles: Default::default(),
            owner: caller(),
        }
    }

    /// The group row, no admin state yet, and no realm config.
    fn read_values() -> Vec<Option<ByteView>> {
        vec![
            Some(stored_group().to_bytes(&actor()).unwrap().into()),
            None,
            None,
        ]
    }

    fn authorize(
        operation: &mut UpdateGroupOperation,
        allowed: bool,
    ) -> Result<Next<Group>, UpdateGroupError> {
        let checked = Ok::<_, AuthorizationError>(allowed);
        operation.next(IoResult::Child(ChildOutcome::new(checked)))
    }

    fn checked(name: &str) -> UpdateGroupOperation {
        let mut operation = UpdateGroupOperation::new(config(name));
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        operation
    }

    #[test]
    fn renames_and_queues() {
        let mut operation = checked("  Platform  ");
        assert!(matches!(
            authorize(&mut operation, true),
            Ok(Next::Call(IoCall::Child(_)))
        ));

        let mut write = GroupUpdate::new(config("  Platform  "));
        assert_eq!(write.transaction(), TransactionMode::Write);
        let Ok(Next::Call(IoCall::Read(reads))) = write.first() else {
            panic!("expected the group reads");
        };
        let target = AdminDocumentTarget::Group { group_id: group() };
        assert_eq!(reads.len(), 3);
        assert_eq!(reads[0].0, GROUP_KEYSPACE);
        assert_eq!(reads[0].1.as_ref(), group().to_bytes().as_slice());
        assert_eq!(reads[1].0, DOCUMENT_STATE_KEYSPACE);
        assert_eq!(reads[1].1, reducer_state_key(&target));
        assert_eq!(reads[2].0, REALM_CONFIG_KEYSPACE);

        let written = write.next(IoResult::Values(read_values()));
        let document = DocumentTarget::GroupAuthorization { group_id: group() };
        match written {
            Ok(Next::Call(IoCall::Write(writes))) => {
                let group_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == GROUP_KEYSPACE)
                    .expect("group row is rewritten");
                let renamed = Group::from_bytes(group_write.2.as_ref()).unwrap();
                assert_eq!(renamed.display_name, "Platform");
                assert_eq!(renamed.group_id, stored_group().group_id);
                assert_eq!(renamed.owner, stored_group().owner);

                let reducer_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == DOCUMENT_STATE_KEYSPACE)
                    .expect("reducer state is written");
                let reducer_state: AdminDocumentState =
                    postcard::from_bytes(reducer_write.2.as_ref()).unwrap();
                assert_eq!(
                    reducer_state.materialized_group_name().as_deref(),
                    Some("Platform")
                );

                let records: Vec<DocumentOutboxRecord> = writes
                    .iter()
                    .filter(|(keyspace, _, _)| keyspace == SYNC_OUTBOX_KEYSPACE)
                    .map(|(_, _, value)| postcard::from_bytes(value).unwrap())
                    .collect();
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].target, document);
                assert!(matches!(
                    &records[0].event,
                    DocumentOutboxEvent::AdminOperation { event, .. }
                        if matches!(
                            &event.op,
                            AdminDocumentOperation::DisplayNameSet { display_name }
                                if display_name == "Platform"
                        )
                ));
            }
            _ => panic!("expected the group writes"),
        }

        let Ok(Next::Then { output, calls }) = write.next(IoResult::Done) else {
            panic!("expected the renamed group with the drain wake");
        };
        assert_eq!(output.display_name, "Platform");
        assert!(matches!(calls.as_slice(), [IoCall::Effect(_)]));
    }

    #[test]
    fn realm_admin_fallback() {
        // A realm admin who is not a group member renames through the realm path.
        let mut operation = checked("Platform");
        assert!(matches!(
            authorize(&mut operation, false),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert!(matches!(
            authorize(&mut operation, true),
            Ok(Next::Call(IoCall::Child(_)))
        ));
    }

    #[test]
    fn rejects_unauthorized_actor() {
        let mut operation = checked("Platform");
        authorize(&mut operation, false).unwrap();
        assert_eq!(
            authorize(&mut operation, false).err(),
            Some(UpdateGroupError::Unauthorized)
        );
    }

    #[test]
    fn rejects_invalid_name() {
        for name in ["   ", &"n".repeat(257)] {
            let mut operation = UpdateGroupOperation::new(config(name));
            assert_eq!(
                operation.first().err(),
                Some(UpdateGroupError::InvalidDisplayName)
            );
        }
    }

    #[test]
    fn reports_missing_group() {
        let mut write = GroupUpdate::new(config("Platform"));
        write.first().unwrap();
        let mut values = read_values();
        values[0] = None;
        assert_eq!(
            write.next(IoResult::Values(values)).err(),
            Some(UpdateGroupError::GroupNotFound)
        );
    }

    #[test]
    fn retries_commit_conflict() {
        // A lost commit reruns the whole write, reading the realm config again.
        let conflict = UpdateGroupError::from(IoError::Conflict);
        assert_eq!(
            conflict,
            UpdateGroupError::StorageError(StorageError::TransactionConflict)
        );
        assert!(GroupUpdate::is_conflict(&conflict));
        assert_eq!(
            GroupUpdate::conflict_retries(),
            crate::groups::CONFLICT_RETRIES as u32
        );
    }
}
