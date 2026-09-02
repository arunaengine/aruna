use aruna_core::admin_document_reducer::{
    AdminDocumentReducerError, AdminDocumentReducerState, GROUP_DISPLAY_NAME_PATH,
};
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{ADMIN_DOCUMENT_STATE_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, stale_admin_document_conflict_delete_entries,
};
use aruna_core::structs::{
    Actor, AuthContext, Group, Permission, PlacementRef, RealmConfigDocument,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, GroupId, Key, KeySpace, TxnId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::placement::placement_ref_for_target;

pub const MAX_GROUP_NAME_LEN: usize = 256;

/// The one group-name rule: trimmed, non-empty and at most
/// `MAX_GROUP_NAME_LEN` bytes. `None` means the name is refused.
pub fn normalize_group_name(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    (!trimmed.is_empty() && trimmed.len() <= MAX_GROUP_NAME_LEN).then(|| trimmed.to_string())
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateGroupConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted (delegated)
    /// credential stays restricted; it is never derived from `actor`.
    pub auth_context: AuthContext,
    pub group_id: GroupId,
    pub display_name: String,
}

/// Renames a group after creation. Only the label changes: the group id and
/// every permission path, bucket and dataset stay as they are.
#[derive(Debug, PartialEq)]
pub struct UpdateGroupOperation {
    config: UpdateGroupConfig,
    txn_id: Option<TxnId>,
    /// Bucket the group rows publish onto, read inside the write transaction.
    fence: crate::placement::fence::WriteFence,
    state: UpdateGroupState,
    output: Option<Result<Group, UpdateGroupError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum UpdateGroupState {
    Init,
    AuthGroupAdmin,
    AuthRealmAdmin,
    StartTransaction,
    ReadCurrent,
    WriteGroupAndAdminState {
        group: Group,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    },
    DeleteStaleAdminConflicts {
        group: Group,
    },
    ReadBucketFence {
        group: Group,
    },
    CommitTransaction {
        group: Group,
    },
    ScheduleOutboxDrain {
        group: Group,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum UpdateGroupError {
    #[error("Unauthorized")]
    Unauthorized,
    #[error("group not found")]
    GroupNotFound,
    #[error("group name must be non-empty and at most {MAX_GROUP_NAME_LEN} bytes")]
    InvalidDisplayName,
    #[error("stored group id does not match the requested group id")]
    GroupIdMismatch,
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("the group's bucket cut over to a new holder set; retry the rename")]
    PlacementFenced,
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentReducerError(#[from] AdminDocumentReducerError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
    #[error("update group did not finish")]
    NotFinished,
}

impl UpdateGroupOperation {
    pub fn new(config: UpdateGroupConfig) -> Self {
        Self {
            config,
            txn_id: None,
            fence: Default::default(),
            state: UpdateGroupState::Init,
            output: None,
        }
    }

    fn trimmed_name(&self) -> Result<String, UpdateGroupError> {
        normalize_group_name(&self.config.display_name).ok_or(UpdateGroupError::InvalidDisplayName)
    }

    fn document_ref(&self) -> DocumentSyncTarget {
        DocumentSyncTarget::GroupAuthorization {
            group_id: self.config.group_id,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::Group {
            group_id: self.config.group_id,
        }
    }

    fn check_permission(&self, path: String) -> Effects {
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path,
                required_permission: Permission::WRITE,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    /// Realm admins may rename a group they are not a member of, so a refused
    /// group-admin check falls back to the realm group administration path.
    fn emit_realm_auth(&mut self) -> Effects {
        self.state = UpdateGroupState::AuthRealmAdmin;
        self.check_permission(format!("/{}/admin/groups", self.config.actor.realm_id))
    }

    fn emit_read_current(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = UpdateGroupState::ReadCurrent;
        let target = self.admin_target();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    GROUP_KEYSPACE.to_string(),
                    ByteView::from(self.config.group_id.to_bytes()),
                ),
                (
                    ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                    admin_document_reducer_state_key(&target),
                ),
                (
                    REALM_CONFIG_KEYSPACE.to_string(),
                    ByteView::from(*self.config.actor.realm_id.as_bytes()),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn emit_write_state(
        &mut self,
        group_value: Option<Value>,
        reducer_state_value: Option<Value>,
        realm_config_value: Option<Value>,
    ) -> Result<Effects, UpdateGroupError> {
        let Some(txn_id) = self.txn_id else {
            return Err(UpdateGroupError::MissingTransaction);
        };
        let display_name = self.trimmed_name()?;
        let group_value = group_value.ok_or(UpdateGroupError::GroupNotFound)?;
        let mut group = Group::from_bytes(&group_value)?;
        if group.group_id != self.config.group_id || group.realm_id != self.config.actor.realm_id {
            return Err(UpdateGroupError::GroupIdMismatch);
        }

        let target = self.admin_target();
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
            .is_some_and(|state| state.target != target)
        {
            return Err(AdminDocumentReducerError::TargetMismatch.into());
        }
        let mut reducer_state = previous_reducer_state
            .clone()
            .unwrap_or_else(|| AdminDocumentReducerState::new(target));
        let admin_event = reducer_state.apply_operation(
            &self.config.actor,
            AdminDocumentOperation::GroupDisplayNameSet {
                display_name: display_name.clone(),
            },
        )?;
        group.display_name = display_name;
        overlay_reducer_name(&mut group, &reducer_state);

        let stale_conflict_deletes = stale_admin_document_conflict_delete_entries(
            previous_reducer_state.as_ref(),
            Some(&reducer_state),
        );
        let document_target = self.document_ref();
        let realm_config = realm_config_value
            .as_deref()
            .map(RealmConfigDocument::from_bytes)
            .transpose()?;
        let placement = realm_config
            .as_ref()
            .map(|config| placement_ref_for_target(config, &document_target, Default::default()))
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
            admin_document_reducer_state_write_entry(&reducer_state)?,
        ];
        let record = new_outbox_record_with_id(
            admin_event.event_id,
            self.config.actor.node_id,
            document_target,
            Vec::new(),
            DocumentSyncOutboxEvent::admin(admin_event),
            placement,
            false,
        )
        .fenced_at(self.fence.generation(&realm_id, &placement));
        writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        writes.extend(admin_document_conflict_write_entries(&reducer_state)?);

        self.state = UpdateGroupState::WriteGroupAndAdminState {
            group,
            stale_conflict_deletes,
        };
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    /// Takes the bucket's fence inside the transaction before committing, so a
    /// departing holder's close rejects or conflicts this write.
    fn emit_commit_transaction(&mut self, group: Group) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(UpdateGroupError::MissingTransaction);
        };
        if self.fence.is_empty() {
            return self.emit_commit(group);
        }
        self.state = UpdateGroupState::ReadBucketFence { group };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: self.fence.reads(),
            txn_id: Some(txn_id),
        })]
    }

    fn emit_commit(&mut self, group: Group) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(UpdateGroupError::MissingTransaction);
        };
        self.state = UpdateGroupState::CommitTransaction { group };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: UpdateGroupError) -> Effects {
        let cleanup = self.abort();
        self.state = UpdateGroupState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(UpdateGroupError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }

    fn handle_auth(&mut self, event: Event, allow_fallback: bool) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return self.unexpected_event("authorization result", got);
        };
        match allowed {
            Ok(true) => {
                self.state = UpdateGroupState::StartTransaction;
                smallvec![Effect::Storage(StorageEffect::StartTransaction {
                    read: false
                })]
            }
            Ok(false) if allow_fallback => self.emit_realm_auth(),
            Ok(false) => self.fail(UpdateGroupError::Unauthorized),
            Err(error) => {
                warn!(error = %error, "Group rename authorization check failed");
                match error {
                    AuthorizationError::StorageError(error) => {
                        self.fail(UpdateGroupError::StorageError(error))
                    }
                    _ => self.fail(UpdateGroupError::Unauthorized),
                }
            }
        }
    }
}

impl Operation for UpdateGroupOperation {
    type Output = Group;
    type Error = UpdateGroupError;

    fn start(&mut self) -> Effects {
        if self.config.auth_context.realm_id != self.config.actor.realm_id
            || self.config.auth_context.user_id != self.config.actor.user_id
        {
            return self.fail(UpdateGroupError::Unauthorized);
        }
        if let Err(error) = self.trimmed_name() {
            return self.fail(error);
        }
        self.state = UpdateGroupState::AuthGroupAdmin;
        self.check_permission(format!(
            "/{}/g/{}/admin",
            self.config.actor.realm_id, self.config.group_id
        ))
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            UpdateGroupState::AuthGroupAdmin => self.handle_auth(event, true),
            UpdateGroupState::AuthRealmAdmin => self.handle_auth(event, false),
            UpdateGroupState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            UpdateGroupState::ReadCurrent => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [
                        (_, group_value),
                        (_, reducer_state_value),
                        (_, realm_config_value),
                    ] = values.as_slice()
                    else {
                        return self.unexpected_event(
                            "storage batch read result with group, admin state, and realm config",
                            format!("{values:?}"),
                        );
                    };
                    match self.emit_write_state(
                        group_value.clone(),
                        reducer_state_value.clone(),
                        realm_config_value.clone(),
                    ) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch read result", format!("{other:?}")),
            },
            UpdateGroupState::WriteGroupAndAdminState {
                group,
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(UpdateGroupError::MissingTransaction);
                    };
                    if !stale_conflict_deletes.is_empty() {
                        self.state = UpdateGroupState::DeleteStaleAdminConflicts { group };
                        return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                            deletes: stale_conflict_deletes,
                            txn_id: Some(txn_id),
                        })];
                    }
                    self.emit_commit_transaction(group)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch write result", format!("{other:?}")),
            },
            UpdateGroupState::DeleteStaleAdminConflicts { group } => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    self.emit_commit_transaction(group)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            UpdateGroupState::ReadBucketFence { group } => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    if self.fence.admits(&values) {
                        self.emit_commit(group)
                    } else {
                        self.fail(UpdateGroupError::PlacementFenced)
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("bucket fence read result", format!("{other:?}")),
            },
            UpdateGroupState::CommitTransaction { group } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.output = Some(Ok(group.clone()));
                    self.state = UpdateGroupState::ScheduleOutboxDrain { group };
                    smallvec![schedule_outbox_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            UpdateGroupState::ScheduleOutboxDrain { .. } => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = UpdateGroupState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document outbox drain; durable outbox remains retryable");
                    self.state = UpdateGroupState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            UpdateGroupState::Init | UpdateGroupState::Finish | UpdateGroupState::Error => {
                let got = format!("{event:?}");
                self.unexpected_event("no event", got)
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            UpdateGroupState::Finish | UpdateGroupState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(UpdateGroupError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

/// Keeps the stored name when the reducer withheld the field on a conflict, so
/// a concurrent rename never blanks the group label (decision Q2).
fn overlay_reducer_name(group: &mut Group, reducer_state: &AdminDocumentReducerState) {
    if !reducer_state
        .conflicts
        .contains_key(GROUP_DISPLAY_NAME_PATH)
        && let Some(display_name) = reducer_state.materialized_group_display_name()
    {
        group.display_name = display_name;
    }
}

#[cfg(test)]
mod tests {
    use super::{UpdateGroupConfig, UpdateGroupError, UpdateGroupOperation};
    use aruna_core::admin_document_reducer::AdminDocumentReducerState;
    use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
    use aruna_core::document::{
        DocumentSyncOutboxEvent, DocumentSyncOutboxRecord, DocumentSyncTarget,
    };
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::keyspaces::{
        ADMIN_DOCUMENT_STATE_KEYSPACE, DOCUMENT_SYNC_OUTBOX_KEYSPACE, GROUP_KEYSPACE,
        REALM_CONFIG_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::storage_entries::admin_document_reducer_state_key;
    use aruna_core::structs::{Actor, AuthContext, Group, RealmId};
    use aruna_core::task::{TaskEvent, TaskKey};
    use aruna_core::types::{GroupId, TxnId, UserId};
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
            display_name: display_name.to_string(),
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

    fn read_values() -> Vec<(ByteView, Option<ByteView>)> {
        let target = AdminDocumentTarget::Group { group_id: group() };
        vec![
            (
                ByteView::from(group().to_bytes()),
                Some(stored_group().to_bytes(&actor()).unwrap().into()),
            ),
            (admin_document_reducer_state_key(&target), None),
            (ByteView::from(*realm().as_bytes()), None),
        ]
    }

    fn authorize(
        operation: &mut UpdateGroupOperation,
        allowed: bool,
    ) -> aruna_core::types::Effects {
        operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult {
                allowed: Ok(allowed),
            },
        ))
    }

    #[test]
    fn renames_and_queues() {
        let mut operation = UpdateGroupOperation::new(config("  Platform  "));
        assert!(matches!(
            operation.start().first(),
            Some(Effect::SubOperation(_))
        ));
        assert!(matches!(
            authorize(&mut operation, true).first(),
            Some(Effect::Storage(StorageEffect::StartTransaction {
                read: false
            }))
        ));

        let txn_id = TxnId::generate();
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let target = AdminDocumentTarget::Group { group_id: group() };
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchRead { reads, txn_id: id }) => {
                assert_eq!(*id, Some(txn_id));
                assert_eq!(reads.len(), 3);
                assert_eq!(reads[0].0, GROUP_KEYSPACE);
                assert_eq!(reads[0].1.as_ref(), group().to_bytes().as_slice());
                assert_eq!(reads[1].0, ADMIN_DOCUMENT_STATE_KEYSPACE);
                assert_eq!(reads[1].1, admin_document_reducer_state_key(&target));
                assert_eq!(reads[2].0, REALM_CONFIG_KEYSPACE);
            }
            other => panic!("unexpected read effect: {other:?}"),
        }

        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: read_values(),
        }));
        let document = DocumentSyncTarget::GroupAuthorization { group_id: group() };
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchWrite { writes, txn_id: id }) => {
                assert_eq!(*id, Some(txn_id));
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
                    .find(|(keyspace, _, _)| keyspace == ADMIN_DOCUMENT_STATE_KEYSPACE)
                    .expect("reducer state is written");
                let reducer_state: AdminDocumentReducerState =
                    postcard::from_bytes(reducer_write.2.as_ref()).unwrap();
                assert_eq!(
                    reducer_state.materialized_group_display_name().as_deref(),
                    Some("Platform")
                );

                let records: Vec<DocumentSyncOutboxRecord> = writes
                    .iter()
                    .filter(|(keyspace, _, _)| keyspace == DOCUMENT_SYNC_OUTBOX_KEYSPACE)
                    .map(|(_, _, value)| postcard::from_bytes(value).unwrap())
                    .collect();
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].target, document);
                assert!(matches!(
                    &records[0].event,
                    DocumentSyncOutboxEvent::AdminOperation { event, .. }
                        if matches!(
                            &event.op,
                            AdminDocumentOperation::GroupDisplayNameSet { display_name }
                                if display_name == "Platform"
                        )
                ));
            }
            other => panic!("unexpected write effect: {other:?}"),
        }

        assert!(matches!(
            operation
                .step(Event::Storage(StorageEvent::BatchWriteResult {
                    entries: Vec::new(),
                }))
                .first(),
            Some(Effect::Storage(StorageEffect::CommitTransaction { .. }))
        ));
        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(matches!(effects.first(), Some(Effect::Task(_))));
        assert!(
            operation
                .step(Event::Task(TaskEvent::TimerScheduled {
                    key: TaskKey::DrainDocumentSyncOutbox,
                    after: std::time::Duration::ZERO,
                }))
                .is_empty()
        );
        assert!(operation.is_complete());
        assert_eq!(operation.finalize().unwrap().display_name, "Platform");
    }

    #[test]
    fn realm_admin_fallback() {
        // A realm admin who is not a group member renames through the realm path.
        let mut operation = UpdateGroupOperation::new(config("Platform"));
        operation.start();
        assert!(matches!(
            authorize(&mut operation, false).first(),
            Some(Effect::SubOperation(_))
        ));
        assert!(matches!(
            authorize(&mut operation, true).first(),
            Some(Effect::Storage(StorageEffect::StartTransaction {
                read: false
            }))
        ));
    }

    #[test]
    fn rejects_unauthorized_actor() {
        let mut operation = UpdateGroupOperation::new(config("Platform"));
        operation.start();
        authorize(&mut operation, false);
        assert!(authorize(&mut operation, false).is_empty());
        assert_eq!(operation.finalize(), Err(UpdateGroupError::Unauthorized));
    }

    #[test]
    fn rejects_invalid_name() {
        for name in ["   ", &"n".repeat(257)] {
            let mut operation = UpdateGroupOperation::new(config(name));
            assert!(operation.start().is_empty());
            assert_eq!(
                operation.finalize(),
                Err(UpdateGroupError::InvalidDisplayName)
            );
        }
    }

    #[test]
    fn reports_missing_group() {
        let mut operation = UpdateGroupOperation::new(config("Platform"));
        operation.start();
        authorize(&mut operation, true);
        let txn_id = TxnId::generate();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let mut values = read_values();
        values[0].1 = None;
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::AbortTransaction { .. }))
        ));
        assert_eq!(operation.finalize(), Err(UpdateGroupError::GroupNotFound));
    }

    #[test]
    fn aborts_open_transaction() {
        let mut operation = UpdateGroupOperation::new(config("Platform"));
        operation.start();
        authorize(&mut operation, true);
        let txn_id = TxnId::generate();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: aruna_core::errors::StorageError::TransactionConflict,
        }));
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::AbortTransaction { txn_id: id }))
                if *id == txn_id
        ));
    }
}
