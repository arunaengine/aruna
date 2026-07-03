use std::str::FromStr;

use aruna_core::NodeId;
use aruna_core::admin_document_reducer::{AdminDocumentReducerError, AdminDocumentReducerState};
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::ADMIN_DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, stale_admin_document_conflict_delete_entries,
};
use aruna_core::structs::{Actor, RealmConfigDocument, RealmNodeKind};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};

#[derive(Debug, Clone, PartialEq)]
pub struct EnsureRealmConfigConfig {
    pub actor: Actor,
    pub target_node_id: NodeId,
    pub target_node_kind: RealmNodeKind,
    pub default_metadata_replication_factor: u32,
    pub realm_description: String,
    pub create_if_missing: bool,
    pub reject_kind_mismatch: bool,
}

#[derive(Debug, PartialEq)]
pub struct EnsureRealmConfigOperation {
    config: EnsureRealmConfigConfig,
    txn_id: Option<TxnId>,
    state: EnsureRealmConfigState,
    output: Option<Result<RealmConfigDocument, EnsureRealmConfigError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum EnsureRealmConfigState {
    Init,
    StartTransaction,
    ReadCurrent,
    WriteDocumentAndAdminState {
        document: RealmConfigDocument,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    },
    DeleteStaleAdminConflicts {
        document: RealmConfigDocument,
    },
    CommitNoop {
        document: RealmConfigDocument,
    },
    CommitTransaction {
        document: RealmConfigDocument,
    },
    ScheduleDocumentSyncOutboxDrain {
        document: RealmConfigDocument,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum EnsureRealmConfigError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentReducerError(#[from] AdminDocumentReducerError),
    #[error("realm config document missing")]
    RealmConfigNotFound,
    #[error("realm config node {node_id} already exists with a different kind")]
    NodeKindMismatch { node_id: NodeId },
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl EnsureRealmConfigOperation {
    pub fn new(config: EnsureRealmConfigConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: EnsureRealmConfigState::Init,
            output: None,
        }
    }

    fn document_ref(&self) -> DocumentSyncTarget {
        DocumentSyncTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn emit_read_current(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = EnsureRealmConfigState::ReadCurrent;
        let document = self.document_ref();
        let target = self.admin_target();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    document.storage_keyspace().to_string(),
                    document.storage_key(),
                ),
                (
                    ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                    admin_document_reducer_state_key(&target),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn emit_write_document_and_admin_state(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<Effects, EnsureRealmConfigError> {
        let Some(txn_id) = self.txn_id else {
            return Err(EnsureRealmConfigError::MissingTransaction);
        };
        let mut document = match document_value.as_deref() {
            Some(value) => RealmConfigDocument::from_bytes(value)?,
            None if self.config.create_if_missing => {
                let mut document = RealmConfigDocument::new(
                    self.config.actor.realm_id,
                    Vec::new(),
                    self.config.default_metadata_replication_factor,
                );
                document.description = self.config.realm_description.clone();
                document
            }
            None => return Err(EnsureRealmConfigError::RealmConfigNotFound),
        };

        if self.config.reject_kind_mismatch {
            let target_node_id = self.config.target_node_id.to_string();
            if document.nodes.iter().any(|node| {
                node.node_id == target_node_id && node.kind != self.config.target_node_kind
            }) {
                return Err(EnsureRealmConfigError::NodeKindMismatch {
                    node_id: self.config.target_node_id,
                });
            }
        }

        let target = self.admin_target();
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

        let mut reducer_state = previous_reducer_state
            .clone()
            .unwrap_or_else(|| AdminDocumentReducerState::new(target));
        if previous_reducer_state.as_ref().is_some_and(|state| {
            realm_config_node_ensure_is_noop(
                &document,
                state,
                &self.config.target_node_id,
                &self.config.target_node_kind,
            )
        }) {
            self.output = Some(Ok(document.clone()));
            return Ok(self.emit_commit_noop(document));
        }

        let admin_event = apply_realm_config_node_ensure(
            &mut reducer_state,
            &self.config.actor,
            self.config.target_node_id,
            self.config.target_node_kind.clone(),
        )?;
        overlay_realm_config_reducer_materialization(&mut document, &reducer_state);

        let stale_conflict_deletes = stale_admin_document_conflict_delete_entries(
            previous_reducer_state.as_ref(),
            Some(&reducer_state),
        );
        let document_target = self.document_ref();
        let mut writes = vec![
            (
                document_target.storage_keyspace().to_string(),
                document_target.storage_key(),
                document.to_bytes(&self.config.actor)?.into(),
            ),
            admin_document_reducer_state_write_entry(&reducer_state)?,
        ];
        let record = new_outbox_record_with_id(
            admin_event.event_id,
            self.config.actor.node_id,
            document_target,
            Vec::new(),
            DocumentSyncOutboxEvent::AdminOperation {
                event: Box::new(admin_event),
            },
        );
        writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        writes.extend(admin_document_conflict_write_entries(&reducer_state)?);

        self.output = Some(Ok(document.clone()));
        self.state = EnsureRealmConfigState::WriteDocumentAndAdminState {
            document,
            stale_conflict_deletes,
        };

        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn emit_commit_noop(&mut self, document: RealmConfigDocument) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(EnsureRealmConfigError::MissingTransaction);
        };
        self.state = EnsureRealmConfigState::CommitNoop { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn emit_commit_transaction(&mut self, document: RealmConfigDocument) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(EnsureRealmConfigError::MissingTransaction);
        };
        self.state = EnsureRealmConfigState::CommitTransaction { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: EnsureRealmConfigError) -> Effects {
        let cleanup = self.abort();
        self.state = EnsureRealmConfigState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(EnsureRealmConfigError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for EnsureRealmConfigOperation {
    type Output = RealmConfigDocument;
    type Error = EnsureRealmConfigError;

    fn start(&mut self) -> Effects {
        self.state = EnsureRealmConfigState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            EnsureRealmConfigState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            EnsureRealmConfigState::ReadCurrent => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, document_value), (_, reducer_state_value)] = values.as_slice() else {
                        return self.unexpected_event(
                            "storage batch read result with realm config and reducer state",
                            format!("{values:?}"),
                        );
                    };
                    match self.emit_write_document_and_admin_state(
                        document_value.clone(),
                        reducer_state_value.clone(),
                    ) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch read result", format!("{other:?}")),
            },
            EnsureRealmConfigState::WriteDocumentAndAdminState {
                document,
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(EnsureRealmConfigError::MissingTransaction);
                    };
                    if !stale_conflict_deletes.is_empty() {
                        self.state = EnsureRealmConfigState::DeleteStaleAdminConflicts { document };
                        return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                            deletes: stale_conflict_deletes,
                            txn_id: Some(txn_id),
                        })];
                    }
                    self.emit_commit_transaction(document)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch write result", format!("{other:?}")),
            },
            EnsureRealmConfigState::DeleteStaleAdminConflicts { document } => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    self.emit_commit_transaction(document)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            EnsureRealmConfigState::CommitNoop { .. } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = EnsureRealmConfigState::Finish;
                    smallvec![]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            EnsureRealmConfigState::CommitTransaction { document } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state =
                        EnsureRealmConfigState::ScheduleDocumentSyncOutboxDrain { document };
                    smallvec![schedule_outbox_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            EnsureRealmConfigState::ScheduleDocumentSyncOutboxDrain { .. } => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = EnsureRealmConfigState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document operation outbox drain; durable outbox remains retryable");
                    self.state = EnsureRealmConfigState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            EnsureRealmConfigState::Finish
            | EnsureRealmConfigState::Error
            | EnsureRealmConfigState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            EnsureRealmConfigState::Finish | EnsureRealmConfigState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or_else(|| {
            Ok(RealmConfigDocument::default_for_realm(
                self.config.actor.realm_id,
                Vec::new(),
            ))
        })
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

fn apply_realm_config_node_ensure(
    state: &mut AdminDocumentReducerState,
    actor: &Actor,
    node_id: NodeId,
    kind: RealmNodeKind,
) -> Result<AdminDocumentEvent, AdminDocumentReducerError> {
    let observed = state.clock.clone();
    let event = AdminDocumentEvent {
        event_id: Ulid::new(),
        target: state.target.clone(),
        origin_node_id: actor.node_id,
        origin_seq: observed.sequence_for(&actor.node_id) + 1,
        observed,
        actor: actor.clone(),
        op: AdminDocumentOperation::RealmConfigNodeEnsured { node_id, kind },
    };
    state.apply(&event)?;
    Ok(event)
}

fn overlay_realm_config_reducer_materialization(
    config: &mut RealmConfigDocument,
    reducer_state: &AdminDocumentReducerState,
) {
    for path in reducer_state.conflicts.keys() {
        if let Some(node_id) = realm_config_node_from_path(path) {
            remove_realm_config_node(config, &node_id);
        }
    }

    for (node_id, kind) in reducer_state.materialized_realm_config_nodes() {
        let path = realm_config_node_path(&node_id);
        if reducer_state.conflicts.contains_key(&path) {
            remove_realm_config_node(config, &node_id);
            continue;
        }
        config.ensure_node(node_id, kind);
    }
}

fn realm_config_node_ensure_is_noop(
    document: &RealmConfigDocument,
    reducer_state: &AdminDocumentReducerState,
    node_id: &NodeId,
    kind: &RealmNodeKind,
) -> bool {
    let path = realm_config_node_path(node_id);
    !reducer_state.conflicts.contains_key(&path)
        && reducer_state
            .materialized_realm_config_nodes()
            .get(node_id)
            .is_some_and(|materialized_kind| materialized_kind == kind)
        && realm_config_document_has_node_kind(document, node_id, kind)
}

fn realm_config_document_has_node_kind(
    document: &RealmConfigDocument,
    node_id: &NodeId,
    kind: &RealmNodeKind,
) -> bool {
    let node_id = node_id.to_string();
    let mut matches = document.nodes.iter().filter(|node| node.node_id == node_id);
    matches.next().is_some_and(|node| node.kind == *kind) && matches.all(|node| node.kind == *kind)
}

fn remove_realm_config_node(config: &mut RealmConfigDocument, node_id: &NodeId) {
    let node_id = node_id.to_string();
    config.nodes.retain(|node| node.node_id != node_id);
}

fn realm_config_node_path(node_id: &NodeId) -> String {
    format!("realm_config.nodes.{node_id}")
}

fn realm_config_node_from_path(path: &str) -> Option<NodeId> {
    let node_id = path.strip_prefix("realm_config.nodes.")?;
    NodeId::from_str(node_id).ok()
}

#[cfg(test)]
mod tests {
    use aruna_core::admin_document_reducer::{
        AdminDocumentConflict, AdminDocumentConflictValue, AdminDocumentReducerState,
    };
    use aruna_core::admin_documents::{
        AdminDocumentClock, AdminDocumentDot, AdminDocumentEvent, AdminDocumentOperation,
        AdminDocumentTarget,
    };
    use aruna_core::document::{
        DocumentSyncOutboxEvent, DocumentSyncOutboxRecord, DocumentSyncTarget,
    };
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        ADMIN_DOCUMENT_CONFLICT_KEYSPACE, ADMIN_DOCUMENT_STATE_KEYSPACE,
        DOCUMENT_SYNC_OUTBOX_KEYSPACE, REALM_CONFIG_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::storage_entries::admin_document_reducer_conflict_key;
    use aruna_core::structs::{Actor, RealmConfigDocument, RealmId, RealmNodeKind};
    use aruna_core::task::{TaskEvent, TaskKey};
    use aruna_core::types::{Effects, Key, KeySpace, TxnId, UserId, Value};
    use ulid::Ulid;

    use super::{
        EnsureRealmConfigConfig, EnsureRealmConfigError, EnsureRealmConfigOperation,
        EnsureRealmConfigState, realm_config_node_path,
    };

    fn node(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn actor(seed: u8, realm_id: RealmId) -> Actor {
        Actor {
            node_id: node(seed),
            user_id: UserId::local(Ulid::from_bytes([seed; 16]), realm_id),
            realm_id,
        }
    }

    fn config(actor: Actor, factor: u32) -> EnsureRealmConfigConfig {
        EnsureRealmConfigConfig {
            target_node_id: actor.node_id,
            target_node_kind: RealmNodeKind::Management,
            actor,
            default_metadata_replication_factor: factor,
            realm_description: "Ensured Realm".to_string(),
            create_if_missing: true,
            reject_kind_mismatch: false,
        }
    }

    fn conflict(path: &str) -> AdminDocumentConflict {
        let value = |seed: u8, value: &str| AdminDocumentConflictValue {
            value: Some(value.to_string()),
            dot: AdminDocumentDot {
                event_id: Ulid::from_bytes([seed; 16]),
                origin_node_id: node(seed),
                origin_seq: 1,
            },
        };
        AdminDocumentConflict {
            path: path.to_string(),
            values: vec![value(3, "server"), value(4, "management")],
        }
    }

    fn batch_write(effects: Effects, txn_id: TxnId) -> Vec<(KeySpace, Key, Value)> {
        match effects.into_iter().next().unwrap() {
            Effect::Storage(StorageEffect::BatchWrite { writes, txn_id: id }) => {
                assert_eq!(id, Some(txn_id));
                writes
            }
            other => panic!("unexpected write effect: {other:?}"),
        }
    }

    fn write_value<'a>(writes: &'a [(KeySpace, Key, Value)], keyspace: &str) -> &'a Value {
        &writes
            .iter()
            .find(|(candidate, _, _)| candidate == keyspace)
            .expect("write exists")
            .2
    }

    #[test]
    fn writes_missing_config_reducer_state_outbox_and_stale_conflict_delete() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let actor = actor(1, realm_id);
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let path = realm_config_node_path(&actor.node_id);
        let mut previous_state = AdminDocumentReducerState::new(target.clone());
        for seed in [3, 4] {
            previous_state.clock.advance(node(seed), 1);
        }
        previous_state
            .conflicts
            .insert(path.clone(), conflict(&path));

        let mut operation = EnsureRealmConfigOperation::new(config(actor.clone(), 7));
        let txn_id = TxnId::new();
        operation.txn_id = Some(txn_id);
        let writes = batch_write(
            operation
                .emit_write_document_and_admin_state(
                    None,
                    Some(postcard::to_allocvec(&previous_state).unwrap().into()),
                )
                .unwrap(),
            txn_id,
        );

        let stored =
            RealmConfigDocument::from_bytes(write_value(&writes, REALM_CONFIG_KEYSPACE)).unwrap();
        let state: AdminDocumentReducerState =
            postcard::from_bytes(write_value(&writes, ADMIN_DOCUMENT_STATE_KEYSPACE)).unwrap();
        let outbox: DocumentSyncOutboxRecord =
            postcard::from_bytes(write_value(&writes, DOCUMENT_SYNC_OUTBOX_KEYSPACE)).unwrap();
        assert_eq!(stored.metadata_replication.default_replication_factor, 7);
        assert_eq!(stored.description, "Ensured Realm");
        assert!(stored.has_node(actor.node_id));
        assert_eq!(
            state.materialized_realm_config_nodes()[&actor.node_id],
            RealmNodeKind::Management
        );
        assert_eq!(outbox.target, DocumentSyncTarget::RealmConfig { realm_id });
        assert!(matches!(
            &outbox.event,
            DocumentSyncOutboxEvent::AdminOperation { event }
                if event.target == target
                    && matches!(event.op, AdminDocumentOperation::RealmConfigNodeEnsured { .. })
        ));

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: vec![],
        }));
        assert_eq!(
            effects.first(),
            Some(&Effect::Storage(StorageEffect::BatchDelete {
                deletes: vec![(
                    ADMIN_DOCUMENT_CONFLICT_KEYSPACE.to_string(),
                    admin_document_reducer_conflict_key(&target, &path),
                )],
                txn_id: Some(txn_id),
            }))
        );
    }

    #[test]
    fn idempotent_same_node_ensure_does_not_duplicate_config_node() {
        let realm_id = RealmId::from_bytes([9; 32]);
        let actor = actor(9, realm_id);
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.ensure_node(actor.node_id, RealmNodeKind::Management);
        let mut operation = EnsureRealmConfigOperation::new(config(actor.clone(), 3));
        let txn_id = TxnId::new();
        operation.txn_id = Some(txn_id);
        let writes = batch_write(
            operation
                .emit_write_document_and_admin_state(
                    Some(document.to_bytes(&actor).unwrap().into()),
                    None,
                )
                .unwrap(),
            txn_id,
        );
        let stored =
            RealmConfigDocument::from_bytes(write_value(&writes, REALM_CONFIG_KEYSPACE)).unwrap();
        assert_eq!(stored.nodes.len(), 1);
        assert!(stored.has_node(actor.node_id));
    }

    #[test]
    fn repeated_ensure_does_not_write_admin_outbox_event() {
        let realm_id = RealmId::from_bytes([10; 32]);
        let actor = actor(10, realm_id);
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let mut previous_state = AdminDocumentReducerState::new(target.clone());
        previous_state
            .apply(&AdminDocumentEvent {
                event_id: Ulid::from_bytes([10; 16]),
                target,
                origin_node_id: actor.node_id,
                origin_seq: 1,
                observed: AdminDocumentClock::default(),
                actor: actor.clone(),
                op: AdminDocumentOperation::RealmConfigNodeEnsured {
                    node_id: actor.node_id,
                    kind: RealmNodeKind::Management,
                },
            })
            .unwrap();
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.ensure_node(actor.node_id, RealmNodeKind::Management);

        let mut operation = EnsureRealmConfigOperation::new(config(actor.clone(), 3));
        let txn_id = TxnId::new();
        operation.txn_id = Some(txn_id);
        let effects = operation
            .emit_write_document_and_admin_state(
                Some(document.to_bytes(&actor).unwrap().into()),
                Some(postcard::to_allocvec(&previous_state).unwrap().into()),
            )
            .unwrap();

        assert_eq!(
            effects.first(),
            Some(&Effect::Storage(StorageEffect::CommitTransaction {
                txn_id
            }))
        );
        assert!(!effects.iter().any(|effect| matches!(
            effect,
            Effect::Storage(StorageEffect::BatchWrite { writes, .. })
                if writes
                    .iter()
                    .any(|(keyspace, _, _)| keyspace == DOCUMENT_SYNC_OUTBOX_KEYSPACE)
        )));

        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(operation.finalize().unwrap(), document);
    }

    #[test]
    fn scheduled_admin_outbox_finishes_without_direct_replication() {
        let realm_id = RealmId::from_bytes([11; 32]);
        let actor = actor(11, realm_id);
        let document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        let mut operation = EnsureRealmConfigOperation::new(config(actor, 3));
        operation.state = EnsureRealmConfigState::ScheduleDocumentSyncOutboxDrain { document };

        let effects = operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainDocumentSyncOutbox,
            after: std::time::Duration::ZERO,
        }));

        assert!(effects.is_empty());
        assert_eq!(operation.state, EnsureRealmConfigState::Finish);
    }

    #[test]
    fn rejects_existing_node_kind_mismatch_when_configured() {
        let realm_id = RealmId::from_bytes([8; 32]);
        let actor = actor(8, realm_id);
        let target_node_id = node(7);
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.ensure_node(target_node_id, RealmNodeKind::Management);

        let mut operation = EnsureRealmConfigOperation::new(EnsureRealmConfigConfig {
            target_node_id,
            target_node_kind: RealmNodeKind::Server,
            reject_kind_mismatch: true,
            ..config(actor.clone(), 3)
        });
        operation.txn_id = Some(TxnId::new());

        let error = operation
            .emit_write_document_and_admin_state(
                Some(document.to_bytes(&actor).unwrap().into()),
                None,
            )
            .unwrap_err();

        assert_eq!(
            error,
            EnsureRealmConfigError::NodeKindMismatch {
                node_id: target_node_id
            }
        );
    }
}
