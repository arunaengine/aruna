use aruna_core::admin_document_reducer::{
    AdminDocumentReducerError, AdminDocumentReducerState,
    overlay_realm_config_placement_reducer_materialization,
};
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
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
use aruna_core::structs::{Actor, NodePlacementEntry, RealmConfigDocument};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::sync_placement::schedule_placement_revalidation_effect;

#[derive(Debug, Clone, PartialEq)]
pub struct SetNodePlacementConfig {
    pub actor: Actor,
    pub entry: NodePlacementEntry,
}

#[derive(Debug, PartialEq)]
pub struct SetNodePlacementOperation {
    config: SetNodePlacementConfig,
    txn_id: Option<TxnId>,
    state: SetNodePlacementState,
    output: Option<Result<RealmConfigDocument, SetNodePlacementError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum SetNodePlacementState {
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
    CommitTransaction {
        document: RealmConfigDocument,
    },
    ScheduleDocumentSyncOutboxDrain {
        document: RealmConfigDocument,
    },
    SchedulePlacementRevalidation,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum SetNodePlacementError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentReducerError(#[from] AdminDocumentReducerError),
    #[error("realm config document missing")]
    RealmConfigNotFound,
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl SetNodePlacementOperation {
    pub fn new(config: SetNodePlacementConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: SetNodePlacementState::Init,
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
        self.state = SetNodePlacementState::ReadCurrent;
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
    ) -> Result<Effects, SetNodePlacementError> {
        let Some(txn_id) = self.txn_id else {
            return Err(SetNodePlacementError::MissingTransaction);
        };

        let Some(document_value) = document_value else {
            return Err(SetNodePlacementError::RealmConfigNotFound);
        };
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;

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
            AdminDocumentOperation::RealmConfigNodePlacementSet {
                entry: self.config.entry.clone(),
            },
        )?;
        overlay_realm_config_placement_reducer_materialization(&mut document, &reducer_state);

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
            false,
        );
        writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        writes.extend(admin_document_conflict_write_entries(&reducer_state)?);

        self.output = Some(Ok(document.clone()));
        self.state = SetNodePlacementState::WriteDocumentAndAdminState {
            document,
            stale_conflict_deletes,
        };

        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn emit_commit_transaction(&mut self, document: RealmConfigDocument) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(SetNodePlacementError::MissingTransaction);
        };
        self.state = SetNodePlacementState::CommitTransaction { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: SetNodePlacementError) -> Effects {
        let cleanup = self.abort();
        self.state = SetNodePlacementState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(SetNodePlacementError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for SetNodePlacementOperation {
    type Output = RealmConfigDocument;
    type Error = SetNodePlacementError;

    fn start(&mut self) -> Effects {
        self.state = SetNodePlacementState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            SetNodePlacementState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            SetNodePlacementState::ReadCurrent => match event {
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
            SetNodePlacementState::WriteDocumentAndAdminState {
                document,
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(SetNodePlacementError::MissingTransaction);
                    };
                    if !stale_conflict_deletes.is_empty() {
                        self.state = SetNodePlacementState::DeleteStaleAdminConflicts { document };
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
            SetNodePlacementState::DeleteStaleAdminConflicts { document } => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    self.emit_commit_transaction(document)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            SetNodePlacementState::CommitTransaction { document } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state =
                        SetNodePlacementState::ScheduleDocumentSyncOutboxDrain { document };
                    smallvec![schedule_outbox_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            SetNodePlacementState::ScheduleDocumentSyncOutboxDrain { .. } => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = SetNodePlacementState::SchedulePlacementRevalidation;
                    smallvec![schedule_placement_revalidation_effect(
                        self.config.actor.realm_id,
                        self.config.actor.node_id,
                    )]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document operation outbox drain; durable outbox remains retryable");
                    self.state = SetNodePlacementState::SchedulePlacementRevalidation;
                    smallvec![schedule_placement_revalidation_effect(
                        self.config.actor.realm_id,
                        self.config.actor.node_id,
                    )]
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            SetNodePlacementState::SchedulePlacementRevalidation => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = SetNodePlacementState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule placement revalidation after realm config mutation");
                    self.state = SetNodePlacementState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "placement revalidation timer schedule",
                    format!("{other:?}"),
                ),
            },
            SetNodePlacementState::Finish
            | SetNodePlacementState::Error
            | SetNodePlacementState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            SetNodePlacementState::Finish | SetNodePlacementState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .expect("set node placement operation must set output")
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};
    use crate::get_realm_config::GetRealmConfigOperation;
    use aruna_core::structs::{DEFAULT_NODE_WEIGHT, RealmId};
    use aruna_core::types::UserId;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    fn test_ctx(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        }
    }

    fn node(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    async fn seed_realm(ctx: &DriverContext) -> (Actor, RealmId) {
        let realm_signing_key = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let actor = Actor {
            node_id: node(1),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: actor.clone(),
                realm_description: "Realm".to_string(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            ctx,
        )
        .await
        .unwrap();
        (actor, realm_id)
    }

    #[tokio::test]
    async fn sets_placement_entry_on_realm_config() {
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let (actor, realm_id) = seed_realm(&ctx).await;

        let entry = NodePlacementEntry {
            node_id: node(2),
            location: "eu-west".to_string(),
            weight: 250,
            full: false,
            draining: false,
            labels: BTreeMap::new(),
        };
        drive(
            SetNodePlacementOperation::new(SetNodePlacementConfig {
                actor: actor.clone(),
                entry: entry.clone(),
            }),
            &ctx,
        )
        .await
        .unwrap();

        let config = drive(GetRealmConfigOperation::new(realm_id), &ctx)
            .await
            .unwrap();
        assert_eq!(config.placement_entry(node(2)), Some(&entry));
    }

    #[tokio::test]
    async fn rejects_reserved_kind_label() {
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let (actor, _realm_id) = seed_realm(&ctx).await;

        let entry = NodePlacementEntry {
            node_id: node(2),
            location: String::new(),
            weight: DEFAULT_NODE_WEIGHT,
            full: false,
            draining: false,
            labels: BTreeMap::from([(
                aruna_core::structs::KIND_LABEL_KEY.to_string(),
                "Server".to_string(),
            )]),
        };
        let error = drive(
            SetNodePlacementOperation::new(SetNodePlacementConfig { actor, entry }),
            &ctx,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            SetNodePlacementError::AdminDocumentReducerError(
                AdminDocumentReducerError::ReservedPlacementLabel
            )
        ));
    }

    #[test]
    fn local_config_mutation_schedules_zero_delay_placement_revalidation() {
        let realm_id = RealmId::from_bytes([3; 32]);
        let actor = Actor {
            node_id: node(1),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        let entry = NodePlacementEntry {
            node_id: node(2),
            location: String::new(),
            weight: DEFAULT_NODE_WEIGHT,
            full: true,
            draining: false,
            labels: BTreeMap::new(),
        };
        let mut operation = SetNodePlacementOperation::new(SetNodePlacementConfig {
            actor: actor.clone(),
            entry,
        });
        operation.state = SetNodePlacementState::ScheduleDocumentSyncOutboxDrain {
            document: RealmConfigDocument::new(realm_id, Vec::new(), 3),
        };

        let effects = operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: aruna_core::task::TaskKey::DrainDocumentSyncOutbox,
            after: std::time::Duration::ZERO,
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Task(aruna_core::task::TaskEffect::ResetTimer {
                key: aruna_core::task::TaskKey::SyncPlacements {
                    realm_id: scheduled_realm,
                    node_id,
                },
                after,
            })] if *scheduled_realm == realm_id && *node_id == actor.node_id && after.is_zero()
        ));
        assert_eq!(
            operation.state,
            SetNodePlacementState::SchedulePlacementRevalidation
        );
    }

    #[test]
    fn conflicted_placement_path_removes_entry() {
        use aruna_core::admin_document_reducer::{
            AdminDocumentConflict, AdminDocumentConflictValue, realm_config_placement_node_path,
        };
        use aruna_core::admin_documents::{AdminDocumentDot, AdminDocumentTarget};
        use ulid::Ulid;

        let realm_id = RealmId::from_bytes([1u8; 32]);
        let entry = NodePlacementEntry {
            node_id: node(2),
            location: "eu".to_string(),
            weight: 100,
            full: false,
            draining: false,
            labels: BTreeMap::new(),
        };
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.placement_map.push(entry.clone());

        let mut reducer_state =
            AdminDocumentReducerState::new(AdminDocumentTarget::RealmConfig { realm_id });
        let path = realm_config_placement_node_path(&entry.node_id);
        reducer_state.conflicts.insert(
            path.clone(),
            AdminDocumentConflict {
                path,
                values: vec![AdminDocumentConflictValue {
                    value: Some("conflicted".to_string()),
                    dot: AdminDocumentDot {
                        event_id: Ulid::from_bytes([3u8; 16]),
                        origin_node_id: node(3),
                        origin_seq: 1,
                    },
                }],
            },
        );

        // Conflicted path: the previously stored entry is removed, not retained.
        overlay_realm_config_placement_reducer_materialization(&mut document, &reducer_state);
        assert!(document.placement_entry(entry.node_id).is_none());
    }
}
