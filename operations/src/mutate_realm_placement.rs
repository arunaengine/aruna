use aruna_core::NodeId;
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
use aruna_core::structs::{
    Actor, BindingScope, NodePlacementEntry, PlacementOverride, PlacementStrategy,
    RealmConfigDocument, StrategyBinding,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::placement::placement_ref_for_target;
use crate::sync_placement::schedule_placement_revalidation_effect;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RealmPlacementMutation {
    UpsertNode(NodePlacementEntry),
    RemoveNode(NodeId),
    UpsertStrategy(PlacementStrategy),
    RemoveStrategy(Ulid),
    SetDefaultStrategy(Ulid),
    SetBinding(StrategyBinding),
    RemoveBinding(BindingScope),
    SetOverride(PlacementOverride),
    RemoveOverride(Vec<u8>),
}

impl RealmPlacementMutation {
    fn admin_operation(&self) -> AdminDocumentOperation {
        match self {
            Self::UpsertNode(entry) => AdminDocumentOperation::RealmConfigNodePlacementSet {
                entry: entry.clone(),
            },
            Self::RemoveNode(node_id) => {
                AdminDocumentOperation::RealmConfigNodePlacementRemoved { node_id: *node_id }
            }
            Self::UpsertStrategy(strategy) => {
                AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                    strategy: strategy.clone(),
                }
            }
            Self::RemoveStrategy(strategy_id) => {
                AdminDocumentOperation::RealmConfigPlacementStrategyRemoved {
                    strategy_id: *strategy_id,
                }
            }
            Self::SetDefaultStrategy(strategy_id) => {
                AdminDocumentOperation::RealmConfigDefaultStrategySet {
                    strategy_id: *strategy_id,
                }
            }
            Self::SetBinding(binding) => AdminDocumentOperation::RealmConfigStrategyBindingSet {
                binding: binding.clone(),
            },
            Self::RemoveBinding(scope) => {
                AdminDocumentOperation::RealmConfigStrategyBindingRemoved {
                    scope: scope.clone(),
                }
            }
            Self::SetOverride(record) => AdminDocumentOperation::RealmConfigPlacementOverrideSet {
                record: record.clone(),
            },
            Self::RemoveOverride(subject) => {
                AdminDocumentOperation::RealmConfigPlacementOverrideRemoved {
                    subject: subject.clone(),
                }
            }
        }
    }

    fn validate(&self, document: &RealmConfigDocument) -> Result<(), MutateRealmPlacementError> {
        match self {
            Self::UpsertStrategy(strategy) if strategy.replica_count == Some(0) => {
                Err(MutateRealmPlacementError::InvalidInput(
                    "placement strategy replica_count must not be zero".to_string(),
                ))
            }
            Self::SetDefaultStrategy(strategy_id) => {
                require_strategy(document, strategy_id, "default strategy")
            }
            Self::SetBinding(binding) => {
                require_strategy(document, &binding.strategy_id, "binding")
            }
            Self::SetOverride(record) => match &record.strategy_id {
                Some(strategy_id) => require_strategy(document, strategy_id, "override"),
                None => Ok(()),
            },
            Self::RemoveStrategy(strategy_id) => {
                let referenced = document.default_strategy_id == Some(*strategy_id)
                    || document
                        .strategy_bindings
                        .iter()
                        .any(|binding| binding.strategy_id == *strategy_id)
                    || document
                        .placement_overrides
                        .iter()
                        .any(|record| record.strategy_id == Some(*strategy_id));
                if referenced {
                    Err(MutateRealmPlacementError::StrategyReferenced {
                        strategy_id: *strategy_id,
                    })
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }
}

fn require_strategy(
    document: &RealmConfigDocument,
    strategy_id: &Ulid,
    reference: &str,
) -> Result<(), MutateRealmPlacementError> {
    if document.strategy(strategy_id).is_none() {
        return Err(MutateRealmPlacementError::InvalidInput(format!(
            "{reference} references missing strategy {strategy_id}"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutateRealmPlacementConfig {
    pub actor: Actor,
    pub mutation: RealmPlacementMutation,
}

#[derive(Debug, PartialEq)]
pub struct MutateRealmPlacementOperation {
    config: MutateRealmPlacementConfig,
    txn_id: Option<TxnId>,
    state: MutateRealmPlacementState,
    output: Option<Result<RealmConfigDocument, MutateRealmPlacementError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum MutateRealmPlacementState {
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
    ScheduleDocumentSyncOutboxDrain,
    SchedulePlacementRevalidation,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum MutateRealmPlacementError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentReducerError(#[from] AdminDocumentReducerError),
    #[error("realm config document missing")]
    RealmConfigNotFound,
    #[error("invalid placement mutation: {0}")]
    InvalidInput(String),
    #[error("placement strategy {strategy_id} is currently referenced")]
    StrategyReferenced { strategy_id: Ulid },
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl MutateRealmPlacementOperation {
    pub fn new(config: MutateRealmPlacementConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: MutateRealmPlacementState::Init,
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
        self.state = MutateRealmPlacementState::ReadCurrent;
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
    ) -> Result<Effects, MutateRealmPlacementError> {
        let Some(txn_id) = self.txn_id else {
            return Err(MutateRealmPlacementError::MissingTransaction);
        };
        let Some(document_value) = document_value else {
            return Err(MutateRealmPlacementError::RealmConfigNotFound);
        };
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;
        self.config.mutation.validate(&document)?;

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
        let admin_event = reducer_state
            .apply_operation(&self.config.actor, self.config.mutation.admin_operation())?;
        overlay_realm_config_placement_reducer_materialization(&mut document, &reducer_state);

        let stale_conflict_deletes = stale_admin_document_conflict_delete_entries(
            previous_reducer_state.as_ref(),
            Some(&reducer_state),
        );
        let document_target = self.document_ref();
        let placement = placement_ref_for_target(&document, &document_target, Default::default());
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
            placement,
            false,
        );
        writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        writes.extend(admin_document_conflict_write_entries(&reducer_state)?);

        self.output = Some(Ok(document.clone()));
        self.state = MutateRealmPlacementState::WriteDocumentAndAdminState {
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
            return self.fail(MutateRealmPlacementError::MissingTransaction);
        };
        self.state = MutateRealmPlacementState::CommitTransaction { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: MutateRealmPlacementError) -> Effects {
        let cleanup = self.abort();
        self.state = MutateRealmPlacementState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(MutateRealmPlacementError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for MutateRealmPlacementOperation {
    type Output = RealmConfigDocument;
    type Error = MutateRealmPlacementError;

    fn start(&mut self) -> Effects {
        self.state = MutateRealmPlacementState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            MutateRealmPlacementState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            MutateRealmPlacementState::ReadCurrent => match event {
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
            MutateRealmPlacementState::WriteDocumentAndAdminState {
                document,
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(MutateRealmPlacementError::MissingTransaction);
                    };
                    if !stale_conflict_deletes.is_empty() {
                        self.state =
                            MutateRealmPlacementState::DeleteStaleAdminConflicts { document };
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
            MutateRealmPlacementState::DeleteStaleAdminConflicts { document } => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    self.emit_commit_transaction(document)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            MutateRealmPlacementState::CommitTransaction { .. } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = MutateRealmPlacementState::ScheduleDocumentSyncOutboxDrain;
                    smallvec![schedule_outbox_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            MutateRealmPlacementState::ScheduleDocumentSyncOutboxDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = MutateRealmPlacementState::SchedulePlacementRevalidation;
                    smallvec![schedule_placement_revalidation_effect(
                        self.config.actor.realm_id,
                        self.config.actor.node_id,
                    )]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document operation outbox drain; durable outbox remains retryable");
                    self.state = MutateRealmPlacementState::SchedulePlacementRevalidation;
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
            MutateRealmPlacementState::SchedulePlacementRevalidation => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = MutateRealmPlacementState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule placement revalidation after realm placement mutation");
                    self.state = MutateRealmPlacementState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "placement revalidation timer schedule",
                    format!("{other:?}"),
                ),
            },
            MutateRealmPlacementState::Finish
            | MutateRealmPlacementState::Error
            | MutateRealmPlacementState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            MutateRealmPlacementState::Finish | MutateRealmPlacementState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .expect("realm placement mutation operation must set output")
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
    use std::collections::BTreeMap;

    use aruna_core::document::DocumentSyncTarget;
    use aruna_core::events::StorageEvent;
    use aruna_core::structs::{DEFAULT_NODE_WEIGHT, DocumentClass, RealmId};
    use aruna_core::task::{TaskEffect, TaskKey};
    use aruna_core::types::UserId;
    use tempfile::tempdir;

    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::get_realm_config::GetRealmConfigOperation;

    fn node(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn actor(realm_id: RealmId) -> Actor {
        Actor {
            node_id: node(1),
            user_id: UserId::local(Ulid::from_bytes([1; 16]), realm_id),
            realm_id,
        }
    }

    fn context(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        }
    }

    async fn seed_config(context: &DriverContext, actor: &Actor) -> RealmConfigDocument {
        let mut document = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
        document.seed_default_placement();
        let target = DocumentSyncTarget::RealmConfig {
            realm_id: actor.realm_id,
        };
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: document.to_bytes(actor).unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        document
    }

    async fn mutate(
        context: &DriverContext,
        actor: &Actor,
        mutation: RealmPlacementMutation,
    ) -> Result<RealmConfigDocument, MutateRealmPlacementError> {
        drive(
            MutateRealmPlacementOperation::new(MutateRealmPlacementConfig {
                actor: actor.clone(),
                mutation,
            }),
            context,
        )
        .await
    }

    fn strategy(strategy_id: Ulid) -> PlacementStrategy {
        PlacementStrategy {
            strategy_id,
            name: "hot".to_string(),
            replica_count: Some(2),
            distinct_locations: true,
            affinity: Vec::new(),
            shard_count: 64,
        }
    }

    #[tokio::test]
    async fn strategy_default_binding_and_override_lifecycle() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([3; 32]);
        let actor = actor(realm_id);
        let initial = seed_config(&context, &actor).await;
        let initial_default = initial.default_strategy_id.unwrap();
        let strategy_id = Ulid::from_bytes([8; 16]);
        let scope = BindingScope::Class(DocumentClass::Metadata);
        let subject = vec![0xab, 0xcd];

        mutate(
            &context,
            &actor,
            RealmPlacementMutation::UpsertStrategy(strategy(strategy_id)),
        )
        .await
        .unwrap();
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::SetDefaultStrategy(strategy_id),
        )
        .await
        .unwrap();
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::SetBinding(StrategyBinding {
                scope: scope.clone(),
                strategy_id,
            }),
        )
        .await
        .unwrap();
        let stored = mutate(
            &context,
            &actor,
            RealmPlacementMutation::SetOverride(PlacementOverride {
                subject: subject.clone(),
                pinned: vec![node(2)],
                excluded: vec![node(3)],
                strategy_id: Some(strategy_id),
            }),
        )
        .await
        .unwrap();

        assert_eq!(stored.default_strategy_id, Some(strategy_id));
        assert!(stored.strategy(&strategy_id).is_some());
        assert!(
            stored
                .strategy_bindings
                .iter()
                .any(|binding| { binding.scope == scope && binding.strategy_id == strategy_id })
        );
        assert!(stored.placement_overrides.iter().any(|record| {
            record.subject == subject && record.strategy_id == Some(strategy_id)
        }));

        mutate(
            &context,
            &actor,
            RealmPlacementMutation::RemoveOverride(subject),
        )
        .await
        .unwrap();
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::RemoveBinding(scope.clone()),
        )
        .await
        .unwrap();
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::SetDefaultStrategy(initial_default),
        )
        .await
        .unwrap();
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::RemoveStrategy(strategy_id),
        )
        .await
        .unwrap();

        let stored = drive(GetRealmConfigOperation::new(realm_id), &context)
            .await
            .unwrap();
        assert!(stored.strategy(&strategy_id).is_none());
        assert_eq!(stored.default_strategy_id, Some(initial_default));
        assert!(
            !stored
                .strategy_bindings
                .iter()
                .any(|binding| binding.scope == scope)
        );
        assert!(
            !stored
                .placement_overrides
                .iter()
                .any(|record| record.subject == vec![0xab, 0xcd])
        );
    }

    #[tokio::test]
    async fn node_placement_lifecycle() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([12; 32]);
        let actor = actor(realm_id);
        seed_config(&context, &actor).await;
        let entry = NodePlacementEntry {
            node_id: node(2),
            location: "eu-west".to_string(),
            weight: 250,
            full: false,
            draining: false,
            labels: BTreeMap::new(),
        };

        let stored = mutate(
            &context,
            &actor,
            RealmPlacementMutation::UpsertNode(entry.clone()),
        )
        .await
        .unwrap();
        assert_eq!(stored.placement_entry(entry.node_id), Some(&entry));

        let stored = mutate(
            &context,
            &actor,
            RealmPlacementMutation::RemoveNode(entry.node_id),
        )
        .await
        .unwrap();
        assert!(stored.placement_entry(entry.node_id).is_none());
    }

    #[tokio::test]
    async fn node_placement_rejects_reserved_kind_label() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([13; 32]);
        let actor = actor(realm_id);
        seed_config(&context, &actor).await;
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

        assert!(matches!(
            mutate(&context, &actor, RealmPlacementMutation::UpsertNode(entry)).await,
            Err(MutateRealmPlacementError::AdminDocumentReducerError(
                AdminDocumentReducerError::ReservedPlacementLabel
            ))
        ));
    }

    #[tokio::test]
    async fn local_validation_rejects_zero_and_dangling_references() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([4; 32]);
        let actor = actor(realm_id);
        seed_config(&context, &actor).await;
        let missing = Ulid::from_bytes([9; 16]);

        let mut zero = strategy(missing);
        zero.replica_count = Some(0);
        assert!(matches!(
            mutate(
                &context,
                &actor,
                RealmPlacementMutation::UpsertStrategy(zero)
            )
            .await,
            Err(MutateRealmPlacementError::InvalidInput(reason)) if reason.contains("zero")
        ));

        for mutation in [
            RealmPlacementMutation::SetDefaultStrategy(missing),
            RealmPlacementMutation::SetBinding(StrategyBinding {
                scope: BindingScope::Realm,
                strategy_id: missing,
            }),
            RealmPlacementMutation::SetOverride(PlacementOverride {
                subject: vec![1],
                pinned: Vec::new(),
                excluded: Vec::new(),
                strategy_id: Some(missing),
            }),
        ] {
            assert!(matches!(
                mutate(&context, &actor, mutation).await,
                Err(MutateRealmPlacementError::InvalidInput(reason)) if reason.contains("missing strategy")
            ));
        }
    }

    #[tokio::test]
    async fn removing_a_referenced_strategy_is_a_conflict() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([5; 32]);
        let actor = actor(realm_id);
        let document = seed_config(&context, &actor).await;
        let strategy_id = document.default_strategy_id.unwrap();

        assert_eq!(
            mutate(
                &context,
                &actor,
                RealmPlacementMutation::RemoveStrategy(strategy_id)
            )
            .await,
            Err(MutateRealmPlacementError::StrategyReferenced { strategy_id })
        );
    }

    #[tokio::test]
    async fn missing_realm_config_is_not_found() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([11; 32]);
        let actor = actor(realm_id);

        assert_eq!(
            mutate(
                &context,
                &actor,
                RealmPlacementMutation::RemoveOverride(Vec::new())
            )
            .await,
            Err(MutateRealmPlacementError::RealmConfigNotFound)
        );
    }

    #[test]
    fn successful_mutation_schedules_zero_delay_revalidation() {
        let realm_id = RealmId::from_bytes([6; 32]);
        let actor = actor(realm_id);
        let mut operation = MutateRealmPlacementOperation::new(MutateRealmPlacementConfig {
            actor: actor.clone(),
            mutation: RealmPlacementMutation::RemoveOverride(Vec::new()),
        });
        operation.state = MutateRealmPlacementState::ScheduleDocumentSyncOutboxDrain;

        let effects = operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainDocumentSyncOutbox,
            after: std::time::Duration::ZERO,
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Task(TaskEffect::ResetTimer {
                key: TaskKey::SyncPlacements {
                    realm_id: scheduled_realm,
                    node_id,
                },
                after,
            })] if *scheduled_realm == realm_id && *node_id == actor.node_id && after.is_zero()
        ));
    }

    #[test]
    fn override_without_strategy_is_valid() {
        let realm_id = RealmId::from_bytes([7; 32]);
        let document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        assert!(
            RealmPlacementMutation::SetOverride(PlacementOverride {
                subject: vec![1],
                pinned: Vec::new(),
                excluded: Vec::new(),
                strategy_id: None,
            })
            .validate(&document)
            .is_ok()
        );
    }

    #[test]
    fn affinity_data_is_not_changed_by_operation_input() {
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([10; 16]),
            name: "affinity".to_string(),
            replica_count: None,
            distinct_locations: false,
            shard_count: 64,
            affinity: vec![aruna_core::structs::AffinityRule {
                matcher: aruna_core::structs::LabelMatch {
                    key: "tier".to_string(),
                    value: "hot".to_string(),
                },
                effect: aruna_core::structs::AffinityEffect::Multiply { permille: 1500 },
            }],
        };
        let mutation = RealmPlacementMutation::UpsertStrategy(strategy.clone());
        assert!(matches!(
            mutation.admin_operation(),
            AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy: stored }
                if stored == strategy
        ));
    }
}
