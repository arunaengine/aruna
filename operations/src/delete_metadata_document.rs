use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncOutboxRecord, DocumentSyncTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::metadata::{
    MetadataDocumentDeleteRecord, MetadataDocumentLifecycleRecord, MetadataEffect, MetadataError,
    MetadataEvent, MetadataGraphLifecycleRecord, MetadataGraphPruneJobRecord,
};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{
    graph_revision_change, metadata_document_lifecycle_revision_change, updated_index_delete,
};
use aruna_core::structs::{
    MetadataAuditOperation, MetadataAuditRecord, MetadataRegistryRecord, PlacementRef,
    RealmConfigDocument,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::Effects;
use aruna_core::util::unix_timestamp_millis;
use byteview::ByteView;
use smallvec::smallvec;
use std::time::Duration;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::document_sync_outbox::{
    new_outbox_record_with_id, schedule_outbox_drain_effect, write_outbox_effect_with_txn,
};
use crate::driver::{DriverContext, drive};
use crate::metadata::prune_queue::{
    new_graph_prune_job, schedule_metadata_graph_prune_drain_effect, write_graph_prune_job_effect,
};
use crate::metadata::repository::{
    StorageReadError, delete_document_index_effect, delete_holders_effect, delete_registry_effect,
    parse_registry_read, read_registry_effect, write_audit_effect,
    write_document_lifecycle_with_revision_effect, write_graph_lifecycle_effect,
};
use crate::persistent_id::{
    MappingRoute, mapping_route_for, parse_mapping_read, read_mapping_effect, tombstone_transition,
};
use crate::placement::{registry_placement, resolve_shard_holders};

#[derive(Debug, PartialEq)]
pub struct DeleteMetadataDocumentOperation {
    actor: aruna_core::structs::Actor,
    group_id: Ulid,
    document_id: Ulid,
    record: Option<MetadataRegistryRecord>,
    lifecycle_record: Option<MetadataGraphLifecycleRecord>,
    document_lifecycle_record: Option<MetadataDocumentLifecycleRecord>,
    prune_job_record: Option<MetadataGraphPruneJobRecord>,
    document_lifecycle_placement_ref: PlacementRef,
    graph_lifecycle_placement_ref: PlacementRef,
    registry_placement_ref: PlacementRef,
    holder_peers: Vec<NodeId>,
    registry_peers: Vec<NodeId>,
    mapping_route: Option<MappingRoute>,
    /// Buckets the tombstones publish onto, read inside the write transaction.
    fence: crate::placement::fence::WriteFence,
    txn_id: Option<Ulid>,
    state: DeleteMetadataDocumentState,
    output: Option<Result<(), DeleteMetadataDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum DeleteMetadataDocumentState {
    Init,
    ReadRecord,
    ReadRealmConfig,
    StartTransaction,
    ReadFence,
    ReadBucketFence,
    WriteGraphLifecycle,
    WriteGraphPruneJob,
    WriteDocumentLifecycle,
    DeleteRegistry,
    DeleteDocumentIndex,
    DeleteHolders,
    DeleteUpdatedIndex,
    WriteAudit,
    WriteDocumentLifecycleOutbox,
    WriteGraphLifecycleOutbox,
    WriteDeleteOutbox,
    ReadPidMapping,
    WritePidTombstone,
    CommitTransaction,
    ScheduleGraphPruneQueue,
    PruneGraph,
    ScheduleGraphLifecycleSync,
    ScheduleDeleteSync,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteMetadataDocumentError {
    #[error(transparent)]
    StorageError(#[from] aruna_core::errors::StorageError),
    #[error(transparent)]
    ConversionError(#[from] aruna_core::errors::ConversionError),
    #[error(transparent)]
    MetadataError(#[from] MetadataError),
    #[error("document not found")]
    DocumentNotFound,
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("the document's bucket cut over to a new holder set; retry the delete")]
    PlacementFenced,
    #[error("document delete sync failed: {0}")]
    SyncDelete(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl DeleteMetadataDocumentOperation {
    pub fn new(actor: aruna_core::structs::Actor, group_id: Ulid, document_id: Ulid) -> Self {
        Self {
            actor,
            group_id,
            document_id,
            record: None,
            lifecycle_record: None,
            document_lifecycle_record: None,
            prune_job_record: None,
            document_lifecycle_placement_ref: PlacementRef::NIL,
            graph_lifecycle_placement_ref: PlacementRef::NIL,
            registry_placement_ref: PlacementRef::NIL,
            holder_peers: Vec::new(),
            registry_peers: Vec::new(),
            mapping_route: None,
            fence: Default::default(),
            txn_id: None,
            state: DeleteMetadataDocumentState::Init,
            output: None,
        }
    }

    fn fresh_copy(&self) -> Self {
        Self::new(self.actor.clone(), self.group_id, self.document_id)
    }

    /// Live holders of the document's bucket; the event-time stamp on the
    /// record is only the fallback for a realm without a readable config.
    fn peers(&self, record: &MetadataRegistryRecord) -> Vec<NodeId> {
        if self.holder_peers.is_empty() {
            record.holder_node_ids.clone()
        } else {
            self.holder_peers.clone()
        }
    }

    fn audit_record(&self, record: &MetadataRegistryRecord) -> MetadataAuditRecord {
        MetadataAuditRecord {
            realm_id: record.realm_id,
            group_id: record.group_id,
            document_id: record.document_id,
            graph_iri: record.graph_iri.clone(),
            user_id: self.actor.user_id,
            node_id: self.actor.node_id,
            operation: MetadataAuditOperation::Delete,
            occurred_at_ms: u64::try_from(chrono::Utc::now().timestamp_millis())
                .unwrap_or_default(),
            details: Some("delete metadata graph".to_string()),
        }
    }

    fn write_graph_lifecycle(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(DeleteMetadataDocumentError::MissingTransaction);
        };
        let Some(lifecycle_record) = self.lifecycle_record.as_ref() else {
            return self.fail(DeleteMetadataDocumentError::DocumentNotFound);
        };
        match write_graph_lifecycle_effect(lifecycle_record, Some(txn_id)) {
            Ok(effect) => {
                self.state = DeleteMetadataDocumentState::WriteGraphLifecycle;
                smallvec![effect]
            }
            Err(error) => self.fail(DeleteMetadataDocumentError::ConversionError(error)),
        }
    }

    fn lifecycle_record(&self, record: &MetadataRegistryRecord) -> MetadataGraphLifecycleRecord {
        MetadataGraphLifecycleRecord::deleted(
            record.graph_iri.clone(),
            record.realm_id,
            record.group_id,
            record.document_id,
            u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
        )
    }

    fn document_lifecycle_record(
        &self,
        record: &MetadataRegistryRecord,
        tombstone: MetadataGraphLifecycleRecord,
    ) -> MetadataDocumentLifecycleRecord {
        MetadataDocumentLifecycleRecord::Delete {
            event: MetadataDocumentDeleteRecord {
                event_id: Ulid::generate(),
                tombstone,
                deleted_after_event_id: record.last_event_id,
            },
        }
    }

    fn document_lifecycle_outbox_record(
        &self,
        record: &MetadataRegistryRecord,
    ) -> Result<DocumentSyncOutboxRecord, DeleteMetadataDocumentError> {
        let Some(lifecycle_record) = self.document_lifecycle_record.as_ref() else {
            return Err(DeleteMetadataDocumentError::DocumentNotFound);
        };
        let bytes = postcard::to_allocvec(lifecycle_record)
            .map_err(|error| DeleteMetadataDocumentError::ConversionError(error.into()))?;
        let change = metadata_document_lifecycle_revision_change(
            lifecycle_record,
            self.actor.node_id,
            self.document_lifecycle_placement_ref,
        );
        // Delete tombstones are published by the document's holder onto its own
        // per-document sync topics; the graph-lifecycle topic in particular is
        // first written here, so these writes must be able to mint their genesis
        // or the whole delete batch stalls. Per-document topics are single-origin,
        // so this does not risk the shared-topic fork this feature guards against.
        Ok(new_outbox_record_with_id(
            lifecycle_record.event_id(),
            self.actor.node_id,
            DocumentSyncTarget::MetadataDocumentLifecycle {
                document_id: record.document_id,
            },
            self.peers(record),
            DocumentSyncOutboxEvent::Upsert { bytes, change },
            self.document_lifecycle_placement_ref,
            true,
        )
        .fenced_at(
            self.fence
                .generation(&record.realm_id, &self.document_lifecycle_placement_ref),
        ))
    }

    fn document_lifecycle_outbox_effect(
        &self,
        record: &MetadataRegistryRecord,
        txn_id: Ulid,
    ) -> Result<Effects, DeleteMetadataDocumentError> {
        let outbox = self.document_lifecycle_outbox_record(record)?;
        Ok(smallvec![
            write_outbox_effect_with_txn(&outbox, Some(txn_id))
                .map_err(|error| { DeleteMetadataDocumentError::ConversionError(error.into()) })?
        ])
    }

    fn graph_lifecycle_outbox_effect(
        &self,
        record: &MetadataRegistryRecord,
        txn_id: Ulid,
    ) -> Result<Effects, DeleteMetadataDocumentError> {
        let Some(lifecycle_record) = self.lifecycle_record.as_ref() else {
            return Err(DeleteMetadataDocumentError::DocumentNotFound);
        };
        let bytes = postcard::to_allocvec(lifecycle_record)
            .map_err(|error| DeleteMetadataDocumentError::ConversionError(error.into()))?;
        let outbox_id = Ulid::generate();
        let change = graph_revision_change(
            lifecycle_record,
            outbox_id,
            self.actor.node_id,
            self.graph_lifecycle_placement_ref,
        );
        let outbox = new_outbox_record_with_id(
            outbox_id,
            self.actor.node_id,
            DocumentSyncTarget::MetadataGraphLifecycle {
                graph_iri: lifecycle_record.graph_iri.clone(),
            },
            self.peers(record),
            DocumentSyncOutboxEvent::Upsert { bytes, change },
            // First (and only) write to the per-graph lifecycle topic, so the
            // deleting holder originates and may mint its genesis.
            self.graph_lifecycle_placement_ref,
            true,
        )
        .fenced_at(
            self.fence
                .generation(&record.realm_id, &self.graph_lifecycle_placement_ref),
        );
        Ok(smallvec![
            write_outbox_effect_with_txn(&outbox, Some(txn_id))
                .map_err(|error| { DeleteMetadataDocumentError::ConversionError(error.into()) })?
        ])
    }

    fn graph_lifecycle_schedule_effect(&self) -> Result<Effects, DeleteMetadataDocumentError> {
        if self.lifecycle_record.is_none() {
            return Err(DeleteMetadataDocumentError::DocumentNotFound);
        }
        Ok(smallvec![schedule_outbox_drain_effect()])
    }

    fn registry_delete_outbox_effect(
        &self,
        record: &MetadataRegistryRecord,
        txn_id: Ulid,
    ) -> Result<Effects, DeleteMetadataDocumentError> {
        let Some(lifecycle_record) = self.document_lifecycle_record.as_ref() else {
            return Err(DeleteMetadataDocumentError::DocumentNotFound);
        };
        let change = metadata_document_lifecycle_revision_change(
            lifecycle_record,
            self.actor.node_id,
            self.registry_placement_ref,
        );
        let outbox = new_outbox_record_with_id(
            lifecycle_record.event_id(),
            self.actor.node_id,
            DocumentSyncTarget::MetadataRegistry {
                group_id: record.group_id,
                document_id: record.document_id,
            },
            self.registry_peers.clone(),
            DocumentSyncOutboxEvent::Delete { change },
            self.registry_placement_ref,
            true,
        )
        .fenced_at(
            self.fence
                .generation(&record.realm_id, &self.registry_placement_ref),
        );
        Ok(smallvec![
            write_outbox_effect_with_txn(&outbox, Some(txn_id))
                .map_err(|error| { DeleteMetadataDocumentError::ConversionError(error.into()) })?
        ])
    }

    fn registry_delete_schedule_effect(&self) -> Effects {
        smallvec![schedule_outbox_drain_effect()]
    }

    fn fail(&mut self, error: DeleteMetadataDocumentError) -> Effects {
        let cleanup = self.abort();
        self.state = DeleteMetadataDocumentState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(DeleteMetadataDocumentError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

/// The timestamp-index key is only reconstructible from the record's own
/// `updated_at_ms`, so it is deleted inside the same transaction as the row.
fn delete_index_effect(record: &MetadataRegistryRecord, txn_id: Option<Ulid>) -> Effect {
    let (key_space, key) = updated_index_delete(record);
    Effect::Storage(StorageEffect::Delete {
        key_space,
        key,
        txn_id,
    })
}

const DELETE_CONFLICT_RETRIES: usize = 3;

fn delete_retry_backoff(attempt: usize, document_id: Ulid) -> Duration {
    let base = crate::queue_backoff::retry_after_ms(attempt as u32, 25, 250);
    let mut head = [0u8; 8];
    head.copy_from_slice(&document_id.to_bytes()[..8]);
    let jitter = u64::from_le_bytes(head) % base;
    Duration::from_millis(base.saturating_add(jitter))
}

pub async fn delete_metadata_document(
    operation: DeleteMetadataDocumentOperation,
    context: &DriverContext,
    document_id: Ulid,
) -> Result<(), DeleteMetadataDocumentError> {
    let mut attempt = 0usize;
    loop {
        match drive(operation.fresh_copy(), context).await {
            Ok(()) => break,
            Err(DeleteMetadataDocumentError::StorageError(StorageError::TransactionConflict))
                if attempt < DELETE_CONFLICT_RETRIES =>
            {
                tokio::time::sleep(delete_retry_backoff(attempt, document_id)).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
    if let Some(metadata_handle) = context.metadata_handle.as_ref() {
        metadata_handle.remove_cached_registry_record(document_id);
    }
    Ok(())
}

impl Operation for DeleteMetadataDocumentOperation {
    type Output = ();
    type Error = DeleteMetadataDocumentError;

    fn start(&mut self) -> Effects {
        self.state = DeleteMetadataDocumentState::ReadRecord;
        smallvec![read_registry_effect(self.group_id, self.document_id, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            DeleteMetadataDocumentState::ReadRecord => match parse_registry_read(event) {
                Ok(Some(record)) => {
                    let realm_id = record.realm_id;
                    self.record = Some(record);
                    self.state = DeleteMetadataDocumentState::ReadRealmConfig;
                    smallvec![Effect::Storage(StorageEffect::Read {
                        key_space: REALM_CONFIG_KEYSPACE.to_string(),
                        key: ByteView::from(*realm_id.as_bytes()),
                        txn_id: None,
                    })]
                }
                Ok(None) => self.fail(DeleteMetadataDocumentError::DocumentNotFound),
                Err(StorageReadError::Storage(error)) => self.fail(error.into()),
                Err(StorageReadError::Conversion(error)) => self.fail(error.into()),
            },
            DeleteMetadataDocumentState::ReadRealmConfig => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(record) = self.record.as_ref() else {
                        return self.fail(DeleteMetadataDocumentError::DocumentNotFound);
                    };
                    if let Some(bytes) = value {
                        let config = match RealmConfigDocument::from_bytes(&bytes) {
                            Ok(config) => config,
                            Err(error) => {
                                return self
                                    .fail(DeleteMetadataDocumentError::ConversionError(error));
                            }
                        };
                        // Every record of the document rides the bucket its
                        // create stamped, so a tombstone lands on the topic the
                        // document itself lives on. Peers are that bucket's live
                        // holders, not the event-time stamp.
                        self.holder_peers = resolve_shard_holders(&config, &record.placement);
                        self.document_lifecycle_placement_ref = record.placement;
                        self.graph_lifecycle_placement_ref = record.placement;
                        // The registry row rides the registry class's own topic,
                        // not the document's capped bucket, so its tombstone must
                        // go where the row went: to every node that has one.
                        self.registry_placement_ref = registry_placement(&config, record);
                        self.registry_peers =
                            resolve_shard_holders(&config, &self.registry_placement_ref);
                        // The PID mapping rides the placement derived from the
                        // structured id, not the deletable registry row, so its
                        // tombstone keeps routing after this delete commits.
                        self.mapping_route = mapping_route_for(
                            &config,
                            record.realm_id,
                            self.document_id,
                            self.actor.node_id,
                        );
                        let mapping_placement =
                            self.mapping_route.as_ref().map(|route| route.placement);
                        self.fence.add(
                            record.realm_id,
                            &config,
                            [record.placement, self.registry_placement_ref]
                                .into_iter()
                                .chain(mapping_placement),
                        );
                    }
                    self.state = DeleteMetadataDocumentState::StartTransaction;
                    smallvec![Effect::Storage(StorageEffect::StartTransaction {
                        read: false
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("realm config read result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.txn_id = Some(txn_id);
                    self.state = DeleteMetadataDocumentState::ReadFence;
                    smallvec![read_registry_effect(
                        self.group_id,
                        self.document_id,
                        Some(txn_id),
                    )]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::ReadFence => match parse_registry_read(event) {
                Ok(Some(record)) => {
                    let lifecycle_record = self.lifecycle_record(&record);
                    self.document_lifecycle_record =
                        Some(self.document_lifecycle_record(&record, lifecycle_record.clone()));
                    self.prune_job_record = Some(new_graph_prune_job(
                        lifecycle_record.graph_iri.clone(),
                        unix_timestamp_millis(),
                    ));
                    self.lifecycle_record = Some(lifecycle_record);
                    self.record = Some(record);
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(DeleteMetadataDocumentError::MissingTransaction);
                    };
                    if self.fence.is_empty() {
                        return self.write_graph_lifecycle();
                    }
                    // The fence joins this transaction's read set, so a
                    // departing holder's close conflicts an uncommitted delete.
                    self.state = DeleteMetadataDocumentState::ReadBucketFence;
                    smallvec![Effect::Storage(StorageEffect::BatchRead {
                        reads: self.fence.reads(),
                        txn_id: Some(txn_id),
                    })]
                }
                Ok(None) => self.fail(DeleteMetadataDocumentError::DocumentNotFound),
                Err(StorageReadError::Storage(error)) => self.fail(error.into()),
                Err(StorageReadError::Conversion(error)) => self.fail(error.into()),
            },
            DeleteMetadataDocumentState::ReadBucketFence => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    if !self.fence.admits(&values) {
                        return self.fail(DeleteMetadataDocumentError::PlacementFenced);
                    }
                    self.write_graph_lifecycle()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("bucket fence read result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::WriteGraphLifecycle => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(DeleteMetadataDocumentError::MissingTransaction);
                    };
                    let Some(prune_job_record) = self.prune_job_record.as_ref() else {
                        return self.fail(DeleteMetadataDocumentError::DocumentNotFound);
                    };
                    self.state = DeleteMetadataDocumentState::WriteGraphPruneJob;
                    match write_graph_prune_job_effect(prune_job_record, Some(txn_id)) {
                        Ok(effect) => smallvec![effect],
                        Err(error) => {
                            self.fail(DeleteMetadataDocumentError::ConversionError(error))
                        }
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => {
                    self.unexpected_event("graph lifecycle write result", format!("{other:?}"))
                }
            },
            DeleteMetadataDocumentState::WriteGraphPruneJob => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(DeleteMetadataDocumentError::MissingTransaction);
                    };
                    let Some(document_lifecycle_record) = self.document_lifecycle_record.as_ref()
                    else {
                        return self.fail(DeleteMetadataDocumentError::DocumentNotFound);
                    };
                    self.state = DeleteMetadataDocumentState::WriteDocumentLifecycle;
                    match write_document_lifecycle_with_revision_effect(
                        document_lifecycle_record,
                        self.actor.node_id,
                        self.document_lifecycle_placement_ref,
                        Some(txn_id),
                    ) {
                        Ok(effect) => smallvec![effect],
                        Err(error) => {
                            self.fail(DeleteMetadataDocumentError::ConversionError(error))
                        }
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => {
                    self.unexpected_event("graph prune job write result", format!("{other:?}"))
                }
            },
            DeleteMetadataDocumentState::WriteDocumentLifecycle => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(DeleteMetadataDocumentError::MissingTransaction);
                    };
                    self.state = DeleteMetadataDocumentState::DeleteRegistry;
                    smallvec![delete_registry_effect(
                        self.group_id,
                        self.document_id,
                        Some(txn_id)
                    )]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => {
                    self.unexpected_event("document lifecycle write result", format!("{other:?}"))
                }
            },
            DeleteMetadataDocumentState::DeleteRegistry => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(DeleteMetadataDocumentError::MissingTransaction);
                    };
                    self.state = DeleteMetadataDocumentState::DeleteDocumentIndex;
                    smallvec![delete_document_index_effect(self.document_id, Some(txn_id))]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("registry delete result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::DeleteDocumentIndex => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(DeleteMetadataDocumentError::MissingTransaction);
                    };
                    self.state = DeleteMetadataDocumentState::DeleteHolders;
                    smallvec![delete_holders_effect(
                        self.group_id,
                        self.document_id,
                        Some(txn_id)
                    )]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => {
                    self.unexpected_event("document index delete result", format!("{other:?}"))
                }
            },
            DeleteMetadataDocumentState::DeleteHolders => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(DeleteMetadataDocumentError::MissingTransaction);
                    };
                    let Some(record) = self.record.as_ref() else {
                        return self.fail(DeleteMetadataDocumentError::DocumentNotFound);
                    };
                    self.state = DeleteMetadataDocumentState::DeleteUpdatedIndex;
                    smallvec![delete_index_effect(record, Some(txn_id))]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("holders delete result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::DeleteUpdatedIndex => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(DeleteMetadataDocumentError::MissingTransaction);
                    };
                    let Some(record) = self.record.as_ref() else {
                        return self.fail(DeleteMetadataDocumentError::DocumentNotFound);
                    };
                    self.state = DeleteMetadataDocumentState::WriteAudit;
                    match write_audit_effect(
                        &self.audit_record(record),
                        Ulid::generate(),
                        Some(txn_id),
                    ) {
                        Ok(effect) => smallvec![effect],
                        Err(error) => {
                            self.fail(DeleteMetadataDocumentError::ConversionError(error))
                        }
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("updated index delete result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::WriteAudit => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(DeleteMetadataDocumentError::MissingTransaction);
                    };
                    let Some(record) = self.record.as_ref() else {
                        return self.fail(DeleteMetadataDocumentError::DocumentNotFound);
                    };
                    self.state = DeleteMetadataDocumentState::WriteDocumentLifecycleOutbox;
                    match self.document_lifecycle_outbox_effect(record, txn_id) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("audit write result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::WriteDocumentLifecycleOutbox => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(DeleteMetadataDocumentError::MissingTransaction);
                    };
                    let Some(record) = self.record.as_ref() else {
                        return self.fail(DeleteMetadataDocumentError::DocumentNotFound);
                    };
                    self.state = DeleteMetadataDocumentState::WriteGraphLifecycleOutbox;
                    match self.graph_lifecycle_outbox_effect(record, txn_id) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.fail(DeleteMetadataDocumentError::SyncDelete(format!(
                        "metadata document lifecycle outbox write failed: {error}"
                    )))
                }
                other => self.unexpected_event(
                    "document lifecycle outbox write result",
                    format!("{other:?}"),
                ),
            },
            DeleteMetadataDocumentState::WriteGraphLifecycleOutbox => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(DeleteMetadataDocumentError::MissingTransaction);
                    };
                    let Some(record) = self.record.as_ref() else {
                        return self.fail(DeleteMetadataDocumentError::DocumentNotFound);
                    };
                    self.state = DeleteMetadataDocumentState::WriteDeleteOutbox;
                    match self.registry_delete_outbox_effect(record, txn_id) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.fail(DeleteMetadataDocumentError::SyncDelete(format!(
                        "metadata graph tombstone outbox write failed: {error}"
                    )))
                }
                other => self
                    .unexpected_event("graph lifecycle outbox write result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::WriteDeleteOutbox => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(DeleteMetadataDocumentError::MissingTransaction);
                    };
                    self.state = DeleteMetadataDocumentState::ReadPidMapping;
                    smallvec![read_mapping_effect(self.document_id, Some(txn_id))]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.fail(DeleteMetadataDocumentError::SyncDelete(format!(
                        "metadata registry delete outbox write failed: {error}"
                    )))
                }
                other => self
                    .unexpected_event("document delete outbox write result", format!("{other:?}")),
            },
            // The PID tombstone commits with the registry row it retires: a
            // best-effort write afterwards can be lost to a crash and leave a
            // deleted document's PID Active forever.
            DeleteMetadataDocumentState::ReadPidMapping => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(DeleteMetadataDocumentError::MissingTransaction);
                };
                let existing = match parse_mapping_read(event) {
                    Ok(existing) => existing,
                    Err(error) => {
                        return self.fail(DeleteMetadataDocumentError::SyncDelete(format!(
                            "persistent id mapping read failed: {error}"
                        )));
                    }
                };
                let transition = match existing.as_ref() {
                    None => Ok(None),
                    Some(mapping) => tombstone_transition(
                        Some(mapping),
                        &self.mapping_route,
                        self.document_id,
                        unix_timestamp_millis(),
                    ),
                };
                match transition {
                    Ok(Some((_, writes))) => {
                        self.state = DeleteMetadataDocumentState::WritePidTombstone;
                        smallvec![Effect::Storage(StorageEffect::BatchWrite {
                            writes,
                            txn_id: Some(txn_id),
                        })]
                    }
                    Ok(None) => {
                        self.state = DeleteMetadataDocumentState::CommitTransaction;
                        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                    }
                    Err(error) => self.fail(DeleteMetadataDocumentError::SyncDelete(format!(
                        "persistent id tombstone encode failed: {error}"
                    ))),
                }
            }
            DeleteMetadataDocumentState::WritePidTombstone => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(DeleteMetadataDocumentError::MissingTransaction);
                    };
                    self.state = DeleteMetadataDocumentState::CommitTransaction;
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.fail(DeleteMetadataDocumentError::SyncDelete(format!(
                        "persistent id tombstone write failed: {error}"
                    )))
                }
                other => self
                    .unexpected_event("persistent id tombstone write result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::CommitTransaction => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = DeleteMetadataDocumentState::ScheduleGraphPruneQueue;
                    smallvec![schedule_metadata_graph_prune_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::ScheduleGraphPruneQueue => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    let Some(record) = self.record.clone() else {
                        return self.fail(DeleteMetadataDocumentError::DocumentNotFound);
                    };
                    self.state = DeleteMetadataDocumentState::PruneGraph;
                    smallvec![Effect::Metadata(MetadataEffect::DeleteGraph {
                        graph_iri: record.graph_iri,
                    })]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(message = %message, "Failed to schedule metadata graph prune queue; durable prune job remains queued");
                    let Some(record) = self.record.clone() else {
                        return self.fail(DeleteMetadataDocumentError::DocumentNotFound);
                    };
                    self.state = DeleteMetadataDocumentState::PruneGraph;
                    smallvec![Effect::Metadata(MetadataEffect::DeleteGraph {
                        graph_iri: record.graph_iri,
                    })]
                }
                other => self
                    .unexpected_event("metadata graph prune timer schedule", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::PruneGraph => match event {
                Event::Metadata(MetadataEvent::GraphDeleted { .. }) => {
                    if self.record.is_none() {
                        return self.fail(DeleteMetadataDocumentError::DocumentNotFound);
                    }
                    self.state = DeleteMetadataDocumentState::ScheduleGraphLifecycleSync;
                    match self.graph_lifecycle_schedule_effect() {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Metadata(MetadataEvent::Error { error, .. }) => {
                    warn!(error = ?error, "Failed to prune local metadata graph; tombstone remains committed");
                    if self.record.is_none() {
                        return self.fail(DeleteMetadataDocumentError::DocumentNotFound);
                    }
                    self.state = DeleteMetadataDocumentState::ScheduleGraphLifecycleSync;
                    match self.graph_lifecycle_schedule_effect() {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                other => self.unexpected_event("metadata graph prune result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::ScheduleGraphLifecycleSync => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    if self.record.is_none() {
                        return self.fail(DeleteMetadataDocumentError::DocumentNotFound);
                    }
                    self.state = DeleteMetadataDocumentState::ScheduleDeleteSync;
                    self.registry_delete_schedule_effect()
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    self.fail(DeleteMetadataDocumentError::SyncDelete(format!(
                        "durable metadata graph sync scheduling failed: {message}"
                    )))
                }
                other => self
                    .unexpected_event("metadata graph sync timer schedule", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::ScheduleDeleteSync => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = DeleteMetadataDocumentState::Finish;
                    self.output = Some(Ok(()));
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    self.fail(DeleteMetadataDocumentError::SyncDelete(format!(
                        "durable metadata delete sync scheduling failed: {message}"
                    )))
                }
                other => self
                    .unexpected_event("metadata delete sync timer schedule", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::Finish
            | DeleteMetadataDocumentState::Error
            | DeleteMetadataDocumentState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DeleteMetadataDocumentState::Finish | DeleteMetadataDocumentState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(()))
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
    use aruna_core::document::{DocumentSyncChange, DocumentSyncChangeKind};
    use aruna_core::keyspaces::{
        DOCUMENT_SYNC_REVISION_KEYSPACE, METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
        METADATA_GRAPH_PRUNE_JOB_KEYSPACE, PERSISTENT_ID_MAPPING_KEYSPACE,
    };
    use aruna_core::storage_entries::document_sync_revision_key;
    use aruna_core::structs::{
        PersistentIdMapping, PersistentIdStatus, PlacementStrategy, RealmId, RealmNodeKind,
        persistent_id_key,
    };

    fn actor() -> aruna_core::structs::Actor {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        aruna_core::structs::Actor {
            node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
            realm_id,
        }
    }

    fn record(actor: &aruna_core::structs::Actor) -> MetadataRegistryRecord {
        let group_id = Ulid::generate();
        let document_id = Ulid::generate();
        let document_path = "datasets/delete-lifecycle";
        let last_event_id = Ulid::generate();
        MetadataRegistryRecord {
            realm_id: actor.realm_id,
            group_id,
            document_id,
            document_path: document_path.to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &actor.realm_id,
                group_id,
                document_path,
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: vec![actor.node_id],
            created_at_ms: 1,
            updated_at_ms: 2,
            establishing_event_id: last_event_id,
            last_event_id,
        }
    }

    fn outbox_from_effects(effects: Effects) -> DocumentSyncOutboxRecord {
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected outbox write, got {effects:?}");
        };
        postcard::from_bytes(value.as_ref()).expect("outbox record decodes")
    }

    #[test]
    fn delete_writes_document_lifecycle_tombstone_outbox_with_fence() {
        let actor = actor();
        let record = record(&actor);
        let mut operation = DeleteMetadataDocumentOperation::new(
            actor.clone(),
            record.group_id,
            record.document_id,
        );
        let tombstone = operation.lifecycle_record(&record);
        operation.document_lifecycle_record =
            Some(operation.document_lifecycle_record(&record, tombstone.clone()));

        let outbox = operation
            .document_lifecycle_outbox_record(&record)
            .expect("outbox record builds");

        assert_eq!(
            outbox.target,
            DocumentSyncTarget::MetadataDocumentLifecycle {
                document_id: record.document_id
            }
        );
        let DocumentSyncOutboxEvent::Upsert { bytes, change } = outbox.event else {
            panic!("expected lifecycle upsert outbox event");
        };
        assert_eq!(change.kind, DocumentSyncChangeKind::Delete);
        let lifecycle: MetadataDocumentLifecycleRecord =
            postcard::from_bytes(&bytes).expect("lifecycle payload decodes");
        let MetadataDocumentLifecycleRecord::Delete { event } = lifecycle else {
            panic!("expected delete lifecycle payload");
        };
        assert_eq!(event.event_id, outbox.outbox_id);
        assert_eq!(event.tombstone, tombstone);
        assert_eq!(event.deleted_after_event_id, record.last_event_id);
    }

    #[test]
    fn delete_drops_index() {
        let actor = actor();
        let record = record(&actor);
        let txn_id = Ulid::generate();
        let mut operation = DeleteMetadataDocumentOperation::new(
            actor.clone(),
            record.group_id,
            record.document_id,
        );
        operation.record = Some(record.clone());
        operation.txn_id = Some(txn_id);
        operation.state = DeleteMetadataDocumentState::DeleteHolders;

        let effects = operation.step(Event::Storage(StorageEvent::DeleteResult {
            key: ByteView::from(Vec::new()),
        }));
        let [
            Effect::Storage(StorageEffect::Delete {
                key_space,
                key,
                txn_id: effect_txn,
            }),
        ] = effects.as_slice()
        else {
            panic!("expected an updated-index delete, got {effects:?}");
        };
        assert_eq!(
            key_space,
            aruna_core::keyspaces::METADATA_UPDATED_INDEX_KEYSPACE
        );
        assert_eq!(
            key.as_ref(),
            aruna_core::storage_entries::updated_index_key(
                record.updated_at_ms,
                record.document_id
            )
            .as_ref()
        );
        assert_eq!(*effect_txn, Some(txn_id));
    }

    // Every record of a document rides the bucket its create stamped, so all
    // three tombstones land on the topic the document itself lives on.
    #[test]
    fn delete_rides_stored_bucket() {
        let actor = actor();
        let mut record = record(&actor);
        // Deterministic identity so the placement collision check cannot flake on
        // the timestamp bytes of a freshly generated document id.
        record.group_id = Ulid::from_bytes([3; 16]);
        record.document_id = Ulid::from_bytes([5; 16]);
        record.graph_iri = MetadataRegistryRecord::graph_iri_for(record.document_id);
        record.permission_path = MetadataRegistryRecord::permission_path_for(
            &record.realm_id,
            record.group_id,
            &record.document_path,
            record.document_id,
        );
        let mut config = RealmConfigDocument::new(record.realm_id, Vec::new(), 3);
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([1; 16]),
            name: "default".to_string(),
            replica_count: Some(1),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 64,
        };
        config.default_strategy_id = Some(strategy.strategy_id);
        config.strategies = vec![strategy.clone()];
        config.ensure_node(actor.node_id, RealmNodeKind::Server);
        record.placement = crate::placement::choose_origin_bucket(
            &config,
            &strategy,
            actor.node_id,
            &record.document_id.to_bytes(),
        )
        .expect("origin holds a bucket");

        let mut operation = DeleteMetadataDocumentOperation::new(
            actor.clone(),
            record.group_id,
            record.document_id,
        );
        let _ = operation.start();
        let _ = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: crate::metadata::repository::metadata_registry_key(
                record.group_id,
                record.document_id,
            ),
            value: Some(postcard::to_allocvec(&record).unwrap().into()),
        }));
        let _ = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(*record.realm_id.as_bytes()),
            value: Some(postcard::to_allocvec(&config).unwrap().into()),
        }));
        let txn_id = Ulid::generate();
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read {
                txn_id: Some(read_txn_id),
                ..
            })] if *read_txn_id == txn_id
        ));
        let _ = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: crate::metadata::repository::metadata_registry_key(
                record.group_id,
                record.document_id,
            ),
            value: Some(postcard::to_allocvec(&record).unwrap().into()),
        }));

        assert_eq!(operation.document_lifecycle_placement_ref, record.placement);
        assert_eq!(operation.graph_lifecycle_placement_ref, record.placement);
        // The registry tombstone follows the registry row, which rides the
        // everywhere-bound registry class rather than the document's capped bucket.
        let registry_ref = registry_placement(&config, &record);
        assert_eq!(operation.registry_placement_ref, registry_ref);
        assert_ne!(registry_ref, record.placement);

        let document_outbox = operation
            .document_lifecycle_outbox_record(&record)
            .expect("document lifecycle outbox builds");
        let graph_outbox = outbox_from_effects(
            operation
                .graph_lifecycle_outbox_effect(&record, Ulid::generate())
                .expect("graph lifecycle outbox builds"),
        );
        let registry_outbox = outbox_from_effects(
            operation
                .registry_delete_outbox_effect(&record, Ulid::generate())
                .expect("registry delete outbox builds"),
        );
        let DocumentSyncOutboxEvent::Upsert {
            change: document_change,
            ..
        } = document_outbox.event
        else {
            panic!("expected document lifecycle upsert");
        };
        let DocumentSyncOutboxEvent::Upsert {
            change: graph_change,
            ..
        } = graph_outbox.event
        else {
            panic!("expected graph lifecycle upsert");
        };
        let DocumentSyncOutboxEvent::Delete {
            change: registry_change,
        } = registry_outbox.event
        else {
            panic!("expected registry delete");
        };
        assert_eq!(document_change.placement, record.placement);
        assert_eq!(graph_change.placement, record.placement);
        assert_eq!(registry_change.placement, registry_ref);
        assert_eq!(document_outbox.peers, vec![actor.node_id]);
    }

    /// A realm whose buckets are activated at generation one, so a delete
    /// resolves a generation and takes the bucket's fence.
    fn activated_config(
        record: &mut MetadataRegistryRecord,
        actor: &aruna_core::structs::Actor,
    ) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(record.realm_id, Vec::new(), 3);
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([1; 16]),
            name: "default".to_string(),
            replica_count: Some(1),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 64,
        };
        config.default_strategy_id = Some(strategy.strategy_id);
        config.strategies = vec![strategy.clone()];
        config.ensure_node(actor.node_id, RealmNodeKind::Server);
        config.snapshot_candidate_map();
        record.placement = crate::placement::choose_origin_bucket(
            &config,
            &strategy,
            actor.node_id,
            &record.document_id.to_bytes(),
        )
        .expect("origin holds a bucket");
        config
    }

    /// Steps a delete to the bucket fence read and answers every fence with
    /// `closed`.
    fn step_to_fence(
        operation: &mut DeleteMetadataDocumentOperation,
        record: &MetadataRegistryRecord,
        config: &RealmConfigDocument,
        txn_id: Ulid,
        closed: Option<u64>,
    ) -> Effects {
        let registry_read = || {
            Event::Storage(StorageEvent::ReadResult {
                key: crate::metadata::repository::metadata_registry_key(
                    record.group_id,
                    record.document_id,
                ),
                value: Some(postcard::to_allocvec(record).unwrap().into()),
            })
        };
        operation.start();
        operation.step(registry_read());
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(*record.realm_id.as_bytes()),
            value: Some(postcard::to_allocvec(config).unwrap().into()),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let effects = operation.step(registry_read());
        let [
            Effect::Storage(StorageEffect::BatchRead {
                reads,
                txn_id: read,
            }),
        ] = effects.as_slice()
        else {
            panic!("the transaction batch-reads the bucket fences, got {effects:?}");
        };
        assert_eq!(*read, Some(txn_id));
        let values = reads
            .iter()
            .map(|(_, key)| {
                (
                    key.clone(),
                    closed.map(|generation| ByteView::from(generation.to_be_bytes().to_vec())),
                )
            })
            .collect();
        operation.step(Event::Storage(StorageEvent::BatchReadResult { values }))
    }

    #[test]
    fn delete_takes_fence() {
        // An admitted delete stamps the generation it resolved at onto every
        // tombstone row, so the drain can bound the predecessor set.
        let actor = actor();
        let mut record = record(&actor);
        let config = activated_config(&mut record, &actor);
        let mut operation = DeleteMetadataDocumentOperation::new(
            actor.clone(),
            record.group_id,
            record.document_id,
        );

        let effects = step_to_fence(&mut operation, &record, &config, Ulid::generate(), None);
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::Write { .. })]
            ),
            "an admitted delete writes the graph lifecycle tombstone, got {effects:?}"
        );
        let document_outbox = operation
            .document_lifecycle_outbox_record(&record)
            .expect("document lifecycle outbox builds");
        let graph_outbox = outbox_from_effects(
            operation
                .graph_lifecycle_outbox_effect(&record, Ulid::generate())
                .expect("graph lifecycle outbox builds"),
        );
        let registry_outbox = outbox_from_effects(
            operation
                .registry_delete_outbox_effect(&record, Ulid::generate())
                .expect("registry delete outbox builds"),
        );
        assert_eq!(document_outbox.generation, 1);
        assert_eq!(graph_outbox.generation, 1);
        assert_eq!(registry_outbox.generation, 1);
    }

    #[test]
    fn fence_rejects_delete() {
        // The departing holder closed generation one: the delete must not
        // commit a tombstone the drained bucket can no longer publish.
        let actor = actor();
        let mut record = record(&actor);
        let config = activated_config(&mut record, &actor);
        let mut operation = DeleteMetadataDocumentOperation::new(
            actor.clone(),
            record.group_id,
            record.document_id,
        );

        let effects = step_to_fence(&mut operation, &record, &config, Ulid::generate(), Some(1));
        assert!(
            !effects.iter().any(|effect| matches!(
                effect,
                Effect::Storage(StorageEffect::Write { .. })
                    | Effect::Storage(StorageEffect::BatchWrite { .. })
            )),
            "a fenced delete writes nothing, got {effects:?}"
        );
        assert_eq!(
            operation.finalize().unwrap_err(),
            DeleteMetadataDocumentError::PlacementFenced
        );
    }

    #[test]
    fn delete_writes_prune_job_in_same_transaction_before_commit() {
        let actor = actor();
        let record = record(&actor);
        let mut fenced = record.clone();
        fenced.last_event_id = Ulid::from_parts(8, 8);
        let mut operation = DeleteMetadataDocumentOperation::new(
            actor.clone(),
            record.group_id,
            record.document_id,
        );
        let effects = operation.start();
        assert_eq!(effects.len(), 1);

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: crate::metadata::repository::metadata_registry_key(
                record.group_id,
                record.document_id,
            ),
            value: Some(postcard::to_allocvec(&record).unwrap().into()),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(*record.realm_id.as_bytes()),
            value: None,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        ));

        let txn_id = Ulid::generate();
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read {
                txn_id: Some(read_txn_id),
                ..
            })] if *read_txn_id == txn_id
        ));

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: crate::metadata::repository::metadata_registry_key(
                fenced.group_id,
                fenced.document_id,
            ),
            value: Some(postcard::to_allocvec(&fenced).unwrap().into()),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { txn_id: Some(write_txn_id), .. })]
                if *write_txn_id == txn_id
        ));

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: crate::metadata::repository::metadata_graph_lifecycle_key(&record.graph_iri),
        }));
        let [
            Effect::Storage(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: Some(write_txn_id),
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected prune job write effect");
        };
        assert_eq!(key_space, METADATA_GRAPH_PRUNE_JOB_KEYSPACE);
        assert_eq!(*write_txn_id, txn_id);
        let job: MetadataGraphPruneJobRecord = postcard::from_bytes(value).unwrap();
        assert_eq!(job.graph_iri, record.graph_iri);
        assert_eq!(job.attempts, 0);
        assert_eq!(job.last_error, None);

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: key.clone(),
        }));
        let [
            Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: Some(write_txn_id),
            }),
        ] = effects.as_slice()
        else {
            panic!("expected document lifecycle revision batch, got {effects:?}");
        };
        assert_eq!(*write_txn_id, txn_id);

        let lifecycle = writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == METADATA_DOCUMENT_LIFECYCLE_KEYSPACE)
            .map(|(_, _, value)| {
                postcard::from_bytes::<MetadataDocumentLifecycleRecord>(value)
                    .expect("lifecycle record decodes")
            })
            .expect("lifecycle write exists");
        let MetadataDocumentLifecycleRecord::Delete { event } = lifecycle else {
            panic!("expected delete lifecycle record");
        };
        assert_eq!(event.deleted_after_event_id, fenced.last_event_id);
        let target = DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: record.document_id,
        };
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
        assert_eq!(revision_key, &document_sync_revision_key(&target));
        assert_eq!(revision.current.event_id, event.event_id);
        assert_eq!(revision.current.actor, actor.node_id);
        assert_eq!(revision.current.generation, event.tombstone.updated_at_ms);
        assert_eq!(revision.kind, DocumentSyncChangeKind::Delete);
    }

    #[test]
    fn delete_fence_missing() {
        let actor = actor();
        let record = record(&actor);
        let txn_id = Ulid::generate();
        let mut operation =
            DeleteMetadataDocumentOperation::new(actor, record.group_id, record.document_id);

        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: crate::metadata::repository::metadata_registry_key(
                record.group_id,
                record.document_id,
            ),
            value: Some(postcard::to_allocvec(&record).unwrap().into()),
        }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(*record.realm_id.as_bytes()),
            value: None,
        }));
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read {
                txn_id: Some(read_txn_id),
                ..
            })] if *read_txn_id == txn_id
        ));

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: crate::metadata::repository::metadata_registry_key(
                record.group_id,
                record.document_id,
            ),
            value: None,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                if *abort_txn == txn_id
        ));
        assert_eq!(
            operation.finalize(),
            Err(DeleteMetadataDocumentError::DocumentNotFound)
        );
    }

    // The PID tombstone is part of the delete, not a follow-up: a crash after the
    // commit must not be able to leave a deleted document's mapping Active.
    #[test]
    // A delete without a minted pid writes no tombstone: the registry-row
    // fence blocks later mints and the landing URL must stay a plain 404.
    fn delete_skips_unminted() {
        let actor = actor();
        let record = record(&actor);
        let txn_id = Ulid::generate();
        let mut operation =
            DeleteMetadataDocumentOperation::new(actor, record.group_id, record.document_id);
        operation.record = Some(record.clone());
        operation.txn_id = Some(txn_id);
        operation.state = DeleteMetadataDocumentState::WriteDeleteOutbox;

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: ByteView::from(record.document_id.to_bytes().to_vec()),
        }));
        let [
            Effect::Storage(StorageEffect::Read {
                key_space,
                txn_id: Some(read_txn_id),
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected a persistent id mapping read, got {effects:?}");
        };
        assert_eq!(key_space, PERSISTENT_ID_MAPPING_KEYSPACE);
        assert_eq!(*read_txn_id, txn_id);

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(persistent_id_key(record.document_id)),
            value: None,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: commit_txn })]
                if *commit_txn == txn_id
        ));
    }

    #[test]
    // A delete of a minted document tombstones the mapping in the same
    // transaction that removes the registry row.
    fn delete_tombstones_minted() {
        let actor = actor();
        let record = record(&actor);
        let txn_id = Ulid::generate();
        let minted = PersistentIdMapping::conceptual(
            record.document_id,
            actor.user_id,
            aruna_core::structs::PersistentIdRevision {
                event_id: Ulid::generate(),
                actor: actor.node_id,
                occurred_at_ms: 1,
            },
        );
        let mut operation =
            DeleteMetadataDocumentOperation::new(actor, record.group_id, record.document_id);
        operation.record = Some(record.clone());
        operation.txn_id = Some(txn_id);
        operation.state = DeleteMetadataDocumentState::ReadPidMapping;
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(persistent_id_key(record.document_id)),
            value: Some(ByteView::from(minted.to_bytes().expect("mapping encodes"))),
        }));
        let [
            Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: Some(write_txn_id),
            }),
        ] = effects.as_slice()
        else {
            panic!("expected a persistent id tombstone batch, got {effects:?}");
        };
        assert_eq!(*write_txn_id, txn_id);
        let mapping = writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == PERSISTENT_ID_MAPPING_KEYSPACE)
            .map(|(_, _, value)| PersistentIdMapping::from_bytes(value).expect("mapping decodes"))
            .expect("the batch carries the mapping row");
        assert_eq!(mapping.target, record.document_id);
        assert_eq!(mapping.status, PersistentIdStatus::Tombstoned);

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: commit_txn })]
                if *commit_txn == txn_id
        ));
    }

    #[test]
    fn delete_ignores_prune_timer_schedule_error_after_job_commit() {
        let actor = actor();
        let record = record(&actor);
        let mut operation =
            DeleteMetadataDocumentOperation::new(actor, record.group_id, record.document_id);
        operation.record = Some(record.clone());
        operation.state = DeleteMetadataDocumentState::ScheduleGraphPruneQueue;

        let effects = operation.step(Event::Task(TaskEvent::Error {
            key: Some(aruna_core::task::TaskKey::DrainMetadataGraphPruneQueue),
            message: "scheduler unavailable".to_string(),
        }));

        assert_eq!(operation.state, DeleteMetadataDocumentState::PruneGraph);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Metadata(MetadataEffect::DeleteGraph { graph_iri })]
                if graph_iri == &record.graph_iri
        ));
    }
}
