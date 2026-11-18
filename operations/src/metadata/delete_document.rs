//! Deletes a metadata document by writing tombstones, audit rows and a graph prune job.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord, DocumentTarget};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{ID_MAPPING_KEYSPACE, METADATA_INDEX_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::metadata::{
    GraphLifecycleRecord, GraphPruneRecord, MetadataDeleteRecord, MetadataEffect, MetadataError,
    MetadataLifecycleRecord,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::storage_entries::{
    graph_lifecycle_entry, graph_prune_entry, graph_revision_change, lifecycle_revision_change,
    metadata_registry_key, registry_delete_entries,
};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::metadata_registry::{
    MetadataAuditOperation, MetadataAuditRecord, MetadataRegistryRecord,
};
use aruna_core::structs::{PersistentIdMapping, persistent_id_key, secondary_index_deletes};
use aruna_core::types::{Key, KeySpace};
use byteview::ByteView;
use thiserror::Error;
use ulid::Ulid;

use crate::metadata::MetadataPhaseSource;
use crate::metadata::id_mapping::{MappingRoute, mapping_route_for, tombstone_transition};
use crate::metadata::repository::{audit_entry, lifecycle_revision_entries, schedule_prune_drain};
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::placement::fence::WriteFence;
use aruna_core::placement::{registry_placement, resolve_shard_holders};

#[derive(Debug, Error, PartialEq)]
pub enum DeleteDocumentError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    MetadataError(#[from] MetadataError),
    #[error("document not found")]
    DocumentNotFound,
    #[error("the document's bucket cut over to a new holder set; retry the delete")]
    PlacementFenced,
    #[error("document delete sync failed: {0}")]
    SyncDelete(String),
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for DeleteDocumentError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            other => Self::Io(other),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum DeleteStage {
    /// Reading the registry row.
    Record,
    /// Reading the realm config and the persistent id mapping.
    Config,
    /// Reading the bucket fences.
    Fence,
    /// Writing the tombstones, the prune job, the audit row and the outbox records.
    Write,
    /// Deleting the registry rows and the persistent id index.
    Delete,
}

type DeleteTurn = Result<Next<()>, DeleteDocumentError>;

/// Deletes one document in a single write transaction: the registry row and its indexes go,
/// tombstones, a prune job, an audit row and the outbox records come. After the commit the
/// prune queue, the local graph prune and the sync drain run as best-effort calls.
#[derive(Debug, PartialEq)]
pub struct DeleteDocumentOperation {
    actor: Actor,
    group_id: Ulid,
    document_id: Ulid,
    record: Option<MetadataRegistryRecord>,
    document_placement_ref: PlacementRef,
    registry_placement_ref: PlacementRef,
    holder_peers: Vec<NodeId>,
    registry_peers: Vec<NodeId>,
    mapping_route: Option<MappingRoute>,
    mapping: Option<PersistentIdMapping>,
    /// Buckets the tombstones publish onto, read inside the write transaction.
    fence: WriteFence,
    /// Registry and persistent id rows deleted once the writes are sent.
    deletes: Vec<(KeySpace, Key)>,
    /// Phase-time and identity sampling; production keeps the defaults.
    phase_source: MetadataPhaseSource,
    stage: DeleteStage,
}

impl DeleteDocumentOperation {
    pub fn new(actor: Actor, group_id: Ulid, document_id: Ulid) -> Self {
        Self {
            actor,
            group_id,
            document_id,
            record: None,
            document_placement_ref: PlacementRef::NIL,
            registry_placement_ref: PlacementRef::NIL,
            holder_peers: Vec::new(),
            registry_peers: Vec::new(),
            mapping_route: None,
            mapping: None,
            fence: WriteFence::default(),
            deletes: Vec::new(),
            phase_source: MetadataPhaseSource::default(),
            stage: DeleteStage::Record,
        }
    }

    /// Replaces the phase-time and identity source so a test can fix a trace.
    pub(crate) fn with_phase_source(mut self, phase_source: MetadataPhaseSource) -> Self {
        self.phase_source = phase_source;
        self
    }

    pub(crate) fn fresh_copy(&self) -> Self {
        Self::new(self.actor.clone(), self.group_id, self.document_id)
            .with_phase_source(self.phase_source)
    }

    fn record(&self) -> Result<&MetadataRegistryRecord, DeleteDocumentError> {
        self.record
            .as_ref()
            .ok_or(DeleteDocumentError::DocumentNotFound)
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

    fn record_read(&mut self, result: IoResult) -> DeleteTurn {
        let bytes = result
            .value()?
            .ok_or(DeleteDocumentError::DocumentNotFound)?;
        let record: MetadataRegistryRecord =
            postcard::from_bytes(&bytes).map_err(ConversionError::from)?;
        let reads = vec![
            (
                REALM_CONFIG_KEYSPACE.to_string(),
                ByteView::from(*record.realm_id.as_bytes()),
            ),
            (
                ID_MAPPING_KEYSPACE.to_string(),
                ByteView::from(persistent_id_key(self.document_id)),
            ),
        ];
        self.record = Some(record);
        self.stage = DeleteStage::Config;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn config_read(&mut self, result: IoResult) -> DeleteTurn {
        let [config, mapping] =
            <[_; 2]>::try_from(result.values()?).map_err(|_| IoError::Unexpected {
                expected: "the realm config and the mapping",
                got: "another count of values",
            })?;
        self.mapping = mapping
            .map(|bytes| PersistentIdMapping::from_bytes(&bytes))
            .transpose()
            .map_err(|error| {
                DeleteDocumentError::SyncDelete(format!(
                    "persistent id mapping read failed: {error}"
                ))
            })?;
        if let Some(bytes) = config {
            self.place(&RealmConfigDocument::from_bytes(&bytes)?)?;
        }
        if self.fence.is_empty() {
            return self.write_all();
        }
        // The fence joins this transaction's read set, so a departing holder's close
        // conflicts an uncommitted delete.
        self.stage = DeleteStage::Fence;
        Ok(Next::Call(IoCall::Read(self.fence.reads())))
    }

    /// Resolves the buckets and holders every tombstone publishes to.
    fn place(&mut self, config: &RealmConfigDocument) -> Result<(), DeleteDocumentError> {
        let record = self.record()?.clone();
        // Every record rides the bucket its create stamped, so a tombstone lands on the
        // document's own topic.
        self.holder_peers = resolve_shard_holders(config, &record.placement);
        self.document_placement_ref = record.placement;
        // The registry tombstone follows its everywhere-bound registry topic.
        self.registry_placement_ref = registry_placement(config, &record);
        self.registry_peers = resolve_shard_holders(config, &self.registry_placement_ref);
        // The PID tombstone keeps the structured id's placement after deletion.
        self.mapping_route = mapping_route_for(
            config,
            record.realm_id,
            self.document_id,
            self.actor.node_id,
        );
        let mapping_placement = self.mapping_route.as_ref().map(|route| route.placement);
        self.fence.add(
            record.realm_id,
            config,
            [record.placement, self.registry_placement_ref]
                .into_iter()
                .chain(mapping_placement),
        );
        Ok(())
    }

    fn fence_read(&mut self, result: IoResult) -> DeleteTurn {
        let keys = self.fence.reads().into_iter().map(|(_, key)| key);
        let rows: Vec<_> = keys.zip(result.values()?).collect();
        if !self.fence.admits(&rows) {
            return Err(DeleteDocumentError::PlacementFenced);
        }
        self.write_all()
    }

    /// Every row the delete writes, with the deletes kept for the next call.
    fn write_all(&mut self) -> DeleteTurn {
        let record = self.record()?.clone();
        // Sample the phase once: the tombstone time and the prune job's creation share this
        // delete's single wall-clock reading.
        let deleted_at_ms = self.phase_source.now_ms();
        let event_id = self.phase_source.next_id();
        let tombstone = GraphLifecycleRecord::deleted(
            record.graph_iri.clone(),
            record.realm_id,
            record.group_id,
            record.document_id,
            deleted_at_ms,
        );
        let lifecycle = MetadataLifecycleRecord::Delete {
            event: MetadataDeleteRecord {
                event_id,
                tombstone: tombstone.clone(),
                deleted_after_id: record.last_event_id,
            },
        };
        let prune_job = GraphPruneRecord::new(tombstone.graph_iri.clone(), deleted_at_ms);
        let mut writes = vec![
            graph_lifecycle_entry(&tombstone)?,
            graph_prune_entry(&prune_job)?,
        ];
        writes.extend(lifecycle_revision_entries(
            &lifecycle,
            self.actor.node_id,
            self.document_placement_ref,
        )?);
        let occurred_at_ms = self.phase_source.now_ms();
        let audit_id = self.phase_source.next_id();
        writes.push(audit_entry(
            &self.audit_record(&record, occurred_at_ms),
            audit_id,
        )?);
        let outbox_id = self.phase_source.next_id();
        for outbox in [
            self.lifecycle_outbox(&record, &lifecycle)?,
            self.graph_outbox(&record, &tombstone, outbox_id)?,
            self.delete_outbox(&record, &lifecycle),
        ] {
            writes.push(outbox_write_entry(&outbox).map_err(ConversionError::from)?);
        }
        self.deletes = registry_delete_entries(&record);
        // Commit the PID tombstone with the registry row so a crash never leaves it active.
        let transition = match self.mapping.as_ref() {
            None => Ok(None),
            Some(mapping) => tombstone_transition(
                Some(mapping),
                &self.mapping_route,
                self.document_id,
                self.phase_source.now_ms(),
            ),
        };
        match transition {
            Ok(Some((mapping, pid_writes))) => {
                writes.extend(pid_writes);
                self.deletes.extend(secondary_index_deletes(&mapping));
            }
            Ok(None) => {}
            Err(error) => {
                return Err(DeleteDocumentError::SyncDelete(format!(
                    "persistent id tombstone encode failed: {error}"
                )));
            }
        }
        self.stage = DeleteStage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn audit_record(
        &self,
        record: &MetadataRegistryRecord,
        occurred_at_ms: u64,
    ) -> MetadataAuditRecord {
        MetadataAuditRecord {
            realm_id: record.realm_id,
            group_id: record.group_id,
            document_id: record.document_id,
            graph_iri: record.graph_iri.clone(),
            user_id: self.actor.user_id,
            node_id: self.actor.node_id,
            operation: MetadataAuditOperation::Delete,
            occurred_at_ms,
            details: Some("delete metadata graph".to_string()),
        }
    }

    fn lifecycle_outbox(
        &self,
        record: &MetadataRegistryRecord,
        lifecycle: &MetadataLifecycleRecord,
    ) -> Result<DocumentOutboxRecord, DeleteDocumentError> {
        let bytes = postcard::to_allocvec(lifecycle).map_err(ConversionError::from)?;
        let change =
            lifecycle_revision_change(lifecycle, self.actor.node_id, self.document_placement_ref);
        // A delete mints the graph-lifecycle topic genesis before publishing its tombstone.
        Ok(new_identified_record(
            lifecycle.event_id(),
            self.actor.node_id,
            DocumentTarget::MetadataDocumentLifecycle {
                document_id: record.document_id,
            },
            self.peers(record),
            DocumentOutboxEvent::Upsert { bytes, change },
            self.document_placement_ref,
            true,
        )
        .fenced_at(
            self.fence
                .generation(&record.realm_id, &self.document_placement_ref),
        ))
    }

    fn graph_outbox(
        &self,
        record: &MetadataRegistryRecord,
        tombstone: &GraphLifecycleRecord,
        outbox_id: Ulid,
    ) -> Result<DocumentOutboxRecord, DeleteDocumentError> {
        let bytes = postcard::to_allocvec(tombstone).map_err(ConversionError::from)?;
        let change = graph_revision_change(
            tombstone,
            outbox_id,
            self.actor.node_id,
            self.document_placement_ref,
        );
        // First (and only) write to the per-graph lifecycle topic, so the deleting holder
        // originates and may mint its genesis.
        Ok(new_identified_record(
            outbox_id,
            self.actor.node_id,
            DocumentTarget::MetadataGraphLifecycle {
                graph_iri: tombstone.graph_iri.clone(),
            },
            self.peers(record),
            DocumentOutboxEvent::Upsert { bytes, change },
            self.document_placement_ref,
            true,
        )
        .fenced_at(
            self.fence
                .generation(&record.realm_id, &self.document_placement_ref),
        ))
    }

    fn delete_outbox(
        &self,
        record: &MetadataRegistryRecord,
        lifecycle: &MetadataLifecycleRecord,
    ) -> DocumentOutboxRecord {
        let change =
            lifecycle_revision_change(lifecycle, self.actor.node_id, self.registry_placement_ref);
        new_identified_record(
            lifecycle.event_id(),
            self.actor.node_id,
            DocumentTarget::MetadataRegistry {
                group_id: record.group_id,
                document_id: record.document_id,
            },
            self.registry_peers.clone(),
            DocumentOutboxEvent::Delete { change },
            self.registry_placement_ref,
            true,
        )
        .fenced_at(
            self.fence
                .generation(&record.realm_id, &self.registry_placement_ref),
        )
    }

    /// After the commit: wake the prune queue, prune the local graph and drain the outbox.
    /// The durable prune job and outbox rows cover any of them that fails.
    fn committed(&self) -> DeleteTurn {
        let record = self.record()?;
        let prune = MetadataEffect::DeleteGraph {
            graph_iri: record.graph_iri.clone(),
        };
        Ok(Next::Then {
            output: (),
            calls: vec![
                IoCall::Effect(Box::new(schedule_prune_drain())),
                IoCall::Effect(Box::new(aruna_core::effects::Effect::Metadata(prune))),
                IoCall::Effect(Box::new(schedule_drain_effect())),
            ],
        })
    }
}

impl Operation for DeleteDocumentOperation {
    type Output = ();
    type Error = DeleteDocumentError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> DeleteTurn {
        let key = metadata_registry_key(self.group_id, self.document_id);
        let read = vec![(METADATA_INDEX_KEYSPACE.to_string(), key)];
        Ok(Next::Call(IoCall::Read(read)))
    }

    fn next(&mut self, result: IoResult) -> DeleteTurn {
        match self.stage {
            DeleteStage::Record => self.record_read(result),
            DeleteStage::Config => self.config_read(result),
            DeleteStage::Fence => self.fence_read(result),
            DeleteStage::Write => {
                result.done()?;
                self.stage = DeleteStage::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            DeleteStage::Delete => {
                result.done()?;
                self.committed()
            }
        }
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use aruna_core::document::DocumentChangeKind;
    use aruna_core::effects::Effect;
    use aruna_core::keyspaces::{
        DOCUMENT_LIFECYCLE_KEYSPACE, GRAPH_LIFECYCLE_KEYSPACE, METADATA_AUDIT_KEYSPACE,
        PRUNE_JOB_KEYSPACE, SYNC_OUTBOX_KEYSPACE, SYNC_REVISION_KEYSPACE,
    };
    use aruna_core::structs::PersistentIdStatus;
    use aruna_core::structs::execution::job::JobId;
    use aruna_core::structs::identity::realm::{RealmId, RealmNodeKind};
    use aruna_core::structs::placement::record::PlacementStrategy;

    use aruna_core::types::Value;

    type Rows = Vec<(KeySpace, Key, Value)>;

    fn actor() -> Actor {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            user_id: aruna_core::UserId::local(Ulid::from_parts(1, 1), realm_id),
            realm_id,
        }
    }

    fn record(actor: &Actor) -> MetadataRegistryRecord {
        let group_id = Ulid::from_bytes([3; 16]);
        let document_id = Ulid::from_bytes([5; 16]);
        let document_path = "datasets/delete-lifecycle";
        let last_event_id = Ulid::from_parts(4, 4);
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

    /// A realm with one strategy; `activated` snapshots the candidate map, so the buckets
    /// carry a generation and the delete reads their fences.
    fn config(record: &mut MetadataRegistryRecord, actor: &Actor, activated: bool) -> Value {
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
        if activated {
            config.snapshot_candidate_map();
        }
        record.placement = aruna_core::placement::choose_origin_bucket(
            &config,
            &strategy,
            actor.node_id,
            &record.document_id.to_bytes(),
        )
        .expect("origin holds a bucket");
        postcard::to_allocvec(&config).unwrap().into()
    }

    fn encoded<T: serde::Serialize>(value: &T) -> Option<Value> {
        Some(postcard::to_allocvec(value).unwrap().into())
    }

    fn fixed_ms() -> u64 {
        1_700_000_000_000
    }

    fn fixed_id() -> Ulid {
        Ulid::from_bytes([0x7a; 16])
    }

    fn operation(actor: &Actor, record: &MetadataRegistryRecord) -> DeleteDocumentOperation {
        DeleteDocumentOperation::new(actor.clone(), record.group_id, record.document_id)
            .with_phase_source(MetadataPhaseSource::fixed(fixed_ms, fixed_id))
    }

    /// Answers the registry read with `record` and the config read with `config` and
    /// `mapping`; returns the next turn.
    fn after_reads(
        operation: &mut DeleteDocumentOperation,
        record: &MetadataRegistryRecord,
        config: Option<Value>,
        mapping: Option<Value>,
    ) -> DeleteTurn {
        operation.first().unwrap();
        operation
            .next(IoResult::Values(vec![encoded(record)]))
            .unwrap();
        operation.next(IoResult::Values(vec![config, mapping]))
    }

    fn written(turn: DeleteTurn) -> Rows {
        match turn {
            Ok(Next::Call(IoCall::Write(rows))) => rows,
            other => panic!("expected the delete's writes, got {other:?}"),
        }
    }

    fn rows_in<'a>(rows: &'a Rows, key_space: &str) -> Vec<&'a Value> {
        rows.iter()
            .filter(|(space, _, _)| space == key_space)
            .map(|(_, _, value)| value)
            .collect()
    }

    fn outboxes(rows: &Rows) -> Vec<DocumentOutboxRecord> {
        rows_in(rows, SYNC_OUTBOX_KEYSPACE)
            .into_iter()
            .map(|value| postcard::from_bytes(value).expect("outbox record decodes"))
            .collect()
    }

    #[test]
    fn delete_runs_in_order() {
        // One transaction reads the row, the config and the mapping, writes, then deletes;
        // the prune queue, graph prune and sync drain follow the commit.
        let actor = actor();
        let record = record(&actor);
        let mut operation = operation(&actor, &record);
        assert_eq!(operation.transaction(), TransactionMode::Write);
        let key = metadata_registry_key(record.group_id, record.document_id);
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Read(vec![(
                METADATA_INDEX_KEYSPACE.to_string(),
                key
            )])))
        );
        let reads = vec![
            (
                REALM_CONFIG_KEYSPACE.to_string(),
                ByteView::from(*record.realm_id.as_bytes()),
            ),
            (
                ID_MAPPING_KEYSPACE.to_string(),
                ByteView::from(persistent_id_key(record.document_id)),
            ),
        ];
        assert_eq!(
            operation.next(IoResult::Values(vec![encoded(&record)])),
            Ok(Next::Call(IoCall::Read(reads)))
        );
        let rows = written(operation.next(IoResult::Values(vec![None, None])));
        for space in [
            GRAPH_LIFECYCLE_KEYSPACE,
            PRUNE_JOB_KEYSPACE,
            DOCUMENT_LIFECYCLE_KEYSPACE,
            SYNC_REVISION_KEYSPACE,
            METADATA_AUDIT_KEYSPACE,
        ] {
            assert_eq!(rows_in(&rows, space).len(), 1, "{space}");
        }
        assert_eq!(outboxes(&rows).len(), 3);
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Call(IoCall::Delete(registry_delete_entries(&record))))
        );
        let prune = MetadataEffect::DeleteGraph {
            graph_iri: record.graph_iri.clone(),
        };
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Then {
                output: (),
                calls: vec![
                    IoCall::Effect(Box::new(schedule_prune_drain())),
                    IoCall::Effect(Box::new(Effect::Metadata(prune))),
                    IoCall::Effect(Box::new(schedule_drain_effect())),
                ],
            })
        );
    }

    #[test]
    fn delete_writes_tombstones() {
        // The lifecycle tombstone follows the stored row, and the prune job starts fresh.
        let actor = actor();
        let mut stored = record(&actor);
        stored.last_event_id = Ulid::from_parts(8, 8);
        let rows = written(after_reads(
            &mut operation(&actor, &stored),
            &stored,
            None,
            None,
        ));
        let job: GraphPruneRecord =
            postcard::from_bytes(rows_in(&rows, PRUNE_JOB_KEYSPACE)[0]).unwrap();
        assert_eq!(job.graph_iri, stored.graph_iri);
        assert_eq!(job.attempts, 0);
        assert_eq!(job.due_at_ms, fixed_ms());
        let lifecycle: MetadataLifecycleRecord =
            postcard::from_bytes(rows_in(&rows, DOCUMENT_LIFECYCLE_KEYSPACE)[0]).unwrap();
        let MetadataLifecycleRecord::Delete { event } = lifecycle else {
            panic!("expected a delete lifecycle record");
        };
        assert_eq!(event.deleted_after_id, stored.last_event_id);
        assert_eq!(event.event_id, fixed_id());
        assert_eq!(event.tombstone.updated_at_ms, fixed_ms());
        let document = &outboxes(&rows)[0];
        assert_eq!(
            document.target,
            DocumentTarget::MetadataDocumentLifecycle {
                document_id: stored.document_id
            }
        );
        let DocumentOutboxEvent::Upsert { change, .. } = &document.event else {
            panic!("expected a lifecycle upsert");
        };
        assert_eq!(change.kind, DocumentChangeKind::Delete);
        assert_eq!(document.peers, vec![actor.node_id]);
    }

    // Every record of a document rides the bucket its create stamped, so the lifecycle
    // tombstones land on the document's own topic and the registry one on the registry's.
    #[test]
    fn delete_rides_stored() {
        let actor = actor();
        let mut stored = record(&actor);
        let config = config(&mut stored, &actor, false);
        let rows = written(after_reads(
            &mut operation(&actor, &stored),
            &stored,
            Some(config.clone()),
            None,
        ));
        let decoded: RealmConfigDocument = postcard::from_bytes(&config).unwrap();
        let registry_ref = registry_placement(&decoded, &stored);
        assert_ne!(registry_ref, stored.placement);
        let placements: Vec<_> = outboxes(&rows)
            .into_iter()
            .map(|outbox| match outbox.event {
                DocumentOutboxEvent::Upsert { change, .. }
                | DocumentOutboxEvent::Delete { change } => change.placement,
                other => panic!("unexpected outbox event {other:?}"),
            })
            .collect();
        assert_eq!(
            placements,
            vec![stored.placement, stored.placement, registry_ref]
        );
    }

    #[test]
    fn delete_takes_fence() {
        // An admitted delete stamps the generation it resolved at onto every tombstone row,
        // so the drain can bound the predecessor set.
        let actor = actor();
        let mut stored = record(&actor);
        let config = config(&mut stored, &actor, true);
        let mut operation = operation(&actor, &stored);
        let Ok(Next::Call(IoCall::Read(fences))) =
            after_reads(&mut operation, &stored, Some(config), None)
        else {
            panic!("the transaction reads the bucket fences");
        };
        let rows = written(operation.next(IoResult::Values(vec![None; fences.len()])));
        for outbox in outboxes(&rows) {
            assert_eq!(outbox.generation, 1);
        }
    }

    #[test]
    fn fence_rejects_delete() {
        // The departing holder closed generation one: no tombstone may commit.
        let actor = actor();
        let mut stored = record(&actor);
        let config = config(&mut stored, &actor, true);
        let mut operation = operation(&actor, &stored);
        let Ok(Next::Call(IoCall::Read(fences))) =
            after_reads(&mut operation, &stored, Some(config), None)
        else {
            panic!("the transaction reads the bucket fences");
        };
        let closed = Some(Value::from(1u64.to_be_bytes().to_vec()));
        assert_eq!(
            operation.next(IoResult::Values(vec![closed; fences.len()])),
            Err(DeleteDocumentError::PlacementFenced)
        );
    }

    #[test]
    fn missing_row_fails() {
        let actor = actor();
        let stored = record(&actor);
        let mut operation = operation(&actor, &stored);
        operation.first().unwrap();
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Err(DeleteDocumentError::DocumentNotFound)
        );
    }

    // The PID tombstone is part of the delete: a crash after the commit must not leave a
    // deleted document's mapping active. Without a minted pid nothing is written for it.
    #[test]
    fn delete_tombstones_minted() {
        let actor = actor();
        let stored = record(&actor);
        let unminted = written(after_reads(
            &mut operation(&actor, &stored),
            &stored,
            None,
            None,
        ));
        assert!(rows_in(&unminted, ID_MAPPING_KEYSPACE).is_empty());

        let revision = aruna_core::structs::PersistentIdRevision {
            event_id: Ulid::from_parts(17, 17),
            actor: actor.node_id,
            occurred_at_ms: 1,
        };
        let mut minted = PersistentIdMapping::requested(
            stored.document_id,
            false,
            actor.user_id,
            JobId::from_bytes([7; 16]),
            stored.public,
            stored.permission_path.clone(),
            revision,
        );
        assert!(minted.activate(actor.user_id, revision));
        let mapping = Some(Value::from(minted.to_bytes().unwrap()));
        let rows = written(after_reads(
            &mut operation(&actor, &stored),
            &stored,
            None,
            mapping,
        ));
        let tombstoned = PersistentIdMapping::from_bytes(rows_in(&rows, ID_MAPPING_KEYSPACE)[0])
            .expect("mapping decodes");
        assert_eq!(tombstoned.target, stored.document_id);
        assert_eq!(tombstoned.status, PersistentIdStatus::Tombstoned);
    }

    #[test]
    fn pid_read_denied() {
        // A mapping that does not decode fails the delete before any write.
        let actor = actor();
        let stored = record(&actor);
        let garbage = Some(Value::from(vec![0xff, 0x00, 0xff]));
        assert!(matches!(
            after_reads(&mut operation(&actor, &stored), &stored, None, garbage),
            Err(DeleteDocumentError::SyncDelete(_))
        ));
    }

    /// The delete samples its identity and phase time from the injected source, and a retry
    /// copy keeps it, so the same inputs emit byte-identical rows.
    #[test]
    fn delete_trace_pinned() {
        let actor = actor();
        let stored = record(&actor);
        let first = written(after_reads(
            &mut operation(&actor, &stored),
            &stored,
            None,
            None,
        ));
        let mut retried = operation(&actor, &stored).fresh_copy();
        let second = written(after_reads(&mut retried, &stored, None, None));
        assert_eq!(first, second);
    }

    #[test]
    fn errors_map() {
        assert_eq!(
            DeleteDocumentError::from(IoError::Conflict),
            DeleteDocumentError::StorageError(StorageError::TransactionConflict)
        );
        assert!(DeleteDocumentOperation::is_conflict(
            &DeleteDocumentError::from(IoError::Conflict)
        ));
        assert_eq!(
            DeleteDocumentError::from(IoError::Aborted),
            DeleteDocumentError::Io(IoError::Aborted)
        );
        let actor = actor();
        let stored = record(&actor);
        let mut operation = operation(&actor, &stored);
        operation.first().unwrap();
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(DeleteDocumentError::Io(IoError::Unexpected { .. }))
        ));
    }
}
