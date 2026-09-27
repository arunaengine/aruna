//! Applies one owner edit to a local replica and queues it for the realm.
//! The holder re-checks authority when the queued edit is forwarded.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::Event;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::METADATA_ACTOR_KEYSPACE;
use aruna_core::metadata::{
    MetadataActor, MetadataBatch, MetadataBatchSource, MetadataEffect, MetadataError, MetadataEvent,
};
use aruna_core::storage_entries::{metadata_actor_entry, metadata_actor_key};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::task::{TaskEvent, TaskKey};
use craqle::ActorId;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::device::enqueue_draft::{EnqueueDraftError, EnqueueDraftInput, EnqueueDraftOperation};
use crate::device::publish_queue::{PublishEntry, PublishKind, PublishState};
use crate::device::replica::{ReplicaRecord, mark_edited, store_replica};
use crate::device::sync_status::read_publish_entries;
use crate::driver::{DriverContext, drive};
use crate::metadata::update_document::UpdateDocumentMutation;

#[derive(Debug, Error, PartialEq)]
pub enum DeviceEditError {
    #[error("this device holds no usable replica of the document")]
    NoReplica,
    #[error("the device store is unavailable")]
    Unavailable,
    #[error("{0}")]
    Invalid(String),
    #[error("the authoring queue already holds the maximum of {limit} entries")]
    QueueFull { limit: usize },
}

/// Applies one edit to the local replica and queues it for the realm.
///
/// The holder merges the same craqle batch so both sides converge; the drain confirms the record.
pub async fn apply_local_edit(
    context: &Arc<DriverContext>,
    owner: UserId,
    node_id: NodeId,
    replica: &ReplicaRecord,
    mutation: UpdateDocumentMutation,
) -> Result<MetadataRegistryRecord, DeviceEditError> {
    let record = replica
        .record
        .as_deref()
        .cloned()
        .ok_or(DeviceEditError::NoReplica)?;
    let authored = authored_source(mutation).ok_or_else(|| {
        DeviceEditError::Invalid("this mutation is not authored on a device".to_string())
    })?;
    let _guard = crate::metadata::update_document::document_lock(record.document_id).await;
    let current = read_actor(context, record.document_id).await?;
    let mut draft_id = Ulid::generate();
    if let Some(current) = &current
        && draft_id <= current.last_event_id
    {
        draft_id = current
            .last_event_id
            .increment()
            .map_err(|_| DeviceEditError::Unavailable)?;
    }
    let actor = MetadataActor::next(current.as_ref(), record.document_id, node_id, draft_id)
        .ok_or(DeviceEditError::Unavailable)?;
    let mut batch = plan_local(context, &record, &authored, (actor.actor, actor.counter)).await?;
    // Holders never get refused dots, so a new edit must not wait for them.
    actor.strip(&mut batch.base_clock);
    let entry = PublishEntry::edit(draft_id, owner, &record, batch.clone(), authored);
    drive(
        EnqueueDraftOperation::new(EnqueueDraftInput { entry }).with_actor(actor),
        context.as_ref(),
    )
    .await
    .map_err(|error| match error {
        EnqueueDraftError::QueueFull { limit } => DeviceEditError::QueueFull { limit },
        other => {
            warn!(error = %other, "Could not queue an offline edit");
            DeviceEditError::Unavailable
        }
    })?;
    merge_local(context, &batch).await?;
    request_persist(context).await;

    let mut edited = replica.clone();
    edited.record = Some(Box::new(record.clone()));
    let mut local_clock = batch.base_clock.clone();
    local_clock.advance(ActorId::from_bytes(batch.actor), batch.counter);
    mark_edited(&mut edited, local_clock);
    edited.displayed_jsonld = render(context, &record.graph_iri).await?;
    edited.dataset_digest = craqle::canonicalize_jsonld(&edited.displayed_jsonld)
        .ok()
        .map(|canonical| canonical.digest);
    store_replica(context, &edited).await;
    arm_drain(context).await;
    Ok(record)
}

/// The submission behind an edit, or `None` for a mutation only a holder
/// originates.
fn authored_source(mutation: UpdateDocumentMutation) -> Option<MetadataBatchSource> {
    match mutation {
        UpdateDocumentMutation::ReplaceRoCrate { jsonld } => {
            Some(MetadataBatchSource::ReplaceRoCrate { jsonld })
        }
        UpdateDocumentMutation::UpsertDataEntity { jsonld } => {
            Some(MetadataBatchSource::UpsertDataEntity { jsonld })
        }
        UpdateDocumentMutation::UpsertContextualEntity { jsonld } => {
            Some(MetadataBatchSource::UpsertContextualEntity { jsonld })
        }
        _ => None,
    }
}

/// Marks an edit the realm permanently refused, so later edits start a fresh actor and
/// never depend on it. Returns whether the actor record was updated.
pub(super) async fn reject_edit(
    context: &Arc<DriverContext>,
    document_id: Ulid,
    dot: ([u8; 32], u64),
) -> bool {
    let _guard = crate::metadata::update_document::document_lock(document_id).await;
    let Ok(Some(mut current)) = read_actor(context, document_id).await else {
        return false;
    };
    current.reject(dot.0, dot.1);
    let Ok((key_space, key, value)) = metadata_actor_entry(&current) else {
        return false;
    };
    matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::WriteResult { .. })
    )
}

/// Whether an edit depends on one the realm permanently refused.
pub(super) async fn refused_edit(
    context: &Arc<DriverContext>,
    document_id: Ulid,
    dot: ([u8; 32], u64),
) -> bool {
    read_actor(context, document_id)
        .await
        .ok()
        .flatten()
        .is_some_and(|current| current.refused(dot.0, dot.1))
}

/// This device's CRDT actor for the document, if it edited the document before.
async fn read_actor(
    context: &Arc<DriverContext>,
    document_id: Ulid,
) -> Result<Option<MetadataActor>, DeviceEditError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_ACTOR_KEYSPACE.to_string(),
            key: metadata_actor_key(document_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| postcard::from_bytes(&bytes).map_err(|_| DeviceEditError::Unavailable))
            .transpose(),
        _ => Err(DeviceEditError::Unavailable),
    }
}

/// Plans the submission under this device's actor without changing the graph.
async fn plan_local(
    context: &Arc<DriverContext>,
    record: &MetadataRegistryRecord,
    authored: &MetadataBatchSource,
    dot: ([u8; 32], u64),
) -> Result<MetadataBatch, DeviceEditError> {
    let (actor, counter) = dot;
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(DeviceEditError::Unavailable)?;
    match metadata
        .send_metadata_effect(MetadataEffect::PlanBatch {
            graph_iri: record.graph_iri.clone(),
            actor,
            counter,
            source: authored.clone(),
        })
        .await
    {
        Event::Metadata(MetadataEvent::BatchPlanned { batch, .. }) => Ok(batch),
        Event::Metadata(MetadataEvent::Error {
            error: MetadataError::InvalidInput(message),
            ..
        }) => Err(DeviceEditError::Invalid(message)),
        Event::Metadata(MetadataEvent::Error {
            error: MetadataError::Validation(violations),
            ..
        }) => Err(DeviceEditError::Invalid(format!("{violations:?}"))),
        other => {
            warn!(document_id = %record.document_id, event = ?other, "An offline edit could not be planned");
            Err(DeviceEditError::Unavailable)
        }
    }
}

/// Applies a durably queued batch to the device's local graph.
async fn merge_local(
    context: &Arc<DriverContext>,
    batch: &MetadataBatch,
) -> Result<(), DeviceEditError> {
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(DeviceEditError::Unavailable)?;
    match metadata
        .send_metadata_effect(MetadataEffect::MergeBatch {
            graph_iri: batch.graph_iri.clone(),
            batch: batch.clone(),
        })
        .await
    {
        Event::Metadata(MetadataEvent::BatchMerged { .. }) => Ok(()),
        other => {
            warn!(event = ?other, "A queued offline edit did not apply");
            Err(DeviceEditError::Unavailable)
        }
    }
}

/// The render of the merged graph this device displays from now on.
async fn render(context: &Arc<DriverContext>, graph_iri: &str) -> Result<String, DeviceEditError> {
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(DeviceEditError::Unavailable)?;
    metadata
        .export_rocrate_jsonld(graph_iri.to_string())
        .await
        .map_err(|error| {
            warn!(error = %error, "Could not render an edited replica");
            DeviceEditError::Unavailable
        })
}

/// Wakes the publish drain so a reachable realm sees the edit at once.
async fn arm_drain(context: &Arc<DriverContext>) {
    let Some(task_handle) = context.task_handle.as_ref() else {
        return;
    };
    if let TaskEvent::Error { message, .. } = task_handle
        .schedule_idle_timer(TaskKey::DrainDeviceIntake, Duration::ZERO)
        .await
    {
        warn!(message = %message, "Failed to arm the device publish drain");
    }
}

/// Asks the metadata backend to make what it just wrote durable. Best effort:
/// a failure leaves exactly the queued work the replay covers.
async fn request_persist(context: &Arc<DriverContext>) {
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return;
    };
    if let Err(error) = metadata.flush_persistence().await {
        warn!(error = %error, "Could not persist an offline edit's graph");
    }
}

/// Whether one queued entry's batch must be re-merged after a restart: only an
/// edit still on its way to the realm, which nothing else restores. A published
/// entry returns by refresh; a parked one is not this device's state.
pub fn replays_edit(entry: &PublishEntry) -> bool {
    matches!(entry.kind, PublishKind::Edit { .. })
        && matches!(
            entry.state,
            PublishState::Pending { .. } | PublishState::Publishing { .. }
        )
}

/// Re-merges every queued edit into this device's replicas, returning the count.
///
/// A crash between the WAL write and persist can leave the graph behind; merging is idempotent by dot.
pub async fn replay_queued_edits(context: &Arc<DriverContext>) -> usize {
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return 0;
    };
    let mut replayed = 0usize;
    for entry in read_publish_entries(context).await {
        let PublishKind::Edit { batch, .. } = &entry.kind else {
            continue;
        };
        if !replays_edit(&entry) {
            continue;
        }
        match metadata
            .send_metadata_effect(MetadataEffect::MergeBatch {
                graph_iri: batch.graph_iri.clone(),
                batch: (**batch).clone(),
            })
            .await
        {
            Event::Metadata(MetadataEvent::BatchMerged { applied, .. }) => {
                replayed += usize::from(applied);
            }
            other => {
                warn!(draft_id = %entry.draft_id, event = ?other, "Could not replay a queued edit");
            }
        }
    }
    if replayed > 0 {
        request_persist(context).await;
    }
    replayed
}

/// Whether this replica may take the edit locally: the owner selected it and
/// a refresh has left it something to edit.
pub fn accepts_edits(replica: &ReplicaRecord) -> bool {
    replica.selected && replica.record.is_some()
}

#[cfg(test)]
mod tests {
    use super::{DeviceEditError, apply_local_edit, read_actor, replays_edit};
    use crate::device::publish_queue::{
        MAX_PUBLISH_ENTRIES, PublishEntry, PublishKind, PublishState, publish_entry,
    };
    use crate::device::replica::{ReplicaOrigin, ReplicaRecord};
    use crate::driver::DriverContext;
    use crate::metadata::{MetadataHandle, MetadataHandleOptions, MetadataSearchStorage};
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::metadata::{
        MetadataBatch, MetadataBatchSource, MetadataCrateRequest, MetadataEffect, MetadataEvent,
        MetadataGraphPolicy, MetadataRequestDurability,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
    use craqle::VectorClock;
    use std::sync::Arc;
    use ulid::Ulid;

    fn node(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn record() -> MetadataRegistryRecord {
        let document_id = Ulid::from_bytes([2u8; 16]);
        MetadataRegistryRecord {
            realm_id: RealmId::from_bytes([3u8; 32]),
            group_id: Ulid::from_bytes([1u8; 16]),
            document_id,
            document_path: "notes".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: false,
            permission_path: "/notes".to_string(),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 2,
            establishing_event_id: Ulid::from_bytes([4u8; 16]),
            last_event_id: Ulid::from_bytes([5u8; 16]),
        }
    }

    fn edit(state: PublishState) -> PublishEntry {
        let mut entry = PublishEntry::edit(
            Ulid::generate(),
            UserId::local(Ulid::generate(), RealmId::from_bytes([3u8; 32])),
            &record(),
            MetadataBatch {
                graph_iri: record().graph_iri,
                actor: [7u8; 32],
                counter: 1,
                base_clock: VectorClock::default(),
                ops: Vec::new(),
                timestamp_millis: 9,
            },
            MetadataBatchSource::UpsertDataEntity {
                jsonld: "{}".to_string(),
            },
        );
        entry.state = state;
        entry
    }

    /// A device node whose replica already holds the document's graph.
    async fn device() -> (
        tempfile::TempDir,
        Arc<DriverContext>,
        MetadataRegistryRecord,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(
            dir.path().join("storage").to_str().expect("storage path"),
        )
        .unwrap();
        let metadata = MetadataHandle::new_with_options(
            dir.path().join("metadata"),
            node(1),
            storage.clone(),
            None,
            None,
            None,
            MetadataHandleOptions::default().with_search_storage(MetadataSearchStorage::Memory),
        )
        .unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: Some(metadata),
            task_handle: None,
            compute_handle: None,
        });
        let record = record();
        assert!(matches!(
            context
                .metadata_handle
                .as_ref()
                .unwrap()
                .send_metadata_effect(MetadataEffect::CreateCrate {
                    request: MetadataCrateRequest {
                        graph_iri: record.graph_iri.clone(),
                        name: "Notes".to_string(),
                        description: "Offline notes".to_string(),
                        date_published: "2026-08-29".to_string(),
                        license: None,
                        policy: MetadataGraphPolicy {
                            public: false,
                            permission_paths: vec![record.permission_path.clone()],
                        },
                        durability: MetadataRequestDurability::Durable,
                        deterministic_actor: Some([1u8; 32]),
                    },
                })
                .await,
            Event::Metadata(MetadataEvent::CreateCrateResult { .. })
        ));
        (dir, context, record)
    }

    fn contact(name: &str) -> crate::metadata::update_document::UpdateDocumentMutation {
        crate::metadata::update_document::UpdateDocumentMutation::UpsertContextualEntity {
            jsonld: format!(r##"{{"@id":"#{name}","@type":"Person","name":"{name}"}}"##),
        }
    }

    fn replica(record: &MetadataRegistryRecord) -> ReplicaRecord {
        let mut replica = ReplicaRecord::new(
            record.document_id,
            record.group_id,
            record.document_path.clone(),
            ReplicaOrigin::Realm,
        );
        replica.record = Some(Box::new(record.clone()));
        replica
    }

    #[tokio::test]
    async fn edits_share_actor() {
        // Reusing the device's actor keeps clocks small; its dots must stay ordered.
        let (_dir, context, record) = device().await;
        let owner = UserId::local(Ulid::generate(), record.realm_id);
        for name in ["ada", "grace"] {
            apply_local_edit(&context, owner, node(1), &replica(&record), contact(name))
                .await
                .expect("edit applies");
        }
        let batches: Vec<MetadataBatch> =
            crate::device::sync_status::read_publish_entries(&context)
                .await
                .into_iter()
                .filter_map(|entry| match entry.kind {
                    PublishKind::Edit { batch, .. } => Some(*batch),
                    PublishKind::Create => None,
                })
                .collect();
        let [first, second] = batches.as_slice() else {
            panic!("expected two queued edits, got {batches:?}");
        };
        assert_eq!((first.actor, first.counter), (second.actor, 1));
        assert_eq!(second.counter, 2);
        assert!(second.base_clock.contains(&craqle::Dot {
            actor: craqle::ActorId::from_bytes(first.actor),
            counter: 1,
        }));
        let stored = read_actor(&context, record.document_id)
            .await
            .expect("actor readable")
            .expect("actor stored");
        assert_eq!((stored.actor, stored.counter), (first.actor, 2));
    }

    #[tokio::test]
    async fn queue_preserves_graph() {
        // Planning must not expose an edit that cannot enter the durable queue.
        let (_dir, context, record) = device().await;
        let owner = UserId::local(Ulid::generate(), record.realm_id);
        for _ in 0..MAX_PUBLISH_ENTRIES {
            let entry = PublishEntry::new(
                Ulid::generate(),
                owner,
                record.group_id,
                "queued".to_string(),
                false,
                "{}".to_string(),
            );
            let (key_space, key, value) = publish_entry(&entry).unwrap();
            assert!(matches!(
                context
                    .storage_handle
                    .send_storage_effect(StorageEffect::Write {
                        key_space,
                        key,
                        value,
                        txn_id: None,
                    })
                    .await,
                Event::Storage(StorageEvent::WriteResult { .. })
            ));
        }
        let mut replica = ReplicaRecord::new(
            record.document_id,
            record.group_id,
            record.document_path.clone(),
            ReplicaOrigin::Realm,
        );
        replica.record = Some(Box::new(record.clone()));
        let metadata = context.metadata_handle.as_ref().unwrap();
        let before = metadata
            .export_rocrate_jsonld(record.graph_iri.clone())
            .await
            .unwrap();

        assert_eq!(
            apply_local_edit(
                &context,
                owner,
                node(1),
                &replica,
                crate::metadata::update_document::UpdateDocumentMutation::UpsertContextualEntity {
                    jsonld: r##"{"@id":"#ada","@type":"Person","name":"Ada"}"##.to_string(),
                },
            )
            .await,
            Err(DeviceEditError::QueueFull {
                limit: MAX_PUBLISH_ENTRIES
            })
        );
        // The refused edit must not consume a dot either.
        assert_eq!(read_actor(&context, record.document_id).await, Ok(None));
        assert_eq!(
            metadata
                .export_rocrate_jsonld(record.graph_iri.clone())
                .await
                .unwrap(),
            before
        );
    }

    #[test]
    fn replays_unpublished_edits() {
        // Only an edit still on its way to the realm is local state nothing
        // else restores; a create carries no batch to replay at all.
        assert!(replays_edit(&edit(PublishState::Pending {
            due_at_ms: 0,
            attempts: 0,
            last_error: None,
        })));
        assert!(replays_edit(&edit(PublishState::Publishing {
            document_id: record().document_id,
            due_at_ms: 0,
            attempts: 1,
        })));
        assert!(!replays_edit(&edit(PublishState::Published {
            document_id: record().document_id,
        })));
        assert!(!replays_edit(&edit(PublishState::Failed {
            reason: "denied".to_string(),
            retryable: false,
            document_id: Some(record().document_id),
        })));

        let create = PublishEntry::new(
            Ulid::generate(),
            UserId::local(Ulid::generate(), RealmId::from_bytes([3u8; 32])),
            Ulid::from_bytes([1u8; 16]),
            "notes".to_string(),
            false,
            "{}".to_string(),
        );
        assert!(matches!(create.kind, PublishKind::Create));
        assert!(!replays_edit(&create));
    }
}
