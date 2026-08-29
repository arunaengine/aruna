//! Edits the owner makes on this device's own replicas.
//!
//! The edit is planned against the local graph, queued, and then merged, so a
//! queue failure leaves the graph unchanged and the realm receives exactly the
//! change set the device applies. Nothing here decides realm authority: the
//! holder re-checks the owner's permission when the drain forwards the batch.

use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::events::Event;
use aruna_core::metadata::{
    MetadataBatch, MetadataBatchSource, MetadataEffect, MetadataError, MetadataEvent,
};
use aruna_core::structs::MetadataRegistryRecord;
use aruna_core::task::{TaskEvent, TaskKey};
use aruna_core::types::UserId;
use craqle::ActorId;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::device::enqueue_draft::{EnqueueDraftError, EnqueueDraftInput, EnqueueDraftOperation};
use crate::device::replica::{ReplicaRecord, mark_edited, store_replica};
use crate::device::repository::{IntakeEntry, IntakeKind, IntakeState};
use crate::device::status::read_intake_entries;
use crate::driver::{DriverContext, drive};
use crate::update_metadata_document::UpdateMetadataDocumentMutation;

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

/// CRDT actor of one offline edit, unique per device and draft so two edits
/// never claim the same dot even when the same document is edited twice.
pub fn device_edit_actor(node_id: NodeId, draft_id: Ulid) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"aruna-device-edit-v1\0");
    hasher.update(node_id.as_bytes());
    hasher.update(&draft_id.to_bytes());
    *hasher.finalize().as_bytes()
}

/// Applies one edit to the local replica and queues it for the realm.
///
/// The batch craqle produced is the edit: the holder merges that same change
/// set, so both sides converge whatever else happened while the device was
/// away. Answers with the record this device holds, which the realm confirms
/// when the drain forwards the batch.
pub async fn apply_local_edit(
    context: &Arc<DriverContext>,
    owner: UserId,
    node_id: NodeId,
    replica: &ReplicaRecord,
    mutation: UpdateMetadataDocumentMutation,
) -> Result<MetadataRegistryRecord, DeviceEditError> {
    let record = replica
        .record
        .as_deref()
        .cloned()
        .ok_or(DeviceEditError::NoReplica)?;
    let authored = authored_source(mutation).ok_or_else(|| {
        DeviceEditError::Invalid("this mutation is not authored on a device".to_string())
    })?;
    let draft_id = Ulid::generate();
    let actor = device_edit_actor(node_id, draft_id);
    let batch = plan_local(context, &record, &authored, actor).await?;
    let entry = IntakeEntry::edit(draft_id, owner, &record, batch.clone(), authored);
    drive(
        EnqueueDraftOperation::new(EnqueueDraftInput { entry }),
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
fn authored_source(mutation: UpdateMetadataDocumentMutation) -> Option<MetadataBatchSource> {
    match mutation {
        UpdateMetadataDocumentMutation::ReplaceRoCrate { jsonld } => {
            Some(MetadataBatchSource::ReplaceRoCrate { jsonld })
        }
        UpdateMetadataDocumentMutation::UpsertDataEntity { jsonld } => {
            Some(MetadataBatchSource::UpsertDataEntity { jsonld })
        }
        UpdateMetadataDocumentMutation::UpsertContextualEntity { jsonld } => {
            Some(MetadataBatchSource::UpsertContextualEntity { jsonld })
        }
        _ => None,
    }
}

/// Plans the submission under this edit's actor without changing the graph.
async fn plan_local(
    context: &Arc<DriverContext>,
    record: &MetadataRegistryRecord,
    authored: &MetadataBatchSource,
    actor: [u8; 32],
) -> Result<MetadataBatch, DeviceEditError> {
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(DeviceEditError::Unavailable)?;
    match metadata
        .send_metadata_effect(MetadataEffect::PlanBatch {
            graph_iri: record.graph_iri.clone(),
            actor,
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

/// Wakes the intake drain so a reachable realm sees the edit at once.
async fn arm_drain(context: &Arc<DriverContext>) {
    let Some(task_handle) = context.task_handle.as_ref() else {
        return;
    };
    if let TaskEvent::Error { message, .. } = task_handle
        .schedule_timer_if_idle(TaskKey::DrainDeviceIntake, Duration::ZERO)
        .await
    {
        warn!(message = %message, "Failed to arm the device intake drain");
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

/// Whether one queued entry's batch has to go back into the local graph after a
/// restart. An edit that is still on its way to the realm is the only local
/// change nothing else would restore; a published one comes back with the next
/// refresh, and a parked one is not this device's state to keep.
pub fn replays_edit(entry: &IntakeEntry) -> bool {
    matches!(entry.kind, IntakeKind::Edit { .. })
        && matches!(
            entry.state,
            IntakeState::Pending { .. } | IntakeState::Publishing { .. }
        )
}

/// Re-merges every queued edit into this device's replicas, and answers how
/// many were replayed.
///
/// Local edits are written to craqle's WAL and persisted afterwards, so a crash
/// in between can leave the graph behind the queue. Merging is idempotent by
/// dot, so a batch the graph already carries costs nothing.
pub async fn replay_queued_edits(context: &Arc<DriverContext>) -> usize {
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return 0;
    };
    let mut replayed = 0usize;
    for entry in read_intake_entries(context).await {
        let IntakeKind::Edit { batch, .. } = &entry.kind else {
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
    use super::{DeviceEditError, apply_local_edit, device_edit_actor, replays_edit};
    use crate::device::replica::{ReplicaOrigin, ReplicaRecord};
    use crate::device::repository::{
        IntakeEntry, IntakeKind, IntakeState, MAX_INTAKE_ENTRIES, intake_entry,
    };
    use crate::driver::DriverContext;
    use crate::metadata::{MetadataHandle, MetadataHandleOptions, MetadataSearchStorage};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::metadata::{
        MetadataBatch, MetadataBatchSource, MetadataCreateCrateRequest, MetadataEffect,
        MetadataEvent, MetadataGraphPolicy, MetadataRequestDurability,
    };
    use aruna_core::structs::{MetadataRegistryRecord, PlacementRef, RealmId};
    use aruna_core::types::UserId;
    use craqle::VectorClock;
    use std::sync::Arc;
    use ulid::Ulid;

    fn node(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn separates_edit_actors() {
        // Two edits must never claim the same dot, and the same edit must
        // derive the same actor after a restart.
        let draft = Ulid::generate();
        assert_eq!(
            device_edit_actor(node(1), draft),
            device_edit_actor(node(1), draft)
        );
        assert_ne!(
            device_edit_actor(node(1), draft),
            device_edit_actor(node(2), draft)
        );
        assert_ne!(
            device_edit_actor(node(1), draft),
            device_edit_actor(node(1), Ulid::generate())
        );
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

    fn edit(state: IntakeState) -> IntakeEntry {
        let mut entry = IntakeEntry::edit(
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

    #[tokio::test]
    async fn queue_preserves_graph() {
        // Planning must not expose an edit that cannot enter the durable queue.
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
                    request: MetadataCreateCrateRequest {
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
        let owner = UserId::local(Ulid::generate(), record.realm_id);
        for _ in 0..MAX_INTAKE_ENTRIES {
            let entry = IntakeEntry::new(
                Ulid::generate(),
                owner,
                record.group_id,
                "queued".to_string(),
                false,
                "{}".to_string(),
            );
            let (key_space, key, value) = intake_entry(&entry).unwrap();
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
                crate::update_metadata_document::UpdateMetadataDocumentMutation::UpsertContextualEntity {
                    jsonld: r##"{"@id":"#ada","@type":"Person","name":"Ada"}"##.to_string(),
                },
            )
            .await,
            Err(DeviceEditError::QueueFull {
                limit: MAX_INTAKE_ENTRIES
            })
        );
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
        assert!(replays_edit(&edit(IntakeState::Pending {
            due_at_ms: 0,
            attempts: 0,
            last_error: None,
        })));
        assert!(replays_edit(&edit(IntakeState::Publishing {
            document_id: record().document_id,
            due_at_ms: 0,
            attempts: 1,
        })));
        assert!(!replays_edit(&edit(IntakeState::Published {
            document_id: record().document_id,
        })));
        assert!(!replays_edit(&edit(IntakeState::Failed {
            reason: "denied".to_string(),
            retryable: false,
        })));

        let create = IntakeEntry::new(
            Ulid::generate(),
            UserId::local(Ulid::generate(), RealmId::from_bytes([3u8; 32])),
            Ulid::from_bytes([1u8; 16]),
            "notes".to_string(),
            false,
            "{}".to_string(),
        );
        assert!(matches!(create.kind, IntakeKind::Create));
        assert!(!replays_edit(&create));
    }
}
