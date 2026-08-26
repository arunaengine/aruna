//! Edits the owner makes on this device's own replicas.
//!
//! The edit is applied to the local graph first and queued afterwards, so the
//! owner sees the result immediately and the realm receives exactly the change
//! set that produced it. Nothing here decides realm authority: the holder
//! re-checks the owner's permission when the drain forwards the batch.

use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::events::Event;
use aruna_core::metadata::{
    MetadataApplyRoCrateRequest, MetadataBatch, MetadataBatchSource, MetadataEffect, MetadataError,
    MetadataEvent, MetadataGraphPolicy, MetadataRequestDurability, MetadataUpsertEntityRequest,
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
use crate::device::repository::IntakeEntry;
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
    let batch = merge_locally(context, &record, &authored, actor).await?;
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

/// Applies the submission to the local graph under this edit's own actor, and
/// answers with the change set craqle committed.
async fn merge_locally(
    context: &Arc<DriverContext>,
    record: &MetadataRegistryRecord,
    authored: &MetadataBatchSource,
    actor: [u8; 32],
) -> Result<MetadataBatch, DeviceEditError> {
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(DeviceEditError::Unavailable)?;
    let graph_iri = record.graph_iri.clone();
    let deterministic_actor = Some(actor);
    let effect = match authored {
        MetadataBatchSource::ReplaceRoCrate { jsonld } => MetadataEffect::ApplyRoCrate {
            request: MetadataApplyRoCrateRequest {
                graph_iri,
                jsonld: jsonld.clone(),
                policy: MetadataGraphPolicy {
                    public: record.public,
                    permission_paths: vec![record.permission_path.clone()],
                }
                .normalized(),
                durability: MetadataRequestDurability::Durable,
                deterministic_actor,
            },
        },
        MetadataBatchSource::UpsertDataEntity { jsonld } => MetadataEffect::UpsertDataEntity {
            request: MetadataUpsertEntityRequest {
                graph_iri,
                jsonld: jsonld.clone(),
                durability: MetadataRequestDurability::Durable,
                deterministic_actor,
            },
        },
        MetadataBatchSource::UpsertContextualEntity { jsonld } => {
            MetadataEffect::UpsertContextualEntity {
                request: MetadataUpsertEntityRequest {
                    graph_iri,
                    jsonld: jsonld.clone(),
                    durability: MetadataRequestDurability::Durable,
                    deterministic_actor,
                },
            }
        }
    };
    match metadata.send_metadata_effect(effect).await {
        Event::Metadata(
            MetadataEvent::ApplyRoCrateResult { batch, .. }
            | MetadataEvent::EntityUpsertResult { batch, .. },
        ) => Ok(batch),
        Event::Metadata(MetadataEvent::Error {
            error: MetadataError::InvalidInput(message),
            ..
        }) => Err(DeviceEditError::Invalid(message)),
        Event::Metadata(MetadataEvent::Error {
            error: MetadataError::Validation(violations),
            ..
        }) => Err(DeviceEditError::Invalid(format!("{violations:?}"))),
        other => {
            warn!(document_id = %record.document_id, event = ?other, "An offline edit did not apply");
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

/// Whether this replica may take the edit locally: the owner selected it and
/// a refresh has left it something to edit.
pub fn accepts_edits(replica: &ReplicaRecord) -> bool {
    replica.selected && replica.record.is_some()
}

#[cfg(test)]
mod tests {
    use super::device_edit_actor;
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
}
