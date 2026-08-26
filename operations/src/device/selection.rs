//! What the owner keeps available offline on this device.
//!
//! Selection is the owner's own decision and lives only here: a selected
//! document gets a local replica the device reads and edits without the realm,
//! and a deselected one goes back to being forwarded like any other read.

use std::sync::Arc;

use aruna_core::structs::SyncRefusal;
use aruna_core::types::GroupId;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::DriverContext;

use super::refresh::{RefreshOutcome, refresh_replica, refusal_reason};
use super::replica::{
    MAX_DEVICE_REPLICAS, ReplicaOrigin, ReplicaRecord, ReplicaState, delete_replica, list_replicas,
    read_replica, store_replica,
};

#[derive(Debug, Error, PartialEq)]
pub enum SelectionError {
    #[error("the realm does not know this document")]
    NotFound,
    #[error("{0}")]
    Refused(String),
    #[error("this device already keeps the maximum of {limit} documents offline")]
    TooManyReplicas { limit: usize },
    #[error("the document has edits this device has not published yet")]
    PendingEdits,
    #[error("the device store is unavailable")]
    Unavailable,
}

/// Keeps one document available offline.
///
/// A first selection needs the realm once: the device has nothing to serve
/// until a holder has handed it the graph. Selecting again is idempotent and
/// only refreshes what is already there.
pub async fn select_document(
    context: &Arc<DriverContext>,
    document_id: Ulid,
) -> Result<ReplicaRecord, SelectionError> {
    if let Some(mut replica) = read_replica(context, document_id).await {
        if !replica.selected {
            replica.selected = true;
            store_replica(context, &replica).await;
        }
        refresh_replica(context, document_id).await;
        return read_replica(context, document_id)
            .await
            .ok_or(SelectionError::Unavailable);
    }
    let held = list_replicas(context)
        .await
        .ok_or(SelectionError::Unavailable)?;
    if held.len() >= MAX_DEVICE_REPLICAS {
        return Err(SelectionError::TooManyReplicas {
            limit: MAX_DEVICE_REPLICAS,
        });
    }
    // The row goes in first because the refresh fills it from the holder's
    // answer; a refresh that brings nothing leaves no half-selected document.
    let provisional = ReplicaRecord::new(
        document_id,
        GroupId::nil(),
        String::new(),
        ReplicaOrigin::Realm,
    );
    if !store_replica(context, &provisional).await {
        return Err(SelectionError::Unavailable);
    }
    match refresh_replica(context, document_id).await {
        RefreshOutcome::Installed => read_replica(context, document_id)
            .await
            .ok_or(SelectionError::Unavailable),
        outcome => {
            delete_replica(context, document_id).await;
            Err(match outcome {
                RefreshOutcome::Refused(SyncRefusal::NotFound) => SelectionError::NotFound,
                RefreshOutcome::Refused(refusal) => {
                    SelectionError::Refused(refusal_reason(&refusal))
                }
                _ => SelectionError::Unavailable,
            })
        }
    }
}

/// Stops keeping one document offline. The local graph is left in place: it is
/// an OR-Set replica, so re-selecting joins onto it instead of resurrecting
/// anything, and deleting it would tombstone the graph on this device.
pub async fn deselect_document(
    context: &Arc<DriverContext>,
    document_id: Ulid,
) -> Result<ReplicaRecord, SelectionError> {
    let replica = read_replica(context, document_id)
        .await
        .ok_or(SelectionError::NotFound)?;
    if replica.pending_edits > 0 {
        return Err(SelectionError::PendingEdits);
    }
    if !delete_replica(context, document_id).await {
        return Err(SelectionError::Unavailable);
    }
    Ok(ReplicaRecord {
        selected: false,
        ..replica
    })
}

/// Selects a document this device created, once the realm has accepted it.
/// The drain calls this, so a draft the owner authored offline stays readable
/// and editable on the device it was written on.
pub async fn track_created(
    context: &Arc<DriverContext>,
    document_id: Ulid,
    group_id: GroupId,
    document_path: String,
) {
    if read_replica(context, document_id).await.is_some() {
        return;
    }
    let mut replica =
        ReplicaRecord::new(document_id, group_id, document_path, ReplicaOrigin::Device);
    replica.state = ReplicaState::LocalOnly;
    store_replica(context, &replica).await;
}
