//! Selects and deselects the documents the owner keeps available offline on this device.
//! A selected document gets a local replica; a deselected one is read through the realm.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::SyncRefusal;
use aruna_core::types::GroupId;
use tracing::warn;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};

use super::refresh::{RefreshOutcome, refresh_replica, refusal_reason};
use super::replica::{
    DeselectDocumentOperation, ReplicaOrigin, ReplicaRecord, SelectionError, TrackCreatedOperation,
    cache_budget,
};
use super::replica_store::{cache_usage, delete_replica, read_replica, store_replica};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;

/// Keeps one document available offline.
/// A first selection needs the realm once for the graph; selecting again is
/// idempotent and only refreshes what is already there.
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

/// Stops keeping one document offline; see [`DeselectDocumentOperation`].
pub async fn deselect_document(
    context: &Arc<DriverContext>,
    document_id: Ulid,
) -> Result<ReplicaRecord, SelectionError> {
    drive(DeselectDocumentOperation::new(document_id), context).await
}

/// Keeps a document the owner just read online as an unselected replica, so it stays readable
/// offline. A stale copy is refreshed; a new one is kept only while the cache budget allows.
pub async fn keep_read(context: &Arc<DriverContext>, record: &MetadataRegistryRecord) {
    let document_id = record.document_id;
    match read_replica(context, document_id).await {
        // A pinned or edited copy follows the refresh beat instead.
        Some(replica) if replica.selected || replica.pending_edits > 0 => {}
        Some(replica)
            if replica
                .record
                .as_ref()
                .is_some_and(|held| held.last_event_id == record.last_event_id) => {}
        Some(_) => {
            refresh_replica(context, document_id).await;
        }
        None => {
            // A graph cannot be removed locally yet, so a full cache takes no new copy.
            if cache_usage(context)
                .await
                .is_none_or(|used| used >= cache_budget())
            {
                return;
            }
            let mut provisional = ReplicaRecord::new(
                document_id,
                record.group_id,
                record.document_path.clone(),
                ReplicaOrigin::Realm,
            );
            provisional.selected = false;
            if !store_replica(context, &provisional).await {
                return;
            }
            if refresh_replica(context, document_id).await != RefreshOutcome::Installed {
                delete_replica(context, document_id).await;
            }
        }
    }
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
    let tracking = TrackCreatedOperation::new(document_id, group_id, document_path);
    if let Err(error) = drive(tracking, context).await {
        warn!(%document_id, error = %error, "Failed to track a created document");
    }
}
