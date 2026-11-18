//! Fetches planned remote versions from the folder's realm node and writes each one under its
//! guard; a failed fetch is recorded on the entry's base row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::keyspaces::SYNC_BASE_KEYSPACE;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::structs::{EntryState, SyncBase, SyncedFolder};
use aruna_core::time::unix_timestamp_millis;
use bytes::Bytes;
use tracing::{debug, warn};
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::replication::bao_read::{BaoReadError, BaoReadOutput};
use crate::replication::managed_read::ManagedReadOperation;
use crate::replication::protocol::{BaoReadRefusal, BaoReadRequest, BaoReadTarget};

use super::materialize::{MaterializeEntryOperation, MaterializeInput, MaterializeOutcome};
use super::reconcile::Download;
use super::repository::{base_entry, base_key};
use super::rows::{read_value, write_rows};

/// Reads one remote version from the folder's realm node.
pub(crate) async fn fetch_remote(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
    relative: &str,
    version: Ulid,
) -> Result<BackendStream<Result<Bytes, StreamError>>, String> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or_else(|| "the network handle is unavailable".to_string())?;
    let realm_id = *net_handle.realm_id();
    let source = VersionedObjectArn::new(
        realm_id,
        folder.remote.node_id,
        folder.remote.bucket.clone(),
        folder.remote.remote_key(relative),
        version,
    )
    .map_err(|error| error.to_string())?;
    let request = BaoReadRequest {
        auth_context: AuthContext {
            user_id: folder.created_by,
            realm_id,
            path_restrictions: None,
            session: None,
        },
        realm_id,
        target: BaoReadTarget::ExactVersion(source),
        expected_blake3: None,
        metadata_only: false,
        destination: None,
        known_refs: Vec::new(),
        range: None,
    };
    let mut answer = drive(
        ManagedReadOperation::new(folder.remote.node_id, request.clone()),
        context,
    )
    .await;
    // A member keeping only the record of a distributed version names a holder of its bytes.
    if let Err(BaoReadError::Refused(BaoReadRefusal::HeldBy { holder, set_id })) = answer {
        let held = BaoReadRequest {
            target: BaoReadTarget::Version {
                set_id,
                version_id: version,
            },
            ..request
        };
        answer = drive(ManagedReadOperation::new(holder, held), context).await;
    }
    match answer {
        Ok(BaoReadOutput::Stream { blob, .. }) => Ok(blob),
        Ok(BaoReadOutput::Metadata { .. }) => {
            Err("the remote version returned no bytes".to_string())
        }
        Err(error) => {
            debug!(relative = %relative, error = %error, "Could not read the remote version");
            Err(error.to_string())
        }
    }
}

/// Fetches every planned remote version and writes it under its guard. Answers
/// how many entries reached the disk.
pub async fn apply_downloads(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
    downloads: Vec<Download>,
) -> usize {
    let mut applied = 0usize;
    for download in downloads {
        let blob = match fetch_remote(context, folder, &download.relative, download.remote_version)
            .await
        {
            Ok(blob) => blob,
            Err(message) => {
                record_fetch_error(context, folder, &download, message.clone()).await;
                warn!(relative = %download.relative, message = %message, "Could not fetch the folder entry");
                continue;
            }
        };
        let outcome = drive(
            MaterializeEntryOperation::new(MaterializeInput {
                folder: folder.clone(),
                relative: download.relative.clone(),
                remote_version: download.remote_version,
                guard: download.guard,
                conflicted: download.conflicted,
                local_fingerprint: download
                    .local
                    .as_ref()
                    .and_then(|side| side.fingerprint.clone()),
                local: download.local.clone(),
                remote: None,
                synced: download.synced.clone(),
                audit: None,
                blob,
            }),
            context,
        )
        .await;
        match outcome {
            Ok(MaterializeOutcome::Written | MaterializeOutcome::Copied { .. }) => applied += 1,
            Ok(MaterializeOutcome::Refused { reason }) => {
                debug!(relative = %download.relative, reason = ?reason, "Kept the local file");
            }
            Ok(MaterializeOutcome::Failed { message }) => {
                warn!(relative = %download.relative, message = %message, "Could not write the folder entry");
            }
            Err(error) => {
                warn!(relative = %download.relative, error = %error, "Could not write the folder entry");
            }
        }
    }
    applied
}

async fn record_fetch_error(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
    download: &Download,
    message: String,
) {
    let Some(bytes) = read_value(
        context,
        SYNC_BASE_KEYSPACE,
        base_key(folder.folder_id, &download.relative),
        None,
    )
    .await
    else {
        warn!(relative = %download.relative, "Could not read the failed download row");
        return;
    };
    let Ok(base) = SyncBase::from_bytes(&bytes) else {
        warn!(relative = %download.relative, "Could not decode the failed download row");
        return;
    };
    let base = SyncBase {
        entry: EntryState::Error { reason: message },
        synced_at_ms: unix_timestamp_millis(),
        ..base
    };
    let Ok(row) = base_entry(folder.folder_id, &download.relative, &base) else {
        warn!(relative = %download.relative, "Could not encode the failed download row");
        return;
    };
    if !write_rows(context, vec![row], None).await {
        warn!(relative = %download.relative, "Could not store the failed download row");
    }
}
