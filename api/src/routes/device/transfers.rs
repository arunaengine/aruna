//! Serves the uploads this device still owes and the downloads it has yet to fetch.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_operations::driver::drive;
use std::collections::HashMap;
use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::OwnerAuth;
use crate::error::{ErrorResponse, ServerResult};
use crate::server::state::ServerState;
use aruna_core::structs::{EntryState, SyncedFolder};
use aruna_operations::device::sync::folders::{
    list_entries, list_folders, list_transfers as read_uploads,
};
use ulid::Ulid;

use super::dto::{DeviceTransfer, DeviceTransferList, download_view, transfer_view};
use super::folders::map_folder_error;

pub(super) fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new().routes(routes!(list_device_transfers))
}

#[utoipa::path(
    get,
    path = "/device/transfers",
    tag = "device",
    summary = "List the transfers this device still owes",
    description = r#"Lists what this device has not finished exchanging with its realm nodes.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled
for.

**Behavior**
- `uploads` are local versions no realm node has pulled yet. A row disappears once the pull is
  acknowledged, so every listed upload is still owed.
- `downloads` are remote versions a folder has decided to fetch but has not written to disk yet.
- A `running` upload is being pulled by its realm node; `bytes_done` counts the bytes that node has
  received so far. Queued, retrying and failed uploads and all downloads report 0.
- The list is node-local and answers while the realm is unreachable."#,
    responses(
        (status = 200, description = "The uploads not yet pulled and the downloads not yet written", body = DeviceTransferList,
            example = json!({
                "uploads": [{
                    "id": "01JFOLDER0123456789ABCDEFG:notes/paper.txt",
                    "direction": "upload",
                    "folder_id": "01JFOLDER0123456789ABCDEFG",
                    "path": "notes/paper.txt",
                    "bucket": "lab-data",
                    "key": "ada/notes/paper.txt",
                    "state": "queued",
                    "bytes_total": 4096,
                    "bytes_done": 0,
                    "attempts": 0
                }],
                "downloads": [{
                    "id": "01JFOLDER0123456789ABCDEFG:notes/reads.fastq",
                    "direction": "download",
                    "folder_id": "01JFOLDER0123456789ABCDEFG",
                    "path": "notes/reads.fastq",
                    "bucket": "lab-data",
                    "key": "ada/notes/reads.fastq",
                    "state": "running",
                    "bytes_total": 1048576,
                    "bytes_done": 0,
                    "attempts": 1,
                    "next_attempt_ms": 1775748191000_i64
                }],
            })),
        (status = 404, description = "This node is not a user node and serves no device plane", body = ErrorResponse),
        crate::openapi::DeviceErrors
    ),
    security(("bearer_auth" = []))
)]
async fn list_device_transfers(
    State(state): State<Arc<ServerState>>,
    _: OwnerAuth,
) -> ServerResult<Json<DeviceTransferList>> {
    let context = state.get_ctx();
    let folders: HashMap<Ulid, SyncedFolder> = drive(list_folders(), &context)
        .await
        .map_err(map_folder_error)?
        .into_iter()
        .map(|folder| (folder.folder_id, folder))
        .collect();

    let uploads = drive(read_uploads(), &context)
        .await
        .map_err(map_folder_error)?
        .into_iter()
        .filter_map(|upload| {
            let folder = folders.get(&upload.folder_id)?;
            let key = folder.remote.remote_key(&upload.relative);
            Some(transfer_view(upload, folder.remote.bucket.clone(), key))
        })
        .collect();

    let mut downloads: Vec<DeviceTransfer> = Vec::new();
    for folder in folders.values() {
        let mut cursor = None;
        loop {
            let (entries, next) = drive(list_entries(folder.folder_id, None, cursor), &context)
                .await
                .map_err(map_folder_error)?;
            for (path, base) in entries {
                if !matches!(
                    base.entry,
                    EntryState::RemoteNew | EntryState::RemoteChanged | EntryState::Error { .. }
                ) {
                    continue;
                }
                let key = folder.remote.remote_key(&path);
                downloads.push(download_view(
                    folder.folder_id.to_string(),
                    path,
                    folder.remote.bucket.clone(),
                    key,
                    &base,
                ));
            }
            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
    }
    Ok(Json(DeviceTransferList { uploads, downloads }))
}
