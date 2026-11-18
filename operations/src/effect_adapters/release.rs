//! Runs release effects: crate reads with the caller's identity and questions to range holders.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::metadata::AuthToken;
use aruna_core::structs::storage::release::{
    CRATE_CHUNK, CrateBytes, CrateRead, CrateReadError, HolderAnswer, HolderRequest, ReleaseEffect,
    ReleaseEvent,
};
use futures_util::StreamExt;

use crate::document_repository::GetConfigOperation;
use crate::driver::{DriverContext, drive};
use crate::realm::peer_trust::is_sync_eligible;
use crate::s3::object::get::{GetObjectError, GetObjectInput, ObjectRangeRequest};
use crate::s3::object::read::GetRoutedOperation;

pub(super) async fn dispatch_release(
    context: &DriverContext,
    effect: ReleaseEffect,
) -> ReleaseEvent {
    match effect {
        ReleaseEffect::ReadCrate(read) => {
            let found = read_part(context, *read, None).await;
            ReleaseEvent::CrateRead(found.map(|(bytes, _)| bytes))
        }
        ReleaseEffect::ReadChunk { read, offset } => {
            let chunk = ObjectRangeRequest::StartEnd {
                start: offset,
                end: offset.saturating_add(CRATE_CHUNK - 1),
            };
            ReleaseEvent::CrateChunk(read_part(context, *read, Some(chunk)).await)
        }
        ReleaseEffect::Holder(request) => ReleaseEvent::Holder(ask_holder(context, request).await),
    }
}

/// Reads `range` of a release crate version, or all of it, and the version's whole size, through
/// a holder when this member keeps the record but not the bytes.
async fn read_part(
    context: &DriverContext,
    read: CrateRead,
    range: Option<ObjectRangeRequest>,
) -> Result<(CrateBytes, u64), CrateReadError> {
    let input = GetObjectInput {
        bucket: read.bucket,
        key: read.key,
        version_id: Some(read.version_id),
        range,
        group_id: read.group_id,
        user_identity: read.auth.user_id,
        realm_id: read.realm_id,
        node_id: read.node_id,
    };
    let restrictions = read.auth.path_restrictions.clone();
    // Boxed, since a routed read runs operations that may read release crates again.
    let routed = GetRoutedOperation::new(input, restrictions, None);
    let result = match Box::pin(drive(routed, context)).await {
        Ok(result) => result,
        Err(
            GetObjectError::NoSuchKey
            | GetObjectError::NoSuchVersion
            | GetObjectError::DeleteMarker,
        ) => return Err(CrateReadError::Missing),
        Err(error) => return Err(CrateReadError::Failed(error.to_string())),
    };
    let size = result.info.size;
    let mut bytes = Vec::new();
    let mut blob = result.blob;
    while let Some(chunk) = blob.next().await {
        let chunk = chunk.map_err(|error| CrateReadError::Failed(error.to_string()))?;
        bytes.extend_from_slice(&chunk);
    }
    let found = CrateBytes {
        metadata: result.metadata,
        bytes,
    };
    Ok((found, size))
}

/// Asks a holder with the caller's bearer, which goes only to a realm infrastructure node.
async fn ask_holder(
    context: &DriverContext,
    request: HolderRequest,
) -> Result<HolderAnswer, String> {
    let handle = context
        .metadata_handle
        .as_ref()
        .ok_or("no metadata handle")?;
    let realm_id = (context.net_handle.as_ref())
        .map(|net| *net.realm_id())
        .ok_or("no realm")?;
    let config = drive(GetConfigOperation::new(realm_id), context)
        .await
        .map_err(|error| error.to_string())?;
    if !is_sync_eligible(&config, request.node_id) {
        return Err(format!("{} is no infrastructure node", request.node_id));
    }
    let token = AuthToken::bearer(request.bearer).map_err(|error| format!("{error:?}"))?;
    handle
        .request_release_holder(request.node_id, Some(token), request.bucket, request.ask)
        .await
        .map_err(|error| format!("{error:?}"))
}
