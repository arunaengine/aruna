//! Looks up buckets, callers and object views shared by the REST routes and the MCP tools.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::checksum::{HASH_BLAKE3, HASH_MD5};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::data::identity::content_id;
use aruna_core::structs::storage::distribution::RouteError;
use aruna_operations::distribution::guard::GuardError;
use aruna_operations::document_repository::GetConfigOperation;
use aruna_operations::driver::drive;
use aruna_operations::forward::routing::{is_user_origin, origin_holds_document};
use aruna_operations::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_operations::s3::object::list::ListedObject;
use rmcp::schemars;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::error::{ServerError, ServerResult};
use crate::server::state::ServerState;

/// Whether this node is a device, from the current realm config; a node the config does not list
/// is unavailable.
pub(crate) async fn user_origin(state: &ServerState) -> ServerResult<bool> {
    let config = drive(
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|_| ServerError::ServiceUnavailable)?;
    is_user_origin(&config, state.get_node_id()).ok_or(ServerError::ServiceUnavailable)
}

/// Whether this node holds a structured metadata document's bucket; `None` when the realm config
/// is unreadable or does not place the document here.
pub(crate) async fn origin_holds(state: &ServerState, document_id: Ulid) -> Option<bool> {
    let realm_id = state.get_realm_id();
    let config = drive(GetConfigOperation::new(realm_id), &state.get_ctx())
        .await
        .ok()?;
    origin_holds_document(&config, realm_id, state.get_node_id(), document_id)
}

/// The bucket a name resolves to on this node; an unknown name is 404.
pub(crate) async fn bucket_info(state: &ServerState, bucket: &str) -> ServerResult<BucketInfo> {
    match drive(
        GetBucketOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(info) => Ok(info),
        Err(GetBucketError::NotFound) => Err(ServerError::NotFound),
        Err(error) => Err(ServerError::InternalError(error.into())),
    }
}

/// The actor for an operation that `auth` starts on this node.
pub(crate) fn actor(state: &ServerState, auth: &AuthContext) -> Actor {
    Actor {
        node_id: state.get_node_id(),
        user_id: auth.user_id,
        realm_id: state.get_realm_id(),
    }
}

/// A write this member of a distributed bucket may not take now: 503 when a retry or the
/// coordinator can take it, 409 when the bucket state refuses it.
pub(crate) fn map_guard_error(error: GuardError) -> ServerError {
    match error {
        GuardError::NotCoordinator { .. }
        | GuardError::RecordMissing(_)
        | GuardError::Unreadable(_)
        | GuardError::Route(
            RouteError::Pending(_) | RouteError::Moving(_) | RouteError::StaleRange(_),
        ) => ServerError::ServiceUnavailableReason(error.to_string()),
        GuardError::Route(_) | GuardError::Unbound(_) => ServerError::Conflict(error.to_string()),
        GuardError::Conversion(error) => ServerError::InternalError(error.into()),
    }
}

#[derive(
    Debug, Clone, PartialEq, Deserialize, Serialize, schemars::JsonSchema, utoipa::ToSchema,
)]
pub struct ObjectOutput {
    pub key: String,
    /// The current version, so a selection can pin exactly what was listed.
    pub version_id: String,
    pub size: Option<u64>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_type: Option<String>,
    pub referenced: bool,
    /// Content address `https://w3id.org/aruna/data/<blake3>` to use as a File entity `@id`.
    /// Absent for a reference whose bytes were never read.
    pub content_id: Option<String>,
    /// A link's binding, kept to check its target before the entry is shown; never sent.
    #[serde(skip)]
    pub link: Option<aruna_core::structs::execution::staging::VersionSourceBinding>,
}

/// One listed object as the listing tools and the REST object listing show it.
pub(crate) fn object_output(object: &ListedObject) -> ObjectOutput {
    let size = entry_size(object);
    let etag = object
        .location
        .as_ref()
        .and_then(|location| location.hashes.get(HASH_MD5))
        .map(hex::encode)
        .or_else(|| {
            object
                .source_metadata
                .as_ref()
                .and_then(|metadata| metadata.etag.clone())
        });
    let content_type = object
        .source_metadata
        .as_ref()
        .and_then(|metadata| metadata.content_type.clone());
    let content_id = object
        .location
        .as_ref()
        .and_then(|location| location.hashes.get(HASH_BLAKE3))
        .and_then(|hash| <[u8; 32]>::try_from(hash.as_slice()).ok())
        .map(content_id);
    let last_modified =
        entry_time(object).map(|time| chrono::DateTime::<chrono::Utc>::from(time).to_rfc3339());
    ObjectOutput {
        key: object.head.key.clone(),
        version_id: object.version_id.to_string(),
        size,
        etag,
        last_modified,
        content_type,
        referenced: object.referenced,
        content_id,
        link: object.link.clone(),
    }
}

fn entry_size(object: &ListedObject) -> Option<u64> {
    object
        .location
        .as_ref()
        .map(|location| location.blob_size)
        .or_else(|| {
            object
                .source_metadata
                .as_ref()
                .map(|metadata| metadata.content_length)
        })
}

fn entry_time(object: &ListedObject) -> Option<std::time::SystemTime> {
    object
        .version_created_at
        .or(object.last_refresh)
        .or_else(|| {
            object
                .source_metadata
                .as_ref()
                .and_then(|metadata| metadata.last_modified)
        })
}
