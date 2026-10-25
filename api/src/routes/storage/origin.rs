//! Serves the origin of a file version: how its content got into the bucket and what a
//! dataset may say about it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::SyncMode;
use aruna_core::structs::execution::source_access::{DigestOrigin, DigestScope};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{bucket_permission_path, object_permission_path};
use aruna_core::structs::storage::distribution::ReadRoute;
use aruna_core::structs::storage::origin::{
    OriginProjection, OriginVersion, SyncHop, VersionOrigin,
};
use aruna_operations::distribution::guard::route_read;
use aruna_operations::distribution::resolve::key_holders;
use aruna_operations::s3::bucket::sources::{FileSource, bucket_sources};
use aruna_operations::s3::object::origin::{OriginError, OriginView, read_origin};
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ValidatedBearer, ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::management_relay::RELAY_HOP_HEADER;
use crate::routes::storage::deletion::bucket_info;
use crate::routes::storage::distributed::map_guard_error;
use crate::routes::storage::relay::{RestForward, relay_get};
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi()]
pub struct OriginApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(OriginApiDoc::openapi())
        .routes(routes!(get_origin))
        .routes(routes!(get_sources))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct OriginParams {
    /// Object key in the bucket.
    pub key: String,
    /// The version; the current version when absent.
    pub version_id: Option<Ulid>,
}

/// How a version's content got into its bucket.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OriginBody {
    /// Uploaded directly; the uploader is `created_by`.
    Upload {
        node_id: String,
        device: Option<String>,
    },
    /// From an external source or a database; details are under `source`.
    Source { database_entry: Option<String> },
    /// Copied from another exact version (global id or versioned node ARN as w3id).
    Copy { from: String },
    /// Registered in place from a storage backend folder.
    Storage { backend_id: String, key: String },
    /// Written by a compute job.
    Job { run_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HopBody {
    pub relationship_id: String,
    /// The exact source version the sync delivered.
    pub from: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SourceDigestBody {
    pub algorithm: String,
    pub digest: String,
    /// `full_object` or `composite`.
    pub scope: String,
    /// `listing_etag`, `manifest` or `verified_read`.
    pub origin: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SourceBody {
    /// Public URL of the source object, when the source has one.
    pub url: Option<String>,
    pub path: String,
    pub etag: Option<String>,
    pub digests: Vec<SourceDigestBody>,
}

/// One statement a dataset may add when this file joins it.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "property")]
pub enum ProjectionBody {
    #[serde(rename = "isBasedOn")]
    BasedOn { id: String },
    /// The run crate convention: a `CreateAction` of the run with this file as `result`.
    #[serde(rename = "CreateAction")]
    CreatedBy { run_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OriginResponse {
    pub version_id: String,
    /// The global id URL, for a version of a distributed bucket.
    pub global_id: Option<String>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    /// Absent for versions minted without a recorded origin.
    pub origin: Option<OriginBody>,
    pub hop: Option<HopBody>,
    pub source: Option<SourceBody>,
    /// The publication-safe projection: uploads and imports in place give none.
    pub projection: Vec<ProjectionBody>,
}

fn version_id(version: &OriginVersion) -> String {
    version.to_id()
}

impl From<OriginView> for OriginResponse {
    fn from(view: OriginView) -> Self {
        Self {
            version_id: view.version_id.to_string(),
            global_id: view.global.map(|global| global.to_url()),
            created_by: view.created_by.to_string(),
            created_at: view.created_at.into(),
            origin: view.origin.map(|origin| match origin {
                VersionOrigin::Upload { node_id, device } => OriginBody::Upload {
                    node_id: node_id.to_string(),
                    device,
                },
                VersionOrigin::Source { database } => OriginBody::Source {
                    database_entry: database.map(|entry| entry.to_string()),
                },
                VersionOrigin::Copy { from } => OriginBody::Copy {
                    from: version_id(&from),
                },
                VersionOrigin::Storage { backend_id, key } => OriginBody::Storage {
                    backend_id: backend_id.to_string(),
                    key,
                },
                VersionOrigin::Job { run_id } => OriginBody::Job {
                    run_id: run_id.to_string(),
                },
            }),
            hop: view.hop.map(
                |SyncHop {
                     relationship_id,
                     from,
                 }| HopBody {
                    relationship_id: relationship_id.to_string(),
                    from: version_id(&from),
                },
            ),
            source: view.source.map(|source| SourceBody {
                url: source.url,
                path: source.path,
                etag: source.etag,
                digests: source
                    .digests
                    .into_iter()
                    .map(|(algorithm, digest)| SourceDigestBody {
                        algorithm: algorithm.hash_key().to_string(),
                        digest: hex::encode(&digest.digest),
                        scope: match digest.scope {
                            DigestScope::FullObject => "full_object",
                            DigestScope::Composite => "composite",
                        }
                        .to_string(),
                        origin: match digest.origin {
                            DigestOrigin::ListingEtag => "listing_etag",
                            DigestOrigin::Manifest => "manifest",
                            DigestOrigin::VerifiedRead => "verified_read",
                        }
                        .to_string(),
                    })
                    .collect(),
            }),
            projection: view
                .projection
                .into_iter()
                .map(|item| match item {
                    OriginProjection::BasedOn { id } => ProjectionBody::BasedOn { id },
                    OriginProjection::CreatedBy { run_id } => ProjectionBody::CreatedBy {
                        run_id: run_id.to_string(),
                    },
                })
                .collect(),
        }
    }
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/objects/origin",
    tag = "data/storage",
    summary = "Read where a file version came from",
    description = r#"Returns the origin record of one file version, its sync hop, the external source
it is bound to, and the statements a dataset may add about it.

**Authentication**: realm bearer token with READ on the file.

**Behavior**
- The origin is set by the server when the version is minted and never changes; members of a
  distributed bucket hold the same origin. A sync destination keeps the source's origin and
  adds a hop naming the relationship and the exact source version.
- Readers of the file see the full record, including the uploader, node and device.
- `projection` is what datasets receive: `isBasedOn` the source URL, the copied version or
  the synced source version; `CreateAction` for job results; nothing for uploads and imports
  in place.

**Limits**
- One version per call; one point read. A member of a distributed bucket that holds no copy
  of the key's range relays the call to a holder with the caller's token."#,
    params(
        ("bucket" = String, Path, description = "Bucket name on this node"),
        OriginParams
    ),
    responses(
        (status = 200, description = "The version's origin", body = OriginResponse, example = json!({
            "version_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2",
            "global_id": null,
            "created_by": "01K8Z1C0RAT0R0000000000000",
            "created_at": "2026-08-20T10:12:00Z",
            "origin": {"kind": "copy", "from": "https://w3id.org/aruna/data/k3V9xQ2mT7aLd0RpYc8WnHfJ5uBsE1gZtO4iXvNq6Ay/01K8T6A2C4E6G8JKMPRSTVXZ13"},
            "hop": null,
            "source": null,
            "projection": [{"property": "isBasedOn", "id": "https://w3id.org/aruna/data/k3V9xQ2mT7aLd0RpYc8WnHfJ5uBsE1gZtO4iXvNq6Ay/01K8T6A2C4E6G8JKMPRSTVXZ13"}]
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No READ on the file", body = ErrorResponse),
        (status = 404, description = "Bucket or version not found", body = ErrorResponse),
        (status = 503, description = "No holder of the key's range answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_origin(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    headers: HeaderMap,
    Path(bucket): Path<String>,
    Query(params): Query<OriginParams>,
) -> ServerResult<Json<OriginResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let info = bucket_info(&state, &bucket).await?;
    let path = object_permission_path(
        state.get_realm_id(),
        info.group_id,
        state.get_node_id(),
        &bucket,
        &params.key,
    );
    ensure_permission(&state, &auth, path, Permission::READ).await?;
    // A member that holds no copy of the key's range asks a holder, which checks again.
    if let Some(binding) = info.distribution.as_ref()
        && !headers.contains_key(RELAY_HOP_HEADER)
    {
        let context = state.get_ctx();
        let route = route_read(context.as_ref(), binding, &params.key, false).await;
        if let ReadRoute::Holder { range_id, node_id } = route.map_err(map_guard_error)? {
            let mut holders = key_holders(context.as_ref(), binding.set_id, &params.key).await;
            holders.retain(|holder| *holder != state.get_node_id());
            holders.push(node_id);
            let path = origin_path(&bucket, &params);
            for node_id in holders {
                let forward = RestForward {
                    set_id: binding.set_id,
                    range_id,
                    node_id,
                };
                if let Some(answer) = relay_get(&state, &forward, &path, bearer.as_ref()).await? {
                    return Ok(Json(answer));
                }
            }
            return Err(ServerError::ServiceUnavailableReason(format!(
                "no holder of range {range_id} of distributed bucket {} answered",
                binding.set_id
            )));
        }
    }
    let view = read_origin(
        state.get_ctx().as_ref(),
        (state.get_realm_id(), state.get_node_id()),
        &bucket,
        &params.key,
        params.version_id,
    )
    .await
    .map_err(|error| match error {
        OriginError::BucketMissing | OriginError::VersionMissing => ServerError::NotFound,
        OriginError::Storage(reason) => ServerError::InternalError(reason),
    })?;
    Ok(Json(view.into()))
}

/// The origin route of one file version, for a holder.
fn origin_path(bucket: &str, params: &OriginParams) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query.append_pair("key", &params.key);
    if let Some(version_id) = params.version_id {
        query.append_pair("version_id", &version_id.to_string());
    }
    let bucket: String = url::form_urlencoded::byte_serialize(bucket.as_bytes()).collect();
    format!("/data/buckets/{bucket}/objects/origin?{}", query.finish())
}

/// One way files get into the bucket.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FileSourceBody {
    /// Another bucket's files are copied in by a sync (`once`, `reference`, `continuous`).
    Sync {
        relationship_id: String,
        source: String,
        mode: String,
    },
    /// A storage backend folder of the group is registered without copying.
    Storage {
        import_id: String,
        backend_id: String,
        backend_prefix: String,
        key_prefix: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct FileSourcesResponse {
    pub sources: Vec<FileSourceBody>,
    /// False when more rows exist than one answer reads.
    pub complete: bool,
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/sources",
    tag = "data/storage",
    summary = "List how files get into a bucket",
    description = r#"Lists the incoming paths of a bucket on this node: syncs that copy files into it
and storage folders registered into it.

**Authentication**: realm bearer token with READ on the bucket.

**Behavior**
- Answered from configuration only; the bucket's objects are never read. Direct uploads are
  always possible and not listed. Files registered from an external source or a database
  show it in their own origin.
- Outgoing syncs and the nodes a bucket is stored on are not sources.

**Limits**
- At most 256 syncs and 256 import rules are read; `complete` says whether that was all."#,
    params(("bucket" = String, Path, description = "Bucket name on this node")),
    responses(
        (status = 200, description = "Incoming paths of the bucket", body = FileSourcesResponse, example = json!({
            "sources": [{
                "kind": "sync",
                "relationship_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2",
                "source": "arn:aruna:k3V9xQ2mT7aLd0RpYc8WnHfJ5uBsE1gZtO4iXvNq6Ay:ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6:s3/spire-staging",
                "mode": "continuous"
            }],
            "complete": true
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No READ on the bucket", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_sources(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
) -> ServerResult<Json<FileSourcesResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let info = bucket_info(&state, &bucket).await?;
    let path = bucket_permission_path(
        state.get_realm_id(),
        info.group_id,
        state.get_node_id(),
        &bucket,
    );
    ensure_permission(&state, &auth, path, Permission::READ).await?;
    let (sources, complete) = bucket_sources(state.get_ctx().as_ref(), info.group_id, &bucket)
        .await
        .map_err(ServerError::InternalError)?;
    Ok(Json(FileSourcesResponse {
        sources: sources
            .into_iter()
            .map(|source| match source {
                FileSource::Sync {
                    relationship_id,
                    source,
                    mode,
                } => FileSourceBody::Sync {
                    relationship_id: relationship_id.to_string(),
                    source,
                    mode: match mode {
                        SyncMode::Once => "once",
                        SyncMode::Reference => "reference",
                        SyncMode::Continuous => "continuous",
                    }
                    .to_string(),
                },
                FileSource::Storage {
                    import_id,
                    backend_id,
                    backend_prefix,
                    key_prefix,
                } => FileSourceBody::Storage {
                    import_id: import_id.to_string(),
                    backend_id: backend_id.to_string(),
                    backend_prefix,
                    key_prefix,
                },
            })
            .collect(),
        complete,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    // A relayed origin read names the same key and version on the holder.
    #[test]
    fn relay_path_kept() {
        let params = OriginParams {
            key: "raw/a b.txt".to_string(),
            version_id: Some(Ulid::from_bytes([1; 16])),
        };
        let path = origin_path("lab", &params);
        assert_eq!(
            path,
            format!(
                "/data/buckets/lab/objects/origin?key=raw%2Fa+b.txt&version_id={}",
                Ulid::from_bytes([1; 16])
            )
        );
    }
}
