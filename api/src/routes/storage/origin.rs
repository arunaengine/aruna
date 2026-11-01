//! Serves the origin of a file version: how its content got into the bucket and what a
//! dataset may say about it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::SyncMode;
use aruna_core::structs::execution::source_access::{DigestOrigin, DigestScope};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{bucket_permission_path, object_permission_path};
use aruna_core::structs::storage::data::identity::content_id;
use aruna_core::structs::storage::distribution::ReadRoute;
use aruna_core::structs::storage::distribution::delegation::COPY_READ_HEADER;
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::origin::{
    OriginProjection, OriginVersion, SyncHop, VersionOrigin,
};
use aruna_core::structs::storage::replication::W3idIdentifier;
use aruna_operations::distribution::guard::{
    GuardError, copy_readers, read_coordinator, route_read,
};
use aruna_operations::distribution::resolve::{
    GlobalResolution, RESOLVE_BUDGET, key_holders, resolve_version,
};
use aruna_operations::s3::bucket::sources::{FileSource, bucket_sources};
use aruna_operations::s3::object::origin::{OriginError, OriginView, bucket_runs, read_origin};
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
use crate::metadata::forwarded_auth_token;
use crate::routes::identifiers::IdentifierView;
use crate::routes::management_relay::RELAY_HOP_HEADER;
use crate::routes::storage::bucket_metadata::{
    BucketIdentifierBody, BucketMetadataBody, DatasetDescriptionBody, describe_bucket,
};
use crate::routes::storage::deletion::bucket_info;
use crate::routes::storage::distributed::map_guard_error;
use crate::routes::storage::relay::{
    self, RestForward, node_directed, node_read, relay_get, relayed_set, stale_miss,
};
use crate::server::state::ServerState;
use aruna_core::structs::storage::bucket_metadata::ReleaseRef;
use aruna_operations::s3::bucket::release::{
    BucketScope, MembershipCaller, MembershipQuery, release_members,
};

/// How long an origin read directed at another node may take, body included.
const RELAY_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(OpenApi)]
#[openapi()]
pub struct OriginApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(OriginApiDoc::openapi())
        .routes(routes!(get_origin))
        .routes(routes!(get_version_origin))
        .routes(routes!(get_sources))
        .routes(routes!(get_bucket_runs))
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
    /// From an external source; details are under `source`.
    Source,
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
    /// Every source of the version. All are equal: a read uses any source that proves the
    /// bytes, healthy ones first. The first one is the source `url` and `path` name.
    pub sources: Vec<SourcePlaceBody>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SourcePlaceBody {
    /// Public URL of the object at this source, when the source has one.
    pub url: Option<String>,
    pub path: String,
    /// The connector of this node the source reads through; absent for a public source.
    pub connector_id: Option<String>,
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
    /// A `PropertyValue` the database's parser found, linked by `identifier`.
    #[serde(rename = "identifier")]
    Identifier {
        name: String,
        value: String,
        url: String,
    },
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
    /// The bucket's metadata now; it may have changed since the version was made.
    pub bucket_metadata: Option<BucketMetadataBody>,
    /// What the bucket's linked dataset says now, read with the caller's access.
    #[serde(default)]
    pub description: Option<DatasetDescriptionBody>,
    /// Links the bucket metadata's identifier rules find in the key and source URL.
    #[serde(default)]
    pub identifiers: Vec<IdentifierView>,
    /// The content id of the bytes, from their BLAKE3; absent for versions not stored here
    /// as bytes, such as references.
    pub content_id: Option<String>,
    /// Ranges a copy answered for because their coordinator could not; it may be behind.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stale_ranges: Vec<String>,
    /// Releases of the bucket that pin exactly this version and the caller may read.
    #[serde(default)]
    pub releases: Vec<ReleaseView>,
    /// `false` when release membership could not be checked everywhere, so one may be missing.
    #[serde(default = "complete_default")]
    pub releases_complete: bool,
}

fn complete_default() -> bool {
    true
}

/// A release that pins a version: its release crate's exact version, label and upstream ids.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReleaseView {
    pub crate_version: String,
    pub label: String,
    pub upstream: Vec<BucketIdentifierBody>,
}

impl From<ReleaseRef> for ReleaseView {
    fn from(release: ReleaseRef) -> Self {
        Self {
            crate_version: release.crate_version.to_id(),
            label: release.label,
            upstream: (release.upstream.into_iter())
                .map(|identifier| BucketIdentifierBody {
                    name: identifier.name,
                    value: identifier.value,
                    url: identifier.url,
                })
                .collect(),
        }
    }
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
                VersionOrigin::Source => OriginBody::Source,
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
                sources: (source.sources.into_iter())
                    .map(|place| SourcePlaceBody {
                        url: place.url,
                        path: place.path,
                        connector_id: place.connector_id.map(|id| id.to_string()),
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
                    OriginProjection::Identifier { name, value, url } => {
                        ProjectionBody::Identifier { name, value, url }
                    }
                })
                .collect(),
            bucket_metadata: view.bucket_metadata.map(Into::into),
            description: None,
            identifiers: view.identifiers.into_iter().map(Into::into).collect(),
            content_id: view.content_hash.map(content_id),
            stale_ranges: Vec::new(),
            releases: Vec::new(),
            releases_complete: true,
        }
    }
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/objects/origin",
    tag = "data/storage",
    summary = "Read where a file version came from",
    description = r#"Returns the origin record of one file version, its sync hop, the external source it is bound to, and the statements a dataset may add about it.

**Authentication**: realm bearer token with READ on the file.

**Behavior**
- The origin is set by the server when the version is minted and never changes; members of a
  distributed bucket hold the same origin. A sync destination keeps the source's origin and
  adds a hop naming the relationship and the exact source version.
- Readers of the file see the full record, including the uploader, node and device.
- `projection` is what datasets receive: `isBasedOn` the source URL, the copied version or
  the synced source version when it is a version of a distributed bucket (a node ARN names
  its bucket and key, so it is left out); `CreateAction` for job results; nothing for
  uploads and imports in place; `identifier` for each link the database's parser finds.
- `bucket_metadata` and `identifiers` come from the bucket's metadata now, not from when the
  version was made; a bucket without metadata gives none. `description` is what the bucket's
  linked dataset says now, read with the caller's access; it is `null` while it cannot be read.
- `releases` lists the bucket's releases whose release crate names exactly this version, among
  those the caller may read. `releases_complete` is `false` when membership could not be
  checked everywhere (a range holder of a distributed bucket did not answer), so a release may
  be missing.
- Without `version_id` the range's coordinator answers. While it cannot, a ready copy answers
  and `stale_ranges` names the range, since the copy may be behind; a 404 from a copy has the
  code `stale_not_found` and says the version may exist.

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
            "projection": [{"property": "isBasedOn", "id": "https://w3id.org/aruna/data/k3V9xQ2mT7aLd0RpYc8WnHfJ5uBsE1gZtO4iXvNq6Ay/01K8T6A2C4E6G8JKMPRSTVXZ13"}],
            "bucket_metadata": null,
            "description": null,
            "identifiers": [],
            "content_id": "https://w3id.org/aruna/data/af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No READ on the file", body = ErrorResponse),
        (status = 404, description = "Bucket or version not found, or not found on a copy that may be behind", body = ErrorResponse),
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
    origin_answer(&state, &auth, bearer, &headers, bucket, params).await
}

/// The origin of one version of a local bucket's key, or the answer of the holder this member
/// relays to with the caller's token.
async fn origin_answer(
    state: &Arc<ServerState>,
    auth: &AuthContext,
    bearer: Option<ValidatedBearer>,
    headers: &HeaderMap,
    bucket: String,
    params: OriginParams,
) -> ServerResult<Json<OriginResponse>> {
    let info = bucket_info(state, &bucket).await?;
    let path = object_permission_path(
        state.get_realm_id(),
        info.group_id,
        state.get_node_id(),
        &bucket,
        &params.key,
    );
    ensure_permission(state, auth, path, Permission::READ).await?;
    relayed_set(&info, headers)?;
    // A member that holds no copy of the key's range asks a holder, which checks again. A copy
    // may lag behind the current version, so only a pinned version comes from one, unless the
    // coordinator cannot answer; then a copy answers, marked stale.
    let mut stale = None;
    if let Some(binding) = info.distribution.as_ref()
        && !headers.contains_key(RELAY_HOP_HEADER)
    {
        let context = state.get_ctx();
        if params.version_id.is_none() {
            let path = origin_path(&bucket, &params);
            let coordinator = read_coordinator(context.as_ref(), binding, &params.key).await;
            let asked = match coordinator.map_err(map_guard_error) {
                Ok(None) => None,
                Ok(Some((range_id, node_id))) => {
                    let target = (binding.set_id, range_id, node_id);
                    let path = (path.as_str(), HeaderMap::new());
                    Some(relay_origin(state, target, vec![node_id], path, bearer.clone()).await)
                }
                Err(error) => Some(Err(error)),
            };
            match asked {
                Some(Err(error)) if error.status_code().is_server_error() => {
                    let readers = copy_readers(context.as_ref(), binding, &params.key).await;
                    let (range_id, here, copies) = readers.map_err(map_guard_error)?;
                    if !here && copies.is_empty() {
                        return Err(error);
                    }
                    stale = Some(range_id);
                    if !here {
                        let target = (binding.set_id, range_id);
                        return copy_origin(state, target, copies, &path, bearer).await;
                    }
                }
                Some(answer) => return answer,
                None => {}
            }
        }
        let route = route_read(context.as_ref(), binding, &params.key, false).await;
        if let ReadRoute::Holder { range_id, node_id } = route.map_err(map_guard_error)? {
            let mut holders = key_holders(context.as_ref(), binding.set_id, &params.key).await;
            holders.retain(|holder| *holder != state.get_node_id());
            holders.push(node_id);
            let target = (binding.set_id, range_id, node_id);
            let path = origin_path(&bucket, &params);
            return relay_origin(state, target, holders, (&path, HeaderMap::new()), bearer).await;
        }
    }
    // A relayed current read sent here as coordinator is refused once this node is none.
    if let Some(binding) = info.distribution.as_ref()
        && params.version_id.is_none()
        && headers.contains_key(RELAY_HOP_HEADER)
        && !headers.contains_key(COPY_READ_HEADER)
    {
        let coordinator = read_coordinator(state.get_ctx().as_ref(), binding, &params.key).await;
        if let Some((range_id, node_id)) = coordinator.map_err(map_guard_error)? {
            return Err(map_guard_error(GuardError::NotCoordinator {
                set_id: binding.set_id,
                range_id,
                node_id,
            }));
        }
    }
    let view = read_origin(
        state.get_ctx().as_ref(),
        (state.get_realm_id(), state.get_node_id()),
        &bucket,
        &params.key,
        params.version_id,
    )
    .await;
    // A copy may not have the version yet, so its miss goes on to the range's coordinator; a
    // miss on a range this node fenced is refused.
    if let (Err(OriginError::VersionMissing), Some(binding), None) =
        (&view, info.distribution.as_ref(), stale)
    {
        let coordinator = read_coordinator(state.get_ctx().as_ref(), binding, &params.key).await;
        if let Some((range_id, node_id)) = coordinator.map_err(map_guard_error)?
            && !headers.contains_key(RELAY_HOP_HEADER)
        {
            let path = origin_path(&bucket, &params);
            let target = (binding.set_id, range_id, node_id);
            // While the coordinator cannot answer, this copy's miss stands, marked.
            let asked = relay_origin(
                state,
                target,
                vec![node_id],
                (&path, HeaderMap::new()),
                bearer,
            );
            return match asked.await {
                Err(error) if error.status_code().is_server_error() => Err(stale_miss(range_id)),
                answer => answer,
            };
        }
    }
    let view = view.map_err(|error| match (error, stale) {
        (OriginError::VersionMissing, Some(range_id)) => stale_miss(range_id),
        (OriginError::BucketMissing | OriginError::VersionMissing, _) => ServerError::NotFound,
        (OriginError::Storage(reason), _) => ServerError::InternalError(reason),
    })?;
    let version_id = view.version_id;
    let token = forwarded_auth_token(bearer.clone())?;
    let metadata = view.bucket_metadata.as_ref();
    let (description, _) = describe_bucket(state, metadata, (Some(auth), token)).await;
    let mut answer = OriginResponse::from(view);
    answer.description = description;
    answer.stale_ranges = stale.iter().map(ToString::to_string).collect();
    // Only releases the caller may read count; any other node is asked with its own bearer.
    let scope = BucketScope {
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        bucket: &bucket,
        info: &info,
    };
    let caller = MembershipCaller {
        auth,
        bearer: bearer.as_ref().map(ValidatedBearer::as_str),
    };
    let query = [MembershipQuery {
        key: params.key.clone(),
        version_id,
    }];
    match release_members(state.get_ctx().as_ref(), scope, caller, &query).await {
        Ok(mut found) => {
            let members = found.answers.remove(0);
            answer.releases = members.releases.into_iter().map(Into::into).collect();
            answer.releases_complete = members.complete;
        }
        Err(_) => answer.releases_complete = false,
    }
    Ok(Json(answer))
}

/// Asks the copies of a range whose coordinator cannot answer; the answer is marked stale and
/// a miss says the version may exist.
async fn copy_origin(
    state: &ServerState,
    (set_id, range_id): (Ulid, Ulid),
    copies: Vec<aruna_core::NodeId>,
    path: &str,
    bearer: Option<ValidatedBearer>,
) -> ServerResult<Json<OriginResponse>> {
    let Some(last) = copies.last().copied() else {
        return Err(ServerError::ServiceUnavailableReason(format!(
            "neither the coordinator nor a copy of range {range_id} of distributed bucket \
             {set_id} answered"
        )));
    };
    let mut sent = HeaderMap::new();
    sent.insert(COPY_READ_HEADER, http::HeaderValue::from_static("1"));
    let target = (set_id, range_id, last);
    match relay_origin(state, target, copies, (path, sent), bearer).await {
        Ok(Json(mut answer)) => {
            answer.stale_ranges = vec![range_id.to_string()];
            Ok(Json(answer))
        }
        Err(ServerError::NotFound) => Err(stale_miss(range_id)),
        Err(error) => Err(error),
    }
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct VersionOriginParams {
    /// The exact version: a global id URL or a versioned object w3id (node ARN).
    pub version: String,
}

#[utoipa::path(
    get,
    path = "/data/origin",
    tag = "data/storage",
    summary = "Read a version's origin by its id",
    description = r#"Returns the origin of one exact version named by the id a crate or another origin uses.

**Authentication**: realm bearer token with READ on the file, checked on the node that holds it.

**Behavior**
- A versioned object w3id of this node is answered as by the bucket origin route. One of
  another node is read on that node with the caller's token; that node may relay once more
  to a holder of a distributed bucket's range, and never further.
- A global id is answered by the holders of the version's range, in turn.
- The answer and its refusals are the holder's: 403 without READ, 404 for an unknown version.

**Limits**
- One version per call; at most two relays."#,
    params(VersionOriginParams),
    responses(
        (status = 200, description = "The version's origin", body = OriginResponse, example = json!({
            "version_id": "01K8T6A2C4E6G8JKMPRSTVXZ13",
            "global_id": "https://w3id.org/aruna/data/k3V9xQ2mT7aLd0RpYc8WnHfJ5uBsE1gZtO4iXvNq6Ay/01K8T6A2C4E6G8JKMPRSTVXZ13",
            "created_by": "01K8Z1C0RAT0R0000000000000",
            "created_at": "2026-08-20T10:12:00Z",
            "origin": {"kind": "job", "run_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2"},
            "hop": null,
            "source": null,
            "projection": [{"property": "CreateAction", "run_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2"}],
            "bucket_metadata": null,
            "description": null,
            "identifiers": [],
            "content_id": null
        })),
        (status = 400, description = "Not a version id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No READ on the file", body = ErrorResponse),
        (status = 404, description = "Version not found", body = ErrorResponse),
        (status = 409, description = "The read was directed at another node", body = ErrorResponse),
        (status = 503, description = "No node holding the version answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_version_origin(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    headers: HeaderMap,
    Query(params): Query<VersionOriginParams>,
) -> ServerResult<Json<OriginResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let deadline = tokio::time::Instant::now() + RELAY_DEADLINE;
    version_answer(&state, &auth, bearer, &headers, (&params.version, deadline)).await
}

/// The origin of the version `version` names, read where it lives with the caller's token.
pub(crate) async fn version_answer(
    state: &Arc<ServerState>,
    auth: &AuthContext,
    bearer: Option<ValidatedBearer>,
    headers: &HeaderMap,
    (version, deadline): (&str, tokio::time::Instant),
) -> ServerResult<Json<OriginResponse>> {
    let directed = node_directed(state, headers)?;
    let refused = || ServerError::BadRequestReason("not a version id".to_string());
    match W3idIdentifier::parse(version).map_err(|_| refused())? {
        W3idIdentifier::VersionedObject(arn) if arn.realm_id != state.get_realm_id() => {
            Err(ServerError::NotFound)
        }
        W3idIdentifier::VersionedObject(arn) if arn.node_id == state.get_node_id() => {
            let params = OriginParams {
                key: arn.key,
                version_id: Some(arn.version),
            };
            origin_answer(state, auth, bearer, headers, arn.bucket, params).await
        }
        // A node-directed read reached its node; it never directs the read on.
        W3idIdentifier::VersionedObject(_) if directed => Err(ServerError::Conflict(
            "the version is not on the node the read was directed at".to_string(),
        )),
        W3idIdentifier::VersionedObject(arn) => {
            let path = relay::query_path("/data/origin", &[("version", version)]);
            node_read(state, arn.node_id, &path, bearer.as_ref(), deadline)
                .await
                .map(Json)
        }
        W3idIdentifier::Global(GlobalId::Version {
            realm_id,
            version_id,
        }) if realm_id == state.get_realm_id() => {
            global_origin(state, auth, bearer, headers, version_id).await
        }
        _ => Err(refused()),
    }
}

/// The origin of a distributed bucket's version from this member, or from its holders in turn.
async fn global_origin(
    state: &Arc<ServerState>,
    auth: &AuthContext,
    bearer: Option<ValidatedBearer>,
    headers: &HeaderMap,
    version_id: Ulid,
) -> ServerResult<Json<OriginResponse>> {
    let deadline = tokio::time::Instant::now() + RESOLVE_BUDGET;
    let found = match resolve_version(state.get_ctx().as_ref(), version_id, deadline).await {
        GlobalResolution::Found(found) => found,
        GlobalResolution::NotFound => return Err(ServerError::NotFound),
        GlobalResolution::Unavailable => {
            return Err(ServerError::ServiceUnavailableReason(
                "the version's bucket did not answer".to_string(),
            ));
        }
        GlobalResolution::Ambiguous => {
            return Err(ServerError::Conflict(
                "more than one distributed bucket holds this version id".to_string(),
            ));
        }
    };
    let params = OriginParams {
        key: found.key.clone(),
        version_id: Some(version_id),
    };
    if found.local {
        return origin_answer(state, auth, bearer, headers, found.bucket, params).await;
    }
    let path = origin_path(&found.bucket, &params);
    // Holders come coordinator first, so its miss decides.
    let Some(coordinator) = found.holders.first().copied() else {
        return Err(ServerError::ServiceUnavailableReason(format!(
            "no holder of version {version_id} is known"
        )));
    };
    let target = (found.set_id, Ulid::nil(), coordinator);
    let sent = (path.as_str(), HeaderMap::new());
    relay_origin(state, target, found.holders, sent, bearer).await
}

/// Asks `holders` in turn for the origin; a copy's miss goes on, the coordinator's decides.
async fn relay_origin(
    state: &ServerState,
    (set_id, range_id, coordinator): (Ulid, Ulid, aruna_core::NodeId),
    holders: Vec<aruna_core::NodeId>,
    (path, sent): (&str, HeaderMap),
    bearer: Option<ValidatedBearer>,
) -> ServerResult<Json<OriginResponse>> {
    for node_id in holders {
        let forward = RestForward {
            set_id,
            range_id,
            node_id,
        };
        match relay_get(state, &forward, path, bearer.as_ref(), sent.clone()).await {
            Ok(Some(answer)) => return Ok(Json(answer)),
            Err(ServerError::NotFound) if node_id != coordinator => {}
            Ok(None) => {}
            Err(error) => return Err(error),
        }
    }
    Err(ServerError::ServiceUnavailableReason(format!(
        "no holder of range {range_id} of distributed bucket {set_id} answered"
    )))
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
    description = r#"Lists the incoming paths of a bucket on this node: syncs that copy files into it and storage folders registered into it.

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

#[derive(Debug, Deserialize, IntoParams)]
pub struct BucketRunsParams {
    /// Only files whose key starts with this; empty or absent for the whole bucket.
    #[serde(default)]
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BucketRunBody {
    /// The job id of the run, as `GET /compute/runs/{run_id}` takes it.
    pub run_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BucketRunsResponse {
    pub runs: Vec<BucketRunBody>,
    /// Every current file under the prefix was read and no run was left out.
    pub complete: bool,
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/runs",
    tag = "data/storage",
    summary = "List the runs that wrote a bucket's files",
    description = r#"Lists the compute runs whose results are current files of the bucket under a prefix, as their origins record them.

**Authentication**: realm bearer token with READ on the bucket, or on the prefix it lists.
Without a token, READ granted by public roles applies, and a refusal answers 401.

**Behavior**
- Asked on a node that holds the bucket; another node answers 404. A distributed bucket is
  answered from the ranges this node holds, so `complete` is false for it.
- Runs are newest first. Files that a later upload replaced count no more.

**Limits**
- At most 5000 current files are read and at most 50 runs named; `complete` is false when
  either limit cut the answer."#,
    params(
        ("bucket" = String, Path, description = "Bucket name on this node"),
        BucketRunsParams
    ),
    responses(
        (status = 200, description = "The runs that wrote current files under the prefix", body = BucketRunsResponse, example = json!({
            "runs": [{ "run_id": "01JJRSTVWXYZ0123456789ABCD" }],
            "complete": true
        })),
        (status = 401, description = "Invalid bearer token, or no public READ without one", body = ErrorResponse),
        (status = 403, description = "No READ on the bucket or the prefix", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []), ())
)]
pub async fn get_bucket_runs(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Query(params): Query<BucketRunsParams>,
) -> ServerResult<Json<BucketRunsResponse>> {
    // Signed out, only public roles grant READ, and a refusal hides whether the bucket exists.
    let signed_out = auth.is_none();
    let auth = auth.unwrap_or_else(|| AuthContext::anonymous(state.get_realm_id()));
    let auth = require_realm_auth(&state, Some(auth))?;
    let hidden = |error| match error {
        ServerError::NotFound | ServerError::Forbidden if signed_out => ServerError::Unauthorized,
        error => error,
    };
    let info = bucket_info(&state, &bucket).await.map_err(hidden)?;
    let (realm_id, node_id) = (state.get_realm_id(), state.get_node_id());
    let path = match params.prefix.is_empty() {
        true => bucket_permission_path(realm_id, info.group_id, node_id, &bucket),
        false => object_permission_path(realm_id, info.group_id, node_id, &bucket, &params.prefix),
    };
    ensure_permission(&state, &auth, path, Permission::READ)
        .await
        .map_err(hidden)?;
    let found = bucket_runs(state.get_ctx().as_ref(), &bucket, &params.prefix)
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let runs = found.run_ids.iter().map(|run_id| BucketRunBody {
        run_id: run_id.to_string(),
    });
    Ok(Json(BucketRunsResponse {
        runs: runs.collect(),
        complete: found.complete && info.distribution.is_none(),
    }))
}

#[cfg(test)]
#[path = "origin_tests.rs"]
mod tests;
