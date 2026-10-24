//! Serves the data link routes: which datasets link an identifier, a file, a folder or a bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{bucket_permission_path, object_permission_path};
use aruna_core::structs::storage::data_link::{DataLink, LinkScope};
use aruna_operations::metadata::api::{
    LINKS_MAX_LIMIT, LinkNodeRequest, LinkPage, LinkQuery, file_queries, links_distributed,
};
use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, ToSchema};

use crate::auth::{ValidatedBearer, ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::metadata::{bearer_token_string, map_api_error};
use crate::routes::storage::deletion::bucket_info;
use crate::server::state::ServerState;

const DEFAULT_LIMIT: usize = 25;

#[derive(Debug, Deserialize, IntoParams)]
pub struct DataLinksParams {
    /// Global id (URL or short path), content w3id or bucket-prefix ARN.
    pub id: String,
    /// Only linked parts: Dataset-typed `hasPart` entries of a dataset root.
    #[serde(default)]
    pub parts_only: bool,
    /// The `next_after` of the previous page.
    pub after: Option<Ulid>,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct BucketLinksParams {
    /// A file: its version, its content and every linked folder that contains its key.
    pub key: Option<String>,
    /// The file version; the current version when absent.
    pub version_id: Option<Ulid>,
    /// A folder (`raw/2026/`) or the bucket root (empty): datasets that link exactly it.
    pub prefix: Option<String>,
    #[serde(default)]
    pub parts_only: bool,
    pub after: Option<Ulid>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DataLinkHit {
    /// The identifier as the dataset names it, in URL or ARN form.
    pub id: String,
    /// A linked part: the dataset's files include everything under it.
    pub part: bool,
    pub subject_iris: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DataLinkDocument {
    pub document_id: String,
    pub group_id: String,
    pub document_path: String,
    pub graph_iri: String,
    pub title: Option<String>,
    pub links: Vec<DataLinkHit>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DataLinksResponse {
    pub documents: Vec<DataLinkDocument>,
    /// Pass as `after` for the next page; absent on the last page.
    pub next_after: Option<String>,
    /// Every metadata node answered within its budget with no materialization unsettled.
    pub complete: bool,
    /// A matching document exists that the caller may not read.
    pub hidden_references_exist: bool,
    pub nodes_failed: usize,
    pub pending_materializations: usize,
    pub failed_materializations: usize,
}

impl From<LinkPage> for DataLinksResponse {
    fn from(page: LinkPage) -> Self {
        Self {
            documents: page
                .documents
                .into_iter()
                .map(|document| DataLinkDocument {
                    document_id: document.document_id.to_string(),
                    group_id: document.group_id,
                    document_path: document.document_path,
                    graph_iri: document.graph_iri,
                    title: document.title,
                    links: document
                        .links
                        .into_iter()
                        .map(|link| DataLinkHit {
                            id: link.id,
                            part: link.part,
                            subject_iris: link.subject_iris,
                        })
                        .collect(),
                })
                .collect(),
            next_after: page.next_after.map(|id| id.to_string()),
            complete: page.complete,
            hidden_references_exist: page.hidden,
            nodes_failed: page.nodes_failed,
            pending_materializations: page.pending_materializations,
            failed_materializations: page.failed_materializations,
        }
    }
}

fn example() -> serde_json::Value {
    serde_json::json!({
        "documents": [{
            "document_id": "01K8Z0DS0REEF0000000000005",
            "group_id": "01K8XW4C2T6B9Q1ZHDNE5A7RMF",
            "document_path": "datasets/reef-survey-2026",
            "graph_iri": "https://w3id.org/aruna/01K8Z0DS0REEF0000000000005",
            "title": "Reef survey 2026",
            "links": [{
                "id": "https://w3id.org/aruna/data/k3V9xQ2mT7aLd0RpYc8WnHfJ5uBsE1gZtO4iXvNq6Ay/01K8Y2B6D8F0H2K4M6P8R0T2V4/cmF3LzIwMjYv",
                "part": true,
                "subject_iris": ["https://w3id.org/aruna/01K8Z0DS0REEF0000000000005"]
            }]
        }],
        "next_after": null,
        "complete": true,
        "hidden_references_exist": false,
        "nodes_failed": 0,
        "pending_materializations": 0,
        "failed_materializations": 0
    })
}

#[utoipa::path(
    get,
    path = "/metadata/links",
    tag = "metadata/references",
    summary = "List datasets that link an Aruna data identifier",
    description = r#"Lists the readable metadata documents that name one Aruna data identifier.

**Authentication**: realm bearer token. Each document is filtered by the caller's READ
permission; unreadable matches only set `hidden_references_exist`.

**Behavior**
- `id` is a global id of a distributed bucket (version, bucket or prefix; URL or short path),
  a content w3id or hash ARN, or a bucket-prefix ARN of an ordinary bucket. URL and short path
  forms of one id find the same documents; literal `contentUrl` values count as well.
- Every metadata node answers from its own link postings; documents merge by id.
- `complete` is false when a node did not answer, a node's budget ended its walk, or a node has
  queued or failed materializations whose links may be missing.

**Limits**
- At most 100 documents per page; continue with `after`."#,
    params(DataLinksParams),
    responses(
        (status = 200, description = "Readable documents that link the identifier", body = DataLinksResponse, example = json!(example())),
        (status = 400, description = "`id` names no Aruna data", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm", body = ErrorResponse),
        (status = 503, description = "No metadata node could answer", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn data_links(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Query(params): Query<DataLinksParams>,
) -> ServerResult<Json<DataLinksResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let link = DataLink::parse(params.id.trim()).ok_or_else(|| {
        ServerError::BadRequestReason("id names no Aruna data identifier".to_string())
    })?;
    let request = LinkNodeRequest {
        queries: vec![LinkQuery::Exact(link)],
        parts_only: params.parts_only,
        after: params.after,
        limit: page_limit(params.limit),
    };
    run(&state, auth, bearer_token, request).await
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/links",
    tag = "metadata/references",
    summary = "List datasets that link a file, folder or bucket",
    description = r#"Lists the readable metadata documents that link a file of this node's bucket, a
folder of it, or the bucket root.

**Authentication**: realm bearer token with READ on the file (`key`) or on the bucket
(`prefix`). Each document is filtered by the caller's READ permission.

**Behavior**
- With `key`: documents naming the file's version (global id) or content, and documents whose
  linked parts contain the key. With `parts_only` only the containing linked parts count: this
  is the file's derived `isPartOf`, computed now and never stored per file.
- With `prefix`: documents that link exactly that folder; an empty prefix is the bucket root.
- A distributed bucket is named by its set, so every member answers the same; an ordinary bucket
  is named by this node.
- Completeness as for `/metadata/links`.

**Limits**
- At most 100 documents per page; continue with `after`. Prefix probes are bounded by the number
  of distinct linked prefix lengths of the bucket, never by its objects."#,
    params(
        ("bucket" = String, Path, description = "Bucket name on this node"),
        BucketLinksParams
    ),
    responses(
        (status = 200, description = "Readable documents that link the file, folder or bucket", body = DataLinksResponse, example = json!(example())),
        (status = 400, description = "Neither or both of `key` and `prefix`", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No READ on the file or bucket", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 503, description = "No metadata node could answer", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn bucket_links(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Path(bucket): Path<String>,
    Query(params): Query<BucketLinksParams>,
) -> ServerResult<Json<DataLinksResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let info = bucket_info(&state, &bucket).await?;
    let (realm_id, node_id) = (state.get_realm_id(), state.get_node_id());
    let queries = match (params.key, params.prefix) {
        (Some(key), None) => {
            let path = object_permission_path(realm_id, info.group_id, node_id, &bucket, &key);
            ensure_permission(&state, &auth, path, Permission::READ).await?;
            file_queries(
                state.get_ctx().as_ref(),
                realm_id,
                node_id,
                &bucket,
                &key,
                params.version_id,
            )
            .await
            .map_err(map_api_error)?
        }
        (None, Some(prefix)) => {
            let path = bucket_permission_path(realm_id, info.group_id, node_id, &bucket);
            ensure_permission(&state, &auth, path, Permission::READ).await?;
            let scope = match &info.distribution {
                Some(binding) => LinkScope::Set {
                    realm_id,
                    set_id: binding.set_id,
                },
                None => LinkScope::Node {
                    realm_id,
                    node_id,
                    bucket: bucket.clone(),
                },
            };
            vec![LinkQuery::Exact(scope.link(&prefix))]
        }
        _ => {
            return Err(ServerError::BadRequestReason(
                "pass exactly one of key and prefix".to_string(),
            ));
        }
    };
    let request = LinkNodeRequest {
        queries,
        parts_only: params.parts_only,
        after: params.after,
        limit: page_limit(params.limit),
    };
    run(&state, auth, bearer_token, request).await
}

fn page_limit(limit: Option<usize>) -> usize {
    limit.unwrap_or(DEFAULT_LIMIT).clamp(1, LINKS_MAX_LIMIT)
}

async fn run(
    state: &Arc<ServerState>,
    auth: AuthContext,
    bearer_token: Option<ValidatedBearer>,
    request: LinkNodeRequest,
) -> ServerResult<Json<DataLinksResponse>> {
    let page = links_distributed(
        state.get_ctx().as_ref(),
        state.get_realm_id(),
        state.get_node_id(),
        Some(auth),
        bearer_token_string(bearer_token),
        request,
    )
    .await
    .map_err(map_api_error)?;
    Ok(Json(page.into()))
}
