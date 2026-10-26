//! Serves the data link routes: which datasets link an identifier, a file, a folder or a bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{bucket_permission_path, object_permission_path};
use aruna_core::structs::storage::data::link::{DataLink, LinkScope};
use aruna_operations::metadata::api::{
    LINKS_MAX_LIMIT, LinkDocument, LinkNodeRequest, LinkPage, LinkQuery, file_queries,
    links_distributed, part_of, resolve_part,
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
    /// Documents per page, 1 to 100 (default 25).
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
    /// Only linked parts: with `key`, the file's derived `isPartOf`.
    #[serde(default)]
    pub parts_only: bool,
    /// The `next_after` of the previous page.
    pub after: Option<Ulid>,
    /// Documents per page, 1 to 100 (default 25).
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
    /// Documents whose links were cut at 65,536 per document; any of them may match.
    pub truncated_documents: usize,
}

impl From<LinkDocument> for DataLinkDocument {
    fn from(document: LinkDocument) -> Self {
        Self {
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
        }
    }
}

impl From<LinkPage> for DataLinksResponse {
    fn from(page: LinkPage) -> Self {
        Self {
            documents: page.documents.into_iter().map(Into::into).collect(),
            next_after: page.next_after.map(|id| id.to_string()),
            complete: page.complete,
            hidden_references_exist: page.hidden,
            nodes_failed: page.nodes_failed,
            pending_materializations: page.pending_materializations,
            failed_materializations: page.failed_materializations,
            truncated_documents: page.truncated_documents,
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
        "failed_materializations": 0,
        "truncated_documents": 0
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
  queued or failed materializations, or documents with more links than it keeps, whose links
  may be missing.

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
        confirm: None,
    };
    run(&state, auth, bearer_token, request).await
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/links",
    tag = "metadata/references",
    summary = "List datasets linking a file, folder or bucket",
    description = r#"Lists the readable metadata documents that link a file of this node's bucket, a folder of it, or the bucket root.

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
        confirm: None,
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

#[derive(Debug, Deserialize, IntoParams)]
pub struct PartOfParams {
    /// Object key in the bucket.
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PartOfDataset {
    pub document_id: String,
    pub title: Option<String>,
    /// The dataset's w3id, which file metadata names as `isPartOf`.
    pub graph_iri: String,
    /// The linked parts of the dataset that contain the file.
    pub part_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PartOfResponse {
    /// At most ten datasets, by document id.
    pub datasets: Vec<PartOfDataset>,
    /// More datasets contain the file; `more_datasets` lists them all.
    pub more: bool,
    pub complete: bool,
    pub hidden_references_exist: bool,
    /// The paged listing of every containing dataset.
    pub more_datasets: String,
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/objects/part-of",
    tag = "metadata/references",
    summary = "List the datasets a file is part of",
    description = r#"Lists the datasets whose live linked parts contain a file: its derived `isPartOf`.

**Authentication**: realm bearer token with READ on the file. Each dataset is filtered by the
caller's READ permission; unreadable ones only set `hidden_references_exist`.

**Behavior**
- A linked part is a Dataset-typed `hasPart` of a dataset root naming the bucket root or a
  folder of the bucket. Only such direct links make a file part of a dataset; a dataset that
  links another dataset gains nothing.
- Membership is computed now from the current links and never stored per file.

**Limits**
- At most ten datasets; `more` says when the paged listing in `more_datasets` has more.
  Denied matches use the same budget."#,
    params(
        ("bucket" = String, Path, description = "Bucket name on this node"),
        PartOfParams
    ),
    responses(
        (status = 200, description = "Datasets the file is part of", body = PartOfResponse, example = json!({
            "datasets": [{
                "document_id": "01K8Z0DS0REEF0000000000005",
                "title": "Reef survey 2026",
                "graph_iri": "https://w3id.org/aruna/01K8Z0DS0REEF0000000000005",
                "part_ids": ["arn:aruna:k3V9xQ2mT7aLd0RpYc8WnHfJ5uBsE1gZtO4iXvNq6Ay:ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6:s3/lab-raw-data/raw/2026/"]
            }],
            "more": false,
            "complete": true,
            "hidden_references_exist": false,
            "more_datasets": "/api/v1/data/buckets/lab-raw-data/links?key=raw%2F2026%2Freef-07%2Freads_R1.fastq.gz&parts_only=true"
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No READ on the file", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 503, description = "No metadata node could answer", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn file_part_of(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Path(bucket): Path<String>,
    Query(params): Query<PartOfParams>,
) -> ServerResult<Json<PartOfResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let info = bucket_info(&state, &bucket).await?;
    let (realm_id, node_id) = (state.get_realm_id(), state.get_node_id());
    let path = object_permission_path(realm_id, info.group_id, node_id, &bucket, &params.key);
    ensure_permission(&state, &auth, path, Permission::READ).await?;
    let found = part_of(
        state.get_ctx().as_ref(),
        realm_id,
        node_id,
        Some(auth),
        bearer_token_string(bearer_token),
        &bucket,
        &params.key,
    )
    .await
    .map_err(map_api_error)?;
    let encoded: String = url::form_urlencoded::byte_serialize(params.key.as_bytes()).collect();
    Ok(Json(PartOfResponse {
        datasets: found
            .datasets
            .into_iter()
            .map(|document| PartOfDataset {
                document_id: document.document_id.to_string(),
                title: document.title,
                graph_iri: document.graph_iri,
                part_ids: document
                    .links
                    .into_iter()
                    .filter(|link| link.part)
                    .map(|link| link.id)
                    .collect(),
            })
            .collect(),
        more: found.more,
        complete: found.complete,
        hidden_references_exist: found.hidden,
        more_datasets: format!("/api/v1/data/buckets/{bucket}/links?key={encoded}&parts_only=true"),
    }))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct PartTargetParams {
    /// A bucket or prefix global id URL, or a bucket-prefix ARN of an ordinary bucket.
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PartTargetResponse {
    /// The canonical identifier of the part.
    pub id: String,
    /// The bucket name; absent for a distributed bucket this node has no record of yet.
    pub bucket: Option<String>,
    pub prefix: String,
    /// The set of a distributed bucket.
    pub set_id: Option<String>,
    /// The node of an ordinary bucket.
    pub node_id: Option<String>,
    /// This node holds the bucket and can list the part's files itself.
    pub held_here: bool,
}

#[utoipa::path(
    get,
    path = "/metadata/links/target",
    tag = "metadata/references",
    summary = "Resolve a linked part to its bucket",
    description = r#"Resolves the identifier of a linked part, a bucket root or a folder, to the bucket and prefix whose files it covers.

**Authentication**: realm bearer token. Listing the files afterwards needs READ on the bucket
on the node that serves them; resolving grants nothing.

**Behavior**
- A distributed bucket is named by its set and browsed on any member; `held_here` says whether
  this node is one. An ordinary bucket is named by its node.
- Parts are always live: the listing shows the bucket's current files.

**Limits**
- One identifier per call; at most two point reads."#,
    params(PartTargetParams),
    responses(
        (status = 200, description = "The bucket and prefix of the part", body = PartTargetResponse, example = json!({
            "id": "https://w3id.org/aruna/data/k3V9xQ2mT7aLd0RpYc8WnHfJ5uBsE1gZtO4iXvNq6Ay/01K8Y2B6D8F0H2K4M6P8R0T2V4/cmF3LzIwMjYv",
            "bucket": "bakrep",
            "prefix": "raw/2026/",
            "set_id": "01K8Y2B6D8F0H2K4M6P8R0T2V4",
            "node_id": null,
            "held_here": true
        })),
        (status = 400, description = "`id` names no bucket or prefix", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn part_target(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(params): Query<PartTargetParams>,
) -> ServerResult<Json<PartTargetResponse>> {
    require_realm_auth(&state, auth)?;
    let link = DataLink::parse(params.id.trim())
        .filter(|link| link.scope().is_some())
        .ok_or_else(|| ServerError::BadRequestReason("id names no bucket or prefix".to_string()))?;
    let target = resolve_part(state.get_ctx().as_ref(), state.get_node_id(), link)
        .await
        .map_err(map_api_error)?;
    let set_id = match &target.link {
        DataLink::SetPrefix { set_id, .. } => Some(set_id.to_string()),
        _ => None,
    };
    Ok(Json(PartTargetResponse {
        id: target.link.to_id(),
        bucket: target.bucket,
        prefix: target.prefix,
        set_id,
        node_id: target.node_id.map(|node| node.to_string()),
        held_here: target.held_here,
    }))
}

#[cfg(test)]
#[path = "links_tests.rs"]
mod tests;
