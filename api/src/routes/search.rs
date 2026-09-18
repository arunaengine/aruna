//! Serves the search routes for buckets, object heads and the combined realm search.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{ValidatedBearer, parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::metadata::{
    MetadataQueryMode, SearchHitResponse, map_api_error, map_query_mode, map_search_hit,
};
use crate::routes::access::users::MIN_QUERY_CHARS;
use crate::server::state::ServerState;
use aruna_core::UserId;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_operations::driver::drive;
use aruna_operations::groups::search_groups::{SearchGroupsInput, SearchGroupsOperation};
use aruna_operations::metadata::api::{
    BucketSearchExecution, BucketSearchRequest, MetadataSearchExecution, MetadataSearchRequest,
    ObjectExecution, ObjectQueryMode, SearchQueryRequest, search_buckets_distributed,
    search_metadata as run_search_metadata, search_objects,
};
use aruna_operations::s3::object::search::ObjectKeyMatch;
use aruna_operations::users::search_users::{SearchUsersInput, SearchUsersOperation};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

const DEFAULT_SEARCH_LIMIT: usize = 10;
const MAX_SEARCH_LIMIT: usize = 100;
const MAX_BUCKET_LIMIT: usize = 50;
const SEARCH_TYPE_DOCUMENTS: &str = "documents";
const SEARCH_TYPE_BUCKETS: &str = "buckets";
const SEARCH_TYPE_GROUPS: &str = "groups";
const SEARCH_TYPE_USERS: &str = "users";

#[derive(OpenApi)]
#[openapi(
    tags((name = "search", description = "Unified realm search"))
)]
pub struct SearchApiDoc;

// Object search uses authenticated live heads with explicit partial or strict federation.
// Pagination is a watermarked live keyset; signed snapshots and directory tiers remain deferred.
pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(SearchApiDoc::openapi())
        .routes(routes!(unified_search))
        .routes(routes!(bucket_search))
        .routes(routes!(object_search))
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct SearchParams {
    #[serde(default)]
    pub q: String,
    /// Comma-separated subset of documents,buckets,groups,users. Defaults to all four.
    #[serde(default)]
    pub types: Option<String>,
    /// Per-section page size (default 10, clamped to 1..=100).
    #[serde(default)]
    pub limit: Option<usize>,
    /// Opaque continuation token. Only accepted when exactly one type is
    /// requested; a multi-type request with a cursor is rejected with 400.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Documents-only: restrict metadata hits to a single group id.
    #[serde(default)]
    pub group_id: Option<String>,
    /// Documents-only: exact RO-Crate conformsTo specification or Profile IRI.
    #[serde(default)]
    pub conforms_to: Option<String>,
    /// Documents-only: search mode (local or distributed).
    #[serde(default)]
    pub mode: Option<MetadataQueryMode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SearchResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub documents: Option<DocumentsSection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buckets: Option<BucketsSection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups: Option<GroupsSection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub users: Option<UsersSection>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct BucketSearchParams {
    #[serde(default)]
    pub q: String,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = ObjectSearchMode)]
pub enum ObjectMode {
    Local,
    #[default]
    DistributedBestEffort,
    DistributedStrict,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = ObjectSearchMatchMode)]
pub enum ObjectMatchMode {
    #[default]
    Substring,
    Prefix,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[schema(as = ObjectSearchParams)]
pub struct ObjectParams {
    #[serde(default)]
    pub q: String,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default, rename = "match")]
    pub match_mode: Option<ObjectMatchMode>,
    #[serde(default)]
    pub mode: Option<ObjectMode>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = ObjectSearchScope)]
pub enum ObjectScope {
    ThisNode,
    Realm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = ObjectSearchResultKind)]
pub enum ObjectResultKind {
    Object,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ObjectSearchChecksum {
    pub algorithm: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = ObjectSearchHit)]
pub struct ObjectHit {
    pub kind: ObjectResultKind,
    pub mode: ObjectMode,
    pub issuer_node_id: String,
    pub group_id: String,
    pub bucket: String,
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_w3id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<ObjectSearchChecksum>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = ObjectSearchIndexFreshness)]
pub struct ObjectIndexFreshness {
    pub source: String,
    pub as_of: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_observed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = ObjectSearchPartitionCoverage)]
pub struct ObjectPartitionCoverage {
    pub node_id: String,
    pub observed_at: String,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = ObjectSearchCoverage)]
pub struct ObjectCoverage {
    pub scope: ObjectScope,
    pub mode: ObjectMode,
    pub index_freshness: ObjectIndexFreshness,
    pub nodes_queried: usize,
    pub nodes_failed: usize,
    pub failed_partitions: Vec<String>,
    pub omitted_partitions: usize,
    pub complete: bool,
    pub truncated: bool,
    pub partitions: Vec<ObjectPartitionCoverage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = ObjectSearchResponse)]
pub struct ObjectResponse {
    pub hits: Vec<ObjectHit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub coverage: ObjectCoverage,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BucketsSection {
    pub hits: Vec<BucketHit>,
    pub nodes_queried: usize,
    pub nodes_failed: usize,
    pub failed_nodes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BucketHit {
    pub arn: String,
    pub bucket: String,
    pub node_id: String,
    pub group_id: String,
    pub group_name: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DocumentsSection {
    pub hits: Vec<SearchHitResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub nodes_queried: usize,
    pub nodes_failed: usize,
    /// True when pagination stopped at the server-side depth cap before the
    /// result set was exhausted.
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GroupsSection {
    pub hits: Vec<GroupHit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// True when the visibility scan stopped at the round cap with more raw
    /// matches pending before a visible page was filled.
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GroupHit {
    pub group_id: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UsersSection {
    pub hits: Vec<UserHit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserHit {
    pub user_id: String,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SearchTypes {
    documents: bool,
    buckets: bool,
    groups: bool,
    users: bool,
}

impl SearchTypes {
    fn all() -> Self {
        Self {
            documents: true,
            buckets: true,
            groups: true,
            users: true,
        }
    }

    fn count(&self) -> usize {
        self.documents as usize + self.buckets as usize + self.groups as usize + self.users as usize
    }
}

fn parse_search_types(types: Option<&str>) -> ServerResult<SearchTypes> {
    let Some(types) = types else {
        return Ok(SearchTypes::all());
    };
    let mut selected = SearchTypes {
        documents: false,
        buckets: false,
        groups: false,
        users: false,
    };
    let mut any = false;
    for value in types.split(',').map(str::trim) {
        if value.is_empty() {
            continue;
        }
        match value {
            SEARCH_TYPE_DOCUMENTS => selected.documents = true,
            SEARCH_TYPE_BUCKETS => selected.buckets = true,
            SEARCH_TYPE_GROUPS => selected.groups = true,
            SEARCH_TYPE_USERS => selected.users = true,
            _ => return Err(ServerError::BadRequest),
        }
        any = true;
    }
    if any {
        Ok(selected)
    } else {
        Ok(SearchTypes::all())
    }
}

#[utoipa::path(
    get,
    path = "/search/buckets",
    tag = "search",
    summary = "Search buckets across the realm",
    description = r#"Searches bucket names across the realm's serving nodes and merges the authorized matches.

**Authentication**: realm bearer token. Every node applies its own authorization and deny policies
to its own buckets, so a bucket the caller may not read never appears.

**Behavior**
- The search fans out over the realm's serving nodes under one shared deadline of about 12 seconds.
- There is no continuation token: answers are merged and cut to the page size, so a broader result
  set is reached by narrowing the query.
- A node that fails, times out or is dropped by the node cap does not fail the request. The answer
  is partial and reports it through `nodes_queried`, `nodes_failed` and `failed_nodes`, which names
  the nodes that did not answer and carries `partition-discovery` when node discovery itself
  failed.
- A partial answer is still 200 and means matching buckets may be missing rather than absent.

**Limits**
- The query is trimmed and must keep at least 2 characters.
- At most 32 nodes are queried per request."#,
    params(
        ("q" = String, Query, description = "Case-insensitive bucket-name substring; trimmed, minimum 2 characters, no wildcards"),
        ("limit" = Option<usize>, Query, description = "Maximum number of merged hits (default 10, clamped to 1..=50)")
    ),
    responses(
        (
            status = 200,
            description = "Merged authorized bucket matches, partial when `nodes_failed` is non-zero",
            body = BucketsSection,
            example = json!({
                "hits": [
                    {
                        "arn": "arn:aruna:cmVhbG0tZXhhbXBsZS0wMTIzNDU2Nzg5YWJjZGVmZ2g:1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978:s3/lab-raw",
                        "bucket": "lab-raw",
                        "node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                        "group_id": "01JABCDEF0123456789ABCDEFG",
                        "group_name": "Lab A",
                        "created_at": "2026-04-09T14:23:11.123+00:00"
                    }
                ],
                "nodes_queried": 3,
                "nodes_failed": 1,
                "failed_nodes": [
                    "2a3b4c5d6e7f89900a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f6789"
                ]
            })
        ),
        (status = 400, description = "Query shorter than 2 characters or otherwise malformed", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn bucket_search(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Query(params): Query<BucketSearchParams>,
) -> ServerResult<(StatusCode, Json<BucketsSection>)> {
    let auth = require_realm_auth(&state, auth)?;
    let query = params.q.trim();
    if query.chars().count() < MIN_QUERY_CHARS {
        return Err(ServerError::BadRequest);
    }
    let result = search_buckets_distributed(
        state.get_ctx().as_ref(),
        state.get_realm_id(),
        state.get_node_id(),
        BucketSearchRequest {
            auth,
            bearer_token: bearer_token.map(|carrier| carrier.as_str().to_string()),
            query: query.to_string(),
            limit: params
                .limit
                .unwrap_or(DEFAULT_SEARCH_LIMIT)
                .clamp(1, MAX_BUCKET_LIMIT),
            target_nodes: None,
        },
    )
    .await
    .map_err(map_api_error)?;
    Ok((StatusCode::OK, Json(map_bucket_section(result))))
}

#[utoipa::path(
    get,
    path = "/search/objects",
    tag = "search",
    summary = "Search current object heads",
    description = r#"Searches current live object heads by key, on this node or across the realm.

**Authentication**: realm bearer token. Every live head is checked against group READ, token path
restrictions and request policies before it can be returned.

**Behavior**
- Delete markers and historical versions are excluded, and no total is exposed.
- `local` searches this node, `distributed_best_effort` returns reachable node pages with explicit
  failed coverage, and `distributed_strict` answers 503 rather than a partial page.
- The cursor is bound to the query, bucket, match type and mode; it composes per-node keyset
  positions and an as-of watermark so newly created versions do not enter a cursor chain.
- `coverage` names the live-head source, observation times, failed partitions, completeness and
  truncation."#,
    params(
        ("q" = String, Query, description = "Case-sensitive key substring or prefix; trimmed, minimum 2 characters"),
        ("bucket" = Option<String>, Query, description = "Optional exact bucket name"),
        ("match" = Option<ObjectMatchMode>, Query, description = "Key match mode: substring (default) or prefix"),
        ("mode" = Option<ObjectMode>, Query, description = "Coverage mode: distributed_best_effort (default), distributed_strict, or local"),
        ("limit" = Option<usize>, Query, description = "Maximum merged hits (default 10, clamped to 1..=100)"),
        ("cursor" = Option<String>, Query, description = "Opaque continuation token from the same query, bucket, match type, and mode")
    ),
    responses(
        (
            status = 200,
            description = "Authorized live object heads with explicit coverage",
            body = ObjectResponse,
            example = json!({
                "hits": [
                    {
                        "kind": "object",
                        "mode": "distributed_best_effort",
                        "issuer_node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                        "group_id": "01JGROUP000000000000000000",
                        "bucket": "results",
                        "key": "run-42/output.csv",
                        "content_w3id": "https://w3id.org/aruna/data/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                        "checksum": {
                            "algorithm": "blake3",
                            "value": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        },
                        "size": 4096,
                        "updated_at": "2026-08-22T12:00:00Z"
                    }
                ],
                "next_cursor": null,
                "coverage": {
                    "scope": "realm",
                    "mode": "distributed_best_effort",
                    "index_freshness": {
                        "source": "live_heads",
                        "as_of": "2026-08-22T12:00:00Z",
                        "oldest_observed_at": "2026-08-22T11:59:00Z"
                    },
                    "nodes_queried": 3,
                    "nodes_failed": 0,
                    "failed_partitions": [],
                    "omitted_partitions": 0,
                    "complete": true,
                    "truncated": false,
                    "partitions": [
                        {
                            "node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                            "observed_at": "2026-08-22T12:00:00Z",
                            "truncated": false
                        }
                    ]
                }
            })
        ),
        (status = 400, description = "Query shorter than 2 characters, or a cursor that does not match this query, bucket, match type and mode", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm", body = ErrorResponse),
        (status = 503, description = "Strict distributed coverage could not be completed, or the live-head scan was unavailable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn object_search(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Query(params): Query<ObjectParams>,
) -> ServerResult<(StatusCode, Json<ObjectResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let query = params.q.trim();
    if query.chars().count() < MIN_QUERY_CHARS {
        return Err(ServerError::BadRequest);
    }
    let bucket = params
        .bucket
        .map(|bucket| bucket.trim().to_string())
        .filter(|bucket| !bucket.is_empty());
    let mode = params.mode.unwrap_or_default();
    let key_match = match params.match_mode.unwrap_or_default() {
        ObjectMatchMode::Substring => ObjectKeyMatch::Substring,
        ObjectMatchMode::Prefix => ObjectKeyMatch::Prefix,
    };
    let result = search_objects(
        state.get_ctx().as_ref(),
        state.get_realm_id(),
        state.get_node_id(),
        SearchQueryRequest {
            auth,
            bearer_token: bearer_token.map(|carrier| carrier.as_str().to_string()),
            query: query.to_string(),
            key_match,
            bucket,
            limit: params
                .limit
                .unwrap_or(DEFAULT_SEARCH_LIMIT)
                .clamp(1, MAX_SEARCH_LIMIT),
            cursor: params.cursor,
            mode: match mode {
                ObjectMode::Local => ObjectQueryMode::Local,
                ObjectMode::DistributedBestEffort => ObjectQueryMode::DistributedBestEffort,
                ObjectMode::DistributedStrict => ObjectQueryMode::DistributedStrict,
            },
            target_nodes: None,
        },
    )
    .await
    .map_err(map_api_error)?;
    Ok((StatusCode::OK, Json(map_search_response(result, mode))))
}

fn map_search_response(result: ObjectExecution, mode: ObjectMode) -> ObjectResponse {
    let oldest_observed_at = result
        .partitions
        .iter()
        .map(|partition| partition.observed_at)
        .min()
        .map(format_system_time);
    let mut failed_partitions = result
        .fanout_stats
        .failed_partitions
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if result.fanout_stats.discovery_failed {
        failed_partitions.push("partition-discovery".to_string());
    }
    if result.omitted_partitions > 0 {
        failed_partitions.push("fanout-cap".to_string());
    }
    let truncated = result.next_cursor.is_some();
    ObjectResponse {
        hits: result
            .hits
            .into_iter()
            .map(|hit| ObjectHit {
                kind: ObjectResultKind::Object,
                mode,
                issuer_node_id: hit.node_id.to_string(),
                group_id: hit.group_id.to_string(),
                bucket: hit.bucket,
                key: hit.key,
                content_w3id: hit.content_w3id,
                checksum: hit.checksum.map(|checksum| ObjectSearchChecksum {
                    algorithm: checksum.algorithm,
                    value: checksum.value,
                }),
                size: hit.size,
                updated_at: hit.updated_at.map(format_system_time),
            })
            .collect(),
        next_cursor: result.next_cursor,
        coverage: ObjectCoverage {
            scope: match mode {
                ObjectMode::Local => ObjectScope::ThisNode,
                ObjectMode::DistributedBestEffort | ObjectMode::DistributedStrict => {
                    ObjectScope::Realm
                }
            },
            mode,
            index_freshness: ObjectIndexFreshness {
                source: "live_heads".to_string(),
                as_of: format_system_time(result.as_of),
                oldest_observed_at,
            },
            nodes_queried: result.fanout_stats.nodes_queried,
            nodes_failed: result.fanout_stats.nodes_failed,
            failed_partitions,
            omitted_partitions: result.omitted_partitions,
            complete: result.complete,
            truncated,
            partitions: result
                .partitions
                .into_iter()
                .map(|partition| ObjectPartitionCoverage {
                    node_id: partition.node_id.to_string(),
                    observed_at: format_system_time(partition.observed_at),
                    truncated: partition.truncated,
                })
                .collect(),
        },
    }
}

fn format_system_time(time: std::time::SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(time).to_rfc3339()
}

#[utoipa::path(
    get,
    path = "/search",
    tag = "search",
    summary = "Search documents, buckets, groups and users",
    description = r#"Searches documents, buckets, groups and users concurrently and returns one sectioned answer.

**Authentication**: realm bearer token. Each section is authorized on its own terms: every node
filters its own document and bucket hits, a group hit is filtered against READ on the group's data
so a caller never learns of a group it may not read, and the user directory searches only public profile fields.

**Behavior**
- Documents and buckets fan out over at most 32 of the realm's serving nodes under one shared
  deadline of about 12 seconds; groups are matched locally.
- A path-restricted caller or a realm policy denial omits the `users` section.
  A section that was not requested is omitted.
- An answer can be partial and is still 200: for documents and buckets a non-zero `nodes_failed`
  means hits may be missing rather than absent.
- Documents set `truncated` when paging stopped at the server-side depth cap, groups when the
  per-hit visibility scan hit its round cap with matches still pending.
- Paging is per section and only for a single-type request: pass the section's `next_cursor` back
  as `cursor`, and a missing `next_cursor` means that section is exhausted. The buckets section has
  no continuation token.
- A document cursor is a signed token bound to the exact query and filters, a group cursor is the
  last returned group id as a ULID, and a user cursor is the last returned user id in its
  `ulid@realm` form."#,
    params(
        ("q" = String, Query, description = "Search query; trimmed, minimum 2 characters, matched as a substring for buckets, groups and users and as a full-text query for documents"),
        ("types" = Option<String>, Query, description = "Comma-separated subset of documents,buckets,groups,users. Defaults to all four; empty entries are ignored and an unknown type returns 400"),
        ("limit" = Option<usize>, Query, description = "Per-section page size (default 10, clamped to 1..=100, and additionally capped at 50 for the buckets section)"),
        ("cursor" = Option<String>, Query, description = "Opaque continuation token from the same section's next_cursor; only accepted when exactly one type is requested"),
        ("group_id" = Option<String>, Query, description = "Documents-only: restrict metadata hits to a single group id, given as a ULID; a malformed id returns 400"),
        ("conforms_to" = Option<String>, Query, description = "Documents-only: exact RO-Crate conformsTo specification or Profile IRI, such as the https://w3id.org/ro/crate/1.3 specification or an https://w3id.org/aruna/profile/{id} Profile"),
        ("mode" = Option<MetadataQueryMode>, Query, description = "Documents-only: local restricts the document search to this node, distributed fans out over the realm; defaults to distributed")
    ),
    responses(
        (
            status = 200,
            description = "Sectioned search results; a section is authoritative only when its failure counters are zero",
            body = SearchResponse,
            example = json!({
                "documents": {
                    "hits": [
                        {
                            "document_id": "01JMETADATA0123456789ABCDE",
                            "group_id": "01JABCDEF0123456789ABCDEFG",
                            "document_path": "datasets/rna-seq",
                            "graph_iri": "https://node.example.test/api/v1/metadata/01JMETADATA0123456789ABCDE",
                            "subject_iri": "https://node.example.test/api/v1/metadata/01JMETADATA0123456789ABCDE#root",
                            "score": 4.5,
                            "title": "RNA-seq reference run",
                            "snippet": "reference run of the RNA-seq pipeline",
                            "subject_types": ["http://schema.org/Dataset"]
                        }
                    ],
                    "next_cursor": "eyJ3IjoiMDFKTUVUQURBVEEwMTIzNDU2Nzg5QUJDREUifQ",
                    "nodes_queried": 3,
                    "nodes_failed": 0,
                    "truncated": false
                },
                "buckets": {
                    "hits": [
                        {
                            "arn": "arn:aruna:cmVhbG0tZXhhbXBsZS0wMTIzNDU2Nzg5YWJjZGVmZ2g:1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978:s3/lab-raw",
                            "bucket": "lab-raw",
                            "node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                            "group_id": "01JABCDEF0123456789ABCDEFG",
                            "group_name": "Lab A",
                            "created_at": "2026-04-09T14:23:11.123+00:00"
                        }
                    ],
                    "nodes_queried": 3,
                    "nodes_failed": 0,
                    "failed_nodes": []
                },
                "groups": {
                    "hits": [
                        {
                            "group_id": "01JABCDEF0123456789ABCDEFG",
                            "display_name": "Lab A"
                        }
                    ],
                    "truncated": false
                },
                "users": {
                    "hits": [
                        {
                            "user_id": "01JUSER0123456789ABCDEFGHI@cmVhbG0tZXhhbXBsZS0wMTIzNDU2Nzg5YWJjZGVmZ2g",
                            "name": "example-user"
                        }
                    ]
                }
            })
        ),
        (status = 400, description = "Query shorter than 2 characters, unknown type, malformed group id, or a cursor that is unsupported, sent with more than one type, or does not match the original query", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn unified_search(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Query(params): Query<SearchParams>,
) -> ServerResult<(StatusCode, Json<SearchResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let bearer = bearer_token.map(|carrier| carrier.as_str().to_string());
    Ok((
        StatusCode::OK,
        Json(run_unified(&state, &auth, bearer, params).await?),
    ))
}

pub(crate) async fn run_unified(
    state: &ServerState,
    auth: &AuthContext,
    bearer: Option<String>,
    params: SearchParams,
) -> ServerResult<SearchResponse> {
    let types = parse_search_types(params.types.as_deref())?;
    if let Some(cursor) = params.cursor.as_deref() {
        if types.count() != 1 {
            return Err(ServerError::BadRequest);
        }
        // Validate the selected section's cursor shape up front so a malformed
        // group or user id returns 400 rather than a downstream 500.
        if types.groups {
            parse_group_id(cursor)?;
        } else if types.users {
            UserId::from_string(cursor).map_err(|_| ServerError::BadRequest)?;
        } else if types.buckets {
            return Err(ServerError::BadRequest);
        }
    }
    let q = params.q.trim().to_string();
    if q.chars().count() < MIN_QUERY_CHARS {
        return Err(ServerError::BadRequest);
    }
    let limit = params
        .limit
        .unwrap_or(DEFAULT_SEARCH_LIMIT)
        .clamp(1, MAX_SEARCH_LIMIT);
    let group_id = params.group_id.as_deref().map(parse_group_id).transpose()?;
    let (documents, buckets, groups, users) = tokio::join!(
        run_documents(
            state,
            auth,
            types.documents,
            &q,
            bearer.clone(),
            params.conforms_to.clone(),
            group_id,
            limit,
            params.cursor.clone(),
            params.mode.clone(),
        ),
        run_buckets(state, auth, types.buckets, &q, bearer, limit),
        run_groups(state, auth, types.groups, &q, limit, params.cursor.clone()),
        run_users(state, auth, types.users, &q, limit, params.cursor.clone()),
    );

    Ok(SearchResponse {
        documents: documents?,
        buckets: buckets?,
        groups: groups?,
        users: users?,
    })
}

async fn run_buckets(
    state: &ServerState,
    auth: &AuthContext,
    requested: bool,
    query: &str,
    bearer_token: Option<String>,
    limit: usize,
) -> ServerResult<Option<BucketsSection>> {
    if !requested {
        return Ok(None);
    }
    let result = search_buckets_distributed(
        state.get_ctx().as_ref(),
        state.get_realm_id(),
        state.get_node_id(),
        BucketSearchRequest {
            auth: auth.clone(),
            bearer_token,
            query: query.to_string(),
            limit: limit.min(MAX_BUCKET_LIMIT),
            target_nodes: None,
        },
    )
    .await
    .map_err(map_api_error)?;
    Ok(Some(map_bucket_section(result)))
}

fn map_bucket_section(result: BucketSearchExecution) -> BucketsSection {
    let mut failed_nodes = result
        .fanout_stats
        .failed_partitions
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if result.fanout_stats.discovery_failed {
        failed_nodes.push("partition-discovery".to_string());
    }
    BucketsSection {
        hits: result
            .hits
            .into_iter()
            .map(|hit| BucketHit {
                arn: hit.arn,
                bucket: hit.bucket,
                node_id: hit.node_id.to_string(),
                group_id: hit.group_id.to_string(),
                group_name: hit.group_name,
                created_at: chrono::DateTime::<chrono::Utc>::from(hit.created_at).to_rfc3339(),
            })
            .collect(),
        nodes_queried: result.fanout_stats.nodes_queried,
        nodes_failed: result.fanout_stats.nodes_failed,
        failed_nodes,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_documents(
    state: &ServerState,
    auth: &AuthContext,
    requested: bool,
    query: &str,
    bearer_token: Option<String>,
    conforms_to: Option<String>,
    group_id: Option<Ulid>,
    limit: usize,
    cursor: Option<String>,
    mode: Option<MetadataQueryMode>,
) -> ServerResult<Option<DocumentsSection>> {
    if !requested {
        return Ok(None);
    }
    let ctx = state.get_ctx();
    let result = run_search_metadata(
        ctx.as_ref(),
        state.get_realm_id(),
        state.get_node_id(),
        MetadataSearchRequest {
            auth: Some(auth.clone()),
            bearer_token,
            graph_iris: None,
            query: query.to_string(),
            conforms_to,
            group_id,
            limit: Some(limit),
            cursor,
            mode: map_query_mode(mode),
            target_nodes: None,
        },
    )
    .await
    .map_err(map_api_error)?;
    Ok(Some(map_documents_section(result)))
}

fn map_documents_section(result: MetadataSearchExecution) -> DocumentsSection {
    DocumentsSection {
        hits: result.hits.into_iter().map(map_search_hit).collect(),
        next_cursor: result.next_cursor,
        nodes_queried: result.fanout_stats.nodes_queried,
        nodes_failed: result.fanout_stats.nodes_failed,
        truncated: result.truncated,
    }
}

async fn run_groups(
    state: &ServerState,
    auth: &AuthContext,
    requested: bool,
    query: &str,
    limit: usize,
    cursor: Option<String>,
) -> ServerResult<Option<GroupsSection>> {
    if !requested {
        return Ok(None);
    }
    let realm_id = state.get_realm_id();
    let mut hits: Vec<GroupHit> = Vec::new();
    let mut next_cursor: Option<String> = None;
    let mut scan_cursor = cursor;
    let mut truncated = false;
    // Fill the page across raw scans so a hidden first match cannot become an
    // empty page; the continuation cursor is only ever a visible group's id.
    'fill: for round in 0..MAX_GROUP_ROUNDS {
        let output = drive(
            SearchGroupsOperation::new(SearchGroupsInput {
                query: query.to_string(),
                limit,
                start_after: scan_cursor.clone(),
            }),
            &state.get_ctx(),
        )
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?;
        let raw_next = output.next_start_after;
        let total = output.groups.len();
        for (index, group) in output.groups.into_iter().enumerate() {
            // Per-result visibility: only disclose a group the caller can read,
            // mirroring document search (READ on the group's data root).
            let path = format!("/{realm_id}/g/{}/data/**", group.group_id);
            if crate::auth::ensure_permission(state, auth, path, Permission::READ)
                .await
                .is_ok()
            {
                let group_id = group.group_id.to_string();
                hits.push(GroupHit {
                    group_id: group_id.clone(),
                    display_name: group.display_name,
                });
                if hits.len() >= limit {
                    let more = index + 1 < total || raw_next.is_some();
                    next_cursor = more.then_some(group_id);
                    break 'fill;
                }
            }
        }
        match raw_next {
            Some(key) => scan_cursor = Some(key),
            None => break 'fill,
        }
        // The round cap stopped the scan while raw matches remain and the page
        // is not yet full: report truncation instead of a false completion.
        if round + 1 == MAX_GROUP_ROUNDS {
            truncated = true;
        }
    }
    Ok(Some(GroupsSection {
        hits,
        next_cursor,
        truncated,
    }))
}

/// Bounds the visibility fill loop so a realm of hidden matches cannot make one
/// request scan without limit; the operation already batches storage internally.
const MAX_GROUP_ROUNDS: usize = 64;

async fn run_users(
    state: &ServerState,
    auth: &AuthContext,
    requested: bool,
    query: &str,
    limit: usize,
    cursor: Option<String>,
) -> ServerResult<Option<UsersSection>> {
    if !requested {
        return Ok(None);
    }
    match crate::routes::access::users::authorize_directory(state, auth, None).await {
        Ok(()) => {}
        Err(ServerError::Forbidden) => return Ok(None),
        Err(error) => return Err(error),
    }
    let output = drive(
        SearchUsersOperation::new(SearchUsersInput {
            realm_id: state.get_realm_id(),
            query: query.to_string(),
            limit,
            start_after: cursor,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
    Ok(Some(UsersSection {
        hits: output
            .users
            .into_iter()
            .map(|user| UserHit {
                user_id: user.user_id.to_string(),
                name: user.name,
            })
            .collect(),
        next_cursor: output.next_start_after,
    }))
}

#[cfg(test)]
#[path = "search_tests.rs"]
mod tests;
