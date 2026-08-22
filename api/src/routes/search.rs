use crate::auth::{ValidatedArunaBearerTokenCarrier, parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::metadata::{
    MetadataQueryMode, MetadataSearchHitResponse, map_metadata_api_error, map_query_mode,
    map_search_hit,
};
use crate::routes::users::MIN_SEARCH_QUERY_CHARS;
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::structs::{AuthContext, Permission};
use aruna_operations::driver::drive;
use aruna_operations::metadata::api::{
    BucketSearchExecution, BucketSearchRequest, MetadataSearchExecution, MetadataSearchRequest,
    ObjectSearchExecution, ObjectSearchQueryMode, ObjectSearchRequest, search_buckets_distributed,
    search_metadata as run_search_metadata, search_objects,
};
use aruna_operations::s3::search_objects::ObjectKeyMatch;
use aruna_operations::search_groups::{SearchGroupsInput, SearchGroupsOperation};
use aruna_operations::search_users::{SearchUsersInput, SearchUsersOperation};
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

// IMPLEMENTED (2H pragmatic inventory): /search/objects searches authenticated
// live local heads and can federate them with explicit partial/strict coverage.
// DEFERRED (#260): durable signed inventory generations/snapshots are still not
// built; pagination is a query-bound live-head keyset with an as-of watermark.
// DEFERRED (#266 directories): the /search groups/users and /search/buckets core
// landed (#427); visibility tiers, public profiles, and the signed bucket
// directory are the deferred enhancements.
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
pub enum ObjectSearchMode {
    Local,
    #[default]
    DistributedBestEffort,
    DistributedStrict,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ObjectSearchMatchMode {
    #[default]
    Substring,
    Prefix,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ObjectSearchParams {
    #[serde(default)]
    pub q: String,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default, rename = "match")]
    pub match_mode: Option<ObjectSearchMatchMode>,
    #[serde(default)]
    pub mode: Option<ObjectSearchMode>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ObjectSearchScope {
    ThisNode,
    Realm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ObjectSearchResultKind {
    Object,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ObjectSearchChecksum {
    pub algorithm: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ObjectSearchHit {
    pub kind: ObjectSearchResultKind,
    pub mode: ObjectSearchMode,
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
pub struct ObjectSearchIndexFreshness {
    pub source: String,
    pub as_of: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_observed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ObjectSearchPartitionCoverage {
    pub node_id: String,
    pub observed_at: String,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ObjectSearchCoverage {
    pub scope: ObjectSearchScope,
    pub mode: ObjectSearchMode,
    pub index_freshness: ObjectSearchIndexFreshness,
    pub nodes_queried: usize,
    pub nodes_failed: usize,
    pub failed_partitions: Vec<String>,
    pub omitted_partitions: usize,
    pub complete: bool,
    pub truncated: bool,
    pub partitions: Vec<ObjectSearchPartitionCoverage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ObjectSearchResponse {
    pub hits: Vec<ObjectSearchHit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub coverage: ObjectSearchCoverage,
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
    pub hits: Vec<MetadataSearchHitResponse>,
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
    description = "Requires a bearer token issued by this realm; a token from another realm is refused with 403. The query is trimmed and must keep at least 2 characters, otherwise the request is rejected with 400. The search fans out over the realm's serving nodes, at most 32 nodes per request and under one shared deadline of about 12 seconds, and every node applies its own authorization and deny policies to its own buckets, so a bucket the caller may not read never appears. Answers are merged and cut to the page size; there is no continuation token, so a broader result set is reached by narrowing the query rather than by paging. A node that fails, is omitted by the node cap or times out does not fail the request: the answer is partial and says so through nodes_queried, nodes_failed and failed_nodes, which names the nodes that did not answer and carries the entry partition-discovery when node discovery itself failed. A partial answer means matching buckets may be missing rather than absent, and the caller may repeat the request.",
    params(
        ("q" = String, Query, description = "Case-insensitive bucket-name substring; trimmed, minimum 2 characters, no wildcards"),
        ("limit" = Option<usize>, Query, description = "Maximum number of merged hits (default 10, clamped to 1..=50)")
    ),
    responses(
        (
            status = 200,
            description = "Authorized federated bucket matches, possibly partial when nodes_failed is non-zero",
            body = BucketsSection,
            example = json!({
                "hits": [{
                    "arn": "arn:aruna:cmVhbG0tZXhhbXBsZS0wMTIzNDU2Nzg5YWJjZGVmZ2g:1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978:s3/lab-raw",
                    "bucket": "lab-raw",
                    "node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                    "group_id": "01JABCDEF0123456789ABCDEFG",
                    "group_name": "Lab A",
                    "created_at": "2026-04-09T14:23:11.123+00:00"
                }],
                "nodes_queried": 3,
                "nodes_failed": 1,
                "failed_nodes": ["2a3b4c5d6e7f89900a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f6789"]
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
    Extension(bearer_token): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Query(params): Query<BucketSearchParams>,
) -> ServerResult<(StatusCode, Json<BucketsSection>)> {
    let auth = require_realm_auth(&state, auth)?;
    let query = params.q.trim();
    if query.chars().count() < MIN_SEARCH_QUERY_CHARS {
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
    .map_err(map_metadata_api_error)?;
    Ok((StatusCode::OK, Json(map_bucket_section(result))))
}

#[utoipa::path(
    get,
    path = "/search/objects",
    tag = "search",
    summary = "Search current object heads locally or across the realm",
    description = "Requires a bearer token issued by this realm. Keys are matched case-sensitively by substring or prefix, optionally inside one exact bucket. Every current live head is checked against group READ, token path restrictions, and request policies before it can be returned; delete markers and historical versions are excluded, and no total is exposed. local searches this node, distributed_best_effort returns reachable node pages with explicit failed coverage, and distributed_strict returns 503 rather than a partial page. The opaque cursor is bound to the query, bucket, match type, and mode; it composes per-node keyset positions and an as-of watermark so newly created versions do not enter a cursor chain. Coverage names the live-head source, observation times, failed partitions, completeness, and truncation.",
    params(
        ("q" = String, Query, description = "Case-sensitive key substring or prefix; trimmed, minimum 2 characters"),
        ("bucket" = Option<String>, Query, description = "Optional exact bucket name"),
        ("match" = Option<ObjectSearchMatchMode>, Query, description = "Key match mode: substring (default) or prefix"),
        ("mode" = Option<ObjectSearchMode>, Query, description = "Coverage mode: distributed_best_effort (default), distributed_strict, or local"),
        ("limit" = Option<usize>, Query, description = "Maximum merged hits (default 10, clamped to 1..=100)"),
        ("cursor" = Option<String>, Query, description = "Opaque continuation token from the same query, bucket, match type, and mode")
    ),
    responses(
        (status = 200, description = "Authorized live object heads with explicit coverage", body = ObjectSearchResponse,
            example = json!({"hits": [{"kind": "object", "mode": "distributed_best_effort", "issuer_node_id": "node-a", "group_id": "01JGROUP000000000000000000", "bucket": "results", "key": "run-42/output.csv", "content_w3id": "https://w3id.org/aruna/data/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "checksum": {"algorithm": "blake3", "value": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}, "size": 4096, "updated_at": "2026-08-22T12:00:00Z"}], "next_cursor": null, "coverage": {"scope": "realm", "mode": "distributed_best_effort", "index_freshness": {"source": "inventory", "as_of": "2026-08-22T12:00:00Z", "oldest_observed_at": "2026-08-22T11:59:00Z"}, "nodes_queried": 3, "nodes_failed": 0, "failed_partitions": [], "omitted_partitions": 0, "complete": true, "truncated": false, "partitions": [{"node_id": "node-a", "observed_at": "2026-08-22T12:00:00Z", "truncated": false}]}})),
        (status = 400, description = "Malformed query or cursor", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm", body = ErrorResponse),
        (status = 503, description = "Strict distributed coverage could not be completed, or the local live-head scan was unavailable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn object_search(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Query(params): Query<ObjectSearchParams>,
) -> ServerResult<(StatusCode, Json<ObjectSearchResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let query = params.q.trim();
    if query.chars().count() < MIN_SEARCH_QUERY_CHARS {
        return Err(ServerError::BadRequest);
    }
    let bucket = params
        .bucket
        .map(|bucket| bucket.trim().to_string())
        .filter(|bucket| !bucket.is_empty());
    let mode = params.mode.unwrap_or_default();
    let key_match = match params.match_mode.unwrap_or_default() {
        ObjectSearchMatchMode::Substring => ObjectKeyMatch::Substring,
        ObjectSearchMatchMode::Prefix => ObjectKeyMatch::Prefix,
    };
    let result = search_objects(
        state.get_ctx().as_ref(),
        state.get_realm_id(),
        state.get_node_id(),
        ObjectSearchRequest {
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
                ObjectSearchMode::Local => ObjectSearchQueryMode::Local,
                ObjectSearchMode::DistributedBestEffort => {
                    ObjectSearchQueryMode::DistributedBestEffort
                }
                ObjectSearchMode::DistributedStrict => ObjectSearchQueryMode::DistributedStrict,
            },
            target_nodes: None,
        },
    )
    .await
    .map_err(map_metadata_api_error)?;
    Ok((
        StatusCode::OK,
        Json(map_object_search_response(result, mode)),
    ))
}

fn map_object_search_response(
    result: ObjectSearchExecution,
    mode: ObjectSearchMode,
) -> ObjectSearchResponse {
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
    ObjectSearchResponse {
        hits: result
            .hits
            .into_iter()
            .map(|hit| ObjectSearchHit {
                kind: ObjectSearchResultKind::Object,
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
        coverage: ObjectSearchCoverage {
            scope: match mode {
                ObjectSearchMode::Local => ObjectSearchScope::ThisNode,
                ObjectSearchMode::DistributedBestEffort | ObjectSearchMode::DistributedStrict => {
                    ObjectSearchScope::Realm
                }
            },
            mode,
            index_freshness: ObjectSearchIndexFreshness {
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
                .map(|partition| ObjectSearchPartitionCoverage {
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
    summary = "Search documents, buckets, groups and users in one request",
    description = "Requires a bearer token issued by this realm; a token from another realm is refused with 403. The four sections are searched concurrently and each one is authorized on its own terms: documents and buckets fan out over the realm's serving nodes, at most 32 nodes under one shared deadline of about 12 seconds, and each node filters its own results; groups are matched locally and then filtered per hit against READ on the group's data, so a caller who may not read a group never learns it exists; the user directory is an admin-scoped read, and a caller without it simply gets no users section instead of an error. A section that was not requested is omitted from the response. Answers can be partial: for documents and buckets, nodes_queried and nodes_failed count the fan-out and a non-zero nodes_failed (a node that failed, timed out or was dropped by the node cap) means hits may be missing rather than absent; documents also set truncated when paging stopped at the server-side depth cap, and groups set truncated when the per-hit visibility scan hit its round cap with matches still pending. A partial answer is still 200 and the caller may repeat the request. Paging is per section and only for a single-type request: pass the section's next_cursor back as cursor. A missing next_cursor means that section is exhausted; a cursor sent with more than one type, or with types=buckets, which has no continuation token, is rejected with 400.",
    params(
        ("q" = String, Query, description = "Search query; trimmed, minimum 2 characters, matched as a substring for buckets, groups and users and as a full-text query for documents"),
        ("types" = Option<String>, Query, description = "Comma-separated subset of documents,buckets,groups,users. Defaults to all four; empty entries are ignored and an unknown type returns 400"),
        ("limit" = Option<usize>, Query, description = "Per-section page size (default 10, clamped to 1..=100, and additionally capped at 50 for the buckets section)"),
        ("cursor" = Option<String>, Query, description = "Opaque continuation token from the same section's next_cursor. Only accepted when exactly one type is requested; for documents it is a signed token bound to the exact query and filters, for groups the last returned group id as a ULID and for users the last returned user id in its ulid@realm form, and a malformed or unsupported cursor returns 400"),
        ("group_id" = Option<String>, Query, description = "Documents-only: restrict metadata hits to a single group id, given as a ULID; a malformed id returns 400"),
        ("conforms_to" = Option<String>, Query, description = "Documents-only: exact RO-Crate conformsTo specification or Profile IRI, such as the https://w3id.org/ro/crate/1.3 specification or an https://w3id.org/aruna/profile/{id} Profile"),
        ("mode" = Option<MetadataQueryMode>, Query, description = "Documents-only: local restricts the document search to this node, distributed fans out over the realm; defaults to distributed")
    ),
    responses(
        (
            status = 200,
            description = "Sectioned search results; a section is omitted when it was not requested and the users section is also omitted when the caller may not read the user directory, and a section is authoritative only when its failure counters are zero",
            body = SearchResponse,
            example = json!({
                "documents": {
                    "hits": [{
                        "document_id": "01JMETADATA0123456789ABCDE",
                        "group_id": "01JABCDEF0123456789ABCDEFG",
                        "document_path": "datasets/rna-seq",
                        "graph_iri": "https://node.example.test/api/v1/metadata/01JMETADATA0123456789ABCDE",
                        "subject_iri": "https://node.example.test/api/v1/metadata/01JMETADATA0123456789ABCDE#root",
                        "score": 4.5,
                        "title": "RNA-seq reference run",
                        "snippet": "reference run of the RNA-seq pipeline"
                    }],
                    "next_cursor": "eyJ3IjoiMDFKTUVUQURBVEEwMTIzNDU2Nzg5QUJDREUifQ",
                    "nodes_queried": 3,
                    "nodes_failed": 0,
                    "truncated": false
                },
                "buckets": {
                    "hits": [{
                        "arn": "arn:aruna:cmVhbG0tZXhhbXBsZS0wMTIzNDU2Nzg5YWJjZGVmZ2g:1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978:s3/lab-raw",
                        "bucket": "lab-raw",
                        "node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                        "group_id": "01JABCDEF0123456789ABCDEFG",
                        "group_name": "Lab A",
                        "created_at": "2026-04-09T14:23:11.123+00:00"
                    }],
                    "nodes_queried": 3,
                    "nodes_failed": 0,
                    "failed_nodes": []
                },
                "groups": {
                    "hits": [{"group_id": "01JABCDEF0123456789ABCDEFG", "display_name": "Lab A"}],
                    "truncated": false
                },
                "users": {
                    "hits": [{
                        "user_id": "01JUSER0123456789ABCDEFGHI@cmVhbG0tZXhhbXBsZS0wMTIzNDU2Nzg5YWJjZGVmZ2g",
                        "name": "example-user"
                    }]
                }
            })
        ),
        (status = 400, description = "Query shorter than 2 characters, unknown type, malformed group id, or a cursor that is multi-type, unsupported or does not match the original query", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn unified_search(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Query(params): Query<SearchParams>,
) -> ServerResult<(StatusCode, Json<SearchResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
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
    if q.chars().count() < MIN_SEARCH_QUERY_CHARS {
        return Err(ServerError::BadRequest);
    }
    let limit = params
        .limit
        .unwrap_or(DEFAULT_SEARCH_LIMIT)
        .clamp(1, MAX_SEARCH_LIMIT);
    let group_id = params.group_id.as_deref().map(parse_group_id).transpose()?;
    let bearer = bearer_token.map(|carrier| carrier.as_str().to_string());

    let (documents, buckets, groups, users) = tokio::join!(
        run_documents(
            &state,
            &auth,
            types.documents,
            &q,
            bearer.clone(),
            params.conforms_to.clone(),
            group_id,
            limit,
            params.cursor.clone(),
            params.mode.clone(),
        ),
        run_buckets(&state, &auth, types.buckets, &q, bearer, limit),
        run_groups(
            &state,
            &auth,
            types.groups,
            &q,
            limit,
            params.cursor.clone()
        ),
        run_users(&state, &auth, types.users, &q, limit, params.cursor.clone()),
    );

    Ok((
        StatusCode::OK,
        Json(SearchResponse {
            documents: documents?,
            buckets: buckets?,
            groups: groups?,
            users: users?,
        }),
    ))
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
    .map_err(map_metadata_api_error)?;
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
    .map_err(map_metadata_api_error)?;
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
    'fill: for round in 0..MAX_GROUP_SCAN_ROUNDS {
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
        if round + 1 == MAX_GROUP_SCAN_ROUNDS {
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
const MAX_GROUP_SCAN_ROUNDS: usize = 64;

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
    // The user directory is an admin-scoped read, so a caller without it gets no
    // user section instead of a failed search.
    let path = format!("/{}/admin/u/**", state.get_realm_id());
    if !crate::auth::permission_granted(state, auth, path, Permission::READ).await? {
        return Ok(None);
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
mod tests {
    use super::*;
    use crate::error::ServerError;
    use crate::routes::metadata::{
        CreateMetadataRequest, CreateMetadataScaffoldRequest, MetadataQueryMode,
        create_metadata_document,
    };
    use aruna_core::UserId;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE, USER_KEYSPACE,
    };
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use aruna_core::structs::{
        Actor, BucketInfo, Group, GroupAuthorizationDocument, NodeCapabilities,
        RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNodeKind, User,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_operations::metadata::MetadataHandle;
    use aruna_operations::metadata::materialization_queue::process_metadata_materialization_batch;
    use aruna_operations::metadata::projector::{
        drain_pending_metadata_projection_queue, replay_metadata_event_log,
    };
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use byteview::ByteView;
    use ed25519_dalek::SigningKey;
    use std::time::SystemTime;
    use tempfile::TempDir;
    use ulid::Ulid;

    struct Fixture {
        _storage_dir: TempDir,
        _metadata_dir: TempDir,
        state: Arc<ServerState>,
        auth: AuthContext,
        actor: Actor,
        realm_id: RealmId,
        groups: [Ulid; 2],
        users: [UserId; 2],
    }

    fn realm_id(seed: u8) -> RealmId {
        RealmId::from_bytes(
            SigningKey::from_bytes(&[seed; 32])
                .verifying_key()
                .to_bytes(),
        )
    }

    async fn write_bytes(state: &ServerState, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        let event = state
            .get_ctx()
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: ByteView::from(key),
                value: ByteView::from(value),
                txn_id: None,
            }))
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn seed_group(
        state: &ServerState,
        actor: &Actor,
        group_id: Ulid,
        name: &str,
        policies: Vec<RequestPolicy>,
    ) {
        let realm = actor.realm_id;
        let mut auth_doc =
            GroupAuthorizationDocument::new_default_group_doc(actor.user_id, realm, group_id);
        auth_doc.policies = policies;
        let group = Group {
            display_name: name.to_string(),
            group_id,
            realm_id: realm,
            roles: auth_doc.roles.keys().copied().collect(),
            owner: actor.user_id,
        };
        write_bytes(
            state,
            GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group.to_bytes(actor).unwrap(),
        )
        .await;
        write_bytes(
            state,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            auth_doc.to_bytes(actor).unwrap(),
        )
        .await;
    }

    async fn seed_bucket(state: &ServerState, actor: &Actor, bucket: &str, group_id: Ulid) {
        write_bytes(
            state,
            S3_BUCKET_KEYSPACE,
            bucket.as_bytes().to_vec(),
            BucketInfo {
                group_id,
                created_at: SystemTime::UNIX_EPOCH,
                created_by: actor.user_id,
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
            }
            .to_bytes()
            .unwrap(),
        )
        .await;
    }

    /// Rewrites the realm config with the given realm-scoped request policies,
    /// keeping the placement and node entries the fan-out needs.
    async fn seed_policies(state: &ServerState, actor: &Actor, policies: Vec<RequestPolicy>) {
        let realm = actor.realm_id;
        let mut config = RealmConfigDocument::default_for_realm(realm, Vec::new());
        config.seed_default_placement();
        config.ensure_node(actor.node_id, RealmNodeKind::Server);
        config.request_policies = policies;
        write_bytes(
            state,
            REALM_CONFIG_KEYSPACE,
            realm.as_bytes().to_vec(),
            config.to_bytes(actor).unwrap(),
        )
        .await;
    }

    fn deny_policy(expression: &str) -> RequestPolicy {
        RequestPolicy {
            policy_id: Ulid::generate(),
            name: "hide-bucket".to_string(),
            kind: PolicyKind::Deny,
            when: None,
            expression: expression.to_string(),
            enabled: true,
        }
    }

    async fn seed_user(state: &ServerState, actor: &Actor, user_id: UserId, name: &str) {
        let user = User {
            user_id,
            name: name.to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: Default::default(),
        };
        write_bytes(
            state,
            USER_KEYSPACE,
            user_id.to_storage_key(),
            user.to_bytes(actor).unwrap(),
        )
        .await;
    }

    async fn setup() -> Fixture {
        let storage_dir = tempfile::tempdir().unwrap();
        let metadata_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let node_id = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
        let realm = realm_id(5);
        let user_id = UserId::local(Ulid::from_bytes([200u8; 16]), realm);
        let actor = Actor {
            node_id,
            user_id,
            realm_id: realm,
        };
        let metadata_handle = MetadataHandle::new(
            metadata_dir.path(),
            node_id,
            storage_handle.clone(),
            None,
            None,
            None,
        )
        .unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: Some(metadata_handle),
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        });
        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm,
                node_id,
                NodeCapabilities::local_node(realm).unwrap(),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );

        // The fixture user holds every realm role so the user directory section
        // of a unified search is authorized.
        let mut realm_doc = RealmAuthorizationDocument::new_default_realm_doc(realm);
        for role in realm_doc.roles.values_mut() {
            role.assigned_users.insert(user_id);
        }
        write_bytes(
            &state,
            AUTH_KEYSPACE,
            realm.as_bytes().to_vec(),
            realm_doc.to_bytes(&actor).unwrap(),
        )
        .await;

        seed_policies(&state, &actor, Vec::new()).await;

        let groups = [Ulid::from_bytes([1u8; 16]), Ulid::from_bytes([2u8; 16])];
        seed_group(&state, &actor, groups[0], "alpha-team", Vec::new()).await;
        seed_group(&state, &actor, groups[1], "alpha-squad", Vec::new()).await;
        seed_bucket(&state, &actor, "alpha-bucket", groups[0]).await;

        let users = [
            UserId::local(Ulid::from_bytes([1u8; 16]), realm),
            UserId::local(Ulid::from_bytes([2u8; 16]), realm),
        ];
        seed_user(&state, &actor, users[0], "beta-anna").await;
        seed_user(&state, &actor, users[1], "beta-bob").await;

        Fixture {
            _storage_dir: storage_dir,
            _metadata_dir: metadata_dir,
            state,
            auth: AuthContext {
                user_id,
                realm_id: realm,
                path_restrictions: None,
            },
            actor,
            realm_id: realm,
            groups,
            users,
        }
    }

    async fn create_doc(fx: &Fixture, group_id: Ulid, path: &str, name: &str) {
        let _ = create_metadata_document(
            State(fx.state.clone()),
            Extension(Some(fx.auth.clone())),
            Extension(None),
            Json(CreateMetadataRequest::Scaffold(
                CreateMetadataScaffoldRequest {
                    group_id: group_id.to_string(),
                    path: path.to_string(),
                    name: name.to_string(),
                    description: "desc".to_string(),
                    date_published: "2026-01-01".to_string(),
                    license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public: true,
                },
            )),
        )
        .await
        .unwrap();
    }

    async fn drain_projection(state: &ServerState) {
        let ctx = state.get_ctx();
        let drained = drain_pending_metadata_projection_queue(ctx.as_ref())
            .await
            .unwrap();
        if drained.markers_examined == 0 {
            replay_metadata_event_log(ctx.as_ref()).await.unwrap();
        }
        process_metadata_materialization_batch(ctx.as_ref())
            .await
            .unwrap();
    }

    async fn flush_search(state: &ServerState) {
        let ctx = state.get_ctx();
        ctx.metadata_handle
            .as_ref()
            .unwrap()
            .flush_search_updates()
            .await
            .unwrap();
    }

    fn params(q: &str) -> SearchParams {
        SearchParams {
            q: q.to_string(),
            ..Default::default()
        }
    }

    async fn search(fx: &Fixture, params: SearchParams) -> ServerResult<SearchResponse> {
        unified_search(
            State(fx.state.clone()),
            Extension(Some(fx.auth.clone())),
            Extension(None),
            Query(params),
        )
        .await
        .map(|(_, Json(body))| body)
    }

    #[tokio::test]
    async fn selects_types() {
        let fx = setup().await;
        let resp = search(
            &fx,
            SearchParams {
                types: Some("groups,users".to_string()),
                ..params("alpha")
            },
        )
        .await
        .unwrap();
        assert!(resp.documents.is_none());
        assert!(resp.buckets.is_none());
        assert!(resp.groups.is_some());
        assert!(resp.users.is_some());
    }

    async fn bucket_hits(fx: &Fixture, query: &str, limit: usize) -> Vec<String> {
        let (_, Json(section)) = bucket_search(
            State(fx.state.clone()),
            Extension(Some(fx.auth.clone())),
            Extension(None),
            Query(BucketSearchParams {
                q: query.to_string(),
                limit: Some(limit),
            }),
        )
        .await
        .unwrap();
        section.hits.into_iter().map(|hit| hit.bucket).collect()
    }

    #[tokio::test]
    async fn policy_hides_bucket() {
        // A realm deny policy must hide the bucket from the dedicated route and
        // from the buckets section of the unified search alike.
        let fx = setup().await;
        seed_bucket(&fx.state, &fx.actor, "beta-bucket", fx.groups[1]).await;
        seed_policies(
            &fx.state,
            &fx.actor,
            vec![deny_policy("path.endsWith('/alpha-bucket')")],
        )
        .await;

        assert_eq!(bucket_hits(&fx, "bucket", 10).await, ["beta-bucket"]);

        let unified = search(
            &fx,
            SearchParams {
                types: Some("buckets".to_string()),
                ..params("bucket")
            },
        )
        .await
        .unwrap();
        let hits = unified.buckets.unwrap().hits;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].bucket, "beta-bucket");
    }

    #[tokio::test]
    async fn group_hides_bucket() {
        // A group-scoped deny policy hides that group's bucket only.
        let fx = setup().await;
        seed_bucket(&fx.state, &fx.actor, "beta-bucket", fx.groups[1]).await;
        seed_group(
            &fx.state,
            &fx.actor,
            fx.groups[0],
            "alpha-team",
            vec![deny_policy("path.endsWith('/alpha-bucket')")],
        )
        .await;

        assert_eq!(bucket_hits(&fx, "bucket", 10).await, ["beta-bucket"]);
    }

    #[tokio::test]
    async fn policy_fills_page() {
        // A hidden first match must not shorten a page that later matches can
        // still fill.
        let fx = setup().await;
        seed_bucket(&fx.state, &fx.actor, "beta-bucket", fx.groups[1]).await;
        seed_bucket(&fx.state, &fx.actor, "gamma-bucket", fx.groups[1]).await;
        seed_policies(
            &fx.state,
            &fx.actor,
            vec![deny_policy("path.endsWith('/alpha-bucket')")],
        )
        .await;

        assert_eq!(
            bucket_hits(&fx, "bucket", 2).await,
            ["beta-bucket", "gamma-bucket"]
        );
    }

    #[tokio::test]
    async fn searches_buckets() {
        let fx = setup().await;
        let (_, Json(dedicated)) = bucket_search(
            State(fx.state.clone()),
            Extension(Some(fx.auth.clone())),
            Extension(None),
            Query(BucketSearchParams {
                q: "bucket".to_string(),
                limit: Some(10),
            }),
        )
        .await
        .unwrap();
        assert_eq!(dedicated.hits.len(), 1);
        assert_eq!(dedicated.hits[0].bucket, "alpha-bucket");
        assert!(dedicated.hits[0].arn.ends_with(":s3/alpha-bucket"));

        let unified = search(
            &fx,
            SearchParams {
                types: Some("buckets".to_string()),
                ..params("bucket")
            },
        )
        .await
        .unwrap();
        assert!(unified.documents.is_none());
        assert_eq!(unified.buckets.unwrap().hits.len(), 1);
        assert!(unified.groups.is_none());
        assert!(unified.users.is_none());
    }

    #[tokio::test]
    async fn filters_group_hits() {
        // A same-realm caller who is not a group member sees no group hits,
        // while a member still sees both matching groups.
        let fx = setup().await;
        let stranger = AuthContext {
            user_id: UserId::local(Ulid::from_bytes([77u8; 16]), fx.realm_id),
            realm_id: fx.realm_id,
            path_restrictions: None,
        };
        let (_, Json(resp)) = unified_search(
            State(fx.state.clone()),
            Extension(Some(stranger)),
            Extension(None),
            Query(SearchParams {
                types: Some("groups".to_string()),
                ..params("alpha")
            }),
        )
        .await
        .unwrap();
        assert!(resp.groups.unwrap().hits.is_empty());

        let member = search(
            &fx,
            SearchParams {
                types: Some("groups".to_string()),
                ..params("alpha")
            },
        )
        .await
        .unwrap();
        assert_eq!(member.groups.unwrap().hits.len(), 2);
    }

    #[tokio::test]
    async fn rejects_unknown_type() {
        let fx = setup().await;
        let result = search(
            &fx,
            SearchParams {
                types: Some("groups,bogus".to_string()),
                ..params("alpha")
            },
        )
        .await;
        assert!(matches!(result, Err(ServerError::BadRequest)));
    }

    #[tokio::test]
    async fn rejects_short_query() {
        let fx = setup().await;
        let result = search(&fx, params("a")).await;
        assert!(matches!(result, Err(ServerError::BadRequest)));
    }

    #[tokio::test]
    async fn rejects_cursor_multi() {
        // A cursor is only valid when exactly one type is requested.
        let fx = setup().await;
        let result = search(
            &fx,
            SearchParams {
                cursor: Some("token".to_string()),
                ..params("alpha")
            },
        )
        .await;
        assert!(matches!(result, Err(ServerError::BadRequest)));
    }

    #[tokio::test]
    async fn rejects_malformed_cursor() {
        // A garbage single-type cursor is caller input: 400, never a 500.
        let fx = setup().await;
        for section in ["groups", "users"] {
            let result = search(
                &fx,
                SearchParams {
                    types: Some(section.to_string()),
                    cursor: Some("garbage".to_string()),
                    ..params("alpha")
                },
            )
            .await;
            assert!(
                matches!(result, Err(ServerError::BadRequest)),
                "{section} cursor should be rejected"
            );
        }
        let result = search(
            &fx,
            SearchParams {
                types: Some("buckets".to_string()),
                cursor: Some("unsupported".to_string()),
                ..params("alpha")
            },
        )
        .await;
        assert!(matches!(result, Err(ServerError::BadRequest)));
    }

    #[tokio::test]
    async fn pages_groups() {
        let fx = setup().await;
        let first = search(
            &fx,
            SearchParams {
                types: Some("groups".to_string()),
                limit: Some(1),
                ..params("alpha")
            },
        )
        .await
        .unwrap()
        .groups
        .unwrap();
        assert_eq!(first.hits.len(), 1);
        assert_eq!(first.hits[0].group_id, fx.groups[0].to_string());
        let cursor = first.next_cursor.clone().unwrap();

        let second = search(
            &fx,
            SearchParams {
                types: Some("groups".to_string()),
                limit: Some(1),
                cursor: Some(cursor),
                ..params("alpha")
            },
        )
        .await
        .unwrap()
        .groups
        .unwrap();
        assert_eq!(second.hits.len(), 1);
        assert_eq!(second.hits[0].group_id, fx.groups[1].to_string());
        assert!(second.next_cursor.is_none());
    }

    #[tokio::test]
    async fn skips_hidden_group() {
        // A hidden first match at limit 1 must not yield an empty page whose
        // cursor exposes the hidden group's id.
        let fx = setup().await;
        let node_id = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
        let hidden_owner = UserId::local(Ulid::from_bytes([240u8; 16]), fx.realm_id);
        let viewer = UserId::local(Ulid::from_bytes([241u8; 16]), fx.realm_id);
        let hidden_actor = Actor {
            node_id,
            user_id: hidden_owner,
            realm_id: fx.realm_id,
        };
        let viewer_actor = Actor {
            node_id,
            user_id: viewer,
            realm_id: fx.realm_id,
        };
        seed_group(
            &fx.state,
            &hidden_actor,
            fx.groups[0],
            "alpha-hidden",
            Vec::new(),
        )
        .await;
        seed_group(
            &fx.state,
            &viewer_actor,
            fx.groups[1],
            "alpha-visible",
            Vec::new(),
        )
        .await;

        let viewer_auth = AuthContext {
            user_id: viewer,
            realm_id: fx.realm_id,
            path_restrictions: None,
        };
        let section = run_groups(&fx.state, &viewer_auth, true, "alpha", 1, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(section.hits.len(), 1);
        assert_eq!(section.hits[0].group_id, fx.groups[1].to_string());
        assert_ne!(
            section.next_cursor.as_deref(),
            Some(fx.groups[0].to_string().as_str())
        );
    }

    #[tokio::test]
    async fn caps_report_truncation() {
        // More hidden matches than the scan cap must report truncation instead of
        // a false completion with an empty page and no cursor.
        let fx = setup().await;
        let node_id = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
        let hidden_owner = UserId::local(Ulid::from_bytes([230u8; 16]), fx.realm_id);
        let viewer = UserId::local(Ulid::from_bytes([231u8; 16]), fx.realm_id);
        let hidden_actor = Actor {
            node_id,
            user_id: hidden_owner,
            realm_id: fx.realm_id,
        };
        for index in 0..=MAX_GROUP_SCAN_ROUNDS as u8 {
            let mut bytes = [16u8; 16];
            bytes[15] = index;
            seed_group(
                &fx.state,
                &hidden_actor,
                Ulid::from_bytes(bytes),
                &format!("alpha-hidden-{index}"),
                Vec::new(),
            )
            .await;
        }

        let viewer_auth = AuthContext {
            user_id: viewer,
            realm_id: fx.realm_id,
            path_restrictions: None,
        };
        let section = run_groups(&fx.state, &viewer_auth, true, "alpha", 1, None)
            .await
            .unwrap()
            .unwrap();
        assert!(section.hits.is_empty());
        assert!(section.next_cursor.is_none());
        assert!(section.truncated);
    }

    #[tokio::test]
    async fn pages_users() {
        let fx = setup().await;
        let first = search(
            &fx,
            SearchParams {
                types: Some("users".to_string()),
                limit: Some(1),
                ..params("beta")
            },
        )
        .await
        .unwrap()
        .users
        .unwrap();
        assert_eq!(first.hits.len(), 1);
        assert_eq!(first.hits[0].user_id, fx.users[0].to_string());
        let cursor = first.next_cursor.clone().unwrap();

        let second = search(
            &fx,
            SearchParams {
                types: Some("users".to_string()),
                limit: Some(1),
                cursor: Some(cursor),
                ..params("beta")
            },
        )
        .await
        .unwrap()
        .users
        .unwrap();
        assert_eq!(second.hits.len(), 1);
        assert_eq!(second.hits[0].user_id, fx.users[1].to_string());
        assert!(second.next_cursor.is_none());
    }

    #[tokio::test]
    async fn filters_documents_group() {
        // group_id passes through to metadata search and constrains the hits.
        let fx = setup().await;
        create_doc(&fx, fx.groups[0], "datasets/one", "gamma-one").await;
        create_doc(&fx, fx.groups[1], "datasets/two", "gamma-two").await;
        drain_projection(&fx.state).await;
        flush_search(&fx.state).await;

        let documents = search(
            &fx,
            SearchParams {
                types: Some("documents".to_string()),
                group_id: Some(fx.groups[0].to_string()),
                mode: Some(MetadataQueryMode::Local),
                ..params("gamma")
            },
        )
        .await
        .unwrap()
        .documents
        .unwrap();
        assert!(!documents.hits.is_empty());
        assert!(
            documents
                .hits
                .iter()
                .all(|hit| hit.group_id == fx.groups[0].to_string())
        );
    }

    #[tokio::test]
    async fn requires_auth() {
        let fx = setup().await;
        let unauthenticated = unified_search(
            State(fx.state.clone()),
            Extension(None),
            Extension(None),
            Query(params("alpha")),
        )
        .await;
        assert!(matches!(unauthenticated, Err(ServerError::Unauthorized)));

        let foreign = AuthContext {
            user_id: UserId::local(Ulid::from_bytes([9u8; 16]), realm_id(9)),
            realm_id: realm_id(9),
            path_restrictions: None,
        };
        assert_ne!(foreign.realm_id, fx.realm_id);
        let wrong_realm = unified_search(
            State(fx.state.clone()),
            Extension(Some(foreign)),
            Extension(None),
            Query(params("alpha")),
        )
        .await;
        assert!(matches!(wrong_realm, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn object_search_requires_auth() {
        let fx = setup().await;
        let result = object_search(
            State(fx.state.clone()),
            Extension(None),
            Extension(None),
            Query(ObjectSearchParams {
                q: "reads".to_string(),
                mode: Some(ObjectSearchMode::Local),
                ..Default::default()
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::Unauthorized)));
    }

    #[test]
    fn object_search_maps_partiality_without_totals() {
        let healthy = iroh::SecretKey::from_bytes(&[21u8; 32]).public();
        let failed = iroh::SecretKey::from_bytes(&[22u8; 32]).public();
        let result = ObjectSearchExecution {
            hits: vec![aruna_operations::s3::search_objects::ObjectInventoryHit {
                node_id: healthy,
                group_id: Ulid::from_bytes([23u8; 16]),
                bucket: "data".to_string(),
                key: "reads/a.fastq".to_string(),
                content_w3id: Some(format!("https://w3id.org/aruna/data/{}", "01".repeat(32))),
                checksum: None,
                size: Some(42),
                updated_at: Some(SystemTime::UNIX_EPOCH),
            }],
            next_cursor: Some("opaque".to_string()),
            as_of: SystemTime::UNIX_EPOCH,
            partitions: vec![
                aruna_operations::metadata::api::ObjectSearchPartitionCoverage {
                    node_id: healthy,
                    observed_at: SystemTime::UNIX_EPOCH,
                    truncated: true,
                },
            ],
            fanout_stats: aruna_operations::metadata::api::MetadataFanoutStats {
                nodes_queried: 2,
                nodes_failed: 1,
                failed_partitions: vec![failed],
                discovery_failed: false,
            },
            omitted_partitions: 0,
            complete: false,
        };

        let response = map_object_search_response(result, ObjectSearchMode::DistributedBestEffort);
        assert_eq!(response.hits.len(), 1);
        assert_eq!(response.coverage.scope, ObjectSearchScope::Realm);
        assert!(!response.coverage.complete);
        assert!(response.coverage.truncated);
        assert_eq!(response.coverage.nodes_failed, 1);
        assert_eq!(
            response.coverage.failed_partitions,
            vec![failed.to_string()]
        );
        assert_eq!(response.coverage.index_freshness.source, "live_heads");
        let encoded = serde_json::to_string(&response).unwrap();
        assert!(!encoded.contains("\"total"));
    }

    #[test]
    fn passes_documents_truncated() {
        // The depth-cap truncation signal must survive the unified mapping.
        let result = MetadataSearchExecution {
            hits: Vec::new(),
            next_cursor: None,
            truncated: true,
            fanout_stats: aruna_operations::metadata::api::MetadataFanoutStats {
                nodes_queried: 1,
                nodes_failed: 0,
                failed_partitions: Vec::new(),
                discovery_failed: false,
            },
        };
        let section = map_documents_section(result);
        assert!(section.truncated);
        assert!(section.next_cursor.is_none());
    }

    #[tokio::test]
    async fn empty_shape() {
        let fx = setup().await;
        let resp = search(
            &fx,
            SearchParams {
                mode: Some(MetadataQueryMode::Local),
                ..params("nomatchquery")
            },
        )
        .await
        .unwrap();
        let documents = resp.documents.unwrap();
        assert!(documents.hits.is_empty());
        assert!(documents.next_cursor.is_none());
        assert!(!documents.truncated);
        let buckets = resp.buckets.unwrap();
        assert!(buckets.hits.is_empty());
        let groups = resp.groups.unwrap();
        assert!(groups.hits.is_empty());
        assert!(groups.next_cursor.is_none());
        let users = resp.users.unwrap();
        assert!(users.hits.is_empty());
        assert!(users.next_cursor.is_none());
    }
}
