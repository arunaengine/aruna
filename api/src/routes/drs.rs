use super::routes_at;
use crate::auth::{ensure_permission, require_realm_auth};
use crate::download::{self, AdmissionError};
use crate::error::ServerError;
use crate::forwarded::{client_ip, external_base_url};
use crate::rate_limit::LocalKey;
use crate::server_state::ServerState;
use aruna_core::structs::{
    ArunaArn, ArunaArnType, AuthContext, BackendLocation, Permission, SourceMetadata,
    VersionedObjectArn, W3idIdentifier, object_permission_path,
};
use aruna_operations::blob::permission_paths::ResolvePathsOperation;
use aruna_operations::driver::{drive, drive_until};
use aruna_operations::realm::get_config::GetConfigOperation;
use aruna_operations::replication::locations::{LocationSummaryError, RemoteLocationOperation};
use aruna_operations::replication::protocol::LocationSummaryRequest;
use aruna_operations::s3::get_bucket::{GetBucketError, GetBucketOperation};
use aruna_operations::s3::get_object::{GetObjectError, GetObjectInput, GetObjectOperation};
use aruna_operations::s3::head_object::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
use axum::body::Body;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{debug, warn};
use ulid::Ulid;
use url::form_urlencoded::byte_serialize;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

const W3ID_DATA_PREFIX: &str = "https://w3id.org/aruna/data/";
const ACCESS_ID_HTTPS: &str = "https";
/// Whole-request budget for the routed probes one call may need, however many
/// identifiers it names.
const DRS_ROUTED_TIMEOUT: Duration = Duration::from_secs(5);
/// Identifiers one bulk request may name. Each foreign one costs a routed probe.
const MAX_BULK_OBJECT_IDS: usize = 100;
/// Routed probes of one bulk request that may be in flight at once.
const BULK_PROBE_CONCURRENCY: usize = 8;

#[derive(OpenApi)]
#[openapi(
    tags((name = "drs", description = "GA4GH DRS content access")),
    components(
        schemas(
            DrsServiceResponse,
            DrsAuthorizationsResponse,
            DrsObjectResponse,
            DrsChecksum,
            DrsAccessMethod,
            DrsAccessUrl,
            DrsBulkBody,
            DrsBulk,
            DrsBulkItem,
            DrsErrorPayload
        )
    )
)]
pub struct DrsApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    routes_at(
        OpenApiRouter::with_openapi(DrsApiDoc::openapi())
            .routes(routes!(get_service_info))
            .routes(routes!(post_objects))
            .routes(routes!(download_object)),
        // Object ids carry `/`, so the runtime template is a catch-all.
        "/ga4gh/drs/v1/objects/{*object_id}",
        routes!(get_authorizations, get_object),
    )
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = DrsServiceInfoResponse)]
pub struct DrsServiceResponse {
    id: String,
    name: String,
    r#type: DrsServiceType,
    organization: DrsOrganization,
    environment: String,
    documentation_url: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DrsServiceType {
    group: &'static str,
    artifact: &'static str,
    version: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DrsOrganization {
    name: String,
    url: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DrsAuthorizationsResponse {
    drs_object_id: String,
    supported_types: Vec<String>,
    passport_auth_issuers: Vec<String>,
    bearer_auth_issuers: Vec<String>,
}

#[cfg(test)]
pub(crate) fn authorizations_response() -> Response {
    drs_json_response(
        StatusCode::OK,
        DrsAuthorizationsResponse {
            drs_object_id: "https://w3id.org/aruna/data/000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
                .to_string(),
            supported_types: vec!["BearerAuth".to_string()],
            passport_auth_issuers: vec![],
            bearer_auth_issuers: vec!["https://login.example.test/realms/aruna".to_string()],
        },
    )
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DrsObjectResponse {
    id: String,
    self_uri: String,
    name: String,
    description: Option<String>,
    size: Option<u64>,
    checksums: Vec<DrsChecksum>,
    mime_type: Option<String>,
    aliases: Vec<String>,
    access_methods: Vec<DrsAccessMethod>,
    contents: Option<Vec<Value>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DrsChecksum {
    #[serde(rename = "type")]
    kind: String,
    checksum: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DrsAccessUrl {
    url: String,
    headers: HashMap<String, String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DrsAccessMethod {
    access_id: String,
    #[serde(rename = "type")]
    kind: String,
    region: Option<String>,
    access_url: Option<DrsAccessUrl>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[schema(as = DrsBulkObjectsRequestBody)]
pub struct DrsBulkBody {
    object_ids: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = DrsBulkObjectsResponse)]
pub struct DrsBulk {
    objects: Vec<DrsBulkItem>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = DrsBulkObjectItem)]
pub struct DrsBulkItem {
    object_id: String,
    result: Value,
}

#[derive(Debug, Deserialize)]
pub struct DownloadQuery {
    object_id: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(example = json!({"status_code": 404, "msg": "DRS object not found"}))]
pub struct DrsErrorPayload {
    status_code: u16,
    msg: String,
}

enum RequestedObjectId {
    CanonicalW3id([u8; 32]),
    ContentHashArn {
        realm_id: aruna_core::structs::RealmId,
        node_id: aruna_core::NodeId,
        hash: [u8; 32],
    },
    VersionedObject(VersionedObjectArn),
}

struct ResolvedObject {
    bucket: String,
    key: String,
    group_id: Ulid,
    version_id: Ulid,
    canonical_w3id: String,
    requested_id: String,
    size: u64,
    /// Ordered, so the same object always reports its checksums in one order.
    hashes: BTreeMap<String, Vec<u8>>,
    source_metadata: Option<SourceMetadata>,
    /// Present only when the bytes live here; a routed resolve reports metadata.
    location: Option<BackendLocation>,
}

#[allow(clippy::large_enum_variant)]
enum ResolveOutcome {
    Found(ResolvedObject),
    Denied,
    NotFound,
    /// The owning node could not answer; absence was never established.
    Unavailable,
}

#[utoipa::path(
    get,
    path = "/ga4gh/drs/v1/service-info",
    tag = "drs",
    summary = "Describe this node's GA4GH DRS service",
    description = r#"Serves the GA4GH service-info document for this node.

**Authentication**: none; the route is deliberately public and a bearer token changes nothing.

**Behavior**
- `id` and `name` are derived from the realm this node serves, and `type` reports the GA4GH
  `org.ga4gh`/`drs` service type with this node's software version.
- `organization.url` is the externally visible base URL of the node, taken from the forwarded scheme
  and host when the request came through a trusted proxy and from the `Host` header otherwise."#,
    responses((
        status = 200,
        description = "GA4GH service-info document for this node",
        body = DrsServiceResponse,
        example = json!({
            "id": "org.aruna.9xC3nQ2vRk5tYbW0aZ7pLmJ4hS6dF8gT1uV3wX5yZ2c",
            "name": "Aruna Realm 9xC3nQ2vRk5tYbW0aZ7pLmJ4hS6dF8gT1uV3wX5yZ2c",
            "type": {
                "group": "org.ga4gh",
                "artifact": "drs",
                "version": "3.0.0-alpha.41"
            },
            "organization": {
                "name": "Aruna",
                "url": "https://node.example.test"
            },
            "environment": "dev",
            "documentation_url": "https://docs.aruna-engine.org"
        })
    ))
)]
pub async fn get_service_info(
    State(state): State<Arc<ServerState>>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
) -> (StatusCode, Json<DrsServiceResponse>) {
    let base_url = external_base_url(state.trusted_proxies(), peer.ip(), &headers);
    (
        StatusCode::OK,
        Json(DrsServiceResponse {
            id: format!("org.aruna.{}", state.get_realm_id()),
            name: format!("Aruna Realm {}", state.get_realm_id()),
            r#type: DrsServiceType {
                group: "org.ga4gh",
                artifact: "drs",
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            organization: DrsOrganization {
                name: "Aruna".to_string(),
                url: base_url,
            },
            environment: "dev".to_string(),
            documentation_url: Some("https://docs.aruna-engine.org".to_string()),
        }),
    )
}

#[utoipa::path(
    options,
    path = "/ga4gh/drs/v1/objects/{object_id}",
    tag = "drs",
    summary = "Report the authentication schemes for a DRS object",
    description = r#"Reports the authentication schemes and bearer issuers this node accepts for DRS content.

**Authentication**: none; the route is deliberately public and answers the same for every caller.

**Behavior**
- The object is never resolved: the identifier is echoed back unparsed, so this operation can
  neither confirm nor deny that an object exists.
- `supported_types` is always `[BearerAuth]`; GA4GH passports are not accepted, so
  `passport_auth_issuers` is always empty.
- `bearer_auth_issuers` lists the OIDC issuers this node currently trusts, and is empty when no
  OIDC validator is configured or it cannot be reached.
- A browser CORS preflight on this path is answered by the CORS layer, where one is configured, and
  never reaches this operation."#,
    params(("object_id" = String, Path, description = "Aruna data W3ID, content-hash `ch` ARN, or versioned `s3` ARN locator; echoed back verbatim and not validated here")),
    responses(
        (status = 200, description = "Authentication schemes and bearer issuers this node accepts for DRS content", body = DrsAuthorizationsResponse, example = json!({
            "drs_object_id": "https://w3id.org/aruna/data/000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            "supported_types": ["BearerAuth"],
            "passport_auth_issuers": [],
            "bearer_auth_issuers": ["https://login.example.test/realms/aruna"]
        }))
    ),
)]
pub async fn get_authorizations(
    State(state): State<Arc<ServerState>>,
    Path(object_id): Path<String>,
) -> Response {
    let issuers = match state.oidc_validator() {
        Ok(validator) => validator.issuers().await,
        Err(_) => {
            warn!("OIDC validator not available");
            vec![]
        }
    };

    let response = DrsAuthorizationsResponse {
        drs_object_id: object_id,
        supported_types: vec!["BearerAuth".to_string()],
        passport_auth_issuers: vec![],
        bearer_auth_issuers: issuers,
    };

    drs_json_response(StatusCode::OK, response)
}

#[utoipa::path(
    get,
    path = "/ga4gh/drs/v1/objects/{object_id}",
    tag = "drs",
    summary = "Resolve a DRS object by identifier",
    description = r#"Resolves one DRS identifier to the object's metadata and access method.

**Authentication**: optional, and it changes the result. A request without a bearer token, or with
one this node cannot validate, is treated as anonymous rather than rejected, and READ is then
evaluated for the Everyone principal, so an anonymous caller resolves only publicly readable
objects.

**Behavior**
- An identifier naming another realm answers 404 without any lookup.
- A versioned identifier naming another node of this realm is probed at that node, which alone
  establishes absence: within one bounded probe budget the answer is its own 404, 403 or the
  object's metadata.
- Only the owning node serves the bytes, so a routed answer advertises no access method.
- A content-hash identifier resolves against every object on this node carrying that content and
  returns the first one the caller may read, so the same digest can resolve differently for
  different callers.
- The response carries the requested identifier in `id`, the canonical W3ID in `aliases` when the
  request used a different form, and the stored checksums in a stable order.
- For an object held here it carries a single `https` access method whose `access_url.url` is a
  direct download URL on this node; no redirect is issued and no signed or time-limited URL is
  minted, so the caller must send its own bearer token to that URL.
- `contents` is always null because bundles are not served.

**Limits**
- The identifier is an Aruna data W3ID, a content-hash `ch` ARN or a versioned `s3` ARN:
  `https://w3id.org/aruna/data/{blake3-hex}`, `https://w3id.org/aruna/data/{versioned-s3-arn}`,
  `arn:aruna:{realm_id}:{node_id}:ch/{blake3-hex}` with 64 lowercase hex characters, or
  `arn:aruna:{realm_id}:{node_id}:s3/{bucket}/{key}@{version-ulid}` whose key is percent-encoded
  except for the separating slashes."#,
    params(("object_id" = String, Path, description = "Aruna data W3ID, content-hash `ch` ARN, or versioned `s3` ARN locator; the whole remainder of the path is taken as the identifier")),
    responses(
        (status = 200, description = "The resolved DRS object, as visible to this caller", body = DrsObjectResponse, example = json!({
            "id": "https://w3id.org/aruna/data/000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            "self_uri": "https://node.example.test/api/v1/ga4gh/drs/v1/objects/https%3A%2F%2Fw3id.org%2Faruna%2Fdata%2F000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            "name": "content-000102030405",
            "description": null,
            "size": 10485760,
            "checksums": [
                {
                    "type": "blake3",
                    "checksum": "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
                },
                {
                    "type": "sha256",
                    "checksum": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
                }
            ],
            "mime_type": "application/octet-stream",
            "aliases": [],
            "access_methods": [
                {
                    "access_id": "https",
                    "type": "https",
                    "region": null,
                    "access_url": {
                        "url": "https://node.example.test/api/v1/ga4gh/drs/v1/download?object_id=https%3A%2F%2Fw3id.org%2Faruna%2Fdata%2F000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
                        "headers": {}
                    }
                }
            ],
            "contents": null
        })),
        (status = 400, description = "The identifier is not one of the accepted forms, or its content hash, key encoding or version is malformed", body = DrsErrorPayload),
        (status = 403, description = "The caller presented a token but may not read the object, or the token belongs to another realm", body = DrsErrorPayload),
        (status = 404, description = "No such object on this node, or an anonymous caller may not read it, which is deliberately indistinguishable", body = DrsErrorPayload),
        (status = 503, description = "The node owning the identifier could not answer within the request's probe budget, so absence was never established; the caller may retry", body = DrsErrorPayload)
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn get_object(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    Path(object_id): Path<String>,
) -> Response {
    let base_url = external_base_url(state.trusted_proxies(), peer.ip(), &headers);
    let anonymous = auth.is_none();
    let auth = match drs_auth(state.as_ref(), auth) {
        Ok(auth) => auth,
        Err(error) => return error.into_response(),
    };

    match resolve_object(state.as_ref(), &auth, &object_id, routed_deadline()).await {
        Ok(ResolveOutcome::Found(resolved)) => {
            drs_json_response(StatusCode::OK, build_object_response(&base_url, &resolved))
        }
        Ok(ResolveOutcome::Denied) => drs_denied_error(anonymous).into_response(),
        Ok(ResolveOutcome::NotFound) => DrsError::not_found("DRS object not found").into_response(),
        Ok(ResolveOutcome::Unavailable) => DrsError::unavailable().into_response(),
        Err(error) => error.into_response(),
    }
}

#[utoipa::path(
    post,
    path = "/ga4gh/drs/v1/objects",
    tag = "drs",
    summary = "Resolve several DRS objects in one request",
    description = r#"Resolves a list of DRS identifiers in one request, one entry per identifier.

**Authentication**: optional, and it changes the result exactly as in the single-object lookup: an
anonymous caller resolves only publicly readable objects.

**Behavior**
- Each identifier is resolved and authorized exactly as the single-object lookup does, so a batch is
  a convenience and not a transaction.
- Identifiers owned by other nodes are probed concurrently against one budget for the whole request,
  so a batch costs no more wall time than a single lookup.
- The request itself succeeds with 200 whenever the body parses and stays inside the cap; entries
  come back in request order, one per identifier, including duplicates.

**Limits**
- At most 100 identifiers may be named, and a longer list is rejected before anything is resolved.

**Errors**: a per-identifier failure is reported inside its own entry as `{status_code, msg}`.
- An owner that did not answer inside the budget is 503, never an absence.
- An unreadable object is 403 for a token-bearing caller and 404 for an anonymous one.
- An object that could not be serialized is 500."#,
    request_body(
        content = DrsBulkBody,
        description = "The DRS identifiers to resolve, in any of the forms the single-object lookup accepts",
        example = json!({
            "object_ids": [
                "https://w3id.org/aruna/data/000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
                "arn:aruna:9xC3nQ2vRk5tYbW0aZ7pLmJ4hS6dF8gT1uV3wX5yZ2c:1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978:s3/reads/run-42/sample.fastq.gz@01JABCDEF0123456789ABCDEFG"
            ]
        })
    ),
    responses(
        (status = 200, description = "One entry per requested identifier, in request order, each holding either the resolved object or a per-identifier error", body = DrsBulk, example = json!({
            "objects": [
                {
                    "object_id": "https://w3id.org/aruna/data/000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
                    "result": {
                        "id": "https://w3id.org/aruna/data/000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
                        "self_uri": "https://node.example.test/api/v1/ga4gh/drs/v1/objects/https%3A%2F%2Fw3id.org%2Faruna%2Fdata%2F000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
                        "name": "content-000102030405",
                        "description": null,
                        "size": 10485760,
                        "checksums": [
                            {
                                "type": "blake3",
                                "checksum": "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
                            }
                        ],
                        "mime_type": "application/octet-stream",
                        "aliases": [],
                        "access_methods": [
                            {
                                "access_id": "https",
                                "type": "https",
                                "region": null,
                                "access_url": {
                                    "url": "https://node.example.test/api/v1/ga4gh/drs/v1/download?object_id=https%3A%2F%2Fw3id.org%2Faruna%2Fdata%2F000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
                                    "headers": {}
                                }
                            }
                        ],
                        "contents": null
                    }
                },
                {
                    "object_id": "arn:aruna:9xC3nQ2vRk5tYbW0aZ7pLmJ4hS6dF8gT1uV3wX5yZ2c:1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978:s3/reads/run-42/sample.fastq.gz@01JABCDEF0123456789ABCDEFG",
                    "result": {
                        "status_code": 404,
                        "msg": "DRS object not found"
                    }
                }
            ]
        })),
        (status = 400, description = "More than 100 identifiers were named; a body that is not valid JSON for a list of identifiers is rejected by the extractor as plain text instead of this payload", body = DrsErrorPayload)
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn post_objects(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<DrsBulkBody>,
) -> Response {
    if body.object_ids.len() > MAX_BULK_OBJECT_IDS {
        return DrsError::bad_request(format!(
            "a bulk request names at most {MAX_BULK_OBJECT_IDS} identifiers"
        ))
        .into_response();
    }
    let base_url = external_base_url(state.trusted_proxies(), peer.ip(), &headers);
    let anonymous = auth.is_none();
    let auth = match drs_auth(state.as_ref(), auth) {
        Ok(auth) => auth,
        Err(error) => return error.into_response(),
    };

    // One budget for the whole batch, spent by bounded concurrent probes, so a
    // foreign identifier cannot multiply into a request-timeout with no answers.
    let deadline = Instant::now() + DRS_ROUTED_TIMEOUT;
    let node = state.as_ref();
    let auth = &auth;
    let base_url = &base_url;
    let objects: Vec<DrsBulkItem> = futures_util::stream::iter(body.object_ids)
        .map(|object_id| async move {
            let result = match resolve_object(node, auth, &object_id, deadline).await {
                Ok(ResolveOutcome::Found(resolved)) => serde_json::to_value(build_object_response(
                    base_url, &resolved,
                ))
                .unwrap_or_else(|_| json!({ "status_code": 500, "msg": "serialization failed" })),
                Ok(ResolveOutcome::Denied) => {
                    let error = drs_denied_error(anonymous);
                    json!({ "status_code": error.status.as_u16(), "msg": error.message })
                }
                Ok(ResolveOutcome::NotFound) => {
                    json!({ "status_code": 404, "msg": "DRS object not found" })
                }
                Ok(ResolveOutcome::Unavailable) => {
                    let error = DrsError::unavailable();
                    json!({ "status_code": error.status.as_u16(), "msg": error.message })
                }
                Err(error) => {
                    json!({ "status_code": error.status.as_u16(), "msg": error.message })
                }
            };
            DrsBulkItem { object_id, result }
        })
        .buffered(BULK_PROBE_CONCURRENCY)
        .collect()
        .await;
    drs_json_response(StatusCode::OK, DrsBulk { objects })
}

/// Maps a source read failure onto its DRS status: an observation the source no
/// longer serves is a 404, transient drift a 503, and an exhausted binding a 409
/// because only an explicit rebind heals it.
fn download_error(error: GetObjectError) -> Response {
    match error {
        GetObjectError::NoSuchKey
        | GetObjectError::NoSuchVersion
        | GetObjectError::DeleteMarker => drs_error(StatusCode::NOT_FOUND, "DRS object not found"),
        GetObjectError::HistoricalReferenceUnavailable => drs_error(
            StatusCode::NOT_FOUND,
            "the recorded reference observation is no longer served by the source",
        ),
        GetObjectError::ReferenceSourceChanged => drs_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "the reference source is changing; retry this download",
        ),
        GetObjectError::ReferenceAdvanceExhausted => drs_error(
            StatusCode::CONFLICT,
            "the reference binding reached its automatic advance limit; rebind it with an explicit write",
        ),
        error => DrsError::internal(error.to_string()).into_response(),
    }
}

#[utoipa::path(
    get,
    path = "/ga4gh/drs/v1/download",
    tag = "drs",
    summary = "Download the bytes of a DRS object",
    description = r#"Streams the bytes of a DRS object from the node that owns it.

**Authentication**: optional, and it changes the result exactly as in the object lookup: an
anonymous caller downloads only publicly readable objects.

**Behavior**
- This is the URL advertised as the object's `https` access method; it streams the bytes itself and
  never redirects, so a client follows no `Location` and needs no signed URL, only its own bearer
  token.
- Only the owning node serves bytes: an identifier that resolves at another node of the realm is
  answered 501, because this node holds the metadata but not the object.
- The 200 carries the raw bytes and a `Content-Length` taken from the stored object; no content type
  is asserted and range requests are not supported.
- Each transfer takes one node-wide download slot and one per-caller slot, keyed by user for an
  authenticated caller and by client address for an anonymous one; a slot that cannot be taken
  refuses the download rather than queueing it.

**Limits**
- A transfer that stalls for 20 seconds, or that is still running after 30 minutes, is cut mid-body:
  the 200 has already been sent, so a client must treat a body shorter than `Content-Length` as a
  failed download and retry."#,
    params(("object_id" = String, Query, description = "Aruna data W3ID, content-hash `ch` ARN, or versioned `s3` ARN locator, URL-encoded because these identifiers contain `:` and `/`")),
    responses(
        (status = 200, description = "Object bytes, streamed inline with a `Content-Length` header and no content type"),
        (status = 400, description = "The identifier is not one of the accepted forms, or its content hash, key encoding or version is malformed", body = DrsErrorPayload),
        (status = 403, description = "An authenticated caller lacks READ; an anonymous caller gets 404 instead, so existence stays hidden", body = DrsErrorPayload),
        (status = 404, description = "No such object on this node, an anonymous caller may not read it, or the recorded reference observation is no longer served by its source", body = DrsErrorPayload),
        (status = 409, description = "Reference binding reached its automatic advance limit; retrying does not help until the reference is rebound by an explicit write", body = DrsErrorPayload),
        (status = 501, description = "The identifier resolves to an object owned by another node, which alone serves its bytes; read the object first and download from the endpoint it names", body = DrsErrorPayload),
        (status = 503, description = "Reference source is changing, or the owning node could not answer within the probe budget; retry. The same status is returned when the node's download capacity is exhausted, which is also retryable", body = DrsErrorPayload)
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn download_object(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    Query(query): Query<DownloadQuery>,
) -> Response {
    let anonymous = auth.is_none();
    let Ok(auth) = drs_auth(state.as_ref(), auth) else {
        return drs_error(StatusCode::NOT_FOUND, "DRS object not found");
    };
    let resolved =
        match resolve_object(state.as_ref(), &auth, &query.object_id, routed_deadline()).await {
            Ok(ResolveOutcome::Found(resolved)) => resolved,
            Ok(ResolveOutcome::Denied) => return drs_denied_error(anonymous).into_response(),
            Ok(ResolveOutcome::NotFound) => {
                return DrsError::not_found("DRS object not found").into_response();
            }
            Ok(ResolveOutcome::Unavailable) => return DrsError::unavailable().into_response(),
            Err(error) => return error.into_response(),
        };
    let Some(resolved_location) = resolved.location.clone() else {
        return drs_error(
            StatusCode::NOT_IMPLEMENTED,
            "object bytes are served by the owning node",
        );
    };

    let key = if anonymous {
        LocalKey::Ip(client_ip(state.trusted_proxies(), peer.ip(), &headers))
    } else {
        LocalKey::User(auth.user_id)
    };
    let permit = match download::admit(state.as_ref(), key) {
        Ok(permit) => permit,
        Err(AdmissionError::Total) => {
            return drs_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "download capacity exhausted",
            );
        }
        Err(AdmissionError::User) => {
            return drs_error(StatusCode::TOO_MANY_REQUESTS, "download capacity exhausted");
        }
    };

    let result = match drive(
        GetObjectOperation::new(GetObjectInput {
            bucket: resolved.bucket.clone(),
            key: resolved.key.clone(),
            version_id: Some(resolved.version_id),
            range: None,
            group_id: resolved.group_id,
            user_identity: auth.user_id,
            node_id: state.get_node_id(),
        }),
        &state.get_ctx(),
    )
    .await
    {
        Ok(result) => result,
        Err(error) => return download_error(error),
    };

    let location = result.location.unwrap_or(resolved_location);

    let mut response = Response::new(download::body(result.blob, permit));
    *response.status_mut() = StatusCode::OK;
    if let Ok(value) = http::HeaderValue::from_str(&location.blob_size.to_string()) {
        response
            .headers_mut()
            .insert(http::header::CONTENT_LENGTH, value);
    }
    response
}

fn build_object_response(base_url: &str, resolved: &ResolvedObject) -> DrsObjectResponse {
    let self_uri = format!(
        "{base_url}/api/v1/ga4gh/drs/v1/objects/{}",
        encode_component(&resolved.requested_id)
    );
    let hash = resolved
        .canonical_w3id
        .strip_prefix(W3ID_DATA_PREFIX)
        .unwrap_or_default();
    let name = format!("content-{}", &hash[..hash.len().min(12)]);
    let checksums = resolved
        .hashes
        .iter()
        .map(|(kind, value)| DrsChecksum {
            kind: kind.clone(),
            checksum: hex::encode(value),
        })
        .collect();
    let aliases = if resolved.requested_id == resolved.canonical_w3id {
        Vec::new()
    } else {
        vec![resolved.canonical_w3id.clone()]
    };
    // Only the owning node can serve the bytes, so a routed answer advertises none.
    let access_methods = resolved
        .location
        .iter()
        .map(|_| DrsAccessMethod {
            access_id: ACCESS_ID_HTTPS.to_string(),
            kind: "https".to_string(),
            region: None,
            access_url: Some(DrsAccessUrl {
                url: format!(
                    "{base_url}/api/v1/ga4gh/drs/v1/download?object_id={}",
                    encode_component(&resolved.requested_id)
                ),
                headers: HashMap::new(),
            }),
        })
        .collect();

    DrsObjectResponse {
        id: resolved.requested_id.clone(),
        self_uri,
        name,
        description: None,
        size: Some(resolved.size),
        checksums,
        mime_type: resolved
            .source_metadata
            .as_ref()
            .and_then(|metadata| metadata.content_type.clone()),
        aliases,
        access_methods,
        contents: None,
    }
}

fn require_drs_auth(
    state: &ServerState,
    auth: Option<AuthContext>,
) -> Result<AuthContext, DrsError> {
    require_realm_auth(state, auth).map_err(|_| DrsError::forbidden("Forbidden"))
}

/// Requests without a bearer token resolve as the Everyone principal. Public
/// roles are then the only grants that can make an object readable; denied
/// anonymous lookups are mapped to 404 at the route layer.
fn drs_auth(state: &ServerState, auth: Option<AuthContext>) -> Result<AuthContext, DrsError> {
    match auth {
        Some(_) => require_drs_auth(state, auth),
        None => Ok(AuthContext::anonymous(state.get_realm_id())),
    }
}

fn drs_denied_error(anonymous: bool) -> DrsError {
    if anonymous {
        DrsError::not_found("DRS object not found")
    } else {
        DrsError::forbidden("Forbidden")
    }
}

fn routed_deadline() -> Instant {
    Instant::now() + DRS_ROUTED_TIMEOUT
}

/// `deadline` bounds every routed probe of one request, so a batch can never
/// cost more wall time than a single lookup does.
async fn resolve_object(
    state: &ServerState,
    auth: &AuthContext,
    object_id: &str,
    deadline: Instant,
) -> Result<ResolveOutcome, DrsError> {
    match parse_object_id(object_id)? {
        RequestedObjectId::CanonicalW3id(hash) => {
            resolve_content_hash(state, auth, object_id, None, &hash).await
        }
        RequestedObjectId::ContentHashArn {
            realm_id,
            node_id,
            hash,
        } => resolve_content_hash(state, auth, object_id, Some((realm_id, node_id)), &hash).await,
        RequestedObjectId::VersionedObject(arn) => {
            resolve_versioned(state, auth, object_id, &arn, deadline).await
        }
    }
}

/// Resolve a versioned ARN owned by another node in this realm. Only the owner
/// establishes absence; anything short of its answer is `Unavailable`.
async fn resolve_routed(
    state: &ServerState,
    auth: &AuthContext,
    requested_id: &str,
    arn: &VersionedObjectArn,
    deadline: Instant,
) -> Result<ResolveOutcome, DrsError> {
    let context = state.get_ctx();
    let config = match drive(GetConfigOperation::new(arn.realm_id), &context).await {
        Ok(config) => config,
        Err(_) => return Ok(ResolveOutcome::Unavailable),
    };
    // A stale config cannot prove a node is not a member, so absence is not 404.
    if !config.has_node(arn.node_id) {
        return Ok(ResolveOutcome::Unavailable);
    }
    let summary = drive_until(
        RemoteLocationOperation::new(
            arn.node_id,
            LocationSummaryRequest {
                realm_id: arn.realm_id,
                bucket: arn.bucket.clone(),
                key: arn.key.clone(),
                version_id: Some(arn.version),
                auth_context: auth.clone(),
            },
        ),
        &context,
        deadline,
    )
    .await;
    let summary = match summary {
        Ok(summary) => summary,
        Err(LocationSummaryError::Denied) => return Ok(ResolveOutcome::Denied),
        Err(LocationSummaryError::BucketNotFound) => return Ok(ResolveOutcome::NotFound),
        Err(_) => return Ok(ResolveOutcome::Unavailable),
    };
    if summary.version_id != Some(arn.version) || !summary.materialized {
        return Ok(ResolveOutcome::NotFound);
    }
    let (Some(group_id), Some(size)) = (summary.group_id, summary.blob_size) else {
        return Ok(ResolveOutcome::Unavailable);
    };
    Ok(ResolveOutcome::Found(ResolvedObject {
        bucket: arn.bucket.clone(),
        key: arn.key.clone(),
        group_id,
        version_id: arn.version,
        canonical_w3id: arn.to_w3id(),
        requested_id: requested_id.to_string(),
        size,
        hashes: summary.hashes.into_iter().collect(),
        source_metadata: None,
        location: None,
    }))
}

async fn resolve_versioned(
    state: &ServerState,
    auth: &AuthContext,
    requested_id: &str,
    arn: &VersionedObjectArn,
    deadline: Instant,
) -> Result<ResolveOutcome, DrsError> {
    // This node serves exactly one realm, so a foreign realm is definitive absence.
    if arn.realm_id != state.get_realm_id() {
        return Ok(ResolveOutcome::NotFound);
    }
    if arn.node_id != state.get_node_id() {
        return resolve_routed(state, auth, requested_id, arn, deadline).await;
    }

    let bucket_info = match drive(
        GetBucketOperation::new(arn.bucket.clone()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(info) => info,
        Err(GetBucketError::NotFound) => return Ok(ResolveOutcome::NotFound),
        Err(error) => {
            return Err(DrsError::internal(error.to_string()));
        }
    };

    let head = match drive(
        HeadObjectOperation::new(HeadObjectInput {
            bucket: arn.bucket.clone(),
            key: arn.key.clone(),
            version_id: Some(arn.version),
        }),
        &state.get_ctx(),
    )
    .await
    {
        Ok(result) => result,
        Err(
            HeadObjectError::NoSuchKey
            | HeadObjectError::NoSuchVersion
            | HeadObjectError::DeleteMarker,
        ) => return Ok(ResolveOutcome::NotFound),
        Err(error) => {
            return Err(DrsError::internal(error.to_string()));
        }
    };
    let Some(location) = head.location else {
        return Ok(ResolveOutcome::NotFound);
    };

    let path = object_permission_path(
        arn.realm_id,
        bucket_info.group_id,
        arn.node_id,
        &arn.bucket,
        &arn.key,
    );
    if !can_read_path(state, auth, &path).await? {
        return Ok(ResolveOutcome::Denied);
    }

    Ok(ResolveOutcome::Found(ResolvedObject {
        bucket: arn.bucket.clone(),
        key: arn.key.clone(),
        group_id: bucket_info.group_id,
        version_id: arn.version,
        canonical_w3id: arn.to_w3id(),
        requested_id: requested_id.to_string(),
        size: location.blob_size,
        hashes: location.hashes.clone().into_iter().collect(),
        source_metadata: head.source_metadata,
        location: Some(location),
    }))
}

async fn resolve_content_hash(
    state: &ServerState,
    auth: &AuthContext,
    requested_id: &str,
    requested_scope: Option<(aruna_core::structs::RealmId, aruna_core::NodeId)>,
    hash: &[u8; 32],
) -> Result<ResolveOutcome, DrsError> {
    if let Some((realm_id, node_id)) = requested_scope
        && (realm_id != state.get_realm_id() || node_id != state.get_node_id())
    {
        return Ok(ResolveOutcome::NotFound);
    }

    let mappings = drive(ResolvePathsOperation::new(*hash), &state.get_ctx())
        .await
        .map_err(|error| DrsError::internal(error.to_string()))?;
    debug!(?mappings);

    let mut any_mapping_on_this_node = false;
    let mut last_permission_check: Option<(String, bool)> = None;

    for mapping in mappings {
        if mapping.realm_id != state.get_realm_id() || mapping.node_id != state.get_node_id() {
            debug!("Realm id or node id mismatch");
            continue;
        }
        any_mapping_on_this_node = true;
        let path = mapping.permission_path();
        let allowed = match &last_permission_check {
            Some((cached_path, allowed)) if cached_path == &path => *allowed,
            _ => {
                let allowed = can_read_path(state, auth, &path).await?;
                last_permission_check = Some((path.clone(), allowed));
                allowed
            }
        };
        if !allowed {
            debug!("No permissions for path: {path}");
            continue;
        }
        let head = match drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: mapping.bucket.clone(),
                key: mapping.key.clone(),
                version_id: Some(mapping.version_id),
            }),
            &state.get_ctx(),
        )
        .await
        {
            Ok(result) => result,
            Err(
                HeadObjectError::NoSuchKey
                | HeadObjectError::NoSuchVersion
                | HeadObjectError::DeleteMarker,
            ) => {
                continue;
            }
            Err(error) => {
                debug!(head_object_error = ?error);
                return Err(DrsError::internal(error.to_string()));
            }
        };
        let Some(location) = head.location else {
            continue;
        };
        if location.get_blake3() != Some(hash.as_slice()) {
            continue;
        }
        return Ok(ResolveOutcome::Found(ResolvedObject {
            bucket: mapping.bucket,
            key: mapping.key,
            group_id: mapping.group_id,
            version_id: mapping.version_id,
            canonical_w3id: format!("{W3ID_DATA_PREFIX}{}", hex::encode(hash)),
            requested_id: requested_id.to_string(),
            size: location.blob_size,
            hashes: location.hashes.clone().into_iter().collect(),
            source_metadata: head.source_metadata,
            location: Some(location),
        }));
    }

    if any_mapping_on_this_node {
        Ok(ResolveOutcome::Denied)
    } else {
        Ok(ResolveOutcome::NotFound)
    }
}

async fn can_read_path(
    state: &ServerState,
    auth: &AuthContext,
    path: &str,
) -> Result<bool, DrsError> {
    match ensure_permission(state, auth, path.to_string(), Permission::READ).await {
        Ok(()) => Ok(true),
        Err(ServerError::Forbidden) => Ok(false),
        Err(error) => Err(DrsError::internal(error.to_string())),
    }
}

fn parse_object_id(object_id: &str) -> Result<RequestedObjectId, DrsError> {
    if object_id.starts_with(W3ID_DATA_PREFIX) {
        return match W3idIdentifier::parse(object_id)
            .map_err(|error| DrsError::bad_request(error.to_string()))?
        {
            W3idIdentifier::ContentHash(hash) => Ok(RequestedObjectId::CanonicalW3id(hash)),
            W3idIdentifier::VersionedObject(arn) => Ok(RequestedObjectId::VersionedObject(arn)),
        };
    }

    let arn =
        ArunaArn::parse(object_id).map_err(|error| DrsError::bad_request(error.to_string()))?;
    debug!(?arn);
    if arn.resource_type == ArunaArnType::S3 {
        return VersionedObjectArn::parse(object_id)
            .map(RequestedObjectId::VersionedObject)
            .map_err(|error| DrsError::bad_request(error.to_string()));
    }
    let hash = decode_blake3_hex(&arn.path)?;

    Ok(RequestedObjectId::ContentHashArn {
        realm_id: arn.realm_id,
        node_id: arn.node_id,
        hash,
    })
}

fn decode_blake3_hex(hash_hex: &str) -> Result<[u8; 32], DrsError> {
    let bytes =
        hex::decode(hash_hex).map_err(|_| DrsError::bad_request("content hash is invalid"))?;
    bytes
        .try_into()
        .map_err(|_| DrsError::bad_request("content hash is invalid"))
}

fn encode_component(value: &str) -> String {
    byte_serialize(value.as_bytes()).collect()
}

fn drs_json_response<T: Serialize>(status: StatusCode, value: T) -> Response {
    let body = serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec());
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
}

fn drs_error(status: StatusCode, message: impl Into<String>) -> Response {
    drs_json_response(
        status,
        DrsErrorPayload {
            status_code: status.as_u16(),
            msg: message.into(),
        },
    )
}

#[derive(Debug)]
struct DrsError {
    status: StatusCode,
    message: String,
}

impl DrsError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }

    /// The owning node did not answer, so absence was never established.
    fn unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: "DRS object owner unavailable".to_string(),
        }
    }
}

impl IntoResponse for DrsError {
    fn into_response(self) -> Response {
        drs_error(self.status, self.message)
    }
}

#[cfg(test)]
#[path = "drs_tests.rs"]
mod tests;
