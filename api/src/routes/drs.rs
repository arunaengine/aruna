use super::routes_at;
use crate::auth::{ensure_permission, require_realm_auth};
use crate::download::{self, AdmissionError};
use crate::error::ServerError;
use crate::forwarded::{client_ip, external_base_url};
use crate::rate_limit::LocalKey;
use crate::server_state::ServerState;
use aruna_core::structs::{
    ArunaArn, ArunaArnType, AuthContext, BackendLocation, Permission, SourceMetadata,
    VersionedObjectArn, W3idDataIdentifier, blob_object_permission_path,
};
use aruna_operations::blob::resolve_blob_permission_paths::ResolveBlobPermissionPathsOperation;
use aruna_operations::driver::{drive, drive_until};
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::replication::location_summary::{
    LocationSummaryError, RemoteLocationSummaryOperation,
};
use aruna_operations::replication::protocol::LocationSummaryRequest;
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use aruna_operations::s3::get_object::{GetObjectError, GetObjectInput, GetObjectOperation};
use aruna_operations::s3::head_object::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
use axum::body::Body;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
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
const DRS_ROUTED_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(OpenApi)]
#[openapi(
    tags((name = "drs", description = "GA4GH DRS content access")),
    components(
        schemas(
            DrsServiceInfoResponse,
            DrsAuthorizationsResponse,
            DrsObjectResponse,
            DrsChecksum,
            DrsAccessMethod,
            DrsAccessUrl,
            DrsBulkObjectsRequestBody,
            DrsBulkObjectsResponse,
            DrsBulkObjectItem,
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
pub struct DrsServiceInfoResponse {
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
pub struct DrsBulkObjectsRequestBody {
    object_ids: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DrsBulkObjectsResponse {
    objects: Vec<DrsBulkObjectItem>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DrsBulkObjectItem {
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
    hashes: HashMap<String, Vec<u8>>,
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
    description = "Deliberately public: the GA4GH service-info document is served without authentication and a bearer token changes nothing. Answered from this node alone, with no lookup and no network call, so it is always current for the node that received the request. `id` and `name` are derived from the realm this node serves, `type` reports the GA4GH `org.ga4gh`/`drs` service type with this node's software version, and `organization.url` is the externally visible base URL of the node, taken from the forwarded scheme and host when the request came through a trusted proxy and from the `Host` header otherwise.",
    responses((
        status = 200,
        description = "GA4GH service-info document for this node",
        body = DrsServiceInfoResponse,
        example = json!({
            "id": "org.aruna.9xC3nQ2vRk5tYbW0aZ7pLmJ4hS6dF8gT1uV3wX5yZ2c",
            "name": "Aruna Realm 9xC3nQ2vRk5tYbW0aZ7pLmJ4hS6dF8gT1uV3wX5yZ2c",
            "type": {"group": "org.ga4gh", "artifact": "drs", "version": "3.0.0-alpha.41"},
            "organization": {"name": "Aruna", "url": "https://node.example.test"},
            "environment": "dev",
            "documentation_url": "https://docs.aruna-engine.org"
        })
    ))
)]
pub async fn get_service_info(
    State(state): State<Arc<ServerState>>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
) -> (StatusCode, Json<DrsServiceInfoResponse>) {
    let base_url = external_base_url(state.trusted_proxies(), peer.ip(), &headers);
    (
        StatusCode::OK,
        Json(DrsServiceInfoResponse {
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
    summary = "Report the authentication schemes accepted for a DRS object",
    description = "Deliberately public and never resolves the object: the identifier is echoed back unparsed, so this operation can neither confirm nor deny that an object exists and answers the same for every caller. `supported_types` is always `[BearerAuth]`; GA4GH passports are not accepted, so `passport_auth_issuers` is always empty. `bearer_auth_issuers` lists the OIDC issuers this node currently trusts, and is empty when no OIDC validator is configured or it cannot be reached. A browser CORS preflight on this path is answered by the CORS layer, when one is configured, and never reaches this operation.",
    params(("object_id" = String, Path, description = "Aruna data W3ID, content-hash ch ARN, or versioned s3 ARN locator, in the same three forms the object lookup accepts; it is echoed back verbatim and is not validated here")),
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
    description = "Authentication is optional and changes the result: an anonymous caller only resolves objects readable by the public role. A request without a bearer token, or with one this node cannot validate, is treated as anonymous rather than rejected, and READ is then evaluated for the Everyone principal. Only content held by this node in this realm resolves; an identifier naming another realm or node answers 404 without any lookup, so this is a node-local operation with no fan-out. A caller that presented a token but lacks READ gets 403, while an anonymous caller in the same position gets the same 404 as for a missing object, so existence is never revealed to an unauthenticated caller. A content-hash identifier is resolved against every object on this node carrying that content and the first one the caller may read is returned, so the same digest can resolve differently for different callers. The response carries the requested identifier in `id`, the canonical W3ID in `aliases` when the request used a different form, the stored checksums including the blake3 content digest, and a single `https` access method whose `access_url.url` is a direct download URL on this node; no redirect is issued and no signed or time-limited URL is minted, so the caller must send its own bearer token to that URL. `contents` is always null because bundles are not served.",
    params(("object_id" = String, Path, description = "Aruna data W3ID, content-hash ch ARN, or versioned s3 ARN locator: `https://w3id.org/aruna/data/{blake3-hex}`, `https://w3id.org/aruna/data/{versioned-s3-arn}`, `arn:aruna:{realm_id}:{node_id}:ch/{blake3-hex}` with 64 lowercase hex characters, or `arn:aruna:{realm_id}:{node_id}:s3/{bucket}/{key}@{version-ulid}` whose key is percent-encoded except for the separating slashes. Identifiers contain slashes, so the whole remainder of the path is taken as the identifier")),
    responses(
        (status = 200, description = "The resolved DRS object, as visible to this caller", body = DrsObjectResponse, example = json!({
            "id": "https://w3id.org/aruna/data/000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            "self_uri": "https://node.example.test/api/v1/ga4gh/drs/v1/objects/https%3A%2F%2Fw3id.org%2Faruna%2Fdata%2F000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            "name": "content-000102030405",
            "description": null,
            "size": 10485760,
            "checksums": [
                {"type": "blake3", "checksum": "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"},
                {"type": "sha256", "checksum": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"}
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
        (status = 404, description = "No such object on this node, or an anonymous caller may not read it", body = DrsErrorPayload)
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
    let auth = match drs_auth_or_anonymous(state.as_ref(), auth) {
        Ok(auth) => auth,
        Err(error) => return error.into_response(),
    };

    match resolve_object(state.as_ref(), &auth, &object_id).await {
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
    description = "Authentication is optional and changes the result: an anonymous caller only resolves objects readable by the public role. Each identifier is resolved and authorized exactly as the single-object lookup does, one after another and node-locally, so a batch is a convenience and not a transaction. The request itself succeeds with 200 whenever the body parses: per-identifier failures are reported inside the matching entry as `{status_code, msg}` instead of failing the batch, an unreadable object appears as 403 for a token-bearing caller and as 404 for an anonymous one, and an object that could not be serialized appears as 500. Entries are returned in the order the identifiers were given, one entry per identifier, including duplicates. The number of identifiers is bounded only by the server's maximum request body size.",
    request_body(
        content = DrsBulkObjectsRequestBody,
        description = "The DRS identifiers to resolve, in any of the forms the single-object lookup accepts",
        example = json!({
            "object_ids": [
                "https://w3id.org/aruna/data/000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
                "arn:aruna:9xC3nQ2vRk5tYbW0aZ7pLmJ4hS6dF8gT1uV3wX5yZ2c:1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978:s3/reads/run-42/sample.fastq.gz@01JABCDEF0123456789ABCDEFG"
            ]
        })
    ),
    responses(
        (status = 200, description = "One entry per requested identifier, in request order, each holding either the resolved object or a per-identifier error", body = DrsBulkObjectsResponse, example = json!({
            "objects": [
                {
                    "object_id": "https://w3id.org/aruna/data/000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
                    "result": {
                        "id": "https://w3id.org/aruna/data/000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
                        "self_uri": "https://node.example.test/api/v1/ga4gh/drs/v1/objects/https%3A%2F%2Fw3id.org%2Faruna%2Fdata%2F000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
                        "name": "content-000102030405",
                        "description": null,
                        "size": 10485760,
                        "checksums": [{"type": "blake3", "checksum": "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"}],
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
                    "result": {"status_code": 404, "msg": "DRS object not found"}
                }
            ]
        })),
        (status = 400, description = "The request body is not valid JSON for a list of identifiers; this rejection comes from the body extractor and is returned as plain text rather than the declared payload", body = DrsErrorPayload)
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn post_objects(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<DrsBulkObjectsRequestBody>,
) -> Response {
    let base_url = external_base_url(state.trusted_proxies(), peer.ip(), &headers);
    let anonymous = auth.is_none();
    let auth = match drs_auth_or_anonymous(state.as_ref(), auth) {
        Ok(auth) => auth,
        Err(error) => return error.into_response(),
    };

    let mut objects = Vec::with_capacity(body.object_ids.len());
    for object_id in body.object_ids {
        let result = match resolve_object(state.as_ref(), &auth, &object_id).await {
            Ok(ResolveOutcome::Found(resolved)) => serde_json::to_value(build_object_response(
                &base_url, &resolved,
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
            Err(error) => json!({ "status_code": error.status.as_u16(), "msg": error.message }),
        };
        objects.push(DrsBulkObjectItem { object_id, result });
    }
    drs_json_response(StatusCode::OK, DrsBulkObjectsResponse { objects })
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
    description = "Authentication is optional and changes the result: an anonymous caller only downloads objects readable by the public role. This is the URL advertised as the object's `https` access method; it streams the bytes itself and never redirects, so a client follows no `Location` and needs no signed URL, only its own bearer token. The identifier is resolved and authorized exactly as the object lookup does, node-locally, and every denial an anonymous caller could observe is reported as 404 so that existence stays hidden; an authenticated caller without READ gets 403. On success the response is 200 with the raw bytes and a `Content-Length` taken from the stored object; no content type is asserted and range requests are not supported. Each transfer takes one node-wide download slot and one per-caller slot, keyed by user for an authenticated caller and by client address for an anonymous one; when a slot cannot be taken the download is refused rather than queued and the caller may retry later. A transfer that stalls for 20 seconds, or that is still running after 30 minutes, is cut mid-body: the 200 has already been sent, so a client must treat a body shorter than `Content-Length` as a failed download and retry.",
    params(("object_id" = String, Query, description = "Aruna data W3ID, content-hash ch ARN, or versioned s3 ARN locator, in the same three forms the object lookup accepts, given as a single query parameter and URL-encoded because these identifiers contain `:` and `/`")),
    responses(
        (status = 200, description = "Object bytes, streamed inline with a `Content-Length` header and no content type"),
        (status = 400, description = "The identifier is not one of the accepted forms, or its content hash, key encoding or version is malformed", body = DrsErrorPayload),
        (status = 404, description = "No such object on this node, an anonymous caller may not read it, or the recorded reference observation is no longer served by its source", body = DrsErrorPayload),
        (status = 409, description = "Reference binding reached its automatic advance limit; retrying does not help until the reference is rebound by an explicit write", body = DrsErrorPayload),
        (status = 503, description = "Reference source is changing; retry. The same status is returned when the node's download capacity is exhausted, which is also retryable", body = DrsErrorPayload)
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
    let Ok(auth) = drs_auth_or_anonymous(state.as_ref(), auth) else {
        return drs_error(StatusCode::NOT_FOUND, "DRS object not found");
    };
    let resolved = match resolve_object(state.as_ref(), &auth, &query.object_id).await {
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
        Ok(Some(Ok(result))) => result,
        Ok(None) => return drs_error(StatusCode::NOT_FOUND, "DRS object not found"),
        Ok(Some(Err(error))) | Err(error) => return download_error(error),
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
fn drs_auth_or_anonymous(
    state: &ServerState,
    auth: Option<AuthContext>,
) -> Result<AuthContext, DrsError> {
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

async fn resolve_object(
    state: &ServerState,
    auth: &AuthContext,
    object_id: &str,
) -> Result<ResolveOutcome, DrsError> {
    match parse_requested_object_id(object_id)? {
        RequestedObjectId::CanonicalW3id(hash) => {
            resolve_content_hash(state, auth, object_id, None, &hash).await
        }
        RequestedObjectId::ContentHashArn {
            realm_id,
            node_id,
            hash,
        } => resolve_content_hash(state, auth, object_id, Some((realm_id, node_id)), &hash).await,
        RequestedObjectId::VersionedObject(arn) => {
            resolve_versioned(state, auth, object_id, &arn).await
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
) -> Result<ResolveOutcome, DrsError> {
    let context = state.get_ctx();
    let config = match drive(GetRealmConfigOperation::new(arn.realm_id), &context).await {
        Ok(config) => config,
        Err(_) => return Ok(ResolveOutcome::Unavailable),
    };
    // A stale config cannot prove a node is not a member, so absence is not 404.
    if !config.has_node(arn.node_id) {
        return Ok(ResolveOutcome::Unavailable);
    }
    let summary = drive_until(
        RemoteLocationSummaryOperation::new(
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
        Instant::now() + DRS_ROUTED_TIMEOUT,
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
) -> Result<ResolveOutcome, DrsError> {
    // This node serves exactly one realm, so a foreign realm is definitive absence.
    if arn.realm_id != state.get_realm_id() {
        return Ok(ResolveOutcome::NotFound);
    }
    if arn.node_id != state.get_node_id() {
        return resolve_routed(state, auth, requested_id, arn).await;
    }

    let bucket_info = match drive(
        GetBucketInfoOperation::new(arn.bucket.clone()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(Some(Ok(info))) => info,
        Ok(Some(Err(GetBucketInfoError::NotFound)))
        | Err(GetBucketInfoError::NotFound)
        | Ok(None) => return Ok(ResolveOutcome::NotFound),
        Ok(Some(Err(error))) | Err(error) => {
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
        Ok(Some(Ok(result))) => result,
        Ok(Some(Err(
            HeadObjectError::NoSuchKey
            | HeadObjectError::NoSuchVersion
            | HeadObjectError::DeleteMarker,
        )))
        | Err(
            HeadObjectError::NoSuchKey
            | HeadObjectError::NoSuchVersion
            | HeadObjectError::DeleteMarker,
        )
        | Ok(None) => return Ok(ResolveOutcome::NotFound),
        Ok(Some(Err(error))) | Err(error) => {
            return Err(DrsError::internal(error.to_string()));
        }
    };
    let Some(location) = head.location else {
        return Ok(ResolveOutcome::NotFound);
    };

    let path = blob_object_permission_path(
        arn.realm_id,
        bucket_info.group_id,
        arn.node_id,
        &arn.bucket,
        &arn.key,
    );
    if !can_read_permission_path(state, auth, &path).await? {
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
        hashes: location.hashes.clone(),
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

    let mappings = drive(
        ResolveBlobPermissionPathsOperation::new(*hash),
        &state.get_ctx(),
    )
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
                let allowed = can_read_permission_path(state, auth, &path).await?;
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
            Ok(Some(Ok(result))) => result,
            Ok(Some(Err(
                HeadObjectError::NoSuchKey
                | HeadObjectError::NoSuchVersion
                | HeadObjectError::DeleteMarker,
            )))
            | Err(
                HeadObjectError::NoSuchKey
                | HeadObjectError::NoSuchVersion
                | HeadObjectError::DeleteMarker,
            )
            | Ok(None) => {
                continue;
            }
            Ok(Some(Err(error))) | Err(error) => {
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
            hashes: location.hashes.clone(),
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

async fn can_read_permission_path(
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

fn parse_requested_object_id(object_id: &str) -> Result<RequestedObjectId, DrsError> {
    if object_id.starts_with(W3ID_DATA_PREFIX) {
        return match W3idDataIdentifier::parse(object_id)
            .map_err(|error| DrsError::bad_request(error.to_string()))?
        {
            W3idDataIdentifier::ContentHash(hash) => Ok(RequestedObjectId::CanonicalW3id(hash)),
            W3idDataIdentifier::VersionedObject(arn) => Ok(RequestedObjectId::VersionedObject(arn)),
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
mod tests {
    use super::{
        GetObjectError, RequestedObjectId, ResolveOutcome, ResolvedObject, W3ID_DATA_PREFIX,
        build_object_response, download_error, drs_denied_error, encode_component,
        get_authorizations, get_object, parse_requested_object_id, resolve_object,
    };
    use crate::openapi::ApiDoc;
    use crate::server_state::ServerState;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, GROUP_KEYSPACE,
        REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
    };
    use aruna_core::structs::{
        Actor, AuthContext, BackendLocation, BackendRef, BlobLocationKey, BlobVersion, BucketInfo,
        Group, GroupAuthorizationDocument, NodeCapabilities, RealmAuthorizationDocument,
        RealmConfigDocument, RealmId, SourceMetadata, VersionKey, VersionedObjectArn,
    };
    use aruna_core::{NodeId, UserId};
    use aruna_operations::driver::DriverContext;
    use aruna_storage::storage::FjallStorage;
    use axum::Extension;
    use axum::body::to_bytes;
    use axum::extract::{ConnectInfo, Path, State};
    use axum::http::{HeaderMap, StatusCode};
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::SystemTime;
    use tempfile::TempDir;
    use ulid::Ulid;

    fn materialized_location(blake3: [u8; 32]) -> BackendLocation {
        let mut hashes = HashMap::new();
        hashes.insert("blake3".to_string(), blake3.to_vec());
        hashes.insert("sha256".to_string(), vec![0xabu8; 32]);
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "objects".to_string(),
            backend_path: "blob.bin".to_string(),
            ulid: Ulid::from_bytes([2u8; 16]),
            compressed: false,
            encrypted: false,
            created_by: UserId::nil(RealmId([3u8; 32])),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 42,
            hashes,
        }
    }

    fn test_realm_id() -> RealmId {
        RealmId::from_bytes(
            *ed25519_dalek::SigningKey::from_bytes(&[7u8; 32])
                .verifying_key()
                .as_bytes(),
        )
    }

    fn test_node_id() -> NodeId {
        NodeId::from_str("ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6")
            .unwrap()
    }

    async fn test_state() -> (TempDir, Arc<ServerState>) {
        let dir = tempfile::tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let ctx = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let state = ServerState::new(
            ctx,
            test_realm_id(),
            test_node_id(),
            NodeCapabilities::local_node(test_realm_id()).expect("capabilities"),
            false,
            None,
            aruna_operations::jobs::runtime::JobsRuntime::new(),
        )
        .await;
        (dir, Arc::new(state))
    }

    async fn write_fixture(state: &ServerState, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        match state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected fixture write event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn authorizations_shape() {
        let (_dir, state) = test_state().await;
        let response = get_authorizations(State(state), Path("object/id".to_string())).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({
                "drs_object_id": "object/id",
                "supported_types": ["BearerAuth"],
                "passport_auth_issuers": [],
                "bearer_auth_issuers": []
            })
        );
    }

    async fn seed_version(state: &ServerState) -> (AuthContext, AuthContext, VersionedObjectArn) {
        let realm_id = state.get_realm_id();
        let node_id = state.get_node_id();
        let group_id = Ulid::from_bytes([4u8; 16]);
        let owner = UserId::new(Ulid::from_bytes([5u8; 16]), realm_id);
        let denied = UserId::new(Ulid::from_bytes([6u8; 16]), realm_id);
        let actor = Actor {
            node_id,
            user_id: owner,
            realm_id,
        };
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        let group_auth =
            GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
        let group = Group {
            display_name: "drs-group".to_string(),
            group_id,
            realm_id,
            roles: group_auth.roles.keys().copied().collect(),
            owner,
        };
        // Request-policy loading fails closed without the realm config, the group
        // record, and the group auth document.
        write_fixture(
            state,
            REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            RealmConfigDocument::default_for_realm(realm_id, Vec::new())
                .to_bytes(&actor)
                .expect("realm config serializes"),
        )
        .await;
        write_fixture(
            state,
            GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group.to_bytes(&actor).expect("group serializes"),
        )
        .await;
        write_fixture(
            state,
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            realm_auth.to_bytes(&actor).expect("realm auth serializes"),
        )
        .await;
        write_fixture(
            state,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group_auth.to_bytes(&actor).expect("group auth serializes"),
        )
        .await;

        let bucket = "mybucket";
        let key = "path/file @ 1.txt";
        let version = Ulid::from_bytes([7u8; 16]);
        let hash = [0x33u8; 32];
        let location = materialized_location(hash);
        let bucket_info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: owner,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        };
        write_fixture(
            state,
            S3_BUCKET_KEYSPACE,
            bucket.as_bytes().to_vec(),
            bucket_info.to_bytes().expect("bucket serializes"),
        )
        .await;
        write_fixture(
            state,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new(bucket, key, version)
                .to_bytes()
                .expect("version key serializes"),
            BlobVersion::materialized(
                hash,
                BackendRef::node_default(),
                SystemTime::UNIX_EPOCH,
                owner,
                None,
            )
            .to_bytes()
            .expect("version serializes"),
        )
        .await;
        write_fixture(
            state,
            BLOB_LOCATIONS_KEYSPACE,
            BlobLocationKey::new(hash, location.backend.clone()).to_bytes(),
            location.to_bytes().expect("location serializes"),
        )
        .await;

        (
            AuthContext {
                user_id: owner,
                realm_id,
                path_restrictions: None,
            },
            AuthContext {
                user_id: denied,
                realm_id,
                path_restrictions: None,
            },
            VersionedObjectArn::new(realm_id, node_id, bucket, key, version)
                .expect("versioned ARN"),
        )
    }

    #[test]
    fn anonymous_drs_denied_error_matches_unknown_object() {
        let anonymous = drs_denied_error(true);
        assert_eq!(anonymous.status, axum::http::StatusCode::NOT_FOUND);
        assert_eq!(anonymous.message, "DRS object not found");

        let authenticated = drs_denied_error(false);
        assert_eq!(authenticated.status, axum::http::StatusCode::FORBIDDEN);
        assert_eq!(authenticated.message, "Forbidden");
    }

    #[test]
    fn parses_canonical_w3id_object_id() {
        let expected_hash = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ];
        let parsed = parse_requested_object_id(
            "https://w3id.org/aruna/data/000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
        )
        .unwrap();

        match parsed {
            RequestedObjectId::CanonicalW3id(hash) => assert_eq!(hash, expected_hash),
            RequestedObjectId::ContentHashArn { .. } => panic!("expected canonical w3id id"),
            RequestedObjectId::VersionedObject(_) => panic!("expected canonical w3id id"),
        }
    }

    #[test]
    fn parses_content_hash_arn_preserving_realm_node_and_hash() {
        let realm_id = test_realm_id();
        let node_id = test_node_id();
        let arn = format!(
            "arn:aruna:{realm_id}:{node_id}:ch/000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        );

        let parsed = parse_requested_object_id(&arn).unwrap();

        match parsed {
            RequestedObjectId::ContentHashArn {
                realm_id: parsed_realm_id,
                node_id: parsed_node_id,
                hash,
            } => {
                assert_eq!(parsed_realm_id, realm_id);
                assert_eq!(parsed_node_id, node_id);
                assert_eq!(
                    hash,
                    [
                        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
                        0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
                        0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
                    ]
                );
            }
            RequestedObjectId::CanonicalW3id(_) => panic!("expected content-hash arn"),
            RequestedObjectId::VersionedObject(_) => panic!("expected content-hash arn"),
        }
    }

    #[test]
    fn rejects_malformed_version() {
        let realm_id = test_realm_id();
        let node_id = test_node_id();
        let bare = format!("arn:aruna:{realm_id}:{node_id}:s3/mybucket/path/file.txt@invalid");

        for object_id in [bare.clone(), format!("{W3ID_DATA_PREFIX}{bare}")] {
            let error = parse_requested_object_id(&object_id)
                .err()
                .expect("malformed version should be rejected");
            assert_eq!(error.status, StatusCode::BAD_REQUEST);
            assert!(
                error
                    .message
                    .contains("versioned object ARN has an invalid ULID")
            );
        }
    }

    #[tokio::test]
    async fn resolves_versioned_ids() {
        let (_dir, state) = test_state().await;
        let (auth, _, arn) = seed_version(state.as_ref()).await;

        for object_id in [arn.to_string(), arn.to_w3id()] {
            let outcome = resolve_object(state.as_ref(), &auth, &object_id)
                .await
                .expect("version resolves");
            let ResolveOutcome::Found(resolved) = outcome else {
                panic!("expected resolved version");
            };
            assert_eq!(resolved.bucket, arn.bucket);
            assert_eq!(resolved.key, arn.key);
            assert_eq!(resolved.version_id, arn.version);
            assert_eq!(resolved.group_id, Ulid::from_bytes([4u8; 16]));
        }
    }

    #[tokio::test]
    async fn rejects_nonlocal_version() {
        let (_dir, state) = test_state().await;
        let auth = AuthContext {
            user_id: UserId::new(Ulid::from_bytes([5u8; 16]), state.get_realm_id()),
            realm_id: state.get_realm_id(),
            path_restrictions: None,
        };
        let other_node = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
        let other_realm = RealmId::from_bytes(
            *ed25519_dalek::SigningKey::from_bytes(&[9u8; 32])
                .verifying_key()
                .as_bytes(),
        );
        let version = Ulid::from_bytes([7u8; 16]);
        // A foreign realm is definitive absence; a foreign node is only unproven.
        let cases = [
            (
                VersionedObjectArn::new(
                    other_realm,
                    state.get_node_id(),
                    "mybucket",
                    "path/file.txt",
                    version,
                )
                .unwrap(),
                StatusCode::NOT_FOUND,
            ),
            (
                VersionedObjectArn::new(
                    state.get_realm_id(),
                    other_node,
                    "mybucket",
                    "path/file.txt",
                    version,
                )
                .unwrap(),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ];

        for (arn, expected) in cases {
            let response = get_object(
                State(state.clone()),
                Extension(Some(auth.clone())),
                ConnectInfo("127.0.0.1:1".parse().unwrap()),
                HeaderMap::new(),
                Path(arn.to_string()),
            )
            .await;
            assert_eq!(response.status(), expected);
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("response body");
            let payload: serde_json::Value =
                serde_json::from_slice(&body).expect("typed error body");
            assert_eq!(payload["status_code"], expected.as_u16());
        }
    }

    #[tokio::test]
    async fn enforces_version_auth() {
        let (_dir, state) = test_state().await;
        let (_, denied, arn) = seed_version(state.as_ref()).await;

        let outcome = resolve_object(state.as_ref(), &denied, &arn.to_string())
            .await
            .expect("authorization resolves");
        assert!(matches!(outcome, ResolveOutcome::Denied));
    }

    #[tokio::test]
    async fn returns_missing_version() {
        let (_dir, state) = test_state().await;
        let (auth, _, arn) = seed_version(state.as_ref()).await;
        let missing = VersionedObjectArn::new(
            arn.realm_id,
            arn.node_id,
            arn.bucket,
            arn.key,
            Ulid::from_bytes([8u8; 16]),
        )
        .unwrap();

        let response = get_object(
            State(state),
            Extension(Some(auth)),
            ConnectInfo("127.0.0.1:1".parse().unwrap()),
            HeaderMap::new(),
            Path(missing.to_string()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("typed error body");
        assert_eq!(payload["status_code"], 404);
        assert_eq!(payload["msg"], "DRS object not found");
    }

    #[test]
    fn materialized_canonical_w3id_response_omits_aliases_and_keeps_download_method() {
        let blake3 = [0x11u8; 32];
        let canonical_w3id = format!("{W3ID_DATA_PREFIX}{}", hex::encode(blake3));
        let resolved = ResolvedObject {
            bucket: "mybucket".to_string(),
            key: "path/file.txt".to_string(),
            group_id: Ulid::from_bytes([4u8; 16]),
            version_id: Ulid::from_bytes([5u8; 16]),
            canonical_w3id: canonical_w3id.clone(),
            requested_id: canonical_w3id.clone(),
            size: 42,
            hashes: materialized_location(blake3).hashes.clone(),
            location: Some(materialized_location(blake3)),
            source_metadata: Some(SourceMetadata {
                content_length: 42,
                content_type: Some("application/octet-stream".to_string()),
                etag: Some("etag-from-materialized".to_string()),
                last_modified: None,
                source_version: None,
            }),
        };

        let response = build_object_response("https://drs.example.test", &resolved);

        assert!(response.aliases.is_empty());
        assert_eq!(response.id, canonical_w3id);
        assert_eq!(response.checksums.len(), 2);
        assert!(
            response
                .checksums
                .iter()
                .any(|checksum| checksum.kind == "blake3"
                    && checksum.checksum == hex::encode(blake3))
        );
        assert!(
            response
                .checksums
                .iter()
                .any(|checksum| checksum.kind == "sha256" && checksum.checksum == "ab".repeat(32))
        );
        assert_eq!(response.access_methods.len(), 1);
        assert_eq!(response.access_methods[0].kind, "https");
        assert_eq!(
            response.access_methods[0].access_url.as_ref().unwrap().url,
            format!(
                "https://drs.example.test/api/v1/ga4gh/drs/v1/download?object_id={}",
                encode_component(&canonical_w3id)
            )
        );
    }

    #[test]
    fn materialized_content_hash_arn_response_exposes_canonical_alias() {
        let realm_id = test_realm_id();
        let node_id = test_node_id();
        let blake3 = [0x22u8; 32];
        let canonical_w3id = format!("{W3ID_DATA_PREFIX}{}", hex::encode(blake3));
        let requested_id = format!("arn:aruna:{realm_id}:{node_id}:ch/{}", hex::encode(blake3));
        let resolved = ResolvedObject {
            bucket: "mybucket".to_string(),
            key: "path/file.txt".to_string(),
            group_id: Ulid::from_bytes([6u8; 16]),
            version_id: Ulid::from_bytes([7u8; 16]),
            canonical_w3id: canonical_w3id.clone(),
            requested_id: requested_id.clone(),
            size: 42,
            hashes: materialized_location(blake3).hashes.clone(),
            location: Some(materialized_location(blake3)),
            source_metadata: Some(SourceMetadata {
                content_length: 42,
                content_type: Some("application/octet-stream".to_string()),
                etag: Some("etag-from-materialized".to_string()),
                last_modified: None,
                source_version: None,
            }),
        };

        let response = build_object_response("https://drs.example.test", &resolved);

        assert_eq!(response.id, requested_id);
        assert_eq!(response.aliases, vec![canonical_w3id.clone()]);
        assert_eq!(response.checksums.len(), 2);
        assert_eq!(response.access_methods.len(), 1);
        assert_eq!(
            response.access_methods[0].access_url.as_ref().unwrap().url,
            format!(
                "https://drs.example.test/api/v1/ga4gh/drs/v1/download?object_id={}",
                encode_component(&requested_id)
            )
        );
    }

    // A lost historical observation is not a server fault, drift is transient,
    // and an exhausted binding needs a rebind: three distinct statuses.
    #[test]
    fn maps_reference_errors() {
        for (error, status) in [
            (
                GetObjectError::HistoricalReferenceUnavailable,
                StatusCode::NOT_FOUND,
            ),
            (
                GetObjectError::ReferenceSourceChanged,
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                GetObjectError::ReferenceAdvanceExhausted,
                StatusCode::CONFLICT,
            ),
            (GetObjectError::NoSuchKey, StatusCode::NOT_FOUND),
            (
                GetObjectError::GetObjectFailed,
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ] {
            assert_eq!(download_error(error).status(), status);
        }
    }

    // Operators reading the spec must see the reference statuses the route can
    // actually return.
    #[test]
    fn download_declares_statuses() {
        let openapi = ApiDoc::openapi();
        let responses = &openapi
            .paths
            .paths
            .get("/ga4gh/drs/v1/download")
            .expect("download path")
            .get
            .as_ref()
            .expect("download operation")
            .responses
            .responses;
        for status in ["404", "409", "503"] {
            assert!(responses.contains_key(status), "missing {status}");
        }
    }

    #[test]
    fn drs_openapi_includes_service_and_object_paths() {
        let openapi = ApiDoc::openapi();
        assert!(
            openapi
                .paths
                .paths
                .contains_key("/ga4gh/drs/v1/service-info")
        );
        assert!(openapi.paths.paths.contains_key("/ga4gh/drs/v1/objects"));
        assert!(
            openapi
                .paths
                .paths
                .contains_key("/ga4gh/drs/v1/objects/{object_id}")
        );
        assert!(openapi.paths.paths.contains_key("/ga4gh/drs/v1/download"));
        let _ = W3ID_DATA_PREFIX;
    }
}
