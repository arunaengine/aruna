//! Serves the GA4GH DRS v1 routes for service info, object lookup, bulk resolve and download.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::routes_at;
use crate::auth::{ensure_permission, require_realm_auth};
use crate::download::{self, AdmissionError};
use crate::error::ServerError;
use crate::forwarded::{client_ip, external_base_url};
use crate::rate_limit::LocalKey;
use crate::server::state::{RestInterfaceRuntime, ServerState};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{BackendLocation, object_permission_path};
use aruna_core::structs::storage::distribution::ticket::{SignedTicket, TICKET_QUERY};
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::replication::{
    ArunaArn, ArunaArnType, VersionedObjectArn, W3idIdentifier,
};
use aruna_operations::auth::bearer_token::token_grant;
use aruna_operations::blob::permission_paths::{ResolvePathsError, ResolvePathsOperation};
use aruna_operations::distribution::resolve::{GlobalResolution, resolve_content, resolve_version};
use aruna_operations::distribution::ticket::{
    RedeemError, TicketInput, issue_ticket, redeem_ticket,
};
use aruna_operations::driver::{drive, drive_until};
use aruna_operations::node::holder_choice::{avoid_holder, choose_holder};
use aruna_operations::node::node_info::read_info_documents;
use aruna_operations::realm::get_config::GetConfigOperation;
use aruna_operations::replication::locations::{LocationSummaryError, RemoteLocationOperation};
use aruna_operations::replication::protocol::LocationSummaryRequest;
use aruna_operations::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_operations::s3::object::get::{GetObjectError, GetObjectInput, GetObjectOperation};
use aruna_operations::s3::object::head::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
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

const DATA_PREFIX: &str = "https://w3id.org/aruna/data/";
const ACCESS_ID_HTTPS: &str = "https";
/// Whole-request budget for the routed probes one call may need, however many
/// identifiers it names.
const DRS_ROUTED_TIMEOUT: Duration = Duration::from_secs(5);
/// Holders of a distributed version asked in turn before it counts as unavailable.
const MAX_HOLDER_TRIES: usize = 3;
/// Identifiers one bulk request may name. Each foreign one costs a routed probe.
const MAX_OBJECT_IDS: usize = 100;
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
    let router = routes_at(
        OpenApiRouter::with_openapi(DrsApiDoc::openapi())
            .routes(routes!(get_service_info))
            .routes(routes!(post_objects))
            .routes(routes!(download_object)),
        // Object ids carry `/`, so the runtime template is a catch-all.
        "/ga4gh/drs/v1/objects/{*object_id}",
        routes!(get_authorizations, get_object),
    );
    routes_at(router, "/resolve/{*id}", routes!(resolve_data))
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
    /// Datasets whose linked parts contain the object, as the caller may see them; absent
    /// when the bytes are not on this node.
    #[serde(skip_serializing_if = "Option::is_none")]
    is_part_of: Option<DrsPartOf>,
}

/// The derived `isPartOf` of an object: at most ten dataset w3ids.
#[derive(Debug, Serialize, ToSchema)]
pub struct DrsPartOf {
    datasets: Vec<String>,
    /// More datasets contain the object than are named here.
    more: bool,
    complete: bool,
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
    /// A pinned URL: serve here and never choose another holder.
    #[serde(default)]
    here: bool,
    /// A download ticket of the node that redirected here.
    ticket: Option<String>,
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
        realm_id: aruna_core::structs::identity::realm::RealmId,
        node_id: aruna_core::NodeId,
        hash: [u8; 32],
    },
    VersionedObject(VersionedObjectArn),
    /// A distributed bucket's version by its global id.
    GlobalVersion {
        realm_id: aruna_core::structs::identity::realm::RealmId,
        version_id: Ulid,
    },
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
    /// The owning node's published base URL when the bytes live there.
    origin: Option<String>,
    /// The members that keep the bytes of a distributed object; empty for ordinary objects.
    holders: Vec<aruna_core::NodeId>,
    /// The base URL of the holder chosen to serve; its access URL is pinned there.
    pinned: Option<String>,
    /// The node that answered for bytes that live elsewhere.
    answered_by: Option<aruna_core::NodeId>,
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
- Only the owning node serves the bytes, so a routed answer's access method is a download URL on
  the owner's published API URL; an owner that published none is unavailable (503).
- A distributed bucket's version is named by its global id (URL or short path
  `/{realm-id}/{version-id}`), resolved on any realm node through the set records: this node's
  member answers when it keeps the bytes, otherwise a chosen holder answers for the caller. The
  access URL is the pinned download URL (`here=true`) of the holder chosen by free download slots.
  A node ARN of a distributed bucket's object is answered only from this node's member and its
  canonical id is the global id.
- A content-hash identifier resolves against every object on this node carrying that content and
  returns the first one the caller may read, so the same digest can resolve differently for
  different callers.
- A content-hash W3ID that no object on this node carries is looked up in the content indexes of
  distributed buckets (at most 16 versions, bounded probes) and answers under the global id of
  the first readable version. A search that could not ask every range answers 503 instead of 404.
- A content hash carried by more objects on this node than one lookup reads answers 409; use
  the global id or a versioned ARN.
- The response carries the requested identifier in `id`, the canonical W3ID in `aliases` when the
  request used a different form, and the stored checksums in a stable order.
- For an object held here it carries a single `https` access method whose `access_url.url` is a
  direct download URL on this node; no redirect is issued and no signed or time-limited URL is
  minted, so the caller must send its own bearer token to that URL.
- `contents` is always null because bundles are not served.
- For an object held here, `is_part_of` names the w3ids of at most ten datasets whose linked
  parts contain it, as the caller may read them, with `more` and `complete` flags. It is
  absent when no metadata node answered.

**Limits**
- The identifier is an Aruna data W3ID, a content-hash `ch` ARN or a versioned `s3` ARN:
  `https://w3id.org/aruna/data/{blake3-hex}`, `https://w3id.org/aruna/data/{versioned-s3-arn}`,
  `arn:aruna:{realm_id}:{node_id}:ch/{blake3-hex}` with 64 lowercase hex characters, or
  `arn:aruna:{realm_id}:{node_id}:s3/{bucket}/{key}@{version-ulid}` whose key is percent-encoded
  except for the separating slashes, or a global id `https://w3id.org/aruna/data/{realm-id}/{version-id}`
  or `/{realm-id}/{version-id}`. Bucket and prefix global ids name no object (400)."#,
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
        (status = 409, description = "More than one distributed bucket holds the version id, a node ARN names a distributed bucket's version this node's member does not hold, or a content hash names more objects than one lookup reads; use the global id", body = DrsErrorPayload),
        (status = 503, description = "The node owning the identifier could not answer within the request's probe budget, or has published no API URL to download from; the caller may retry", body = DrsErrorPayload)
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn get_object(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<crate::auth::ValidatedBearer>>,
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
        Ok(ResolveOutcome::Found(mut resolved)) => {
            pin_holder(state.as_ref(), &mut resolved).await;
            let mut object = build_object_response(&base_url, &resolved);
            if resolved.location.is_some() {
                let caller = (!anonymous).then_some(auth);
                let bearer = crate::metadata::bearer_token_string(bearer);
                object.is_part_of = drs_part_of(state.as_ref(), caller, bearer, &resolved).await;
            }
            drs_json_response(StatusCode::OK, object)
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
- An owner that did not answer inside the budget, or published no API URL, is 503, never an
  absence.
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
    if body.object_ids.len() > MAX_OBJECT_IDS {
        return DrsError::bad_request(format!(
            "a bulk request names at most {MAX_OBJECT_IDS} identifiers"
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
                Ok(ResolveOutcome::Found(mut resolved)) => {
                    pin_holder(node, &mut resolved).await;
                    serde_json::to_value(build_object_response(base_url, &resolved)).unwrap_or_else(
                        |_| json!({ "status_code": 500, "msg": "serialization failed" }),
                    )
                }
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
    description = r#"Streams the bytes of a DRS object, or redirects to the node that serves them.

**Authentication**: optional, and it changes the result exactly as in the object lookup: an
anonymous caller downloads only publicly readable objects. A `ticket` from the node that
redirected here stands in for the caller's bearer token.

**Behavior**
- Every original download chooses who serves: for a distributed object two random holders with
  a fresh heartbeat, the one with more free download slots wins, and this node counts when it
  keeps the bytes; an ordinary object of another node is served by that node.
- When another node serves, the answer is a temporary redirect (307, `Cache-Control: no-store`)
  to its pinned download URL (`here=true`). For an authenticated caller the URL carries a
  single-use download ticket valid for a few seconds, because browsers drop `Authorization`
  on cross-origin redirects; the target checks the ticket, revocation and the caller's
  permission on its own path. A caller whose token cannot be named in a ticket (an external
  OIDC token) is served here when this node keeps the bytes; otherwise the answer is 503 with the
  pinned URL of the serving node, to request there with the same token.
- A pinned URL serves here and never chooses again; a holder that cannot serve answers 503 and
  the client retries the original URL.
- The 200 carries the raw bytes and a `Content-Length` taken from the stored object; no content type
  is asserted and range requests are not supported.
- Each transfer takes one node-wide download slot and one per-caller slot, keyed by user for an
  authenticated caller and by client address for an anonymous one; a slot that cannot be taken
  refuses the download rather than queueing it. Heartbeats report the slot use.

**Limits**
- A transfer that stalls for 20 seconds, or that is still running after 30 minutes, is cut mid-body:
  the 200 has already been sent, so a client must treat a body shorter than `Content-Length` as a
  failed download and retry."#,
    params(
        ("object_id" = String, Query, description = "Aruna data W3ID (content hash, versioned `s3` ARN or global id), short global id `/{realm-id}/{version-id}`, content-hash `ch` ARN, or versioned `s3` ARN locator, URL-encoded because these identifiers contain `:` and `/`"),
        ("here" = Option<bool>, Query, description = "Pinned URL: serve on this node and never redirect"),
        ("ticket" = Option<String>, Query, description = "Download ticket of the redirecting node; single use, never logged")
    ),
    responses(
        (status = 200, description = "Object bytes, streamed inline with a `Content-Length` header and no content type"),
        (status = 307, description = "Another node serves the bytes; `Location` is its pinned download URL, with a download ticket for an authenticated caller, and the answer is never cached"),
        (status = 400, description = "The identifier is not one of the accepted forms, or its content hash, key encoding or version is malformed", body = DrsErrorPayload),
        (status = 403, description = "An authenticated caller lacks READ, or the download ticket is refused; an anonymous caller gets 404 instead, so existence stays hidden", body = DrsErrorPayload),
        (status = 404, description = "No such object, an anonymous caller may not read it, or the recorded reference observation is no longer served by its source", body = DrsErrorPayload),
        (status = 409, description = "Reference binding reached its automatic advance limit, or more than one distributed bucket holds the version id", body = DrsErrorPayload),
        (status = 503, description = "Reference source is changing, no holder could answer within the probe budget, this pinned holder cannot serve, the node's download capacity is exhausted, or a token no ticket can carry must be sent to the named holder; retry", body = DrsErrorPayload)
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn download_object(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<crate::auth::ValidatedBearer>>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    Query(query): Query<DownloadQuery>,
) -> Response {
    let caller = Caller {
        auth,
        bearer,
        address: client_ip(state.trusted_proxies(), peer.ip(), &headers),
    };
    serve_download(&state, caller, &query.object_id, query.here, query.ticket).await
}

#[derive(Debug, Deserialize)]
pub struct ResolveQuery {
    #[serde(default)]
    here: bool,
    ticket: Option<String>,
}

#[utoipa::path(
    get,
    path = "/resolve/{id}",
    tag = "drs",
    summary = "Resolve an Aruna data w3id to its bytes",
    description = r#"The node route the realm registry redirects `https://w3id.org/aruna/data/{id}` to.

**Authentication**: optional, exactly as for `GET /ga4gh/drs/v1/download`.

**Behavior**: `{id}` is the part after `https://w3id.org/aruna/data/`: a global id
`{realm-id}/{version-id}`, a versioned `s3` node ARN or a content hash. The answer is the
download of that identifier: the bytes, or a temporary redirect (307, `Cache-Control:
no-store`) to the holder chosen to serve them. Bucket and prefix ids name no object (400)."#,
    params(
        ("id" = String, Path, description = "Identifier after the w3id data base; the whole remainder of the path"),
        ("here" = Option<bool>, Query, description = "Pinned URL: serve on this node and never redirect"),
        ("ticket" = Option<String>, Query, description = "Download ticket of the redirecting node; single use, never logged")
    ),
    responses(
        (status = 200, description = "Object bytes, streamed inline with a `Content-Length` header"),
        (status = 307, description = "Another node serves the bytes; `Location` is its pinned download URL"),
        (status = 400, description = "The identifier is malformed or names no object", body = DrsErrorPayload),
        (status = 403, description = "An authenticated caller lacks READ, or the download ticket is refused", body = DrsErrorPayload),
        (status = 404, description = "No such object, or an anonymous caller may not read it", body = DrsErrorPayload),
        (status = 409, description = "More than one distributed bucket holds the version id", body = DrsErrorPayload),
        (status = 503, description = "No holder could answer now; retry", body = DrsErrorPayload)
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn resolve_data(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<crate::auth::ValidatedBearer>>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    uri: http::Uri,
    Query(query): Query<ResolveQuery>,
) -> Response {
    let caller = Caller {
        auth,
        bearer,
        address: client_ip(state.trusted_proxies(), peer.ip(), &headers),
    };
    // The raw path keeps an ARN key percent-encoded, as its canonical form requires.
    let Some(id) = raw_id(&uri) else {
        return DrsError::bad_request("the path names no identifier").into_response();
    };
    let object_id = format!("{DATA_PREFIX}{id}");
    serve_download(&state, caller, &object_id, query.here, query.ticket).await
}

/// The still percent-encoded identifier after `/resolve/` of a request path.
fn raw_id(uri: &http::Uri) -> Option<String> {
    let (_, id) = uri.path().split_once("/resolve/")?;
    Some(id.to_string())
}

/// Who asks for a download: the authenticated caller, its token and its address.
struct Caller {
    auth: Option<AuthContext>,
    bearer: Option<crate::auth::ValidatedBearer>,
    address: std::net::IpAddr,
}

/// Serves or redirects one download of `object_id`; `here` marks a pinned URL.
async fn serve_download(
    state: &Arc<ServerState>,
    caller: Caller,
    object_id: &str,
    here: bool,
    ticket: Option<String>,
) -> Response {
    let mut auth = caller.auth;
    if auth.is_none()
        && let Some(encoded) = ticket.as_deref()
    {
        match redeem(state, encoded, object_id).await {
            Ok(redeemed) => auth = Some(redeemed),
            Err(response) => return response,
        }
    }
    let anonymous = auth.is_none();
    let Ok(auth) = drs_auth(state.as_ref(), auth) else {
        return drs_error(StatusCode::NOT_FOUND, "DRS object not found");
    };
    let resolved = match resolve_object(state.as_ref(), &auth, object_id, routed_deadline()).await {
        Ok(ResolveOutcome::Found(resolved)) => resolved,
        Ok(ResolveOutcome::Denied) => return drs_denied_error(anonymous).into_response(),
        Ok(ResolveOutcome::NotFound) => {
            return DrsError::not_found("DRS object not found").into_response();
        }
        Ok(ResolveOutcome::Unavailable) => return DrsError::unavailable().into_response(),
        Err(error) => return error.into_response(),
    };
    let token = crate::metadata::bearer_token_string(caller.bearer);
    let grant = token.as_deref().and_then(|token| token_grant(token).ok());
    // A caller no ticket can name is served here whenever the bytes are here.
    let ticketable = anonymous || grant.is_some();
    if !here && (ticketable || resolved.location.is_none()) {
        let target = match chosen_holder(state.as_ref(), &resolved).await {
            Some(node_id) => Some(node_id),
            None if resolved.location.is_none() => resolved.answered_by,
            None => None,
        };
        if let Some(node_id) = target {
            if !ticketable {
                return holder_hint(state.as_ref(), node_id, object_id).await;
            }
            let ticket = grant.map(|grant| TicketInput {
                target: node_id,
                user_id: auth.user_id,
                grant,
                restrictions: auth.path_restrictions.clone(),
                session: auth.session.clone(),
                object_id: object_id.to_string(),
            });
            return holder_redirect(state.as_ref(), node_id, object_id, ticket).await;
        }
    }
    let Some(resolved_location) = resolved.location.clone() else {
        return drs_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "this node cannot serve the object now; retry the original URL",
        );
    };

    let key = if anonymous {
        LocalKey::Ip(caller.address)
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
            realm_id: auth.realm_id,
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

/// Redeems a download ticket for `object_id`; a refusal is the response to send.
async fn redeem(
    state: &Arc<ServerState>,
    encoded: &str,
    object_id: &str,
) -> Result<AuthContext, Response> {
    let refused = || drs_error(StatusCode::FORBIDDEN, "download ticket refused");
    let signed = SignedTicket::decode(encoded).map_err(|_| refused())?;
    let now = now_millis() / 1000;
    match redeem_ticket(&state.get_ctx(), &signed, object_id, now).await {
        Ok(auth) => Ok(auth),
        Err(RedeemError::Unavailable) => Err(DrsError::unavailable().into_response()),
        Err(_) => Err(refused()),
    }
}

/// The node that serves a distributed object instead of this one, of two fresh holders the one
/// with more free download slots; this node counts when it keeps the bytes. `None` serves here.
async fn chosen_holder(
    state: &ServerState,
    resolved: &ResolvedObject,
) -> Option<aruna_core::NodeId> {
    if resolved.holders.is_empty() {
        return None;
    }
    let local = state.get_node_id();
    let candidates: Vec<_> = resolved
        .holders
        .iter()
        .copied()
        .filter(|node_id| *node_id != local || resolved.location.is_some())
        .collect();
    choose_holder(&state.get_ctx(), &candidates, now_millis())
        .await
        .map(|chosen| chosen.node_id)
        .filter(|node_id| *node_id != local)
}

/// Points the access URL of a distributed object at the holder chosen to serve it.
async fn pin_holder(state: &ServerState, resolved: &mut ResolvedObject) {
    if let Some(node_id) = chosen_holder(state, resolved).await
        && let Ok(Some(origin)) = owner_origin(state, node_id).await
    {
        resolved.pinned = Some(origin);
    }
}

/// The pinned download URL of `object_id` on `node_id`.
async fn pinned_url(
    state: &ServerState,
    node_id: aruna_core::NodeId,
    object_id: &str,
) -> Option<String> {
    let origin = owner_origin(state, node_id).await.ok()??;
    Some(format!(
        "{origin}/api/v1/ga4gh/drs/v1/download?object_id={}&here=true",
        encode_component(object_id)
    ))
}

/// A redirect would drop a credential no ticket can carry, so the caller gets the holder's
/// pinned URL to request with the same credential instead.
async fn holder_hint(
    state: &ServerState,
    node_id: aruna_core::NodeId,
    object_id: &str,
) -> Response {
    match pinned_url(state, node_id, object_id).await {
        Some(url) => drs_error(
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "node {node_id} serves this object; a redirect cannot carry this credential, \
                 so request {url} with it"
            ),
        ),
        None => DrsError::unavailable().into_response(),
    }
}

/// A temporary, uncached redirect to `node_id`'s pinned download URL, with a ticket when the
/// caller is authenticated. The URL is never logged.
async fn holder_redirect(
    state: &ServerState,
    node_id: aruna_core::NodeId,
    object_id: &str,
    ticket: Option<TicketInput>,
) -> Response {
    let Some(mut url) = pinned_url(state, node_id, object_id).await else {
        return DrsError::unavailable().into_response();
    };
    if let Some(input) = ticket {
        let signed = issue_ticket(&state.get_ctx(), input, now_millis() / 1000);
        let Some(encoded) = signed.and_then(|signed| signed.encode().ok()) else {
            return DrsError::unavailable().into_response();
        };
        url.push_str(&format!("&{TICKET_QUERY}={encoded}"));
    }
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::TEMPORARY_REDIRECT;
    let Ok(location) = http::HeaderValue::from_str(&url) else {
        return DrsError::unavailable().into_response();
    };
    let headers = response.headers_mut();
    headers.insert(http::header::LOCATION, location);
    headers.insert(
        http::header::CACHE_CONTROL,
        http::HeaderValue::from_static("no-store"),
    );
    response
}

/// The datasets whose linked parts contain a local object; `None` when no metadata node
/// could answer, so the object itself is still served.
async fn drs_part_of(
    state: &ServerState,
    auth: Option<AuthContext>,
    bearer: Option<String>,
    resolved: &ResolvedObject,
) -> Option<DrsPartOf> {
    let found = aruna_operations::metadata::api::part_of(
        state.get_ctx().as_ref(),
        state.get_realm_id(),
        state.get_node_id(),
        auth,
        bearer,
        &resolved.bucket,
        &resolved.key,
    )
    .await
    .ok()?;
    Some(DrsPartOf {
        datasets: found
            .datasets
            .into_iter()
            .map(|document| document.graph_iri)
            .collect(),
        more: found.more,
        complete: found.complete,
    })
}

fn build_object_response(base_url: &str, resolved: &ResolvedObject) -> DrsObjectResponse {
    let self_uri = format!(
        "{base_url}/api/v1/ga4gh/drs/v1/objects/{}",
        encode_component(&resolved.requested_id)
    );
    let hash = resolved
        .canonical_w3id
        .strip_prefix(DATA_PREFIX)
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
    // A chosen holder serves at its pinned URL; otherwise the node with the bytes serves.
    let origin = match (&resolved.pinned, &resolved.location) {
        (Some(pinned), _) => Some((pinned.as_str(), "&here=true")),
        (None, Some(_)) => Some((base_url, "")),
        (None, None) => resolved.origin.as_deref().map(|origin| (origin, "")),
    };
    let access_methods = origin
        .into_iter()
        .map(|(origin, pin)| DrsAccessMethod {
            access_id: ACCESS_ID_HTTPS.to_string(),
            kind: "https".to_string(),
            region: None,
            access_url: Some(DrsAccessUrl {
                url: format!(
                    "{origin}/api/v1/ga4gh/drs/v1/download?object_id={}{pin}",
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
        is_part_of: None,
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
            // A local copy the caller may not read leaves distributed copies to try.
            match resolve_content_hash(state, auth, object_id, None, &hash).await? {
                ResolveOutcome::Found(object) => Ok(ResolveOutcome::Found(object)),
                local => {
                    let found = resolve_distributed_hash(state, auth, object_id, hash, deadline);
                    Ok(match (found.await?, local) {
                        (ResolveOutcome::NotFound, local) => local,
                        (ResolveOutcome::Unavailable, ResolveOutcome::Denied) => {
                            ResolveOutcome::Denied
                        }
                        (outcome, _) => outcome,
                    })
                }
            }
        }
        RequestedObjectId::ContentHashArn {
            realm_id,
            node_id,
            hash,
        } => resolve_content_hash(state, auth, object_id, Some((realm_id, node_id)), &hash).await,
        RequestedObjectId::VersionedObject(arn) => {
            resolve_versioned(state, auth, object_id, &arn, deadline).await
        }
        RequestedObjectId::GlobalVersion {
            realm_id,
            version_id,
        } => resolve_global(state, auth, object_id, realm_id, version_id, deadline).await,
    }
}

/// Resolves a bare content hash that no local bucket holds through the content indexes of
/// distributed buckets; the first readable version answers under its global id. An incomplete
/// search without a hit is unavailable, never absent.
async fn resolve_distributed_hash(
    state: &ServerState,
    auth: &AuthContext,
    requested_id: &str,
    hash: [u8; 32],
    deadline: Instant,
) -> Result<ResolveOutcome, DrsError> {
    let resolution = resolve_content(&state.get_ctx(), hash, deadline).await;
    let realm_id = state.get_realm_id();
    let mut denied = false;
    for found in resolution.versions {
        let version_id = found.version_id;
        // One version that cannot be resolved does not end the search for the others.
        match resolve_global(state, auth, requested_id, realm_id, version_id, deadline).await {
            Ok(ResolveOutcome::Found(object)) => return Ok(ResolveOutcome::Found(object)),
            Ok(ResolveOutcome::Denied) => denied = true,
            Ok(ResolveOutcome::NotFound | ResolveOutcome::Unavailable) | Err(_) => {}
        }
    }
    Ok(match (denied, resolution.incomplete) {
        (true, _) => ResolveOutcome::Denied,
        (false, true) => ResolveOutcome::Unavailable,
        (false, false) => ResolveOutcome::NotFound,
    })
}

/// Resolves a distributed bucket's version on any realm node: this node's member answers when
/// it keeps the bytes, otherwise a chosen holder answers for the caller.
async fn resolve_global(
    state: &ServerState,
    auth: &AuthContext,
    requested_id: &str,
    realm_id: aruna_core::structs::identity::realm::RealmId,
    version_id: Ulid,
    deadline: Instant,
) -> Result<ResolveOutcome, DrsError> {
    if realm_id != state.get_realm_id() {
        return Ok(ResolveOutcome::NotFound);
    }
    let context = state.get_ctx();
    let found = match resolve_version(&context, version_id, deadline).await {
        GlobalResolution::Found(found) => found,
        GlobalResolution::NotFound => return Ok(ResolveOutcome::NotFound),
        GlobalResolution::Unavailable => return Ok(ResolveOutcome::Unavailable),
        GlobalResolution::Ambiguous => {
            return Err(DrsError::conflict(
                "more than one distributed bucket holds this version id",
            ));
        }
    };
    let canonical = GlobalId::Version {
        realm_id,
        version_id,
    }
    .to_url();
    let object = ObjectRef {
        bucket: &found.bucket,
        key: &found.key,
        version_id,
        canonical_w3id: &canonical,
    };
    let with_holders = |outcome: ResolveOutcome| match outcome {
        ResolveOutcome::Found(mut resolved) => {
            resolved.holders = found.holders.clone();
            ResolveOutcome::Found(resolved)
        }
        outcome => outcome,
    };
    if found.local {
        // A member without the bytes of the version asks a holder instead.
        match resolve_local(state, auth, requested_id, &object).await? {
            ResolveOutcome::NotFound => {}
            outcome => return Ok(with_holders(outcome)),
        }
    }
    let local = state.get_node_id();
    let mut remote: Vec<_> = found
        .holders
        .iter()
        .copied()
        .filter(|node_id| *node_id != local)
        .collect();
    // A holder of the range may lack this version's bytes, so its miss proves no absence.
    let mut outcome = ResolveOutcome::Unavailable;
    for _ in 0..MAX_HOLDER_TRIES {
        // A spent budget would make a healthy holder look unavailable.
        if Instant::now() >= deadline {
            break;
        }
        let Some(holder) = choose_holder(&context, &remote, now_millis()).await else {
            break;
        };
        remote.retain(|node_id| *node_id != holder.node_id);
        match resolve_at(state, auth, requested_id, holder.node_id, &object, deadline).await? {
            ResolveOutcome::Found(object) => {
                return Ok(with_holders(ResolveOutcome::Found(object)));
            }
            ResolveOutcome::Denied => outcome = ResolveOutcome::Denied,
            // A holder that could not answer is skipped by the next choices for a while.
            ResolveOutcome::Unavailable => avoid_holder(&context, holder.node_id, now_millis()),
            ResolveOutcome::NotFound => {}
        }
    }
    Ok(outcome)
}

/// One exact object version a resolution names.
struct ObjectRef<'a> {
    bucket: &'a str,
    key: &'a str,
    version_id: Ulid,
    canonical_w3id: &'a str,
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
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
    let canonical = arn.to_w3id();
    let object = ObjectRef {
        bucket: &arn.bucket,
        key: &arn.key,
        version_id: arn.version,
        canonical_w3id: &canonical,
    };
    resolve_at(state, auth, requested_id, arn.node_id, &object, deadline).await
}

/// Asks `node_id` about one version for the caller; only its answer establishes absence.
async fn resolve_at(
    state: &ServerState,
    auth: &AuthContext,
    requested_id: &str,
    node_id: aruna_core::NodeId,
    object: &ObjectRef<'_>,
    deadline: Instant,
) -> Result<ResolveOutcome, DrsError> {
    let summary = drive_until(
        RemoteLocationOperation::new(
            node_id,
            LocationSummaryRequest {
                realm_id: state.get_realm_id(),
                bucket: object.bucket.to_string(),
                key: object.key.to_string(),
                version_id: Some(object.version_id),
                auth_context: auth.clone(),
            },
        ),
        &state.get_ctx(),
        deadline,
    )
    .await;
    let summary = match summary {
        Ok(summary) => summary,
        Err(LocationSummaryError::Denied) => return Ok(ResolveOutcome::Denied),
        Err(LocationSummaryError::BucketNotFound) => return Ok(ResolveOutcome::NotFound),
        Err(_) => return Ok(ResolveOutcome::Unavailable),
    };
    if summary.version_id != Some(object.version_id) || !summary.materialized {
        return Ok(ResolveOutcome::NotFound);
    }
    let (Some(group_id), Some(size)) = (summary.group_id, summary.blob_size) else {
        return Ok(ResolveOutcome::Unavailable);
    };
    let Some(origin) = owner_origin(state, node_id).await? else {
        return Ok(ResolveOutcome::Unavailable);
    };
    Ok(ResolveOutcome::Found(ResolvedObject {
        bucket: object.bucket.to_string(),
        key: object.key.to_string(),
        group_id,
        version_id: object.version_id,
        canonical_w3id: object.canonical_w3id.to_string(),
        requested_id: requested_id.to_string(),
        size,
        hashes: summary.hashes.into_iter().collect(),
        source_metadata: None,
        location: None,
        origin: Some(origin),
        holders: Vec::new(),
        pinned: None,
        answered_by: Some(node_id),
    }))
}

/// The owner's published base URL, or `None` when it has not published one.
async fn owner_origin(
    state: &ServerState,
    node_id: aruna_core::NodeId,
) -> Result<Option<String>, DrsError> {
    let documents = read_info_documents(&state.get_ctx(), &[node_id])
        .await
        .map_err(DrsError::internal)?;
    let Some(published) = documents.get(&node_id).and_then(|doc| doc.urls.api.clone()) else {
        return Ok(None);
    };
    // The published url is absolute, so no bind address is used.
    let unused = std::net::SocketAddr::from(([0, 0, 0, 0], 0));
    Ok(Some(
        RestInterfaceRuntime::from_bind_address(unused, Some(&published)).base_url,
    ))
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
    // A member bucket of a distributed bucket is named by the global id, never by a node ARN.
    let member = drive(
        GetBucketOperation::new(arn.bucket.clone()),
        &state.get_ctx(),
    )
    .await
    .is_ok_and(|info| info.distribution.is_some());
    let canonical = match member {
        true => GlobalId::Version {
            realm_id: arn.realm_id,
            version_id: arn.version,
        }
        .to_url(),
        false => arn.to_w3id(),
    };
    let object = ObjectRef {
        bucket: &arn.bucket,
        key: &arn.key,
        version_id: arn.version,
        canonical_w3id: &canonical,
    };
    match resolve_local(state, auth, requested_id, &object).await? {
        // Only this node's current binding was asked, which proves no absence.
        ResolveOutcome::NotFound if member => Err(DrsError::conflict(
            "not resolvable as a node ARN of a distributed bucket; use the global id",
        )),
        outcome => Ok(outcome),
    }
}

/// Resolves one version of a bucket on this node, checked on this node's own path.
async fn resolve_local(
    state: &ServerState,
    auth: &AuthContext,
    requested_id: &str,
    object: &ObjectRef<'_>,
) -> Result<ResolveOutcome, DrsError> {
    let bucket_info = match drive(
        GetBucketOperation::new(object.bucket.to_string()),
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
            bucket: object.bucket.to_string(),
            key: object.key.to_string(),
            version_id: Some(object.version_id),
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
        state.get_realm_id(),
        bucket_info.group_id,
        state.get_node_id(),
        object.bucket,
        object.key,
    );
    if !can_read_path(state, auth, &path).await? {
        return Ok(ResolveOutcome::Denied);
    }

    Ok(ResolveOutcome::Found(ResolvedObject {
        bucket: object.bucket.to_string(),
        key: object.key.to_string(),
        group_id: bucket_info.group_id,
        version_id: object.version_id,
        canonical_w3id: object.canonical_w3id.to_string(),
        requested_id: requested_id.to_string(),
        size: location.blob_size,
        hashes: location.hashes.clone().into_iter().collect(),
        source_metadata: head.source_metadata,
        location: Some(location),
        origin: None,
        holders: Vec::new(),
        pinned: None,
        answered_by: None,
    }))
}

async fn resolve_content_hash(
    state: &ServerState,
    auth: &AuthContext,
    requested_id: &str,
    requested_scope: Option<(
        aruna_core::structs::identity::realm::RealmId,
        aruna_core::NodeId,
    )>,
    hash: &[u8; 32],
) -> Result<ResolveOutcome, DrsError> {
    if let Some((realm_id, node_id)) = requested_scope
        && (realm_id != state.get_realm_id() || node_id != state.get_node_id())
    {
        return Ok(ResolveOutcome::NotFound);
    }

    let mappings = match drive(ResolvePathsOperation::new(*hash), &state.get_ctx()).await {
        Ok(mappings) => mappings,
        Err(ResolvePathsError::TooManyAliases) => {
            return Err(DrsError::conflict(
                "the content hash names more objects than one lookup reads; it is ambiguous, use the global id or a versioned ARN",
            ));
        }
        Err(error) => return Err(DrsError::internal(error.to_string())),
    };
    debug!(?mappings);

    let mut any_local_mapping = false;
    let mut last_permission_check: Option<(String, bool)> = None;

    for mapping in mappings {
        if mapping.realm_id != state.get_realm_id() || mapping.node_id != state.get_node_id() {
            debug!("Realm id or node id mismatch");
            continue;
        }
        any_local_mapping = true;
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
            canonical_w3id: format!("{DATA_PREFIX}{}", hex::encode(hash)),
            requested_id: requested_id.to_string(),
            size: location.blob_size,
            hashes: location.hashes.clone().into_iter().collect(),
            source_metadata: head.source_metadata,
            location: Some(location),
            origin: None,
            holders: Vec::new(),
            pinned: None,
            answered_by: None,
        }));
    }

    if any_local_mapping {
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
    if object_id.starts_with(DATA_PREFIX) {
        return match W3idIdentifier::parse(object_id)
            .map_err(|error| DrsError::bad_request(error.to_string()))?
        {
            W3idIdentifier::ContentHash(hash) => Ok(RequestedObjectId::CanonicalW3id(hash)),
            W3idIdentifier::VersionedObject(arn) => Ok(RequestedObjectId::VersionedObject(arn)),
            W3idIdentifier::Global(GlobalId::Set { .. }) => Err(DrsError::bad_request(
                "bucket and prefix ids name no object; use a version id or a content hash",
            )),
            W3idIdentifier::Global(GlobalId::Version {
                realm_id,
                version_id,
            }) => Ok(RequestedObjectId::GlobalVersion {
                realm_id,
                version_id,
            }),
        };
    }
    // The short path form of a global id.
    if object_id.starts_with('/') {
        return match GlobalId::parse(object_id)
            .map_err(|error| DrsError::bad_request(error.to_string()))?
        {
            GlobalId::Version {
                realm_id,
                version_id,
            } => Ok(RequestedObjectId::GlobalVersion {
                realm_id,
                version_id,
            }),
            GlobalId::Set { .. } => Err(DrsError::bad_request(
                "bucket and prefix ids name no object; use a version id or a content hash",
            )),
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

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
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
