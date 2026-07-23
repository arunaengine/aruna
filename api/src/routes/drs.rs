use crate::auth::{ensure_permission, require_realm_auth};
use crate::error::ServerError;
use crate::server_state::ServerState;
use aruna_core::structs::{
    ArunaArn, ArunaArnType, AuthContext, BackendLocation, Permission, SourceMetadata,
    VersionedObjectArn, W3idDataIdentifier, blob_object_permission_path,
};
use aruna_operations::blob::resolve_blob_permission_paths::ResolveBlobPermissionPathsOperation;
use aruna_operations::driver::drive;
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use aruna_operations::s3::get_object::{GetObjectError, GetObjectInput, GetObjectOperation};
use aruna_operations::s3::head_object::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, options, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, warn};
use ulid::Ulid;
use url::form_urlencoded::byte_serialize;
use utoipa::{OpenApi, ToSchema};

const W3ID_DATA_PREFIX: &str = "https://w3id.org/aruna/data/";
const ACCESS_ID_HTTPS: &str = "https";

#[derive(OpenApi)]
#[openapi(
    tags((name = "drs", description = "GA4GH DRS content access")),
    paths(get_service_info, get_object, post_objects, download_object, get_authorizations),
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

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/ga4gh/drs/v1/service-info", get(get_service_info))
        .route("/ga4gh/drs/v1/objects", post(post_objects))
        .route(
            "/ga4gh/drs/v1/objects/{*object_id}",
            options(get_authorizations),
        )
        .route("/ga4gh/drs/v1/objects/{*object_id}", get(get_object))
        .route("/ga4gh/drs/v1/download", get(download_object))
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
    location: BackendLocation,
    source_metadata: Option<SourceMetadata>,
}

#[allow(clippy::large_enum_variant)]
enum ResolveOutcome {
    Found(ResolvedObject),
    Denied,
    NotFound,
}

#[utoipa::path(
    get,
    path = "/ga4gh/drs/v1/service-info",
    tag = "drs",
    responses((status = 200, body = DrsServiceInfoResponse))
)]
pub async fn get_service_info(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> (StatusCode, Json<DrsServiceInfoResponse>) {
    let base_url = external_base_url(&headers);
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
    params(("object_id" = String, Path, description = "Aruna data W3ID, content-hash ch ARN, or versioned s3 ARN locator")),
    responses(
        (status = 200, body = DrsAuthorizationsResponse),
        (status = 400, body = DrsErrorPayload),
        (status = 404, body = DrsErrorPayload)
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
    params(("object_id" = String, Path, description = "Aruna data W3ID, content-hash ch ARN, or versioned s3 ARN locator")),
    responses(
        (status = 200, body = DrsObjectResponse),
        (status = 400, body = DrsErrorPayload),
        (status = 403, body = DrsErrorPayload),
        (status = 404, body = DrsErrorPayload)
    ),
    security((),("bearer_auth" = []))
)]
pub async fn get_object(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    headers: HeaderMap,
    Path(object_id): Path<String>,
) -> Response {
    let anonymous = auth.is_none();
    let auth = match drs_auth_or_anonymous(state.as_ref(), auth) {
        Ok(auth) => auth,
        Err(error) => return error.into_response(),
    };

    match resolve_object(state.as_ref(), &auth, &object_id).await {
        Ok(ResolveOutcome::Found(resolved)) => {
            drs_json_response(StatusCode::OK, build_object_response(&headers, &resolved))
        }
        Ok(ResolveOutcome::Denied) => drs_denied_error(anonymous).into_response(),
        Ok(ResolveOutcome::NotFound) => DrsError::not_found("DRS object not found").into_response(),
        Err(error) => error.into_response(),
    }
}

#[utoipa::path(
    post,
    path = "/ga4gh/drs/v1/objects",
    tag = "drs",
    request_body = DrsBulkObjectsRequestBody,
    responses(
        (status = 200, body = DrsBulkObjectsResponse),
        (status = 400, body = DrsErrorPayload)
    )
)]
pub async fn post_objects(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    headers: HeaderMap,
    Json(body): Json<DrsBulkObjectsRequestBody>,
) -> Response {
    let anonymous = auth.is_none();
    let auth = match drs_auth_or_anonymous(state.as_ref(), auth) {
        Ok(auth) => auth,
        Err(error) => return error.into_response(),
    };

    let mut objects = Vec::with_capacity(body.object_ids.len());
    for object_id in body.object_ids {
        let result = match resolve_object(state.as_ref(), &auth, &object_id).await {
            Ok(ResolveOutcome::Found(resolved)) => serde_json::to_value(build_object_response(
                &headers, &resolved,
            ))
            .unwrap_or_else(|_| json!({ "status_code": 500, "msg": "serialization failed" })),
            Ok(ResolveOutcome::Denied) => {
                let error = drs_denied_error(anonymous);
                json!({ "status_code": error.status.as_u16(), "msg": error.message })
            }
            Ok(ResolveOutcome::NotFound) => {
                json!({ "status_code": 404, "msg": "DRS object not found" })
            }
            Err(error) => json!({ "status_code": error.status.as_u16(), "msg": error.message }),
        };
        objects.push(DrsBulkObjectItem { object_id, result });
    }
    drs_json_response(StatusCode::OK, DrsBulkObjectsResponse { objects })
}

#[utoipa::path(
    get,
    path = "/ga4gh/drs/v1/download",
    tag = "drs",
    params(("object_id" = String, Query, description = "Aruna data W3ID, content-hash ch ARN, or versioned s3 ARN locator")),
    responses(
        (status = 200, description = "Object bytes"),
        (status = 400, body = DrsErrorPayload),
        (status = 404, body = DrsErrorPayload)
    )
)]
pub async fn download_object(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
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
        Err(error) => return error.into_response(),
    };

    let result = match drive(
        GetObjectOperation::new(GetObjectInput {
            bucket: resolved.bucket.clone(),
            key: resolved.key.clone(),
            version_id: Some(resolved.version_id),
            range: None,
            group_id: resolved.group_id,
            user_identity: auth.user_id,
        }),
        &state.get_ctx(),
    )
    .await
    {
        Ok(Some(Ok(result))) => result,
        Ok(Some(Err(
            GetObjectError::NoSuchKey
            | GetObjectError::NoSuchVersion
            | GetObjectError::DeleteMarker,
        )))
        | Err(
            GetObjectError::NoSuchKey
            | GetObjectError::NoSuchVersion
            | GetObjectError::DeleteMarker,
        )
        | Ok(None) => {
            return drs_error(StatusCode::NOT_FOUND, "DRS object not found");
        }
        Ok(Some(Err(error))) | Err(error) => {
            return DrsError::internal(error.to_string()).into_response();
        }
    };

    let location = result.location.unwrap_or_else(|| resolved.location.clone());

    let mut response = Response::new(Body::from_stream(result.blob));
    *response.status_mut() = StatusCode::OK;
    if let Ok(value) = http::HeaderValue::from_str(&location.blob_size.to_string()) {
        response
            .headers_mut()
            .insert(http::header::CONTENT_LENGTH, value);
    }
    response
}

fn build_object_response(headers: &HeaderMap, resolved: &ResolvedObject) -> DrsObjectResponse {
    let self_uri = format!(
        "{}/api/v1/ga4gh/drs/v1/objects/{}",
        external_base_url(headers),
        encode_component(&resolved.requested_id)
    );
    let hash = resolved
        .canonical_w3id
        .strip_prefix(W3ID_DATA_PREFIX)
        .unwrap_or_default();
    let name = format!("content-{}", &hash[..hash.len().min(12)]);
    let checksums = resolved
        .location
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
    let access_methods = vec![DrsAccessMethod {
        access_id: ACCESS_ID_HTTPS.to_string(),
        kind: "https".to_string(),
        region: None,
        access_url: Some(DrsAccessUrl {
            url: format!(
                "{}/api/v1/ga4gh/drs/v1/download?object_id={}",
                external_base_url(headers),
                encode_component(&resolved.requested_id)
            ),
            headers: HashMap::new(),
        }),
    }];

    DrsObjectResponse {
        id: resolved.requested_id.clone(),
        self_uri,
        name,
        description: None,
        size: Some(resolved.location.blob_size),
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

async fn resolve_versioned(
    state: &ServerState,
    auth: &AuthContext,
    requested_id: &str,
    arn: &VersionedObjectArn,
) -> Result<ResolveOutcome, DrsError> {
    if arn.realm_id != state.get_realm_id() || arn.node_id != state.get_node_id() {
        return Ok(ResolveOutcome::NotFound);
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
        location,
        source_metadata: head.source_metadata,
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
            location,
            source_metadata: head.source_metadata,
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

fn external_base_url(headers: &HeaderMap) -> String {
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("http");
    let host = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get(http::header::HOST))
        .and_then(|value| value.to_str().ok())
        .unwrap_or("localhost");
    format!("{scheme}://{host}")
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
}

impl IntoResponse for DrsError {
    fn into_response(self) -> Response {
        drs_error(self.status, self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        RequestedObjectId, ResolveOutcome, ResolvedObject, W3ID_DATA_PREFIX, build_object_response,
        drs_denied_error, encode_component, get_object, parse_requested_object_id, resolve_object,
    };
    use crate::openapi::ApiDoc;
    use crate::server_state::ServerState;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE,
    };
    use aruna_core::structs::{
        Actor, AuthContext, BackendLocation, BlobVersion, BucketInfo, GroupAuthorizationDocument,
        NodeCapabilities, RealmAuthorizationDocument, RealmId, SourceMetadata, VersionKey,
        VersionedObjectArn,
    };
    use aruna_core::{NodeId, UserId};
    use aruna_operations::driver::DriverContext;
    use aruna_storage::storage::FjallStorage;
    use axum::Extension;
    use axum::body::to_bytes;
    use axum::extract::{Path, State};
    use axum::http::{HeaderMap, HeaderValue, StatusCode};
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::SystemTime;
    use tempfile::TempDir;
    use ulid::Ulid;

    fn forwarded_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        headers.insert(
            "x-forwarded-host",
            HeaderValue::from_static("drs.example.test"),
        );
        headers
    }

    fn materialized_location(blake3: [u8; 32]) -> BackendLocation {
        let mut hashes = HashMap::new();
        hashes.insert("blake3".to_string(), blake3.to_vec());
        hashes.insert("sha256".to_string(), vec![0xabu8; 32]);
        BackendLocation {
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
            BlobVersion::materialized(hash, SystemTime::UNIX_EPOCH, owner, None)
                .to_bytes()
                .expect("version serializes"),
        )
        .await;
        write_fixture(
            state,
            BLOB_LOCATIONS_KEYSPACE,
            hash.to_vec(),
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
        let arns = [
            VersionedObjectArn::new(
                state.get_realm_id(),
                other_node,
                "mybucket",
                "path/file.txt",
                version,
            )
            .unwrap(),
            VersionedObjectArn::new(
                other_realm,
                state.get_node_id(),
                "mybucket",
                "path/file.txt",
                version,
            )
            .unwrap(),
        ];

        for arn in arns {
            let response = get_object(
                State(state.clone()),
                Extension(Some(auth.clone())),
                HeaderMap::new(),
                Path(arn.to_string()),
            )
            .await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("response body");
            let payload: serde_json::Value =
                serde_json::from_slice(&body).expect("typed error body");
            assert_eq!(payload["status_code"], 404);
            assert_eq!(payload["msg"], "DRS object not found");
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
            location: materialized_location(blake3),
            source_metadata: Some(SourceMetadata {
                content_length: 42,
                content_type: Some("application/octet-stream".to_string()),
                etag: Some("etag-from-materialized".to_string()),
                last_modified: None,
                source_version: None,
            }),
        };

        let response = build_object_response(&forwarded_headers(), &resolved);

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
            location: materialized_location(blake3),
            source_metadata: Some(SourceMetadata {
                content_length: 42,
                content_type: Some("application/octet-stream".to_string()),
                etag: Some("etag-from-materialized".to_string()),
                last_modified: None,
                source_version: None,
            }),
        };

        let response = build_object_response(&forwarded_headers(), &resolved);

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
