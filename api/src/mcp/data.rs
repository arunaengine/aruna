use super::context::member_groups;
use super::{
    McpServer, authorize_tool, bad_request, empty_extras, internal_error, request_auth,
    server_error, tool_extras,
};
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::{
    AuthContext, BucketInfo, OBJECT_CONTENT_TYPE_KEY, Permission, blob_bucket_permission_path,
    blob_object_permission_path,
};
use aruna_operations::driver::{bucket_snapshot, drive, gate_context, now_ms};
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use aruna_operations::s3::get_object::{
    GetObjectError, GetObjectInput, ObjectRangeRequest, get_object_routed,
};
use aruna_operations::s3::list_buckets::{ListBucketsInput, ListBucketsOperation};
use aruna_operations::s3::list_objects_v2::{
    ListObjectsV2ContinuationToken, ListObjectsV2Input, ListObjectsV2Operation,
};
use aruna_operations::s3::put_object::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation,
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use rmcp::Json;
use rmcp::handler::server::tool::Extension;
use rmcp::model::CallToolResult;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const MAX_TEXT_BYTES: usize = 1024 * 1024;

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct BucketOutput {
    pub bucket: String,
    pub group_id: String,
    pub created_at: String,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct BucketsOutput {
    pub buckets: Vec<BucketOutput>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ListObjectsInput {
    pub bucket: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct ObjectOutput {
    pub key: String,
    pub size: Option<u64>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_type: Option<String>,
    pub referenced: bool,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct ObjectsOutput {
    pub bucket: String,
    pub objects: Vec<ObjectOutput>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ReadObjectInput {
    pub bucket: String,
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<usize>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct ReadObjectOutput {
    pub bucket: String,
    pub key: String,
    pub offset: u64,
    pub bytes: usize,
    pub content_type: String,
    pub truncated: bool,
    pub text: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct WriteObjectInput {
    pub bucket: String,
    pub key: String,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct WriteObjectOutput {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub size: u64,
    pub content_type: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SearchKind {
    Documents,
    Buckets,
    Groups,
    Users,
}

impl SearchKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Documents => "documents",
            Self::Buckets => "buckets",
            Self::Groups => "groups",
            Self::Users => "users",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct SearchInput {
    pub q: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<SearchKind>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct SearchOutput {
    pub results: serde_json::Value,
}

pub(crate) fn toolset() -> rmcp::handler::server::router::tool::ToolRouter<McpServer> {
    McpServer::data_router()
}

#[tool_router(router = data_router)]
impl McpServer {
    #[tool(
        description = "List data buckets readable by the authenticated user on this node",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn list_buckets(
        &self,
        Extension(parts): Extension<http::request::Parts>,
    ) -> Result<Json<BucketsOutput>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let mut buckets = Vec::new();
        for group in member_groups(self, &auth).await? {
            let result = drive(
                ListBucketsOperation::new(ListBucketsInput {
                    group_id: group.group_id,
                    prefix: None,
                    continuation_token: None,
                    max_buckets: None,
                }),
                &self.state.get_ctx(),
            )
            .await
            .and_then(|result| result.transpose())
            .map_err(internal_error)?
            .ok_or_else(|| internal_error("bucket listing did not finish"))?;
            for (bucket, info) in result.buckets {
                authorize_tool(
                    &self.state,
                    &auth,
                    blob_bucket_permission_path(
                        self.state.get_realm_id(),
                        info.group_id,
                        self.state.get_node_id(),
                        &bucket,
                    ),
                    Permission::READ,
                    empty_extras("list_buckets"),
                )
                .await
                .map_err(server_error)?;
                buckets.push(BucketOutput {
                    bucket,
                    group_id: info.group_id.to_string(),
                    created_at: chrono::DateTime::<chrono::Utc>::from(info.created_at).to_rfc3339(),
                });
            }
        }
        buckets.sort_by(|left, right| left.bucket.cmp(&right.bucket));
        Ok(Json(BucketsOutput { buckets }))
    }

    #[tool(
        description = "List up to 200 objects in an Aruna bucket with optional prefix and cursor",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn list_objects(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<ListObjectsInput>,
    ) -> Result<Json<ObjectsOutput>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let extras = tool_extras("list_objects", &input)?;
        let bucket_info = self.bucket_info(&input.bucket).await?;
        authorize_tool(
            &self.state,
            &auth,
            blob_bucket_permission_path(
                self.state.get_realm_id(),
                bucket_info.group_id,
                self.state.get_node_id(),
                &input.bucket,
            ),
            Permission::READ,
            extras,
        )
        .await
        .map_err(server_error)?;
        let cursor = input.cursor.as_deref().map(decode_cursor).transpose()?;
        let limit = input.limit.unwrap_or(100).clamp(1, 200);
        let result = drive(
            ListObjectsV2Operation::new(ListObjectsV2Input {
                bucket: input.bucket.clone(),
                group_id: bucket_info.group_id,
                continuation_token: cursor,
                max_keys: Some(limit),
                prefix: input.prefix,
                delimiter: None,
                start_after: None,
            }),
            &self.state.get_ctx(),
        )
        .await
        .and_then(|result| result.transpose())
        .map_err(internal_error)?
        .ok_or_else(|| internal_error("object listing did not finish"))?;
        let objects = result
            .objects
            .into_iter()
            .map(|object| {
                let size = object
                    .location
                    .as_ref()
                    .map(|location| location.blob_size)
                    .or_else(|| {
                        object
                            .source_metadata
                            .as_ref()
                            .map(|metadata| metadata.content_length)
                    });
                let etag = object
                    .location
                    .as_ref()
                    .and_then(|location| location.hashes.get(HASH_MD5))
                    .map(hex::encode)
                    .or_else(|| {
                        object
                            .source_metadata
                            .as_ref()
                            .and_then(|metadata| metadata.etag.clone())
                    });
                let content_type = object
                    .source_metadata
                    .as_ref()
                    .and_then(|metadata| metadata.content_type.clone());
                let last_modified = object
                    .version_created_at
                    .or(object.last_refresh)
                    .or_else(|| {
                        object
                            .source_metadata
                            .as_ref()
                            .and_then(|metadata| metadata.last_modified)
                    })
                    .map(|time| chrono::DateTime::<chrono::Utc>::from(time).to_rfc3339());
                ObjectOutput {
                    key: object.head.key,
                    size,
                    etag,
                    last_modified,
                    content_type,
                    referenced: object.referenced,
                }
            })
            .collect();
        let next_cursor = result
            .continuation_token
            .as_ref()
            .map(encode_cursor)
            .transpose()?;
        Ok(Json(ObjectsOutput {
            bucket: input.bucket,
            objects,
            next_cursor,
        }))
    }

    #[tool(
        description = "Read a bounded UTF-8 text range from an Aruna object",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn read_object(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<ReadObjectInput>,
    ) -> Result<Json<ReadObjectOutput>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let extras = tool_extras("read_object", &input)?;
        Ok(Json(read_text(self, &auth, input, extras).await?))
    }

    #[tool(
        description = "Write at most 1 MiB of UTF-8 text to an Aruna object",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn write_object(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<WriteObjectInput>,
    ) -> Result<Json<WriteObjectOutput>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let extras = tool_extras("write_object", &input)?;
        Ok(Json(write_text(self, &auth, input, extras).await?))
    }

    #[tool(
        description = "Search Aruna documents, buckets, groups, and users through the unified search service",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn search(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<SearchInput>,
    ) -> Result<Json<SearchOutput>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let extras = tool_extras("search", &input)?;
        authorize_search(self, &auth, extras).await?;
        let bearer = parts
            .extensions
            .get::<Option<crate::auth::ValidatedArunaBearerTokenCarrier>>()
            .cloned()
            .flatten()
            .map(|carrier| carrier.as_str().to_string());
        let response = crate::routes::search::run_unified(
            &self.state,
            &auth,
            bearer,
            crate::routes::search::SearchParams {
                q: input.q,
                types: input.kind.map(|kind| kind.as_str().to_string()),
                limit: None,
                cursor: None,
                group_id: None,
                conforms_to: None,
                mode: None,
            },
        )
        .await
        .map_err(server_error)?;
        let results = serde_json::to_value(response).map_err(internal_error)?;
        Ok(Json(SearchOutput { results }))
    }

    async fn bucket_info(&self, bucket: &str) -> Result<BucketInfo, CallToolResult> {
        drive(
            GetBucketInfoOperation::new(bucket.to_string()),
            &self.state.get_ctx(),
        )
        .await
        .and_then(|result| result.transpose())
        .map_err(map_bucket_error)?
        .ok_or_else(|| internal_error("bucket lookup did not finish"))
    }
}

pub(crate) async fn read_text(
    server: &McpServer,
    auth: &AuthContext,
    input: ReadObjectInput,
    extras: aruna_operations::request_policy::PolicyRequestExtras,
) -> Result<ReadObjectOutput, CallToolResult> {
    crate::s3::util::validate_object_key(&input.key)
        .map_err(|error| bad_request(error.to_string()))?;
    let max_bytes = bounded_bytes(input.max_bytes)?;
    let offset = input.offset.unwrap_or(0);
    let bucket_info = server.bucket_info(&input.bucket).await?;
    authorize_tool(
        &server.state,
        auth,
        blob_object_permission_path(
            server.state.get_realm_id(),
            bucket_info.group_id,
            server.state.get_node_id(),
            &input.bucket,
            &input.key,
        ),
        Permission::READ,
        extras,
    )
    .await
    .map_err(server_error)?;
    let range = if offset == 0 {
        None
    } else {
        Some(ObjectRangeRequest::StartEnd {
            start: offset,
            end: offset
                .checked_add(max_bytes as u64)
                .ok_or_else(|| bad_request("offset and max_bytes overflow"))?,
        })
    };
    let mut result = get_object_routed(
        &server.state.get_ctx(),
        GetObjectInput {
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            version_id: None,
            range,
            group_id: bucket_info.group_id,
            user_identity: auth.user_id,
            node_id: server.state.get_node_id(),
        },
        auth.path_restrictions.clone(),
    )
    .await
    .and_then(|result| result.transpose())
    .map_err(map_get_error)?
    .ok_or_else(|| internal_error("object read did not finish"))?;
    let content_type = result
        .metadata
        .remove(OBJECT_CONTENT_TYPE_KEY)
        .or_else(|| {
            result
                .source_metadata
                .as_ref()
                .and_then(|metadata| metadata.content_type.clone())
        })
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let mut bytes = Vec::with_capacity(max_bytes.saturating_add(1));
    while let Some(chunk) = result.blob.next().await {
        let chunk = chunk.map_err(internal_error)?;
        let remaining = max_bytes.saturating_add(1).saturating_sub(bytes.len());
        bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
        if bytes.len() > max_bytes {
            break;
        }
    }
    let truncated = bytes.len() > max_bytes;
    bytes.truncate(max_bytes);
    let byte_count = bytes.len();
    let text = String::from_utf8(bytes).map_err(|_| {
        bad_request(format!(
            "object is not UTF-8 text; content type is {content_type}"
        ))
    })?;
    Ok(ReadObjectOutput {
        bucket: input.bucket,
        key: input.key,
        offset,
        bytes: byte_count,
        content_type,
        truncated,
        text,
    })
}

pub(crate) async fn write_text(
    server: &McpServer,
    auth: &AuthContext,
    input: WriteObjectInput,
    extras: aruna_operations::request_policy::PolicyRequestExtras,
) -> Result<WriteObjectOutput, CallToolResult> {
    crate::s3::util::validate_object_key(&input.key)
        .map_err(|error| bad_request(error.to_string()))?;
    let size = input.text.len();
    if size > MAX_TEXT_BYTES {
        return Err(server_error(crate::error::ServerError::PayloadTooLarge(
            format!("text exceeds {MAX_TEXT_BYTES} bytes"),
        )));
    }
    let bucket_info = server.bucket_info(&input.bucket).await?;
    authorize_tool(
        &server.state,
        auth,
        blob_object_permission_path(
            server.state.get_realm_id(),
            bucket_info.group_id,
            server.state.get_node_id(),
            &input.bucket,
            &input.key,
        ),
        Permission::WRITE,
        extras,
    )
    .await
    .map_err(server_error)?;
    let content_type = input
        .content_type
        .clone()
        .unwrap_or_else(|| "text/plain; charset=utf-8".to_string());
    let realm = drive(
        GetRealmConfigOperation::new(server.state.get_realm_id()),
        &server.state.get_ctx(),
    )
    .await
    .map_err(internal_error)?;
    let quota_ceiling = realm.quota.effective_group_ceiling(&bucket_info.group_id);
    let routing = bucket_snapshot(&server.state.get_ctx(), &bucket_info)
        .await
        .map_err(internal_error)?;
    let gate = gate_context(
        &server.state.get_ctx(),
        server.state.get_realm_id(),
        now_ms(),
    )
    .await
    .map_err(internal_error)?;
    let body = BackendStream::new(stream::iter([Ok::<Bytes, std::io::Error>(Bytes::from(
        input.text.into_bytes(),
    ))]));
    let mut operation = PutObjectOperation::new(PutObjectConfig {
        user_id: auth.user_id,
        group_id: bucket_info.group_id,
        realm_id: server.state.get_realm_id(),
        node_id: server.state.get_node_id(),
        request: PutObjectInput {
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            content_length: Some(size as u64),
            body: Some(body),
        },
        expected_checksums: Vec::new(),
        checksum_type: None,
        exists: false,
        version_source: None,
        preassigned_version_id: None,
        quota_ceiling,
        routing,
    })
    .with_rocrate_limits(server.state.rocrate_limits().clone())
    .with_metadata(HashMap::from([(
        OBJECT_CONTENT_TYPE_KEY.to_string(),
        content_type.clone(),
    )]));
    if let Some(gate) = gate {
        operation = operation.with_gate(gate);
    }
    let result = drive(operation, &server.state.get_ctx())
        .await
        .and_then(|result| result.transpose())
        .map_err(map_put_error)?
        .ok_or_else(|| internal_error("object write did not finish"))?;
    crate::s3::s3_service::ArunaS3Service::new(
        server.state.get_ctx(),
        server.state.get_realm_id(),
        server.state.get_node_id(),
    )
    .await
    .complete_put(
        auth.clone(),
        bucket_info.group_id,
        input.bucket.clone(),
        input.key.clone(),
        result.version_id,
        result.location.blob_size,
    )
    .await;
    Ok(WriteObjectOutput {
        bucket: input.bucket,
        key: input.key,
        version_id: result.version_id.to_string(),
        size: result.location.blob_size,
        content_type,
    })
}

fn bounded_bytes(max_bytes: Option<usize>) -> Result<usize, CallToolResult> {
    let max_bytes = max_bytes.unwrap_or(MAX_TEXT_BYTES);
    if !(1..=MAX_TEXT_BYTES).contains(&max_bytes) {
        return Err(bad_request(format!(
            "max_bytes must be between 1 and {MAX_TEXT_BYTES}"
        )));
    }
    Ok(max_bytes)
}

fn decode_cursor(cursor: &str) -> Result<ListObjectsV2ContinuationToken, CallToolResult> {
    let bytes = STANDARD
        .decode(cursor)
        .map_err(|_| bad_request("invalid object cursor"))?;
    ListObjectsV2ContinuationToken::from_bytes(&bytes)
        .map_err(|_| bad_request("invalid object cursor"))
}

fn encode_cursor(cursor: &ListObjectsV2ContinuationToken) -> Result<String, CallToolResult> {
    cursor
        .to_bytes()
        .map(|bytes| STANDARD.encode(bytes))
        .map_err(internal_error)
}

/// Search visibility is decided per hit by the shared REST path; the gate here
/// is the realm policy layer only, as for the REST route.
async fn authorize_search(
    server: &McpServer,
    auth: &AuthContext,
    extras: aruna_operations::request_policy::PolicyRequestExtras,
) -> Result<(), CallToolResult> {
    super::authorize_self(server.state.as_ref(), auth, Permission::READ, extras)
        .await
        .map_err(server_error)
}

fn map_bucket_error(error: GetBucketInfoError) -> CallToolResult {
    match error {
        GetBucketInfoError::NotFound => server_error(crate::error::ServerError::NotFound),
        GetBucketInfoError::StorageError(error) => internal_error(error),
        GetBucketInfoError::ConversionError(error) => internal_error(error),
        GetBucketInfoError::InvalidStateEvent {
            state,
            expected,
            received,
        } => internal_error(format!(
            "unexpected bucket lookup event in {state:?}: expected {expected}, got {received:?}"
        )),
        GetBucketInfoError::GetBucketInfoFailed => internal_error("bucket lookup failed"),
    }
}

fn map_get_error(error: GetObjectError) -> CallToolResult {
    match error {
        GetObjectError::NoSuchKey
        | GetObjectError::NoSuchVersion
        | GetObjectError::DeleteMarker
        | GetObjectError::HistoricalReferenceUnavailable => {
            server_error(crate::error::ServerError::NotFound)
        }
        error @ GetObjectError::InvalidRange => bad_request(error),
        error @ (GetObjectError::ReferenceSourceChanged
        | GetObjectError::ReferenceAdvanceExhausted) => {
            server_error(crate::error::ServerError::Conflict(error.to_string()))
        }
        GetObjectError::GovernedUnavailable => server_error(crate::error::ServerError::Forbidden),
        GetObjectError::StorageError(error) => internal_error(error),
        GetObjectError::ConversionError(error) => internal_error(error),
        GetObjectError::InvalidState { current, expected } => internal_error(format!(
            "invalid object read state {current:?}; expected {expected:?}"
        )),
        GetObjectError::InvalidStateEvent {
            state,
            expected,
            received,
        } => internal_error(format!(
            "unexpected object read event in {state:?}: expected {expected}, got {received:?}"
        )),
        GetObjectError::NoTransactionFound => internal_error("object read transaction is missing"),
        GetObjectError::UsageError(error) => internal_error(error),
        GetObjectError::ResolveReferenceError(error) => internal_error(error),
        GetObjectError::StagingSourceError(error) => internal_error(error),
        GetObjectError::ManagedCopyError(error) => internal_error(error),
        GetObjectError::PolicyError(error) => internal_error(error),
        error @ GetObjectError::BlobNotLocal { .. } => internal_error(error),
        GetObjectError::GetObjectFailed => internal_error("object read failed"),
    }
}

fn map_put_error(error: PutObjectError) -> CallToolResult {
    match error {
        error @ (PutObjectError::MissingBody
        | PutObjectError::IncompleteBody
        | PutObjectError::MissingExpectedChecksum(_)
        | PutObjectError::ChecksumMismatch(_)) => bad_request(error),
        error @ PutObjectError::QuotaExceeded { .. } => {
            server_error(crate::error::ServerError::Conflict(error.to_string()))
        }
        PutObjectError::StorageError(error) => internal_error(error),
        PutObjectError::InvalidOperationState => internal_error("invalid object write state"),
        PutObjectError::NoTransactionFound => internal_error("object write transaction is missing"),
        PutObjectError::MissingOutput => internal_error("object write output is missing"),
        PutObjectError::MissingHash(hash) => {
            internal_error(format!("object hash is missing: {hash}"))
        }
        PutObjectError::WriteFailed(error) => internal_error(error),
        PutObjectError::BlobWriteFailed(error) => internal_error(error),
        PutObjectError::InvalidPreassignedVersion => internal_error("invalid object version"),
        PutObjectError::ConversionError(error) => internal_error(error),
        PutObjectError::UsageUpdateError(error) => internal_error(error),
        PutObjectError::QuotaGateError(error) => internal_error(error),
        PutObjectError::RoutingFailed(error) => internal_error(error),
        PutObjectError::BackendFenceError(error) => internal_error(error),
        PutObjectError::ManagedCopyError(error) => internal_error(error),
        PutObjectError::PolicyError(error) => internal_error(error),
        PutObjectError::PolicyGate(error) => internal_error(error),
        PutObjectError::PurgeFence(error) => internal_error(error),
        PutObjectError::PutObjectFailed => internal_error("object write failed"),
    }
}
