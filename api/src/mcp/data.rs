use super::context::member_groups;
use super::{
    JsonPayload, McpServer, authorize_tool, bad_request, empty_extras, explained, internal_error,
    request_auth, server_error, tool_extras,
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
    /// Bucket name as the S3 surface uses it, for example `project-data`. Three
    /// to 63 characters of lowercase letters, digits, dots, and hyphens. Call
    /// `list_buckets` for the readable names; this is not an `s3://` URL and
    /// carries no key.
    pub bucket: String,
    /// Optional key prefix filter, for example `reads/2026/`. Matched literally
    /// from the start of the key, with no wildcards and no leading slash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Opaque continuation token copied verbatim from the `next_cursor` of a
    /// previous `list_objects` answer for the same bucket and prefix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Maximum number of objects to return. Defaults to 100 and is clamped to
    /// the range 1 to 200.
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
    /// Bucket name as the S3 surface uses it, for example `project-data`. Call
    /// `list_buckets` for the readable names; this is not an `s3://` URL.
    pub bucket: String,
    /// Object key inside the bucket, for example `reads/sample.fastq.gz`.
    /// Relative, with no leading slash, no `..` segment, and no control
    /// character. Call `list_objects` for the keys in a bucket.
    pub key: String,
    /// Byte offset to start reading from. Defaults to 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    /// Maximum number of bytes to return. Defaults to 1048576, which is also
    /// the maximum; a value outside 1 to 1048576 is refused. The answer sets
    /// `truncated` when the object continues past this window.
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
    /// Bucket name as the S3 surface uses it, for example `project-data`. It
    /// must already exist and the caller needs write permission on it.
    pub bucket: String,
    /// Object key inside the bucket, for example `notes/summary.md`. Relative,
    /// with no leading slash, no `..` segment, and no control character. An
    /// existing key is replaced with a new version.
    pub key: String,
    /// The full UTF-8 body to store, at most 1048576 bytes. This tool writes
    /// text only; binary content must go through the S3 surface.
    pub text: String,
    /// MIME type recorded for the object, for example `application/json`.
    /// Defaults to `text/plain; charset=utf-8`.
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

/// Which section of the unified search to query.
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
    /// Search text, at least two characters after trimming. Matched as a
    /// substring for buckets, groups, and users, and as a full-text query over
    /// name, description, keywords, and identifier for documents. Plain terms
    /// only: boolean operators, quotes, and wildcards are stripped.
    pub q: String,
    /// Restrict the answer to one section: `documents`, `buckets`, `groups`, or
    /// `users`. Omit to search all four. Each section returns at most ten hits,
    /// and the users section is present only for a caller with admin read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<SearchKind>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct SearchOutput {
    pub results: JsonPayload,
}

pub(crate) fn toolset() -> rmcp::handler::server::router::tool::ToolRouter<McpServer> {
    McpServer::data_router()
}

#[tool_router(router = data_router)]
impl McpServer {
    #[tool(
        description = "List the buckets on this node that the caller may read, each with its name, owning group_id, and creation time. Call it first to obtain the bucket name that list_objects, read_object, write_object, and run_script require. Buckets are node-local, so a name absent here may still exist on another node. Takes no arguments.",
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
        description = "List objects in one bucket, each with key, size, etag, last_modified, content_type, and whether it is a reference. Call list_buckets first for a valid bucket name. Narrow the answer with a key prefix, and follow next_cursor for the next page. Use read_object to fetch the text of one key.",
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
        .map_err(|error| object_error(error, "read"))?;
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
        description = "Read a bounded UTF-8 text window from one object and return the text with its offset, byte count, content type, and a truncated flag. Call list_objects for a valid key. Use offset and max_bytes to walk a large object; the window is at most 1 MiB. An object that is not valid UTF-8 is refused, so this tool cannot fetch binary content.",
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
        description = "Write at most 1 MiB of UTF-8 text to one object and return the bucket, key, new version_id, size, and content type. The bucket must already exist and the caller needs write permission on it; call list_buckets for names. The whole body is replaced, creating a new version of an existing key, so read_object first when appending. Use create_dataset for RO-Crate metadata rather than storing it as an object.",
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
        description = "Search across documents, buckets, groups, and users in one call and return the matching sections, each with its hits and paging state. Use it to find something by name when no id is known; use search_datasets for a metadata search that also filters by Profile conformance and group. Document hits carry document_id for get_dataset, bucket hits carry the bucket name for list_objects, and group hits carry group_id. A section the caller may not read is omitted rather than refused.",
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
        Ok(Json(SearchOutput {
            results: JsonPayload(results),
        }))
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

/// Bucket names are node-local, so a caller needs to be told to look at this
/// node's list rather than assume the bucket is elsewhere.
fn missing_bucket() -> CallToolResult {
    explained(
        crate::error::ServerError::NotFound,
        "no bucket with that name exists on this node; call list_buckets for readable names, and \
         pass the bare bucket name without an s3:// prefix or a key",
    )
}

pub(crate) async fn read_text(
    server: &McpServer,
    auth: &AuthContext,
    input: ReadObjectInput,
    extras: aruna_operations::request_policy::PolicyRequestExtras,
) -> Result<ReadObjectOutput, CallToolResult> {
    validate_key(&input.key)?;
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
    .map_err(|error| object_error(error, "read"))?;
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
            "the object is not UTF-8 text and its content type is {content_type}; this tool \
             returns text only, so fetch binary content through the S3 surface"
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
    validate_key(&input.key)?;
    let size = input.text.len();
    if size > MAX_TEXT_BYTES {
        return Err(server_error(crate::error::ServerError::PayloadTooLarge(
            format!(
                "text is {size} bytes and this tool stores at most {MAX_TEXT_BYTES}; split the \
                 content across keys or upload it through the S3 surface"
            ),
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
    .map_err(|error| object_error(error, "write"))?;
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

fn object_error(error: crate::error::ServerError, action: &str) -> CallToolResult {
    match error {
        crate::error::ServerError::Forbidden => explained(
            error,
            format!(
                "the caller holds no {action} permission there; call list_buckets for the \
                 buckets it may use"
            ),
        ),
        error => server_error(error),
    }
}

/// The S3 key rule reads as an opaque `InvalidArgument`; a tool caller needs the
/// shape a key must have.
fn validate_key(key: &str) -> Result<(), CallToolResult> {
    crate::s3::util::validate_object_key(key).map_err(|error| {
        bad_request(format!(
            "key is not a valid object key ({}); pass a relative key such as \
             reads/sample.fastq.gz, with no leading slash, no `..` segment, and no control \
             character, and call list_objects for existing keys",
            error.message().unwrap_or_default()
        ))
    })
}

fn bounded_bytes(max_bytes: Option<usize>) -> Result<usize, CallToolResult> {
    let max_bytes = max_bytes.unwrap_or(MAX_TEXT_BYTES);
    if !(1..=MAX_TEXT_BYTES).contains(&max_bytes) {
        return Err(bad_request(format!(
            "max_bytes must be between 1 and {MAX_TEXT_BYTES}; omit it to read the full {MAX_TEXT_BYTES} byte window"
        )));
    }
    Ok(max_bytes)
}

fn decode_cursor(cursor: &str) -> Result<ListObjectsV2ContinuationToken, CallToolResult> {
    const REASON: &str = "cursor must be a next_cursor value copied verbatim from a previous \
                          list_objects answer for the same bucket and prefix; omit it to start at \
                          the first page";
    let bytes = STANDARD.decode(cursor).map_err(|_| bad_request(REASON))?;
    ListObjectsV2ContinuationToken::from_bytes(&bytes).map_err(|_| bad_request(REASON))
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
        GetBucketInfoError::NotFound => missing_bucket(),
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
        | GetObjectError::HistoricalReferenceUnavailable => explained(
            crate::error::ServerError::NotFound,
            "the bucket holds no readable object under that key; call list_objects for the keys \
             it does hold",
        ),
        error @ GetObjectError::InvalidRange => bad_request(format!(
            "{error} offset must be below the object size; call list_objects for the size and \
             read from offset 0 for a fresh read"
        )),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn body(result: CallToolResult) -> serde_json::Value {
        assert_eq!(result.is_error, Some(true));
        result
            .structured_content
            .expect("a tool error carries the structured body")
    }

    #[test]
    fn key_rejects_traversal() {
        let text = body(validate_key("../secret").unwrap_err());
        assert_eq!(text["code"], "Bad request");
        assert!(
            text["error"]
                .as_str()
                .unwrap_or_default()
                .contains("relative key")
        );
        assert!(validate_key("reads/sample.fastq.gz").is_ok());
    }

    #[test]
    fn bounded_bytes_range() {
        assert_eq!(bounded_bytes(None).unwrap(), MAX_TEXT_BYTES);
        assert_eq!(bounded_bytes(Some(1024)).unwrap(), 1024);
        assert!(bounded_bytes(Some(0)).is_err());
        assert!(bounded_bytes(Some(MAX_TEXT_BYTES + 1)).is_err());
    }

    #[test]
    fn cursor_rejects_garbage() {
        // Non base64 and well-formed base64 that is not a token both refuse.
        let text = body(decode_cursor("!not base64!").unwrap_err());
        assert!(
            text["error"]
                .as_str()
                .unwrap_or_default()
                .contains("next_cursor")
        );
        assert!(decode_cursor("Zm9v").is_err());
    }

    #[test]
    fn object_error_forbidden() {
        let forbidden = body(object_error(crate::error::ServerError::Forbidden, "write"));
        assert!(
            forbidden["error"]
                .as_str()
                .unwrap_or_default()
                .contains("write permission")
        );
        assert_eq!(
            body(object_error(crate::error::ServerError::NotFound, "read"))["code"],
            "Not found"
        );
    }

    #[test]
    fn bucket_error_maps() {
        assert!(
            body(map_bucket_error(GetBucketInfoError::NotFound))["error"]
                .as_str()
                .unwrap_or_default()
                .contains("list_buckets")
        );
        assert_eq!(
            body(map_bucket_error(GetBucketInfoError::GetBucketInfoFailed))["code"],
            "Internal error"
        );
    }

    #[test]
    fn get_error_categories() {
        assert_eq!(
            body(map_get_error(GetObjectError::NoSuchKey))["code"],
            "Not found"
        );
        assert_eq!(
            body(map_get_error(GetObjectError::InvalidRange))["code"],
            "Bad request"
        );
        assert_eq!(
            body(map_get_error(GetObjectError::GovernedUnavailable))["code"],
            "Forbidden"
        );
        assert_eq!(
            body(map_get_error(GetObjectError::ReferenceSourceChanged))["code"],
            "Conflict"
        );
    }

    #[test]
    fn put_error_categories() {
        assert_eq!(
            body(map_put_error(PutObjectError::MissingBody))["code"],
            "Bad request"
        );
        assert_eq!(
            body(map_put_error(PutObjectError::QuotaExceeded {
                limit: 10,
                usage: 20
            }))["code"],
            "Conflict"
        );
        assert_eq!(
            body(map_put_error(PutObjectError::PutObjectFailed))["code"],
            "Internal error"
        );
    }

    #[test]
    fn search_kind_names() {
        assert_eq!(SearchKind::Documents.as_str(), "documents");
        assert_eq!(SearchKind::Buckets.as_str(), "buckets");
        assert_eq!(SearchKind::Groups.as_str(), "groups");
        assert_eq!(SearchKind::Users.as_str(), "users");
    }
}
