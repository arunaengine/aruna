use std::collections::BTreeSet;
use std::sync::Arc;

use aruna_core::structs::{
    AuthContext, Permission, StoragePurgeScope, StoragePurgeSpec, blob_bucket_permission_path,
    blob_object_permission_path,
};
use aruna_operations::driver::drive;
use aruna_operations::jobs::JOB_RETENTION_MS;
use aruna_operations::jobs::service::submit_storage_purge_job;
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use aruna_operations::s3::list_multipart_uploads::{
    ListMultipartUploadsInput, ListMultipartUploadsOperation,
};
use aruna_operations::s3::list_object_versions::{
    ListObjectVersionsInput, ListObjectVersionsItem, ListObjectVersionsOperation,
};
use aruna_operations::sync_relationship::{
    ListSyncRelationshipsOperation, SyncRelationshipDirection,
};
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{permission_granted, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;

const DEFAULT_PREFLIGHT_LIMIT: usize = 1_000;
const MAX_PREFLIGHT_LIMIT: usize = 1_000;

#[derive(OpenApi)]
#[openapi()]
pub struct StorageDeletionApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(StorageDeletionApiDoc::openapi())
        .routes(routes!(deletion_preflight))
        .routes(routes!(submit_purge))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PurgeScopeRequest {
    File { bucket: String, key: String },
    Prefix { bucket: String, prefix: String },
    Bucket { bucket: String },
}

impl PurgeScopeRequest {
    fn to_core(&self) -> ServerResult<StoragePurgeScope> {
        let scope = match self {
            Self::File { bucket, key } if !bucket.is_empty() && !key.is_empty() => {
                StoragePurgeScope::File {
                    bucket: bucket.clone(),
                    key: key.clone(),
                }
            }
            Self::Prefix { bucket, prefix } if !bucket.is_empty() && !prefix.is_empty() => {
                StoragePurgeScope::Prefix {
                    bucket: bucket.clone(),
                    prefix: prefix.clone(),
                }
            }
            Self::Bucket { bucket } if !bucket.is_empty() => StoragePurgeScope::Bucket {
                bucket: bucket.clone(),
            },
            _ => return Err(ServerError::BadRequest),
        };
        Ok(scope)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeletionPreflightRequest {
    pub scope: PurgeScopeRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_key_marker: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id_marker: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multipart_key_marker: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multipart_upload_id_marker: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PurgeCountsResponse {
    pub current_heads: u64,
    pub noncurrent_versions: u64,
    pub delete_markers: u64,
    pub open_multipart_uploads: u64,
    /// False means every number is the count in this bounded page, not a total.
    pub complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PurgePermissionsResponse {
    pub read: bool,
    pub purge: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PurgeTruncationResponse {
    pub truncated: bool,
    pub versions_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_version_key_marker: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_version_id_marker: Option<String>,
    pub multipart_uploads_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_multipart_key_marker: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_multipart_upload_id_marker: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SyncDeletionSideEffectResponse {
    pub relationship_id: String,
    pub direction: String,
    pub source: String,
    pub target: String,
    pub action: String,
    pub blocker: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReferenceCoverageResponse {
    pub complete: bool,
    pub hidden_references_exist: Option<bool>,
    pub queried_nodes: u32,
    pub failed_nodes: u32,
    pub index_freshness: String,
    pub excluded: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeletionPreflightResponse {
    pub scope: PurgeScopeRequest,
    pub counts: PurgeCountsResponse,
    pub sync_relationships_apply_to_bucket_delete: bool,
    pub sync_relationships: Vec<SyncDeletionSideEffectResponse>,
    pub permissions: PurgePermissionsResponse,
    pub truncation: PurgeTruncationResponse,
    /// The Realm-complete backlink service is a separate dependency. Until it
    /// is available, this field explicitly refuses to imply "unreferenced".
    pub reference_coverage: ReferenceCoverageResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SubmitPurgeRequest {
    pub scope: PurgeScopeRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SubmitPurgeResponse {
    pub job_id: String,
    pub created: bool,
    pub status_url: String,
}

#[utoipa::path(
    post,
    path = "/data/storage/deletion/preflight",
    tag = "data/storage",
    summary = "Preview what a permanent deletion would destroy",
    description = r#"Reports the bounded inventory and consequences of permanently deleting one storage scope.

**Authentication**: bearer token issued for this realm and READ on the selected scope; the reported
`permissions.purge` says separately whether the same caller could actually run the purge.

**Behavior**
- Nothing is deleted or reserved here.
- The inventory is one bounded page of this node's own rows: `counts.complete` false means every
  number counts that page rather than the scope, and `truncation` carries the markers a following
  request resumes from.
- Sync relationships that a bucket delete would act on are listed with the ones that block it.
- `reference_coverage` never implies that the scope is unreferenced: it reports how much of the
  backlink question this node could answer at all."#,
    request_body(
        content = DeletionPreflightRequest,
        description = "The scope to inspect plus optional page bound and resume markers",
        example = json!({
            "scope": {
                "kind": "prefix",
                "bucket": "datasets",
                "prefix": "raw/"
            },
            "limit": 1000
        })
    ),
    responses(
        (
            status = 200,
            description = "Bounded, scope-aware deletion inventory and consequences",
            body = DeletionPreflightResponse,
            example = json!({
                "scope": {
                    "kind": "prefix",
                    "bucket": "datasets",
                    "prefix": "raw/"
                },
                "counts": {
                    "current_heads": 42,
                    "noncurrent_versions": 7,
                    "delete_markers": 1,
                    "open_multipart_uploads": 0,
                    "complete": true
                },
                "sync_relationships_apply_to_bucket_delete": false,
                "sync_relationships": [],
                "permissions": {
                    "read": true,
                    "purge": false
                },
                "truncation": {
                    "truncated": false,
                    "versions_truncated": false,
                    "multipart_uploads_truncated": false
                },
                "reference_coverage": {
                    "complete": false,
                    "hidden_references_exist": null,
                    "queried_nodes": 1,
                    "failed_nodes": 0,
                    "index_freshness": "local",
                    "excluded": [
                        "remote_nodes"
                    ]
                }
            })
        ),
        (status = 400, description = "Invalid scope, marker, or limit", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller cannot read the selected scope", body = ErrorResponse),
        (status = 404, description = "Bucket not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn deletion_preflight(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<DeletionPreflightRequest>,
) -> ServerResult<(StatusCode, Json<DeletionPreflightResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let scope = request.scope.to_core()?;
    let info = bucket_info(&state, scope.bucket()).await?;
    let path = scope_permission_path(&state, info.group_id, &scope);
    let can_read = permission_granted(&state, &auth, path.clone(), Permission::READ).await?;
    if !can_read {
        return Err(ServerError::Forbidden);
    }
    let can_purge = permission_granted(&state, &auth, path, Permission::WRITE).await?;
    let limit = request.limit.unwrap_or(DEFAULT_PREFLIGHT_LIMIT);
    if !(1..=MAX_PREFLIGHT_LIMIT).contains(&limit)
        || (request.version_id_marker.is_some() && request.version_key_marker.is_none())
        || (request.multipart_upload_id_marker.is_some() && request.multipart_key_marker.is_none())
    {
        return Err(ServerError::BadRequest);
    }
    let version_id_marker = request
        .version_id_marker
        .as_deref()
        .map(Ulid::from_string)
        .transpose()
        .map_err(|_| ServerError::BadRequest)?;
    let multipart_upload_id_marker = request
        .multipart_upload_id_marker
        .as_deref()
        .map(Ulid::from_string)
        .transpose()
        .map_err(|_| ServerError::BadRequest)?;

    let versions = drive(
        ListObjectVersionsOperation::new(ListObjectVersionsInput {
            bucket: scope.bucket().to_string(),
            prefix: scope.list_prefix().map(str::to_string),
            delimiter: None,
            key_marker: request.version_key_marker,
            version_id_marker,
            max_keys: Some(limit),
        }),
        &state.get_ctx(),
    )
    .await
    .and_then(|result| result.transpose())
    .map_err(|error| ServerError::InternalError(error.to_string()))?
    .ok_or_else(|| ServerError::InternalError("version inventory returned no result".into()))?;
    let uploads = drive(
        ListMultipartUploadsOperation::new(ListMultipartUploadsInput {
            bucket: scope.bucket().to_string(),
            prefix: scope.list_prefix().map(str::to_string),
            delimiter: None,
            key_marker: request.multipart_key_marker,
            upload_id_marker: multipart_upload_id_marker,
            max_uploads: limit,
        })
        .complete_scan(),
        &state.get_ctx(),
    )
    .await
    .and_then(|result| result.transpose())
    .map_err(|error| ServerError::InternalError(error.to_string()))?
    .ok_or_else(|| ServerError::InternalError("multipart inventory returned no result".into()))?;

    let (items, versions_truncated, next_version_key_marker, next_version_id_marker) =
        scoped_version_page(&scope, versions);
    let (uploads, uploads_truncated, next_multipart_key_marker, next_multipart_upload_id_marker) =
        scoped_multipart_page(&scope, uploads);
    let mut current_heads = 0u64;
    let mut noncurrent_versions = 0u64;
    let mut delete_markers = 0u64;
    for item in items {
        match item {
            ListObjectVersionsItem::Version {
                is_latest: true, ..
            } => current_heads += 1,
            ListObjectVersionsItem::Version { .. } => noncurrent_versions += 1,
            ListObjectVersionsItem::DeleteMarker { .. } => delete_markers += 1,
        }
    }
    let truncated = versions_truncated || uploads_truncated;
    let sync_relationships = if scope.is_bucket() {
        list_sync_side_effects(&state, scope.bucket()).await?
    } else {
        Vec::new()
    };

    Ok((
        StatusCode::OK,
        Json(DeletionPreflightResponse {
            scope: request.scope,
            counts: PurgeCountsResponse {
                current_heads,
                noncurrent_versions,
                delete_markers,
                open_multipart_uploads: uploads.len() as u64,
                complete: !truncated,
            },
            sync_relationships_apply_to_bucket_delete: scope.is_bucket(),
            sync_relationships,
            permissions: PurgePermissionsResponse {
                read: true,
                purge: can_purge,
            },
            truncation: PurgeTruncationResponse {
                truncated,
                versions_truncated,
                next_version_key_marker,
                next_version_id_marker: next_version_id_marker.map(|id| id.to_string()),
                multipart_uploads_truncated: uploads_truncated,
                next_multipart_key_marker,
                next_multipart_upload_id_marker: next_multipart_upload_id_marker
                    .map(|id| id.to_string()),
            },
            reference_coverage: ReferenceCoverageResponse {
                complete: false,
                hidden_references_exist: None,
                queried_nodes: 0,
                failed_nodes: 0,
                index_freshness: "not_evaluated".to_string(),
                excluded: vec![
                    "realm_backlink_fanout_unavailable".to_string(),
                    "imported_relative_or_external_file_ids".to_string(),
                ],
            },
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/data/storage/purge/jobs",
    tag = "data/storage",
    summary = "Submit a permanent purge of one storage scope",
    description = r#"Queues a permanent purge of one storage scope as a job.

**Authentication**: bearer token issued for this realm and WRITE on the selected scope.

**Behavior**
- The purge runs as a job: a 201 means it was durably queued, never that anything was deleted yet,
  so poll the returned `status_url`.
- Deletion is permanent and no version survives it, which is why the preflight surface exists to
  report the consequences first.
- An `idempotency_key` makes a retried submission return the retained job with a 200 instead of
  queuing a second purge.

**Errors**: an `idempotency_key` that names a different scope than the retained job is a 409."#,
    request_body(
        content = SubmitPurgeRequest,
        description = "The scope to purge plus an optional idempotency key",
        example = json!({
            "scope": {
                "kind": "prefix",
                "bucket": "datasets",
                "prefix": "raw/"
            },
            "idempotency_key": "purge-raw-2026-04-09-001"
        })
    ),
    responses(
        (
            status = 201,
            description = "Purge job created",
            body = SubmitPurgeResponse,
            example = json!({
                "job_id": "01JABCDEF0123456789ABCDEFG",
                "created": true,
                "status_url": "/api/v1/jobs/01JABCDEF0123456789ABCDEFG"
            })
        ),
        (
            status = 200,
            description = "Idempotent replay returned the retained job",
            body = SubmitPurgeResponse,
            example = json!({
                "job_id": "01JABCDEF0123456789ABCDEFG",
                "created": false,
                "status_url": "/api/v1/jobs/01JABCDEF0123456789ABCDEFG"
            })
        ),
        (status = 400, description = "Invalid scope or idempotency key", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller cannot purge the selected scope", body = ErrorResponse),
        (status = 404, description = "Bucket not found", body = ErrorResponse),
        (status = 409, description = "Idempotency key conflicts with a different purge", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn submit_purge(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<SubmitPurgeRequest>,
) -> ServerResult<(StatusCode, Json<SubmitPurgeResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    if request
        .idempotency_key
        .as_ref()
        .is_some_and(|key| key.is_empty() || key.len() > 256)
    {
        return Err(ServerError::BadRequest);
    }
    let scope = request.scope.to_core()?;
    let info = bucket_info(&state, scope.bucket()).await?;
    let path = scope_permission_path(&state, info.group_id, &scope);
    if !permission_granted(&state, &auth, path, Permission::WRITE).await? {
        return Err(ServerError::Forbidden);
    }
    let result = submit_storage_purge_job(
        &state.get_ctx(),
        StoragePurgeSpec {
            scope,
            group_id: info.group_id,
            auth_context: auth,
            node_id: state.get_node_id(),
        },
        state.get_node_id(),
        request.idempotency_key,
        JOB_RETENTION_MS,
    )
    .await
    .map_err(super::jobs::map_submit_error)?;
    let urls = super::jobs::job_urls(&state, result.job_id).await?;
    Ok((
        if result.created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(SubmitPurgeResponse {
            job_id: result.job_id.to_string(),
            created: result.created,
            status_url: urls.status_url,
        }),
    ))
}

async fn bucket_info(
    state: &ServerState,
    bucket: &str,
) -> ServerResult<aruna_core::structs::BucketInfo> {
    match drive(
        GetBucketInfoOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    .and_then(|result| result.transpose())
    {
        Ok(Some(info)) => Ok(info),
        Ok(None) | Err(GetBucketInfoError::NotFound) => Err(ServerError::NotFound),
        Err(error) => Err(ServerError::InternalError(error.to_string())),
    }
}

fn scope_permission_path(state: &ServerState, group_id: Ulid, scope: &StoragePurgeScope) -> String {
    match scope {
        StoragePurgeScope::Bucket { bucket } | StoragePurgeScope::Prefix { bucket, .. } => {
            blob_bucket_permission_path(state.get_realm_id(), group_id, state.get_node_id(), bucket)
        }
        StoragePurgeScope::File { bucket, key } => blob_object_permission_path(
            state.get_realm_id(),
            group_id,
            state.get_node_id(),
            bucket,
            key,
        ),
    }
}

fn scoped_version_page(
    scope: &StoragePurgeScope,
    result: aruna_operations::s3::list_object_versions::ListObjectVersionsResult,
) -> (
    Vec<ListObjectVersionsItem>,
    bool,
    Option<String>,
    Option<Ulid>,
) {
    let mut items = result.items;
    let mut truncated = result.is_truncated;
    let mut key_marker = result.next_key_marker;
    let mut version_marker = result.next_version_id_marker;
    if let StoragePurgeScope::File { key, .. } = scope {
        items.retain(|item| match item {
            ListObjectVersionsItem::Version { key: item, .. }
            | ListObjectVersionsItem::DeleteMarker { key: item, .. } => item == key,
        });
        if key_marker.as_deref() != Some(key.as_str()) {
            truncated = false;
            key_marker = None;
            version_marker = None;
        }
    }
    (items, truncated, key_marker, version_marker)
}

fn scoped_multipart_page(
    scope: &StoragePurgeScope,
    result: aruna_operations::s3::list_multipart_uploads::ListMultipartUploadsResult,
) -> (
    Vec<aruna_core::structs::MultipartUpload>,
    bool,
    Option<String>,
    Option<Ulid>,
) {
    let mut uploads = result.uploads;
    let mut truncated = result.is_truncated;
    let mut key_marker = result.next_key_marker;
    let mut upload_marker = result.next_upload_id_marker;
    if let StoragePurgeScope::File { key, .. } = scope {
        uploads.retain(|upload| upload.key == *key);
        if key_marker.as_deref() != Some(key.as_str()) {
            truncated = false;
            key_marker = None;
            upload_marker = None;
        }
    }
    (uploads, truncated, key_marker, upload_marker)
}

async fn list_sync_side_effects(
    state: &ServerState,
    bucket: &str,
) -> ServerResult<Vec<SyncDeletionSideEffectResponse>> {
    let mut response = Vec::new();
    let mut seen = BTreeSet::new();
    for (direction, label) in [
        (SyncRelationshipDirection::Outgoing, "outgoing"),
        (SyncRelationshipDirection::Incoming, "incoming"),
    ] {
        let relationships = drive(
            ListSyncRelationshipsOperation::new(direction, Some(bucket.to_string())),
            &state.get_ctx(),
        )
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
        for relationship in relationships {
            if !seen.insert((relationship.id, label)) {
                continue;
            }
            response.push(SyncDeletionSideEffectResponse {
                relationship_id: relationship.id.to_string(),
                direction: label.to_string(),
                source: relationship.source.to_string(),
                target: relationship.target.to_string(),
                action: "remove_local_relationship_and_repair_remote_mirror".to_string(),
                blocker: false,
            });
        }
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::SYNC_RELATIONSHIP_OUT_KEYSPACE;
    use aruna_core::structs::{
        ArunaArn, NodeCapabilities, RealmId, SyncMode, SyncRelationship, SyncState,
        SyncStatusSnapshot, sync_relationship_key,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_operations::jobs::runtime::JobsRuntime;
    use aruna_storage::storage::FjallStorage;
    use std::time::SystemTime;

    #[tokio::test]
    async fn bucket_preflight_discloses_sync_cleanup_as_a_non_blocker() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId::from_bytes(
            ed25519_dalek::SigningKey::from_bytes(&[7u8; 32])
                .verifying_key()
                .to_bytes(),
        );
        let local_node = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
        let remote_node = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
        let state = ServerState::new(
            Arc::new(DriverContext {
                storage_handle: storage.clone(),
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
                compute_handle: None,
            }),
            realm_id,
            local_node,
            NodeCapabilities::user_node(realm_id).unwrap(),
            false,
            None,
            JobsRuntime::new(),
        )
        .await;
        let relationship = SyncRelationship {
            id: Ulid::from_bytes([3u8; 16]),
            source: ArunaArn::s3_bucket(realm_id, local_node, "bucket").unwrap(),
            target: ArunaArn::s3_bucket(realm_id, remote_node, "replica").unwrap(),
            mode: SyncMode::Continuous,
            reference_handling: Default::default(),
            reference_serving: false,
            replicate_deletes: true,
            created_by: aruna_core::UserId::nil(realm_id),
            created_at: SystemTime::UNIX_EPOCH,
            state: SyncState::Enabled,
            status: SyncStatusSnapshot::default(),
        };
        assert!(matches!(
            storage
                .send_storage_effect(StorageEffect::Write {
                    key_space: SYNC_RELATIONSHIP_OUT_KEYSPACE.to_string(),
                    key: sync_relationship_key("bucket", relationship.id).into(),
                    value: relationship.to_bytes().unwrap().into(),
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        let disclosed = list_sync_side_effects(&state, "bucket").await.unwrap();

        assert_eq!(disclosed.len(), 1);
        assert_eq!(disclosed[0].relationship_id, relationship.id.to_string());
        assert_eq!(disclosed[0].direction, "outgoing");
        assert_eq!(
            disclosed[0].action,
            "remove_local_relationship_and_repair_remote_mirror"
        );
        assert!(!disclosed[0].blocker);
    }
}
