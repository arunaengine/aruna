use crate::auth::{parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::structs::{
    AuthContext, BackendRef, Permission, RoutingTarget, StorageRoutingRule, target_warnings,
};
use aruna_operations::driver::{drive, node_routing};
use aruna_operations::group_routing::{
    GetGroupRoutingOperation, GroupRoutingInputsOperation, PutGroupRoutingError,
    PutGroupRoutingOperation,
};
use aruna_operations::s3::bucket_routing::{
    GetBucketRoutingError, GetBucketRoutingOperation, PutBucketRoutingError,
    PutBucketRoutingOperation,
};
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "storage-routing", description = "Write routing rules for buckets and groups")),
    paths(
        get_bucket_routing,
        put_bucket_routing,
        get_group_routing,
        put_group_routing
    )
)]
pub struct StorageRoutingApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route(
            "/buckets/{bucket}/storage-routing",
            get(get_bucket_routing).put(put_bucket_routing),
        )
        .route(
            "/groups/{group_id}/storage-routing",
            get(get_group_routing).put(put_group_routing),
        )
}

/// A rule target names either a group storage backend or a storage class, and
/// exactly one of the two fields must be set. Operator backend names are
/// rejected: tenants never bind node topology.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct RoutingTargetRequest {
    /// Set this or `class`, never both and never neither.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_id: Option<String>,
    /// Set this or `backend_id`, never both and never neither.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct StorageRoutingRuleRequest {
    #[serde(default)]
    pub key_prefix: String,
    #[serde(default)]
    pub exact: bool,
    pub target: RoutingTargetRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketRoutingRequest {
    pub rules: Vec<StorageRoutingRuleRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketRoutingResponse {
    pub bucket: String,
    pub rules: Vec<StorageRoutingRuleRequest>,
    /// Advisory notes about targets this node cannot serve. The rules are
    /// stored regardless, because the record replicates to other nodes.
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct GroupRoutingRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_target: Option<RoutingTargetRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct GroupRoutingResponse {
    pub group_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_target: Option<RoutingTargetRequest>,
    pub warnings: Vec<String>,
}

impl TryFrom<RoutingTargetRequest> for RoutingTarget {
    type Error = ServerError;

    fn try_from(value: RoutingTargetRequest) -> Result<Self, Self::Error> {
        match (value.backend_id, value.class) {
            (Some(backend_id), None) => Ulid::from_str(&backend_id)
                .map(|id| RoutingTarget::Backend(BackendRef::Group(id)))
                .map_err(|_| ServerError::BadRequest),
            (None, Some(class)) => Ok(RoutingTarget::Class(class)),
            _ => Err(ServerError::BadRequest),
        }
    }
}

impl From<RoutingTarget> for RoutingTargetRequest {
    fn from(value: RoutingTarget) -> Self {
        match value {
            RoutingTarget::Backend(BackendRef::Group(id)) => Self {
                backend_id: Some(id.to_string()),
                class: None,
            },
            RoutingTarget::Backend(BackendRef::Node(name)) => Self {
                backend_id: Some(name),
                class: None,
            },
            RoutingTarget::Class(class) => Self {
                backend_id: None,
                class: Some(class),
            },
        }
    }
}

impl TryFrom<StorageRoutingRuleRequest> for StorageRoutingRule {
    type Error = ServerError;

    fn try_from(value: StorageRoutingRuleRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            key_prefix: value.key_prefix,
            exact: value.exact,
            target: value.target.try_into()?,
        })
    }
}

impl From<StorageRoutingRule> for StorageRoutingRuleRequest {
    fn from(value: StorageRoutingRule) -> Self {
        Self {
            key_prefix: value.key_prefix,
            exact: value.exact,
            target: value.target.into(),
        }
    }
}

fn map_group_error(error: PutGroupRoutingError) -> ServerError {
    match error {
        PutGroupRoutingError::InvalidTarget(reason) => {
            ServerError::BadRequestReason(reason.to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_put_error(error: PutBucketRoutingError) -> ServerError {
    match error {
        PutBucketRoutingError::NoSuchBucket | PutBucketRoutingError::GroupMismatch => {
            ServerError::NotFound
        }
        PutBucketRoutingError::InvalidRules(reason) => {
            ServerError::BadRequestReason(reason.to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

/// Advisory notes from this node's own class table, read against the backends
/// the group itself registered.
async fn warnings_for<'a>(
    state: &ServerState,
    group_id: Ulid,
    targets: impl IntoIterator<Item = &'a RoutingTarget>,
) -> Vec<String> {
    let context = state.get_ctx();
    let inputs = drive(GroupRoutingInputsOperation::new(group_id), &context)
        .await
        .unwrap_or_default();
    let catalog = node_routing(&context)
        .catalog
        .with_group_backends(inputs.backend_ids);
    target_warnings(&catalog, targets)
}

async fn group_of_bucket(state: &ServerState, bucket: &str) -> ServerResult<Ulid> {
    match drive(
        GetBucketInfoOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(Some(Ok(info))) => Ok(info.group_id),
        Ok(Some(Err(GetBucketInfoError::NotFound))) | Err(GetBucketInfoError::NotFound) => {
            Err(ServerError::NotFound)
        }
        Ok(Some(Err(err))) | Err(err) => Err(ServerError::InternalError(err.to_string())),
        Ok(None) => Err(ServerError::NotFound),
    }
}

/// Routing decides where a group's bytes physically land, so it takes group
/// admin rights, not the write rights that suffice for objects.
pub(crate) async fn ensure_group_admin(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
) -> ServerResult<()> {
    crate::auth::ensure_permission(
        state,
        auth,
        format!("/{}/g/{group_id}/admin/**", state.get_realm_id()),
        Permission::WRITE,
    )
    .await
}

#[utoipa::path(
    get,
    path = "/buckets/{bucket}/storage-routing",
    tag = "storage-routing",
    params(("bucket" = String, Path, description = "Bucket name")),
    responses(
        (status = 200, description = "Bucket routing rules", body = BucketRoutingResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Bucket not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_bucket_routing(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
) -> ServerResult<Json<BucketRoutingResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = group_of_bucket(&state, &bucket).await?;
    ensure_group_admin(&state, &auth, group_id).await?;

    let rules = drive(
        GetBucketRoutingOperation::new(bucket.clone()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        GetBucketRoutingError::NoSuchBucket => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    })?
    .transpose()
    .map_err(|error| ServerError::InternalError(error.to_string()))?
    .unwrap_or_default();

    let warnings = warnings_for(&state, group_id, rules.iter().map(|rule| &rule.target)).await;
    Ok(Json(BucketRoutingResponse {
        bucket,
        rules: rules.into_iter().map(Into::into).collect(),
        warnings,
    }))
}

#[utoipa::path(
    put,
    path = "/buckets/{bucket}/storage-routing",
    tag = "storage-routing",
    params(("bucket" = String, Path, description = "Bucket name")),
    request_body = BucketRoutingRequest,
    responses(
        (status = 200, description = "Bucket routing rules stored", body = BucketRoutingResponse),
        (status = 400, description = "Invalid rules", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Bucket not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_bucket_routing(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Json(request): Json<BucketRoutingRequest>,
) -> ServerResult<Json<BucketRoutingResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = group_of_bucket(&state, &bucket).await?;
    ensure_group_admin(&state, &auth, group_id).await?;

    let rules = request
        .rules
        .into_iter()
        .map(StorageRoutingRule::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    let stored = drive(
        PutBucketRoutingOperation::new(bucket.clone(), group_id, rules),
        &state.get_ctx(),
    )
    .await
    .map_err(map_put_error)?
    .transpose()
    .map_err(map_put_error)?
    .unwrap_or_default();

    let warnings = warnings_for(&state, group_id, stored.iter().map(|rule| &rule.target)).await;
    Ok(Json(BucketRoutingResponse {
        bucket,
        rules: stored.into_iter().map(Into::into).collect(),
        warnings,
    }))
}

#[utoipa::path(
    get,
    path = "/groups/{group_id}/storage-routing",
    tag = "storage-routing",
    params(("group_id" = String, Path, description = "Group id")),
    responses(
        (status = 200, description = "Group routing default", body = GroupRoutingResponse),
        (status = 400, description = "Invalid group id", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_group_routing(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<Json<GroupRoutingResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    ensure_group_admin(&state, &auth, group_id).await?;

    let record = drive(GetGroupRoutingOperation::new(group_id), &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?
        .transpose()
        .map_err(|error| ServerError::InternalError(error.to_string()))?
        .flatten();

    let target = record.and_then(|record| record.default_target);
    let warnings = warnings_for(&state, group_id, target.iter()).await;
    Ok(Json(GroupRoutingResponse {
        group_id: group_id.to_string(),
        default_target: target.map(Into::into),
        warnings,
    }))
}

#[utoipa::path(
    put,
    path = "/groups/{group_id}/storage-routing",
    tag = "storage-routing",
    params(("group_id" = String, Path, description = "Group id")),
    request_body = GroupRoutingRequest,
    responses(
        (status = 200, description = "Group routing default stored", body = GroupRoutingResponse),
        (status = 400, description = "Invalid target", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_group_routing(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Json(request): Json<GroupRoutingRequest>,
) -> ServerResult<Json<GroupRoutingResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    ensure_group_admin(&state, &auth, group_id).await?;

    let target = request
        .default_target
        .map(RoutingTarget::try_from)
        .transpose()?;

    let record = drive(
        PutGroupRoutingOperation::new(group_id, target, auth.user_id, SystemTime::now()),
        &state.get_ctx(),
    )
    .await
    .map_err(map_group_error)?
    .transpose()
    .map_err(map_group_error)?;

    let target = record.and_then(|record| record.default_target);
    let warnings = warnings_for(&state, group_id, target.iter()).await;
    Ok(Json(GroupRoutingResponse {
        group_id: group_id.to_string(),
        default_target: target.map(Into::into),
        warnings,
    }))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::openapi::ApiDoc;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, S3_BUCKET_KEYSPACE};
    use aruna_core::structs::{
        Actor, BucketInfo, Group, GroupAuthorizationDocument, NodeCapabilities,
        RealmAuthorizationDocument, RealmId,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_storage::storage;
    use tempfile::TempDir;

    pub(crate) struct TestState {
        _storage_dir: TempDir,
        pub(crate) auth: AuthContext,
        pub(crate) other_auth: AuthContext,
        pub(crate) group_id: Ulid,
        pub(crate) bucket: String,
        pub(crate) state: Arc<ServerState>,
    }

    fn class_rule(class: &str) -> StorageRoutingRuleRequest {
        StorageRoutingRuleRequest {
            key_prefix: "archive/".to_string(),
            exact: false,
            target: RoutingTargetRequest {
                backend_id: None,
                class: Some(class.to_string()),
            },
        }
    }

    #[tokio::test]
    async fn bucket_rules_roundtrip() {
        let test = setup_state().await;

        let Json(stored) = put_bucket_routing(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.bucket.clone()),
            Json(BucketRoutingRequest {
                rules: vec![class_rule("cold")],
            }),
        )
        .await
        .unwrap();

        assert_eq!(stored.rules, vec![class_rule("cold")]);
        // The node offers no cold backend, so the rule is stored with a warning.
        assert_eq!(stored.warnings.len(), 1);

        let Json(fetched) = get_bucket_routing(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.bucket.clone()),
        )
        .await
        .unwrap();

        assert_eq!(fetched.rules, stored.rules);
    }

    #[tokio::test]
    async fn rejects_operator_backend() {
        let test = setup_state().await;

        let result = put_bucket_routing(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.bucket.clone()),
            Json(BucketRoutingRequest {
                rules: vec![StorageRoutingRuleRequest {
                    key_prefix: String::new(),
                    exact: false,
                    target: RoutingTargetRequest {
                        backend_id: Some("cold".to_string()),
                        class: None,
                    },
                }],
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::BadRequest)));
    }

    #[tokio::test]
    async fn rejects_non_admin() {
        let test = setup_state().await;

        let result = put_bucket_routing(
            State(test.state.clone()),
            Extension(Some(test.other_auth.clone())),
            Path(test.bucket.clone()),
            Json(BucketRoutingRequest { rules: Vec::new() }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn group_default_roundtrip() {
        let test = setup_state().await;

        let Json(empty) = get_group_routing(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.group_id.to_string()),
        )
        .await
        .unwrap();
        assert_eq!(empty.default_target, None);

        let Json(stored) = put_group_routing(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.group_id.to_string()),
            Json(GroupRoutingRequest {
                default_target: Some(RoutingTargetRequest {
                    backend_id: None,
                    class: Some("cold".to_string()),
                }),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            stored
                .default_target
                .and_then(|target| target.class)
                .as_deref(),
            Some("cold")
        );

        let Json(cleared) = put_group_routing(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.group_id.to_string()),
            Json(GroupRoutingRequest {
                default_target: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(cleared.default_target, None);
    }

    #[tokio::test]
    async fn rejects_invalid_class() {
        let test = setup_state().await;

        let result = put_group_routing(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.group_id.to_string()),
            Json(GroupRoutingRequest {
                default_target: Some(RoutingTargetRequest {
                    backend_id: None,
                    class: Some("NOT VALID".to_string()),
                }),
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::BadRequestReason(_))));
    }

    #[tokio::test]
    async fn rejects_foreign_backend() {
        // Nothing registered this id for the group, so the rule must not store.
        let test = setup_state().await;
        let foreign = Ulid::generate();

        let result = put_bucket_routing(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.bucket.clone()),
            Json(BucketRoutingRequest {
                rules: vec![StorageRoutingRuleRequest {
                    key_prefix: String::new(),
                    exact: false,
                    target: RoutingTargetRequest {
                        backend_id: Some(foreign.to_string()),
                        class: None,
                    },
                }],
            }),
        )
        .await;

        let Err(ServerError::BadRequestReason(reason)) = result else {
            panic!("expected a 400 naming the backend, got {result:?}")
        };
        assert!(reason.contains(&foreign.to_string()), "{reason}");
    }

    #[test]
    fn openapi_lists_routes() {
        let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();

        assert!(openapi["paths"]["/buckets/{bucket}/storage-routing"]["put"].is_object());
        assert!(openapi["paths"]["/groups/{group_id}/storage-routing"]["put"].is_object());
        assert!(
            openapi["components"]["schemas"]["BucketRoutingResponse"]["properties"]
                .get("warnings")
                .is_some()
        );
    }

    pub(crate) async fn setup_state() -> TestState {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId([3u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let other_user_id = UserId::local(Ulid::generate(), realm_id);
        let actor = Actor {
            node_id,
            user_id,
            realm_id,
        };
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let group_id = Ulid::generate();
        let group_auth =
            GroupAuthorizationDocument::new_default_group_doc(user_id, realm_id, group_id);
        let group = Group {
            display_name: "routing-group".to_string(),
            group_id,
            realm_id,
            roles: group_auth.roles.keys().copied().collect(),
            owner: user_id,
        };
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        let bucket = "routed".to_string();
        let bucket_info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: user_id,
            cors_configuration: None,
            replication: None,
            storage_routing: Vec::new(),
        };

        write_doc(
            &driver_ctx,
            AUTH_KEYSPACE,
            (*realm_id.as_bytes()).into(),
            realm_auth.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            AUTH_KEYSPACE,
            group_id.to_bytes().into(),
            group_auth.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            GROUP_KEYSPACE,
            group_id.to_bytes().into(),
            group.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            S3_BUCKET_KEYSPACE,
            bucket.as_bytes().to_vec().into(),
            bucket_info.to_bytes().unwrap().into(),
        )
        .await;

        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm_id,
                node_id,
                NodeCapabilities::local_node(realm_id).unwrap(),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );

        TestState {
            _storage_dir: storage_dir,
            auth: AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
            },
            other_auth: AuthContext {
                user_id: other_user_id,
                realm_id,
                path_restrictions: None,
            },
            group_id,
            bucket,
            state,
        }
    }

    async fn write_doc(
        driver_ctx: &Arc<DriverContext>,
        key_space: &str,
        key: byteview::ByteView,
        value: byteview::ByteView,
    ) {
        let event = driver_ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key,
                value,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }
}
