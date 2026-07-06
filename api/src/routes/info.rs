use crate::error::{ServerError, ServerResult};
pub use crate::server_state::PortalStatus;
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::alpn::Alpn;
use aruna_core::errors::StorageError;
use aruna_core::structs::{
    Actor, AuthContext, GroupQuotaOverride, Permission, QuotaConfig, UserGroupCapOverride,
};
use aruna_core::structs::{ConnectionAddressStatus, PeerConnectionStatus, RequestSummaryState};
use aruna_core::structs::{RealmConfigDocument, RealmNodeKind};
use aruna_core::structs::{USAGE_GLOBAL_KEY, UsageCounters};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::driver::drive;
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::get_realm_nodes::GetRealmNodesOperation;
use aruna_operations::set_realm_quota::{
    SetRealmQuotaConfig, SetRealmQuotaError, SetRealmQuotaOperation,
};
use aruna_operations::status::load_node_observability_status;
use aruna_operations::usage_stats::{LoadUsageCountersOperation, RealmUsageScope};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, put};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tracing::warn;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "info", description = "Node information endpoints")),
    paths(get_info, get_realm_info, set_realm_quota, get_usage)
)]
pub struct InfoApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/info", get(get_info))
        .route("/info/realm", get(get_realm_info))
        .route("/info/realm/quota", put(set_realm_quota))
        .route("/info/usage", get(get_usage))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct InfoResponse {
    pub node: NodeStatus,
    pub api_version: String,
    pub portal: PortalStatus,
    pub my_addresses: Vec<String>,
    pub connections: Vec<PeerConnectionInfo>,
    pub services: ServicesStatus,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct NodeStatus {
    pub status: ServiceStatus,
    pub realm_id: String,
    pub peer_id: String,
    pub capabilities: NodeCapabilityKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum NodeCapabilityKind {
    Management,
    Server,
    Local,
}

impl From<&aruna_core::structs::NodeCapabilities> for NodeCapabilityKind {
    fn from(capabilities: &aruna_core::structs::NodeCapabilities) -> Self {
        match capabilities {
            aruna_core::structs::NodeCapabilities::Management { .. } => Self::Management,
            aruna_core::structs::NodeCapabilities::Server { .. } => Self::Server,
            aruna_core::structs::NodeCapabilities::Local { .. } => Self::Local,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ServiceStatus {
    Available,
    NotConfigured,
    Unavailable,
}

impl From<aruna_core::structs::Status> for ServiceStatus {
    fn from(status: aruna_core::structs::Status) -> Self {
        match status {
            aruna_core::structs::Status::Available => Self::Available,
            aruna_core::structs::Status::NotConfigured => Self::NotConfigured,
            aruna_core::structs::Status::Unavailable => Self::Unavailable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PeerConnectionInfo {
    pub peer_id: String,
    pub status: PeerStatus,
    pub active_addresses: Vec<ConnectionAddressInfo>,
    pub last_error: Option<String>,
    pub next_retry_secs: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PeerStatus {
    Connected,
    Known,
    Unreachable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ConnectionAddressInfo {
    pub status: AddressStatus,
    pub address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rtt_ms: Option<u64>,
    pub protocol_connections: Vec<ProtocolConnectionInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AddressStatus {
    Active,
    NotAssigned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ProtocolConnectionInfo {
    pub connection_id: u64,
    pub protocol: Option<String>,
    pub side: String,
    pub status: ProtocolConnectionStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolConnectionStatus {
    Open,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ServicesStatus {
    pub network: NetworkServiceStatus,
    pub blob: BlobServiceStatus,
    pub database: DatabaseServiceStatus,
    pub interfaces: InterfaceServicesStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct NetworkServiceStatus {
    pub status: ServiceStatus,
    pub discovery: Vec<String>,
    pub relay: Option<String>,
    pub relay_urls: Vec<String>,
    pub routing_table_size: Option<usize>,
    pub requests: RequestSummary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RequestSummary {
    pub total: u64,
    pub failure_rate: f64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BlobServiceStatus {
    pub status: ServiceStatus,
    pub backend: Option<String>,
    pub max_bucket_size: Option<u64>,
    pub multipart_bucket: Option<String>,
    pub timeouts_secs: Option<TimeoutConfigSecs>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TimeoutConfigSecs {
    pub connect: u64,
    pub io: u64,
    pub transfer_idle: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DatabaseServiceStatus {
    pub status: ServiceStatus,
    pub requests: RequestSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InterfaceServicesStatus {
    pub rest: InterfaceStatus,
    pub s3: InterfaceStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InterfaceStatus {
    pub status: ServiceStatus,
    pub bind: Option<String>,
    pub url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RealmInfoResponse {
    pub realm_id: String,
    pub description: String,
    pub metadata_replication: RealmMetadataReplicationResponse,
    pub oidc_providers: Vec<RealmOidcProviderResponse>,
    #[schema(value_type = Object)]
    pub discovery: Value,
    pub nodes: Vec<RealmNodeInfoResponse>,
    pub quota: RealmQuotaConfig,
    pub interfaces: InterfaceServicesStatus,
}

/// Realm-wide quota policy. Used both as the response for the current settings
/// and as the replace-semantics request body for updating them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmQuotaConfig {
    pub default_group_quota_bytes: Option<u64>,
    pub grace_factor_percent: u32,
    pub warn_threshold_percent: u32,
    pub group_overrides: Vec<RealmGroupQuotaOverride>,
    pub max_groups_per_user: Option<u32>,
    /// Redacted from unauthenticated `/info/realm` responses.
    pub user_group_cap_overrides: Vec<RealmUserGroupCapOverride>,
    pub max_devices_per_user: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmGroupQuotaOverride {
    pub group_id: String,
    pub quota_bytes: Option<u64>,
    pub grace_factor_percent: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmUserGroupCapOverride {
    pub user_id: String,
    pub max_groups: Option<u32>,
}

impl From<QuotaConfig> for RealmQuotaConfig {
    fn from(quota: QuotaConfig) -> Self {
        Self {
            default_group_quota_bytes: quota.default_group_quota_bytes,
            grace_factor_percent: quota.grace_factor_percent,
            warn_threshold_percent: quota.warn_threshold_percent,
            group_overrides: quota
                .group_overrides
                .into_iter()
                .map(|over| RealmGroupQuotaOverride {
                    group_id: over.group_id.to_string(),
                    quota_bytes: over.quota_bytes,
                    grace_factor_percent: over.grace_factor_percent,
                })
                .collect(),
            max_groups_per_user: quota.max_groups_per_user,
            user_group_cap_overrides: quota
                .user_group_cap_overrides
                .into_iter()
                .map(|over| RealmUserGroupCapOverride {
                    user_id: over.user_id.to_string(),
                    max_groups: over.max_groups,
                })
                .collect(),
            max_devices_per_user: quota.max_devices_per_user,
        }
    }
}

impl RealmQuotaConfig {
    fn into_quota_config(self) -> ServerResult<QuotaConfig> {
        let group_overrides = self
            .group_overrides
            .into_iter()
            .map(|over| {
                Ok(GroupQuotaOverride {
                    group_id: Ulid::from_string(&over.group_id).map_err(|_| {
                        ServerError::BadRequestReason(format!(
                            "invalid group id in group_overrides: {}",
                            over.group_id
                        ))
                    })?,
                    quota_bytes: over.quota_bytes,
                    grace_factor_percent: over.grace_factor_percent,
                })
            })
            .collect::<ServerResult<Vec<_>>>()?;
        let user_group_cap_overrides = self
            .user_group_cap_overrides
            .into_iter()
            .map(|over| {
                Ok(UserGroupCapOverride {
                    user_id: UserId::from_string(&over.user_id).map_err(|_| {
                        ServerError::BadRequestReason(format!(
                            "invalid user id in user_group_cap_overrides: {}",
                            over.user_id
                        ))
                    })?,
                    max_groups: over.max_groups,
                })
            })
            .collect::<ServerResult<Vec<_>>>()?;
        Ok(QuotaConfig {
            default_group_quota_bytes: self.default_group_quota_bytes,
            grace_factor_percent: self.grace_factor_percent,
            warn_threshold_percent: self.warn_threshold_percent,
            group_overrides,
            max_groups_per_user: self.max_groups_per_user,
            user_group_cap_overrides,
            max_devices_per_user: self.max_devices_per_user,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmMetadataReplicationResponse {
    pub default_replication_factor: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmOidcProviderResponse {
    pub id: String,
    pub issuer: String,
    pub audience: String,
    pub discovery_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmNodeInfoResponse {
    pub node_id: String,
    pub kind: RealmNodeKindInfo,
    pub configured: bool,
    pub present: bool,
    pub connection_status: RealmNodeConnectionStatus,
    /// Placement map entry (location/weight/status) when the node is mapped.
    pub placement: Option<RealmNodePlacementResponse>,
    /// Latest published node info document (labels/urls/utilization) if received.
    pub info: Option<RealmNodeInfoDocumentResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmNodePlacementResponse {
    pub location: String,
    pub weight: u32,
    pub full: bool,
    pub draining: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmNodeInfoDocumentResponse {
    pub labels: std::collections::BTreeMap<String, String>,
    pub urls: RealmNodeUrlsResponse,
    pub utilization: RealmNodeUtilizationResponse,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmNodeUrlsResponse {
    pub api: Option<String>,
    pub s3: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmNodeUtilizationResponse {
    pub storage_bytes_used: u64,
    pub documents_held: u64,
    pub load_permille: u32,
    pub heartbeat_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RealmNodeKindInfo {
    Management,
    Server,
    Local,
    User,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RealmNodeConnectionStatus {
    Connected,
    Configured,
}

impl From<&RealmNodeKind> for RealmNodeKindInfo {
    fn from(value: &RealmNodeKind) -> Self {
        match value {
            RealmNodeKind::Management => Self::Management,
            RealmNodeKind::Server => Self::Server,
            RealmNodeKind::Local => Self::Local,
            RealmNodeKind::User => Self::User,
        }
    }
}

#[utoipa::path(
    get,
    path = "/info/realm",
    tag = "info",
    responses(
        (status = 200, description = "Realm information", body = RealmInfoResponse),
        (status = 404, description = "Realm config not found", body = crate::error::ErrorResponse)
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn get_realm_info(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<RealmInfoResponse>)> {
    let config = drive(
        GetRealmConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        aruna_operations::get_realm_config::GetRealmConfigError::DocumentNotFound => {
            ServerError::NotFound
        }
        other => ServerError::InternalError(other.to_string()),
    })?;
    let present_nodes = load_realm_presence_best_effort(&state).await;
    let node_info_docs = load_node_info_documents_best_effort(&state, &config).await;
    let response = map_realm_info_response(
        &state,
        config,
        present_nodes,
        interface_services_status(&state).await,
        node_info_docs,
        auth.as_ref()
            .is_some_and(|auth| auth.realm_id == state.get_realm_id()),
    )?;
    Ok((StatusCode::OK, Json(response)))
}

async fn load_node_info_documents_best_effort(
    state: &ServerState,
    config: &RealmConfigDocument,
) -> BTreeMap<aruna_core::NodeId, aruna_core::structs::NodeInfoDocument> {
    let node_ids: Vec<aruna_core::NodeId> = config
        .nodes
        .iter()
        .filter_map(|node| node.node_id.parse().ok())
        .collect();
    match aruna_operations::node_info::read_node_info_documents(
        &state.get_ctx().storage_handle,
        &node_ids,
    )
    .await
    {
        Ok(documents) => documents,
        Err(error) => {
            warn!(error = %error, "Failed to load node info documents for realm info");
            BTreeMap::new()
        }
    }
}

fn map_node_info_document(
    document: &aruna_core::structs::NodeInfoDocument,
) -> RealmNodeInfoDocumentResponse {
    RealmNodeInfoDocumentResponse {
        labels: document.labels.clone(),
        urls: RealmNodeUrlsResponse {
            api: document.urls.api.clone(),
            s3: document.urls.s3.clone(),
        },
        utilization: RealmNodeUtilizationResponse {
            storage_bytes_used: document.utilization.storage_bytes_used,
            documents_held: document.utilization.documents_held,
            load_permille: document.utilization.load_permille,
            heartbeat_at_ms: document.utilization.heartbeat_at_ms,
        },
        updated_at_ms: document.updated_at_ms,
    }
}

async fn authorize_realm_config_admin(
    state: &Arc<ServerState>,
    auth: Option<AuthContext>,
) -> ServerResult<AuthContext> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    let realm_id = state.get_realm_id();
    if auth.realm_id != realm_id || !state.is_management_node() {
        return Err(ServerError::Forbidden);
    }

    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth.clone(),
            path: format!("/{realm_id}/admin/config"),
            required_permission: Permission::WRITE,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
    if !allowed {
        return Err(ServerError::Forbidden);
    }

    Ok(auth)
}

#[utoipa::path(
    put,
    path = "/info/realm/quota",
    tag = "info",
    request_body = RealmQuotaConfig,
    responses(
        (status = 200, description = "Updated realm quota configuration", body = RealmQuotaConfig),
        (status = 400, description = "Invalid quota configuration", body = crate::error::ErrorResponse),
        (status = 403, description = "Caller is not a realm config admin", body = crate::error::ErrorResponse),
        (status = 404, description = "Realm config not found", body = crate::error::ErrorResponse),
        (status = 409, description = "Concurrent quota update conflict", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_realm_quota(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<RealmQuotaConfig>,
) -> ServerResult<(StatusCode, Json<RealmQuotaConfig>)> {
    let auth = authorize_realm_config_admin(&state, auth).await?;
    let quota = request.into_quota_config()?;
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: auth.user_id,
        realm_id: auth.realm_id,
    };
    let stored = drive(
        SetRealmQuotaOperation::new(SetRealmQuotaConfig { actor, quota }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_set_realm_quota_error)?;
    Ok((StatusCode::OK, Json(RealmQuotaConfig::from(stored.quota))))
}

fn map_set_realm_quota_error(error: SetRealmQuotaError) -> ServerError {
    match error {
        SetRealmQuotaError::RealmConfigNotFound => ServerError::NotFound,
        SetRealmQuotaError::InvalidQuota { reason } => ServerError::BadRequestReason(reason),
        SetRealmQuotaError::StorageError(StorageError::TransactionConflict) => {
            ServerError::Conflict("concurrent realm quota update conflict; retry".to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

/// Storage usage. The flat fields report this node's local counters (kept for
/// backward compatibility) and are always present. `realm` reports the
/// realm-wide total summed across every node and is only included for
/// authenticated callers, so unauthenticated requests never disclose it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct UsageResponse {
    pub buckets: u64,
    pub objects: u64,
    pub stored_blobs: u64,
    pub stored_bytes: u64,
    pub logical_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub realm: Option<UsageTotals>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota: Option<GroupQuotaStatus>,
}

/// Per-group quota status derived from the realm quota config. Attached only to
/// the group usage endpoint; `/info/usage` and the plain constructors leave it
/// `None` so their output is unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct GroupQuotaStatus {
    /// Effective pre-grace group quota (override else default). `None` = unlimited.
    pub quota_bytes: Option<u64>,
    /// Enforced hard cap (quota x grace). `None` = unlimited.
    pub ceiling_bytes: Option<u64>,
    pub warn_threshold_percent: u32,
    /// True when the group's realm-wide `logical_bytes` has reached the
    /// fractional `quota_bytes * warn_threshold_percent / 100` threshold; always
    /// false when unlimited.
    pub warning: bool,
}

impl GroupQuotaStatus {
    /// Builds the status from the realm quota config and the group's realm-wide
    /// `logical_bytes` — the same counter the put-object `QuotaGate` enforces.
    pub fn resolve(
        quota: &QuotaConfig,
        group_id: &aruna_core::types::GroupId,
        realm_group_logical_bytes: u64,
    ) -> Self {
        let quota_bytes = quota.effective_group_quota_bytes(group_id);
        let warning = match quota_bytes {
            Some(limit) => {
                u128::from(realm_group_logical_bytes) * 100
                    >= u128::from(limit) * u128::from(quota.warn_threshold_percent)
            }
            None => false,
        };
        Self {
            quota_bytes,
            ceiling_bytes: quota.effective_group_ceiling(group_id),
            warn_threshold_percent: quota.warn_threshold_percent,
            warning,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UsageTotals {
    pub buckets: u64,
    pub objects: u64,
    pub stored_blobs: u64,
    pub stored_bytes: u64,
    pub logical_bytes: u64,
}

impl From<UsageCounters> for UsageTotals {
    fn from(counters: UsageCounters) -> Self {
        Self {
            buckets: counters.buckets,
            objects: counters.objects,
            stored_blobs: counters.stored_blobs,
            stored_bytes: counters.stored_bytes,
            logical_bytes: counters.logical_bytes,
        }
    }
}

impl UsageResponse {
    pub fn new(local: UsageCounters, realm: UsageCounters) -> Self {
        Self::with_realm(local, Some(realm.into()))
    }

    pub fn with_realm(local: UsageCounters, realm: Option<UsageTotals>) -> Self {
        Self {
            buckets: local.buckets,
            objects: local.objects,
            stored_blobs: local.stored_blobs,
            stored_bytes: local.stored_bytes,
            logical_bytes: local.logical_bytes,
            realm,
            quota: None,
        }
    }
}

pub async fn load_usage_counters(state: &ServerState, key: Vec<u8>) -> ServerResult<UsageCounters> {
    drive(LoadUsageCountersOperation::new(key), &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))
}

pub async fn load_realm_usage(
    state: &ServerState,
    scope: RealmUsageScope,
) -> ServerResult<UsageCounters> {
    aruna_operations::usage_stats::load_realm_usage(&state.get_ctx(), state.get_node_id(), scope)
        .await
        .map_err(ServerError::InternalError)
}

#[utoipa::path(
    get,
    path = "/info/usage",
    tag = "info",
    responses(
        (
            status = 200,
            description = "Local storage usage always; realm-wide totals only for authenticated callers",
            body = UsageResponse
        )
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn get_usage(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<UsageResponse>)> {
    let local = load_usage_counters(&state, USAGE_GLOBAL_KEY.to_vec()).await?;
    let realm = match auth {
        Some(_) => Some(
            load_realm_usage(&state, RealmUsageScope::Global)
                .await?
                .into(),
        ),
        None => None,
    };
    Ok((
        StatusCode::OK,
        Json(UsageResponse::with_realm(local, realm)),
    ))
}

fn map_realm_info_response(
    state: &ServerState,
    config: RealmConfigDocument,
    present_nodes: HashSet<aruna_core::NodeId>,
    interfaces: InterfaceServicesStatus,
    node_info_docs: BTreeMap<aruna_core::NodeId, aruna_core::structs::NodeInfoDocument>,
    include_user_quota_overrides: bool,
) -> ServerResult<RealmInfoResponse> {
    let discovery = serde_json::to_value(&config.discovery)
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let current_node = state.get_node_id();
    let nodes = config
        .nodes
        .iter()
        .map(|node| {
            let parsed = node.node_id.parse::<aruna_core::NodeId>().ok();
            let is_current = node.node_id == current_node.to_string();
            let present =
                is_current || parsed.is_some_and(|node_id| present_nodes.contains(&node_id));
            let placement = parsed
                .and_then(|node_id| config.placement_entry(node_id))
                .map(|entry| RealmNodePlacementResponse {
                    location: entry.effective_location().to_string(),
                    weight: entry.weight,
                    full: entry.full,
                    draining: entry.draining,
                });
            let info = parsed
                .and_then(|node_id| node_info_docs.get(&node_id))
                .map(map_node_info_document);
            RealmNodeInfoResponse {
                node_id: node.node_id.clone(),
                kind: RealmNodeKindInfo::from(&node.kind),
                configured: true,
                present,
                connection_status: if present {
                    RealmNodeConnectionStatus::Connected
                } else {
                    RealmNodeConnectionStatus::Configured
                },
                placement,
                info,
            }
        })
        .collect();

    let mut quota = RealmQuotaConfig::from(config.quota);
    if !include_user_quota_overrides {
        quota.user_group_cap_overrides.clear();
    }

    Ok(RealmInfoResponse {
        realm_id: config.realm_id.to_string(),
        description: config.description,
        metadata_replication: RealmMetadataReplicationResponse {
            default_replication_factor: config.metadata_replication.default_replication_factor,
        },
        oidc_providers: config
            .oidc_providers
            .into_iter()
            .map(|provider| RealmOidcProviderResponse {
                id: provider.id,
                issuer: provider.issuer,
                audience: provider.audience,
                discovery_url: provider.discovery_url,
            })
            .collect(),
        discovery,
        nodes,
        quota,
        interfaces,
    })
}

async fn load_realm_presence_best_effort(state: &ServerState) -> HashSet<aruna_core::NodeId> {
    match drive(
        GetRealmNodesOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(mut nodes) => {
            nodes.insert(state.get_node_id());
            nodes
        }
        Err(error) => {
            warn!(error = %error, "realm node discovery failed for realm info response");
            HashSet::from([state.get_node_id()])
        }
    }
}

async fn interface_services_status(state: &ServerState) -> InterfaceServicesStatus {
    let interface_runtime = state.interface_state().await;
    InterfaceServicesStatus {
        rest: match interface_runtime.rest {
            Some(rest) => InterfaceStatus {
                status: ServiceStatus::Available,
                bind: Some(rest.bind_address.to_string()),
                url: Some(rest.api_base_url),
            },
            None => InterfaceStatus {
                status: ServiceStatus::Unavailable,
                bind: None,
                url: None,
            },
        },
        s3: match interface_runtime.s3 {
            Some(s3) => InterfaceStatus {
                status: ServiceStatus::Available,
                bind: Some(s3.bind_address.to_string()),
                url: Some(s3.base_url),
            },
            None => InterfaceStatus {
                status: ServiceStatus::Unavailable,
                bind: None,
                url: None,
            },
        },
    }
}

#[utoipa::path(
    get,
    path = "/info",
    tag = "info",
    responses(
        (status = 200, description = "Node information", body = InfoResponse)
    )
)]
pub async fn get_info(State(state): State<Arc<ServerState>>) -> (StatusCode, Json<InfoResponse>) {
    let ctx = state.get_ctx();
    let observability = load_node_observability_status(ctx.as_ref()).await;

    let (network, my_addresses, connections, warnings) = match observability.network {
        Some(info) => (
            NetworkServiceStatus {
                status: ServiceStatus::Available,
                discovery: info.discovery_methods,
                relay: Some(info.relay_method),
                relay_urls: info.relay_urls,
                routing_table_size: info.routing_table_size,
                requests: RequestSummary::from_state(&info.requests),
            },
            info.endpoint_addr
                .addrs
                .iter()
                .map(transport_addr_to_string)
                .collect(),
            info.connections
                .iter()
                .map(|peer| PeerConnectionInfo {
                    peer_id: peer.node_id.to_string(),
                    status: PeerStatus::from(peer.status),
                    active_addresses: peer
                        .active_addresses
                        .iter()
                        .map(|address| ConnectionAddressInfo {
                            status: AddressStatus::from(address.status),
                            address: address.address.clone(),
                            rtt_ms: address.rtt_ms,
                            protocol_connections: address
                                .protocol_connections
                                .iter()
                                .map(|connection| ProtocolConnectionInfo {
                                    connection_id: connection.connection_id,
                                    protocol: protocol_name(connection.alpn),
                                    side: side_name(connection.side),
                                    status: ProtocolConnectionStatus::Open,
                                })
                                .collect(),
                        })
                        .collect(),
                    last_error: peer.last_error.clone(),
                    next_retry_secs: peer.next_retry_in_secs,
                })
                .collect(),
            info.warnings,
        ),
        None => (
            NetworkServiceStatus {
                status: ServiceStatus::Unavailable,
                discovery: Vec::new(),
                relay: None,
                relay_urls: Vec::new(),
                routing_table_size: None,
                requests: RequestSummary::default(),
            },
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ),
    };

    let blob = match observability.blob {
        Some(info) => BlobServiceStatus {
            status: ServiceStatus::from(info.status),
            backend: Some(info.backend_type.to_string()),
            max_bucket_size: info.max_bucket_size,
            multipart_bucket: info.multipart_bucket,
            timeouts_secs: Some(TimeoutConfigSecs {
                connect: info.timeouts.control_plane_connect_timeout.as_secs(),
                io: info.timeouts.control_plane_io_timeout.as_secs(),
                transfer_idle: info.timeouts.transfer_idle_timeout.as_secs(),
            }),
        },
        None => BlobServiceStatus {
            status: ServiceStatus::NotConfigured,
            backend: None,
            max_bucket_size: None,
            multipart_bucket: None,
            timeouts_secs: None,
        },
    };

    let interfaces = interface_services_status(&state).await;

    let database = DatabaseServiceStatus {
        status: ServiceStatus::from(observability.database.status),
        requests: RequestSummary::from_state(&observability.database.requests),
    };

    (
        StatusCode::OK,
        Json(InfoResponse {
            node: NodeStatus {
                status: ServiceStatus::Available,
                realm_id: state.get_realm_id().to_string(),
                peer_id: state.get_node_id().to_string(),
                capabilities: NodeCapabilityKind::from(state.node_capabilities()),
            },
            api_version: env!("CARGO_PKG_VERSION").to_string(),
            portal: state.portal_status().await,
            my_addresses,
            connections,
            services: ServicesStatus {
                network,
                blob,
                database,
                interfaces,
            },
            warnings,
        }),
    )
}

impl RequestSummary {
    fn default() -> Self {
        Self::from_counts(0, 0, None)
    }

    fn from_state(state: &RequestSummaryState) -> Self {
        Self::from_counts(state.total, state.failures, state.last_error.clone())
    }

    fn from_counts(total: u64, failures: u64, last_error: Option<String>) -> Self {
        Self {
            total,
            failure_rate: if total == 0 {
                0.0
            } else {
                failures as f64 / total as f64
            },
            last_error,
        }
    }
}

impl From<PeerConnectionStatus> for PeerStatus {
    fn from(status: PeerConnectionStatus) -> Self {
        match status {
            PeerConnectionStatus::Connected => Self::Connected,
            PeerConnectionStatus::Known => Self::Known,
            PeerConnectionStatus::Unreachable => Self::Unreachable,
        }
    }
}

impl From<ConnectionAddressStatus> for AddressStatus {
    fn from(status: ConnectionAddressStatus) -> Self {
        match status {
            ConnectionAddressStatus::Active => Self::Active,
            ConnectionAddressStatus::NotAssigned => Self::NotAssigned,
        }
    }
}

fn protocol_name(alpn: Option<Alpn>) -> Option<String> {
    alpn.map(|alpn| match alpn {
        Alpn::Dht => "dht".to_string(),
        Alpn::Bao => "bao".to_string(),
        Alpn::DocumentSync => "document_sync".to_string(),
        Alpn::Metadata => "metadata".to_string(),
        Alpn::Notification => "notification".to_string(),
    })
}

fn side_name(side: iroh::endpoint::Side) -> String {
    match side {
        iroh::endpoint::Side::Client => "client".to_string(),
        iroh::endpoint::Side::Server => "server".to_string(),
    }
}

fn transport_addr_to_string(addr: &iroh::TransportAddr) -> String {
    match addr {
        iroh::TransportAddr::Ip(addr) => addr.to_string(),
        iroh::TransportAddr::Relay(url) => url.to_string(),
        _ => format!("{addr:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BlobServiceStatus, DatabaseServiceStatus, InfoResponse, InterfaceServicesStatus,
        InterfaceStatus, NetworkServiceStatus, NodeCapabilityKind, NodeStatus, PortalStatus,
        RealmQuotaConfig, RealmUserGroupCapOverride, RequestSummary, ServiceStatus, ServicesStatus,
        get_info, get_realm_info, get_usage, map_set_realm_quota_error, set_realm_quota,
    };
    use crate::error::ServerError;
    use crate::openapi::ApiDoc;
    use crate::server_state::ServerState;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::errors::StorageError;
    use aruna_core::structs::{Actor, AuthContext, NodeCapabilities, QuotaConfig, RealmId};
    use aruna_operations::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_operations::set_realm_quota::SetRealmQuotaError;
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::{Extension, Json};
    use ed25519_dalek::SigningKey;
    use std::sync::Arc;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    async fn setup_state() -> (Arc<ServerState>, TempDir) {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });

        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let realm_signing_key = SigningKey::generate(&mut csprng);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let node_id = iroh::SecretKey::generate().public();

        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm_id,
                node_id,
                NodeCapabilities::local_node(realm_id).unwrap(),
                false,
                None,
            )
            .await,
        );

        (state, tempdir)
    }

    #[tokio::test]
    async fn get_info_returns_unavailable_optional_statuses_when_handles_are_missing() {
        let (state, _tempdir) = setup_state().await;
        let baseline = state.get_ctx().storage_handle.snapshot_metrics();
        let expected_node = NodeStatus {
            status: ServiceStatus::Available,
            realm_id: state.get_realm_id().to_string(),
            peer_id: state.get_node_id().to_string(),
            capabilities: NodeCapabilityKind::Local,
        };

        let (status, Json(response)) = get_info(State(state)).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            response,
            InfoResponse {
                node: expected_node,
                api_version: env!("CARGO_PKG_VERSION").to_string(),
                portal: PortalStatus {
                    installed: false,
                    mode: "disabled".to_string(),
                    version: None,
                    source: None,
                    url: None,
                    checksum: None,
                    fetched_at: None,
                    last_error: None,
                },
                my_addresses: Vec::new(),
                connections: Vec::new(),
                services: ServicesStatus {
                    network: NetworkServiceStatus {
                        status: ServiceStatus::Unavailable,
                        discovery: Vec::new(),
                        relay: None,
                        relay_urls: Vec::new(),
                        routing_table_size: None,
                        requests: RequestSummary {
                            total: 0,
                            failure_rate: 0.0,
                            last_error: None,
                        },
                    },
                    blob: BlobServiceStatus {
                        status: ServiceStatus::NotConfigured,
                        backend: None,
                        max_bucket_size: None,
                        multipart_bucket: None,
                        timeouts_secs: None,
                    },
                    database: DatabaseServiceStatus {
                        status: ServiceStatus::Available,
                        requests: RequestSummary {
                            total: baseline.requests_total,
                            failure_rate: if baseline.requests_total == 0 {
                                0.0
                            } else {
                                baseline.failed_total as f64 / baseline.requests_total as f64
                            },
                            last_error: baseline.last_error,
                        },
                    },
                    interfaces: InterfaceServicesStatus {
                        rest: InterfaceStatus {
                            status: ServiceStatus::Unavailable,
                            bind: None,
                            url: None,
                        },
                        s3: InterfaceStatus {
                            status: ServiceStatus::Unavailable,
                            bind: None,
                            url: None,
                        },
                    },
                },
                warnings: Vec::new(),
            }
        );
    }

    #[tokio::test]
    async fn get_info_reports_registered_interface_paths() {
        let (state, _tempdir) = setup_state().await;
        state
            .register_rest_interface("0.0.0.0:3000".parse().unwrap())
            .await;
        state
            .register_s3_interface("0.0.0.0:1337".parse().unwrap(), "127.0.0.1:1337")
            .await;

        let (status, Json(response)) = get_info(State(state)).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            response.services.interfaces,
            InterfaceServicesStatus {
                rest: InterfaceStatus {
                    status: ServiceStatus::Available,
                    bind: Some("0.0.0.0:3000".to_string()),
                    url: Some("http://127.0.0.1:3000/api/v1".to_string()),
                },
                s3: InterfaceStatus {
                    status: ServiceStatus::Available,
                    bind: Some("0.0.0.0:1337".to_string()),
                    url: Some("http://127.0.0.1:1337".to_string()),
                },
            }
        );
    }

    #[tokio::test]
    async fn get_info_reports_storage_error_metrics() {
        let (state, _tempdir) = setup_state().await;
        let ctx = state.get_ctx();
        let baseline = ctx.storage_handle.snapshot_metrics();

        let _ = ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: "missing".to_string(),
                key: b"key".to_vec().into(),
                txn_id: Some(ulid::Ulid::new()),
            })
            .await;

        let (status, Json(response)) = get_info(State(state)).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            response.services.database,
            DatabaseServiceStatus {
                status: ServiceStatus::Available,
                requests: RequestSummary {
                    total: baseline.requests_total + 1,
                    failure_rate: (baseline.failed_total + 1) as f64
                        / (baseline.requests_total + 1) as f64,
                    last_error: Some("Transaction not found".to_string()),
                },
            }
        );
    }

    #[test]
    fn openapi_includes_info_path() {
        let openapi = ApiDoc::openapi();

        assert!(openapi.paths.paths.contains_key("/info"));
    }

    async fn seed_usage_state(state: &Arc<ServerState>) {
        use aruna_core::keyspaces::{USAGE_NODE_STATS_KEYSPACE, USAGE_STATS_KEYSPACE};
        use aruna_core::structs::{
            NodeUsageSnapshot, node_usage_global_key, usage_global_shard_key,
        };

        let ctx = state.get_ctx();
        // This node's live local total.
        ctx.storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: USAGE_STATS_KEYSPACE.to_string(),
                key: usage_global_shard_key(0).into(),
                value: aruna_core::structs::UsageCounters {
                    buckets: 2,
                    ..Default::default()
                }
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: None,
            })
            .await;
        // A remote node's replicated snapshot.
        let remote = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
        ctx.storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: USAGE_NODE_STATS_KEYSPACE.to_string(),
                key: node_usage_global_key(remote).into(),
                value: NodeUsageSnapshot {
                    node_id: remote,
                    counters: aruna_core::structs::UsageCounters {
                        buckets: 3,
                        ..Default::default()
                    },
                }
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: None,
            })
            .await;
    }

    fn test_auth_context(state: &Arc<ServerState>) -> AuthContext {
        AuthContext {
            user_id: UserId::local(Ulid::new(), state.get_realm_id()),
            realm_id: state.get_realm_id(),
            path_restrictions: None,
        }
    }

    #[tokio::test]
    async fn get_usage_reports_realm_totals_for_authenticated_callers() {
        let (state, _tempdir) = setup_state().await;
        seed_usage_state(&state).await;

        let auth = test_auth_context(&state);
        let (status, Json(response)) = get_usage(State(state), Extension(Some(auth)))
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response.buckets, 2, "flat fields report local total");
        let realm = response.realm.expect("authenticated caller sees realm");
        assert_eq!(realm.buckets, 5, "realm sums local and remote");
    }

    #[tokio::test]
    async fn get_usage_omits_realm_totals_for_unauthenticated_callers() {
        let (state, _tempdir) = setup_state().await;
        seed_usage_state(&state).await;

        let (status, Json(response)) = get_usage(State(state), Extension(None)).await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            response.buckets, 2,
            "flat local fields stay unauthenticated"
        );
        assert!(
            response.realm.is_none(),
            "unauthenticated callers must not see realm-wide totals"
        );
    }

    #[test]
    fn group_quota_status_reports_warning_and_unlimited() {
        let group = Ulid::new();
        let unlimited_group = Ulid::new();
        let quota = QuotaConfig {
            default_group_quota_bytes: Some(1_000),
            grace_factor_percent: 110,
            warn_threshold_percent: 85,
            group_overrides: vec![aruna_core::structs::GroupQuotaOverride {
                group_id: unlimited_group,
                quota_bytes: None,
                grace_factor_percent: None,
            }],
            ..QuotaConfig::default()
        };

        // Finite default quota, usage below the 850-byte warn threshold.
        let below = super::GroupQuotaStatus::resolve(&quota, &group, 800);
        assert_eq!(below.quota_bytes, Some(1_000));
        assert_eq!(below.ceiling_bytes, Some(1_100));
        assert_eq!(below.warn_threshold_percent, 85);
        assert!(!below.warning);

        // At the threshold the warning fires.
        let at = super::GroupQuotaStatus::resolve(&quota, &group, 850);
        assert!(at.warning);

        // An override with quota_bytes: None is unlimited and never warns.
        let unlimited = super::GroupQuotaStatus::resolve(&quota, &unlimited_group, u64::MAX);
        assert_eq!(unlimited.quota_bytes, None);
        assert_eq!(unlimited.ceiling_bytes, None);
        assert!(!unlimited.warning);
    }

    #[test]
    fn group_quota_status_uses_fractional_warn_threshold_without_flooring() {
        let group = Ulid::new();
        let quota = QuotaConfig {
            default_group_quota_bytes: Some(3),
            warn_threshold_percent: 85,
            ..QuotaConfig::default()
        };

        let below = super::GroupQuotaStatus::resolve(&quota, &group, 2);
        assert!(!below.warning);
        let at = super::GroupQuotaStatus::resolve(&quota, &group, 3);
        assert!(at.warning);

        let tiny_quota = QuotaConfig {
            default_group_quota_bytes: Some(1),
            warn_threshold_percent: 85,
            ..QuotaConfig::default()
        };
        let zero = super::GroupQuotaStatus::resolve(&tiny_quota, &group, 0);
        assert!(!zero.warning);
    }

    #[test]
    fn openapi_includes_realm_quota_path() {
        let openapi = ApiDoc::openapi();

        assert!(openapi.paths.paths.contains_key("/info/realm/quota"));
    }

    #[test]
    fn set_realm_quota_transaction_conflict_maps_to_http_conflict() {
        let error = map_set_realm_quota_error(SetRealmQuotaError::StorageError(
            StorageError::TransactionConflict,
        ));

        assert!(matches!(
            error,
            ServerError::Conflict(message) if message.contains("retry")
        ));
    }

    async fn setup_management_state() -> (Arc<ServerState>, RealmId, UserId, TempDir) {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
        });

        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let realm_signing_key = SigningKey::generate(&mut csprng);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let user_id = UserId::local(Ulid::new(), realm_id);
        let node_id = iroh::SecretKey::generate().public();

        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: Actor {
                    node_id,
                    user_id,
                    realm_id,
                },
                realm_description: "Realm".to_string(),
                oidc_providers: vec![],
                node_location: None,
                node_weight: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap();
        drive(
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                actor: Actor {
                    node_id,
                    user_id,
                    realm_id,
                },
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm_id,
                node_id,
                NodeCapabilities::management_node(realm_signing_key).unwrap(),
                false,
                None,
            )
            .await,
        );

        (state, realm_id, user_id, tempdir)
    }

    #[tokio::test]
    async fn set_realm_quota_requires_authentication() {
        let (state, _realm_id, _admin, _tempdir) = setup_management_state().await;
        let body = RealmQuotaConfig::from(QuotaConfig::default());

        let error = set_realm_quota(State(state), Extension(None), Json(body))
            .await
            .unwrap_err();

        assert!(matches!(error, ServerError::Unauthorized));
    }

    #[tokio::test]
    async fn set_realm_quota_rejects_non_admin() {
        let (state, realm_id, _admin, _tempdir) = setup_management_state().await;
        let stranger = AuthContext {
            user_id: UserId::local(Ulid::new(), realm_id),
            realm_id,
            path_restrictions: None,
        };
        let body = RealmQuotaConfig::from(QuotaConfig::default());

        let error = set_realm_quota(State(state), Extension(Some(stranger)), Json(body))
            .await
            .unwrap_err();

        assert!(matches!(error, ServerError::Forbidden));
    }

    #[tokio::test]
    async fn admin_sets_and_reads_realm_quota() {
        let (state, realm_id, admin, _tempdir) = setup_management_state().await;
        let auth = AuthContext {
            user_id: admin,
            realm_id,
            path_restrictions: None,
        };
        let mut body = RealmQuotaConfig::from(QuotaConfig::default());
        body.default_group_quota_bytes = Some(4096);

        let (status, Json(stored)) = set_realm_quota(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Json(body),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(stored.default_group_quota_bytes, Some(4096));
        assert_eq!(stored.max_devices_per_user, None);

        let (status, Json(info)) = get_realm_info(State(state), Extension(Some(auth)))
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(info.quota.default_group_quota_bytes, Some(4096));
        assert_eq!(info.quota.max_devices_per_user, None);
    }

    #[tokio::test]
    async fn get_realm_info_redacts_user_quota_overrides_for_anonymous_callers() {
        let (state, realm_id, admin, _tempdir) = setup_management_state().await;
        let auth = AuthContext {
            user_id: admin,
            realm_id,
            path_restrictions: None,
        };
        let mut body = RealmQuotaConfig::from(QuotaConfig::default());
        body.user_group_cap_overrides = vec![RealmUserGroupCapOverride {
            user_id: admin.to_string(),
            max_groups: Some(1),
        }];

        let (_status, Json(_stored)) = set_realm_quota(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Json(body),
        )
        .await
        .unwrap();

        let (_status, Json(anonymous)) = get_realm_info(State(state.clone()), Extension(None))
            .await
            .unwrap();
        assert!(anonymous.quota.user_group_cap_overrides.is_empty());

        let (_status, Json(authenticated)) = get_realm_info(State(state), Extension(Some(auth)))
            .await
            .unwrap();
        assert_eq!(authenticated.quota.user_group_cap_overrides.len(), 1);
        assert_eq!(
            authenticated.quota.user_group_cap_overrides[0].user_id,
            admin.to_string()
        );
    }

    #[tokio::test]
    async fn get_realm_info_includes_placement_and_node_info() {
        use aruna_core::keyspaces::NODE_INFO_KEYSPACE;
        use aruna_core::structs::{
            NodeInfoDocument, NodeUrls, NodeUtilization, node_info_storage_key,
        };

        let (state, _realm_id, _admin, _tempdir) = setup_management_state().await;
        let node_id = state.get_node_id();

        // The creating node's placement entry is seeded at realm creation with
        // the default location/weight. Publish a node info document for it too.
        let document = NodeInfoDocument {
            node_id,
            labels: std::collections::BTreeMap::from([("tier".to_string(), "hot".to_string())]),
            urls: NodeUrls {
                api: None,
                s3: Some("s3.example".to_string()),
            },
            utilization: NodeUtilization {
                storage_bytes_used: 4_096,
                documents_held: 0,
                load_permille: 0,
                heartbeat_at_ms: 1_700_000_000_000,
            },
            updated_at_ms: 1_700_000_000_500,
        };
        state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: NODE_INFO_KEYSPACE.to_string(),
                key: node_info_storage_key(node_id).into(),
                value: document.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;

        let (status, Json(info)) = get_realm_info(State(state), Extension(None)).await.unwrap();
        assert_eq!(status, StatusCode::OK);
        let node = info
            .nodes
            .iter()
            .find(|node| node.node_id == node_id.to_string())
            .expect("creating node present in realm info");

        let placement = node.placement.as_ref().expect("placement entry present");
        assert_eq!(placement.location, "default");
        assert_eq!(placement.weight, 100);
        assert!(!placement.full);
        assert!(!placement.draining);

        let node_info = node.info.as_ref().expect("node info document present");
        assert_eq!(node_info.labels.get("tier"), Some(&"hot".to_string()));
        assert_eq!(node_info.urls.s3.as_deref(), Some("s3.example"));
        assert_eq!(node_info.utilization.storage_bytes_used, 4_096);
    }

    #[tokio::test]
    async fn set_realm_quota_surfaces_invalid_reason_in_bad_request_body() {
        use axum::response::IntoResponse;

        let (state, realm_id, admin, _tempdir) = setup_management_state().await;
        let auth = AuthContext {
            user_id: admin,
            realm_id,
            path_restrictions: None,
        };
        let mut body = RealmQuotaConfig::from(QuotaConfig::default());
        body.warn_threshold_percent = 0;

        let error = set_realm_quota(State(state), Extension(Some(auth)), Json(body))
            .await
            .unwrap_err();
        assert!(matches!(error, ServerError::BadRequestReason(_)));

        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: crate::error::ErrorResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            parsed.code.as_deref(),
            Some("Bad request"),
            "machine code stays identical to plain BadRequest"
        );
        assert!(
            parsed.error.contains("warn_threshold_percent"),
            "body must carry the validation reason, got: {}",
            parsed.error
        );
    }

    #[tokio::test]
    async fn set_realm_quota_rejects_unsupported_device_cap() {
        let (state, realm_id, admin, _tempdir) = setup_management_state().await;
        let auth = AuthContext {
            user_id: admin,
            realm_id,
            path_restrictions: None,
        };
        let mut body = RealmQuotaConfig::from(QuotaConfig::default());
        body.max_devices_per_user = Some(2);

        let error = set_realm_quota(State(state), Extension(Some(auth)), Json(body))
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ServerError::BadRequestReason(reason) if reason.contains("max_devices_per_user")
        ));
    }
}
