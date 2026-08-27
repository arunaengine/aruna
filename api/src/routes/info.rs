use crate::auth::{ensure_permission, permission_granted, require_realm_auth};
use crate::error::{ServerError, ServerResult};
pub use crate::server_state::PortalStatus;
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::alpn::Alpn;
use aruna_core::errors::StorageError;
use aruna_core::structs::{
    Actor, AuthContext, GroupQuotaOverride, Permission, PlacementScope, QuotaConfig,
    UserGroupCapOverride, policy_admin_path,
};
use aruna_core::structs::{BackendRef, USAGE_GLOBAL_KEY, UsageCounters};
use aruna_core::structs::{ConnectionAddressStatus, PeerConnectionStatus, RequestSummaryState};
use aruna_core::structs::{RealmConfigDocument, RealmNodeKind};
use aruna_core::util::unix_timestamp_millis;
use aruna_operations::allocate_handle::{HandleAllocationError, provision_metadata_binding};
use aruna_operations::driver::{backend_used_bytes, drive};
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::get_realm_nodes::{
    GetRealmNodesOperation, REALM_DISCOVERY_TIMEOUT, RealmPresence,
};
use aruna_operations::metadata::stats::{count_realm_documents, count_realm_groups};
use aruna_operations::metadata::{MetadataHandle, PeerContacts};
use aruna_operations::mutate_realm_placement::{
    MutateRealmPlacementConfig, MutateRealmPlacementError, RealmPlacementMutation,
    drive_realm_placement_mutation,
};
use aruna_operations::placement::transition::transition_health;
use aruna_operations::set_realm_quota::{
    SetRealmQuotaConfig, SetRealmQuotaError, SetRealmQuotaOperation,
};
use aruna_operations::status::load_node_observability_status;
use aruna_operations::usage_stats::{LoadUsageCountersOperation, RealmUsageScope};
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tracing::warn;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(
    tags((name = "info", description = "Node information endpoints"))
)]
pub struct InfoApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(InfoApiDoc::openapi())
        .routes(routes!(get_info))
        .routes(routes!(get_realm_info))
        .routes(routes!(get_realm_placement, mutate_realm_placement))
        .routes(routes!(set_realm_quota))
        .routes(routes!(get_usage))
}

/// Node information. `node.status`, `node.realm_id` and `api_version` are public:
/// what a client needs to health check the node and learn which realm to
/// authenticate against. Node identity, addresses and peer topology need a token
/// of this realm; backend detail, request metrics and warnings need a realm
/// config admin. Gated values are absent, never restructured, so a client keeps
/// parsing the shape it always parsed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct InfoResponse {
    pub node: NodeStatus,
    pub api_version: String,
    /// Portal deployment detail. Realm config admins only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub portal: Option<PortalStatus>,
    /// Node listen addresses. Realm-authenticated callers only, else empty.
    pub my_addresses: Vec<String>,
    /// Peer topology. Realm-authenticated callers only, else empty.
    pub connections: Vec<PeerConnectionInfo>,
    pub services: ServicesStatus,
    /// Operational warnings. Realm config admins only, else empty.
    pub warnings: Vec<String>,
}

/// Node health and identity. `status` and `realm_id` are public; `peer_id` and
/// `capabilities` need a token of this realm.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct NodeStatus {
    pub status: ServiceStatus,
    pub realm_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<NodeCapabilityKind>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum NodeCapabilityKind {
    Management,
    Server,
    User,
}

impl From<&aruna_core::structs::NodeCapabilities> for NodeCapabilityKind {
    fn from(capabilities: &aruna_core::structs::NodeCapabilities) -> Self {
        match capabilities {
            aruna_core::structs::NodeCapabilities::Management { .. } => Self::Management,
            aruna_core::structs::NodeCapabilities::Server { .. } => Self::Server,
            aruna_core::structs::NodeCapabilities::User { .. } => Self::User,
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
    /// Populated for realm config admins only.
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

/// Node backend services. `interfaces` is always present; `network` needs a
/// token of this realm, `blob` and `database` a realm config admin.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ServicesStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<NetworkServiceStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob: Option<BlobServiceStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<DatabaseServiceStatus>,
    pub interfaces: InterfaceServicesStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct NetworkServiceStatus {
    pub status: ServiceStatus,
    pub discovery: Vec<String>,
    pub relay: Option<String>,
    pub relay_urls: Vec<String>,
    pub routing_table_size: Option<usize>,
    /// Request metrics and last error. Realm config admins only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requests: Option<RequestSummary>,
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
    /// Every registered backend. The aggregate `status` above is the default
    /// backend's, so single-backend consumers keep one headline signal.
    pub backends: Vec<BackendStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BackendStatus {
    pub name: String,
    pub backend: String,
    pub class: Option<String>,
    /// Whether tenant routing rules may target this backend's class.
    pub allow_tenants: bool,
    /// Operator allowance for user data on this backend. A write routed here is
    /// refused once `used_bytes` reaches it.
    pub quota_bytes: Option<u64>,
    /// Stored user-data bytes on this backend, from the maintained counters.
    /// Absent when those counters could not be read.
    pub used_bytes: Option<u64>,
    pub default: bool,
    pub status: ServiceStatus,
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
    /// Local listen address. Realm-authenticated callers only.
    pub bind: Option<String>,
    pub url: Option<String>,
}

/// Realm information. `realm_id`, `description`, `oidc_providers`, the public
/// interface urls, metadata replication policy and aggregate public overview
/// are public. The overview exposes only live document, group and configured
/// membership counts; each nullable value is unknown rather than zero when this
/// node cannot answer it. Realm topology (`nodes`, `discovery`), quota policy
/// and interface listen addresses need a token of this realm and are otherwise
/// absent or empty.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RealmInfoResponse {
    pub realm_id: String,
    pub description: String,
    pub metadata_replication: RealmMetadataReplicationResponse,
    pub oidc_providers: Vec<RealmOidcProviderResponse>,
    /// Count-only realm overview, available to anonymous and authenticated
    /// callers. The optional wrapper permits an older or otherwise unable node
    /// to omit the whole extension without changing the rest of the response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_overview: Option<RealmPublicOverview>,
    /// True on a management node, the only kind that mints enrollments and
    /// issues the realm's credentials. Public.
    pub is_management_node: bool,
    /// API base urls of the realm's management nodes, from their node
    /// information documents; a device follows one to enroll. Public.
    pub management_urls: Vec<String>,
    /// Realm discovery configuration. Realm-authenticated callers only.
    #[schema(value_type = Object)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discovery: Option<Value>,
    /// Realm nodes. Realm-authenticated callers only, else empty.
    pub nodes: Vec<RealmNodeInfoResponse>,
    /// Realm quota policy. Realm-authenticated callers only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota: Option<RealmQuotaConfig>,
    pub interfaces: InterfaceServicesStatus,
}

/// Public, count-only realm overview. Nullable fields mean this node could not
/// answer; they never use zero as a stand-in for unknown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmPublicOverview {
    /// Lifecycle-live metadata documents in the realm. This is the registry
    /// document count, not caller-filtered and not multiplied by replicas.
    pub live_datasets: Option<u64>,
    /// Groups stored for this realm.
    pub groups: Option<u64>,
    /// Nodes in the replicated realm configuration, regardless of DHT presence
    /// or health.
    pub nodes_configured: Option<u64>,
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
    pub user_group_cap_overrides: Vec<RealmUserGroupCapOverride>,
    pub max_devices_per_user: Option<u32>,
    pub device_requests_per_minute: Option<u32>,
    pub device_concurrent_pulls: Option<u32>,
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
            device_requests_per_minute: quota.device_requests_per_minute,
            device_concurrent_pulls: quota.device_concurrent_pulls,
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
            device_requests_per_minute: self.device_requests_per_minute,
            device_concurrent_pulls: self.device_concurrent_pulls,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmPlacementConfigResponse {
    pub strategies: Vec<RealmPlacementStrategy>,
    pub default_strategy_id: Option<String>,
    pub job_family_strategy_id: String,
    pub bindings: Vec<RealmPlacementBinding>,
    pub overrides: Vec<RealmPlacementOverride>,
    pub transitions: RealmTransitionHealthResponse,
}

/// Health of the realm's in-flight placement transitions. Counts only: nothing
/// here changes where a request routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmTransitionHealthResponse {
    pub active: usize,
    pub incomplete_buckets: usize,
    pub stalled_buckets: usize,
    /// Transitions still incomplete after a day.
    pub overdue: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmPlacementStrategy {
    pub strategy_id: String,
    pub name: String,
    pub replica_count: Option<u32>,
    pub distinct_locations: bool,
    pub affinity: Vec<RealmPlacementAffinityRule>,
    pub shard_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmPlacementAffinityRule {
    pub key: String,
    pub value: String,
    pub effect: RealmPlacementAffinityEffect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RealmPlacementAffinityEffect {
    Filter,
    Multiply { permille: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmPlacementBinding {
    pub scope: RealmPlacementBindingScope,
    pub strategy_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RealmPlacementBindingScope {
    Realm,
    Group {
        group_id: String,
    },
    Class {
        document_class: RealmPlacementDocumentClass,
    },
    MetadataPathPrefix {
        prefix: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RealmPlacementDocumentClass {
    Admin,
    Group,
    User,
    Metadata,
    MetadataRegistry,
    JobControl,
    PlacementPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmPlacementOverride {
    pub subject: String,
    pub pinned: Vec<String>,
    pub excluded: Vec<String>,
    pub strategy_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "mutation", rename_all = "snake_case", deny_unknown_fields)]
pub enum RealmPlacementMutationRequest {
    UpsertStrategy {
        strategy: RealmPlacementStrategy,
    },
    RemoveStrategy {
        strategy_id: String,
    },
    SetDefaultStrategy {
        strategy_id: String,
    },
    SetBinding {
        binding: RealmPlacementBinding,
    },
    RemoveBinding {
        scope: RealmPlacementBindingScope,
    },
    SetOverride {
        placement_override: RealmPlacementOverride,
    },
    RemoveOverride {
        subject: String,
    },
    ProvisionMetadataBinding {
        strategy_id: String,
        group_id: Option<String>,
    },
}

enum RealmPlacementAction {
    Mutation(RealmPlacementMutation),
    Provision {
        strategy_id: Ulid,
        group_id: Option<Ulid>,
    },
}

impl RealmPlacementConfigResponse {
    fn from_document(document: &RealmConfigDocument) -> Self {
        let health = transition_health(document, unix_timestamp_millis());
        Self {
            transitions: RealmTransitionHealthResponse {
                active: health.active,
                incomplete_buckets: health.incomplete_buckets,
                stalled_buckets: health.stalled_buckets,
                overdue: health.overdue,
            },
            strategies: document
                .strategies
                .iter()
                .map(RealmPlacementStrategy::from)
                .collect(),
            default_strategy_id: document.default_strategy_id.map(|id| id.to_string()),
            job_family_strategy_id: document.job_family_strategy_id.to_string(),
            bindings: document
                .strategy_bindings
                .iter()
                .map(RealmPlacementBinding::from)
                .collect(),
            overrides: document
                .placement_overrides
                .iter()
                .map(RealmPlacementOverride::from)
                .collect(),
        }
    }
}

impl From<&aruna_core::structs::PlacementStrategy> for RealmPlacementStrategy {
    fn from(strategy: &aruna_core::structs::PlacementStrategy) -> Self {
        Self {
            strategy_id: strategy.strategy_id.to_string(),
            name: strategy.name.clone(),
            replica_count: strategy.replica_count,
            distinct_locations: strategy.distinct_locations,
            affinity: strategy
                .affinity
                .iter()
                .map(|rule| RealmPlacementAffinityRule {
                    key: rule.matcher.key.clone(),
                    value: rule.matcher.value.clone(),
                    effect: match rule.effect {
                        aruna_core::structs::AffinityEffect::Filter => {
                            RealmPlacementAffinityEffect::Filter
                        }
                        aruna_core::structs::AffinityEffect::Multiply { permille } => {
                            RealmPlacementAffinityEffect::Multiply { permille }
                        }
                    },
                })
                .collect(),
            shard_count: strategy.shard_count,
        }
    }
}

impl RealmPlacementStrategy {
    fn into_core(self) -> ServerResult<aruna_core::structs::PlacementStrategy> {
        Ok(aruna_core::structs::PlacementStrategy {
            strategy_id: parse_ulid(&self.strategy_id, "strategy_id")?,
            name: self.name,
            replica_count: self.replica_count,
            distinct_locations: self.distinct_locations,
            affinity: self
                .affinity
                .into_iter()
                .map(|rule| aruna_core::structs::AffinityRule {
                    matcher: aruna_core::structs::LabelMatch {
                        key: rule.key,
                        value: rule.value,
                    },
                    effect: match rule.effect {
                        RealmPlacementAffinityEffect::Filter => {
                            aruna_core::structs::AffinityEffect::Filter
                        }
                        RealmPlacementAffinityEffect::Multiply { permille } => {
                            aruna_core::structs::AffinityEffect::Multiply { permille }
                        }
                    },
                })
                .collect(),
            shard_count: self.shard_count,
        })
    }
}

impl From<&aruna_core::structs::StrategyBinding> for RealmPlacementBinding {
    fn from(binding: &aruna_core::structs::StrategyBinding) -> Self {
        Self {
            scope: RealmPlacementBindingScope::from(&binding.scope),
            strategy_id: binding.strategy_id.to_string(),
        }
    }
}

impl RealmPlacementBinding {
    fn into_core(self) -> ServerResult<aruna_core::structs::StrategyBinding> {
        Ok(aruna_core::structs::StrategyBinding {
            scope: self.scope.into_core()?,
            strategy_id: parse_ulid(&self.strategy_id, "strategy_id")?,
        })
    }
}

impl From<&aruna_core::structs::BindingScope> for RealmPlacementBindingScope {
    fn from(scope: &aruna_core::structs::BindingScope) -> Self {
        match scope {
            aruna_core::structs::BindingScope::Realm => Self::Realm,
            aruna_core::structs::BindingScope::Group(group_id) => Self::Group {
                group_id: group_id.to_string(),
            },
            aruna_core::structs::BindingScope::Class(document_class) => Self::Class {
                document_class: RealmPlacementDocumentClass::from(*document_class),
            },
            aruna_core::structs::BindingScope::MetadataPathPrefix(prefix) => {
                Self::MetadataPathPrefix {
                    prefix: prefix.clone(),
                }
            }
        }
    }
}

impl RealmPlacementBindingScope {
    fn into_core(self) -> ServerResult<aruna_core::structs::BindingScope> {
        Ok(match self {
            Self::Realm => aruna_core::structs::BindingScope::Realm,
            Self::Group { group_id } => {
                aruna_core::structs::BindingScope::Group(parse_ulid(&group_id, "group_id")?)
            }
            Self::Class { document_class } => {
                aruna_core::structs::BindingScope::Class(document_class.into())
            }
            Self::MetadataPathPrefix { prefix } => {
                aruna_core::structs::BindingScope::MetadataPathPrefix(prefix)
            }
        })
    }
}

impl From<aruna_core::structs::DocumentClass> for RealmPlacementDocumentClass {
    fn from(document_class: aruna_core::structs::DocumentClass) -> Self {
        match document_class {
            aruna_core::structs::DocumentClass::Admin => Self::Admin,
            aruna_core::structs::DocumentClass::Group => Self::Group,
            aruna_core::structs::DocumentClass::User => Self::User,
            aruna_core::structs::DocumentClass::Metadata => Self::Metadata,
            aruna_core::structs::DocumentClass::MetadataRegistry => Self::MetadataRegistry,
            aruna_core::structs::DocumentClass::JobControl => Self::JobControl,
            aruna_core::structs::DocumentClass::PlacementPolicy => Self::PlacementPolicy,
        }
    }
}

impl From<RealmPlacementDocumentClass> for aruna_core::structs::DocumentClass {
    fn from(document_class: RealmPlacementDocumentClass) -> Self {
        match document_class {
            RealmPlacementDocumentClass::Admin => Self::Admin,
            RealmPlacementDocumentClass::Group => Self::Group,
            RealmPlacementDocumentClass::User => Self::User,
            RealmPlacementDocumentClass::Metadata => Self::Metadata,
            RealmPlacementDocumentClass::MetadataRegistry => Self::MetadataRegistry,
            RealmPlacementDocumentClass::JobControl => Self::JobControl,
            RealmPlacementDocumentClass::PlacementPolicy => Self::PlacementPolicy,
        }
    }
}

impl From<&aruna_core::structs::PlacementOverride> for RealmPlacementOverride {
    fn from(record: &aruna_core::structs::PlacementOverride) -> Self {
        Self {
            subject: hex::encode(&record.subject),
            pinned: record.pinned.iter().map(ToString::to_string).collect(),
            excluded: record.excluded.iter().map(ToString::to_string).collect(),
            strategy_id: record.strategy_id.map(|id| id.to_string()),
        }
    }
}

impl RealmPlacementOverride {
    fn into_core(self) -> ServerResult<aruna_core::structs::PlacementOverride> {
        Ok(aruna_core::structs::PlacementOverride {
            subject: parse_subject(&self.subject)?,
            pinned: parse_node_ids(self.pinned, "pinned")?,
            excluded: parse_node_ids(self.excluded, "excluded")?,
            strategy_id: self
                .strategy_id
                .map(|id| parse_ulid(&id, "strategy_id"))
                .transpose()?,
        })
    }
}

impl RealmPlacementMutationRequest {
    fn into_core(self) -> ServerResult<RealmPlacementAction> {
        let mutation = match self {
            Self::UpsertStrategy { strategy } => {
                RealmPlacementMutation::UpsertStrategy(strategy.into_core()?)
            }
            Self::RemoveStrategy { strategy_id } => {
                RealmPlacementMutation::RemoveStrategy(parse_ulid(&strategy_id, "strategy_id")?)
            }
            Self::SetDefaultStrategy { strategy_id } => {
                RealmPlacementMutation::SetDefaultStrategy(parse_ulid(&strategy_id, "strategy_id")?)
            }
            Self::SetBinding { binding } => {
                RealmPlacementMutation::SetBinding(binding.into_core()?)
            }
            Self::RemoveBinding { scope } => {
                RealmPlacementMutation::RemoveBinding(scope.into_core()?)
            }
            Self::SetOverride { placement_override } => {
                RealmPlacementMutation::SetOverride(placement_override.into_core()?)
            }
            Self::RemoveOverride { subject } => {
                RealmPlacementMutation::RemoveOverride(parse_subject(&subject)?)
            }
            Self::ProvisionMetadataBinding {
                strategy_id,
                group_id,
            } => {
                return Ok(RealmPlacementAction::Provision {
                    strategy_id: parse_ulid(&strategy_id, "strategy_id")?,
                    group_id: group_id
                        .map(|group_id| parse_ulid(&group_id, "group_id"))
                        .transpose()?,
                });
            }
        };
        Ok(RealmPlacementAction::Mutation(mutation))
    }
}

fn parse_ulid(value: &str, field: &str) -> ServerResult<Ulid> {
    Ulid::from_string(value)
        .map_err(|_| ServerError::BadRequestReason(format!("invalid {field}: {value}")))
}

fn parse_subject(value: &str) -> ServerResult<Vec<u8>> {
    hex::decode(value)
        .map_err(|_| ServerError::BadRequestReason("subject must be valid hex".to_string()))
}

fn parse_node_ids(values: Vec<String>, field: &str) -> ServerResult<Vec<aruna_core::NodeId>> {
    values
        .into_iter()
        .map(|value| {
            value.parse::<aruna_core::NodeId>().map_err(|_| {
                ServerError::BadRequestReason(format!("invalid node id in {field}: {value}"))
            })
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmMetadataReplicationResponse {
    pub default_replication_factor: Option<u32>,
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
    /// Owner of a `user` node; null for infrastructure nodes.
    pub owner: Option<String>,
    pub configured: bool,
    pub present: bool,
    pub connection_status: RealmNodeConnectionStatus,
    /// When a `user` node last reached this node, in unix milliseconds. This
    /// node's own observation; absent for other kinds and for a device it has
    /// not seen.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_ms: Option<u64>,
    /// Placement map entry (location/weight/status) when the node is mapped.
    pub placement: Option<RealmNodePlacementResponse>,
    /// Latest published node info document (capabilities/labels/urls/utilization) if received.
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
    pub executors: Vec<ExecutorCapabilityResponse>,
    pub labels: std::collections::BTreeMap<String, String>,
    pub urls: RealmNodeUrlsResponse,
    pub utilization: RealmNodeUtilizationResponse,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ExecutorCapabilityResponse {
    pub kind: String,
    pub file_staging: bool,
    pub direct_s3: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmNodeUrlsResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s3: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmNodeUtilizationResponse {
    pub storage_bytes_used: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub documents_held: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_permille: Option<u32>,
    pub heartbeat_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RealmNodeKindInfo {
    Management,
    Server,
    User,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RealmNodeConnectionStatus {
    Connected,
    Configured,
    /// A device that reached this node recently. Not a connection: it is what
    /// this node itself saw, and only this node saw it.
    Seen,
    /// Presence does not describe this node: a device publishes none.
    Unknown,
}

impl From<&RealmNodeKind> for RealmNodeKindInfo {
    fn from(value: &RealmNodeKind) -> Self {
        match value {
            RealmNodeKind::Management => Self::Management,
            RealmNodeKind::Server => Self::Server,
            RealmNodeKind::User { .. } => Self::User,
        }
    }
}

#[utoipa::path(
    get,
    path = "/info/realm",
    tag = "info",
    summary = "Read the realm's public settings and node topology",
    description = r#"Answers every caller, but answers a realm member with more.

**Authentication**: optional bearer token; without a usable token of this realm, and that includes a
token of another realm or one this node cannot validate, the response is the public part only.

**Behavior**
- The public part is the realm id, description, metadata replication policy, the OIDC providers a
  client needs to obtain a token, the public interface urls, `public_overview`, and where the
  realm's management nodes are: `is_management_node` says whether this node is one, and
  `management_urls` lists the api base urls the management nodes published, this node's own first.
  Only a management node mints enrollments and issues the realm's credentials, so a device or a
  client that reached any other node follows one of these urls instead. The list comes from the
  node information documents that reached this node, so it may lag or omit a fresh node.
- That overview contains only three nullable aggregates: `live_datasets` is the realm's
  lifecycle-live metadata registry count, never caller-filtered or replica-multiplied; `groups` is
  the realm's stored group count; and `nodes_configured` is the membership count in this node's
  replicated realm configuration, never DHT presence or health.
- A null overview value means this node could not answer and is never encoded as zero. No resource
  titles, group membership, storage/capacity, bucket/object, or node-health detail is implied by
  these counts.
- A bearer token of this realm additionally reveals the realm's discovery configuration, its quota
  policy, the node list and the interface listen addresses, and receives the same public overview.
- Gated values are absent or empty, never restructured, so one parser handles both.
- The node list is the realm's configured membership read from this node's replicated realm
  configuration; `placement` is the node's entry in the placement map and `info` is the last node
  information document that reached this node, so it may lag or be absent.
- Liveness is a separate, deliberately conservative signal: presence is resolved through a bounded
  realm lookup with a four second budget, and if that lookup is stale, times out or fails, only this
  node counts as present.
- `present` true and `connection_status` `connected` therefore mean the peer was confirmed live by a
  fresh lookup just now; `configured` means no fresh confirmation, which is not evidence that the
  peer is down. Stale presence is candidate data and is never reported as a connection.
- A `user` node is a device and publishes no presence at all, so `present` is always false and
  `connection_status` is never `connected`. That is the absence of a signal, never a report that
  the device is down.
- A device is instead reported from what this node itself saw: `last_seen_ms` is when the device
  last reached this node over an authorized request, and `connection_status` `seen` means that was
  within the last three minutes, otherwise `unknown`. Both are this node's own observation, are
  never realm state, and start empty after a restart; a device that talks to a different realm node
  is `unknown` here and `seen` there. A node answering about itself reports itself seen now."#,
    responses(
        (
            status = 200,
            description = "Realm information at the caller's level of access; discovery, quota, nodes and interface bind addresses only for a caller holding a token of this realm",
            body = RealmInfoResponse,
            examples(
                ("Anonymous" = (
                    summary = "What a client needs to reach the realm and obtain a token",
                    value = json!({
                        "realm_id": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                        "description": "Example realm",
                        "metadata_replication": {"default_replication_factor": 3},
                        "public_overview": {
                            "live_datasets": 4096,
                            "groups": 12,
                            "nodes_configured": 3
                        },
                        "is_management_node": false,
                        "management_urls": ["https://mgmt.example.test/api/v1"],
                        "oidc_providers": [
                            {
                                "id": "example",
                                "issuer": "https://idp.example.test/realms/aruna",
                                "audience": "aruna",
                                "discovery_url": "https://idp.example.test/realms/aruna/.well-known/openid-configuration"
                            }
                        ],
                        "nodes": [],
                        "interfaces": {
                            "rest": {"status": "available", "bind": null, "url": "https://node.example.test/api/v1"},
                            "s3": {"status": "available", "bind": null, "url": "https://s3.example.test"}
                        }
                    })
                )),
                ("Realm token" = (
                    summary = "A realm member also sees discovery, quota and the node topology",
                    value = json!({
                        "realm_id": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                        "description": "Example realm",
                        "metadata_replication": {"default_replication_factor": 3},
                        "public_overview": {
                            "live_datasets": 4096,
                            "groups": 12,
                            "nodes_configured": 3
                        },
                        "is_management_node": true,
                        "management_urls": ["https://node.example.test/api/v1"],
                        "oidc_providers": [],
                        "discovery": {"Dynamic": {"methods": [{"DhtSigned": {"ttl_secs": 3600, "refresh_after_secs": 1800}}]}},
                        "nodes": [
                            {
                                "node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                                "kind": "server",
                                "owner": null,
                                "configured": true,
                                "present": true,
                                "connection_status": "connected",
                                "placement": {"location": "dc-a", "weight": 100, "full": false, "draining": false},
                                "info": {
                                    "executors": [{"kind": "docker", "file_staging": true, "direct_s3": false}],
                                    "labels": {"zone": "dc-a"},
                                    "urls": {"api": "https://node.example.test/api/v1"},
                                    "utilization": {
                                        "storage_bytes_used": 1073741824,
                                        "documents_held": 128,
                                        "load_permille": 120,
                                        "heartbeat_at_ms": 1775744591123_i64
                                    },
                                    "updated_at_ms": 1775744591123_i64
                                }
                            },
                            {
                                "node_id": "2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a",
                                "kind": "user",
                                "owner": "01JHKMNPQR0123456789ABCDEF@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                                "configured": true,
                                "present": false,
                                "connection_status": "seen",
                                "last_seen_ms": 1775744591123_i64,
                                "placement": null,
                                "info": null
                            }
                        ],
                        "quota": {
                            "default_group_quota_bytes": 10737418240_i64,
                            "grace_factor_percent": 110,
                            "warn_threshold_percent": 80,
                            "group_overrides": [],
                            "max_groups_per_user": 10,
                            "user_group_cap_overrides": [],
                            "max_devices_per_user": 5,
                            "device_requests_per_minute": 600,
                            "device_concurrent_pulls": 8
                        },
                        "interfaces": {
                            "rest": {"status": "available", "bind": "0.0.0.0:3000", "url": "https://node.example.test/api/v1"},
                            "s3": {"status": "available", "bind": "0.0.0.0:1337", "url": "https://s3.example.test"}
                        }
                    })
                ))
            )
        ),
        (status = 404, description = "This node holds no configuration document for its realm, so there is nothing to report yet", body = crate::error::ErrorResponse)
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
    let realm_authenticated = auth.is_some_and(|auth| auth.realm_id == state.get_realm_id());

    let mut interfaces = interface_services_status(&state).await;
    if !realm_authenticated {
        interfaces.rest.bind = None;
        interfaces.s3.bind = None;
    }

    let metadata_replication = RealmMetadataReplicationResponse {
        default_replication_factor: config.effective_default_metadata_replication_factor(),
    };
    let live_datasets = match count_realm_documents(&state.get_ctx(), config.realm_id).await {
        Ok(count) => count,
        Err(error) => {
            warn!(error = %error, "metadata document count unavailable for public realm overview");
            None
        }
    };
    let groups = match count_realm_groups(&state.get_ctx(), config.realm_id).await {
        Ok(count) => Some(count),
        Err(error) => {
            warn!(error = %error, "group count unavailable for public realm overview");
            None
        }
    };
    let public_overview = Some(RealmPublicOverview {
        live_datasets,
        groups,
        nodes_configured: u64::try_from(config.nodes.len()).ok(),
    });

    let node_info_docs = load_node_info_documents_best_effort(&state, &config).await;
    let management_urls = management_urls(
        &state,
        &config,
        &node_info_docs,
        interfaces.rest.url.as_deref(),
    );

    let (discovery, nodes, quota) = if realm_authenticated {
        let present_nodes = load_realm_presence_best_effort(&state).await;
        let discovery = serde_json::to_value(&config.discovery)
            .map_err(|error| ServerError::InternalError(error.to_string()))?;
        let contacts = state
            .get_ctx()
            .metadata_handle
            .as_ref()
            .map(MetadataHandle::peer_contacts)
            .unwrap_or_default();
        let nodes = map_realm_nodes(
            &state,
            &config,
            present_nodes,
            node_info_docs,
            &contacts,
            unix_timestamp_millis(),
        );
        (
            Some(discovery),
            nodes,
            Some(RealmQuotaConfig::from(config.quota.clone())),
        )
    } else {
        (None, Vec::new(), None)
    };

    Ok((
        StatusCode::OK,
        Json(RealmInfoResponse {
            realm_id: config.realm_id.to_string(),
            description: config.description,
            metadata_replication,
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
            public_overview,
            is_management_node: state.is_management_node(),
            management_urls,
            discovery,
            nodes,
            quota,
            interfaces,
        }),
    ))
}

/// Every management node of the realm with the api url it published, in
/// realm-config order. The caller decides where its own node belongs.
pub(crate) fn management_node_urls(
    config: &RealmConfigDocument,
    node_info_docs: &BTreeMap<aruna_core::NodeId, aruna_core::structs::NodeInfoDocument>,
) -> Vec<(aruna_core::NodeId, Option<String>)> {
    config
        .nodes
        .iter()
        .filter(|node| matches!(node.kind, RealmNodeKind::Management))
        .filter_map(|node| node.node_id.parse::<aruna_core::NodeId>().ok())
        .map(|node_id| {
            let url = node_info_docs
                .get(&node_id)
                .and_then(|doc| doc.urls.api.clone());
            (node_id, url)
        })
        .collect()
}

/// The management nodes' published api urls, this node first. A management
/// node whose own document has not landed yet names its published interface.
fn management_urls(
    state: &ServerState,
    config: &RealmConfigDocument,
    node_info_docs: &BTreeMap<aruna_core::NodeId, aruna_core::structs::NodeInfoDocument>,
    own_url: Option<&str>,
) -> Vec<String> {
    let current = state.get_node_id();
    let mut urls: Vec<String> = Vec::new();
    for (node_id, published) in management_node_urls(config, node_info_docs) {
        let is_current = node_id == current;
        let url = published.or_else(|| is_current.then(|| own_url.map(str::to_string)).flatten());
        let Some(url) = url else {
            continue;
        };
        if urls.contains(&url) {
            continue;
        }
        if is_current {
            urls.insert(0, url);
        } else {
            urls.push(url);
        }
    }
    urls
}

pub(crate) async fn load_node_info_documents_best_effort(
    state: &ServerState,
    config: &RealmConfigDocument,
) -> BTreeMap<aruna_core::NodeId, aruna_core::structs::NodeInfoDocument> {
    let node_ids: Vec<aruna_core::NodeId> = config
        .nodes
        .iter()
        .filter_map(|node| node.node_id.parse().ok())
        .collect();
    match aruna_operations::node_info::read_node_info_documents(&state.get_ctx(), &node_ids).await {
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
        executors: document
            .executors
            .iter()
            .map(|executor| ExecutorCapabilityResponse {
                kind: executor.kind.clone(),
                file_staging: executor.file_staging,
                direct_s3: executor.direct_s3,
            })
            .collect(),
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

async fn is_realm_config_admin(state: &ServerState, auth: &AuthContext) -> ServerResult<bool> {
    let realm_id = state.get_realm_id();
    permission_granted(
        state,
        auth,
        format!("/{realm_id}/admin/config"),
        Permission::WRITE,
    )
    .await
}

async fn authorize_realm_config_admin(
    state: &Arc<ServerState>,
    auth: Option<AuthContext>,
) -> ServerResult<AuthContext> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    if auth.realm_id != state.get_realm_id() || !state.is_management_node() {
        return Err(ServerError::Forbidden);
    }
    if !is_realm_config_admin(state, &auth).await? {
        return Err(ServerError::Forbidden);
    }

    Ok(auth)
}

/// How much of `/info` a caller may see. Foreign-realm tokens are treated like
/// anonymous callers: `/info` answers every caller, it just answers with less.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InfoAccess {
    Public,
    Realm,
    Admin,
}

async fn info_access(state: &ServerState, auth: Option<&AuthContext>) -> InfoAccess {
    let Some(auth) = auth else {
        return InfoAccess::Public;
    };
    if auth.realm_id != state.get_realm_id() {
        return InfoAccess::Public;
    }
    match is_realm_config_admin(state, auth).await {
        Ok(true) => InfoAccess::Admin,
        Ok(false) => InfoAccess::Realm,
        Err(error) => {
            warn!(error = %error, "realm admin check failed for info response");
            InfoAccess::Realm
        }
    }
}

#[utoipa::path(
    get,
    path = "/info/realm/placement",
    tag = "info",
    summary = "Read the realm's placement strategies, bindings and overrides",
    description = r#"Returns the placement policy as stored in this node's copy of the realm configuration.

**Authentication**: realm bearer token with WRITE on the realm's configuration admin path. A
management node serves it, and every other node relays the call to one.

**Behavior**
- The response carries the defined strategies with their replica count, distinctness requirement,
  affinity rules and shard count; the default strategy; the immutable job-family strategy id; the
  bindings that map a scope (the realm, a group, a document class or a metadata path prefix) to a
  strategy; and the per-subject overrides that pin or exclude individual nodes.
- This is policy, not a placement result: it says how replicas are chosen, not where any particular
  document currently sits.

**Limits**
- `job_family_strategy_id` names a strategy that cannot be removed or have its shard count
  reshaped: those mutations fail with `JobFamilyImmutable`.
- Removing a strategy that is still referenced fails with `StrategyReferenced`."#,
    responses(
        (
            status = 200,
            description = "The realm's placement policy as this management node has it",
            body = RealmPlacementConfigResponse,
            example = json!({
                "strategies": [
                    {
                        "strategy_id": "01JABCDEF0123456789ABCDEFG",
                        "name": "three-replicas-across-sites",
                        "replica_count": 3,
                        "distinct_locations": true,
                        "affinity": [{"key": "zone", "value": "dc-a", "effect": {"kind": "multiply", "permille": 1500}}],
                        "shard_count": 16
                    }
                ],
                "default_strategy_id": "01JABCDEF0123456789ABCDEFG",
                "job_family_strategy_id": "01JABCDEF0123456789ABCDEFG",
                "bindings": [
                    {"scope": {"kind": "class", "document_class": "metadata"}, "strategy_id": "01JABCDEF0123456789ABCDEFG"}
                ],
                "overrides": [
                    {
                        "subject": "0102030405060708",
                        "pinned": ["1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978"],
                        "excluded": [],
                        "strategy_id": null
                    }
                ],
                "transitions": {
                    "active": 1,
                    "incomplete_buckets": 2,
                    "stalled_buckets": 0,
                    "overdue": 0
                }
            })
        ),
        (status = 401, description = "Missing or unusable bearer token", body = crate::error::ErrorResponse),
        (status = 403, description = "Caller is not a realm config admin", body = crate::error::ErrorResponse),
        (status = 404, description = "Realm config not found", body = crate::error::ErrorResponse),
        (status = 500, description = "Unexpected server error", body = crate::error::ErrorResponse),
        (status = 503, description = "Called on a node that is not a management node and no management node was reachable; code `no_management_node`", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_realm_placement(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<RealmPlacementConfigResponse>)> {
    authorize_realm_config_admin(&state, auth).await?;
    let document = drive(
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
    Ok((
        StatusCode::OK,
        Json(RealmPlacementConfigResponse::from_document(&document)),
    ))
}

#[utoipa::path(
    patch,
    path = "/info/realm/placement",
    tag = "info",
    summary = "Apply one change to the realm's placement policy",
    description = r#"Applies exactly one change to the realm's placement policy and returns the whole policy.

**Authentication**: realm bearer token with WRITE on the realm's configuration admin path. A
management node serves it, and every other node relays the call to one.

**Behavior**
- The body carries exactly one change, selected by its `mutation` field: define or replace a
  strategy, remove one, set the default, set or remove a binding for a scope, set or remove a
  per-subject override, or provision a metadata binding for a strategy.
- Provisioning is idempotent, an existing binding for the same scope and strategy is returned
  unchanged instead of allocating a second one.
- The whole placement policy after the change is returned, so a client never has to re-read to learn
  the new state.
- The change is written to the replicated realm configuration, which means it is durable here when
  the response is sent and reaches the other realm nodes asynchronously; it does not move any data
  by itself, existing replicas are relocated by later placement work.

**Limits**
- `job_family_strategy_id` identifies the job-family strategy, which cannot be removed or have its
  shard count reshaped; those mutations fail with `JobFamilyImmutable`.

**Errors**: removing a strategy that a binding or override still points at fails with
`StrategyReferenced` and is refused with 409 rather than leaving a dangling reference, and a
concurrent update of the same configuration is also 409, where retrying the request is the expected
response."#,
    request_body(
        content = RealmPlacementMutationRequest,
        description = "Exactly one placement change, discriminated by `mutation`. Ids are ULIDs, node ids are hex-encoded, an override `subject` is a hex-encoded key prefix",
        examples(
            ("Define a strategy" = (
                summary = "Create or replace a strategy that spreads three replicas over distinct locations",
                value = json!({
                    "mutation": "upsert_strategy",
                    "strategy": {
                        "strategy_id": "01JABCDEF0123456789ABCDEFG",
                        "name": "three-replicas-across-sites",
                        "replica_count": 3,
                        "distinct_locations": true,
                        "affinity": [{"key": "zone", "value": "dc-a", "effect": {"kind": "filter"}}],
                        "shard_count": 16
                    }
                })
            )),
            ("Bind a scope" = (
                summary = "Route every metadata document of one group through that strategy",
                value = json!({
                    "mutation": "set_binding",
                    "binding": {
                        "scope": {"kind": "group", "group_id": "01JMETADATA0123456789ABCDE"},
                        "strategy_id": "01JABCDEF0123456789ABCDEFG"
                    }
                })
            )),
            ("Provision a metadata binding" = (
                summary = "Idempotently allocate the realm-wide metadata binding for a strategy",
                value = json!({
                    "mutation": "provision_metadata_binding",
                    "strategy_id": "01JABCDEF0123456789ABCDEFG",
                    "group_id": null
                })
            ))
        )
    ),
    responses(
        (
            status = 200,
            description = "The complete placement policy after the change was applied",
            body = RealmPlacementConfigResponse,
            example = json!({
                "strategies": [
                    {
                        "strategy_id": "01JABCDEF0123456789ABCDEFG",
                        "name": "three-replicas-across-sites",
                        "replica_count": 3,
                        "distinct_locations": true,
                        "affinity": [{"key": "zone", "value": "dc-a", "effect": {"kind": "filter"}}],
                        "shard_count": 16
                    }
                ],
                "default_strategy_id": "01JABCDEF0123456789ABCDEFG",
                "job_family_strategy_id": "01JABCDEF0123456789ABCDEFG",
                "bindings": [
                    {"scope": {"kind": "realm"}, "strategy_id": "01JABCDEF0123456789ABCDEFG"}
                ],
                "overrides": [],
                "transitions": {
                    "active": 0,
                    "incomplete_buckets": 0,
                    "stalled_buckets": 0,
                    "overdue": 0
                }
            })
        ),
        (status = 400, description = "Malformed body, an id that is not a ULID, a node id or subject that does not decode, an unknown strategy, or a change the realm configuration rejects as invalid", body = crate::error::ErrorResponse),
        (status = 401, description = "Missing or unusable bearer token", body = crate::error::ErrorResponse),
        (status = 403, description = "Caller is not a realm config admin", body = crate::error::ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = crate::error::ErrorResponse),
        (status = 409, description = "The strategy is still referenced by a binding or override, the placement handle space is exhausted, or another update of the realm configuration won the race; the caller may retry", body = crate::error::ErrorResponse),
        (status = 500, description = "Unexpected server error", body = crate::error::ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = crate::error::ErrorResponse),
        (status = 503, description = "Storage cleanup capacity exhausted, or no management node was reachable to serve the relayed call; code `no_management_node`", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn mutate_realm_placement(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    request: Result<Json<RealmPlacementMutationRequest>, JsonRejection>,
) -> ServerResult<(StatusCode, Json<RealmPlacementConfigResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let Json(request) =
        request.map_err(|error| ServerError::BadRequestReason(error.body_text()))?;
    let action = request.into_core()?;
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: auth.user_id,
        realm_id: state.get_realm_id(),
    };
    let context = state.get_ctx();
    let document = match action {
        RealmPlacementAction::Mutation(mutation) => {
            // Request policies live at this boundary; the operation only checks roles.
            ensure_permission(
                &state,
                &auth,
                policy_admin_path(actor.realm_id),
                Permission::WRITE,
            )
            .await?;
            drive_realm_placement_mutation(
                MutateRealmPlacementConfig { actor, mutation },
                Some(auth),
                &context,
            )
            .await
            .map_err(map_mutate_realm_placement_error)?
        }
        RealmPlacementAction::Provision {
            strategy_id,
            group_id,
        } => {
            // Handle allocation carries no authorization of its own.
            authorize_realm_config_admin(&state, Some(auth)).await?;
            let scope = group_id
                .map(PlacementScope::Group)
                .unwrap_or(PlacementScope::Realm(actor.realm_id));
            provision_metadata_binding(context.as_ref(), actor.clone(), scope, strategy_id)
                .await
                .map_err(map_handle_error)?;
            drive(GetRealmConfigOperation::new(actor.realm_id), &context)
                .await
                .map_err(|error| match error {
                    aruna_operations::get_realm_config::GetRealmConfigError::DocumentNotFound => {
                        ServerError::NotFound
                    }
                    other => ServerError::InternalError(other.to_string()),
                })?
        }
    };
    Ok((
        StatusCode::OK,
        Json(RealmPlacementConfigResponse::from_document(&document)),
    ))
}

fn map_handle_error(error: HandleAllocationError) -> ServerError {
    match error {
        HandleAllocationError::StrategyNotFound(strategy_id) => ServerError::BadRequestReason(
            format!("placement strategy {strategy_id} does not exist"),
        ),
        HandleAllocationError::PlacementHandleExhausted { .. } => {
            ServerError::Conflict("placement handle space is exhausted".to_string())
        }
        HandleAllocationError::Append(error) => map_mutate_realm_placement_error(error),
        HandleAllocationError::ReadConfig(
            aruna_operations::get_realm_config::GetRealmConfigError::DocumentNotFound,
        ) => ServerError::NotFound,
        HandleAllocationError::Storage(StorageError::TransactionConflict) => {
            ServerError::Conflict("concurrent placement provisioning conflict; retry".to_string())
        }
        HandleAllocationError::Storage(StorageError::CleanupCapacity) => {
            ServerError::ServiceUnavailableReason(
                "storage cleanup capacity exhausted; retry".to_string(),
            )
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_mutate_realm_placement_error(error: MutateRealmPlacementError) -> ServerError {
    match error {
        MutateRealmPlacementError::RealmConfigNotFound => ServerError::NotFound,
        MutateRealmPlacementError::InvalidInput(reason) => ServerError::BadRequestReason(reason),
        error @ (MutateRealmPlacementError::AdminDocumentReducerError(_)
        | MutateRealmPlacementError::EmptyShardHolders { .. }
        | MutateRealmPlacementError::UnknownTransition { .. }
        | MutateRealmPlacementError::ForceWithoutProof { .. }) => {
            ServerError::BadRequestReason(error.to_string())
        }
        MutateRealmPlacementError::Unauthorized { .. } => ServerError::Forbidden,
        MutateRealmPlacementError::StrategyReferenced { strategy_id } => ServerError::Conflict(
            format!("placement strategy {strategy_id} is currently referenced"),
        ),
        MutateRealmPlacementError::JobFamilyImmutable { strategy_id } => ServerError::Conflict(
            format!("placement strategy {strategy_id} is the immutable job-family strategy"),
        ),
        error @ MutateRealmPlacementError::TransitionInFlight { .. } => {
            ServerError::Conflict(error.to_string())
        }
        MutateRealmPlacementError::StorageError(StorageError::TransactionConflict) => {
            ServerError::Conflict("concurrent realm placement update conflict; retry".to_string())
        }
        MutateRealmPlacementError::StorageError(StorageError::CleanupCapacity) => {
            ServerError::ServiceUnavailableReason(
                "storage cleanup capacity exhausted; retry".to_string(),
            )
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

#[utoipa::path(
    put,
    path = "/info/realm/quota",
    tag = "info",
    summary = "Replace the realm-wide quota policy",
    description = r#"Replaces the realm-wide quota policy wholesale and echoes back the stored result.

**Authentication**: realm bearer token with WRITE on the realm's configuration admin path; an
anonymous caller is rejected. A management node serves it, and every other node relays the call to
one.

**Behavior**
- This replaces the stored policy rather than patching it: overrides absent from the body are
  dropped, so send the complete intended policy.
- `default_group_quota_bytes` is the pre-grace allowance per group and null means unlimited.
- `grace_factor_percent` scales that allowance into the hard ceiling a write is refused at, while
  `warn_threshold_percent` only decides when a group is reported as warning.
- Per-group overrides replace both values for one group, and per-user overrides cap how many groups
  a user may hold.
- `max_devices_per_user` caps how many devices one user may enroll; null leaves device enrollment
  uncapped. Enrolled devices and unclaimed device enrollment secrets both occupy a slot.
- `device_requests_per_minute` and `device_concurrent_pulls` bound what one enrolled device may ask
  of each realm node: how many requests it may send per minute and how many it may keep in flight,
  which is what bounds long blob pulls. Both are per device and per node, and null leaves that
  dimension uncapped.
- Device limits are enforced where a device's requests actually arrive, at the node's inbound
  network admission, not at REST: an over-budget request is dropped there, so the device sees a
  transport failure and reports the retryable 503 class to its owner rather than a 429. The 429
  with `Retry-After` stays the answer of the per-address and per-principal REST limiters.
- Quota is evaluated against a group's realm-wide logical bytes, which are aggregated from counters
  that replicate between nodes, so enforcement follows a policy change as those counters and the
  realm configuration propagate.

**Limits**
- `grace_factor_percent` must be at least 100.
- `warn_threshold_percent` must be between 1 and 100.
- `max_devices_per_user` must be null or greater than zero; zero is refused rather than read as a
  ban on device enrollment.
- `device_requests_per_minute` and `device_concurrent_pulls` must be null or greater than zero;
  zero would silence every enrolled device and is refused."#,
    request_body(
        content = RealmQuotaConfig,
        description = "The complete quota policy to store; it replaces the current one, including all override lists",
        example = json!({
            "default_group_quota_bytes": 10737418240_i64,
            "grace_factor_percent": 110,
            "warn_threshold_percent": 80,
            "group_overrides": [
                {"group_id": "01JABCDEF0123456789ABCDEFG", "quota_bytes": 107374182400_i64, "grace_factor_percent": 120}
            ],
            "max_groups_per_user": 10,
            "user_group_cap_overrides": [
                {"user_id": "01JHKMNPQR0123456789ABCDEF@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8", "max_groups": 25}
            ],
            "max_devices_per_user": 5,
            "device_requests_per_minute": 600,
            "device_concurrent_pulls": 8
        })
    ),
    responses(
        (
            status = 200,
            description = "The quota policy now stored in the realm configuration",
            body = RealmQuotaConfig,
            example = json!({
                "default_group_quota_bytes": 10737418240_i64,
                "grace_factor_percent": 110,
                "warn_threshold_percent": 80,
                "group_overrides": [
                    {"group_id": "01JABCDEF0123456789ABCDEFG", "quota_bytes": 107374182400_i64, "grace_factor_percent": 120}
                ],
                "max_groups_per_user": 10,
                "user_group_cap_overrides": [
                    {"user_id": "01JHKMNPQR0123456789ABCDEF@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8", "max_groups": 25}
                ],
                "max_devices_per_user": 5,
                "device_requests_per_minute": 600,
                "device_concurrent_pulls": 8
            })
        ),
        (status = 400, description = "A percentage outside its allowed range, a duplicate or malformed override, an id that is not a ULID or user identifier, or a zero device cap or device limit", body = crate::error::ErrorResponse),
        (status = 403, description = "Caller is not a realm config admin", body = crate::error::ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = crate::error::ErrorResponse),
        (status = 409, description = "Another update of the realm configuration won the race; the caller may retry with the same body", body = crate::error::ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = crate::error::ErrorResponse),
        (status = 503, description = "Storage cleanup capacity exhausted, or no management node was reachable to serve the relayed call; code `no_management_node`", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_realm_quota(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<RealmQuotaConfig>,
) -> ServerResult<(StatusCode, Json<RealmQuotaConfig>)> {
    let auth = require_realm_auth(&state, auth)?;
    // Request policies live at this boundary; the operation only checks roles.
    ensure_permission(
        &state,
        &auth,
        policy_admin_path(state.get_realm_id()),
        Permission::WRITE,
    )
    .await?;
    let quota = request.into_quota_config()?;
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: auth.user_id,
        realm_id: state.get_realm_id(),
    };
    let stored = drive(
        SetRealmQuotaOperation::new(SetRealmQuotaConfig {
            actor,
            auth_context: auth,
            quota,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_set_realm_quota_error)?;
    Ok((StatusCode::OK, Json(RealmQuotaConfig::from(stored.quota))))
}

fn map_set_realm_quota_error(error: SetRealmQuotaError) -> ServerError {
    match error {
        SetRealmQuotaError::RealmConfigNotFound => ServerError::NotFound,
        SetRealmQuotaError::Unauthorized | SetRealmQuotaError::NotManagementNode => {
            ServerError::Forbidden
        }
        SetRealmQuotaError::InvalidQuota { reason } => ServerError::BadRequestReason(reason),
        SetRealmQuotaError::StorageError(StorageError::TransactionConflict) => {
            ServerError::Conflict("concurrent realm quota update conflict; retry".to_string())
        }
        SetRealmQuotaError::StorageError(StorageError::CleanupCapacity) => {
            ServerError::ServiceUnavailableReason(
                "storage cleanup capacity exhausted; retry".to_string(),
            )
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

/// Storage usage. The flat fields report this node's local counters, `realm` the
/// realm-wide total summed across every node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct UsageResponse {
    pub buckets: u64,
    pub objects: u64,
    pub stored_blobs: u64,
    pub stored_bytes: u64,
    pub logical_bytes: u64,
    pub referenced_bytes: u64,
    pub realm: UsageTotals,
    /// Realm-wide total of live metadata documents, excluding lifecycle-deleted
    /// ones. This is the realm's document volume, not a count of what the
    /// calling principal may read, so it is not filtered per caller. Absent on
    /// the group usage endpoint and on nodes without a metadata subsystem, so
    /// an absent count never reads as zero documents.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_documents: Option<u64>,
    /// Exact lifecycle-live metadata documents whose root is neither a Profile
    /// nor a Process Run. Present only on the group usage endpoint when all
    /// purpose counts can be computed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_count: Option<u64>,
    /// Exact lifecycle-live metadata documents whose root `@type` contains the
    /// W3C Profiles Vocabulary Profile IRI. Group usage only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_count: Option<u64>,
    /// Exact lifecycle-live non-Profile documents whose root conforms to the
    /// bundled Process Run Crate profile. Group usage only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_run_count: Option<u64>,
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
    pub referenced_bytes: u64,
}

impl From<UsageCounters> for UsageTotals {
    fn from(counters: UsageCounters) -> Self {
        Self {
            buckets: counters.buckets,
            objects: counters.objects,
            stored_blobs: counters.stored_blobs,
            stored_bytes: counters.stored_bytes,
            logical_bytes: counters.logical_bytes,
            referenced_bytes: counters.referenced_bytes,
        }
    }
}

impl UsageResponse {
    pub fn new(local: UsageCounters, realm: UsageCounters) -> Self {
        Self {
            buckets: local.buckets,
            objects: local.objects,
            stored_blobs: local.stored_blobs,
            stored_bytes: local.stored_bytes,
            logical_bytes: local.logical_bytes,
            referenced_bytes: local.referenced_bytes,
            realm: realm.into(),
            metadata_documents: None,
            dataset_count: None,
            profile_count: None,
            process_run_count: None,
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
    summary = "Report this node's and the realm's storage usage",
    description = r#"Reports this node's own storage counters together with the realm-wide totals.

**Authentication**: realm bearer token; an anonymous caller gets 401 and a token of another realm
403. No further permission is checked, because the figures are realm-wide totals and not per-caller
views.

**Behavior**
- The flat fields are this node's own counters, while `realm` is the total summed from every realm
  node's replicated usage snapshot, so it is eventually consistent: a node whose snapshot has not
  arrived or has not refreshed yet is simply not part of the sum, which can make the total lag
  reality after a burst of writes.
- `metadata_documents` counts the realm's live metadata documents, excluding lifecycle-deleted ones,
  and is deliberately unfiltered by what the caller may read.
- It is omitted, never zeroed, when this node has no metadata subsystem or the count cannot be
  produced, so an absent field means unknown.
- `quota` is not reported here, it belongs to the per-group usage view."#,
    responses(
        (
            status = 200,
            description = "This node's counters plus the realm-wide totals",
            body = UsageResponse,
            example = json!({
                "buckets": 4,
                "objects": 128,
                "stored_blobs": 130,
                "stored_bytes": 1073741824,
                "logical_bytes": 1099511627776_i64,
                "referenced_bytes": 2147483648_i64,
                "realm": {
                    "buckets": 12,
                    "objects": 512,
                    "stored_blobs": 530,
                    "stored_bytes": 4294967296_i64,
                    "logical_bytes": 4398046511104_i64,
                    "referenced_bytes": 8589934592_i64
                },
                "metadata_documents": 4096
            })
        ),
        (status = 401, description = "Missing or unusable bearer token", body = crate::error::ErrorResponse),
        (status = 403, description = "Caller is not a member of this realm", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_usage(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<UsageResponse>)> {
    require_realm_auth(&state, auth)?;
    let local = load_usage_counters(&state, USAGE_GLOBAL_KEY.to_vec()).await?;
    let realm = load_realm_usage(&state, RealmUsageScope::Global).await?;
    let mut response = UsageResponse::new(local, realm);
    // Best effort: storage counters stay reportable when the metadata
    // subsystem cannot answer, and the omitted field never reads as zero.
    response.metadata_documents =
        match count_realm_documents(&state.get_ctx(), state.get_realm_id()).await {
            Ok(count) => count,
            Err(error) => {
                warn!(error = %error, "metadata document count unavailable for usage response");
                None
            }
        };
    Ok((StatusCode::OK, Json(response)))
}

fn map_realm_nodes(
    state: &ServerState,
    config: &RealmConfigDocument,
    present_nodes: HashSet<aruna_core::NodeId>,
    node_info_docs: BTreeMap<aruna_core::NodeId, aruna_core::structs::NodeInfoDocument>,
    contacts: &PeerContacts,
    now_ms: u64,
) -> Vec<RealmNodeInfoResponse> {
    let current_node = state.get_node_id();
    config
        .nodes
        .iter()
        .map(|node| {
            let parsed = node.node_id.parse::<aruna_core::NodeId>().ok();
            let is_current = node.node_id == current_node.to_string();
            let kind = RealmNodeKindInfo::from(&node.kind);
            // A device publishes no realm presence, so presence carries no
            // statement about it, not even on the device's own node.
            let is_device = matches!(kind, RealmNodeKindInfo::User);
            let present = !is_device
                && (is_current || parsed.is_some_and(|node_id| present_nodes.contains(&node_id)));
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
            // A device answering for itself is in contact by definition.
            let last_seen_ms = match (is_device, is_current) {
                (true, true) => Some(now_ms),
                (true, false) => parsed.and_then(|node_id| contacts.last_seen(&node_id)),
                (false, _) => None,
            };
            let seen_recently = is_device
                && (is_current
                    || parsed.is_some_and(|node_id| contacts.seen_recently(&node_id, now_ms)));
            RealmNodeInfoResponse {
                node_id: node.node_id.clone(),
                kind,
                owner: node.kind.owner().map(|owner| owner.to_string()),
                configured: true,
                present,
                connection_status: match (is_device, seen_recently, present) {
                    (true, true, _) => RealmNodeConnectionStatus::Seen,
                    (true, false, _) => RealmNodeConnectionStatus::Unknown,
                    (false, _, true) => RealmNodeConnectionStatus::Connected,
                    (false, _, false) => RealmNodeConnectionStatus::Configured,
                },
                last_seen_ms,
                placement,
                info,
            }
        })
        .collect()
}

/// Stale presence is candidate data, so it may not report a peer as connected;
/// only the local node stays present until a fresh lookup confirms the rest.
fn presence_nodes(
    presence: RealmPresence,
    local: aruna_core::NodeId,
) -> HashSet<aruna_core::NodeId> {
    if presence.is_stale() {
        return HashSet::from([local]);
    }
    let mut nodes = presence.into_nodes();
    nodes.insert(local);
    nodes
}

async fn load_realm_presence_best_effort(state: &ServerState) -> HashSet<aruna_core::NodeId> {
    // A realm with offline nodes must degrade to local-only presence rather
    // than stall the dashboard.
    let discovery = tokio::time::timeout(
        REALM_DISCOVERY_TIMEOUT,
        drive(
            GetRealmNodesOperation::new(state.get_realm_id()),
            &state.get_ctx(),
        ),
    )
    .await;
    match discovery {
        Ok(Ok(presence)) => presence_nodes(presence, state.get_node_id()),
        Ok(Err(error)) => {
            warn!(error = %error, "realm node discovery failed for realm info response");
            HashSet::from([state.get_node_id()])
        }
        Err(_) => {
            warn!("realm node discovery timed out for realm info response");
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

/// Adds the usage each backend's quota is measured against. Read once per
/// request from the maintained counters, not from the store itself.
async fn backend_statuses(
    state: &ServerState,
    backends: Vec<aruna_core::structs::BackendState>,
) -> Vec<BackendStatus> {
    let context = state.get_ctx();
    let mut statuses = Vec::with_capacity(backends.len());
    for backend in backends {
        let used_bytes = backend_used_bytes(&context, &BackendRef::Node(backend.name.clone()))
            .await
            .inspect_err(
                |error| warn!(backend = %backend.name, error = %error, "Backend usage unavailable"),
            )
            .ok();
        statuses.push(BackendStatus {
            name: backend.name,
            backend: backend.backend_type.to_string(),
            class: backend.class,
            allow_tenants: backend.allow_tenants,
            quota_bytes: backend.quota_bytes,
            used_bytes,
            default: backend.default,
            status: ServiceStatus::from(backend.status),
        });
    }
    statuses
}

#[utoipa::path(
    get,
    path = "/info",
    tag = "info",
    summary = "Report this node's health, version and service status",
    description = r#"The health check of a single node, answered locally and never routed to a peer.

**Authentication**: optional bearer token; every caller is answered, but the amount of detail
depends on the token, and a token of another realm or one that cannot be validated here counts as
anonymous.

**Behavior**
- The route always returns 200 for a node that is serving requests at all.
- Anonymous callers see only what is needed to health check the node and learn where to
  authenticate: node status, realm id, api version and the public interface urls.
- A bearer token of this realm adds the node's own identity and capability kind, its listen
  addresses, its peer connections and the network service summary.
- A token with WRITE on the realm's configuration admin path additionally reveals the blob and
  database services, every registered storage backend with its quota and used bytes, the portal
  deployment state, operational warnings and the last error of each peer connection.
- Gated values are absent or empty rather than restructured, so the same parser works at every
  level.
- Backend `used_bytes` comes from maintained counters and is absent when they cannot be read, which
  is not the same as zero."#,
    responses(
        (
            status = 200,
            description = "Node health and version; node identity, addresses and topology for realm-authenticated callers, backend detail for realm admins",
            body = InfoResponse,
            examples(
                ("Anonymous" = (
                    summary = "Public health check: status, realm and public urls only",
                    value = json!({
                        "node": {"status": "available", "realm_id": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"},
                        "api_version": "3.0.0-alpha.41",
                        "my_addresses": [],
                        "connections": [],
                        "services": {
                            "interfaces": {
                                "rest": {"status": "available", "bind": null, "url": "https://node.example.test/api/v1"},
                                "s3": {"status": "unavailable", "bind": null, "url": null}
                            }
                        },
                        "warnings": []
                    })
                )),
                ("Realm token" = (
                    summary = "A realm member also sees node identity, addresses and peer topology",
                    value = json!({
                        "node": {
                            "status": "available",
                            "realm_id": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                            "peer_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                            "capabilities": "server"
                        },
                        "api_version": "3.0.0-alpha.41",
                        "my_addresses": ["192.0.2.10:4433", "https://relay.example.test/"],
                        "connections": [
                            {
                                "peer_id": "2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a",
                                "status": "connected",
                                "active_addresses": [
                                    {
                                        "status": "active",
                                        "address": "192.0.2.11:4433",
                                        "rtt_ms": 12,
                                        "protocol_connections": [
                                            {"connection_id": 7, "protocol": "document_sync", "side": "client", "status": "open"}
                                        ]
                                    }
                                ],
                                "last_error": null,
                                "next_retry_secs": null
                            }
                        ],
                        "services": {
                            "network": {
                                "status": "available",
                                "discovery": ["dns"],
                                "relay": "default",
                                "relay_urls": ["https://relay.example.test/"],
                                "routing_table_size": 24
                            },
                            "interfaces": {
                                "rest": {"status": "available", "bind": "0.0.0.0:3000", "url": "https://node.example.test/api/v1"},
                                "s3": {"status": "available", "bind": "0.0.0.0:1337", "url": "https://s3.example.test"}
                            }
                        },
                        "warnings": []
                    })
                ))
            )
        )
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn get_info(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> (StatusCode, Json<InfoResponse>) {
    let access = info_access(&state, auth.as_ref()).await;
    let realm = access != InfoAccess::Public;
    let admin = access == InfoAccess::Admin;

    let mut interfaces = interface_services_status(&state).await;
    if !realm {
        interfaces.rest.bind = None;
        interfaces.s3.bind = None;
    }

    let mut response = InfoResponse {
        node: NodeStatus {
            status: ServiceStatus::Available,
            realm_id: state.get_realm_id().to_string(),
            peer_id: realm.then(|| state.get_node_id().to_string()),
            capabilities: realm.then(|| NodeCapabilityKind::from(state.node_capabilities())),
        },
        api_version: env!("CARGO_PKG_VERSION").to_string(),
        portal: None,
        my_addresses: Vec::new(),
        connections: Vec::new(),
        services: ServicesStatus {
            network: None,
            blob: None,
            database: None,
            interfaces,
        },
        warnings: Vec::new(),
    };

    if !realm {
        return (StatusCode::OK, Json(response));
    }

    let ctx = state.get_ctx();
    let observability = load_node_observability_status(ctx.as_ref()).await;

    let (network, warnings) = match observability.network {
        Some(info) => {
            response.my_addresses = info
                .endpoint_addr
                .addrs
                .iter()
                .map(transport_addr_to_string)
                .collect();
            response.connections = info
                .connections
                .iter()
                .map(|peer| map_peer_connection(peer, admin))
                .collect();
            (
                NetworkServiceStatus {
                    status: ServiceStatus::Available,
                    discovery: info.discovery_methods,
                    relay: Some(info.relay_method),
                    relay_urls: info.relay_urls,
                    routing_table_size: info.routing_table_size,
                    requests: admin.then(|| RequestSummary::from_state(&info.requests)),
                },
                info.warnings,
            )
        }
        None => (
            NetworkServiceStatus {
                status: ServiceStatus::Unavailable,
                discovery: Vec::new(),
                relay: None,
                relay_urls: Vec::new(),
                routing_table_size: None,
                requests: admin.then(RequestSummary::default),
            },
            Vec::new(),
        ),
    };
    response.services.network = Some(network);

    if admin {
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
                backends: backend_statuses(&state, info.backends).await,
            },
            None => BlobServiceStatus {
                status: ServiceStatus::NotConfigured,
                backend: None,
                max_bucket_size: None,
                multipart_bucket: None,
                timeouts_secs: None,
                backends: Vec::new(),
            },
        };
        response.services.blob = Some(blob);
        response.services.database = Some(DatabaseServiceStatus {
            status: ServiceStatus::from(observability.database.status),
            requests: RequestSummary::from_state(&observability.database.requests),
        });
        response.portal = Some(state.portal_status().await);
        response.warnings = warnings;
    }

    (StatusCode::OK, Json(response))
}

/// Maps a live peer connection to its wire form. `last_error` leaks internal
/// diagnostics, so it is populated for realm config admins only.
fn map_peer_connection(
    peer: &aruna_core::structs::PeerConnectionState,
    admin: bool,
) -> PeerConnectionInfo {
    PeerConnectionInfo {
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
        last_error: admin.then(|| peer.last_error.clone()).flatten(),
        next_retry_secs: peer.next_retry_in_secs,
    }
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
        Alpn::NativeReference => "native_reference".to_string(),
        Alpn::Notification => "notification".to_string(),
        Alpn::Shard => "shard".to_string(),
        Alpn::JobControl => "job_control".to_string(),
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
        InterfaceServicesStatus, InterfaceStatus, NodeCapabilityKind, PeerContacts,
        RealmNodeConnectionStatus, RealmNodeKindInfo, RealmPlacementBinding,
        RealmPlacementBindingScope, RealmPlacementMutationRequest, RealmPlacementOverride,
        RealmPlacementStrategy, RealmQuotaConfig, RealmUserGroupCapOverride, ServiceStatus,
        get_info, get_realm_info, get_realm_placement, get_usage, map_handle_error,
        map_mutate_realm_placement_error, map_realm_nodes, map_set_realm_quota_error,
        mutate_realm_placement, presence_nodes, set_realm_quota,
    };
    use crate::error::ServerError;
    use crate::openapi::ApiDoc;
    use crate::server_state::ServerState;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keys::generate_signing_key;
    use aruna_core::keyspaces::GROUP_KEYSPACE;
    use aruna_core::structs::{
        Actor, AuthContext, DocumentClass, Group, NodeCapabilities, PlacementScope, QuotaConfig,
        RealmId,
    };
    use aruna_operations::allocate_handle::{HandleAllocationError, allocate_placement_binding};
    use aruna_operations::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_operations::get_realm_nodes::RealmPresence;
    use aruna_operations::mutate_realm_placement::MutateRealmPlacementError;
    use aruna_operations::set_realm_quota::SetRealmQuotaError;
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use axum::body::Body;
    use axum::extract::{FromRequest, State};
    use axum::http::StatusCode;
    use axum::{Extension, Json};
    use std::collections::{BTreeMap, HashSet};
    use std::sync::Arc;
    use tempfile::{TempDir, tempdir};
    use tower::ServiceExt;
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
            compute_handle: None,
        });

        let realm_signing_key = generate_signing_key();
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let node_id = iroh::SecretKey::generate().public();

        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm_id,
                node_id,
                NodeCapabilities::user_node(realm_id).unwrap(),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );

        (state, tempdir)
    }

    fn foreign_auth() -> AuthContext {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        AuthContext {
            user_id: UserId::local(Ulid::generate(), realm_id),
            realm_id,
            path_restrictions: None,
        }
    }

    fn key_set(value: &serde_json::Value) -> std::collections::HashSet<&str> {
        value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect()
    }

    /// Anonymous and foreign-realm callers keep the flat shape, but every gated
    /// value is absent or empty: the node reports only health and realm, the
    /// interfaces only their public urls, and node identity, topology, backend
    /// detail and warnings are gone.
    #[tokio::test]
    async fn anonymous_info_hides_detail() {
        let (state, _tempdir) = setup_state().await;
        state
            .register_rest_interface("0.0.0.0:3000".parse().unwrap())
            .await;

        for auth in [None, Some(foreign_auth())] {
            let (status, Json(response)) = get_info(State(state.clone()), Extension(auth)).await;

            assert_eq!(status, StatusCode::OK);
            assert_eq!(response.node.status, ServiceStatus::Available);
            assert!(response.node.peer_id.is_none());
            assert!(response.node.capabilities.is_none());
            assert!(response.portal.is_none());
            assert!(response.my_addresses.is_empty());
            assert!(response.connections.is_empty());
            assert!(response.warnings.is_empty());
            assert!(response.services.network.is_none());
            assert!(response.services.blob.is_none());
            assert!(response.services.database.is_none());
            assert_eq!(
                response.services.interfaces.rest.url.as_deref(),
                Some("http://127.0.0.1:3000/api/v1"),
                "clients still learn the public api url"
            );
            assert!(response.services.interfaces.rest.bind.is_none());

            let body = serde_json::to_value(&response).unwrap();
            assert_eq!(
                key_set(&body),
                std::collections::HashSet::from([
                    "node",
                    "api_version",
                    "my_addresses",
                    "connections",
                    "services",
                    "warnings",
                ])
            );
            assert_eq!(
                key_set(&body["node"]),
                std::collections::HashSet::from(["status", "realm_id"])
            );
            assert_eq!(
                key_set(&body["services"]),
                std::collections::HashSet::from(["interfaces"])
            );
            assert_eq!(body["api_version"], env!("CARGO_PKG_VERSION"));
            assert_eq!(body["node"]["realm_id"], state.get_realm_id().to_string());
        }
    }

    /// A realm token unlocks node identity, addresses and peers; backend detail
    /// and request metrics stay admin-only.
    #[tokio::test]
    async fn realm_token_sees_topology() {
        let (state, _tempdir) = setup_state().await;
        state
            .register_rest_interface("0.0.0.0:3000".parse().unwrap())
            .await;
        state
            .register_s3_interface("0.0.0.0:1337".parse().unwrap(), "127.0.0.1:1337")
            .await;
        let auth = test_auth_context(&state);

        let (status, Json(response)) = get_info(State(state.clone()), Extension(Some(auth))).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(response.node.peer_id, Some(state.get_node_id().to_string()));
        assert_eq!(response.node.capabilities, Some(NodeCapabilityKind::User));
        let network = response.services.network.expect("realm token sees network");
        assert_eq!(network.status, ServiceStatus::Unavailable);
        assert!(
            network.requests.is_none(),
            "request metrics stay admin-only"
        );
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
        assert!(
            response.services.blob.is_none(),
            "backend detail stays admin-only"
        );
        assert!(response.services.database.is_none());
        assert!(response.portal.is_none());
        assert!(response.warnings.is_empty());
    }

    /// Backend detail and errors need a realm config admin; a plain realm member
    /// sees node identity but no backend services.
    #[tokio::test]
    async fn admin_sees_operations() {
        let (state, realm_id, admin, _tempdir) = setup_management_state().await;
        let member = AuthContext {
            user_id: UserId::local(Ulid::generate(), realm_id),
            realm_id,
            path_restrictions: None,
        };

        let (_, Json(response)) = get_info(State(state.clone()), Extension(Some(member))).await;
        assert!(
            response.node.peer_id.is_some(),
            "realm member sees node identity"
        );
        assert!(
            response.services.blob.is_none(),
            "plain realm member is not an admin"
        );
        assert!(response.services.database.is_none());
        assert!(response.portal.is_none());

        let (status, Json(response)) =
            get_info(State(state), Extension(Some(admin_auth(realm_id, admin)))).await;

        assert_eq!(status, StatusCode::OK);
        assert!(response.node.peer_id.is_some());
        assert_eq!(
            response.services.blob.expect("admin sees blob").status,
            ServiceStatus::NotConfigured
        );
        assert_eq!(
            response
                .services
                .database
                .expect("admin sees database")
                .status,
            ServiceStatus::Available
        );
        assert_eq!(response.portal.expect("admin sees portal").mode, "disabled");
    }

    /// `last_error` on a peer connection leaks internal diagnostics, so a realm
    /// member sees it redacted while a realm config admin sees it in full.
    #[test]
    fn peer_error_admin_only() {
        let peer = aruna_core::structs::PeerConnectionState {
            node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            status: aruna_core::structs::PeerConnectionStatus::Unreachable,
            active_addresses: Vec::new(),
            last_error: Some("dial refused: 10.0.0.9:4433".to_string()),
            next_retry_in_secs: Some(5),
        };

        let member = super::map_peer_connection(&peer, false);
        assert!(
            member.last_error.is_none(),
            "non-admin must not see peer errors"
        );
        assert_eq!(member.next_retry_secs, Some(5));
        assert_eq!(member.peer_id, peer.node_id.to_string());

        let admin = super::map_peer_connection(&peer, true);
        assert_eq!(
            admin.last_error.as_deref(),
            Some("dial refused: 10.0.0.9:4433")
        );
    }

    #[tokio::test]
    async fn admin_info_reports_storage_errors() {
        let (state, realm_id, admin, _tempdir) = setup_management_state().await;

        let _ = state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: "missing".to_string(),
                key: b"key".to_vec().into(),
                txn_id: Some(ulid::Ulid::generate()),
            })
            .await;

        let (status, Json(response)) =
            get_info(State(state), Extension(Some(admin_auth(realm_id, admin)))).await;

        assert_eq!(status, StatusCode::OK);
        let database = response.services.database.expect("admin sees database");
        assert_eq!(database.status, ServiceStatus::Available);
        assert_eq!(
            database.requests.last_error.as_deref(),
            Some("Transaction not found")
        );
        assert!(database.requests.failure_rate > 0.0);
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
            user_id: UserId::local(Ulid::generate(), state.get_realm_id()),
            realm_id: state.get_realm_id(),
            path_restrictions: None,
        }
    }

    /// The usage directory is closed to anonymous and foreign-realm callers.
    #[tokio::test]
    async fn usage_requires_realm_auth() {
        let (state, _tempdir) = setup_state().await;
        seed_usage_state(&state).await;

        assert!(matches!(
            get_usage(State(state.clone()), Extension(None)).await,
            Err(ServerError::Unauthorized)
        ));
        assert!(matches!(
            get_usage(State(state.clone()), Extension(Some(foreign_auth()))).await,
            Err(ServerError::Forbidden)
        ));

        let auth = test_auth_context(&state);
        let (status, Json(response)) = get_usage(State(state), Extension(Some(auth)))
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response.buckets, 2, "flat fields report local total");
        assert_eq!(response.realm.buckets, 5, "realm sums local and remote");
        assert_eq!(
            response.metadata_documents, None,
            "a node without a metadata subsystem omits the count"
        );
        let body = serde_json::to_value(&response).unwrap();
        assert!(!body.as_object().unwrap().contains_key("metadata_documents"));
    }

    /// The reported total covers every live document in the realm, including
    /// the private ones the caller holds no role for.
    #[tokio::test]
    async fn usage_counts_documents() {
        use aruna_core::storage_entries::metadata_registry_write_entries;
        use aruna_core::structs::{MetadataRegistryRecord, PlacementRef};

        let storage_dir = tempdir().unwrap();
        let metadata_dir = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let node_id = iroh::SecretKey::generate().public();
        let metadata_handle = aruna_operations::metadata::MetadataHandle::new(
            metadata_dir.path(),
            node_id,
            storage_handle.clone(),
            None,
            None,
            None,
        )
        .unwrap();
        let realm_signing_key = generate_signing_key();
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let driver_ctx = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: Some(metadata_handle),
            task_handle: None,
            compute_handle: None,
        });
        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm_id,
                node_id,
                NodeCapabilities::user_node(realm_id).unwrap(),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );

        let group_id = Ulid::generate();
        let mut writes = Vec::new();
        for (index, public) in [(0u8, true), (1, true), (2, false)] {
            let document_id = Ulid::from_parts(index.into(), index.into());
            let path = format!("datasets/{index}");
            let record = MetadataRegistryRecord {
                realm_id,
                group_id,
                document_id,
                document_path: path.clone(),
                graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
                public,
                permission_path: MetadataRegistryRecord::permission_path_for(
                    &realm_id,
                    group_id,
                    &path,
                    document_id,
                ),
                placement: PlacementRef::NIL,
                holder_node_ids: Vec::new(),
                created_at_ms: 1,
                updated_at_ms: 1,
                establishing_event_id: Ulid::nil(),
                last_event_id: Ulid::nil(),
            };
            writes.extend(metadata_registry_write_entries(&record).unwrap());
        }
        assert!(matches!(
            state
                .get_ctx()
                .storage_handle
                .send_storage_effect(StorageEffect::BatchWrite {
                    writes,
                    txn_id: None,
                })
                .await,
            aruna_core::events::Event::Storage(
                aruna_core::events::StorageEvent::BatchWriteResult { .. }
            )
        ));

        let auth = test_auth_context(&state);
        let (_, Json(response)) = get_usage(State(state), Extension(Some(auth)))
            .await
            .unwrap();

        assert_eq!(response.metadata_documents, Some(3));
    }

    #[test]
    fn group_quota_status_reports_warning_and_unlimited() {
        let group = Ulid::generate();
        let unlimited_group = Ulid::generate();
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
        let group = Ulid::generate();
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

    #[test]
    fn quota_capacity_unavailable() {
        // Cleanup capacity is transient, so it must not read as an internal error.
        let error = map_set_realm_quota_error(SetRealmQuotaError::StorageError(
            StorageError::CleanupCapacity,
        ));

        assert!(matches!(error, ServerError::ServiceUnavailableReason(_)));
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
            compute_handle: None,
        });

        let realm_signing_key = generate_signing_key();
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let user_id = UserId::local(Ulid::generate(), realm_id);
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
                node_labels: Default::default(),
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
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );

        (state, realm_id, user_id, tempdir)
    }

    fn admin_auth(realm_id: RealmId, user_id: UserId) -> AuthContext {
        AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        }
    }

    /// Installs a realm deny policy for one permission path.
    async fn deny_path(state: &ServerState, path: &str) {
        let realm_id = state.get_realm_id();
        let mut config = drive(
            aruna_operations::get_realm_config::GetRealmConfigOperation::new(realm_id),
            &state.get_ctx(),
        )
        .await
        .unwrap();
        config
            .request_policies
            .push(aruna_core::request_policy::RequestPolicy {
                policy_id: Ulid::generate(),
                name: "deny-path".to_string(),
                kind: aruna_core::request_policy::PolicyKind::Deny,
                when: None,
                expression: format!("path == '{path}'"),
                enabled: true,
            });
        let actor = Actor {
            node_id: state.get_node_id(),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
                key: realm_id.as_bytes().to_vec().into(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;
    }

    #[tokio::test]
    async fn policy_blocks_admin() {
        // The config-admin gate must honour realm request policies, not only
        // the RBAC roles the permission check reads.
        let (state, realm_id, admin, _tempdir) = setup_management_state().await;
        let auth = admin_auth(realm_id, admin);
        let _ = get_realm_placement(State(state.clone()), Extension(Some(auth.clone())))
            .await
            .expect("the realm admin sees the placement config");

        deny_path(&state, &format!("/{realm_id}/admin/config")).await;

        assert!(matches!(
            get_realm_placement(State(state.clone()), Extension(Some(auth))).await,
            Err(ServerError::Forbidden)
        ));
    }

    fn placement_strategy(strategy_id: Ulid) -> RealmPlacementStrategy {
        RealmPlacementStrategy {
            strategy_id: strategy_id.to_string(),
            name: "hot".to_string(),
            replica_count: Some(2),
            distinct_locations: true,
            affinity: Vec::new(),
            shard_count: 64,
        }
    }

    async fn provision_strategy(state: &ServerState, actor: Actor, strategy_id: Ulid) {
        allocate_placement_binding(
            state.get_ctx().as_ref(),
            actor.clone(),
            PlacementScope::Realm(actor.realm_id),
            DocumentClass::Metadata,
            strategy_id,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn realm_placement_admin_endpoints_require_auth_and_management_node() {
        let (state, realm_id, _admin, _tempdir) = setup_management_state().await;
        assert!(matches!(
            get_realm_placement(State(state.clone()), Extension(None)).await,
            Err(ServerError::Unauthorized)
        ));

        let (local_state, _tempdir) = setup_state().await;
        let local_auth = AuthContext {
            user_id: UserId::local(Ulid::generate(), local_state.get_realm_id()),
            realm_id: local_state.get_realm_id(),
            path_restrictions: None,
        };
        assert!(matches!(
            get_realm_placement(State(local_state), Extension(Some(local_auth))).await,
            Err(ServerError::Forbidden)
        ));

        let request = RealmPlacementMutationRequest::RemoveOverride {
            subject: "00".to_string(),
        };
        assert!(matches!(
            mutate_realm_placement(State(state.clone()), Extension(None), Ok(Json(request))).await,
            Err(ServerError::Unauthorized)
        ));

        let stranger = AuthContext {
            user_id: UserId::local(Ulid::generate(), realm_id),
            realm_id,
            path_restrictions: None,
        };
        assert!(matches!(
            get_realm_placement(State(state), Extension(Some(stranger))).await,
            Err(ServerError::Forbidden)
        ));
    }

    #[tokio::test]
    async fn realm_placement_strategy_default_binding_and_override_lifecycle() {
        let (state, realm_id, admin, _tempdir) = setup_management_state().await;
        let auth = admin_auth(realm_id, admin);
        let job_family_strategy_id = drive(
            aruna_operations::get_realm_config::GetRealmConfigOperation::new(realm_id),
            &state.get_ctx(),
        )
        .await
        .unwrap()
        .job_family_strategy_id
        .to_string();
        let (_, Json(initial)) =
            get_realm_placement(State(state.clone()), Extension(Some(auth.clone())))
                .await
                .unwrap();
        assert_eq!(
            serde_json::to_value(&initial).unwrap()["job_family_strategy_id"].as_str(),
            Some(job_family_strategy_id.as_str())
        );
        let initial_default = initial.default_strategy_id.unwrap();
        let strategy_id = Ulid::from_bytes([21; 16]);
        let scope = RealmPlacementBindingScope::Realm;
        let node_id = state.get_node_id().to_string();

        let (_status, Json(after_upsert)) = mutate_realm_placement(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Ok(Json(RealmPlacementMutationRequest::UpsertStrategy {
                strategy: placement_strategy(strategy_id),
            })),
        )
        .await
        .unwrap();
        assert_eq!(
            serde_json::to_value(after_upsert).unwrap()["job_family_strategy_id"].as_str(),
            Some(job_family_strategy_id.as_str())
        );
        let _ = mutate_realm_placement(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Ok(Json(
                RealmPlacementMutationRequest::ProvisionMetadataBinding {
                    strategy_id: strategy_id.to_string(),
                    group_id: None,
                },
            )),
        )
        .await
        .unwrap();

        for request in [
            RealmPlacementMutationRequest::SetDefaultStrategy {
                strategy_id: strategy_id.to_string(),
            },
            RealmPlacementMutationRequest::SetBinding {
                binding: RealmPlacementBinding {
                    scope: scope.clone(),
                    strategy_id: strategy_id.to_string(),
                },
            },
            RealmPlacementMutationRequest::SetOverride {
                placement_override: RealmPlacementOverride {
                    subject: "abcd".to_string(),
                    pinned: vec![node_id],
                    excluded: Vec::new(),
                    strategy_id: Some(strategy_id.to_string()),
                },
            },
        ] {
            let (status, _) = mutate_realm_placement(
                State(state.clone()),
                Extension(Some(auth.clone())),
                Ok(Json(request)),
            )
            .await
            .unwrap();
            assert_eq!(status, StatusCode::OK);
        }

        let (_, Json(stored)) =
            get_realm_placement(State(state.clone()), Extension(Some(auth.clone())))
                .await
                .unwrap();
        assert_eq!(stored.default_strategy_id, Some(strategy_id.to_string()));
        assert!(stored.strategies.iter().any(|strategy| {
            strategy.strategy_id == strategy_id.to_string() && strategy.replica_count == Some(2)
        }));
        assert!(stored.bindings.iter().any(|binding| {
            binding.scope == scope && binding.strategy_id == strategy_id.to_string()
        }));
        assert!(stored.overrides.iter().any(|record| {
            record.subject == "abcd" && record.strategy_id == Some(strategy_id.to_string())
        }));

        let error = mutate_realm_placement(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Ok(Json(RealmPlacementMutationRequest::RemoveStrategy {
                strategy_id: strategy_id.to_string(),
            })),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ServerError::Conflict(message) if message.contains("referenced")));

        for request in [
            RealmPlacementMutationRequest::RemoveOverride {
                subject: "abcd".to_string(),
            },
            RealmPlacementMutationRequest::RemoveBinding {
                scope: scope.clone(),
            },
            RealmPlacementMutationRequest::SetDefaultStrategy {
                strategy_id: initial_default,
            },
        ] {
            let (_status, _body) = mutate_realm_placement(
                State(state.clone()),
                Extension(Some(auth.clone())),
                Ok(Json(request)),
            )
            .await
            .unwrap();
        }

        let error = mutate_realm_placement(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Ok(Json(RealmPlacementMutationRequest::RemoveStrategy {
                strategy_id: strategy_id.to_string(),
            })),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ServerError::Conflict(message) if message.contains("referenced")));

        let (_, Json(stored)) = get_realm_placement(State(state), Extension(Some(auth)))
            .await
            .unwrap();
        assert!(
            stored
                .strategies
                .iter()
                .any(|strategy| strategy.strategy_id == strategy_id.to_string())
        );
        assert!(!stored.bindings.iter().any(|binding| binding.scope == scope));
        assert!(
            !stored
                .overrides
                .iter()
                .any(|record| record.subject == "abcd")
        );
    }

    #[tokio::test]
    async fn realm_info_reports_finite_and_unbounded_default_strategy_replication() {
        let (state, realm_id, admin, _tempdir) = setup_management_state().await;
        let auth = admin_auth(realm_id, admin);
        let strategy_id = Ulid::from_bytes([24; 16]);

        let (_status, _body) = mutate_realm_placement(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Ok(Json(RealmPlacementMutationRequest::UpsertStrategy {
                strategy: placement_strategy(strategy_id),
            })),
        )
        .await
        .unwrap();
        provision_strategy(
            state.as_ref(),
            Actor {
                node_id: state.get_node_id(),
                user_id: admin,
                realm_id,
            },
            strategy_id,
        )
        .await;
        let (_status, _body) = mutate_realm_placement(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Ok(Json(RealmPlacementMutationRequest::SetDefaultStrategy {
                strategy_id: strategy_id.to_string(),
            })),
        )
        .await
        .unwrap();

        let (_, Json(info)) = get_realm_info(State(state.clone()), Extension(Some(auth.clone())))
            .await
            .unwrap();
        assert_eq!(
            info.metadata_replication.default_replication_factor,
            Some(2)
        );

        let mut unbounded = placement_strategy(strategy_id);
        unbounded.replica_count = None;
        let _ = mutate_realm_placement(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Ok(Json(RealmPlacementMutationRequest::UpsertStrategy {
                strategy: unbounded,
            })),
        )
        .await
        .unwrap();

        let (_, Json(info)) = get_realm_info(State(state), Extension(Some(auth)))
            .await
            .unwrap();
        assert_eq!(info.metadata_replication.default_replication_factor, None);
        assert_eq!(
            serde_json::to_value(info.metadata_replication).unwrap()["default_replication_factor"],
            serde_json::Value::Null
        );
    }

    #[test]
    fn realm_metadata_replication_schema_allows_unbounded_default() {
        let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let factor = &openapi["components"]["schemas"]["RealmMetadataReplicationResponse"]["properties"]
            ["default_replication_factor"];
        assert_eq!(
            factor["type"],
            serde_json::json!(["integer", "null"]),
            "default_replication_factor schema must represent finite and unbounded defaults"
        );
    }

    #[tokio::test]
    async fn realm_placement_rejects_zero_replicas_dangling_refs_and_invalid_strings() {
        let (state, realm_id, admin, _tempdir) = setup_management_state().await;
        let auth = admin_auth(realm_id, admin);
        let missing = Ulid::from_bytes([22; 16]);
        let mut zero = placement_strategy(missing);
        zero.replica_count = Some(0);

        for request in [
            RealmPlacementMutationRequest::UpsertStrategy { strategy: zero },
            RealmPlacementMutationRequest::SetDefaultStrategy {
                strategy_id: missing.to_string(),
            },
            RealmPlacementMutationRequest::SetBinding {
                binding: RealmPlacementBinding {
                    scope: RealmPlacementBindingScope::Realm,
                    strategy_id: missing.to_string(),
                },
            },
            RealmPlacementMutationRequest::SetOverride {
                placement_override: RealmPlacementOverride {
                    subject: "00".to_string(),
                    pinned: Vec::new(),
                    excluded: Vec::new(),
                    strategy_id: Some(missing.to_string()),
                },
            },
            RealmPlacementMutationRequest::ProvisionMetadataBinding {
                strategy_id: missing.to_string(),
                group_id: None,
            },
        ] {
            assert!(matches!(
                mutate_realm_placement(
                    State(state.clone()),
                    Extension(Some(auth.clone())),
                    Ok(Json(request))
                )
                .await,
                Err(ServerError::BadRequestReason(_))
            ));
        }

        for request in [
            RealmPlacementMutationRequest::RemoveStrategy {
                strategy_id: "not-a-ulid".to_string(),
            },
            RealmPlacementMutationRequest::RemoveOverride {
                subject: "not-hex".to_string(),
            },
            RealmPlacementMutationRequest::SetOverride {
                placement_override: RealmPlacementOverride {
                    subject: "00".to_string(),
                    pinned: vec!["not-a-node".to_string()],
                    excluded: Vec::new(),
                    strategy_id: None,
                },
            },
        ] {
            assert!(matches!(
                mutate_realm_placement(
                    State(state.clone()),
                    Extension(Some(auth.clone())),
                    Ok(Json(request))
                )
                .await,
                Err(ServerError::BadRequestReason(_))
            ));
        }

        let request = axum::http::Request::builder()
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"mutation":"remove_override","subject":"00","extra":true}"#,
            ))
            .unwrap();
        let rejection = Json::<RealmPlacementMutationRequest>::from_request(request, &())
            .await
            .unwrap_err();
        assert!(matches!(
            mutate_realm_placement(State(state), Extension(Some(auth)), Err(rejection)).await,
            Err(ServerError::BadRequestReason(_))
        ));
    }

    #[tokio::test]
    async fn realm_placement_route_is_registered() {
        let (state, _realm_id, _admin, _tempdir) = setup_management_state().await;
        let response = crate::routes::rest_router(state)
            .oneshot(
                axum::http::Request::builder()
                    .uri("/info/realm/placement")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn openapi_registers_realm_placement_get_patch_and_schemas() {
        let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let path = &openapi["paths"]["/info/realm/placement"];
        assert!(path.get("get").is_some());
        assert!(path.get("patch").is_some());
        assert!(
            openapi["components"]["schemas"]
                .get("RealmPlacementMutationRequest")
                .is_some()
        );
        assert!(
            openapi["components"]["schemas"]
                .get("RealmPlacementConfigResponse")
                .is_some()
        );
    }

    #[test]
    fn realm_placement_transaction_conflict_maps_to_http_conflict() {
        let error = map_mutate_realm_placement_error(MutateRealmPlacementError::StorageError(
            StorageError::TransactionConflict,
        ));
        assert!(matches!(
            error,
            ServerError::Conflict(message) if message.contains("retry")
        ));
    }

    #[test]
    fn realm_placement_missing_config_maps_to_not_found() {
        assert!(matches!(
            map_mutate_realm_placement_error(MutateRealmPlacementError::RealmConfigNotFound),
            ServerError::NotFound
        ));
    }

    #[test]
    fn placement_capacity_unavailable() {
        // Cleanup capacity is transient on both placement paths.
        assert!(matches!(
            map_mutate_realm_placement_error(MutateRealmPlacementError::StorageError(
                StorageError::CleanupCapacity
            )),
            ServerError::ServiceUnavailableReason(_)
        ));
        assert!(matches!(
            map_handle_error(HandleAllocationError::Storage(
                StorageError::CleanupCapacity
            )),
            ServerError::ServiceUnavailableReason(_)
        ));
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
            user_id: UserId::local(Ulid::generate(), realm_id),
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
        body.max_devices_per_user = Some(3);
        body.device_requests_per_minute = Some(600);
        body.device_concurrent_pulls = Some(8);

        let (status, Json(stored)) = set_realm_quota(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Json(body),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(stored.default_group_quota_bytes, Some(4096));
        assert_eq!(stored.max_devices_per_user, Some(3));
        assert_eq!(stored.device_requests_per_minute, Some(600));
        assert_eq!(stored.device_concurrent_pulls, Some(8));

        let (status, Json(info)) = get_realm_info(State(state), Extension(Some(auth)))
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
        let quota = info.quota.expect("realm token sees quota");
        assert_eq!(quota.default_group_quota_bytes, Some(4096));
        assert_eq!(quota.max_devices_per_user, Some(3));
    }

    /// Anonymous callers keep what they need to authenticate; realm topology,
    /// discovery and quota policy need a token of this realm.
    #[tokio::test]
    async fn realm_info_gates_detail() {
        let (state, realm_id, admin, _tempdir) = setup_management_state().await;
        state
            .register_rest_interface("0.0.0.0:3000".parse().unwrap())
            .await;
        let auth = admin_auth(realm_id, admin);
        let mut body = RealmQuotaConfig::from(QuotaConfig::default());
        body.user_group_cap_overrides = vec![RealmUserGroupCapOverride {
            user_id: admin.to_string(),
            max_groups: Some(1),
        }];
        let _ = set_realm_quota(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Json(body),
        )
        .await
        .unwrap();

        for anonymous in [None, Some(foreign_auth())] {
            let (_status, Json(info)) = get_realm_info(State(state.clone()), Extension(anonymous))
                .await
                .unwrap();
            assert_eq!(info.realm_id, realm_id.to_string());
            assert_eq!(info.description, "Realm");
            assert!(info.nodes.is_empty(), "realm topology is not public");
            assert!(info.is_management_node, "the node kind is public");
            assert_eq!(
                info.management_urls,
                vec!["http://127.0.0.1:3000/api/v1".to_string()],
                "a management node names its own published url"
            );
            assert!(info.quota.is_none(), "quota policy is not public");
            assert!(info.discovery.is_none(), "discovery is not public");
            assert_eq!(
                info.interfaces.rest.url.as_deref(),
                Some("http://127.0.0.1:3000/api/v1"),
                "clients still learn the public api url"
            );
            assert!(
                info.interfaces.rest.bind.is_none(),
                "listen address is not public"
            );
            let body = serde_json::to_value(&info).unwrap();
            assert!(
                body.get("metadata_replication").is_some(),
                "replication policy stays public"
            );
            assert!(
                body.get("detail").is_none(),
                "flat shape has no detail wrapper"
            );
        }

        let (_status, Json(info)) = get_realm_info(State(state), Extension(Some(auth)))
            .await
            .unwrap();
        assert_eq!(info.interfaces.rest.bind.as_deref(), Some("0.0.0.0:3000"));
        assert!(!info.nodes.is_empty());
        assert!(info.discovery.is_some());
        let quota = info.quota.expect("realm token sees quota");
        assert_eq!(quota.user_group_cap_overrides.len(), 1);
        assert_eq!(quota.user_group_cap_overrides[0].user_id, admin.to_string());
    }

    /// Signed-out callers receive only aggregate overview values. An
    /// unavailable metadata count is serialized as null, never as a false zero.
    #[tokio::test]
    async fn anonymous_realm_info_exposes_count_only_public_overview() {
        let (state, realm_id, admin, _tempdir) = setup_management_state().await;
        let group = Group {
            display_name: "Protected group title".to_string(),
            group_id: Ulid::generate(),
            realm_id,
            roles: Default::default(),
            owner: admin,
        };
        let actor = Actor {
            node_id: state.get_node_id(),
            user_id: admin,
            realm_id,
        };
        match state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: GROUP_KEYSPACE.to_string(),
                key: group.group_id.to_bytes().to_vec().into(),
                value: group.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected group write: {other:?}"),
        }

        let (status, Json(info)) = get_realm_info(State(state), Extension(None)).await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert!(info.nodes.is_empty());
        assert!(info.discovery.is_none());
        assert!(info.quota.is_none());
        let overview = info.public_overview.as_ref().expect("public overview");
        assert_eq!(overview.live_datasets, None);
        assert_eq!(overview.groups, Some(1));
        assert_eq!(overview.nodes_configured, Some(1));

        let body = serde_json::to_value(&info).unwrap();
        assert_eq!(
            key_set(&body["public_overview"]),
            std::collections::HashSet::from(["live_datasets", "groups", "nodes_configured",])
        );
        assert!(body["public_overview"]["live_datasets"].is_null());
        assert_ne!(body["public_overview"]["live_datasets"], 0);
        assert!(body.get("discovery").is_none());
        assert!(body.get("quota").is_none());
        assert_eq!(body["nodes"], serde_json::json!([]));
        assert!(!body.to_string().contains("Protected group title"));
    }

    #[tokio::test]
    async fn management_urls_follow_documents() {
        // The published document names the url a device follows; without one
        // a management node falls back to its own interface, and a server
        // node lists the others but never itself.
        use aruna_core::keyspaces::NODE_INFO_KEYSPACE;
        use aruna_core::structs::{
            NodeInfoDocument, NodeUrls, NodeUtilization, node_info_storage_key,
        };

        let (state, _realm_id, _admin, _tempdir) = setup_management_state().await;
        state
            .register_rest_interface("0.0.0.0:3000".parse().unwrap())
            .await;
        let node_id = state.get_node_id();
        let document = NodeInfoDocument {
            node_id,
            executors: Vec::new(),
            labels: Default::default(),
            urls: NodeUrls {
                api: Some("https://mgmt.example/api/v1".to_string()),
                s3: None,
            },
            utilization: NodeUtilization {
                storage_bytes_used: 0,
                documents_held: None,
                load_permille: None,
                heartbeat_at_ms: 1_700_000_000_000,
            },
            updated_at_ms: 1_700_000_000_500,
            epoch: aruna_core::structs::AdvertisementEpoch {
                membership_generation: 1,
                publisher_generation: 1,
                observed_at_ms: 1_700_000_000_500,
            },
            compute_draining: false,
            leaving: false,
            demand: Default::default(),
            reservation: Default::default(),
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

        let (_status, Json(info)) = get_realm_info(State(state), Extension(None)).await.unwrap();
        assert!(info.is_management_node);
        assert_eq!(
            info.management_urls,
            vec!["https://mgmt.example/api/v1".to_string()]
        );
    }

    #[tokio::test]
    async fn get_realm_info_includes_placement_and_node_info() {
        use aruna_core::keyspaces::NODE_INFO_KEYSPACE;
        use aruna_core::structs::{
            NodeInfoDocument, NodeUrls, NodeUtilization, node_info_storage_key,
        };

        let (state, realm_id, admin, _tempdir) = setup_management_state().await;
        let node_id = state.get_node_id();

        // The creating node's placement entry is seeded at realm creation with
        // the default location/weight. Publish a node info document for it too.
        let mut docker = aruna_core::compute::ExecutorCapability::new(
            "docker".to_string(),
            aruna_core::structs::PlacementSubject {
                node_id,
                generation: 1,
                location: "eu-west".to_string(),
                labels: std::collections::BTreeMap::new(),
                executor_kind: None,
                local_to_controller: true,
            },
        )
        .expect("subject is valid");
        docker.file_staging = true;
        docker.direct_s3 = true;
        let document = NodeInfoDocument {
            node_id,
            executors: vec![docker],
            labels: std::collections::BTreeMap::from([("tier".to_string(), "hot".to_string())]),
            urls: NodeUrls {
                api: None,
                s3: Some("s3.example".to_string()),
            },
            utilization: NodeUtilization {
                storage_bytes_used: 4_096,
                documents_held: None,
                load_permille: None,
                heartbeat_at_ms: 1_700_000_000_000,
            },
            updated_at_ms: 1_700_000_000_500,
            epoch: aruna_core::structs::AdvertisementEpoch {
                membership_generation: 1,
                publisher_generation: 1,
                observed_at_ms: 1_700_000_000_500,
            },
            compute_draining: false,
            leaving: false,
            demand: Default::default(),
            reservation: Default::default(),
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

        let (status, Json(info)) =
            get_realm_info(State(state), Extension(Some(admin_auth(realm_id, admin))))
                .await
                .unwrap();
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
        assert_eq!(node_info.executors.len(), 1);
        assert_eq!(node_info.executors[0].kind, "docker");
        assert_eq!(node_info.labels.get("tier"), Some(&"hot".to_string()));
        assert_eq!(node_info.urls.s3.as_deref(), Some("s3.example"));
        assert_eq!(node_info.utilization.storage_bytes_used, 4_096);
        let serialized = serde_json::to_value(node_info).unwrap();
        assert!(serialized["urls"].get("api").is_none());
        assert!(serialized["utilization"].get("documents_held").is_none());
        assert!(serialized["utilization"].get("load_permille").is_none());
    }

    #[test]
    fn node_info_openapi_marks_optional_fields_as_not_required() {
        let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();
        for (schema, optional_fields) in [
            ("RealmNodeUrlsResponse", &["api", "s3"][..]),
            (
                "RealmNodeUtilizationResponse",
                &["documents_held", "load_permille"][..],
            ),
        ] {
            let schema = &openapi["components"]["schemas"][schema];
            for field in optional_fields {
                assert!(schema["properties"].get(field).is_some());
                assert!(
                    !schema["required"]
                        .as_array()
                        .is_some_and(|required| required.iter().any(|value| value == field))
                );
            }
        }
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
    async fn rejects_zero_cap() {
        // A zero device quota is refused by the route, not silently stored.
        let (state, realm_id, admin, _tempdir) = setup_management_state().await;
        let auth = AuthContext {
            user_id: admin,
            realm_id,
            path_restrictions: None,
        };
        let mut body = RealmQuotaConfig::from(QuotaConfig::default());
        body.max_devices_per_user = Some(0);

        let error = set_realm_quota(State(state), Extension(Some(auth)), Json(body))
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ServerError::BadRequestReason(reason) if reason.contains("max_devices_per_user")
        ));
    }

    #[tokio::test]
    async fn rejects_zero_limits() {
        // Zero would silence an enrolled device; only null is uncapped.
        let (state, realm_id, admin, _tempdir) = setup_management_state().await;
        let auth = AuthContext {
            user_id: admin,
            realm_id,
            path_restrictions: None,
        };
        for body in [
            RealmQuotaConfig {
                device_requests_per_minute: Some(0),
                ..RealmQuotaConfig::from(QuotaConfig::default())
            },
            RealmQuotaConfig {
                device_concurrent_pulls: Some(0),
                ..RealmQuotaConfig::from(QuotaConfig::default())
            },
        ] {
            let error = set_realm_quota(
                State(state.clone()),
                Extension(Some(auth.clone())),
                Json(body),
            )
            .await
            .unwrap_err();

            assert!(matches!(
                error,
                ServerError::BadRequestReason(reason) if reason.starts_with("device_")
            ));
        }
    }

    #[test]
    fn stale_stays_configured() {
        // A bounded-stale snapshot may not report a remote peer as connected.
        let local = iroh::SecretKey::from_bytes(&[41u8; 32]).public();
        let remote = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
        let nodes = HashSet::from([remote]);

        let fresh = presence_nodes(RealmPresence::new(nodes.clone(), false), local);
        assert!(fresh.contains(&remote) && fresh.contains(&local));

        let stale = presence_nodes(RealmPresence::new(nodes, true), local);
        assert_eq!(stale, HashSet::from([local]));
    }

    #[tokio::test]
    async fn device_never_connected() {
        // Devices publish no presence, so a presence answer naming one, and
        // this node answering about itself, may still not connect it.
        let (state, realm_id, owner, _tempdir) = setup_management_state().await;
        let mut config = drive(
            aruna_operations::get_realm_config::GetRealmConfigOperation::new(realm_id),
            &state.get_ctx(),
        )
        .await
        .unwrap();
        let device = iroh::SecretKey::from_bytes(&[43u8; 32]).public();
        for node_id in [device, state.get_node_id()] {
            config.nodes.push(aruna_core::structs::RealmNode {
                node_id: node_id.to_string(),
                kind: aruna_core::structs::RealmNodeKind::User { owner },
            });
        }

        let present = HashSet::from([device, state.get_node_id()]);
        let nodes = map_realm_nodes(
            &state,
            &config,
            present,
            BTreeMap::new(),
            &PeerContacts::default(),
            10_000,
        );

        let devices: Vec<_> = nodes
            .iter()
            .filter(|node| node.kind == RealmNodeKindInfo::User)
            .collect();
        assert_eq!(devices.len(), 2);
        for node in devices {
            assert!(!node.present, "a device is never presence-confirmed");
            assert_ne!(node.connection_status, RealmNodeConnectionStatus::Connected);
        }
        let infra = nodes
            .iter()
            .find(|node| node.kind != RealmNodeKindInfo::User)
            .unwrap();
        assert!(infra.present);
        assert_eq!(
            infra.connection_status,
            RealmNodeConnectionStatus::Connected
        );
    }

    #[tokio::test]
    async fn reports_device_seen() {
        // A device is reported from this node's own contact record, and the
        // device's own node is serving the request, so it saw itself now.
        let (state, realm_id, owner, _tempdir) = setup_management_state().await;
        let mut config = drive(
            aruna_operations::get_realm_config::GetRealmConfigOperation::new(realm_id),
            &state.get_ctx(),
        )
        .await
        .unwrap();
        let recent = iroh::SecretKey::from_bytes(&[44u8; 32]).public();
        let stale = iroh::SecretKey::from_bytes(&[45u8; 32]).public();
        for node_id in [recent, stale, state.get_node_id()] {
            config.nodes.push(aruna_core::structs::RealmNode {
                node_id: node_id.to_string(),
                kind: aruna_core::structs::RealmNodeKind::User { owner },
            });
        }
        let now_ms = 1_000_000;
        let window = aruna_operations::metadata::PEER_CONTACT_WINDOW.as_millis() as u64;
        let contacts = PeerContacts::default();
        contacts.note(recent, now_ms - window);
        contacts.note(stale, now_ms - window - 1);

        let nodes = map_realm_nodes(
            &state,
            &config,
            HashSet::new(),
            BTreeMap::new(),
            &contacts,
            now_ms,
        );
        let device = |node_id: aruna_core::NodeId| {
            nodes
                .iter()
                .find(|node| {
                    node.node_id == node_id.to_string() && node.kind == RealmNodeKindInfo::User
                })
                .unwrap()
        };

        assert_eq!(
            device(recent).connection_status,
            RealmNodeConnectionStatus::Seen
        );
        assert_eq!(device(recent).last_seen_ms, Some(now_ms - window));
        assert_eq!(
            device(stale).connection_status,
            RealmNodeConnectionStatus::Unknown
        );
        assert_eq!(device(stale).last_seen_ms, Some(now_ms - window - 1));
        let current = device(state.get_node_id());
        assert_eq!(current.connection_status, RealmNodeConnectionStatus::Seen);
        assert_eq!(current.last_seen_ms, Some(now_ms));
        let infra = nodes
            .iter()
            .find(|node| node.kind != RealmNodeKindInfo::User)
            .unwrap();
        assert_eq!(infra.last_seen_ms, None, "only devices report contact");
    }
}
