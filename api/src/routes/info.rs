//! Serves the system routes for node and realm status, placement, quota and usage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{ensure_permission, permission_granted, require_realm_auth};
use crate::error::{ServerError, ServerResult};
pub use crate::server::state::PortalStatus;
use crate::server::state::ServerState;
use aruna_core::UserId;
use aruna_core::alpn::Alpn;
use aruna_core::errors::StorageError;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{GroupQuotaOverride, QuotaConfig, UserCapOverride};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::structs::placement::record::PlacementScope;
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::usage::{USAGE_GLOBAL_KEY, UsageCounters};
use aruna_core::structs::{ConnectionAddressStatus, PeerConnectionStatus, RequestSummaryState};
use aruna_core::time::unix_timestamp_millis;
use aruna_operations::device::realm_documents::installed_management_urls;
use aruna_operations::driver::{backend_used_bytes, drive};
use aruna_operations::metadata::PeerContacts;
use aruna_operations::metadata::stats::{count_realm_documents, count_realm_groups};
use aruna_operations::node::status::load_status;
use aruna_operations::node::usage_stats::{LoadCountersOperation, RealmUsageScope};
use aruna_operations::placement::allocate_handle::{
    HandleAllocationError, provision_metadata_binding,
};
use aruna_operations::placement::transition::transition_health;
use aruna_operations::realm::get_config::GetConfigOperation;
use aruna_operations::realm::get_nodes::{
    GetNodesOperation, REALM_DISCOVERY_TIMEOUT, RealmPresence,
};
use aruna_operations::realm::mutate_placement::{
    MutatePlacementConfig, MutatePlacementError, RealmPlacementMutation, drive_placement_mutation,
};
use aruna_operations::realm::set_quota::{SetQuotaConfig, SetQuotaError, SetQuotaOperation};
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
    tags(
        (name = "system/info", description = "Node information and usage"),
        (name = "system/realm", description = "Realm configuration, placement and quota")
    )
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

/// Node information. `node.status`, `node.realm_id` and `api_version` are public;
/// node identity, addresses, peer topology, backend detail and warnings need a realm
/// token or config admin, and gated values are absent, never restructured.
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

impl From<&aruna_core::structs::identity::auth::NodeCapabilities> for NodeCapabilityKind {
    fn from(capabilities: &aruna_core::structs::identity::auth::NodeCapabilities) -> Self {
        match capabilities {
            aruna_core::structs::identity::auth::NodeCapabilities::Management { .. } => {
                Self::Management
            }
            aruna_core::structs::identity::auth::NodeCapabilities::Server { .. } => Self::Server,
            aruna_core::structs::identity::auth::NodeCapabilities::User { .. } => Self::User,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mcp: Option<InterfaceStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InterfaceStatus {
    pub status: ServiceStatus,
    /// Local listen address. Realm-authenticated callers only.
    pub bind: Option<String>,
    pub url: Option<String>,
}

/// Realm information. Identity, description, oidc providers, public urls and the
/// count-only overview are public; topology, quota and listen addresses need a realm
/// token, and gated values are absent or empty rather than restructured.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RealmInfoResponse {
    pub realm_id: String,
    pub description: String,
    pub metadata_replication: RealmReplicationResponse,
    pub oidc_providers: Vec<RealmProviderResponse>,
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
    pub nodes: Vec<NodeInfoResponse>,
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
    #[serde(rename = "default_group_quota_bytes")]
    pub default_quota_bytes: Option<u64>,
    pub grace_factor_percent: u32,
    pub warn_threshold_percent: u32,
    pub group_overrides: Vec<RealmQuotaOverride>,
    #[serde(rename = "max_groups_per_user")]
    pub groups_per_user: Option<u32>,
    #[serde(rename = "user_group_cap_overrides")]
    pub group_cap_overrides: Vec<GroupCapOverride>,
    #[serde(rename = "max_devices_per_user")]
    pub devices_per_user: Option<u32>,
    #[serde(rename = "device_requests_per_minute")]
    pub device_request_rate: Option<u32>,
    pub device_concurrent_pulls: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmGroupQuotaOverride)]
pub struct RealmQuotaOverride {
    pub group_id: String,
    pub quota_bytes: Option<u64>,
    pub grace_factor_percent: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmUserGroupCapOverride)]
pub struct GroupCapOverride {
    pub user_id: String,
    pub max_groups: Option<u32>,
}

impl From<QuotaConfig> for RealmQuotaConfig {
    fn from(quota: QuotaConfig) -> Self {
        Self {
            default_quota_bytes: quota.default_quota_bytes,
            grace_factor_percent: quota.grace_factor_percent,
            warn_threshold_percent: quota.warn_threshold_percent,
            group_overrides: quota
                .group_overrides
                .into_iter()
                .map(|over| RealmQuotaOverride {
                    group_id: over.group_id.to_string(),
                    quota_bytes: over.quota_bytes,
                    grace_factor_percent: over.grace_factor_percent,
                })
                .collect(),
            groups_per_user: quota.groups_per_user,
            group_cap_overrides: quota
                .group_cap_overrides
                .into_iter()
                .map(|over| GroupCapOverride {
                    user_id: over.user_id.to_string(),
                    max_groups: over.max_groups,
                })
                .collect(),
            devices_per_user: quota.devices_per_user,
            device_request_rate: quota.device_request_rate,
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
        let group_cap_overrides = self
            .group_cap_overrides
            .into_iter()
            .map(|over| {
                Ok(UserCapOverride {
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
            default_quota_bytes: self.default_quota_bytes,
            grace_factor_percent: self.grace_factor_percent,
            warn_threshold_percent: self.warn_threshold_percent,
            group_overrides,
            groups_per_user: self.groups_per_user,
            group_cap_overrides,
            devices_per_user: self.devices_per_user,
            device_request_rate: self.device_request_rate,
            device_concurrent_pulls: self.device_concurrent_pulls,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmPlacementConfigResponse)]
pub struct RealmPlacementResponse {
    pub strategies: Vec<RealmPlacementStrategy>,
    pub default_strategy_id: Option<String>,
    #[serde(rename = "job_family_strategy_id")]
    pub family_strategy_id: String,
    pub bindings: Vec<RealmBinding>,
    pub overrides: Vec<RealmPlacementOverride>,
    pub transitions: RealmHealthResponse,
}

/// Health of the realm's in-flight placement transitions. Counts only: nothing
/// here changes where a request routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmTransitionHealthResponse)]
pub struct RealmHealthResponse {
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
    pub affinity: Vec<RealmAffinityRule>,
    pub shard_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmPlacementAffinityRule)]
pub struct RealmAffinityRule {
    pub key: String,
    pub value: String,
    pub effect: RealmAffinityEffect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[schema(as = RealmPlacementAffinityEffect)]
pub enum RealmAffinityEffect {
    Filter,
    Multiply { permille: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmPlacementBinding)]
pub struct RealmBinding {
    pub scope: RealmBindingScope,
    pub strategy_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[schema(as = RealmPlacementBindingScope)]
pub enum RealmBindingScope {
    Realm,
    Group { group_id: String },
    Class { document_class: RealmPlacementClass },
    MetadataPathPrefix { prefix: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = RealmPlacementDocumentClass)]
pub enum RealmPlacementClass {
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
#[schema(as = RealmPlacementMutationRequest)]
pub enum RealmPlacementRequest {
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
        binding: RealmBinding,
    },
    RemoveBinding {
        scope: RealmBindingScope,
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
    SetNodeAttributes {
        node_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        location: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        labels: Option<Vec<RealmPlacementLabel>>,
    },
}

/// One node placement label. Absent from a request means unchanged; a present
/// list replaces the whole set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmPlacementNodeLabel)]
pub struct RealmPlacementLabel {
    pub key: String,
    pub value: String,
}

enum RealmPlacementAction {
    Mutation(RealmPlacementMutation),
    Provision {
        strategy_id: Ulid,
        group_id: Option<Ulid>,
    },
}

impl RealmPlacementResponse {
    fn from_document(document: &RealmConfigDocument) -> Self {
        let health = transition_health(document, unix_timestamp_millis());
        Self {
            transitions: RealmHealthResponse {
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
            family_strategy_id: document.family_strategy_id.to_string(),
            bindings: document
                .strategy_bindings
                .iter()
                .map(RealmBinding::from)
                .collect(),
            overrides: document
                .placement_overrides
                .iter()
                .map(RealmPlacementOverride::from)
                .collect(),
        }
    }
}

impl From<&aruna_core::structs::placement::record::PlacementStrategy> for RealmPlacementStrategy {
    fn from(strategy: &aruna_core::structs::placement::record::PlacementStrategy) -> Self {
        Self {
            strategy_id: strategy.strategy_id.to_string(),
            name: strategy.name.clone(),
            replica_count: strategy.replica_count,
            distinct_locations: strategy.distinct_locations,
            affinity: strategy
                .affinity
                .iter()
                .map(|rule| RealmAffinityRule {
                    key: rule.matcher.key.clone(),
                    value: rule.matcher.value.clone(),
                    effect: match rule.effect {
                        aruna_core::structs::placement::record::AffinityEffect::Filter => {
                            RealmAffinityEffect::Filter
                        }
                        aruna_core::structs::placement::record::AffinityEffect::Multiply {
                            permille,
                        } => RealmAffinityEffect::Multiply { permille },
                    },
                })
                .collect(),
            shard_count: strategy.shard_count,
        }
    }
}

impl RealmPlacementStrategy {
    fn into_core(self) -> ServerResult<aruna_core::structs::placement::record::PlacementStrategy> {
        Ok(aruna_core::structs::placement::record::PlacementStrategy {
            strategy_id: parse_ulid(&self.strategy_id, "strategy_id")?,
            name: self.name,
            replica_count: self.replica_count,
            distinct_locations: self.distinct_locations,
            affinity: self
                .affinity
                .into_iter()
                .map(
                    |rule| aruna_core::structs::placement::record::AffinityRule {
                        matcher: aruna_core::structs::placement::record::LabelMatch {
                            key: rule.key,
                            value: rule.value,
                        },
                        effect: match rule.effect {
                            RealmAffinityEffect::Filter => {
                                aruna_core::structs::placement::record::AffinityEffect::Filter
                            }
                            RealmAffinityEffect::Multiply { permille } => {
                                aruna_core::structs::placement::record::AffinityEffect::Multiply {
                                    permille,
                                }
                            }
                        },
                    },
                )
                .collect(),
            shard_count: self.shard_count,
        })
    }
}

impl From<&aruna_core::structs::placement::record::StrategyBinding> for RealmBinding {
    fn from(binding: &aruna_core::structs::placement::record::StrategyBinding) -> Self {
        Self {
            scope: RealmBindingScope::from(&binding.scope),
            strategy_id: binding.strategy_id.to_string(),
        }
    }
}

impl RealmBinding {
    fn into_core(self) -> ServerResult<aruna_core::structs::placement::record::StrategyBinding> {
        Ok(aruna_core::structs::placement::record::StrategyBinding {
            scope: self.scope.into_core()?,
            strategy_id: parse_ulid(&self.strategy_id, "strategy_id")?,
        })
    }
}

impl From<&aruna_core::structs::placement::record::BindingScope> for RealmBindingScope {
    fn from(scope: &aruna_core::structs::placement::record::BindingScope) -> Self {
        match scope {
            aruna_core::structs::placement::record::BindingScope::Realm => Self::Realm,
            aruna_core::structs::placement::record::BindingScope::Group(group_id) => Self::Group {
                group_id: group_id.to_string(),
            },
            aruna_core::structs::placement::record::BindingScope::Class(document_class) => {
                Self::Class {
                    document_class: RealmPlacementClass::from(*document_class),
                }
            }
            aruna_core::structs::placement::record::BindingScope::MetadataPathPrefix(prefix) => {
                Self::MetadataPathPrefix {
                    prefix: prefix.clone(),
                }
            }
        }
    }
}

impl RealmBindingScope {
    fn into_core(self) -> ServerResult<aruna_core::structs::placement::record::BindingScope> {
        Ok(match self {
            Self::Realm => aruna_core::structs::placement::record::BindingScope::Realm,
            Self::Group { group_id } => {
                aruna_core::structs::placement::record::BindingScope::Group(parse_ulid(
                    &group_id, "group_id",
                )?)
            }
            Self::Class { document_class } => {
                aruna_core::structs::placement::record::BindingScope::Class(document_class.into())
            }
            Self::MetadataPathPrefix { prefix } => {
                aruna_core::structs::placement::record::BindingScope::MetadataPathPrefix(prefix)
            }
        })
    }
}

impl From<aruna_core::structs::placement::record::DocumentClass> for RealmPlacementClass {
    fn from(document_class: aruna_core::structs::placement::record::DocumentClass) -> Self {
        match document_class {
            aruna_core::structs::placement::record::DocumentClass::Admin => Self::Admin,
            aruna_core::structs::placement::record::DocumentClass::Group => Self::Group,
            aruna_core::structs::placement::record::DocumentClass::User => Self::User,
            aruna_core::structs::placement::record::DocumentClass::Metadata => Self::Metadata,
            aruna_core::structs::placement::record::DocumentClass::MetadataRegistry => {
                Self::MetadataRegistry
            }
            aruna_core::structs::placement::record::DocumentClass::JobControl => Self::JobControl,
            aruna_core::structs::placement::record::DocumentClass::PlacementPolicy => {
                Self::PlacementPolicy
            }
        }
    }
}

impl From<RealmPlacementClass> for aruna_core::structs::placement::record::DocumentClass {
    fn from(document_class: RealmPlacementClass) -> Self {
        match document_class {
            RealmPlacementClass::Admin => Self::Admin,
            RealmPlacementClass::Group => Self::Group,
            RealmPlacementClass::User => Self::User,
            RealmPlacementClass::Metadata => Self::Metadata,
            RealmPlacementClass::MetadataRegistry => Self::MetadataRegistry,
            RealmPlacementClass::JobControl => Self::JobControl,
            RealmPlacementClass::PlacementPolicy => Self::PlacementPolicy,
        }
    }
}

impl From<&aruna_core::structs::placement::record::PlacementOverride> for RealmPlacementOverride {
    fn from(record: &aruna_core::structs::placement::record::PlacementOverride) -> Self {
        Self {
            subject: hex::encode(&record.subject),
            pinned: record.pinned.iter().map(ToString::to_string).collect(),
            excluded: record.excluded.iter().map(ToString::to_string).collect(),
            strategy_id: record.strategy_id.map(|id| id.to_string()),
        }
    }
}

impl RealmPlacementOverride {
    fn into_core(self) -> ServerResult<aruna_core::structs::placement::record::PlacementOverride> {
        Ok(aruna_core::structs::placement::record::PlacementOverride {
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

impl RealmPlacementRequest {
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
            Self::SetNodeAttributes {
                node_id,
                location,
                labels,
            } => RealmPlacementMutation::SetNodeAttributes {
                node_id: parse_node_id(&node_id)?,
                location,
                labels: labels.map(|labels| {
                    labels
                        .into_iter()
                        .map(|label| (label.key, label.value))
                        .collect()
                }),
            },
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

fn parse_node_id(value: &str) -> ServerResult<aruna_core::NodeId> {
    value
        .parse::<aruna_core::NodeId>()
        .map_err(|_| ServerError::BadRequestReason(format!("invalid node_id: {value}")))
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
#[schema(as = RealmMetadataReplicationResponse)]
pub struct RealmReplicationResponse {
    pub default_replication_factor: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmOidcProviderResponse)]
pub struct RealmProviderResponse {
    pub id: String,
    pub issuer: String,
    pub audience: String,
    pub discovery_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmNodeInfoResponse)]
pub struct NodeInfoResponse {
    pub node_id: String,
    pub kind: NodeKindInfo,
    /// Owner of a `user` node; null for infrastructure nodes.
    pub owner: Option<String>,
    pub configured: bool,
    pub present: bool,
    pub connection_status: RealmConnectionStatus,
    /// When a `user` node last reached this node, in unix milliseconds. This
    /// node's own observation; absent for other kinds and for a device it has
    /// not seen.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_ms: Option<u64>,
    /// Placement map entry (location/weight/status) when the node is mapped.
    pub placement: Option<NodePlacementResponse>,
    /// Latest published node info document (capabilities/labels/urls/utilization) if received.
    pub info: Option<NodeDocumentResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmNodePlacementResponse)]
pub struct NodePlacementResponse {
    pub location: String,
    pub weight: u32,
    pub full: bool,
    pub draining: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmNodeInfoDocumentResponse)]
pub struct NodeDocumentResponse {
    pub executors: Vec<ExecutorCapabilityResponse>,
    pub labels: std::collections::BTreeMap<String, String>,
    pub urls: RealmUrlsResponse,
    pub utilization: RealmUtilizationResponse,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ExecutorCapabilityResponse {
    pub kind: String,
    pub file_staging: bool,
    pub direct_s3: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmNodeUrlsResponse)]
pub struct RealmUrlsResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s3: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmNodeUtilizationResponse)]
pub struct RealmUtilizationResponse {
    pub storage_bytes_used: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub documents_held: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_permille: Option<u32>,
    pub heartbeat_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = RealmNodeKindInfo)]
pub enum NodeKindInfo {
    Management,
    Server,
    User,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = RealmNodeConnectionStatus)]
pub enum RealmConnectionStatus {
    Connected,
    Configured,
    /// A device that reached this node recently. Not a connection: it is what
    /// this node itself saw, and only this node saw it.
    Seen,
    /// Presence does not describe this node: a device publishes none.
    Unknown,
}

impl From<&RealmNodeKind> for NodeKindInfo {
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
    path = "/system/realm",
    tag = "system/realm",
    summary = "Read the realm's public settings and node topology",
    description = r#"Returns the realm's public settings, and its discovery, quota and node topology to a realm member.

**Authentication**: optional bearer token; without a usable token of this realm, and that includes a
token of another realm or one this node cannot validate, the response is the public part only.

**Behavior**
- The public part is the realm id, description, metadata replication policy, the OIDC providers a
  client needs to obtain a token, the public interface urls, `public_overview`, and where the
  realm's management nodes are: `is_management_node` says whether this node is one, and
  `management_urls` lists the api base urls the management nodes published, this node's own first.
  Only a management node mints enrollments and issues the realm's credentials, so a device or a
  client that reached any other node follows one of these urls instead.
- `public_overview` carries three nullable aggregates: `live_datasets`, the realm's lifecycle-live
  metadata registry count, never caller-filtered or replica-multiplied; `groups`, the realm's
  stored group count; and `nodes_configured`, the membership count in this node's replicated realm
  configuration, never DHT presence or health. A null value means this node could not answer and is
  never encoded as zero.
- A bearer token of this realm additionally reveals the realm's discovery configuration, its quota
  policy, the node list and the interface listen addresses. Gated values are absent or empty, never
  restructured, so one parser handles both.
- The node list is the realm's configured membership read from this node's replicated realm
  configuration; `placement` is the node's entry in the placement map and `info` is the last node
  information document that reached this node, so both may lag or be absent, as may
  `management_urls`.
- Liveness is a separate, deliberately conservative signal: presence is resolved through a bounded
  realm lookup with a four second budget, and if that lookup is stale, times out or fails, only this
  node counts as present. `present` true with `connection_status` `connected` means the peer was
  confirmed live just now; `configured` means no fresh confirmation, which is not evidence that the
  peer is down.
- A `user` node is a device and publishes no presence at all, so `present` is always false. That is
  the absence of a signal, never a report that the device is down. A device is instead reported
  from what this node itself saw: `last_seen_ms` is when it last reached this node over an
  authorized request, and `connection_status` `seen` means within the last three minutes, otherwise
  `unknown`. Both are this node's own observation, never realm state, and start empty after a
  restart."#,
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
    Ok((StatusCode::OK, Json(run_realm_info(&state, auth).await?)))
}

pub(crate) async fn run_realm_info(
    state: &ServerState,
    auth: Option<AuthContext>,
) -> ServerResult<RealmInfoResponse> {
    let config = drive(
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        aruna_operations::realm::get_config::GetConfigError::DocumentNotFound => {
            ServerError::NotFound
        }
        other => ServerError::InternalError(other.to_string()),
    })?;
    let realm_authenticated = auth.is_some_and(|auth| auth.realm_id == state.get_realm_id());

    let mut interfaces = interface_services_status(state).await;
    if !realm_authenticated {
        interfaces.rest.bind = None;
        interfaces.s3.bind = None;
        if let Some(mcp) = interfaces.mcp.as_mut() {
            mcp.bind = None;
        }
    }

    let metadata_replication = RealmReplicationResponse {
        default_replication_factor: config.effective_replication_factor(),
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

    let node_info_docs = load_node_documents(state, &config).await;
    let mut management_urls = management_urls(
        state,
        &config,
        &node_info_docs,
        interfaces.rest.url.as_deref(),
    );
    // A device holds no peer node-info document, so the list a realm node
    // installed on it is what the portal is offered there.
    if management_urls.is_empty() {
        management_urls = installed_management_urls(&state.get_ctx(), config.realm_id).await;
    }

    let (discovery, nodes, quota) = if realm_authenticated {
        let present_nodes = load_realm_presence(state).await;
        let discovery = serde_json::to_value(&config.discovery)
            .map_err(|error| ServerError::InternalError(error.to_string()))?;
        let contacts = state.peer_contacts();
        let nodes = map_realm_nodes(
            state,
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

    Ok(RealmInfoResponse {
        realm_id: config.realm_id.to_string(),
        description: config.description,
        metadata_replication,
        oidc_providers: config
            .oidc_providers
            .into_iter()
            .map(|provider| RealmProviderResponse {
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
    })
}

/// Every management node of the realm with the api url it published, in
/// realm-config order. The caller decides where its own node belongs.
pub(crate) fn management_node_urls(
    config: &RealmConfigDocument,
    node_info_docs: &BTreeMap<
        aruna_core::NodeId,
        aruna_core::structs::storage::node_info::NodeInfoDocument,
    >,
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
    node_info_docs: &BTreeMap<
        aruna_core::NodeId,
        aruna_core::structs::storage::node_info::NodeInfoDocument,
    >,
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

pub(crate) async fn load_node_documents(
    state: &ServerState,
    config: &RealmConfigDocument,
) -> BTreeMap<aruna_core::NodeId, aruna_core::structs::storage::node_info::NodeInfoDocument> {
    let node_ids: Vec<aruna_core::NodeId> = config
        .nodes
        .iter()
        .filter_map(|node| node.node_id.parse().ok())
        .collect();
    match aruna_operations::node::node_info::read_info_documents(&state.get_ctx(), &node_ids).await
    {
        Ok(documents) => documents,
        Err(error) => {
            warn!(error = %error, "Failed to load node info documents for realm info");
            BTreeMap::new()
        }
    }
}

fn map_node_document(
    document: &aruna_core::structs::storage::node_info::NodeInfoDocument,
) -> NodeDocumentResponse {
    NodeDocumentResponse {
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
        urls: RealmUrlsResponse {
            api: document.urls.api.clone(),
            s3: document.urls.s3.clone(),
        },
        utilization: RealmUtilizationResponse {
            storage_bytes_used: document.utilization.storage_bytes_used,
            documents_held: document.utilization.documents_held,
            load_permille: document.utilization.load_permille,
            heartbeat_at_ms: document.utilization.heartbeat_at_ms,
        },
        updated_at_ms: document.updated_at_ms,
    }
}

async fn is_realm_admin(state: &ServerState, auth: &AuthContext) -> ServerResult<bool> {
    let realm_id = state.get_realm_id();
    permission_granted(
        state,
        auth,
        format!("/{realm_id}/admin/config"),
        Permission::WRITE,
    )
    .await
}

async fn require_realm_admin(
    state: &Arc<ServerState>,
    auth: Option<AuthContext>,
) -> ServerResult<AuthContext> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    if auth.realm_id != state.get_realm_id() || !state.is_management_node() {
        return Err(ServerError::Forbidden);
    }
    if !is_realm_admin(state, &auth).await? {
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
    match is_realm_admin(state, auth).await {
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
    path = "/system/realm/placement",
    tag = "system/realm",
    summary = "Read record placement strategies, bindings and overrides",
    description = r#"Returns the record placement strategies as stored in this node's copy of the realm configuration.

**Authentication**: realm bearer token with WRITE on the realm configuration admin path. A
management node serves the call and every other node relays it to one.

**Behavior**
- Carries the defined strategies with their replica count, distinctness requirement, affinity rules
  and shard count; the default strategy; the immutable job-family strategy id; the bindings that
  map a scope (the realm, a group, a document class or a metadata path prefix) to a strategy; and
  the per-subject overrides that pin or exclude individual nodes.
- Strategies place metadata records and job families. They never decide where S3 object bytes may
  live; that is what the placement policy documents under `/data/placement/policies` do.
- This is a rule set, not a placement result: it says how replicas are chosen, not where any
  particular document currently sits.

**Limits**
- The strategy named by `job_family_strategy_id` cannot be removed or have its shard count
  reshaped, and a strategy a binding or override still references cannot be removed."#,
    responses(
        (
            status = 200,
            description = "The realm's record placement strategies as this management node has them",
            body = RealmPlacementResponse,
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
        (status = 401, description = "Missing or invalid bearer token", body = crate::error::ErrorResponse),
        (status = 403, description = "Caller is not a realm config admin", body = crate::error::ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = crate::error::ErrorResponse),
        (status = 500, description = "The stored realm configuration could not be read or decoded here", body = crate::error::ErrorResponse),
        (status = 503, description = "Called on a node that is not a management node and no management node was reachable; code `no_management_node`", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_realm_placement(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<RealmPlacementResponse>)> {
    require_realm_admin(&state, auth).await?;
    let document = drive(
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        aruna_operations::realm::get_config::GetConfigError::DocumentNotFound => {
            ServerError::NotFound
        }
        other => ServerError::InternalError(other.to_string()),
    })?;
    Ok((
        StatusCode::OK,
        Json(RealmPlacementResponse::from_document(&document)),
    ))
}

#[utoipa::path(
    patch,
    path = "/system/realm/placement",
    tag = "system/realm",
    summary = "Apply one change to the record placement strategies",
    description = r#"Applies exactly one change to the realm's record placement strategies and returns all of them.

**Authentication**: realm bearer token with WRITE on the realm configuration admin path. A
management node serves the call and every other node relays it to one.

**Behavior**
- The body carries exactly one change, selected by its `mutation` field: define or replace a
  strategy, remove one, set the default, set or remove a binding for a scope, set or remove a
  per-subject override, edit a node's placement attributes, or provision a metadata binding for a
  strategy.
- `set_node_attributes` edits the location and labels of a node the realm already places. An
  omitted field keeps its stored value and a present `labels` list replaces the whole set. A real
  change advances that node's storage subject, which makes it revalidate its registered copies and
  quarantine the ones its new attributes no longer admit, and it serves no governed data until that
  walk finishes. Submitting the values already stored changes nothing.
- Provisioning is idempotent: an existing binding for the same scope and strategy is returned
  unchanged instead of allocating a second one.
- The whole strategy set after the change is returned, so a client never has to re-read to learn
  the new state.
- The change is written to the replicated realm configuration: it is durable here when the response
  is sent and reaches the other realm nodes asynchronously. It moves no data by itself; existing
  replicas are relocated by later placement work.

**Limits**
- The strategy named by `job_family_strategy_id` cannot be removed or have its shard count
  reshaped; those mutations fail with `JobFamilyImmutable`.
- `set_node_attributes` refuses an unknown node, a location longer than 64 bytes, a derived label
  key (`aruna-engine.org/kind`, `.../location`, `.../node`, `.../storage-class/*`), and any edit
  while that node is draining."#,
    request_body(
        content = RealmPlacementRequest,
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
                summary = "Place every metadata document of one group with that strategy",
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
            )),
            ("Edit node attributes" = (
                summary = "Move a node to another location and replace its labels; the node revalidates its copies",
                value = json!({
                    "mutation": "set_node_attributes",
                    "node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                    "location": "eu-west",
                    "labels": [{"key": "tier", "value": "hot"}]
                })
            ))
        )
    ),
    responses(
        (
            status = 200,
            description = "The complete strategy set after the change was applied",
            body = RealmPlacementResponse,
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
        (status = 400, description = "Malformed body, an id that is not a ULID, a node id or subject that does not decode, an unknown strategy, a node the realm does not place, a location or label the placement map refuses, or a change the realm configuration rejects as invalid", body = crate::error::ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = crate::error::ErrorResponse),
        (status = 403, description = "Caller is not a realm config admin", body = crate::error::ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = crate::error::ErrorResponse),
        (status = 409, description = "The strategy is still referenced by a binding or override (`StrategyReferenced`), so removing it would leave a dangling reference, the placement handle space is exhausted, or another update of the realm configuration won the race; the caller may retry", body = crate::error::ErrorResponse),
        (status = 500, description = "The realm configuration could not be read or written here", body = crate::error::ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = crate::error::ErrorResponse),
        (status = 503, description = "Storage cleanup capacity exhausted, or no management node was reachable to serve the relayed call; code `no_management_node`", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn mutate_realm_placement(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    request: Result<Json<RealmPlacementRequest>, JsonRejection>,
) -> ServerResult<(StatusCode, Json<RealmPlacementResponse>)> {
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
            drive_placement_mutation(
                MutatePlacementConfig { actor, mutation },
                Some(auth),
                &context,
            )
            .await
            .map_err(map_placement_error)?
        }
        RealmPlacementAction::Provision {
            strategy_id,
            group_id,
        } => {
            // Handle allocation carries no authorization of its own.
            require_realm_admin(&state, Some(auth)).await?;
            let scope = group_id
                .map(PlacementScope::Group)
                .unwrap_or(PlacementScope::Realm(actor.realm_id));
            provision_metadata_binding(context.as_ref(), actor.clone(), scope, strategy_id)
                .await
                .map_err(map_handle_error)?;
            drive(GetConfigOperation::new(actor.realm_id), &context)
                .await
                .map_err(|error| match error {
                    aruna_operations::realm::get_config::GetConfigError::DocumentNotFound => {
                        ServerError::NotFound
                    }
                    other => ServerError::InternalError(other.to_string()),
                })?
        }
    };
    Ok((
        StatusCode::OK,
        Json(RealmPlacementResponse::from_document(&document)),
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
        HandleAllocationError::Append(error) => map_placement_error(error),
        HandleAllocationError::ReadConfig(
            aruna_operations::realm::get_config::GetConfigError::DocumentNotFound,
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

fn map_placement_error(error: MutatePlacementError) -> ServerError {
    match error {
        MutatePlacementError::ConfigMissing => ServerError::NotFound,
        MutatePlacementError::InvalidInput(reason) => ServerError::BadRequestReason(reason),
        error @ (MutatePlacementError::AdminDocumentError(_)
        | MutatePlacementError::EmptyShardHolders { .. }
        | MutatePlacementError::UnknownTransition { .. }
        | MutatePlacementError::ForceWithoutProof { .. }) => {
            ServerError::BadRequestReason(error.to_string())
        }
        MutatePlacementError::Unauthorized { .. } => ServerError::Forbidden,
        MutatePlacementError::StrategyReferenced { strategy_id } => ServerError::Conflict(format!(
            "placement strategy {strategy_id} is currently referenced"
        )),
        MutatePlacementError::JobFamilyImmutable { strategy_id } => ServerError::Conflict(format!(
            "placement strategy {strategy_id} is the immutable job-family strategy"
        )),
        error @ MutatePlacementError::TransitionInFlight { .. } => {
            ServerError::Conflict(error.to_string())
        }
        MutatePlacementError::StorageError(StorageError::TransactionConflict) => {
            ServerError::Conflict("concurrent realm placement update conflict; retry".to_string())
        }
        MutatePlacementError::StorageError(StorageError::CleanupCapacity) => {
            ServerError::ServiceUnavailableReason(
                "storage cleanup capacity exhausted; retry".to_string(),
            )
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

#[utoipa::path(
    put,
    path = "/system/realm/quota",
    tag = "system/realm",
    summary = "Replace the realm-wide quota policy",
    description = r#"Replaces the realm-wide quota policy wholesale and echoes back the stored result.

**Authentication**: realm bearer token with WRITE on the realm configuration admin path. A
management node serves the call and every other node relays it to one.

**Behavior**
- This replaces the stored policy rather than patching it: overrides absent from the body are
  dropped, so send the complete intended policy.
- `default_group_quota_bytes` is the pre-grace allowance per group and null means unlimited;
  `grace_factor_percent` scales it into the hard ceiling a write is refused at, while
  `warn_threshold_percent` only decides when a group is reported as warning.
- Per-group overrides replace both values for one group, and per-user overrides cap how many groups
  a user may hold.
- `max_devices_per_user` caps how many devices one user may enroll, counting unclaimed enrollment
  secrets as well as enrolled devices; null leaves device enrollment uncapped.
- `device_requests_per_minute` and `device_concurrent_pulls` bound what one enrolled device may ask
  of each realm node: how many requests it may send per minute and how many it may keep in flight,
  which is what bounds long blob pulls. Both are per device and per node, and null leaves that
  dimension uncapped.
- Device limits are enforced where a device's requests arrive, at the node's inbound network
  admission, not at REST: an over-budget request is dropped there, so the device sees a transport
  failure and reports the retryable 503 class to its owner rather than a 429.
- Quota is evaluated against a group's realm-wide logical bytes, which are aggregated from counters
  that replicate between nodes, so enforcement follows a policy change as those counters and the
  realm configuration propagate.

**Limits**
- `grace_factor_percent` must be at least 100 and `warn_threshold_percent` between 1 and 100.
- `max_devices_per_user`, `device_requests_per_minute` and `device_concurrent_pulls` must be null or
  greater than zero; zero is refused rather than read as a ban on enrollment or a silenced device."#,
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
        (status = 401, description = "Missing or invalid bearer token", body = crate::error::ErrorResponse),
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
        SetQuotaOperation::new(SetQuotaConfig {
            actor,
            auth_context: auth,
            quota,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_quota_error)?;
    Ok((StatusCode::OK, Json(RealmQuotaConfig::from(stored.quota))))
}

fn map_quota_error(error: SetQuotaError) -> ServerError {
    match error {
        SetQuotaError::ConfigMissing => ServerError::NotFound,
        SetQuotaError::Unauthorized | SetQuotaError::NotManagementNode => ServerError::Forbidden,
        SetQuotaError::InvalidQuota { reason } => ServerError::BadRequestReason(reason),
        SetQuotaError::StorageError(StorageError::TransactionConflict) => {
            ServerError::Conflict("concurrent realm quota update conflict; retry".to_string())
        }
        SetQuotaError::StorageError(StorageError::CleanupCapacity) => {
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
    /// Physical blob copies this node holds. Copies are content-addressed and
    /// shared between the groups referencing them, so they are attributed to the
    /// node, never to a group: the group usage endpoint omits this field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stored_blobs: Option<u64>,
    /// Bytes those copies occupy. Omitted wherever `stored_blobs` is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stored_bytes: Option<u64>,
    pub logical_bytes: u64,
    pub referenced_bytes: u64,
    pub realm: UsageTotals,
    /// Realm-wide live metadata document count, excluding lifecycle-deleted ones.
    /// Not filtered per caller and absent (never zero) on the group usage endpoint
    /// and on nodes without a metadata subsystem.
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
    /// `logical_bytes`, the same counter the put-object `QuotaGate` enforces.
    pub fn resolve(
        quota: &QuotaConfig,
        group_id: &aruna_core::types::GroupId,
        group_logical_bytes: u64,
    ) -> Self {
        let quota_bytes = quota.group_quota_bytes(group_id);
        let warning = match quota_bytes {
            Some(limit) => {
                u128::from(group_logical_bytes) * 100
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stored_blobs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stored_bytes: Option<u64>,
    pub logical_bytes: u64,
    pub referenced_bytes: u64,
}

impl From<UsageCounters> for UsageTotals {
    fn from(counters: UsageCounters) -> Self {
        Self {
            buckets: counters.buckets,
            objects: counters.objects,
            stored_blobs: Some(counters.stored_blobs),
            stored_bytes: Some(counters.stored_bytes),
            logical_bytes: counters.logical_bytes,
            referenced_bytes: counters.referenced_bytes,
        }
    }
}

impl UsageTotals {
    fn without_stored(counters: UsageCounters) -> Self {
        Self {
            stored_blobs: None,
            stored_bytes: None,
            ..Self::from(counters)
        }
    }
}

impl UsageResponse {
    pub fn new(local: UsageCounters, realm: UsageCounters) -> Self {
        Self {
            buckets: local.buckets,
            objects: local.objects,
            stored_blobs: Some(local.stored_blobs),
            stored_bytes: Some(local.stored_bytes),
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

    /// Group scope. Physical copies are content-addressed and shared between
    /// groups, so the counters carry no group dimension and the `stored_*`
    /// fields are omitted instead of reported as an unattributed zero.
    pub fn for_group(local: UsageCounters, realm: UsageCounters) -> Self {
        Self {
            stored_blobs: None,
            stored_bytes: None,
            realm: UsageTotals::without_stored(realm),
            ..Self::new(local, realm)
        }
    }
}

pub async fn load_usage_counters(state: &ServerState, key: Vec<u8>) -> ServerResult<UsageCounters> {
    drive(LoadCountersOperation::new(key), &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))
}

pub async fn load_realm_usage(
    state: &ServerState,
    scope: RealmUsageScope,
) -> ServerResult<UsageCounters> {
    aruna_operations::node::usage_stats::load_realm_usage(
        &state.get_ctx(),
        state.get_node_id(),
        scope,
    )
    .await
    .map_err(ServerError::InternalError)
}

#[utoipa::path(
    get,
    path = "/system/usage",
    tag = "system/info",
    summary = "Report this node's and the realm's storage usage",
    description = r#"Reports this node's own storage counters together with the realm-wide totals.

**Authentication**: realm bearer token. No further permission is checked, because the figures are
realm-wide totals and not per-caller views.

**Behavior**
- The flat fields are this node's own counters, while `realm` is the total summed from every realm
  node's replicated usage snapshot, so it is eventually consistent: a node whose snapshot has not
  arrived or has not refreshed yet is simply not part of the sum, which can make the total lag
  reality after a burst of writes.
- `stored_blobs` and `stored_bytes` count physical, content-addressed blob copies. A copy is shared
  by every group referencing it, so it is attributed to this node and its backend, never to a group;
  the group usage endpoint therefore omits both fields.
- `metadata_documents` counts the realm's live metadata documents, excluding lifecycle-deleted ones,
  and is deliberately unfiltered by what the caller may read. It is omitted, never zeroed, when this
  node has no metadata subsystem or the count cannot be produced, so an absent field means unknown.
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
        (status = 401, description = "Missing or invalid bearer token", body = crate::error::ErrorResponse),
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
    node_info_docs: BTreeMap<
        aruna_core::NodeId,
        aruna_core::structs::storage::node_info::NodeInfoDocument,
    >,
    contacts: &PeerContacts,
    now_ms: u64,
) -> Vec<NodeInfoResponse> {
    let current_node = state.get_node_id();
    config
        .nodes
        .iter()
        .map(|node| {
            let parsed = node.node_id.parse::<aruna_core::NodeId>().ok();
            let is_current = node.node_id == current_node.to_string();
            let kind = NodeKindInfo::from(&node.kind);
            // A device publishes no realm presence, so presence carries no
            // statement about it, not even on the device's own node.
            let is_device = matches!(kind, NodeKindInfo::User);
            let present = !is_device
                && (is_current || parsed.is_some_and(|node_id| present_nodes.contains(&node_id)));
            let placement = parsed
                .and_then(|node_id| config.placement_entry(node_id))
                .map(|entry| NodePlacementResponse {
                    location: entry.effective_location().to_string(),
                    weight: entry.weight,
                    full: entry.full,
                    draining: entry.draining,
                });
            let info = parsed
                .and_then(|node_id| node_info_docs.get(&node_id))
                .map(map_node_document);
            // A device answering for itself is in contact by definition.
            let last_seen_ms = match (is_device, is_current) {
                (true, true) => Some(now_ms),
                (true, false) => parsed.and_then(|node_id| contacts.last_seen(&node_id)),
                (false, _) => None,
            };
            let seen_recently = is_device
                && (is_current
                    || parsed.is_some_and(|node_id| contacts.seen_recently(&node_id, now_ms)));
            NodeInfoResponse {
                node_id: node.node_id.clone(),
                kind,
                owner: node.kind.owner().map(|owner| owner.to_string()),
                configured: true,
                present,
                connection_status: match (is_device, seen_recently, present) {
                    (true, true, _) => RealmConnectionStatus::Seen,
                    (true, false, _) => RealmConnectionStatus::Unknown,
                    (false, _, true) => RealmConnectionStatus::Connected,
                    (false, _, false) => RealmConnectionStatus::Configured,
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

async fn load_realm_presence(state: &ServerState) -> HashSet<aruna_core::NodeId> {
    // A realm with offline nodes must degrade to local-only presence rather
    // than stall the dashboard.
    let discovery = tokio::time::timeout(
        REALM_DISCOVERY_TIMEOUT,
        drive(
            GetNodesOperation::new(state.get_realm_id()),
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
        mcp: interface_runtime.mcp.map(|mcp| InterfaceStatus {
            status: ServiceStatus::Available,
            bind: Some(mcp.bind_address.to_string()),
            url: Some(mcp.url),
        }),
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
    path = "/system/info",
    tag = "system/info",
    summary = "Report this node's health, version and service status",
    description = r#"Health-checks this single node, answered locally and never routed to a peer.

**Authentication**: optional bearer token; every caller is answered, but the amount of detail
depends on the token, and a token of another realm or one that cannot be validated here counts as
anonymous.

**Behavior**
- The route always returns 200 for a node that is serving requests at all.
- An anonymous caller sees only what health-checks the node and says where to authenticate: node
  status, realm id, api version and the public interface urls.
- A bearer token of this realm adds the node's own identity and capability kind, its listen
  addresses, its peer connections and the network service summary.
- A token with WRITE on the realm configuration admin path additionally reveals the blob and
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
    (StatusCode::OK, Json(run_node_info(&state, auth).await))
}

pub(crate) async fn run_node_info(state: &ServerState, auth: Option<AuthContext>) -> InfoResponse {
    let access = info_access(state, auth.as_ref()).await;
    let realm = access != InfoAccess::Public;
    let admin = access == InfoAccess::Admin;

    let mut interfaces = interface_services_status(state).await;
    if !realm {
        interfaces.rest.bind = None;
        interfaces.s3.bind = None;
        if let Some(mcp) = interfaces.mcp.as_mut() {
            mcp.bind = None;
        }
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
        return response;
    }

    let ctx = state.get_ctx();
    let snapshot = load_status(ctx.as_ref()).await;

    let (network, warnings) = match snapshot.network {
        Some(info) => {
            response.my_addresses = info
                .endpoint_addr
                .addrs
                .iter()
                .map(format_transport_addr)
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
        let blob = match snapshot.blob {
            Some(info) => BlobServiceStatus {
                status: ServiceStatus::from(info.status),
                backend: Some(info.backend_type.to_string()),
                max_bucket_size: info.max_bucket_size,
                multipart_bucket: info.multipart_bucket,
                timeouts_secs: Some(TimeoutConfigSecs {
                    connect: info.timeouts.control_connect_timeout.as_secs(),
                    io: info.timeouts.control_io_timeout.as_secs(),
                    transfer_idle: info.timeouts.transfer_idle_timeout.as_secs(),
                }),
                backends: backend_statuses(state, info.backends).await,
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
            status: ServiceStatus::from(snapshot.database.status),
            requests: RequestSummary::from_state(&snapshot.database.requests),
        });
        response.portal = Some(state.portal_status().await);
        response.warnings = warnings;
    }

    response
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
        next_retry_secs: peer.retry_in_secs,
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

fn format_transport_addr(addr: &iroh::TransportAddr) -> String {
    match addr {
        iroh::TransportAddr::Ip(addr) => addr.to_string(),
        iroh::TransportAddr::Relay(url) => url.to_string(),
        _ => format!("{addr:?}"),
    }
}

#[cfg(test)]
#[path = "info_tests.rs"]
mod tests;
