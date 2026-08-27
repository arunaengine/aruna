//! Realm-admin administration of the compute plane.
//!
//! Three things live here: the realm compute configuration the planner and the
//! standing quota gate read, the eventually consistent demand and reservation
//! snapshots an operator judges pressure by, and the operator drain that stops
//! this node taking new work. Every number reported is approximate across
//! partitions and says so; none of these surfaces cancels admitted work.

use std::sync::Arc;

use aruna_core::compute_quota::{ComputeQuota, ResourceTotals};
use aruna_core::structs::{
    Actor, AuthContext, GroupComputeQuota, LocationLink, Permission, RealmComputeConfig,
    policy_admin_path,
};
use aruna_operations::driver::drive;
use aruna_operations::get_realm_config::{GetRealmConfigError, GetRealmConfigOperation};
use aruna_operations::node_info::{
    departure_report, group_demand, read_node_info_documents, read_operator_drain,
    set_operator_drain,
};
use aruna_operations::set_realm_compute::{
    SetRealmComputeConfig, SetRealmComputeError, SetRealmComputeOperation,
};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;

#[derive(OpenApi)]
#[openapi(tags((
    name = "compute",
    description = "Realm-admin administration of compute links, quotas, pressure and drain"
)))]
pub struct ComputeApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(ComputeApiDoc::openapi())
        .routes(routes!(get_compute_config, put_compute_config))
        .routes(routes!(get_compute_snapshots))
        .routes(routes!(set_compute_drain))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct LocationLinkBody {
    pub from: String,
    pub to: String,
    pub bandwidth_bytes_per_sec: u64,
}

/// Every dimension is optional and an unconfigured one is unbounded, never zero.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ComputeQuotaBody {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_jobs: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cpu_cores: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_ram_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_disk_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_job_cpu_cores: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_job_ram_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_job_disk_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_job_walltime_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct GroupQuotaBody {
    pub group_id: String,
    pub quota: ComputeQuotaBody,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ComputeConfigBody {
    pub links: Vec<LocationLinkBody>,
    pub pessimistic_bandwidth_bytes_per_sec: u64,
    pub availability_stale_after_ms: u64,
    pub witness_base_delay_ms: u64,
    pub default_group_quota: ComputeQuotaBody,
    pub group_quotas: Vec<GroupQuotaBody>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ResourceTotalsBody {
    pub count: u32,
    pub cpu_cores: u64,
    pub ram_bytes: u64,
    pub disk_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct NodeSnapshotBody {
    pub node_id: String,
    pub membership_generation: u64,
    pub publisher_generation: u64,
    pub observed_at_ms: u64,
    pub compute_draining: bool,
    pub leaving: bool,
    /// Exact local capacity this publisher holds for accepted executions.
    pub reserved: ResourceTotalsBody,
    /// Logical admitted demand groups this publisher observes.
    pub demand_groups: usize,
    /// The publisher holds more nonterminal families than it reports, so the
    /// merged view understates it instead of guessing.
    pub demand_truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct GroupDemandBody {
    pub group_id: String,
    pub demand: ResourceTotalsBody,
    pub truncated: bool,
    pub quota: ComputeQuotaBody,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct DepartureBody {
    pub departed_at_ms: u64,
    pub membership_generation: u64,
    /// Executions still reserved here when this node departed. They are
    /// unresolved, never terminal: a departing node may not declare a remotely
    /// observed execution finished.
    pub unresolved: Vec<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ComputeSnapshotsResponse {
    /// Always true: totals merge replicated snapshots, so a partition may
    /// overshoot a cap before it converges.
    pub approximate: bool,
    pub operator_draining: bool,
    pub nodes: Vec<NodeSnapshotBody>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<GroupDemandBody>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub departure: Option<DepartureBody>,
}

#[derive(Debug, Clone, Default, Deserialize, ToSchema)]
pub struct SnapshotQuery {
    pub group_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct DrainRequest {
    pub draining: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct DrainResponse {
    pub draining: bool,
    /// False when the node already had this drain state, so nothing was
    /// republished.
    pub changed: bool,
}

impl From<ComputeQuota> for ComputeQuotaBody {
    fn from(quota: ComputeQuota) -> Self {
        Self {
            max_jobs: quota.max_jobs,
            max_cpu_cores: quota.max_cpu_cores,
            max_ram_bytes: quota.max_ram_bytes,
            max_disk_bytes: quota.max_disk_bytes,
            max_job_cpu_cores: quota.max_job_cpu_cores,
            max_job_ram_bytes: quota.max_job_ram_bytes,
            max_job_disk_bytes: quota.max_job_disk_bytes,
            max_job_walltime_ms: quota.max_job_walltime_ms,
        }
    }
}

impl From<ComputeQuotaBody> for ComputeQuota {
    fn from(body: ComputeQuotaBody) -> Self {
        Self {
            max_jobs: body.max_jobs,
            max_cpu_cores: body.max_cpu_cores,
            max_ram_bytes: body.max_ram_bytes,
            max_disk_bytes: body.max_disk_bytes,
            max_job_cpu_cores: body.max_job_cpu_cores,
            max_job_ram_bytes: body.max_job_ram_bytes,
            max_job_disk_bytes: body.max_job_disk_bytes,
            max_job_walltime_ms: body.max_job_walltime_ms,
        }
    }
}

impl From<ResourceTotals> for ResourceTotalsBody {
    fn from(totals: ResourceTotals) -> Self {
        Self {
            count: totals.count,
            cpu_cores: totals.cpu_cores,
            ram_bytes: totals.ram_bytes,
            disk_bytes: totals.disk_bytes,
        }
    }
}

fn config_body(compute: &RealmComputeConfig) -> ComputeConfigBody {
    ComputeConfigBody {
        links: compute
            .links
            .iter()
            .map(|link| LocationLinkBody {
                from: link.from.clone(),
                to: link.to.clone(),
                bandwidth_bytes_per_sec: link.bandwidth_bytes_per_sec,
            })
            .collect(),
        pessimistic_bandwidth_bytes_per_sec: compute.pessimistic_bandwidth_bytes_per_sec,
        availability_stale_after_ms: compute.availability_stale_after_ms,
        witness_base_delay_ms: compute.witness_base_delay_ms,
        default_group_quota: compute.default_group_quota.into(),
        group_quotas: compute
            .group_quotas
            .iter()
            .map(|entry| GroupQuotaBody {
                group_id: entry.group_id.to_string(),
                quota: entry.quota.into(),
            })
            .collect(),
    }
}

fn compute_config(body: ComputeConfigBody) -> ServerResult<RealmComputeConfig> {
    let mut group_quotas = Vec::with_capacity(body.group_quotas.len());
    for entry in body.group_quotas {
        group_quotas.push(GroupComputeQuota {
            group_id: Ulid::from_string(&entry.group_id).map_err(|_| ServerError::BadRequest)?,
            quota: entry.quota.into(),
        });
    }
    Ok(RealmComputeConfig {
        links: body
            .links
            .into_iter()
            .map(|link| LocationLink {
                from: link.from,
                to: link.to,
                bandwidth_bytes_per_sec: link.bandwidth_bytes_per_sec,
            })
            .collect(),
        pessimistic_bandwidth_bytes_per_sec: body.pessimistic_bandwidth_bytes_per_sec,
        availability_stale_after_ms: body.availability_stale_after_ms,
        witness_base_delay_ms: body.witness_base_delay_ms,
        default_group_quota: body.default_group_quota.into(),
        group_quotas,
    })
}

/// Only a genuinely absent document is a 404; a storage or decode failure must
/// not read as "this realm has no configuration".
fn map_config_error(error: GetRealmConfigError) -> ServerError {
    match error {
        GetRealmConfigError::DocumentNotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    }
}

async fn require_config_admin(
    state: &Arc<ServerState>,
    auth: Option<AuthContext>,
    permission: Permission,
) -> ServerResult<AuthContext> {
    let auth = require_realm_auth(state, auth)?;
    ensure_permission(state, &auth, policy_admin_path(auth.realm_id), permission).await?;
    Ok(auth)
}

#[utoipa::path(
    get,
    path = "/admin/compute/config",
    tag = "compute",
    summary = "Read the realm compute configuration",
    description = r#"Returns the realm compute configuration this node currently holds.

**Authentication**: bearer token issued for this realm, with READ on the realm configuration path.

**Behavior**
- Reports the operator knowledge no node can measure for itself: the directed bandwidth between
  placement locations the planner estimates transfers with, the bandwidth to assume for a link
  nobody configured, how long an availability sample still counts for ranking, the per-rank delay
  of the leaderless witness schedule, and the standing compute quotas new admissions are decided
  against.
- A node-local read of the replicated realm configuration, so a change written on another node can
  be missing here until it arrives.
- A quota dimension that is absent is unbounded, never zero, and a group entry replaces the realm
  default wholesale rather than merging with it.

**Errors**: only a genuinely absent document is a 404; a read or decode failure is a 500, because
absence was never established."#,
    responses(
        (status = 200, description = "The compute configuration this node currently holds", body = ComputeConfigBody, example = json!({
            "links": [
                {
                    "from": "eu-west",
                    "to": "us-east",
                    "bandwidth_bytes_per_sec": 125000000_i64
                }
            ],
            "pessimistic_bandwidth_bytes_per_sec": 12500000_i64,
            "availability_stale_after_ms": 300000,
            "witness_base_delay_ms": 30000,
            "default_group_quota": {
                "max_jobs": 32,
                "max_cpu_cores": 128
            },
            "group_quotas": [
                {
                    "group_id": "01JABCDEF0123456789ABCDEFG",
                    "quota": {
                        "max_jobs": 8,
                        "max_job_walltime_ms": 3600000
                    }
                }
            ]
        })),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, or the caller may not read the realm configuration", body = ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = ErrorResponse),
        (status = 500, description = "The stored realm configuration could not be read or decoded here; absence was never established", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_compute_config(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Json<ComputeConfigBody>> {
    let auth = require_config_admin(&state, auth, Permission::READ).await?;
    let config = drive(
        GetRealmConfigOperation::new(auth.realm_id),
        &state.get_ctx(),
    )
    .await
    .map_err(map_config_error)?;
    Ok(Json(config_body(&config.compute)))
}

#[utoipa::path(
    put,
    path = "/admin/compute/config",
    tag = "compute",
    summary = "Replace the realm compute configuration",
    description = r#"Replaces the stored realm compute configuration with the submitted one.

**Authentication**: bearer token issued for this realm, with WRITE on the realm configuration path.
A management node serves it, and every other node relays the call to one.

**Behavior**
- The body replaces the stored configuration wholesale rather than patching it: links and group
  quotas absent from it are dropped, so send the complete intended configuration.
- Link direction matters, because asymmetric uplinks between sites are the normal case.
- Quotas bound new logical admissions only. Lowering one never cancels, pauses or reclaims work
  that is already admitted, queued, preparing or running.
- The demand view is replicated, so a concurrent partition may overshoot a cap before it converges;
  the only consequence of an observed overshoot is that further admissions are refused.
- The change is published through the shared realm-configuration path, so a concurrent update
  converges instead of one writer silently winning, and it takes effect on other nodes as the
  configuration propagates.

**Limits** (all refused with 400)
- A duplicate directed link pair and a zero bandwidth are refused instead of clamped, since a zero
  would make one transfer estimate infinite.
- `witness_base_delay_ms` must be greater than zero: it is the per-rank fallback delay of the
  leaderless schedule, and zero would let every witness plan at once."#,
    request_body(
        content = ComputeConfigBody,
        description = "The complete compute configuration to store",
        example = json!({
            "links": [
                {
                    "from": "eu-west",
                    "to": "us-east",
                    "bandwidth_bytes_per_sec": 125000000_i64
                },
                {
                    "from": "us-east",
                    "to": "eu-west",
                    "bandwidth_bytes_per_sec": 62500000_i64
                }
            ],
            "pessimistic_bandwidth_bytes_per_sec": 12500000_i64,
            "availability_stale_after_ms": 300000,
            "witness_base_delay_ms": 30000,
            "default_group_quota": {
                "max_jobs": 32,
                "max_cpu_cores": 128
            },
            "group_quotas": [
                {
                    "group_id": "01JABCDEF0123456789ABCDEFG",
                    "quota": {
                        "max_jobs": 8,
                        "max_job_walltime_ms": 3600000
                    }
                }
            ]
        })
    ),
    responses(
        (status = 200, description = "The compute configuration now stored in the realm configuration", body = ComputeConfigBody, example = json!({
            "links": [
                {
                    "from": "eu-west",
                    "to": "us-east",
                    "bandwidth_bytes_per_sec": 125000000_i64
                },
                {
                    "from": "us-east",
                    "to": "eu-west",
                    "bandwidth_bytes_per_sec": 62500000_i64
                }
            ],
            "pessimistic_bandwidth_bytes_per_sec": 12500000_i64,
            "availability_stale_after_ms": 300000,
            "witness_base_delay_ms": 30000,
            "default_group_quota": {
                "max_jobs": 32,
                "max_cpu_cores": 128
            },
            "group_quotas": [
                {
                    "group_id": "01JABCDEF0123456789ABCDEFG",
                    "quota": {
                        "max_jobs": 8,
                        "max_job_walltime_ms": 3600000
                    }
                }
            ]
        })),
        (status = 400, description = "A malformed group id, a duplicate directed link or group entry, an empty or oversized location, a zero bandwidth, or a zero witness delay", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, or the caller may not administer the realm configuration", body = ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = ErrorResponse),
        (status = 409, description = "Another update of the realm configuration won the race; the caller may retry with the same body", body = ErrorResponse),
        (status = 503, description = "Storage cleanup capacity exhausted, or no management node was reachable to serve the relayed call; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_compute_config(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<ComputeConfigBody>,
) -> ServerResult<Json<ComputeConfigBody>> {
    // Request policies live at this boundary; the operation only checks roles.
    let auth = require_config_admin(&state, auth, Permission::WRITE).await?;
    let compute = compute_config(request)?;
    let stored = drive(
        SetRealmComputeOperation::new(SetRealmComputeConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: state.get_realm_id(),
            },
            auth_context: auth,
            compute,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_compute_error)?;
    Ok(Json(config_body(&stored.compute)))
}

fn map_compute_error(error: SetRealmComputeError) -> ServerError {
    use aruna_core::errors::StorageError;
    match error {
        SetRealmComputeError::RealmConfigNotFound => ServerError::NotFound,
        SetRealmComputeError::Unauthorized | SetRealmComputeError::NotManagementNode => {
            ServerError::Forbidden
        }
        SetRealmComputeError::InvalidCompute { reason } => ServerError::BadRequestReason(reason),
        SetRealmComputeError::StorageError(StorageError::TransactionConflict) => {
            ServerError::Conflict(
                "concurrent realm configuration update conflict; retry".to_string(),
            )
        }
        SetRealmComputeError::StorageError(StorageError::CleanupCapacity) => {
            ServerError::ServiceUnavailableReason(
                "storage cleanup capacity exhausted; retry".to_string(),
            )
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

#[utoipa::path(
    get,
    path = "/admin/compute/snapshots",
    tag = "compute",
    summary = "Read the observed compute demand and reservation snapshots",
    description = r#"Reports the compute demand and reservation snapshots this node has replicated.

**Authentication**: bearer token issued for this realm, with READ on the realm configuration path.

**Behavior**
- Two different controls are reported side by side and are never summed: logical admitted demand,
  which is what the standing group quota is decided against, and exact physical reservations, which
  is capacity a target actually holds for accepted executions.
- Every publisher stamps its snapshot with its membership and publisher generations plus the time
  it observed them, so a stale or superseded advertisement is recognizable rather than silently
  averaged in.
- Passing `group_id` adds that group's merged demand next to the standing quota it is judged
  against; a family that several holders admitted still counts once.
- `departure` is present only when this node itself departed and reports the executions it could
  not resolve, which are unresolved rather than finished: a departing node never declares a
  remotely observed execution terminal, and removal is never blocked because those copies or
  executions exist.

**Limits**
- All totals are approximate: they merge what this node has replicated, so a partition may overshoot
  a cap before convergence.
- `demand_truncated` marks a publisher whose snapshot understates it, either because a group holds
  more nonterminal families than the snapshot names or because whole groups could not be named at
  all.
- `group_id` must be a ULID; any other value is refused with 400."#,
    params(
        ("group_id" = Option<String>, Query, description = "Merge one group's demand and report the standing quota it is judged against")
    ),
    responses(
        (status = 200, description = "The snapshots this node has replicated, all approximate", body = ComputeSnapshotsResponse, example = json!({
            "approximate": true,
            "operator_draining": false,
            "nodes": [
                {
                    "node_id": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
                    "membership_generation": 4,
                    "publisher_generation": 118,
                    "observed_at_ms": 1755500000000u64,
                    "compute_draining": false,
                    "leaving": false,
                    "reserved": {
                        "count": 2,
                        "cpu_cores": 6,
                        "ram_bytes": 12884901888_i64,
                        "disk_bytes": 0
                    },
                    "demand_groups": 1,
                    "demand_truncated": false
                }
            ],
            "group": {
                "group_id": "01JABCDEF0123456789ABCDEFG",
                "demand": {
                    "count": 3,
                    "cpu_cores": 10,
                    "ram_bytes": 21474836480_i64,
                    "disk_bytes": 0
                },
                "truncated": false,
                "quota": {
                    "max_jobs": 8,
                    "max_job_walltime_ms": 3600000
                }
            }
        })),
        (status = 400, description = "`group_id` is not a ULID", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, or the caller may not read the realm configuration", body = ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = ErrorResponse),
        (status = 500, description = "The stored realm configuration could not be read or decoded here; absence was never established", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_compute_snapshots(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<SnapshotQuery>,
) -> ServerResult<Json<ComputeSnapshotsResponse>> {
    let auth = require_config_admin(&state, auth, Permission::READ).await?;
    let group_id = query
        .group_id
        .as_deref()
        .map(Ulid::from_string)
        .transpose()
        .map_err(|_| ServerError::BadRequest)?;
    let context = state.get_ctx();
    let config = drive(GetRealmConfigOperation::new(auth.realm_id), &context)
        .await
        .map_err(map_config_error)?;
    let members = config
        .node_ids()
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let documents = read_node_info_documents(&context, &members)
        .await
        .map_err(ServerError::InternalError)?;

    let nodes = documents
        .values()
        .map(|document| NodeSnapshotBody {
            node_id: document.node_id.to_string(),
            membership_generation: document.epoch.membership_generation,
            publisher_generation: document.epoch.publisher_generation,
            observed_at_ms: document.epoch.observed_at_ms,
            compute_draining: document.compute_draining,
            leaving: document.leaving,
            reserved: document.reservation.reserved.into(),
            demand_groups: document.demand.groups.len(),
            // Whole groups the snapshot could not name understate it just as a
            // truncated group does.
            demand_truncated: document.demand.truncated
                || document.demand.groups.iter().any(|group| group.truncated),
        })
        .collect();

    let group = match group_id {
        Some(group_id) => {
            let (demand, truncated) =
                group_demand(&context, auth.realm_id, state.get_node_id(), &group_id)
                    .await
                    .map_err(ServerError::InternalError)?;
            let quota = config
                .compute
                .effective_quota(&group_id)
                .map_err(|error| ServerError::InternalError(error.to_string()))?;
            Some(GroupDemandBody {
                group_id: group_id.to_string(),
                demand: demand.into(),
                truncated,
                quota: quota.into(),
            })
        }
        None => None,
    };
    let departure = departure_report(&context)
        .await
        .map_err(ServerError::InternalError)?
        .map(|report| DepartureBody {
            departed_at_ms: report.departed_at_ms,
            membership_generation: report.membership_generation,
            unresolved: report
                .unresolved
                .iter()
                .map(ulid::Ulid::to_string)
                .collect(),
            truncated: report.truncated,
        });
    Ok(Json(ComputeSnapshotsResponse {
        approximate: true,
        operator_draining: read_operator_drain(&context)
            .await
            .map_err(ServerError::InternalError)?,
        nodes,
        group,
        departure,
    }))
}

#[utoipa::path(
    post,
    path = "/admin/compute/drain",
    tag = "compute",
    summary = "Drain or undrain this node's compute plane",
    description = r#"Sets whether this node advertises its compute plane as draining.

**Authentication**: bearer token issued for this realm, with WRITE on the realm configuration path.

**Behavior**
- A drained node advertises itself as draining, so no planner selects it for new executions and it
  declines launch offers, while everything that already holds a receipt keeps running: draining
  never cancels admitted, queued, preparing or running work, and it never revokes a reservation.
- The flag is this node's own operator decision and is stored separately from the departure state a
  placement change causes, so returning to the placement map cannot silently undrain a node an
  operator drained; undrain it explicitly with `draining` false.
- A node that is leaving the realm stays draining regardless.
- The change is republished in this node's advertisement, so other nodes stop planning here as it
  propagates, and `changed` is false when the node already had that state."#,
    request_body(
        content = DrainRequest,
        description = "Whether this node's compute plane should be drained",
        example = json!({
            "draining": true
        })
    ),
    responses(
        (status = 200, description = "The drain state now advertised by this node", body = DrainResponse, example = json!({
            "draining": true,
            "changed": true
        })),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, or the caller may not administer the realm configuration", body = ErrorResponse),
        (status = 503, description = "The advertisement could not be republished; retryable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_compute_drain(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<DrainRequest>,
) -> ServerResult<(StatusCode, Json<DrainResponse>)> {
    let auth = require_config_admin(&state, auth, Permission::WRITE).await?;
    let changed = set_operator_drain(
        &state.get_ctx(),
        state.get_node_id(),
        auth.realm_id,
        request.draining,
    )
    .await
    .map_err(ServerError::ServiceUnavailableReason)?;
    Ok((
        StatusCode::OK,
        Json(DrainResponse {
            draining: request.draining,
            changed,
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_round_trips() {
        // The transport form must preserve every planner and quota fact: a
        // dropped link would silently change every transfer estimate.
        let stored = RealmComputeConfig {
            links: vec![LocationLink {
                from: "eu-west".to_string(),
                to: "us-east".to_string(),
                bandwidth_bytes_per_sec: 125_000_000,
            }],
            group_quotas: vec![GroupComputeQuota {
                group_id: Ulid::from_bytes([7u8; 16]),
                quota: ComputeQuota {
                    max_jobs: Some(4),
                    max_job_walltime_ms: Some(1_000),
                    ..ComputeQuota::default()
                },
            }],
            ..RealmComputeConfig::default()
        };

        let parsed = compute_config(config_body(&stored)).expect("body parses");

        assert_eq!(parsed, stored);
    }

    #[test]
    fn conflict_is_retryable() {
        // A concurrent realm-configuration update must read as a retryable
        // conflict, not as a rejected configuration.
        use aruna_core::errors::StorageError;

        let conflict = map_compute_error(SetRealmComputeError::StorageError(
            StorageError::TransactionConflict,
        ));
        assert!(matches!(conflict, ServerError::Conflict(_)));
        assert!(matches!(
            map_compute_error(SetRealmComputeError::RealmConfigNotFound),
            ServerError::NotFound
        ));
        assert!(matches!(
            map_compute_error(SetRealmComputeError::InvalidCompute {
                reason: "zero bandwidth".to_string()
            }),
            ServerError::BadRequestReason(_)
        ));
    }

    #[test]
    fn capacity_is_unavailable() {
        // Cleanup capacity is transient, so it must not read as an internal error.
        use aruna_core::errors::StorageError;

        assert!(matches!(
            map_compute_error(SetRealmComputeError::StorageError(
                StorageError::CleanupCapacity
            )),
            ServerError::ServiceUnavailableReason(_)
        ));
    }

    #[test]
    fn rejects_bad_group_id() {
        let mut body = config_body(&RealmComputeConfig::default());
        body.group_quotas.push(GroupQuotaBody {
            group_id: "not-a-ulid".to_string(),
            quota: ComputeQuotaBody::default(),
        });
        assert!(compute_config(body).is_err());
    }
}
