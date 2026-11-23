//! Serves the realm quota route and the quota types it reads and writes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{RealmAuth, ensure_permission};
use crate::error::{ServerError, ServerResult};
use crate::lookup::actor;
use crate::server::state::ServerState;
use aruna_core::errors::StorageError;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::identity::realm::{
    GroupQuotaOverride, QuotaConfig, ReplicaQuota, UserCapOverride,
};
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::user::id::UserId;
use aruna_ops_identity::realm::set_quota::{SetQuotaConfig, SetQuotaError, SetQuotaOperation};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ulid::Ulid;
use utoipa::ToSchema;

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
    /// Whether copies of a distributed range besides its coordinator's count toward group
    /// quotas; `false` when omitted. References never count.
    #[serde(default)]
    pub count_replicas: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmGroupQuotaOverride)]
pub struct RealmQuotaOverride {
    pub group_id: String,
    pub quota_bytes: Option<u64>,
    pub grace_factor_percent: Option<u32>,
    /// Overrides the realm's `count_replicas` for this group; omitted keeps it.
    #[serde(default)]
    pub count_replicas: Option<bool>,
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
                    count_replicas: None,
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
            count_replicas: false,
        }
    }
}

impl RealmQuotaConfig {
    /// Adds the stored replica setting, a group's beside its quota override.
    pub(super) fn with_replicas(mut self, replicas: &ReplicaQuota) -> Self {
        self.count_replicas = replicas.count;
        for over in &mut self.group_overrides {
            let group_id = Ulid::from_string(&over.group_id).ok();
            over.count_replicas = group_id.and_then(|id| replicas.groups.get(&id).copied());
        }
        self
    }

    fn into_quota_config(self) -> ServerResult<(QuotaConfig, ReplicaQuota)> {
        let mut replicas = ReplicaQuota {
            count: self.count_replicas,
            groups: Default::default(),
        };
        let group_overrides = self
            .group_overrides
            .into_iter()
            .map(|over| {
                let group_id = Ulid::from_string(&over.group_id).map_err(|_| {
                    ServerError::BadRequestReason(format!(
                        "invalid group id in group_overrides: {}",
                        over.group_id
                    ))
                })?;
                if let Some(count) = over.count_replicas {
                    replicas.groups.insert(group_id, count);
                }
                Ok(GroupQuotaOverride {
                    group_id,
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
        let quota = QuotaConfig {
            default_quota_bytes: self.default_quota_bytes,
            grace_factor_percent: self.grace_factor_percent,
            warn_threshold_percent: self.warn_threshold_percent,
            group_overrides,
            groups_per_user: self.groups_per_user,
            group_cap_overrides,
            devices_per_user: self.devices_per_user,
            device_request_rate: self.device_request_rate,
            device_concurrent_pulls: self.device_concurrent_pulls,
        };
        Ok((quota, replicas))
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
- Per-group overrides replace these values for one group, and per-user overrides cap how many
  groups a user may hold.
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
- A distributed bucket counts each object once, at the node coordinating its key range.
  `count_replicas` (default false, overridable per group) decides whether the copies other member
  nodes hold count as well. References to external data never count toward a quota.

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
                {"group_id": "01JABCDEF0123456789ABCDEFG", "quota_bytes": 107374182400_i64, "grace_factor_percent": 120, "count_replicas": true}
            ],
            "max_groups_per_user": 10,
            "user_group_cap_overrides": [
                {"user_id": "01JHKMNPQR0123456789ABCDEF@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8", "max_groups": 25}
            ],
            "max_devices_per_user": 5,
            "device_requests_per_minute": 600,
            "device_concurrent_pulls": 8,
            "count_replicas": false
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
                    {"group_id": "01JABCDEF0123456789ABCDEFG", "quota_bytes": 107374182400_i64, "grace_factor_percent": 120, "count_replicas": true}
                ],
                "max_groups_per_user": 10,
                "user_group_cap_overrides": [
                    {"user_id": "01JHKMNPQR0123456789ABCDEF@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8", "max_groups": 25}
                ],
                "max_devices_per_user": 5,
                "device_requests_per_minute": 600,
                "device_concurrent_pulls": 8,
                "count_replicas": false
            })
        ),
        (status = 400, description = "A percentage outside its allowed range, a duplicate or malformed override, an id that is not a ULID or user identifier, or a zero device cap or device limit", body = crate::error::ErrorResponse),
        (status = 403, description = "Caller is not a realm config admin", body = crate::error::ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = crate::error::ErrorResponse),
        (status = 409, description = "Another update of the realm configuration won the race; the caller may retry with the same body", body = crate::error::ErrorResponse),
        crate::openapi::RelayErrors
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_realm_quota(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Json(request): Json<RealmQuotaConfig>,
) -> ServerResult<(StatusCode, Json<RealmQuotaConfig>)> {
    // Request policies live at this boundary; the operation only checks roles.
    ensure_permission(
        &state,
        &auth,
        policy_admin_path(state.get_realm_id()),
        Permission::WRITE,
    )
    .await?;
    let (quota, replicas) = request.into_quota_config()?;
    let actor = actor(&state, &auth);
    let stored = drive(
        SetQuotaOperation::new(SetQuotaConfig {
            actor,
            auth_context: auth,
            quota,
            replicas,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(ServerError::from)?;
    let response = RealmQuotaConfig::from(stored.quota).with_replicas(&stored.replica_quota);
    Ok((StatusCode::OK, Json(response)))
}

impl From<SetQuotaError> for ServerError {
    fn from(error: SetQuotaError) -> Self {
        match error {
            SetQuotaError::ConfigMissing => ServerError::NotFound,
            SetQuotaError::Unauthorized | SetQuotaError::NotManagementNode => {
                ServerError::Forbidden
            }
            SetQuotaError::InvalidQuota { reason } => ServerError::BadRequestReason(reason),
            SetQuotaError::StorageError(StorageError::TransactionConflict) => {
                ServerError::Conflict("concurrent realm quota update conflict; retry".to_string())
            }
            SetQuotaError::StorageError(error) => error.into(),
            other => ServerError::InternalError(other.into()),
        }
    }
}
