//! Serves the system routes for node and realm status, placement, quota and usage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod node;
mod placement;
mod quota;
mod realm;
#[cfg(test)]
mod tests;
mod usage;

pub(crate) use self::node::run_node_info;
pub use self::node::{
    BackendCapabilities, BackendLimits, BackendSuitability, DownloadSlotsResponse,
    ExecutorCapabilityResponse, NodeDocumentResponse, NodeInfoResponse, NodeKindInfo,
    NodePlacementResponse, RealmConnectionStatus, RealmUrlsResponse, RealmUtilizationResponse,
    TelemetryState, get_info,
};
pub use self::placement::{
    RealmAffinityEffect, RealmAffinityRule, RealmBinding, RealmBindingScope, RealmHealthResponse,
    RealmPlacementClass, RealmPlacementLabel, RealmPlacementOverride, RealmPlacementRequest,
    RealmPlacementResponse, RealmPlacementStrategy, get_realm_placement, mutate_realm_placement,
};
pub use self::quota::{GroupCapOverride, RealmQuotaConfig, RealmQuotaOverride, set_realm_quota};
pub(crate) use self::realm::run_realm_info;
pub use self::realm::{
    PublicNode, RealmInfoResponse, RealmProviderResponse, RealmPublicOverview,
    RealmReplicationResponse, get_realm_info,
};
pub(crate) use self::usage::distinct_buckets;
pub use self::usage::{
    GroupQuotaStatus, UsageResponse, UsageTotals, get_usage, load_realm_usage, load_usage_counters,
};
use crate::auth::permission_granted;
use crate::error::{ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use std::sync::Arc;
use tracing::warn;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(node::get_info))
        .routes(routes!(realm::get_realm_info))
        .routes(routes!(
            placement::get_realm_placement,
            placement::mutate_realm_placement
        ))
        .routes(routes!(quota::set_realm_quota))
        .routes(routes!(usage::get_usage))
}

pub(crate) async fn is_realm_admin(state: &ServerState, auth: &AuthContext) -> ServerResult<bool> {
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
pub(crate) enum InfoAccess {
    Public,
    Realm,
    Admin,
}

pub(crate) async fn info_access(state: &ServerState, auth: Option<&AuthContext>) -> InfoAccess {
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
