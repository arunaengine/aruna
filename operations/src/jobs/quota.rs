//! The logical admission quota seam, wired to the replicated demand view.

use crate::driver::DriverContext;
use crate::node_info::group_demand;
use aruna_core::NodeId;
use aruna_core::compute_quota::{ComputeQuota, QuotaDenied, admits};
use aruna_core::structs::{EffectiveResources, RealmConfigDocument};
use aruna_core::types::GroupId;
use tracing::info;

/// Standing-quota decision before one submission is logically admitted.
/// `Ok(Some(reason))` is a denial the admission applies only to a FRESH claim;
/// `Err` means the quota or demand view is unavailable and admission fails
/// closed. An overshoot observed after convergence cancels nothing.
pub async fn quota_refusal(
    context: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    group_id: GroupId,
    resources: &EffectiveResources,
) -> Result<Option<QuotaDenied>, String> {
    let quota = config
        .compute
        .effective_quota(&group_id)
        .map_err(|error| format!("compute quota unavailable: {error}"))?;
    if quota == ComputeQuota::default() {
        return Ok(None);
    }
    let (view, truncated) = group_demand(context, config.realm_id, local, &group_id)
        .await
        .map_err(|error| format!("quota demand view unavailable: {error}"))?;
    if truncated {
        return Err("quota demand view is truncated".to_string());
    }
    match admits(&view, &quota, resources) {
        Ok(()) => Ok(None),
        Err(denied) => {
            info!(
                group_id = %group_id,
                scope = ?denied.scope,
                dimension = ?denied.dimension,
                observed = denied.observed,
                requested = denied.requested,
                limit = denied.limit,
                "Standing compute quota refused a new admission"
            );
            Ok(Some(denied))
        }
    }
}
