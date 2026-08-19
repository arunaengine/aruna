//! The logical admission quota seam, wired to the replicated demand view.

use crate::driver::DriverContext;
use crate::jobs::records::rows::from_bytes;
use crate::node_info::group_demand;
use aruna_core::NodeId;
use aruna_core::compute_quota::{ComputeQuota, QuotaDenied, admits};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::JOB_ADMISSION_QUOTA_KEYSPACE;
use aruna_core::structs::{EffectiveResources, RealmConfigDocument};
use aruna_core::types::{GroupId, Key};
use tracing::info;

/// Standing-quota decision before one submission is logically admitted.
/// `Ok((Some(reason), _))` is a denial applied only to a FRESH claim;
/// `Err` means the quota or demand view is unavailable and admission fails
/// closed. An overshoot observed after convergence cancels nothing.
pub async fn quota_refusal(
    context: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    group_id: GroupId,
    resources: &EffectiveResources,
) -> Result<(Option<QuotaDenied>, Option<u64>), String> {
    let quota = config
        .compute
        .effective_quota(&group_id)
        .map_err(|error| format!("compute quota unavailable: {error}"))?;
    if quota == ComputeQuota::default() {
        return Ok((None, None));
    }
    let before = quota_revision(context, group_id).await?;
    let (view, truncated) = group_demand(context, config.realm_id, local, &group_id)
        .await
        .map_err(|error| format!("quota demand view unavailable: {error}"))?;
    if truncated {
        return Err("quota demand view is truncated".to_string());
    }
    let revision = quota_revision(context, group_id).await?;
    if revision != before {
        return Err("quota demand changed during admission check".to_string());
    }
    match admits(&view, &quota, resources) {
        Ok(()) => Ok((None, Some(revision))),
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
            Ok((Some(denied), Some(revision)))
        }
    }
}

async fn quota_revision(context: &DriverContext, group_id: GroupId) -> Result<u64, String> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: JOB_ADMISSION_QUOTA_KEYSPACE.to_string(),
            key: Key::from(group_id.to_bytes().as_slice()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| from_bytes::<u64>(&value).map_err(|error| error.to_string()))
            .transpose()
            .map(Option::unwrap_or_default),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("quota revision read failed: {other:?}")),
    }
}
