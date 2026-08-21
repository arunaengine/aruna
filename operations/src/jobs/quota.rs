//! The logical admission quota seam, wired to the replicated demand view.

use crate::driver::DriverContext;
use crate::jobs::records::rows::from_bytes;
use crate::node_info::group_demand;
use aruna_core::NodeId;
use aruna_core::compute_quota::{
    ComputeQuota, QuotaDenied, ResourceTotals, admits, understated_denial,
};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::JOB_ADMISSION_QUOTA_KEYSPACE;
use aruna_core::structs::{EffectiveResources, RealmConfigDocument};
use aruna_core::types::{GroupId, Key};
use tracing::{info, warn};

/// Reads of the group's demand view one check makes before it gives up on a
/// group whose revision keeps moving under it.
const QUOTA_ATTEMPTS: usize = 3;

/// Standing-quota decision before one submission is logically admitted.
/// `Ok((Some(reason), _))` is a denial applied only to a FRESH claim;
/// `Err` means the quota or demand view is unavailable and admission fails
/// closed. An overshoot observed after convergence cancels nothing.
///
/// A group whose merged view is understated is denied rather than admitted: the
/// cap cannot be shown to hold, and a refusal is a quota decision about that
/// group, never an availability failure of the node.
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
    for _ in 0..QUOTA_ATTEMPTS {
        let before = quota_revision(context, group_id).await?;
        let (view, truncated) = group_demand(context, config.realm_id, local, &group_id)
            .await
            .map_err(|error| format!("quota demand view unavailable: {error}"))?;
        let revision = quota_revision(context, group_id).await?;
        // A concurrent admission moved the group under this read; the view it
        // produced is not the one the decision would commit against.
        if revision != before {
            continue;
        }
        return Ok((
            decide(&quota, &view, truncated, resources, group_id),
            Some(revision),
        ));
    }
    Err("quota demand view did not settle".to_string())
}

fn decide(
    quota: &ComputeQuota,
    view: &ResourceTotals,
    truncated: bool,
    resources: &EffectiveResources,
    group_id: GroupId,
) -> Option<QuotaDenied> {
    let denied = match admits(view, quota, resources) {
        Err(denied) => Some(denied),
        Ok(()) if truncated => {
            warn!(group_id = %group_id, "Standing compute quota decided on an understated group view");
            understated_denial(quota, resources)
        }
        Ok(()) => None,
    }?;
    info!(
        group_id = %group_id,
        scope = ?denied.scope,
        dimension = ?denied.dimension,
        observed = denied.observed,
        requested = denied.requested,
        limit = denied.limit,
        "Standing compute quota refused a new admission"
    );
    Some(denied)
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::compute_quota::{QuotaDimension, QuotaScope};
    use ulid::Ulid;

    fn resources() -> EffectiveResources {
        EffectiveResources {
            cpu_cores: 1,
            ram_bytes: 0,
            disk_bytes: 0,
            max_walltime_ms: 1_000,
            preemptible: false,
        }
    }

    #[test]
    fn understated_view_denies() {
        // An understated group view is a quota decision about that group, not
        // an availability failure, and it never touches an uncapped group.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let capped = ComputeQuota {
            max_jobs: Some(4),
            ..Default::default()
        };
        let denied = decide(
            &capped,
            &ResourceTotals::default(),
            true,
            &resources(),
            group_id,
        )
        .expect("an understated capped group denies");
        assert_eq!(denied.scope, QuotaScope::Group);
        assert_eq!(denied.dimension, QuotaDimension::Jobs);
        assert_eq!(denied.limit, 4);

        assert_eq!(
            decide(
                &capped,
                &ResourceTotals::default(),
                false,
                &resources(),
                group_id
            ),
            None
        );
        let per_job = ComputeQuota {
            max_job_cpu_cores: Some(2),
            ..Default::default()
        };
        assert_eq!(
            decide(
                &per_job,
                &ResourceTotals::default(),
                true,
                &resources(),
                group_id
            ),
            None
        );
    }
}
