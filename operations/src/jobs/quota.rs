//! Checks one submission against the group compute quota using the replicated demand view.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use crate::jobs::demand::{DemandError, DemandSnapshotOperation, merge_view};
use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::quota::{
    ComputeDemandSnapshot, ComputeQuota, QuotaDenied, ResourceTotals, admits, understated_denial,
};
use aruna_core::heartbeat::HeldHeartbeat;
use aruna_core::keyspaces::ADMISSION_QUOTA_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::execution::job::EffectiveResources;
use aruna_core::structs::execution::job::record_rows::from_bytes;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::node_info::AdvertisementEpoch;
use aruna_core::types::{GroupId, Key};
use thiserror::Error;
use tracing::{info, warn};

/// Reads of the group's demand view one check makes before it gives up on a
/// group whose revision keeps moving under it.
const QUOTA_ATTEMPTS: usize = 3;

/// Why the quota check could not decide; admission then fails closed.
#[derive(Debug, Error, PartialEq)]
pub(crate) enum QuotaError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("{0}")]
    Unavailable(String),
}

/// The refusal a standing quota gives a new claim, and the group revision it decided against.
pub(crate) type QuotaVerdict = (Option<QuotaDenied>, Option<u64>);

#[derive(Debug, PartialEq)]
enum QuotaStep {
    Before,
    Demand,
    After,
}

/// Standing-quota decision before one submission is admitted. A denial applies only to a FRESH
/// claim and an overshoot after convergence cancels nothing; an unavailable view fails
/// admission closed and an understated view is denied. The group view is the local demand
/// merged with the fresh heartbeats of the other sync-eligible members, read between two
/// reads of the group revision: a concurrent admission that moved the group reads it again.
#[derive(Debug, PartialEq)]
pub(crate) struct QuotaCheckOperation {
    config: RealmConfigDocument,
    local: NodeId,
    group_id: GroupId,
    resources: EffectiveResources,
    fresh: BTreeMap<NodeId, HeldHeartbeat>,
    attempts: usize,
    before: u64,
    view: Option<(ResourceTotals, bool)>,
    step: QuotaStep,
}

type QuotaNext = Result<Next<QuotaVerdict>, QuotaError>;

impl QuotaCheckOperation {
    pub(crate) fn new(
        config: RealmConfigDocument,
        local: NodeId,
        group_id: GroupId,
        resources: EffectiveResources,
        fresh: BTreeMap<NodeId, HeldHeartbeat>,
    ) -> Self {
        Self {
            config,
            local,
            group_id,
            resources,
            fresh,
            attempts: 0,
            before: 0,
            view: None,
            step: QuotaStep::Before,
        }
    }

    fn quota(&self) -> Result<ComputeQuota, QuotaError> {
        let quota = self.config.compute.effective_quota(&self.group_id);
        quota
            .map_err(|error| QuotaError::Unavailable(format!("compute quota unavailable: {error}")))
    }

    fn revision(&mut self, step: QuotaStep) -> QuotaNext {
        self.step = step;
        let row = (
            ADMISSION_QUOTA_KEYSPACE.to_string(),
            Key::from(self.group_id.to_bytes().as_slice()),
        );
        Ok(Next::Call(IoCall::Read(vec![row])))
    }
}

fn read_revision(result: IoResult) -> Result<u64, QuotaError> {
    let [value] = values::<1>(result)?;
    value
        .map(|value| from_bytes::<u64>(&value))
        .transpose()
        .map(Option::unwrap_or_default)
        .map_err(|error| QuotaError::Unavailable(error.to_string()))
}

impl Operation for QuotaCheckOperation {
    type Output = QuotaVerdict;
    type Error = QuotaError;

    fn first(&mut self) -> QuotaNext {
        if self.quota()? == ComputeQuota::default() {
            return Ok(Next::Done((None, None)));
        }
        self.revision(QuotaStep::Before)
    }

    fn next(&mut self, result: IoResult) -> QuotaNext {
        match self.step {
            QuotaStep::Before => {
                self.before = read_revision(result)?;
                self.step = QuotaStep::Demand;
                let snapshot = DemandSnapshotOperation::new(AdvertisementEpoch::default());
                Ok(Next::Call(IoCall::child(snapshot)))
            }
            QuotaStep::Demand => {
                let unavailable = |error: String| {
                    QuotaError::Unavailable(format!("quota demand view unavailable: {error}"))
                };
                let local = result
                    .child::<ComputeDemandSnapshot, DemandError>()?
                    .map_err(|error| unavailable(error.to_string()))?;
                let members = self
                    .config
                    .sync_eligible_nodes()
                    .map_err(|error| unavailable(error.to_string()))?;
                let view = merge_view(local, &members, self.local, &self.fresh, &self.group_id);
                self.view = Some(view);
                self.revision(QuotaStep::After)
            }
            QuotaStep::After => {
                let revision = read_revision(result)?;
                // A concurrent admission moved the group under this read; the view it
                // produced is not the one the decision would commit against.
                if revision != self.before {
                    self.attempts += 1;
                    if self.attempts >= QUOTA_ATTEMPTS {
                        let settled = "quota demand view did not settle".to_string();
                        return Err(QuotaError::Unavailable(settled));
                    }
                    return self.revision(QuotaStep::Before);
                }
                let (view, truncated) = self.view.take().unwrap_or_default();
                let quota = self.quota()?;
                let denied = decide(&quota, &view, truncated, &self.resources, self.group_id);
                Ok(Next::Done((denied, Some(revision))))
            }
        }
    }
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

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::compute::quota::{QuotaDimension, QuotaScope};
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
            job_cpu_cores: Some(2),
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

    fn check(max_jobs: Option<u32>) -> QuotaCheckOperation {
        let fixture = crate::tests::records::Family::new([1u8; 32]);
        let mut config = fixture.config.clone();
        config.compute.default_group_quota = ComputeQuota {
            max_jobs,
            ..Default::default()
        };
        QuotaCheckOperation::new(
            config,
            fixture.holder.public(),
            Ulid::from_bytes([1u8; 16]),
            resources(),
            BTreeMap::new(),
        )
    }

    fn revision(value: u64) -> IoResult {
        let bytes = aruna_core::structs::execution::job::record_rows::to_bytes(&value).unwrap();
        IoResult::Values(vec![Some(bytes.as_slice().into())])
    }

    fn snapshot() -> IoResult {
        let result = Ok::<_, DemandError>(ComputeDemandSnapshot::default());
        IoResult::Child(aruna_core::calls::ChildOutcome::new(result))
    }

    // An uncapped group reads nothing; a capped one decides against a settled revision.
    #[test]
    fn decides_on_settled_view() {
        assert_eq!(check(None).first(), Ok(Next::Done((None, None))));
        let mut capped = check(Some(4));
        assert!(matches!(capped.first(), Ok(Next::Call(IoCall::Read(_)))));
        assert!(matches!(
            capped.next(revision(3)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert!(matches!(
            capped.next(snapshot()),
            Ok(Next::Call(IoCall::Read(_)))
        ));
        let Ok(Next::Done((_, settled))) = capped.next(revision(3)) else {
            panic!("an unmoved group is decided");
        };
        assert_eq!(settled, Some(3));
    }

    // A group that keeps moving under the read is unavailable after the last attempt.
    #[test]
    fn moving_group_unavailable() {
        let mut capped = check(Some(4));
        capped.first().expect("revision read");
        for attempt in 0..QUOTA_ATTEMPTS as u64 {
            capped.next(revision(attempt * 2)).expect("demand read");
            capped.next(snapshot()).expect("revision read");
            let after = capped.next(revision(attempt * 2 + 1));
            if attempt + 1 == QUOTA_ATTEMPTS as u64 {
                assert!(matches!(after, Err(QuotaError::Unavailable(_))));
            } else {
                assert!(matches!(after, Ok(Next::Call(IoCall::Read(_)))));
            }
        }
    }
}
