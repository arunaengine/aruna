//! Describes what an executor backend offers and what it advertises at a placement subject.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use super::{ExecutorCapability, ResourceEnvelope};
use crate::structs::placement::policy::{PlacementPolicyError, PlacementSubject};
use crate::structs::storage::node_info::{NODE_LABEL_KEY, stamp_location};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackendCaps {
    pub file_staging: bool,
    pub direct_s3: bool,
    pub s3_mount: bool,
    /// The backend proves where workers run and enforces their network
    /// isolation, which protected data requires before open networking.
    pub network_policy: bool,
    /// The workload runs on the controller host, so it inherits the
    /// controller's execution subject.
    pub local_site: bool,
    /// Where a non-local backend's workers run. `None` means their placement is
    /// unproven, so the advertisement carries no site details at all.
    pub worker_site: Option<WorkerSite>,
    /// Static ceilings; `None` is unmeasured and never filters.
    pub limits: ResourceEnvelope,
    /// The backend can open a byte channel to a running attempt, which an
    /// interactive session needs.
    pub session: bool,
}

impl BackendCaps {
    /// The execution site this backend advertises. A backend whose workers are neither the
    /// controller nor a declared worker site cannot prove placement, so it advertises no
    /// location and no site labels rather than the controller's.
    pub fn site(&self, subject: &PlacementSubject) -> PlacementSubject {
        let mut site = match (self.local_site, &self.worker_site) {
            (true, _) => PlacementSubject {
                local_to_controller: true,
                ..subject.clone()
            },
            (false, Some(site)) => {
                let mut labels = site.labels.clone();
                stamp_location(&mut labels, &site.location);
                PlacementSubject {
                    local_to_controller: false,
                    location: site.location.clone(),
                    labels,
                    ..subject.clone()
                }
            }
            (false, None) => PlacementSubject {
                local_to_controller: false,
                location: String::new(),
                labels: BTreeMap::new(),
                ..subject.clone()
            },
        };
        // The node runs every backend it offers, so a submission can pin it by its id.
        site.labels
            .insert(NODE_LABEL_KEY.to_string(), subject.node_id.to_string());
        site
    }
}

/// The operator-declared execution site of workers that do not run on the
/// controller host.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerSite {
    pub location: String,
    pub labels: BTreeMap<String, String>,
}

/// One enabled backend's wire kind and capabilities, as plain data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutorOffer {
    kind: String,
    caps: BackendCaps,
}

impl ExecutorOffer {
    pub fn new(kind: String, caps: BackendCaps) -> Self {
        Self { kind, caps }
    }

    /// The advertisement of this backend under `subject`, at the site it actually runs on.
    pub fn capability(
        &self,
        subject: &PlacementSubject,
        policy_draining: bool,
    ) -> Result<ExecutorCapability, PlacementPolicyError> {
        let caps = &self.caps;
        let mut capability = ExecutorCapability::new(self.kind.clone(), caps.site(subject))?;
        capability.file_staging = caps.file_staging;
        capability.direct_s3 = caps.direct_s3;
        capability.s3_mount = caps.s3_mount;
        capability.network_policy = caps.network_policy;
        capability.session = caps.session;
        capability.limits = caps.limits;
        capability.policy_draining = policy_draining;
        Ok(capability)
    }
}
