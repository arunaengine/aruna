//! Describes what an executor backend offers and what it advertises at a placement subject.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use super::backend::{DeclaredCapabilities, WorkloadSuitability};
use super::{ExecutorCapability, ResourceEnvelope};
use crate::structs::placement::policy::{PlacementPolicyError, PlacementSubject};
use crate::structs::storage::node_info::{NODE_LABEL_KEY, stamp_location};
use crate::types::GroupId;

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
    /// The longest run the operator allows; `None` is unbounded.
    pub max_walltime_ms: Option<u64>,
    pub declared: DeclaredCapabilities,
    /// Written suitability; `None` takes the defaults of the backend's kind.
    pub suitability: Option<WorkloadSuitability>,
    /// Groups allowed to run here; empty allows every group.
    pub groups: Vec<GroupId>,
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

/// What a launch reads of one local backend: its wire kind, its limits and its config digest now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendLimit {
    pub kind: String,
    pub limits: ResourceEnvelope,
    pub config_digest: [u8; 32],
}

/// Each local backend's limits, by its node-local name.
pub type BackendLimits = BTreeMap<String, BackendLimit>;

/// One enabled backend's name, wire kind, home digest and capabilities, as plain data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutorOffer {
    name: String,
    kind: String,
    home: [u8; 32],
    caps: BackendCaps,
}

impl ExecutorOffer {
    /// `home` is the digest of where the backend's attempts live; see `identity::home_digest`.
    pub fn new((name, kind): (String, String), home: [u8; 32], caps: BackendCaps) -> Self {
        Self {
            name,
            kind,
            home,
            caps,
        }
    }

    /// Where the backend's attempts live.
    pub fn home(&self) -> [u8; 32] {
        self.home
    }

    /// The advertisement of this backend under `subject`, at the site it actually runs on.
    pub fn capability(
        &self,
        subject: &PlacementSubject,
        policy_draining: bool,
    ) -> Result<ExecutorCapability, PlacementPolicyError> {
        let caps = &self.caps;
        let site = caps.site(subject);
        let mut capability = ExecutorCapability::new(self.name.clone(), self.kind.clone(), site)?;
        capability.file_staging = caps.file_staging;
        capability.direct_s3 = caps.direct_s3;
        capability.s3_mount = caps.s3_mount;
        capability.network_policy = caps.network_policy;
        capability.session = caps.session;
        capability.limits = caps.limits;
        capability.policy_draining = policy_draining;
        capability.max_walltime_ms = caps.max_walltime_ms;
        capability.declared = caps.declared.clone();
        capability.suitability = caps.suitability.unwrap_or(capability.suitability);
        capability.groups = allowed_groups(caps);
        capability.config_digest = config_digest(&self.home, caps, capability.suitability);
        Ok(capability)
    }
}

const CONFIG_DOMAIN: &[u8] = b"aruna-compute-config-v1";

/// The groups a backend allows, in one canonical order for the advertisement and the digest.
fn allowed_groups(caps: &BackendCaps) -> Vec<GroupId> {
    let mut groups = caps.groups.clone();
    groups.sort_unstable();
    groups.dedup();
    groups
}

fn text(hasher: &mut blake3::Hasher, value: &str) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value.as_bytes());
}

/// Digest over the home digest and the capabilities placement and admission read.
pub fn config_digest(
    home: &[u8; 32],
    caps: &BackendCaps,
    suitability: WorkloadSuitability,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CONFIG_DOMAIN);
    hasher.update(home);
    let flags = [
        caps.file_staging,
        caps.direct_s3,
        caps.s3_mount,
        caps.network_policy,
        caps.local_site,
        caps.session,
    ];
    hasher.update(&flags.map(u8::from));
    match &caps.worker_site {
        Some(site) => {
            hasher.update(&[1]);
            text(&mut hasher, &site.location);
            hasher.update(&(site.labels.len() as u64).to_le_bytes());
            for (key, value) in &site.labels {
                text(&mut hasher, key);
                text(&mut hasher, value);
            }
        }
        None => {
            hasher.update(&[0]);
        }
    }
    let limits = &caps.limits;
    for ceiling in [
        limits.max_cpu_cores.map(u64::from),
        limits.max_ram_bytes,
        limits.max_disk_bytes,
        limits.max_concurrent.map(u64::from),
        caps.max_walltime_ms,
    ] {
        match ceiling {
            Some(value) => hasher.update(&[1]).update(&value.to_le_bytes()),
            None => hasher.update(&[0]),
        };
    }
    for value in [
        suitability.batch,
        suitability.sessions,
        suitability.services,
    ] {
        hasher.update(&[value as u8]);
    }
    let declared = &caps.declared;
    hasher.update(&declared.gpus.to_le_bytes());
    hasher.update(&(declared.architectures.len() as u64).to_le_bytes());
    for architecture in &declared.architectures {
        text(&mut hasher, architecture);
    }
    let flags = [
        declared.stdin,
        declared.multi_container,
        declared.services,
        declared.ingress,
    ];
    hasher.update(&flags.map(u8::from));
    let groups = allowed_groups(caps);
    hasher.update(&(groups.len() as u64).to_le_bytes());
    for group_id in &groups {
        hasher.update(&group_id.to_bytes());
    }
    *hasher.finalize().as_bytes()
}
