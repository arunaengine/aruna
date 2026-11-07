//! Holds the compute backend settings for Docker, Apptainer, Kubernetes and Slurm.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::ResourceEnvelope;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use super::WorkerSite;

/// Node selector entries one backend stamps on every pod it creates.
pub const MAX_SELECTOR_ENTRIES: usize = 16;
/// Name of the internal bridge network interactive sessions join.
pub const SESSION_NETWORK: &str = "aruna-sessions";
/// Subnet of that network when the operator configures none.
pub const DEFAULT_SESSION_SUBNET: &str = "172.30.255.0/24";

#[derive(Clone, Debug)]
pub enum ComputeConfig {
    Disabled,
    Docker(DockerConfig),
    Apptainer(ApptainerConfig),
    Kubernetes(KubernetesConfig),
}

#[derive(Clone, Debug)]
pub struct DockerConfig {
    pub state_root: PathBuf,
    pub stop_grace_secs: i32,
    pub keep_failed: bool,
    pub default_mem_bytes: Option<i64>,
    pub default_nano_cpus: Option<i64>,
    pub default_disk_bytes: Option<u64>,
    pub default_max_walltime: Option<Duration>,
    pub pids_limit: i64,
    /// Subnet of the internal bridge interactive sessions join. The node's S3
    /// server listens on its gateway, the only endpoint a session reaches.
    pub session_subnet: String,
    pub pull_deadline: Duration,
    /// Static ceilings this host offers. Hard eligibility plus the basis of the
    /// advertised ranking availability.
    pub envelope: ResourceEnvelope,
}

impl Default for DockerConfig {
    fn default() -> Self {
        Self {
            state_root: PathBuf::from("./compute-state"),
            stop_grace_secs: 10,
            keep_failed: false,
            default_mem_bytes: Some(2 * 1024 * 1024 * 1024),
            default_nano_cpus: Some(2_000_000_000),
            default_disk_bytes: None,
            default_max_walltime: Some(Duration::from_secs(24 * 60 * 60)),
            pids_limit: 2048,
            session_subnet: DEFAULT_SESSION_SUBNET.to_string(),
            pull_deadline: Duration::from_secs(300),
            envelope: ResourceEnvelope::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ApptainerConfig {
    pub state_root: PathBuf,
    pub sif_cache: PathBuf,
    pub cgroup_root: PathBuf,
    pub stop_grace: Duration,
    pub pull_deadline: Duration,
    /// Applied when a request declares no ceiling, so no attempt runs outside a
    /// cgroup limit.
    pub default_mem_bytes: Option<u64>,
    pub default_cpu_cores: Option<u32>,
    pub envelope: ResourceEnvelope,
}

impl Default for ApptainerConfig {
    fn default() -> Self {
        Self {
            state_root: PathBuf::from("./compute-state/apptainer"),
            sif_cache: PathBuf::from("./compute-state/sif"),
            cgroup_root: PathBuf::from("/sys/fs/cgroup/aruna"),
            stop_grace: Duration::from_secs(10),
            pull_deadline: Duration::from_secs(300),
            default_mem_bytes: Some(2 * 1024 * 1024 * 1024),
            default_cpu_cores: Some(2),
            envelope: ResourceEnvelope::default(),
        }
    }
}

/// One Slurm cluster whose compute nodes run Apptainer and share `state_root` with this node.
#[derive(Clone, Debug)]
pub struct SlurmConfig {
    pub state_root: PathBuf,
    pub sbatch: PathBuf,
    pub squeue: PathBuf,
    pub sacct: PathBuf,
    pub scancel: PathBuf,
    /// The Apptainer command on the compute nodes.
    pub apptainer: String,
    pub partition: Option<String>,
    pub account: Option<String>,
    /// How long one Slurm command may take.
    pub command_timeout: Duration,
    /// Applied when a request declares no ceiling, so no job runs without one.
    pub default_mem_bytes: Option<u64>,
    pub default_cpu_cores: Option<u32>,
    /// Placement location of the compute nodes; empty leaves their site unproven.
    pub execution_location: String,
    pub execution_labels: BTreeMap<String, String>,
    pub envelope: ResourceEnvelope,
}

impl Default for SlurmConfig {
    fn default() -> Self {
        Self {
            state_root: PathBuf::from("./compute-state/slurm"),
            sbatch: PathBuf::from("sbatch"),
            squeue: PathBuf::from("squeue"),
            sacct: PathBuf::from("sacct"),
            scancel: PathBuf::from("scancel"),
            apptainer: "apptainer".to_string(),
            partition: None,
            account: None,
            command_timeout: Duration::from_secs(30),
            default_mem_bytes: Some(2 * 1024 * 1024 * 1024),
            default_cpu_cores: Some(2),
            execution_location: String::new(),
            execution_labels: BTreeMap::new(),
            envelope: ResourceEnvelope::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct KubernetesConfig {
    pub namespace: String,
    pub service_account: String,
    pub storage_class: String,
    pub helper_image: String,
    pub pull_deadline: Duration,
    pub s3_cidrs: Vec<String>,
    pub s3_port: u16,
    /// CSI driver name for S3 mounts; `None` disables the feature.
    pub s3_mount_driver: Option<String>,
    /// KubeDAL mounts run input views and session data with their own endpoint and credential;
    /// `None` mounts them through `s3_mount_driver`, whose endpoint is driver wide.
    pub kubedal: Option<Box<KubedalMount>>,
    /// Manifest files the operator adds to the compute namespace, such as a
    /// network policy for a cluster the standard policies cannot express.
    pub policy_manifests: Vec<PathBuf>,
    /// Placement location of the worker nodes, which are not the controller's
    /// site. Empty means the operator has not declared it, so this backend
    /// advertises no location or labels at all instead of the controller's.
    pub execution_location: String,
    /// Placement labels of those worker nodes.
    pub execution_labels: BTreeMap<String, String>,
    /// Stamped on every task and helper pod, so a declared worker site is also
    /// the site Kubernetes schedules on.
    pub node_selector: BTreeMap<String, String>,
    /// Applied when a request declares no ceiling, so no pod runs unbounded.
    pub default_mem_bytes: Option<u64>,
    pub default_cpu_cores: Option<u32>,
    pub default_disk_bytes: Option<u64>,
    /// Configured namespace envelope: Kubernetes exposes no immutable ceiling
    /// this backend could read instead.
    pub envelope: ResourceEnvelope,
}

impl Default for KubernetesConfig {
    fn default() -> Self {
        Self {
            namespace: "default".to_string(),
            service_account: DEFAULT_WORKLOAD_SA.to_string(),
            storage_class: String::new(),
            helper_image: String::new(),
            pull_deadline: Duration::from_secs(300),
            s3_cidrs: Vec::new(),
            s3_port: 443,
            s3_mount_driver: None,
            kubedal: None,
            policy_manifests: Vec::new(),
            execution_location: String::new(),
            execution_labels: BTreeMap::new(),
            node_selector: BTreeMap::new(),
            default_mem_bytes: Some(2 * 1024 * 1024 * 1024),
            default_cpu_cores: Some(2),
            default_disk_bytes: None,
            envelope: ResourceEnvelope::default(),
        }
    }
}

/// The KubeDAL driver and the S3 gateway of this node as the cluster's hosts reach it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KubedalMount {
    pub driver: String,
    pub endpoint: String,
    pub region: String,
    /// Worker cache for cached inputs; `None` mounts every input through FUSE.
    pub cache: Option<KubedalCache>,
}

/// The KubeDAL content cache this node's runs share on their workers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KubedalCache {
    /// KubeDAL `cacheScope` of the runs' DataNodes, shared inside the compute namespace.
    pub scope: String,
    /// Labels of workers with a cache volume, which runs with cached inputs prefer.
    pub worker_labels: BTreeMap<String, String>,
}

impl KubernetesConfig {
    /// Whether any driver can mount a run's inputs.
    pub fn mounts_inputs(&self) -> bool {
        self.s3_mount_driver.is_some() || self.kubedal.is_some()
    }
}

/// Service account every workload pod runs as unless the operator names another.
pub const DEFAULT_WORKLOAD_SA: &str = "aruna-workload";

impl KubernetesConfig {
    /// The worker execution site, when the operator declared one and pods are
    /// actually pinned to it. Without both, worker placement is unproven.
    pub fn worker_site(&self) -> Option<WorkerSite> {
        match self.execution_location.trim().is_empty() || self.node_selector.is_empty() {
            true => None,
            false => Some(WorkerSite {
                location: self.execution_location.trim().to_string(),
                labels: self.execution_labels.clone(),
            }),
        }
    }
}
