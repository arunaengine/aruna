//! Parses the compute entries of the node's backends file and checks them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::compute::{ExecutorKind, MAX_ADVERTISED_EXECUTORS, MAX_BACKEND_GROUPS};
use crate::errors::ConversionError;
use crate::structs::placement::policy::MAX_KIND_LEN;
use crate::structs::storage::backends::BackendsFile;
use crate::types::GroupId;

/// The name of the one backend a node builds from its environment.
pub const DEFAULT_BACKEND: &str = "default";

/// How well a backend fits one workload kind, from worst to best.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Suitability {
    Refused,
    Possible,
    Good,
    Best,
}

/// One `[compute.<name>]` table of the backends file. Secrets never live here.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ComputeEntry {
    pub kind: String,
    #[serde(default)]
    pub class: Option<String>,
    /// An unavailable backend does not stop the node.
    #[serde(default)]
    pub optional: bool,
    /// Worker site; empty leaves it unproven.
    #[serde(default)]
    pub execution_location: String,
    #[serde(default)]
    pub execution_labels: BTreeMap<String, String>,
    /// Groups allowed to run here; empty allows every group.
    #[serde(default)]
    pub groups: Vec<GroupId>,
    /// Storage backends or import roots of this node that the workers reach.
    #[serde(default)]
    pub data_reach: Vec<String>,
    /// The node's S3 endpoint as this backend's tasks reach it.
    #[serde(default)]
    pub s3_url: Option<String>,
    #[serde(default)]
    pub max_cpu_cores: Option<u32>,
    #[serde(default)]
    pub max_ram_bytes: Option<u64>,
    #[serde(default)]
    pub max_disk_bytes: Option<u64>,
    #[serde(default)]
    pub max_concurrent: Option<u32>,
    #[serde(default)]
    pub max_walltime_ms: Option<u64>,
    #[serde(default)]
    pub capabilities: DeclaredCapabilities,
    #[serde(default)]
    pub suitability: SuitabilityEntry,
    #[serde(default)]
    pub docker: Option<DockerEntry>,
    #[serde(default)]
    pub apptainer: Option<ApptainerEntry>,
    #[serde(default)]
    pub kubernetes: Option<KubernetesEntry>,
    #[serde(default)]
    pub slurm: Option<SlurmEntry>,
}

/// Properties the operator declares because the adapter cannot know them.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct DeclaredCapabilities {
    pub gpus: u32,
    pub architectures: Vec<String>,
    pub stdin: bool,
    pub multi_container: bool,
    pub services: bool,
    pub ingress: bool,
}

/// Suitability per workload kind as written; a missing key takes the kind default.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct SuitabilityEntry {
    pub batch: Option<Suitability>,
    pub sessions: Option<Suitability>,
    pub services: Option<Suitability>,
}

/// Effective suitability per workload kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadSuitability {
    pub batch: Suitability,
    pub sessions: Suitability,
    pub services: Suitability,
}

impl WorkloadSuitability {
    /// The defaults of `kind` where nothing is written: Slurm suits batch best, Kubernetes
    /// services, and Apptainer, Slurm and unknown kinds refuse sessions and services.
    pub fn of_kind(kind: &ExecutorKind) -> Self {
        use Suitability::{Best, Good, Possible, Refused};
        let (batch, sessions, services) = match kind {
            ExecutorKind::Kubernetes => (Good, Good, Best),
            ExecutorKind::Slurm => (Best, Refused, Refused),
            ExecutorKind::Docker => (Good, Good, Possible),
            _ => (Good, Refused, Refused),
        };
        Self {
            batch,
            sessions,
            services,
        }
    }
}

/// The settings `ARUNA_COMPUTE_DOCKER_*` hold for the environment backend.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DockerEntry {
    /// Daemon address; else `DOCKER_HOST`, else the platform's default socket.
    #[serde(default)]
    pub docker_host: Option<String>,
    pub state_root: String,
    #[serde(default)]
    pub disk_bytes: Option<u64>,
    #[serde(default)]
    pub session_subnet: Option<String>,
    /// Seconds.
    #[serde(default)]
    pub pull_deadline: Option<u64>,
    #[serde(default)]
    pub keep_failed: bool,
}

/// The settings `ARUNA_COMPUTE_APPTAINER_*` hold for the environment backend.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApptainerEntry {
    pub state_root: String,
    pub cgroup_root: String,
    #[serde(default)]
    pub sif_cache: Option<String>,
    /// Seconds.
    #[serde(default)]
    pub stop_grace: Option<u64>,
    /// Seconds.
    #[serde(default)]
    pub pull_deadline: Option<u64>,
}

/// The settings `ARUNA_COMPUTE_SLURM_*` hold for the environment backend.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SlurmEntry {
    /// Shared with the compute nodes.
    pub state_root: String,
    #[serde(default)]
    pub partition: Option<String>,
    #[serde(default)]
    pub account: Option<String>,
}

/// The settings `ARUNA_COMPUTE_K8S_*` hold for the environment backend.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KubernetesEntry {
    /// Kubeconfig file; without it the client runs in-cluster.
    #[serde(default)]
    pub kubeconfig: Option<String>,
    #[serde(default)]
    pub context: Option<String>,
    #[serde(default = "default_namespace")]
    pub namespace: String,
    pub storage_class: String,
    pub helper_image: String,
    #[serde(default)]
    pub s3_cidrs: Vec<String>,
    #[serde(default)]
    pub s3_port: Option<String>,
    #[serde(default)]
    pub mount_driver: Option<String>,
    #[serde(default)]
    pub kubedal_driver: Option<String>,
    #[serde(default)]
    pub kubedal_cache: Option<CacheEntry>,
    #[serde(default)]
    pub session_endpoint: Option<String>,
    #[serde(default)]
    pub policy_manifests: Vec<String>,
    #[serde(default)]
    pub service_account: Option<String>,
    #[serde(default)]
    pub node_selector: BTreeMap<String, String>,
    /// Seconds.
    #[serde(default)]
    pub pull_deadline: Option<u64>,
}

/// The KubeDAL worker cache of one Kubernetes entry.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CacheEntry {
    pub scope: String,
    #[serde(default)]
    pub worker_labels: BTreeMap<String, String>,
}

fn default_namespace() -> String {
    "default".to_string()
}

/// The system one backend's attempts live in. Two backends never share one.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum RuntimeDestination {
    Docker { host: String, state_root: String },
    Apptainer { state_root: String },
    Slurm { state_root: String },
    Kubernetes { server: String, namespace: String },
}

impl RuntimeDestination {
    /// Whether attempts run on this node itself, decided from the destination only.
    pub fn is_local(&self) -> bool {
        match self {
            Self::Docker { host, .. } => local_daemon(host),
            Self::Apptainer { .. } => true,
            Self::Slurm { .. } | Self::Kubernetes { .. } => false,
        }
    }
}

/// Whether a Docker daemon address reaches a daemon on this node: a Unix socket
/// or a Windows named pipe. Every other scheme, known or not, is remote.
pub fn local_daemon(address: &str) -> bool {
    let address = address.trim();
    address.starts_with("unix://") || address.starts_with("npipe://") || address.starts_with('/')
}

/// The daemon address Docker uses when neither the entry nor `DOCKER_HOST` names one.
pub const DOCKER_DEFAULT_HOST: &str = match cfg!(windows) {
    true => "npipe:////./pipe/docker_engine",
    false => "unix:///var/run/docker.sock",
};

/// What the caller reads outside the file to resolve runtime destinations.
pub struct DestinationInputs<'a> {
    /// `DOCKER_HOST` of the node process.
    pub docker_host: Option<&'a str>,
    /// The API server of a Kubernetes entry, from its kubeconfig and context or in-cluster.
    pub kube_server: &'a dyn Fn(&KubernetesEntry) -> Result<String, String>,
}

/// One compute entry after its checks, with the values derived from it.
#[derive(Clone, Debug, PartialEq)]
pub struct ComputeBackend {
    pub name: String,
    pub kind: ExecutorKind,
    pub entry: ComputeEntry,
    pub destination: RuntimeDestination,
    pub suitability: WorkloadSuitability,
}

/// Checks every compute entry of `file` and resolves its runtime destination.
pub fn resolve(
    file: &BackendsFile,
    inputs: &DestinationInputs<'_>,
) -> Result<Vec<ComputeBackend>, ConversionError> {
    if file.compute.len() > MAX_ADVERTISED_EXECUTORS {
        return Err(invalid(format!(
            "a node runs at most {MAX_ADVERTISED_EXECUTORS} compute backends"
        )));
    }
    let mut backends = Vec::new();
    let mut destinations = BTreeMap::new();
    for (name, entry) in &file.compute {
        let backend = resolve_entry(file, name, entry, inputs)?;
        if let Some(other) = destinations.insert(backend.destination.clone(), name) {
            return Err(invalid(format!(
                "compute backends `{other}` and `{name}` share one runtime destination"
            )));
        }
        backends.push(backend);
    }
    let session_hosts: Vec<&str> = backends
        .iter()
        .filter(|backend| backend.kind == ExecutorKind::Docker)
        .filter(|backend| backend.suitability.sessions != Suitability::Refused)
        .map(|backend| backend.name.as_str())
        .collect();
    // The session bridge is one listener per node.
    if session_hosts.len() > 1 {
        return Err(invalid(format!(
            "only one Docker compute backend may host sessions, found {}",
            session_hosts.join(", ")
        )));
    }
    Ok(backends)
}

fn resolve_entry(
    file: &BackendsFile,
    name: &str,
    entry: &ComputeEntry,
    inputs: &DestinationInputs<'_>,
) -> Result<ComputeBackend, ConversionError> {
    if name.is_empty() || name.len() > MAX_KIND_LEN || name.trim() != name {
        return Err(invalid(format!(
            "compute backend name `{name}` must be 1..={MAX_KIND_LEN} bytes without outer spaces"
        )));
    }
    if name == DEFAULT_BACKEND {
        return Err(invalid(format!(
            "compute backend name `{DEFAULT_BACKEND}` is reserved for the environment backend"
        )));
    }
    if entry.groups.len() > MAX_BACKEND_GROUPS {
        return Err(invalid(format!(
            "compute backend `{name}` allows at most {MAX_BACKEND_GROUPS} groups"
        )));
    }
    if entry
        .class
        .as_deref()
        .is_some_and(|class| class.trim().is_empty() || class.len() > MAX_KIND_LEN)
    {
        return Err(invalid(format!(
            "compute backend `{name}` sets an empty or overlong class"
        )));
    }
    let zero = [
        entry.max_cpu_cores.map(u64::from),
        entry.max_ram_bytes,
        entry.max_disk_bytes,
        entry.max_concurrent.map(u64::from),
        entry.max_walltime_ms,
    ]
    .contains(&Some(0));
    if zero {
        return Err(invalid(format!(
            "compute backend `{name}` sets a ceiling of zero; omit it to leave it unmeasured"
        )));
    }
    if let Some(unknown) = entry
        .data_reach
        .iter()
        .find(|reach| !file.backend.contains_key(*reach) && !file.import_root.contains_key(*reach))
    {
        return Err(invalid(format!(
            "compute backend `{name}` reaches unknown storage backend or import root `{unknown}`"
        )));
    }
    let (kind, destination) = destination(name, entry, inputs)?;
    let suitability = suitability(name, &kind, entry, &destination)?;
    Ok(ComputeBackend {
        name: name.to_string(),
        kind,
        entry: entry.clone(),
        destination,
        suitability,
    })
}

/// The kind and runtime destination of one entry; its one kind table must match `kind`.
fn destination(
    name: &str,
    entry: &ComputeEntry,
    inputs: &DestinationInputs<'_>,
) -> Result<(ExecutorKind, RuntimeDestination), ConversionError> {
    let tables = [
        entry.docker.is_some(),
        entry.apptainer.is_some(),
        entry.kubernetes.is_some(),
        entry.slurm.is_some(),
    ];
    if tables.iter().filter(|present| **present).count() != 1 {
        return Err(invalid(format!(
            "compute backend `{name}` must have exactly one kind table"
        )));
    }
    let kind = ExecutorKind::from_wire(entry.kind.trim());
    if let ExecutorKind::Ext(other) = &kind {
        return Err(invalid(format!(
            "compute backend `{name}` has unknown kind `{other}`"
        )));
    }
    let destination = match (&kind, entry) {
        (
            ExecutorKind::Docker,
            ComputeEntry {
                docker: Some(docker),
                ..
            },
        ) => RuntimeDestination::Docker {
            host: [docker.docker_host.as_deref(), inputs.docker_host]
                .into_iter()
                .flatten()
                .map(str::trim)
                .find(|host| !host.is_empty())
                .unwrap_or(DOCKER_DEFAULT_HOST)
                .to_string(),
            state_root: state_root(name, &docker.state_root)?,
        },
        (
            ExecutorKind::Apptainer,
            ComputeEntry {
                apptainer: Some(apptainer),
                ..
            },
        ) => RuntimeDestination::Apptainer {
            state_root: state_root(name, &apptainer.state_root)?,
        },
        (
            ExecutorKind::Slurm,
            ComputeEntry {
                slurm: Some(slurm), ..
            },
        ) => RuntimeDestination::Slurm {
            state_root: state_root(name, &slurm.state_root)?,
        },
        (
            ExecutorKind::Kubernetes,
            ComputeEntry {
                kubernetes: Some(kubernetes),
                ..
            },
        ) => RuntimeDestination::Kubernetes {
            server: (inputs.kube_server)(kubernetes).map_err(|error| {
                invalid(format!(
                    "compute backend `{name}` has no API server: {error}"
                ))
            })?,
            namespace: kubernetes.namespace.trim().to_string(),
        },
        _ => {
            return Err(invalid(format!(
                "compute backend `{name}` of kind `{}` needs a table of that kind",
                entry.kind
            )));
        }
    };
    Ok((kind, destination))
}

/// State roots are absolute and kept in their lexical form, so two spellings of one folder
/// cannot pass as two. `..` is refused, since only the file system could resolve it.
fn state_root(name: &str, root: &str) -> Result<String, ConversionError> {
    let path = std::path::Path::new(root.trim());
    let parent = path
        .components()
        .any(|component| component == std::path::Component::ParentDir);
    match path.is_absolute() && !parent {
        true => Ok(path
            .components()
            .collect::<std::path::PathBuf>()
            .to_string_lossy()
            .into_owned()),
        false => Err(invalid(format!(
            "compute backend `{name}` must set an absolute state_root without `..`"
        ))),
    }
}

/// The written suitability over the kind defaults. A backend without the session
/// capability refuses sessions, and saying otherwise is an error.
fn suitability(
    name: &str,
    kind: &ExecutorKind,
    entry: &ComputeEntry,
    destination: &RuntimeDestination,
) -> Result<WorkloadSuitability, ConversionError> {
    use Suitability::Refused;
    let defaults = WorkloadSuitability::of_kind(kind);
    let session_capable = match kind {
        ExecutorKind::Docker => destination.is_local(),
        ExecutorKind::Apptainer => true,
        ExecutorKind::Kubernetes => entry
            .kubernetes
            .as_ref()
            .is_some_and(|kubernetes| !kubernetes.s3_cidrs.is_empty()),
        _ => false,
    };
    let written = entry.suitability;
    if !session_capable && written.sessions.is_some_and(|value| value != Refused) {
        return Err(invalid(format!(
            "compute backend `{name}` cannot host sessions; only sessions = \"refused\" is valid"
        )));
    }
    Ok(WorkloadSuitability {
        batch: written.batch.unwrap_or(defaults.batch),
        sessions: match session_capable {
            true => written.sessions.unwrap_or(defaults.sessions),
            false => Refused,
        },
        services: written.services.unwrap_or(defaults.services),
    })
}

fn invalid(message: String) -> ConversionError {
    ConversionError::FromStrError(message)
}

#[cfg(test)]
#[path = "backend_tests.rs"]
mod tests;
