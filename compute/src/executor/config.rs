use std::path::PathBuf;
use std::time::Duration;

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
}

#[derive(Clone, Debug)]
pub struct KubernetesConfig {
    pub namespace: String,
    pub storage_class: String,
    pub helper_image: String,
    pub pull_deadline: Duration,
    pub s3_cidrs: Vec<String>,
    pub s3_port: u16,
}
