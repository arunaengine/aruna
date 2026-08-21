pub mod executor;
pub mod registry;

pub use executor::ExecutorBackend;
pub use executor::config::{
    ApptainerConfig, ComputeConfig, DEFAULT_WORKLOAD_SA, DockerConfig, KubernetesConfig,
};
pub use executor::dispatch_helper;
pub use registry::ExecutorRegistry;
