pub mod executor;
pub mod registry;

pub use executor::ExecutorBackend;
pub use executor::config::{ApptainerConfig, ComputeConfig, DockerConfig, KubernetesConfig};
pub use registry::ExecutorRegistry;
