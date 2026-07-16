pub mod backend;
pub mod config;
pub mod logs;
pub mod registry;

#[cfg(feature = "docker")]
pub mod docker;

pub use backend::ExecutorBackend;
pub use config::{ApptainerConfig, ComputeConfig, DockerConfig, KubernetesConfig};
pub use registry::ExecutorRegistry;
