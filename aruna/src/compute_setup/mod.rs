//! Selection and construction of the node's compute executor.
//!
//! The operator selects a backend through `ARUNA_COMPUTE_EXECUTOR`; each
//! backend module reads its own settings and builds its executor. The node
//! keeps running without compute when the operator marks compute optional.

mod settings;

#[cfg(feature = "apptainer")]
mod apptainer;
#[cfg(feature = "docker")]
mod docker;
#[cfg(feature = "kubernetes")]
mod kubernetes;

use aruna_compute::ExecutorRegistry;
use std::sync::Arc;
use tracing::warn;

use crate::config::Config;

pub(crate) use settings::{session_s3_address, session_subnet};

/// Why a selected compute backend could not be built. The categories stay
/// distinct so an invalid configuration is never silently treated as
/// unavailable compute.
#[derive(Debug)]
pub(crate) enum ComputeBuildError {
    Config(String),
    Unavailable(String),
}

impl From<String> for ComputeBuildError {
    fn from(error: String) -> Self {
        Self::Config(error)
    }
}

impl From<&'static str> for ComputeBuildError {
    fn from(error: &'static str) -> Self {
        Self::Config(error.to_string())
    }
}

/// Builds the registry for the selected backend, or `None` when compute is
/// turned off. An unavailable backend is allowed only when the operator marks
/// compute optional; invalid configuration always fails.
pub(crate) async fn build_registry(
    config: &Config,
) -> Result<Option<Arc<ExecutorRegistry>>, String> {
    let selected = dotenvy::var("ARUNA_COMPUTE_EXECUTOR").unwrap_or_else(|_| "none".to_string());
    let result = match selected.trim() {
        // A supervisor that turns compute off writes a disabling value rather
        // than unsetting the key, which an inherited environment would refill.
        "none" | "off" | "" => return Ok(None),
        "docker" => build_docker(config).await,
        "apptainer" => build_apptainer(config).await,
        "kubernetes" => build_kubernetes(config).await,
        other => Err(ComputeBuildError::Config(format!(
            "unknown ARUNA_COMPUTE_EXECUTOR `{other}`"
        ))),
    };
    let registry = match result {
        Ok(registry) => Some(Arc::new(registry)),
        Err(ComputeBuildError::Unavailable(error))
            if settings::env_true("ARUNA_COMPUTE_OPTIONAL") =>
        {
            warn!(executor = %selected, reason = %error, "Compute executor unavailable; running without compute");
            None
        }
        Err(ComputeBuildError::Config(error) | ComputeBuildError::Unavailable(error)) => {
            return Err(error);
        }
    };
    Ok(registry)
}

#[cfg(feature = "docker")]
use docker::build as build_docker;
#[cfg(not(feature = "docker"))]
async fn build_docker(_config: &Config) -> Result<ExecutorRegistry, ComputeBuildError> {
    Err("Docker executor feature is not compiled".to_string().into())
}

#[cfg(feature = "apptainer")]
use apptainer::build as build_apptainer;
#[cfg(not(feature = "apptainer"))]
async fn build_apptainer(_config: &Config) -> Result<ExecutorRegistry, ComputeBuildError> {
    Err("Apptainer executor feature is not compiled"
        .to_string()
        .into())
}

#[cfg(feature = "kubernetes")]
use kubernetes::build as build_kubernetes;
#[cfg(not(feature = "kubernetes"))]
async fn build_kubernetes(_config: &Config) -> Result<ExecutorRegistry, ComputeBuildError> {
    Err("Kubernetes executor feature is not compiled"
        .to_string()
        .into())
}
