//! Builds the Apptainer executor backend and its registry from the parsed compute settings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_compute::registry::ExecutorRegistry;
use tracing::info;

use super::settings::compute_s3_endpoint;
use super::{ApptainerSettings, ComputeBuildError, ComputeSettings};
use aruna_config::Config;

pub(super) async fn build(
    settings: &ComputeSettings,
    apptainer: &ApptainerSettings,
    config: &Config,
) -> Result<ExecutorRegistry, ComputeBuildError> {
    let backend = aruna_compute::executor::apptainer::ApptainerBackend::with_config(
        aruna_compute::executor::config::ApptainerConfig {
            state_root: apptainer.state_root.clone(),
            sif_cache: apptainer.sif_cache.clone(),
            cgroup_root: apptainer.cgroup_root.clone(),
            stop_grace: apptainer.stop_grace,
            pull_deadline: apptainer.pull_deadline,
            envelope: settings
                .envelope
                .expect("a selected apptainer backend has an envelope"),
            ..aruna_compute::executor::config::ApptainerConfig::default()
        },
    )
    .map_err(|error| error.to_string())?;
    aruna_compute::executor::ExecutorBackend::health(&backend)
        .await
        .map_err(|error| ComputeBuildError::Unavailable(error.to_string()))?;
    info!("Apptainer executor backend enabled");
    Ok(ExecutorRegistry::new()
        .with_backend(std::sync::Arc::new(backend))
        .with_workspace_endpoint(
            compute_s3_endpoint(settings, config),
            "eu-central-1".to_string(),
        ))
}
