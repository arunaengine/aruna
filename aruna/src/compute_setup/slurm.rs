//! Builds the Slurm executor backend and its registry from the parsed compute settings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_compute::ExecutorRegistry;
use tracing::info;

use super::settings::compute_s3_endpoint;
use super::{ComputeBuildError, ComputeSettings, SlurmSettings};
use crate::config::Config;

pub(super) async fn build(
    settings: &ComputeSettings,
    slurm: &SlurmSettings,
    config: &Config,
) -> Result<ExecutorRegistry, ComputeBuildError> {
    let backend = aruna_compute::executor::apptainer::slurm::SlurmBackend::with_config(
        aruna_compute::SlurmConfig {
            state_root: slurm.state_root.clone(),
            partition: slurm.partition.clone(),
            account: slurm.account.clone(),
            execution_location: slurm.execution_location.clone(),
            execution_labels: slurm.execution_labels.clone(),
            envelope: settings
                .envelope
                .expect("a selected slurm backend has an envelope"),
            ..aruna_compute::SlurmConfig::default()
        },
    )
    .map_err(|error| error.to_string())?;
    aruna_compute::ExecutorBackend::health(&backend)
        .await
        .map_err(|error| ComputeBuildError::Unavailable(error.to_string()))?;
    info!("Slurm executor backend enabled");
    Ok(ExecutorRegistry::new()
        .with_backend(std::sync::Arc::new(backend))
        .with_workspace_endpoint(
            compute_s3_endpoint(settings, config),
            "eu-central-1".to_string(),
        ))
}
