//! Shows the portal status while the configured portal artifact is prepared in the background.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_api::server::state::ServerState;
use aruna_config::PortalConfig;
use aruna_config::portal::{artifact_status_base, portal_dir, prepare_artifact_status};
use aruna_wire::info::PortalStatus;
use std::sync::Arc;
use tracing::{info, warn};

pub async fn initialize(config: PortalConfig, state: Arc<ServerState>) {
    match config {
        PortalConfig::Disabled => {
            state.set_portal_status(PortalStatus::default()).await;
        }
        PortalConfig::Artifact {
            artifact: config, ..
        } => {
            state
                .set_portal_status(artifact_status_base(&config, false))
                .await;
            tokio::spawn(async move {
                let portal_dir = portal_dir(&config);
                let status = prepare_artifact_status(config).await;
                if status.installed {
                    info!("Portal artifact cache prepared");
                    state.set_portal_dir(status, portal_dir).await;
                } else if let Some(error) = &status.last_error {
                    warn!(error = %error, "Portal artifact preparation failed");
                    state.set_portal_status(status).await;
                } else {
                    state.set_portal_status(status).await;
                }
            });
        }
    }
}
