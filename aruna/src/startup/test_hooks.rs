//! Startup barriers for binary tests, compiled only in debug builds, around recovery runs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[cfg(debug_assertions)]
use std::path::PathBuf;
use std::sync::Arc;

use aruna_operations::driver::DriverContext;
use aruna_operations::node::startup::{RecoveryConfig, RecoveryStatus, run_recovery};
#[cfg(debug_assertions)]
use tokio_util::sync::CancellationToken;
#[cfg(debug_assertions)]
use tracing::{info, warn};

/// Pends after recording that core publication started, or returns at once when
/// the test did not arm `ARUNA_TEST_CORE_PUBLICATION_BARRIER`.
#[cfg(debug_assertions)]
pub async fn core_publication_barrier() {
    let Ok(path) = std::env::var("ARUNA_TEST_CORE_PUBLICATION_BARRIER") else {
        return;
    };
    let path = PathBuf::from(path);
    if let Err(error) = std::fs::write(&path, b"active") {
        warn!(error = %error, "Failed to arm core publication test barrier");
        return;
    }
    let _barrier = CorePublicationBarrier { path };
    std::future::pending::<()>().await;
}

#[cfg(not(debug_assertions))]
pub async fn core_publication_barrier() {}

/// Records the join when the pending barrier future is dropped.
#[cfg(debug_assertions)]
struct CorePublicationBarrier {
    path: PathBuf,
}

#[cfg(debug_assertions)]
impl Drop for CorePublicationBarrier {
    fn drop(&mut self) {
        info!(
            event = "test.core_publication.joined",
            "Core publication joined"
        );
        if let Err(error) = std::fs::write(&self.path, b"joined") {
            warn!(error = %error, "Failed to record core publication join");
        }
    }
}

/// Runs recovery, joining a test barrier that keeps it active until a signal.
/// Without `ARUNA_TEST_RECOVERY_BARRIER` this is just `run_recovery`.
pub async fn recover_child(
    context: Arc<DriverContext>,
    config: RecoveryConfig,
    status: RecoveryStatus,
    cancelled: tokio_util::sync::CancellationToken,
) {
    #[cfg(debug_assertions)]
    {
        let Ok(path) = std::env::var("ARUNA_TEST_RECOVERY_BARRIER") else {
            run_recovery(context, config, status, cancelled).await;
            return;
        };
        let barrier = RecoveryBarrier {
            path: PathBuf::from(path),
        };
        tokio::join!(
            run_recovery(context, config, status.clone(), cancelled.clone()),
            watch_recovery(&barrier, status, cancelled),
        );
    }
    #[cfg(not(debug_assertions))]
    run_recovery(context, config, status, cancelled).await;
}

/// Records recovery activity while the watched status stays degraded.
#[cfg(debug_assertions)]
struct RecoveryBarrier {
    path: PathBuf,
}

#[cfg(debug_assertions)]
impl Drop for RecoveryBarrier {
    fn drop(&mut self) {
        info!(event = "test.recovery.joined", "Recovery child joined");
        if let Err(error) = std::fs::write(&self.path, b"joined") {
            warn!(error = %error, "Failed to record recovery join");
        }
    }
}

#[cfg(debug_assertions)]
async fn watch_recovery(
    barrier: &RecoveryBarrier,
    status: RecoveryStatus,
    cancelled: CancellationToken,
) {
    loop {
        if cancelled.is_cancelled() {
            return;
        }
        match status.snapshot().state {
            aruna_operations::node::startup::RecoveryState::Degraded => {
                if let Err(error) = std::fs::write(&barrier.path, b"active") {
                    warn!(error = %error, "Failed to record recovery activity");
                }
                cancelled.cancelled().await;
                return;
            }
            aruna_operations::node::startup::RecoveryState::Converged => return,
            aruna_operations::node::startup::RecoveryState::Pending
            | aruna_operations::node::startup::RecoveryState::Running => {}
        }
        tokio::task::yield_now().await;
    }
}
