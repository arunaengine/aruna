//! Runs the periodic sweep that purges expired S3 sessions until shutdown.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use aruna_tasks::shutdown::Shutdown;
use tracing::warn;

use crate::driver::{DriverContext, drive};
use crate::s3::session::{PURGE_BATCH, PurgeSessionsOperation};

const SWEEP_INTERVAL: Duration = Duration::from_secs(60);

pub fn spawn_session_sweep(context: Arc<DriverContext>, shutdown: &Shutdown) {
    let token = shutdown.token();
    shutdown.spawn(async move {
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(SWEEP_INTERVAL) => {}
            }
            loop {
                match drive(
                    PurgeSessionsOperation::new(SystemTime::now()),
                    context.as_ref(),
                )
                .await
                {
                    Ok(result) if result.scanned == PURGE_BATCH && result.removed > 0 => {
                        tokio::task::yield_now().await
                    }
                    Ok(_) => break,
                    Err(error) => {
                        warn!(error = ?error, "S3 session expiry sweep failed");
                        break;
                    }
                }
            }
        }
    });
}
