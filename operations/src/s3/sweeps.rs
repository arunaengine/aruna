//! Runs the periodic S3 session expiry and credential cutoff sweeps until shutdown.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use aruna_core::NodeId;
use aruna_core::shutdown::Shutdown;
use aruna_core::structs::identity::realm::RealmId;
use tracing::warn;

use crate::driver::{DriverContext, drive};
use crate::s3::access::cutoff::{CutoffSweepConfig, CutoffSweepOperation};
use crate::s3::session::{PURGE_BATCH, PurgeSessionsOperation};

/// How often expired S3 sessions are purged.
const SESSION_SWEEP: Duration = Duration::from_secs(60);

pub fn spawn_session_sweep(context: Arc<DriverContext>, shutdown: &Shutdown) {
    let token = shutdown.token();
    shutdown.spawn(async move {
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(SESSION_SWEEP) => {}
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

/// A cutoff stays in the realm's revocation set for 366 days, so hourly sweeps delete every
/// credential it ends long before then. A node offline for that whole time can miss it.
const CUTOFF_SWEEP: Duration = Duration::from_secs(60 * 60);

/// Sweeps once per `CUTOFF_SWEEP` until shutdown; a failed sweep is retried at the next one.
pub fn spawn_cutoff_sweep(
    context: Arc<DriverContext>,
    shutdown: &Shutdown,
    realm_id: RealmId,
    node_id: NodeId,
) {
    let token = shutdown.token();
    shutdown.spawn(async move {
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(CUTOFF_SWEEP) => {}
            }
            let now = aruna_core::time::unix_timestamp_secs();
            let config = CutoffSweepConfig {
                realm_id,
                node_id,
                now,
            };
            match drive(CutoffSweepOperation::new(config), context.as_ref()).await {
                Ok(0) => {}
                Ok(revoked) => tracing::info!(revoked, "Deleted S3 credentials of cut-off owners"),
                Err(error) => warn!(error = %error, "S3 credential cutoff sweep failed"),
            }
        }
    });
}
