//! Runs the periodic S3 session expiry, credential cutoff and key recovery sweeps until shutdown.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use std::collections::BTreeSet;

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_ENCRYPTION_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::shutdown::Shutdown;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{BucketEncryption, EncryptionMode};
use aruna_core::types::GroupId;
use aruna_core::{NodeId, UserId};
use tracing::warn;

use crate::driver::{DriverContext, drive};
use crate::s3::access::cutoff::{CutoffSweepConfig, CutoffSweepOperation};
use crate::s3::bucket::holders::LookupKeysOperation;
use crate::s3::bucket::key::recovery::{
    RecoveryInput, RecoveryNoticeError, RecoveryNoticeOperation,
};
use crate::s3::key_status::KeyStatusOperation;
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

/// Checks one bucket: reads its holders, asks the key directory and tells the holders if the
/// recovery path weakened. Call it after a group or realm role change and at startup.
async fn check_recovery(
    context: &DriverContext,
    (realm_id, node_id): (RealmId, NodeId),
    bucket: &str,
    group_id: GroupId,
) -> Result<Vec<UserId>, RecoveryNoticeError> {
    let status = KeyStatusOperation::new(bucket.to_string(), realm_id, group_id);
    let snapshot = drive(status, context)
        .await
        .map_err(|error| StorageError::ReadError(error.to_string()))?;
    let Some(info) = snapshot.info else {
        return Ok(Vec::new());
    };
    let users: BTreeSet<_> = std::iter::once(info.created_by)
        .chain(snapshot.admins.iter().copied())
        .chain(snapshot.grants.iter().map(|grant| grant.user_id))
        .collect();
    // A failed lookup counts as unavailable, as a missing answer does.
    let lookups = drive(LookupKeysOperation::new(realm_id, node_id, users), context)
        .await
        .unwrap_or_default();
    let input = RecoveryInput {
        bucket: bucket.to_string(),
        group_id,
        realm_id,
        node_id,
        lookups,
        now_ms: aruna_core::time::unix_timestamp_millis(),
    };
    drive(RecoveryNoticeOperation::new(input), context).await
}

/// How often the node checks its vault-locked buckets for weakened recovery; role changes reach
/// it through document sync, so no local event marks them.
const RECOVERY_SWEEP: Duration = Duration::from_secs(15 * 60);

/// Checks every vault-locked bucket of this node once.
async fn check_buckets(context: &DriverContext, origin: (RealmId, NodeId)) {
    let mut start = None;
    loop {
        let scan = StorageEffect::Iter {
            key_space: BUCKET_ENCRYPTION_KEYSPACE.to_string(),
            prefix: None,
            start: start.take().map(IterStart::After),
            limit: 1_000,
            txn_id: None,
        };
        let event = context.storage_handle.send_storage_effect(scan).await;
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            warn!("Bucket recovery sweep could not list buckets");
            return;
        };
        for (key, value) in values {
            let locked = BucketEncryption::from_bytes(value.as_ref())
                .is_ok_and(|settings| settings.mode == EncryptionMode::VaultLocked);
            let bucket = String::from_utf8_lossy(key.as_ref()).into_owned();
            let group_id = match locked {
                true => bucket_group(context, &bucket).await,
                false => None,
            };
            let Some(group_id) = group_id else {
                continue;
            };
            if let Err(error) = check_recovery(context, origin, &bucket, group_id).await {
                warn!(%bucket, error = %error, "Bucket recovery check failed");
            }
        }
        match next_start_after {
            Some(next) => start = Some(next),
            None => return,
        }
    }
}

async fn bucket_group(context: &DriverContext, bucket: &str) -> Option<GroupId> {
    let read = StorageEffect::Read {
        key_space: S3_BUCKET_KEYSPACE.to_string(),
        key: bucket.as_bytes().to_vec().into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => BucketInfo::from_bytes(value.as_ref())
            .ok()
            .map(|info| info.group_id),
        _ => None,
    }
}

/// Checks the vault-locked buckets right after startup and then at a fixed interval.
pub fn spawn_recovery_sweep(
    context: Arc<DriverContext>,
    origin: (RealmId, NodeId),
    shutdown: &Shutdown,
) {
    let token = shutdown.token();
    shutdown.spawn(async move {
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = check_buckets(context.as_ref(), origin) => {}
            }
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(RECOVERY_SWEEP) => {}
            }
        }
    });
}
