//! Activates a bucket key through an install or unlock, then resumes the work that waited for it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::UnlockStatus;

use crate::driver::{DriverContext, drive};
use crate::jobs::key_wake::wake_unlocked;

/// Runs `operation`, an install or an unlock, then resumes the work that waited for its key; a
/// failed resume is only reported, since the key is usable either way.
pub async fn activate_and_wake<O>(
    context: &DriverContext,
    operation: O,
    origin: (RealmId, NodeId),
    limits: &RoCrateLimits,
) -> Result<UnlockStatus, O::Error>
where
    O: Operation<Output = UnlockStatus>,
{
    let status = drive(operation, context).await?;
    let now_ms = aruna_core::time::unix_timestamp_millis();
    if let Err(error) = wake_unlocked(context, status.key, now_ms, origin, limits).await {
        tracing::warn!(
            bucket_id = %status.key.bucket_id,
            generation = status.key.generation,
            error = %error,
            "Failed to resume work waiting for an activated key"
        );
    }
    Ok(status)
}
