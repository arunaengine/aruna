//! Keeps the source bytes of an external reference on a copy-on-use or cache-on-use member
//! after a read served it from its source.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The whole object is read again, bound to its ETag or checked against its digests, and
// published like any kept copy; else nothing is kept.

use aruna_core::time::unix_timestamp_millis;
use aruna_core::structs::storage::distribution::on_use::{
    CACHE_CAPACITY_BYTES, FillClaim, PullPlan, claim_key,
};
use tracing::warn;
use ulid::Ulid;

use super::keep::{ClaimFillOperation, Claimed, KeptCopy, PublishKeptOperation, Published};
use super::pull::{drop_copy, drop_evicted, fill_slot, release};
use super::source_plan::{KeepPlanOperation, Plan, SourceKeep, SpoolSourceOperation};
use crate::driver::{DriverContext, drive};

/// Keeps the reference's bytes per the member's strategy and range behaviour: before the read
/// is served when it fills first (`true`), else in the background when a fill slot is free.
pub async fn keep_first(context: &DriverContext, keep: SourceKeep) -> bool {
    let plan = match drive(KeepPlanOperation::new(keep.clone()), context).await {
        Ok(Some(plan)) => plan,
        Ok(None) => return false,
        Err(error) => {
            warn!(%error, version_id = %keep.version_id, "A reference was not kept");
            return false;
        }
    };
    if keep.ranged && plan.pull == PullPlan::FillFirst(plan.kind) {
        return match keep_source(context, &keep, plan).await {
            Ok(kept) => kept,
            Err(reason) => {
                warn!(reason, version_id = %keep.version_id, "A reference was not kept");
                false
            }
        };
    }
    let Some(slot) = fill_slot() else {
        return false;
    };
    let context = context.clone();
    tokio::spawn(async move {
        let _slot = slot;
        if let Err(reason) = keep_source(&context, &keep, plan).await {
            warn!(reason, version_id = %keep.version_id, "A reference was not kept");
        }
    });
    false
}

/// Claims, reads, verifies and publishes one kept copy of a reference; `true` once kept.
async fn keep_source(
    context: &DriverContext,
    keep: &SourceKeep,
    plan: Plan,
) -> Result<bool, String> {
    let claim = claim_key(plan.kind, plan.set_id, keep.version_id, plan.identity);
    let claimed = FillClaim {
        claim_id: Ulid::generate(),
        claimed_at_ms: unix_timestamp_millis(),
    };
    match drive(ClaimFillOperation::new(claim.clone(), claimed), context).await {
        Ok(Claimed::Claimed) => {}
        Ok(Claimed::Busy) => return Ok(false),
        Err(error) => return Err(error.to_string()),
    }
    let spool = SpoolSourceOperation::new(keep.clone(), plan.clone());
    let copy = match drive(spool, context).await {
        Ok(copy) => copy,
        Err(error) => {
            release(context, claim).await;
            return Err(error.to_string());
        }
    };
    let (location, blake3, size) = copy;
    let kept = KeptCopy {
        kind: plan.kind,
        set_id: plan.set_id,
        version_id: keep.version_id,
        identity: plan.identity,
        blake3,
        size,
        location: location.clone(),
        claim_key: claim.clone(),
        capacity: CACHE_CAPACITY_BYTES,
        order: Ulid::generate(),
        member_id: plan.member_id,
    };
    match drop_evicted(
        context,
        drive(PublishKeptOperation::new(kept), context).await,
    )
    .await
    {
        Ok(Published::Kept { .. }) => Ok(true),
        Ok(Published::Unkept) => {
            drop_copy(context, &location).await;
            Ok(false)
        }
        Err(error) => {
            drop_copy(context, &location).await;
            release(context, claim).await;
            Err(error.to_string())
        }
    }
}
