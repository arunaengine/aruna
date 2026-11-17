//! Keeps the source bytes of an external reference on a copy-on-use or cache-on-use member
//! after a read served it from its source.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The whole object is read again, bound to its ETag or checked against its digests, and
// published like any kept copy; else nothing is kept.

use aruna_core::effects::{BlobEffect, Effect, StagingSourceEffect};
use aruna_core::events::{BlobEvent, Event, StagingSourceEvent};
use aruna_core::structs::checksum::unmatched_checksum;
use aruna_core::structs::execution::source_access::ResolvedSourceAccess;
use aruna_core::structs::storage::blob::BlobVersionState;
use aruna_core::structs::storage::distribution::on_use::{
    CACHE_CAPACITY_BYTES, FillClaim, KeptKind, PullPlan, claim_key,
};
use tracing::warn;
use ulid::Ulid;

use super::keep::{ClaimFillOperation, Claimed, KeptCopy, PublishKeptOperation, Published};
use super::pull::{drop_copy, drop_evicted, fill_slot, release};
use super::source_plan::{KeepPlanOperation, Plan, SourceKeep};
use crate::connectors::{ResolveBindingInput, ResolveBindingOperation};
use crate::driver::{DriverContext, drive, now_ms};
use crate::s3::object::get::strong_validator;

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
        claimed_at_ms: now_ms(),
    };
    match drive(ClaimFillOperation::new(claim.clone(), claimed), context).await {
        Ok(Claimed::Claimed) => {}
        Ok(Claimed::Busy) => return Ok(false),
        Err(error) => return Err(error.to_string()),
    }
    let copy = match read_copy(context, keep, &plan).await {
        Ok(copy) => copy,
        Err(reason) => {
            release(context, claim).await;
            return Err(reason);
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

/// Reads the whole source object into a hidden copy and checks it against the record: the
/// recorded ETag binds the read, or the recorded digests must match the copy.
async fn read_copy(
    context: &DriverContext,
    keep: &SourceKeep,
    plan: &Plan,
) -> Result<
    (
        aruna_core::structs::storage::blob::BackendLocation,
        [u8; 32],
        u64,
    ),
    String,
> {
    let BlobVersionState::Reference {
        source,
        cached_metadata,
        ..
    } = &plan.version.state
    else {
        return Err("the version is no reference".to_string());
    };
    let input = ResolveBindingInput {
        source: source.clone(),
        group_id: Some(keep.group_id),
    };
    let access = drive(ResolveBindingOperation::new(input), context)
        .await
        .map_err(|error| error.to_string())?;
    let ResolvedSourceAccess::OpenDal { kind, .. } = &access;
    let bound = kind.enforces_if_match() && strong_validator(cached_metadata.etag.as_deref());
    let enforced = cached_metadata.enforced_checksums();
    if !bound && enforced.is_empty() {
        return Err("the source allows no verified read".to_string());
    }
    let read = Effect::StagingSource(StagingSourceEffect::Read {
        access,
        range: None,
        expected: bound.then(|| cached_metadata.clone()),
    });
    let stream = match crate::effect_adapters::dispatch_effect(read, context, 0).await {
        Event::StagingSource(StagingSourceEvent::ReadResult { metadata, stream })
            if metadata.content_length == cached_metadata.content_length =>
        {
            stream
        }
        Event::StagingSource(StagingSourceEvent::ReadResult { .. }) => {
            return Err("the source object changed its length".to_string());
        }
        Event::StagingSource(StagingSourceEvent::Error { error }) => return Err(error.to_string()),
        _ => return Err("unexpected answer to the source read".to_string()),
    };
    let blob_handle = context.blob_handle.as_ref().ok_or("no blob handle")?;
    let name = match plan.kind {
        KeptKind::Attachment => format!("{}-{}", plan.set_id, keep.version_id),
        KeptKind::Cache => hex::encode(plan.identity),
    };
    let spool = BlobEffect::SpoolHidden {
        namespace: plan.kind.namespace(),
        name,
        created_by: keep.user_id,
        max_bytes: Some(cached_metadata.content_length),
        deadline: None,
        blob: stream,
    };
    let (location, blake3, size) = match blob_handle.send_blob_effect(spool).await {
        Event::Blob(BlobEvent::HiddenSpooled {
            location,
            blake3,
            size,
        }) => (location, blake3, size),
        Event::Blob(BlobEvent::Error(error)) => return Err(error.to_string()),
        _ => return Err("unexpected answer to the copy write".to_string()),
    };
    if size != cached_metadata.content_length
        || unmatched_checksum(&enforced, &location.hashes).is_some()
    {
        drop_copy(context, &location).await;
        return Err("the source bytes do not match the record".to_string());
    }
    Ok((location, blake3, size))
}
