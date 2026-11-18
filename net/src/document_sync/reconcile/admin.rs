//! Applies admin document operations through the plans of core.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document_apply::admin::{AdminOperationPlan, ConfigRunPlan};
use aruna_core::document_apply::distribution::{DistributionPlan, Handled, RetryParkedPlan};

use super::*;
use crate::document_sync::plan_run::retry_plan;
use crate::document_sync::storage::sync_storage;

pub(crate) async fn apply_admin_operation(
    storage: &StorageHandle,
    target: DocumentTarget,
    event: AdminDocumentEvent,
) -> Result<()> {
    if matches!(event.target, AdminDocumentTarget::Distribution { .. })
        && matches!(target, DocumentTarget::RealmConfig { .. })
    {
        return apply_distribution_operation(storage, target, event)
            .await
            .map(|_| ());
    }
    let build = || AdminOperationPlan {
        target: target.clone(),
        event: event.clone(),
    };
    let conflicted = "admin document operation conflict retries exhausted";
    let follow_up = retry_plan(storage, build, conflicted).await?;
    if follow_up.sync {
        sync_storage(storage).await?;
    }
    match follow_up.retry_parked {
        Some(realm_id) => retry_parked(storage, realm_id).await,
        None => Ok(()),
    }
}

pub(in crate::document_sync) async fn apply_distribution_operation(
    storage: &StorageHandle,
    target: DocumentTarget,
    event: AdminDocumentEvent,
) -> Result<Handled> {
    let build = || DistributionPlan {
        target: target.clone(),
        event: event.clone(),
    };
    retry_plan(
        storage,
        build,
        "distribution snapshot apply conflict retries exhausted",
    )
    .await
}

pub(in crate::document_sync) async fn retry_parked(
    storage: &StorageHandle,
    realm_id: RealmId,
) -> Result<()> {
    let build = || RetryParkedPlan { realm_id };
    retry_plan(
        storage,
        build,
        "retrying parked snapshots exhausted its conflict retries",
    )
    .await
}

/// Applies a buffered run of coalescible realm-config events, if any, and drops the validation
/// snapshot they outdated.
pub(in crate::document_sync) async fn flush_config_run(
    storage: &StorageHandle,
    run: &mut Option<(DocumentTarget, Vec<AdminDocumentEvent>)>,
    validation_cache: &mut ConfigValidationCache,
) -> Result<()> {
    if let Some((target, events)) = run.take() {
        let build = || ConfigRunPlan {
            target: target.clone(),
            events: events.clone(),
        };
        let conflicted = "realm config admin operation conflict retries exhausted";
        retry_plan(storage, build, conflicted).await?;
        validation_cache.invalidate();
    }
    Ok(())
}
