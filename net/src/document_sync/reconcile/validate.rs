//! Validates replicated admin events through the plan of core, keeping its config snapshot.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::document_apply::validate::{ConfigSnapshot, ValidateAdminPlan};

use super::*;
use crate::document_sync::plan_run::run_plan;

/// The realm config snapshot shared by the admin events of one batch.
#[derive(Default)]
pub(in crate::document_sync) struct ConfigValidationCache {
    snapshot: Option<Arc<ConfigSnapshot>>,
}

impl ConfigValidationCache {
    pub(in crate::document_sync) fn invalidate(&mut self) {
        self.snapshot = None;
    }
}

/// Validates one replicated administrative event. Authority comes from the origin's signature,
/// never the transport publisher.
#[allow(clippy::too_many_arguments)]
pub(in crate::document_sync) async fn validate_admin_event(
    storage: &StorageHandle,
    topic_id: ::irokle::TopicId,
    actor_id: ::irokle::ActorId,
    target: &DocumentTarget,
    event: &AdminDocumentEvent,
    realm_id: RealmId,
    placement: &PlacementRef,
    signature: &iroh::Signature,
    cache: &mut ConfigValidationCache,
) -> Result<AdminEventValidation> {
    let plan = ValidateAdminPlan {
        topic_id,
        actor_id,
        target: target.clone(),
        event: event.clone(),
        realm_id,
        placement: *placement,
        signature: *signature,
        cache: cache.snapshot.clone(),
    };
    let (validation, snapshot) = run_plan(storage, plan).await?;
    cache.snapshot = snapshot;
    Ok(validation)
}
