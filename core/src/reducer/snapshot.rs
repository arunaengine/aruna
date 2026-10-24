//! Keeps one distributed bucket's highest valid snapshot from its bound publisher. Distribution
//! events bypass event-id deduplication, so no per-event history grows with them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::structs::storage::distribution::snapshot::{Installed, install};
use crate::structs::storage::distribution::{DistributionError, DistributionView, SignedSnapshot};

const VIEW_PATH: &str = "distribution.view";

impl AdminDocumentState {
    /// Applies a distribution event without touching the clock or the applied event ids.
    pub(super) fn apply_set(
        &mut self,
        event: &AdminDocumentEvent,
        realm_id: &RealmId,
        set_id: &Ulid,
    ) -> Result<AdminApplyStatus, AdminDocumentError> {
        match &event.op {
            AdminDocumentOperation::DistributionPublished { snapshot } => {
                self.install_snapshot(event, *realm_id, *set_id, snapshot)
            }
            _ => Err(AdminDocumentError::UnsupportedTarget),
        }
    }

    fn install_snapshot(
        &mut self,
        event: &AdminDocumentEvent,
        realm_id: RealmId,
        set_id: Ulid,
        signed: &SignedSnapshot,
    ) -> Result<AdminApplyStatus, AdminDocumentError> {
        let snapshot = &signed.snapshot;
        if snapshot.set_id() != set_id {
            return Err(DistributionError::SetMismatch(snapshot.set_id()).into());
        }
        // K1: only the publisher bound into the set id publishes its revisions.
        if event.origin_node_id != snapshot.root.publisher {
            return Err(DistributionError::NotPublisher.into());
        }
        signed.verify(realm_id)?;
        match install(self.distribution_view().as_ref(), signed.clone())? {
            Installed::Changed(view) => {
                let value = hex::encode(view.to_bytes().map_err(|_| DistributionError::TooLarge)?);
                let version = AdminAttributeVersion {
                    value: Some(value),
                    dot: event.dot(),
                };
                self.user_subject_ids.insert(VIEW_PATH.to_string(), version);
                Ok(AdminApplyStatus::Applied)
            }
            Installed::Unchanged => Ok(AdminApplyStatus::Duplicate),
        }
    }

    /// The highest valid snapshot of a distribution target and any equivocation evidence.
    pub fn distribution_view(&self) -> Option<DistributionView> {
        if !matches!(&self.target, AdminDocumentTarget::Distribution { .. }) {
            return None;
        }
        let value = self.user_subject_ids.get(VIEW_PATH)?.value.as_deref()?;
        DistributionView::from_bytes(&hex::decode(value).ok()?).ok()
    }
}
