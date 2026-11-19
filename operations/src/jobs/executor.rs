//! Holds the turns of distribution ranges and the cleanup hook of cancelled jobs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};

use aruna_core::structs::execution::job::JobPayload;
use ulid::Ulid;

/// One turn per distribution range, shared by every job on this node: page imports into one
/// range commit one at a time instead of conflicting on the range's key sample.
#[derive(Clone, Debug, Default)]
pub struct RangeTurns(Arc<Mutex<HashMap<(Ulid, Ulid), Turn>>>);

pub(crate) type Turn = Arc<tokio::sync::Mutex<()>>;

impl RangeTurns {
    /// The turns of `ranges`, keyed by set and range, in key order. Holding every lock in this
    /// order is what lets jobs share ranges; the turns end when the guards drop.
    pub fn turn_locks(&self, ranges: &BTreeSet<(Ulid, Ulid)>) -> Vec<Turn> {
        let mut map = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        // A turn nobody holds or waits for is dropped.
        map.retain(|_, turn| Arc::strong_count(turn) > 1);
        (ranges.iter())
            .map(|range| map.entry(*range).or_default().clone())
            .collect()
    }
}

/// Idempotent cleanup hook run on the cancellation path. Execution/crate payloads
/// carry no in-process side effects here.
pub fn run_cleanup(payload: &JobPayload) {
    match payload {
        JobPayload::Probe { cleanup_marker, .. } => {
            if let Some(marker) = cleanup_marker {
                let _ = std::fs::remove_file(marker);
            }
        }
        JobPayload::Execution(_)
        | JobPayload::Staging(_)
        | JobPayload::ImportRoCrate(_)
        | JobPayload::ExportRoCrate(_)
        | JobPayload::Harvest(_)
        | JobPayload::StoragePurge(_)
        | JobPayload::StoragePurgeFiles(_)
        | JobPayload::MintPersistentId(_)
        | JobPayload::WriteRunCrate { .. }
        | JobPayload::TerminalCleanup { .. }
        | JobPayload::CopyObject(_)
        | JobPayload::RegisterIdentifiers(_)
        | JobPayload::BackendImport(_)
        | JobPayload::WebhookDelivery(_)
        | JobPayload::EmailDelivery(_)
        | JobPayload::PluginWork(_) => {}
    }
}
