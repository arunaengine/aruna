//! Keeps the evidence of rejected sync events next to the cursor that moves past them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use byteview::ByteView;
use tracing::warn;

use super::registry::Row;
use super::{ApplyError, Step, invalid};
use crate::keyspaces::{QUARANTINE_USAGE_KEYSPACE, SYNC_QUARANTINE_KEYSPACE};
use crate::row_plan::{NeedsRows, PlanRows, PlanStop, Planned, RowPlan};
use crate::structs::{
    QUARANTINE_USAGE_KEY, SyncQuarantineCapacity, SyncQuarantineError, SyncQuarantineInput,
    SyncQuarantineUsage, SyncRejection, build_quarantine_entries, quarantine_usage_entry,
};

/// Evidence rows for permanently rejected events, chained through the usage total and
/// deduplicated by transport identity. `None` means the store is full: cursors stay unwritten so
/// the events redeliver.
pub fn quarantine_entries(
    rows: &mut PlanRows,
    rejections: &[SyncRejection],
) -> Step<Option<Vec<Row>>> {
    if rejections.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let usage = rows.read(
        QUARANTINE_USAGE_KEYSPACE,
        ByteView::from(QUARANTINE_USAGE_KEY),
    );
    let mut waiting = false;
    let keys: Vec<Vec<u8>> = (rejections.iter())
        .map(|rejection| rejection.identity.storage_key())
        .collect();
    for key in &keys {
        rows.want(SYNC_QUARANTINE_KEYSPACE, ByteView::from(key.clone()));
    }
    let mut usage = match usage {
        Ok(Some(value)) => SyncQuarantineUsage::from_bytes(value.as_ref()).map_err(invalid)?,
        Ok(None) => SyncQuarantineUsage::default(),
        Err(NeedsRows) => return Err(NeedsRows.into()),
    };
    let mut stored = Vec::with_capacity(keys.len());
    for key in &keys {
        match rows.read(SYNC_QUARANTINE_KEYSPACE, ByteView::from(key.clone())) {
            Ok(value) => stored.push(value.map(|value| value.len() as u64)),
            Err(NeedsRows) => {
                waiting = true;
                stored.push(None);
            }
        }
    }
    if waiting {
        return Err(PlanStop::NeedsRows);
    }
    let quarantined_at_ms = rows.now_ms();
    let mut pending: BTreeMap<Vec<u8>, Row> = BTreeMap::new();
    for ((rejection, key), stored) in rejections.iter().zip(keys).zip(stored) {
        // A redelivered event swaps its pending bytes instead of adding to the usage total; an
        // earlier row of this batch swaps the same way.
        let replaced_bytes = match pending.get(&key) {
            Some((_, _, value)) => Some(value.len() as u64),
            None => stored,
        };
        match build_quarantine_entries(
            SyncQuarantineInput {
                identity: rejection.identity,
                evidence: rejection.evidence.clone(),
                reason: &rejection.reason,
                quarantined_at_ms,
                replaced_bytes,
            },
            usage,
            SyncQuarantineCapacity::default(),
        ) {
            Ok(write) => {
                usage = write.usage;
                pending.insert(key, write.row);
            }
            Err(SyncQuarantineError::AtCapacity { .. }) => {
                warn!(
                    topic_id = %rejection.identity.topic,
                    actor_id = %rejection.identity.actor,
                    actor_seq = rejection.identity.actor_seq,
                    records = usage.records,
                    bytes = usage.bytes,
                    "Holding the sync cursor: the quarantine store is at capacity"
                );
                return Ok(None);
            }
            Err(error) => return Err(invalid(error).into()),
        }
    }
    let mut entries = pending.into_values().collect::<Vec<_>>();
    entries.push(quarantine_usage_entry(usage).map_err(invalid)?);
    Ok(Some(entries))
}

/// Writes rejection evidence and the topic cursor together, so the cursor never moves past
/// evidence that is not durable. `false` means the store is full and nothing was written.
#[derive(Clone, Debug, PartialEq)]
pub struct CursorEvidencePlan {
    pub rejections: Vec<SyncRejection>,
    pub cursor: Row,
}

impl RowPlan for CursorEvidencePlan {
    type Output = bool;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<bool>> {
        let Some(mut writes) = quarantine_entries(rows, &self.rejections)? else {
            return Ok(Planned::output(false));
        };
        writes.push(self.cursor.clone());
        Ok(Planned::changes(true, Vec::new(), writes))
    }
}
