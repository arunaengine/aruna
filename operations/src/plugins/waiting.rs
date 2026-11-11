//! Rechecks the events waiting on this receiver: a ready one becomes a queued run in the slot
//! it held, or is denied; one past its waiting expiry ends `expired` with a gap.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::keyspaces::{PLUGIN_GAPS_KEYSPACE, PLUGIN_QUEUE_KEYSPACE, PLUGIN_WAITING_KEYSPACE};
use aruna_core::plugin_runs::{GapReason, GapRow, WAITING_EXPIRY_MS, reported_key};
use aruna_core::structs::identity::realm::RealmConfigDocument;

use super::decide::{Verdict, WaitingRow, judge, record};
use super::forward::Outcome;
use super::runs::{ADMISSIONS, RunError, admit_in};
use super::store::{StoreError, Txn, decode, encode, transact};
use crate::driver::DriverContext;

/// Decides each waiting event that is ready or expired; returns how many left the waiting set.
pub async fn recheck_waiting(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    now_ms: u64,
) -> Result<usize, StoreError> {
    let _admitting = ADMISSIONS.lock().await;
    let rows = Txn::direct(ctx).scan(PLUGIN_WAITING_KEYSPACE, &[]).await?;
    let mut left = 0;
    for (key, value) in rows {
        let waiting: WaitingRow = decode(&value)?;
        let expired = now_ms > waiting.received_ms.saturating_add(WAITING_EXPIRY_MS);
        let verdict = match expired {
            true => None,
            false => match judge(ctx, config, local, &waiting.row, now_ms).await? {
                Verdict::Unready | Verdict::Unavailable => continue,
                verdict => Some(verdict),
            },
        };
        // The slot is freed and taken by the run in one transaction, so it never counts twice.
        transact(ctx, async |txn: &Txn<'_>| {
            txn.delete(PLUGIN_WAITING_KEYSPACE, vec![key.clone()])
                .await?;
            txn.delete(PLUGIN_QUEUE_KEYSPACE, vec![key.clone()]).await?;
            let outcome = match &verdict {
                Some(Verdict::Run(run)) => match admit_in(txn, run).await {
                    Ok(()) => Outcome::Queued,
                    Err(RunError::Store(error)) => return Err(error),
                    Err(error) => return Err(StoreError(error.to_string())),
                },
                Some(Verdict::Denied(reason)) => Outcome::Denied(reason.clone()),
                _ => {
                    expire(txn, local, &waiting).await?;
                    Outcome::Expired
                }
            };
            record(txn, &waiting.row, &outcome, now_ms).await
        })
        .await?;
        left += 1;
    }
    Ok(left)
}

/// Counts an expired waiting event in this receiver's own gap row.
async fn expire(txn: &Txn<'_>, local: NodeId, waiting: &WaitingRow) -> Result<(), StoreError> {
    let (install_id, kind) = (waiting.row.install_id, waiting.row.event.kind);
    let key = reported_key(local, install_id, kind, GapReason::Waited);
    let mut row = match txn.read(PLUGIN_GAPS_KEYSPACE, &key).await? {
        Some(bytes) => decode(&bytes)?,
        None => GapRow::new(kind, GapReason::Waited),
    };
    row.add(waiting.row.event.anchor_ms);
    txn.write(PLUGIN_GAPS_KEYSPACE, vec![(key, encode(&row)?)])
        .await
}

#[cfg(test)]
#[path = "waiting_tests.rs"]
mod tests;
