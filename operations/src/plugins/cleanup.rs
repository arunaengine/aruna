//! Deletes plugin event rows that are no longer needed: gap counts past the dedup retention on
//! senders and receivers, and the replay, waiting and gap rows of removed installs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::{
    PLUGIN_GAPS_KEYSPACE, PLUGIN_QUEUE_KEYSPACE, PLUGIN_REPLAYS_KEYSPACE, PLUGIN_WAITING_KEYSPACE,
};
use aruna_core::plugin_installs::InstallState;
use aruna_core::plugin_runs::{DEDUP_RETENTION_MS, GapRow, RECEIVER_GAP};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use ulid::Ulid;

use super::store::{StoreError, Txn, decode};
use crate::driver::DriverContext;

/// How a row's install stands in the realm config.
#[derive(PartialEq)]
enum Standing {
    Kept,
    Removing,
    Absent,
}

fn standing(config: &RealmConfigDocument, install_id: Ulid) -> Standing {
    match config.installs.iter().find(|i| i.install_id == install_id) {
        Some(install) if install.state == InstallState::Removing => Standing::Removing,
        Some(_) => Standing::Kept,
        None => Standing::Absent,
    }
}

fn install_at(key: &[u8], at: usize) -> Ulid {
    Ulid::from_bytes(
        key.get(at..at + 16)
            .and_then(|id| id.try_into().ok())
            .unwrap_or_default(),
    )
}

/// Deletes the rows this node no longer needs; returns how many. Only this node matched the
/// installs of its replay rows, so an absent install was removed; a receiver may not have an
/// install record yet, so its waiting and reported rows go only while the install is removing.
pub async fn sweep_events(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    now_ms: u64,
) -> Result<usize, StoreError> {
    let txn = Txn::direct(ctx);
    let mut dropped = Vec::new();
    for (key, _) in txn.scan(PLUGIN_REPLAYS_KEYSPACE, &[]).await? {
        if standing(config, install_at(&key, 1)) != Standing::Kept {
            dropped.push(key);
        }
    }
    let replays = dropped.len();
    txn.delete(PLUGIN_REPLAYS_KEYSPACE, dropped).await?;
    let mut waiting = Vec::new();
    for (key, _) in txn.scan(PLUGIN_WAITING_KEYSPACE, &[]).await? {
        if standing(config, install_at(&key, 0)) == Standing::Removing {
            waiting.push(key);
        }
    }
    let held = waiting.len();
    txn.delete(PLUGIN_QUEUE_KEYSPACE, waiting.clone()).await?;
    txn.delete(PLUGIN_WAITING_KEYSPACE, waiting).await?;
    let mut gaps = Vec::new();
    for (key, value) in txn.scan(PLUGIN_GAPS_KEYSPACE, &[]).await? {
        let expired = decode::<GapRow>(&value)
            .is_ok_and(|row| now_ms > row.latest_ms.saturating_add(DEDUP_RETENTION_MS));
        // A sender row starts with the install id, a receiver row with the sender's 32 bytes.
        let at = match key.first() == Some(&RECEIVER_GAP) {
            true => 33,
            false => 1,
        };
        if expired || standing(config, install_at(&key, at)) == Standing::Removing {
            gaps.push(key);
        }
    }
    let counted = gaps.len();
    txn.delete(PLUGIN_GAPS_KEYSPACE, gaps).await?;
    Ok(replays + held + counted)
}

#[cfg(test)]
#[path = "cleanup_tests.rs"]
mod tests;
