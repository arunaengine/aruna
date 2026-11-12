//! Deletes rows that outlived their purpose: secret use audit rows with their index rows after
//! their retention, and webhook delivery ids after theirs or once their install is removing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::{ADMIN_AUDIT_KEYSPACE, PLUGIN_HOOKS_KEYSPACE};
use aruna_core::plugin_installs::InstallState;
use aruna_core::plugin_runs::{HOOK_RETENTION_MS, HOOK_ROW, HOOK_TIME_ROW};
use aruna_core::structs::identity::admin_audit::{AdminAuditRecord, AuditIndex, audit_prefix};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use ulid::Ulid;

use super::store::{StoreError, Txn, decode, transact};
use crate::driver::DriverContext;

/// How long a node keeps the audit rows of the secret uses it served, like finished runs.
pub const USE_AUDIT_RETENTION: u64 = 30 * 24 * 3600 * 1000;
const PAGE: usize = 1_000;
/// The realm audit key the use prune checked last, kept in the webhook keyspace. Every older
/// row was pruned or is kept for good, so the prune goes on from there after a restart.
const AUDIT_CURSOR: &[u8] = b"c";

/// Deletes the use audit rows past their retention; answers how many records went.
pub async fn prune_uses(ctx: &DriverContext, now_ms: u64) -> Result<usize, StoreError> {
    let mut pruned = 0;
    loop {
        let (count, more) = prune_page(ctx, now_ms, PAGE).await?;
        pruned += count;
        if !more {
            return Ok(pruned);
        }
    }
}

/// One page of the use prune, written with its cursor in one transaction; answers how many
/// records went and whether older rows may follow.
async fn prune_page(
    ctx: &DriverContext,
    now_ms: u64,
    page: usize,
) -> Result<(usize, bool), StoreError> {
    let cutoff = now_ms.saturating_sub(USE_AUDIT_RETENTION);
    let prefix = audit_prefix(AuditIndex::Realm);
    transact(ctx, async |txn: &Txn<'_>| {
        let after = txn.read(PLUGIN_HOOKS_KEYSPACE, AUDIT_CURSOR).await?;
        let (rows, next) = (txn.page(ADMIN_AUDIT_KEYSPACE, &prefix, after, page)).await?;
        let (mut dropped, mut count, mut checked, mut young) = (Vec::new(), 0, None, false);
        for (key, value) in rows {
            let id = key
                .get(prefix.len()..)
                .and_then(|id| <[u8; 16]>::try_from(id).ok());
            if id.is_some_and(|id| Ulid::from_bytes(id).timestamp_ms() >= cutoff) {
                young = true;
                break;
            }
            let record = AdminAuditRecord::from_bytes(&value).ok();
            let record = record.filter(|record| record.secret_use.is_some());
            if let Some(entries) = record.and_then(|record| record.entries().ok()) {
                dropped.extend(entries.into_iter().map(|(_, key, _)| key.to_vec()));
                count += 1;
            }
            checked = Some(key);
        }
        txn.delete(ADMIN_AUDIT_KEYSPACE, dropped).await?;
        if let Some(key) = checked {
            let cursor = vec![(AUDIT_CURSOR.to_vec(), key)];
            txn.write(PLUGIN_HOOKS_KEYSPACE, cursor).await?;
        }
        Ok((count, !young && next.is_some()))
    })
    .await
}

/// Deletes the webhook delivery ids past their retention, and every one of a removing
/// install; answers how many went.
pub async fn prune_hooks(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    now_ms: u64,
) -> Result<usize, StoreError> {
    let cutoff = now_ms.saturating_sub(HOOK_RETENTION_MS);
    let txn = Txn::direct(ctx);
    let mut pruned = 0;
    loop {
        let (rows, next) = (txn.page(PLUGIN_HOOKS_KEYSPACE, &[HOOK_TIME_ROW], None, PAGE)).await?;
        let total = rows.len();
        let old: Vec<Vec<u8>> = (rows.into_iter())
            .map(|(key, _)| key)
            .take_while(|key| received(key).is_none_or(|at| at < cutoff))
            .collect();
        let ids = (old.iter()).filter_map(|key| Some([&[HOOK_ROW][..], key.get(9..)?].concat()));
        let ids = ids.collect();
        pruned += old.len();
        let done = old.len() < total || next.is_none();
        remove(ctx, ids, old).await?;
        if done {
            break;
        }
    }
    let removing =
        (config.installs.iter()).filter(|install| install.state == InstallState::Removing);
    for install in removing {
        let prefix = [&[HOOK_ROW][..], &install.install_id.to_bytes()].concat();
        let (mut ids, mut times) = (Vec::new(), Vec::new());
        for (key, value) in txn.scan(PLUGIN_HOOKS_KEYSPACE, &prefix).await? {
            if let (Ok(at), Some(rest)) = (decode::<u64>(&value), key.get(1..)) {
                times.push([&[HOOK_TIME_ROW][..], &at.to_be_bytes(), rest].concat());
            }
            ids.push(key);
        }
        pruned += ids.len();
        remove(ctx, ids, times).await?;
    }
    Ok(pruned)
}

/// The receipt time of a time row.
fn received(key: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(key.get(1..9)?.try_into().ok()?))
}

async fn remove(
    ctx: &DriverContext,
    ids: Vec<Vec<u8>>,
    times: Vec<Vec<u8>>,
) -> Result<(), StoreError> {
    transact(ctx, async |txn: &Txn<'_>| {
        txn.delete(PLUGIN_HOOKS_KEYSPACE, ids).await?;
        txn.delete(PLUGIN_HOOKS_KEYSPACE, times).await
    })
    .await
}

#[cfg(test)]
#[path = "prune_tests.rs"]
mod tests;
