//! Matches this node's pending journal rows to the active installs with event triggers and
//! queues forward rows for their receivers, or counts a gap where a row cannot be queued.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::NodeId;
use aruna_core::keyspaces::{EVENT_JOURNAL_KEYSPACE, PLUGIN_GAPS_KEYSPACE, PLUGIN_OUTBOX_KEYSPACE};
use aruna_core::plugin_installs::InstallState;
use aruna_core::plugin_runs::{
    EVENTS_QUEUED, ForwardRow, GapReason, GapRow, JOURNAL_RETENTION_MS, JOURNAL_ROW, PENDING_ROW,
    SourceEvent, gap_key, outbox_key,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::types::GroupId;
use aruna_plugins::trigger::TriggerKind;
use ulid::Ulid;

use super::check::covers;
use super::dispatch::event_trigger;
use super::grants::parse_grants;
use super::install::{pick_version, read_manifest};
use super::journal::JOURNAL_PAGE;
use super::lifecycle::coverage;
use super::store::{StoreError, Txn, decode, encode, transact};
use crate::driver::DriverContext;
use crate::realm::mutate_placement::node_kind;

/// An active install with event triggers, as the matcher sees it.
pub struct Target {
    install_id: Ulid,
    group_id: GroupId,
    revision: u64,
    coverage: BTreeMap<TriggerKind, BTreeSet<String>>,
}

impl Target {
    /// Whether the install's trigger of the event's kind covers the event's path.
    fn matches(&self, event: &SourceEvent) -> bool {
        self.group_id == event.group_id
            && (self.coverage.get(&event_trigger(event.kind)))
                .is_some_and(|patterns| patterns.iter().any(|pattern| covers(pattern, &event.path)))
    }
}

/// The active installs whose manifest declares event triggers, with their coverage.
pub fn targets(config: &RealmConfigDocument) -> Vec<Target> {
    (config.installs.iter())
        .filter(|install| install.state == InstallState::Active)
        .filter_map(|install| {
            let plugin = pick_version(config, &install.plugin, Some(&install.digest)).ok()?;
            let manifest = read_manifest(&plugin).ok()?;
            let coverage = coverage(&manifest, &parse_grants(&install.grants));
            (!coverage.is_empty()).then_some(Target {
                install_id: install.install_id,
                group_id: install.group_id,
                revision: install.revision,
                coverage,
            })
        })
        .collect()
}

/// Matches every pending row of this node; returns how many rows were read. A user node keeps
/// no journal: its rows are deleted unmatched.
pub async fn match_pending(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    now_ms: u64,
) -> Result<usize, StoreError> {
    match_bounded(ctx, config, local, now_ms, EVENTS_QUEUED).await
}

pub(crate) async fn match_bounded(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    now_ms: u64,
    bound: usize,
) -> Result<usize, StoreError> {
    let (first, _) = (Txn::direct(ctx))
        .page(EVENT_JOURNAL_KEYSPACE, &[PENDING_ROW], None, 1)
        .await?;
    if first.is_empty() {
        return Ok(0);
    }
    let keep = !matches!(node_kind(config, local), Some(RealmNodeKind::User { .. }));
    let targets = match keep {
        true => targets(config),
        false => Vec::new(),
    };
    let mut read = 0;
    loop {
        let page = transact(ctx, async |txn: &Txn<'_>| {
            match_page(txn, &targets, keep, now_ms, bound).await
        })
        .await?;
        read += page;
        if page < JOURNAL_PAGE {
            return Ok(read);
        }
    }
}

/// One page of pending rows in one transaction: the forward rows and gaps of the targets each
/// row matches, and the pending rows deleted.
async fn match_page(
    txn: &Txn<'_>,
    targets: &[Target],
    keep: bool,
    now_ms: u64,
    bound: usize,
) -> Result<usize, StoreError> {
    let (rows, _) = (txn.page(EVENT_JOURNAL_KEYSPACE, &[PENDING_ROW], None, JOURNAL_PAGE)).await?;
    let stale = now_ms.saturating_sub(JOURNAL_RETENTION_MS);
    let (mut done, mut forward) = (Vec::new(), Vec::new());
    let mut queued: BTreeMap<Ulid, usize> = BTreeMap::new();
    let mut gaps: BTreeMap<Vec<u8>, GapRow> = BTreeMap::new();
    for (key, value) in &rows {
        done.push(key.clone());
        if !keep {
            done.push([&[JOURNAL_ROW][..], &key[1..]].concat());
            continue;
        }
        let Ok(event) = SourceEvent::from_bytes(value) else {
            continue;
        };
        for target in targets.iter().filter(|target| target.matches(&event)) {
            let id = target.install_id;
            let count = match queued.get(&id) {
                Some(count) => *count,
                None => (txn
                    .page(PLUGIN_OUTBOX_KEYSPACE, &id.to_bytes(), None, bound)
                    .await?)
                    .0
                    .len(),
            };
            let reason = match () {
                () if event.journaled_ms < stale => GapReason::Pruned,
                () if count >= bound => GapReason::Full,
                () => {
                    let row = ForwardRow {
                        install_id: id,
                        revision: target.revision,
                        event: event.clone(),
                    };
                    forward.push((outbox_key(id, key), encode(&row)?));
                    queued.insert(id, count + 1);
                    continue;
                }
            };
            queued.insert(id, count);
            count_gap(txn, &mut gaps, id, &event, reason).await?;
        }
    }
    txn.write(PLUGIN_OUTBOX_KEYSPACE, forward).await?;
    let gaps = (gaps.into_iter())
        .map(|(key, row)| Ok((key, encode(&row)?)))
        .collect::<Result<_, StoreError>>()?;
    txn.write(PLUGIN_GAPS_KEYSPACE, gaps).await?;
    txn.delete(EVENT_JOURNAL_KEYSPACE, done).await?;
    Ok(rows.len())
}

/// Counts `event` in the install's gap row of its kind and `reason`.
async fn count_gap(
    txn: &Txn<'_>,
    gaps: &mut BTreeMap<Vec<u8>, GapRow>,
    install_id: Ulid,
    event: &SourceEvent,
    reason: GapReason,
) -> Result<(), StoreError> {
    let key = gap_key(install_id, event.kind, reason);
    if !gaps.contains_key(&key) {
        let stored = match txn.read(PLUGIN_GAPS_KEYSPACE, &key).await? {
            Some(bytes) => decode(&bytes)?,
            None => GapRow::new(event.kind, reason),
        };
        gaps.insert(key.clone(), stored);
    }
    if let Some(row) = gaps.get_mut(&key) {
        row.add(event.anchor_ms);
    }
    Ok(())
}

#[cfg(test)]
#[path = "matcher_tests.rs"]
mod tests;
