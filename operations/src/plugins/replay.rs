//! Replays kept journal rows for an install whose event coverage grew on this node: a new or
//! resumed install from its activation, a widened one from its widening, on the recording clock.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::NodeId;
use aruna_core::keyspaces::{EVENT_JOURNAL_KEYSPACE, PLUGIN_REPLAYS_KEYSPACE};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{
    EVENTS_QUEUED, GapReason, JOURNAL_RETENTION_MS, JOURNAL_ROW, PENDING_ROW, SourceEvent,
    SourceKind, journal_key,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::types::GroupId;
use aruna_plugins::trigger::TriggerKind;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::clock::{ClockReading, SKEW_MARGIN_MS, local_start};
use super::journal::JOURNAL_PAGE;
use super::lifecycle::held_covers;
use super::matcher::{Outbox, Target, targets};
use super::store::{StoreError, Txn, decode, encode, transact};
use crate::driver::DriverContext;
use crate::realm::mutate_placement::node_kind;

pub(crate) const COVERAGE_ROW: u8 = b'c';
const REPLAY_ROW: u8 = b'r';

type Coverage = BTreeMap<TriggerKind, BTreeSet<String>>;

/// What this node matched for an install, at which revision and activation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoverageRow {
    pub revision: u64,
    pub active_since_ms: Option<u64>,
    pub coverage: Coverage,
    /// Why the last replay started at the earliest kept row.
    pub note: Option<String>,
}

/// A replay in progress: the coverage it adds to `old`, the last row it read, and the first row
/// the matcher reads itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayRow {
    pub group_id: GroupId,
    pub next: CoverageRow,
    pub old: Coverage,
    pub after: Vec<u8>,
    pub until: Vec<u8>,
}

pub(crate) fn row_key(kind: u8, install_id: Ulid) -> Vec<u8> {
    [&[kind][..], &install_id.to_bytes()].concat()
}

async fn stored<T: serde::de::DeserializeOwned>(
    txn: &Txn<'_>,
    key: &[u8],
) -> Result<Option<T>, StoreError> {
    let bytes = txn.read(PLUGIN_REPLAYS_KEYSPACE, key).await?;
    bytes.map(|bytes| decode(&bytes)).transpose()
}

/// The coverage this node held before, the boundary the grown part starts at, and the node
/// whose clock it uses; `None` when nothing grew since the coverage row.
fn growth(
    install: &PluginInstall,
    row: Option<&CoverageRow>,
    coverage: &Coverage,
) -> Option<(Coverage, u64, Option<NodeId>)> {
    let since = install.active_since_ms?;
    let Some(row) = row.filter(|row| row.active_since_ms == install.active_since_ms) else {
        return Some((Coverage::new(), since, install.activated_by));
    };
    let grown = (coverage.iter()).any(|(kind, patterns)| {
        let held = row.coverage.get(kind);
        (patterns.iter()).any(|pattern| held.is_none_or(|held| !held_covers(held, pattern)))
    });
    if row.revision == install.revision || !grown {
        return None;
    }
    // A skipped revision may have grown the coverage earlier than the last widening.
    match &install.widened {
        Some(widened) if widened.extends == row.revision => {
            Some((row.coverage.clone(), widened.at_ms, Some(widened.by)))
        }
        _ => Some((row.coverage.clone(), since, install.activated_by)),
    }
}

/// Starts a replay for each active install whose coverage grew here since its coverage row;
/// `probe` reads another node's clock. Returns how many replays started.
pub async fn plan_replays<F: Future<Output = Option<ClockReading>>>(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    now_ms: u64,
    probe: impl Fn(NodeId) -> F,
) -> Result<usize, StoreError> {
    if matches!(node_kind(config, local), Some(RealmNodeKind::User { .. })) {
        return Ok(0);
    }
    let txn = Txn::direct(ctx);
    let mut started = 0;
    for target in targets(config) {
        let id = target.install_id;
        let Some(install) = config.installs.iter().find(|i| i.install_id == id) else {
            continue;
        };
        let row: Option<CoverageRow> = stored(&txn, &row_key(COVERAGE_ROW, id)).await?;
        let task: Option<ReplayRow> = stored(&txn, &row_key(REPLAY_ROW, id)).await?;
        let next = CoverageRow {
            revision: install.revision,
            active_since_ms: install.active_since_ms,
            coverage: target.coverage.clone(),
            note: None,
        };
        if task.is_some_and(|task| task.next.revision == next.revision) {
            continue;
        }
        let Some((old, boundary, by)) = growth(install, row.as_ref(), &target.coverage) else {
            if row
                .as_ref()
                .is_some_and(|row| row.revision != next.revision)
            {
                let rows = vec![(row_key(COVERAGE_ROW, id), encode(&next)?)];
                txn.write(PLUGIN_REPLAYS_KEYSPACE, rows).await?;
            }
            continue;
        };
        let start = match by {
            Some(node) if node == local => Some(boundary.saturating_sub(SKEW_MARGIN_MS)),
            Some(node) => local_start(boundary, probe(node).await),
            None => None,
        };
        let task = start_replay(&txn, &target, next, old, (start, by), now_ms).await?;
        let rows = vec![(row_key(REPLAY_ROW, id), encode(&task)?)];
        txn.write(PLUGIN_REPLAYS_KEYSPACE, rows).await?;
        started += 1;
    }
    Ok(started)
}

/// The replay row of a grown coverage, counting a gap per grown kind when the journal was
/// already pruned past the start.
async fn start_replay(
    txn: &Txn<'_>,
    target: &Target,
    mut next: CoverageRow,
    old: Coverage,
    (start, by): (Option<u64>, Option<NodeId>),
    now_ms: u64,
) -> Result<ReplayRow, StoreError> {
    let kept_from = now_ms.saturating_sub(JOURNAL_RETENTION_MS);
    if start.is_some_and(|start| start < kept_from) {
        let mut outbox = Outbox::new(now_ms);
        for kind in [
            SourceKind::DatasetCreated,
            SourceKind::DataUploaded,
            SourceKind::DatasetUpdated,
            SourceKind::SyncCompleted,
            SourceKind::SyncFailed,
        ] {
            let trigger = super::dispatch::event_trigger(kind);
            if target.coverage.contains_key(&trigger)
                && old.get(&trigger) != target.coverage.get(&trigger)
            {
                // The journal no longer shows how many events the pruned part held.
                let range = (start.unwrap_or_default(), kept_from);
                let id = target.install_id;
                outbox
                    .lost_range(txn, id, (kind, GapReason::Pruned), range)
                    .await?;
            }
        }
        outbox.commit(txn).await?;
    }
    if start.is_none() {
        let node = by.map_or_else(|| "unknown".to_string(), |node| node.to_string());
        let note = format!("the clock of node {node} was not readable; replayed every kept event");
        next.note = Some(note);
    }
    let (pending, _) = (txn.page(EVENT_JOURNAL_KEYSPACE, &[PENDING_ROW], None, 1)).await?;
    let until = match pending.first() {
        Some((key, _)) => [&[JOURNAL_ROW][..], &key[1..]].concat(),
        None => journal_key(JOURNAL_ROW, now_ms.saturating_add(1), Ulid::nil()),
    };
    Ok(ReplayRow {
        group_id: target.group_id,
        next,
        old,
        after: journal_key(JOURNAL_ROW, start.unwrap_or_default(), Ulid::nil()),
        until,
    })
}

/// Continues every replay to its end, a page per transaction; returns the rows read.
pub async fn run_replays(ctx: &DriverContext, now_ms: u64) -> Result<usize, StoreError> {
    replay_pages(ctx, now_ms, JOURNAL_PAGE, usize::MAX).await
}

/// At most `pages` pages of `page` rows of each replay.
pub(crate) async fn replay_pages(
    ctx: &DriverContext,
    now_ms: u64,
    page: usize,
    pages: usize,
) -> Result<usize, StoreError> {
    let tasks = (Txn::direct(ctx))
        .scan(PLUGIN_REPLAYS_KEYSPACE, &[REPLAY_ROW])
        .await?;
    let mut read = 0;
    for (key, _) in tasks {
        for _ in 0..pages {
            let step = transact(ctx, async |txn: &Txn<'_>| {
                replay_page(txn, &key, now_ms, page).await
            });
            let (rows, done) = step.await?;
            read += rows;
            if done {
                break;
            }
        }
    }
    Ok(read)
}

/// One page of a replay: forward rows for the newly covered events it reads, then the replay
/// row continued, or the coverage row written once the replay reaches `until`.
async fn replay_page(
    txn: &Txn<'_>,
    key: &[u8],
    now_ms: u64,
    page: usize,
) -> Result<(usize, bool), StoreError> {
    let Some(bytes) = txn.read(PLUGIN_REPLAYS_KEYSPACE, key).await? else {
        return Ok((0, true));
    };
    let mut task: ReplayRow = decode(&bytes)?;
    let install_id = Ulid::from_bytes(key[1..].try_into().unwrap_or_default());
    let journal = txn.page(
        EVENT_JOURNAL_KEYSPACE,
        &[JOURNAL_ROW],
        Some(task.after.clone()),
        page,
    );
    let (rows, next) = journal.await?;
    let target = |coverage: &Coverage| Target {
        install_id,
        group_id: task.group_id,
        revision: task.next.revision,
        coverage: coverage.clone(),
    };
    let (new, old) = (target(&task.next.coverage), target(&task.old));
    let mut outbox = Outbox::new(now_ms);
    let mut read = 0;
    for (row, value) in rows.iter().take_while(|(row, _)| *row < task.until) {
        read += 1;
        task.after = row.clone();
        let Ok(event) = SourceEvent::from_bytes(value) else {
            continue;
        };
        if new.matches(&event) && !old.matches(&event) {
            let stale = event.journaled_ms < now_ms.saturating_sub(JOURNAL_RETENTION_MS);
            let install = (install_id, task.next.revision);
            outbox
                .offer(txn, install, row, &event, stale, EVENTS_QUEUED)
                .await?;
        }
    }
    outbox.commit(txn).await?;
    let done = next.is_none() || read < rows.len();
    let (space, coverage) = (PLUGIN_REPLAYS_KEYSPACE, row_key(COVERAGE_ROW, install_id));
    match done {
        true => {
            txn.write(space, vec![(coverage, encode(&task.next)?)])
                .await?;
            txn.delete(space, vec![key.to_vec()]).await?;
        }
        false => {
            txn.write(space, vec![(key.to_vec(), encode(&task)?)])
                .await?
        }
    }
    Ok((read, done))
}

#[cfg(test)]
#[path = "replay_tests.rs"]
mod tests;
