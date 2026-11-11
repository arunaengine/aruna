//! Decides forwarded plugin events on the install's receiver, once per install, trigger and
//! source id: a run is queued, the event waits for its install record, or it is denied.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use std::collections::BTreeMap;

use aruna_core::keyspaces::{
    PLUGIN_GAPS_KEYSPACE, PLUGIN_QUEUE_KEYSPACE, PLUGIN_REPLAYS_KEYSPACE, PLUGIN_SEEN_KEYSPACE,
    PLUGIN_WAITING_KEYSPACE,
};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{
    DEDUP_RETENTION_MS, ForwardRow, GapReason, GapRow, PluginRun, RECEIVER_GAP, RUNS_QUEUED,
    SourceKind, WAITING_ROW, queue_key, reported_key,
};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::time::unix_timestamp_millis;
use aruna_plugins::broker::{ErrorKind, HostError};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::account::prerequisites_held;
use super::artifacts::local_config;
use super::check::{RunScope, check_call};
use super::dispatch::{event_run, event_trigger};
use super::forward::{Decisions, EventBatch, Outcome};
use super::replay::{COVERAGE_ROW, CoverageRow, row_key};
use super::runs::{ADMISSIONS, RunError, admit_in};
use super::store::{StoreError, Txn, decode, encode, transact};
use crate::driver::DriverContext;
use crate::metadata::protocol::MetadataReadError;
use crate::realm::mutate_placement::node_kind;

/// The decision a resend of an event gets, with its anchor for the dedup retention.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeenRow {
    pub outcome: Outcome,
    pub anchor_ms: u64,
    pub decided_ms: u64,
}

/// An event that waits on this receiver for its install record or a prerequisite write.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitingRow {
    pub row: ForwardRow,
    pub received_ms: u64,
}

/// Whether an event can be decided now, and how.
pub(crate) enum Verdict {
    /// The install record or a prerequisite write is not here yet.
    Unready,
    Run(Box<PluginRun>),
    Denied(String),
    /// The check could not read what it needs; nothing is decided.
    Unavailable,
}

/// Key of an event's dedup record: install, trigger kind, then the source id.
pub fn seen_key(install_id: Ulid, kind: SourceKind, source: &str) -> Vec<u8> {
    let mut key = install_id.to_bytes().to_vec();
    key.push(kind as u8);
    key.extend_from_slice(source.as_bytes());
    key
}

/// The receiver side of a forward from an infrastructure peer.
pub(crate) async fn serve_events(
    ctx: &DriverContext,
    peer: NodeId,
    batch: EventBatch,
) -> Result<Decisions, MetadataReadError> {
    let (config, local) = local_config(ctx).await?;
    if !node_kind(&config, peer).is_some_and(|kind| kind.is_sync_eligible()) {
        return Err(MetadataReadError::Forbidden);
    }
    let decided = decide_batch(ctx, &config, (local, peer), batch, unix_timestamp_millis());
    decided.await.map_err(|_| MetadataReadError::Unavailable)
}

/// Decides each event of a batch from `sender` and keeps its newest gap reports.
pub async fn decide_batch(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    (local, sender): (NodeId, NodeId),
    batch: EventBatch,
    now_ms: u64,
) -> Result<Decisions, StoreError> {
    let _admitting = ADMISSIONS.lock().await;
    let mut decisions = Decisions::default();
    for row in batch.events {
        let outcome = decide(ctx, config, local, &row, now_ms).await?;
        decisions
            .events
            .push((row.install_id, row.event.source, outcome));
    }
    decisions.gaps = keep_reports(ctx, sender, batch.gaps).await?;
    Ok(decisions)
}

/// One event's outcome. Busy writes nothing, so the sender keeps the row and sends it again.
async fn decide(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    row: &ForwardRow,
    now_ms: u64,
) -> Result<Outcome, StoreError> {
    let seen = seen_key(row.install_id, row.event.kind, &row.event.source);
    if let Some(bytes) = Txn::direct(ctx).read(PLUGIN_SEEN_KEYSPACE, &seen).await? {
        return Ok(decode::<SeenRow>(&bytes)?.outcome);
    }
    // Its dedup record would be gone, so a run now could repeat one already made.
    if now_ms > row.event.anchor_ms.saturating_add(DEDUP_RETENTION_MS) {
        let late = async |txn: &Txn<'_>| local_gap(txn, local, row, GapReason::Late).await;
        transact(ctx, late).await?;
        return Ok(Outcome::Expired);
    }
    let verdict = judge(ctx, config, local, row, now_ms).await?;
    transact(ctx, async |txn: &Txn<'_>| {
        let outcome = match &verdict {
            Verdict::Unavailable => return Ok(Outcome::Busy),
            Verdict::Run(run) => match admit_in(txn, run).await {
                Err(RunError::Full(_)) => return Ok(Outcome::Busy),
                Err(RunError::Store(error)) => return Err(error),
                Ok(()) | Err(RunError::NotFound | RunError::NotWaiting(_)) => Outcome::Queued,
            },
            Verdict::Unready if !hold(txn, row, now_ms).await? => return Ok(Outcome::Busy),
            Verdict::Unready => Outcome::Waiting,
            Verdict::Denied(reason) => Outcome::Denied(reason.clone()),
        };
        record(txn, row, &outcome, now_ms).await?;
        Ok(outcome)
    })
    .await
}

/// Counts an event this receiver did not run in its own gap row of `reason`.
pub(crate) async fn local_gap(
    txn: &Txn<'_>,
    local: NodeId,
    row: &ForwardRow,
    reason: GapReason,
) -> Result<(), StoreError> {
    let (install_id, kind) = (row.install_id, row.event.kind);
    let key = reported_key(local, install_id, kind, reason);
    let mut gap = match txn.read(PLUGIN_GAPS_KEYSPACE, &key).await? {
        Some(bytes) => decode(&bytes)?,
        None => GapRow::new(kind, reason),
    };
    gap.add(row.event.anchor_ms);
    txn.write(PLUGIN_GAPS_KEYSPACE, vec![(key, encode(&gap)?)])
        .await
}

/// Deletes the dedup records past their retention; returns how many.
pub async fn prune_seen(ctx: &DriverContext, now_ms: u64) -> Result<usize, StoreError> {
    let txn = Txn::direct(ctx);
    let (mut after, mut pruned) = (None, 0);
    loop {
        let (rows, next) = txn.page(PLUGIN_SEEN_KEYSPACE, &[], after, 1_000).await?;
        let old: Vec<Vec<u8>> = (rows.into_iter())
            .filter(|(_, value)| {
                decode::<SeenRow>(value)
                    .is_ok_and(|seen| now_ms > seen.anchor_ms.saturating_add(DEDUP_RETENTION_MS))
            })
            .map(|(key, _)| key)
            .collect();
        pruned += old.len();
        txn.delete(PLUGIN_SEEN_KEYSPACE, old).await?;
        match next {
            Some(key) => after = Some(key),
            None => return Ok(pruned),
        }
    }
}

/// Writes the event's dedup record with its outcome.
pub(crate) async fn record(
    txn: &Txn<'_>,
    row: &ForwardRow,
    outcome: &Outcome,
    now_ms: u64,
) -> Result<(), StoreError> {
    let key = seen_key(row.install_id, row.event.kind, &row.event.source);
    let record = SeenRow {
        outcome: outcome.clone(),
        anchor_ms: row.event.anchor_ms,
        decided_ms: now_ms,
    };
    txn.write(PLUGIN_SEEN_KEYSPACE, vec![(key, encode(&record)?)])
        .await
}

/// Whether the local install record and its prerequisite writes are here, then the check.
pub(crate) async fn judge(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    row: &ForwardRow,
    now_ms: u64,
) -> Result<Verdict, StoreError> {
    let install = (config.installs.iter()).find(|i| i.install_id == row.install_id);
    let Some(install) = install.filter(|install| install.revision >= row.revision) else {
        return Ok(Verdict::Unready);
    };
    if !prerequisites_held(ctx, install).await.map_err(StoreError)? {
        return Ok(Verdict::Unready);
    }
    Ok(
        match trigger_check(ctx, config, local, install, row, now_ms).await {
            Ok(run) => Verdict::Run(Box::new(run)),
            Err(error) if error.kind == ErrorKind::Unavailable => Verdict::Unavailable,
            Err(error) => Verdict::Denied(error.message),
        },
    )
}

/// Steps 0 to 4 for the event's path as a new run of the install would pass them.
async fn trigger_check(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    install: &PluginInstall,
    row: &ForwardRow,
    now_ms: u64,
) -> Result<PluginRun, HostError> {
    let none = || HostError::new(ErrorKind::Denied, "the install has no trigger of this kind");
    let run = event_run(config, install, &row.event, now_ms).ok_or_else(none)?;
    let scope = RunScope {
        realm_id: config.realm_id,
        local,
        run,
    };
    let needs = event_trigger(row.event.kind).needs();
    let target = needs.map(|permission| (permission, row.event.path.as_str()));
    check_call(ctx, &scope, target).await?;
    Ok(scope.run)
}

/// Takes a queue slot for a waiting event; `false` when the install's queue is full.
async fn hold(txn: &Txn<'_>, row: &ForwardRow, now_ms: u64) -> Result<bool, StoreError> {
    let prefix = row.install_id.to_bytes();
    if txn.scan(PLUGIN_QUEUE_KEYSPACE, &prefix).await?.len() >= RUNS_QUEUED {
        return Ok(false);
    }
    let key = queue_key(row.install_id, Ulid::generate());
    let waiting = WaitingRow {
        row: row.clone(),
        received_ms: now_ms,
    };
    let slot = vec![(key.clone(), WAITING_ROW.to_vec())];
    txn.write(PLUGIN_QUEUE_KEYSPACE, slot).await?;
    txn.write(PLUGIN_WAITING_KEYSPACE, vec![(key, encode(&waiting)?)])
        .await?;
    Ok(true)
}

/// Keeps each report from `sender` that is newer than the one kept; answers the sequence kept.
async fn keep_reports(
    ctx: &DriverContext,
    sender: NodeId,
    reports: Vec<(Ulid, GapRow)>,
) -> Result<Vec<(Ulid, SourceKind, GapReason, u64)>, StoreError> {
    transact(ctx, async |txn: &Txn<'_>| {
        let mut acked = Vec::new();
        for (install_id, report) in reports {
            let key = reported_key(sender, install_id, report.kind, report.reason);
            let kept = txn.read(PLUGIN_GAPS_KEYSPACE, &key).await?;
            let kept = kept.map(|bytes| decode::<GapRow>(&bytes)).transpose()?;
            let sequence = kept.as_ref().map_or(0, |kept| kept.sequence);
            acked.push((
                install_id,
                report.kind,
                report.reason,
                sequence.max(report.sequence),
            ));
            if report.sequence > sequence {
                txn.write(PLUGIN_GAPS_KEYSPACE, vec![(key, encode(&report)?)])
                    .await?;
            }
        }
        Ok(acked)
    })
    .await
}

/// What the receiver holds about an install's event delivery.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Delivery {
    pub waiting: usize,
    pub oldest_ms: Option<u64>,
    /// Undelivered events by kind and reason, summed over the nodes that counted them.
    pub gaps: Vec<GapRow>,
    /// Why the last replay here started at the earliest kept event.
    pub note: Option<String>,
}

/// The install's waiting events, gap totals and replay note on this receiver.
pub async fn event_delivery(ctx: &DriverContext, install_id: Ulid) -> Result<Delivery, StoreError> {
    let txn = Txn::direct(ctx);
    let prefix = install_id.to_bytes();
    let waiting = txn.scan(PLUGIN_WAITING_KEYSPACE, &prefix).await?;
    let received = (waiting.iter())
        .filter_map(|(_, value)| decode::<WaitingRow>(value).ok())
        .map(|waiting| waiting.received_ms);
    let mut totals: BTreeMap<(SourceKind, GapReason), GapRow> = BTreeMap::new();
    for (key, value) in txn.scan(PLUGIN_GAPS_KEYSPACE, &[RECEIVER_GAP]).await? {
        if key.get(33..49) != Some(&prefix[..]) {
            continue;
        }
        let row: GapRow = decode(&value)?;
        let total = (totals.entry((row.kind, row.reason)))
            .or_insert_with(|| GapRow::new(row.kind, row.reason));
        total.count += row.count;
        total.unknown |= row.unknown;
        total.earliest_ms = total.earliest_ms.min(row.earliest_ms);
        total.latest_ms = total.latest_ms.max(row.latest_ms);
    }
    let coverage = row_key(COVERAGE_ROW, install_id);
    let coverage = txn.read(PLUGIN_REPLAYS_KEYSPACE, &coverage).await?;
    let coverage = coverage.and_then(|bytes| decode::<CoverageRow>(&bytes).ok());
    Ok(Delivery {
        waiting: waiting.len(),
        oldest_ms: received.min(),
        gaps: totals.into_values().collect(),
        note: coverage.and_then(|row| row.note),
    })
}

#[cfg(test)]
#[path = "decide_tests.rs"]
mod tests;
