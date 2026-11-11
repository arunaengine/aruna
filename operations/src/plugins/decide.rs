//! Decides forwarded plugin events on the install's receiver, once per install, trigger and
//! source id: a run is queued, the event waits for its install record, or it is denied.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::keyspaces::{
    PLUGIN_GAPS_KEYSPACE, PLUGIN_QUEUE_KEYSPACE, PLUGIN_SEEN_KEYSPACE, PLUGIN_WAITING_KEYSPACE,
};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{
    ForwardRow, GapReason, GapRow, PluginRun, RUNS_QUEUED, SourceKind, WAITING_ROW, queue_key,
    reported_key,
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

/// What a decided event writes besides its dedup record.
enum Admission {
    Run(Box<PluginRun>),
    Wait,
    Refuse,
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
    let install = (config.installs.iter()).find(|i| i.install_id == row.install_id);
    let ready = match install {
        Some(install) if install.revision >= row.revision => {
            prerequisites_held(ctx, install).await.map_err(StoreError)?
        }
        _ => false,
    };
    let (outcome, admission) = match install.filter(|_| ready) {
        None => (Outcome::Waiting, Admission::Wait),
        Some(install) => match trigger_check(ctx, config, local, install, row, now_ms).await {
            Ok(run) => (Outcome::Queued, Admission::Run(Box::new(run))),
            Err(error) if error.kind == ErrorKind::Unavailable => return Ok(Outcome::Busy),
            Err(error) => (Outcome::Denied(error.message), Admission::Refuse),
        },
    };
    transact(ctx, async |txn: &Txn<'_>| {
        let full = match &admission {
            Admission::Run(run) => match admit_in(txn, run).await {
                Err(RunError::Full(_)) => true,
                Err(RunError::Store(error)) => return Err(error),
                Ok(()) | Err(RunError::NotFound | RunError::NotWaiting(_)) => false,
            },
            Admission::Wait => !hold(txn, row, now_ms).await?,
            Admission::Refuse => false,
        };
        if full {
            return Ok(Outcome::Busy);
        }
        let record = SeenRow {
            outcome: outcome.clone(),
            anchor_ms: row.event.anchor_ms,
            decided_ms: now_ms,
        };
        txn.write(PLUGIN_SEEN_KEYSPACE, vec![(seen.clone(), encode(&record)?)])
            .await?;
        Ok(outcome)
    })
    .await
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

#[cfg(test)]
#[path = "decide_tests.rs"]
mod tests;
