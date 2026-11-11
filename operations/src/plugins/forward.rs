//! Sends this node's forward rows to each install's receiver, with the gap rows the receiver has
//! not acknowledged, and deletes the rows the receiver decided.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::keyspaces::{
    GRAPH_LIFECYCLE_KEYSPACE, MATERIALIZATION_STATUS_KEYSPACE, PLUGIN_GAPS_KEYSPACE,
    PLUGIN_OUTBOX_KEYSPACE,
};
use aruna_core::metadata::{
    GraphLifecycleRecord, MaterializationState, MaterializationStatusRecord,
};
use aruna_core::plugin_installs::InstallState;
use aruna_core::plugin_runs::{
    ForwardRow, GapReason, GapRow, SEND_EXPIRY_MS, SENDER_GAP, SourceKind, gap_key,
};
use aruna_core::storage_entries::{graph_lifecycle_key, materialization_status_key};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::matcher::Outbox;
use super::receiver::lost_reason;
use super::store::{StoreError, Txn, decode, encode, transact};
use crate::driver::DriverContext;

/// Forward rows of one install read for sending in one pass at most.
const BATCH: usize = 100;
/// Encoded row bytes of one request at most, well below the transport's 16 MiB message bound.
const REQUEST_BYTES: usize = 4 * 1024 * 1024;
const MAX_BACKOFF_MS: u64 = 10 * 60 * 1000;

/// Matched events and unacknowledged gap counts for one receiver.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventBatch {
    pub events: Vec<ForwardRow>,
    pub gaps: Vec<(Ulid, GapRow)>,
}

/// How the receiver decided one event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Queued,
    Waiting,
    Denied(String),
    Expired,
    /// The install's queue is full: the sender keeps the row and sends it again later.
    Busy,
}

/// The receiver's answer: an outcome per event by install and source id, and the gap sequence
/// it holds per install, kind and reason.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decisions {
    pub events: Vec<(Ulid, String, Outcome)>,
    pub gaps: Vec<(Ulid, SourceKind, GapReason, u64)>,
}

/// When each receiver is tried again after a failed or busy send.
#[derive(Debug, Default)]
pub struct Backoff(BTreeMap<NodeId, (u32, u64)>);

impl Backoff {
    fn ready(&self, node: NodeId, now_ms: u64) -> bool {
        self.0.get(&node).is_none_or(|(_, at)| *at <= now_ms)
    }

    fn failed(&mut self, node: NodeId, now_ms: u64) {
        let tries = self.0.get(&node).map_or(0, |(tries, _)| *tries) + 1;
        let wait = (1000u64 << tries.min(20)).min(MAX_BACKOFF_MS);
        self.0.insert(node, (tries, now_ms + wait));
    }
}

/// One request's batch and the outbox key of each event in it by install and source id.
type Pending = (EventBatch, BTreeMap<(Ulid, String), Vec<u8>>);

/// One receiver's requests, each with the encoded row bytes it holds.
#[derive(Default)]
struct Requests {
    bound: usize,
    open: Vec<(Pending, usize)>,
}

impl Requests {
    /// The request with room for `size` more bytes; a new one when the last is full. A row
    /// larger than the bound gets a request of its own.
    fn room(&mut self, size: usize) -> &mut Pending {
        let fits =
            (self.open.last()).is_some_and(|(_, used)| *used == 0 || used + size <= self.bound);
        if !fits {
            self.open.push((Pending::default(), 0));
        }
        let last = self.open.len() - 1;
        let (pending, used) = &mut self.open[last];
        *used += size;
        pending
    }
}

/// The requests to `node`, each holding at most `bound` encoded row bytes.
fn receiver_requests(
    batches: &mut BTreeMap<NodeId, Requests>,
    node: NodeId,
    bound: usize,
) -> &mut Requests {
    let requests = batches.entry(node).or_default();
    requests.bound = bound;
    requests
}

/// Sends each reachable receiver its batches and applies their decisions; returns how many
/// rows were decided. Rows of removed installs are dropped, rows past their send expiry become
/// gaps.
pub async fn send_outbox<F: Future<Output = Result<Decisions, String>>>(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    now_ms: u64,
    backoff: &mut Backoff,
    send: impl Fn(NodeId, EventBatch) -> F,
) -> Result<usize, StoreError> {
    send_bounded(ctx, config, (now_ms, REQUEST_BYTES), backoff, send).await
}

pub(crate) async fn send_bounded<F: Future<Output = Result<Decisions, String>>>(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    (now_ms, bound): (u64, usize),
    backoff: &mut Backoff,
    send: impl Fn(NodeId, EventBatch) -> F,
) -> Result<usize, StoreError> {
    let mut batches: BTreeMap<NodeId, Requests> = BTreeMap::new();
    let open = |node: NodeId| lost_reason(config, node).is_none() && backoff.ready(node, now_ms);
    let mut after = None;
    while let Some(install_id) = next_install(ctx, after).await? {
        after = Some(install_id);
        let install = (config.installs.iter()).find(|i| i.install_id == install_id);
        let Some(install) = install.filter(|i| i.state != InstallState::Removing) else {
            drop_rows(ctx, PLUGIN_OUTBOX_KEYSPACE, &install_id.to_bytes()).await?;
            continue;
        };
        let pending = open(install.receiver)
            .then(|| receiver_requests(&mut batches, install.receiver, bound));
        collect(ctx, install_id, now_ms, pending).await?;
    }
    let gaps = Txn::direct(ctx)
        .scan(PLUGIN_GAPS_KEYSPACE, &[SENDER_GAP])
        .await?;
    for (key, value) in gaps {
        let install_id = Ulid::from_bytes(key[1..17].try_into().unwrap_or_default());
        let install = (config.installs.iter()).find(|i| i.install_id == install_id);
        let Some(install) = install.filter(|i| i.state != InstallState::Removing) else {
            drop_rows(ctx, PLUGIN_GAPS_KEYSPACE, &key).await?;
            continue;
        };
        let row: GapRow = decode(&value)?;
        if row.sequence > row.acked && open(install.receiver) {
            let (batch, _) =
                receiver_requests(&mut batches, install.receiver, bound).room(value.len() + 16);
            batch.gaps.push((install_id, row));
        }
    }
    let mut decided = 0;
    for (receiver, requests) in batches {
        let mut failed = false;
        for ((batch, keys), _) in requests.open {
            let Ok(decisions) = send(receiver, batch).await else {
                failed = true;
                break;
            };
            let (count, busy) = apply(ctx, &keys, decisions).await?;
            decided += count;
            if busy {
                failed = true;
                break;
            }
        }
        match failed {
            true => backoff.failed(receiver, now_ms),
            false => _ = backoff.0.remove(&receiver),
        }
    }
    Ok(decided)
}

/// The next install after `after` with forward rows on this node.
async fn next_install(
    ctx: &DriverContext,
    after: Option<Ulid>,
) -> Result<Option<Ulid>, StoreError> {
    // A forward key is the install id, then 24 bytes of journal time and event id.
    let start = after.map(|id| [&id.to_bytes()[..], &[0xff; 24]].concat());
    let (rows, _) = (Txn::direct(ctx))
        .page(PLUGIN_OUTBOX_KEYSPACE, &[], start, 1)
        .await?;
    let first = rows
        .first()
        .and_then(|(key, _)| key.get(..16)?.try_into().ok());
    Ok(first.map(Ulid::from_bytes))
}

async fn drop_rows(ctx: &DriverContext, space: &str, prefix: &[u8]) -> Result<(), StoreError> {
    let txn = Txn::direct(ctx);
    loop {
        let (rows, _) = txn.page(space, prefix, None, 1_000).await?;
        if rows.is_empty() {
            return Ok(());
        }
        txn.delete(space, rows.into_iter().map(|(key, _)| key).collect())
            .await?;
    }
}

/// Reads the install's first rows: those past their send expiry become gaps, the others join
/// `pending` unless their dataset is not readable here yet.
async fn collect(
    ctx: &DriverContext,
    install_id: Ulid,
    now_ms: u64,
    mut pending: Option<&mut Requests>,
) -> Result<(), StoreError> {
    transact(ctx, async |txn: &Txn<'_>| {
        let prefix = install_id.to_bytes();
        let (rows, _) = txn
            .page(PLUGIN_OUTBOX_KEYSPACE, &prefix, None, BATCH)
            .await?;
        let (mut expired, mut outbox) = (Vec::new(), Outbox::new(now_ms));
        for (key, value) in rows {
            let row: ForwardRow = decode(&value)?;
            let size = value.len();
            if now_ms > row.queued_ms.saturating_add(SEND_EXPIRY_MS) {
                let event = (row.event.kind, row.event.anchor_ms);
                outbox
                    .gap(txn, install_id, event, GapReason::Expired)
                    .await?;
                expired.push(key);
            } else if let Some(requests) = pending.as_deref_mut()
                && !unreadable(txn, &row).await?
            {
                let (batch, keys) = requests.room(size);
                keys.insert((install_id, row.event.source.clone()), key);
                batch.events.push(row);
            }
        }
        outbox.commit(txn).await?;
        txn.delete(PLUGIN_OUTBOX_KEYSPACE, expired).await
    })
    .await
}

/// Whether a created dataset is not readable here yet: neither its creation is materialized
/// nor the document deleted. A parked job keeps the row until its send expiry counts a gap.
async fn unreadable(txn: &Txn<'_>, row: &ForwardRow) -> Result<bool, StoreError> {
    let event = &row.event;
    let parsed = (
        event.resource.parse::<Ulid>(),
        event.revision.parse::<Ulid>(),
    );
    let (Ok(document), Ok(event_id)) = parsed else {
        return Ok(false);
    };
    if event.kind != SourceKind::DatasetCreated {
        return Ok(false);
    }
    let key = materialization_status_key(document);
    let status = txn.read(MATERIALIZATION_STATUS_KEYSPACE, &key).await?;
    let status = status.and_then(|bytes| decode::<MaterializationStatusRecord>(&bytes).ok());
    let materialized = status.is_some_and(|status| {
        status.state == MaterializationState::Materialized && status.event_id >= event_id
    });
    if materialized {
        return Ok(false);
    }
    let key = graph_lifecycle_key(&MetadataRegistryRecord::graph_iri_for(document));
    let lifecycle = txn.read(GRAPH_LIFECYCLE_KEYSPACE, &key).await?;
    let lifecycle = lifecycle.and_then(|bytes| decode::<GraphLifecycleRecord>(&bytes).ok());
    Ok(!lifecycle.is_some_and(|record| record.is_deleted()))
}

/// Deletes the decided rows and records the acknowledged gap sequences in one transaction;
/// returns how many rows were decided and whether any was busy.
async fn apply(
    ctx: &DriverContext,
    keys: &BTreeMap<(Ulid, String), Vec<u8>>,
    decisions: Decisions,
) -> Result<(usize, bool), StoreError> {
    transact(ctx, async |txn: &Txn<'_>| {
        let busy = (decisions.events.iter()).any(|(.., outcome)| *outcome == Outcome::Busy);
        let decided: Vec<Vec<u8>> = (decisions.events.into_iter())
            .filter(|(.., outcome)| *outcome != Outcome::Busy)
            .filter_map(|(install, source, _)| keys.get(&(install, source)).cloned())
            .collect();
        let count = decided.len();
        txn.delete(PLUGIN_OUTBOX_KEYSPACE, decided).await?;
        for (install_id, kind, reason, sequence) in decisions.gaps {
            let key = gap_key(install_id, kind, reason);
            let Some(bytes) = txn.read(PLUGIN_GAPS_KEYSPACE, &key).await? else {
                continue;
            };
            let mut row: GapRow = decode(&bytes)?;
            row.acked = row.acked.max(sequence.min(row.sequence));
            txn.write(PLUGIN_GAPS_KEYSPACE, vec![(key, encode(&row)?)])
                .await?;
        }
        Ok((count, busy))
    })
    .await
}

#[cfg(test)]
#[path = "forward_tests.rs"]
mod tests;
