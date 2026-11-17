//! Sends this node's forward rows to each install's receiver, with the gap rows the receiver has
//! not acknowledged, and deletes the rows the receiver decided.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::keyspaces::{
    EVENT_LOG_KEYSPACE, GRAPH_LIFECYCLE_KEYSPACE, MATERIALIZATION_STATUS_KEYSPACE,
    PLUGIN_GAPS_KEYSPACE, PLUGIN_OUTBOX_KEYSPACE,
};
use aruna_core::metadata::{
    GraphLifecycleRecord, MaterializationState, MaterializationStatusRecord, MetadataBatch,
    MetadataEventPayload, MetadataEventRecord,
};
use aruna_core::plugin_installs::InstallState;
use aruna_core::plugin_runs::{
    ForwardRow, GapReason, GapRow, SEND_EXPIRY_MS, SENDER_GAP, SourceKind, gap_key,
};
use aruna_core::storage_entries::{event_log_key, graph_lifecycle_key, materialization_status_key};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::matcher::Outbox;
use super::receiver::lost_reason;
use super::rows::{Answer, Ask, Plan, RowsOperation, Scan, Turn, done, row as row_of, scan_rows, unexpected, values, Rows, StoreError, decode, encode};
use crate::driver::{DriverContext, drive};
use crate::metadata::projector::dot_applied;

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
    while let Some(install_id) = drive(next_install(after), ctx).await? {
        after = Some(install_id);
        let install = (config.installs.iter()).find(|i| i.install_id == install_id);
        let Some(install) = install.filter(|i| i.state != InstallState::Removing) else {
            let prefix = install_id.to_bytes().to_vec();
            drive(drop_rows(PLUGIN_OUTBOX_KEYSPACE, prefix), ctx).await?;
            continue;
        };
        let pending = open(install.receiver)
            .then(|| receiver_requests(&mut batches, install.receiver, bound));
        collect(ctx, install_id, now_ms, pending).await?;
    }
    let gaps = Scan::new(PLUGIN_GAPS_KEYSPACE, vec![SENDER_GAP]);
    for (key, value) in drive(scan_rows(gaps), ctx).await? {
        let install_id = Ulid::from_bytes(key[1..17].try_into().unwrap_or_default());
        let install = (config.installs.iter()).find(|i| i.install_id == install_id);
        let Some(install) = install.filter(|i| i.state != InstallState::Removing) else {
            drive(drop_rows(PLUGIN_GAPS_KEYSPACE, key), ctx).await?;
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
            let (count, busy) = drive(apply(&keys, decisions), ctx).await?;
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
fn next_install(after: Option<Ulid>) -> RowsOperation<NextInstall> {
    // A forward key is the install id, then 24 bytes of journal time and event id.
    let start = after.map(|id| [&id.to_bytes()[..], &[0xff; 24]].concat());
    RowsOperation::direct(NextInstall(start))
}

#[derive(Debug, PartialEq)]
pub struct NextInstall(Option<Vec<u8>>);

impl Plan for NextInstall {
    type Output = Option<Ulid>;
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<Self::Output>, StoreError> {
        Ok(Turn::Ask(Ask::Page {
            space: PLUGIN_OUTBOX_KEYSPACE,
            prefix: Vec::new(),
            after: self.0.take(),
            limit: 1,
        }))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<Self::Output>, StoreError> {
        let Answer::Page(rows, _) = answer else {
            return Err(unexpected(answer));
        };
        let first = (rows.first()).and_then(|(key, _)| key.get(..16)?.try_into().ok());
        Ok(Turn::Finish(first.map(Ulid::from_bytes)))
    }
}

/// Deletes every row below `prefix`, a page at a time.
fn drop_rows(space: &'static str, prefix: Vec<u8>) -> RowsOperation<DropRows> {
    RowsOperation::direct(DropRows {
        space,
        prefix,
        deleting: false,
    })
}

#[derive(Debug, PartialEq)]
pub struct DropRows {
    space: &'static str,
    prefix: Vec<u8>,
    deleting: bool,
}

impl DropRows {
    fn page(&self) -> Ask {
        Ask::Page {
            space: self.space,
            prefix: self.prefix.clone(),
            after: None,
            limit: 1_000,
        }
    }
}

impl Plan for DropRows {
    type Output = ();
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<()>, StoreError> {
        Ok(Turn::Ask(self.page()))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<()>, StoreError> {
        if std::mem::take(&mut self.deleting) {
            done(answer)?;
            return Ok(Turn::Ask(self.page()));
        }
        let Answer::Page(rows, _) = answer else {
            return Err(unexpected(answer));
        };
        if rows.is_empty() {
            return Ok(Turn::Finish(()));
        }
        self.deleting = true;
        let keys = rows.into_iter().map(|(key, _)| (self.space, key));
        Ok(Turn::Ask(Ask::Delete(keys.collect())))
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
    let kept = drive(expire_rows(install_id, now_ms), ctx).await?;
    let Some(requests) = pending.as_deref_mut() else {
        return Ok(());
    };
    for (key, value) in kept {
        let row: ForwardRow = decode(&value)?;
        if unreadable(ctx, &row).await? {
            continue;
        }
        let (batch, keys) = requests.room(value.len());
        keys.insert((install_id, row.event.source.clone()), key);
        batch.events.push(row);
    }
    Ok(())
}

/// Counts the install's first rows past their send expiry as gaps and deletes them in one
/// transaction; answers the other rows.
fn expire_rows(install_id: Ulid, now_ms: u64) -> RowsOperation<ExpireRows> {
    RowsOperation::transaction(ExpireRows {
        install_id,
        now_ms,
        phase: ExpirePhase::Reading,
    })
}

#[derive(Debug, PartialEq)]
pub struct ExpireRows {
    install_id: Ulid,
    now_ms: u64,
    phase: ExpirePhase,
}

#[derive(Debug, PartialEq)]
enum ExpirePhase {
    Reading,
    Gaps(Box<Outbox>, Rows, Vec<(&'static str, Vec<u8>)>),
    Writing(Rows, Vec<(&'static str, Vec<u8>)>),
    Deleting(Rows),
}

impl Plan for ExpireRows {
    type Output = Rows;
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<Rows>, StoreError> {
        Ok(Turn::Ask(Ask::Page {
            space: PLUGIN_OUTBOX_KEYSPACE,
            prefix: self.install_id.to_bytes().to_vec(),
            after: None,
            limit: BATCH,
        }))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<Rows>, StoreError> {
        match std::mem::replace(&mut self.phase, ExpirePhase::Deleting(Vec::new())) {
            ExpirePhase::Reading => {
                let Answer::Page(rows, _) = answer else {
                    return Err(unexpected(answer));
                };
                let mut outbox = Outbox::new(self.now_ms, BTreeMap::new());
                let (mut kept, mut expired) = (Vec::new(), Vec::new());
                for (key, value) in rows {
                    let row: ForwardRow = decode(&value)?;
                    if self.now_ms > row.queued_ms.saturating_add(SEND_EXPIRY_MS) {
                        let event = (row.event.kind, row.event.anchor_ms);
                        outbox.gap(self.install_id, event, GapReason::Expired);
                        expired.push((PLUGIN_OUTBOX_KEYSPACE, key));
                    } else {
                        kept.push((key, value));
                    }
                }
                let ask = outbox.gap_ask();
                self.phase = ExpirePhase::Gaps(Box::new(outbox), kept, expired);
                Ok(Turn::Ask(ask))
            }
            ExpirePhase::Gaps(outbox, kept, expired) => {
                let Answer::Values(values) = answer else {
                    return Err(unexpected(answer));
                };
                self.phase = ExpirePhase::Writing(kept, expired);
                Ok(Turn::Ask(Ask::Write(outbox.rows(values)?)))
            }
            ExpirePhase::Writing(kept, expired) => {
                done(answer)?;
                self.phase = ExpirePhase::Deleting(kept);
                Ok(Turn::Ask(Ask::Delete(expired)))
            }
            ExpirePhase::Deleting(kept) => {
                done(answer)?;
                Ok(Turn::Finish(kept))
            }
        }
    }
}

/// Whether a dataset event is not readable here yet: its own change is not materialized and the
/// document is not deleted. A parked job keeps the row until its send expiry counts a gap.
async fn unreadable(ctx: &DriverContext, row: &ForwardRow) -> Result<bool, StoreError> {
    let event = &row.event;
    let parsed = (
        event.resource.parse::<Ulid>(),
        event.revision.parse::<Ulid>(),
    );
    let (Ok(document), Ok(event_id)) = parsed else {
        return Ok(false);
    };
    let facts = match event.kind {
        SourceKind::DataUploaded
        | SourceKind::SyncCompleted
        | SourceKind::SyncFailed
        | SourceKind::JobFinished => {
            return Ok(false);
        }
        SourceKind::DatasetCreated | SourceKind::DatasetUpdated => {
            let updated = event.kind == SourceKind::DatasetUpdated;
            drive(dataset_facts(document, event_id, updated), ctx).await?
        }
    };
    let applied = match (event.kind, facts.batch) {
        (SourceKind::DatasetCreated, _) => facts.materialized(|id| id >= event_id),
        (_, Some((graph_iri, batch))) => dot_applied(ctx, &graph_iri, &batch)
            .await
            .map_err(|error| StoreError(error.to_string()))?,
        (_, None) => facts.logged && facts.materialized(|id| id == event_id),
    };
    Ok(!applied && !facts.deleted)
}

/// What this node holds about a dataset event: the document's status, the event's own record
/// and whether the document is deleted.
fn dataset_facts(document: Ulid, event_id: Ulid, updated: bool) -> RowsOperation<DatasetFacts> {
    RowsOperation::direct(DatasetFacts {
        document,
        event_id,
        updated,
    })
}

#[derive(Debug, PartialEq)]
pub struct DatasetFacts {
    document: Ulid,
    event_id: Ulid,
    /// Whether the event is a change, whose own record decides.
    updated: bool,
}

/// The rows read for [`unreadable`].
#[derive(Debug, Default, PartialEq)]
struct Facts {
    status: Option<MaterializationStatusRecord>,
    /// Whether the event's record is in the event log.
    logged: bool,
    /// The graph and batch of a logged change that has one.
    batch: Option<(String, MetadataBatch)>,
    deleted: bool,
}

impl Facts {
    /// Whether the document's status is materialized at an event id that `covers` accepts.
    fn materialized(&self, covers: impl Fn(Ulid) -> bool) -> bool {
        self.status.as_ref().is_some_and(|status| {
            status.state == MaterializationState::Materialized && covers(status.event_id)
        })
    }
}

impl Plan for DatasetFacts {
    type Output = Facts;
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<Facts>, StoreError> {
        let graph = graph_lifecycle_key(&MetadataRegistryRecord::graph_iri_for(self.document));
        Ok(Turn::Ask(Ask::Read(vec![
            (MATERIALIZATION_STATUS_KEYSPACE, materialization_status_key(self.document)),
            (EVENT_LOG_KEYSPACE, event_log_key(self.document, self.event_id)),
            (GRAPH_LIFECYCLE_KEYSPACE, graph),
        ])))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<Facts>, StoreError> {
        let [status, logged, lifecycle] = values::<3>(answer)?;
        let status = status.and_then(|bytes| decode::<MaterializationStatusRecord>(&bytes).ok());
        let logged = logged.filter(|_| self.updated);
        let record = (logged.as_ref())
            .map(|bytes| decode::<MetadataEventRecord>(bytes))
            .transpose()?;
        let batch = record.and_then(|record| match record.payload {
            MetadataEventPayload::ApplyBatch { batch, .. } => Some((record.record.graph_iri, batch)),
            _ => None,
        });
        let lifecycle = lifecycle.and_then(|bytes| decode::<GraphLifecycleRecord>(&bytes).ok());
        Ok(Turn::Finish(Facts {
            status,
            logged: logged.is_some(),
            batch,
            deleted: lifecycle.is_some_and(|record| record.is_deleted()),
        }))
    }
}

/// Deletes the decided rows and records the acknowledged gap sequences in one transaction;
/// returns how many rows were decided and whether any was busy.
fn apply(keys: &BTreeMap<(Ulid, String), Vec<u8>>, decisions: Decisions) -> RowsOperation<Apply> {
    let busy = (decisions.events.iter()).any(|(.., outcome)| *outcome == Outcome::Busy);
    let decided: Vec<_> = (decisions.events.into_iter())
        .filter(|(.., outcome)| *outcome != Outcome::Busy)
        .filter_map(|(install, source, _)| keys.get(&(install, source)).cloned())
        .map(|key| (PLUGIN_OUTBOX_KEYSPACE, key))
        .collect();
    let acked = (decisions.gaps.into_iter())
        .map(|(install_id, kind, reason, sequence)| (gap_key(install_id, kind, reason), sequence))
        .collect();
    RowsOperation::transaction(Apply {
        count: decided.len(),
        busy,
        decided: Some(decided),
        acked,
        phase: ApplyPhase::Deleting,
    })
}

#[derive(Debug, PartialEq)]
pub struct Apply {
    count: usize,
    busy: bool,
    decided: Option<Vec<(&'static str, Vec<u8>)>>,
    acked: Vec<(Vec<u8>, u64)>,
    phase: ApplyPhase,
}

#[derive(Debug, PartialEq)]
enum ApplyPhase {
    Deleting,
    Reading,
    Writing,
}

impl Plan for Apply {
    type Output = (usize, bool);
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<Self::Output>, StoreError> {
        Ok(Turn::Ask(Ask::Delete(self.decided.take().unwrap_or_default())))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<Self::Output>, StoreError> {
        match self.phase {
            ApplyPhase::Deleting => {
                done(answer)?;
                self.phase = ApplyPhase::Reading;
                let reads = (self.acked.iter()).map(|(key, _)| (PLUGIN_GAPS_KEYSPACE, key.clone()));
                Ok(Turn::Ask(Ask::Read(reads.collect())))
            }
            ApplyPhase::Reading => {
                let Answer::Values(values) = answer else {
                    return Err(unexpected(answer));
                };
                let mut rows = Vec::new();
                for ((key, sequence), value) in std::mem::take(&mut self.acked).into_iter().zip(values) {
                    let Some(bytes) = value else {
                        continue;
                    };
                    let mut row: GapRow = decode(&bytes)?;
                    row.acked = row.acked.max(sequence.min(row.sequence));
                    rows.push(row_of(PLUGIN_GAPS_KEYSPACE, key, encode(&row)?));
                }
                self.phase = ApplyPhase::Writing;
                Ok(Turn::Ask(Ask::Write(rows)))
            }
            ApplyPhase::Writing => {
                done(answer)?;
                Ok(Turn::Finish((self.count, self.busy)))
            }
        }
    }
}

#[cfg(test)]
#[path = "forward_tests.rs"]
mod tests;
