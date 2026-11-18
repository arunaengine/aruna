//! Sends this node's forward rows to each install's receiver, with the gap rows the receiver has
//! not acknowledged, and deletes the rows the receiver decided.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use aruna_core::backoff::capped_backoff;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::{HoldScope, Operation, TransactionMode};

use aruna_core::NodeId;
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::keyspaces::{
    EVENT_LOG_KEYSPACE, GRAPH_LIFECYCLE_KEYSPACE, MATERIALIZATION_STATUS_KEYSPACE,
    PLUGIN_GAPS_KEYSPACE, PLUGIN_OUTBOX_KEYSPACE,
};
use aruna_core::metadata::{
    GraphLifecycleRecord, MaterializationState, MaterializationStatusRecord, MetadataBatch,
    MetadataEffect, MetadataEvent, MetadataEventPayload, MetadataEventRecord,
};
use aruna_core::plugin_installs::InstallState;
use aruna_core::plugin_runs::{
    Decisions, EventBatch, ForwardRow, GapReason, GapRow, Outcome, SEND_EXPIRY_MS, SENDER_GAP,
    SourceKind, gap_key,
};
use aruna_core::storage_entries::{event_log_key, graph_lifecycle_key, materialization_status_key};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

use super::decide::DecideBatch;
use super::matcher::Outbox;
use super::peer::{ask_peer, peer_answer};
use super::receiver::lost_reason;
use super::rows::{
    Rows, Scan, StoreError, all_values, decode, delete_call, done, encode, page_call, page_of,
    read_call, row as row_of, values,
};
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage};

/// Forward rows of one install read for sending in one pass at most.
const BATCH: usize = 100;
/// Encoded row bytes of one request at most, well below the transport's 16 MiB message bound.
pub(crate) const REQUEST_BYTES: usize = 4 * 1024 * 1024;
/// The longest wait before a receiver is tried again.
const MAX_BACKOFF: Duration = Duration::from_secs(10 * 60);

/// When each receiver is tried again after a failed or busy send.
#[derive(Debug, Default, PartialEq)]
pub struct Backoff(BTreeMap<NodeId, (u32, u64)>);

impl Backoff {
    pub(crate) fn ready(&self, node: NodeId, now_ms: u64) -> bool {
        self.0.get(&node).is_none_or(|(_, at)| *at <= now_ms)
    }

    pub(crate) fn failed(&mut self, node: NodeId, now_ms: u64) {
        let tries = self.0.get(&node).map_or(0, |(tries, _)| *tries) + 1;
        let wait = capped_backoff(Duration::from_secs(1), MAX_BACKOFF, tries, 0);
        let wait = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX);
        self.0.insert(node, (tries, now_ms + wait));
    }

    /// A receiver that took every request is tried at once next time.
    pub(crate) fn cleared(&mut self, node: NodeId) {
        self.0.remove(&node);
    }
}

/// One request's batch and the outbox key of each event in it by install and source id.
pub(crate) type Pending = (EventBatch, BTreeMap<(Ulid, String), Vec<u8>>);

/// One receiver's requests, each with the encoded row bytes it holds.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Requests {
    bound: usize,
    pub(crate) open: Vec<(Pending, usize)>,
}

impl Requests {
    /// The request with room for `size` more bytes; a new one when the last is full. A row
    /// larger than the bound gets a request of its own.
    pub(crate) fn room(&mut self, size: usize) -> &mut Pending {
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
pub(crate) fn receiver_requests(
    batches: &mut BTreeMap<NodeId, Requests>,
    node: NodeId,
    bound: usize,
) -> &mut Requests {
    let requests = batches.entry(node).or_default();
    requests.bound = bound;
    requests
}

/// The next install after `after` with forward rows on this node.
pub(crate) fn next_install(after: Option<Ulid>) -> NextInstall {
    // A forward key is the install id, then 24 bytes of journal time and event id.
    let start = after.map(|id| [&id.to_bytes()[..], &[0xff; 24]].concat());
    NextInstall(start)
}

#[derive(Debug, PartialEq)]
pub struct NextInstall(Option<Vec<u8>>);

impl Operation for NextInstall {
    type Output = Option<Ulid>;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, StoreError> {
        Ok(Next::Call(page_call(
            PLUGIN_OUTBOX_KEYSPACE,
            Vec::new(),
            self.0.take(),
            1,
        )))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, StoreError> {
        let (rows, _) = page_of(answer)?;
        let first = (rows.first()).and_then(|(key, _)| key.get(..16)?.try_into().ok());
        Ok(Next::Done(first.map(Ulid::from_bytes)))
    }
}

/// Deletes every row below `prefix`, a page at a time.
pub(crate) fn drop_rows(space: &'static str, prefix: Vec<u8>) -> DropRows {
    DropRows {
        space,
        prefix,
        deleting: false,
    }
}

#[derive(Debug, PartialEq)]
pub struct DropRows {
    space: &'static str,
    prefix: Vec<u8>,
    deleting: bool,
}

impl DropRows {
    fn page(&self) -> IoCall {
        page_call(self.space, self.prefix.clone(), None, 1_000)
    }
}

impl Operation for DropRows {
    type Output = ();
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<()>, StoreError> {
        Ok(Next::Call(self.page()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<()>, StoreError> {
        if std::mem::take(&mut self.deleting) {
            done(answer)?;
            return Ok(Next::Call(self.page()));
        }
        let (rows, _) = page_of(answer)?;
        if rows.is_empty() {
            return Ok(Next::Done(()));
        }
        self.deleting = true;
        let keys = rows.into_iter().map(|(key, _)| (self.space, key));
        Ok(Next::Call(delete_call(keys.collect())))
    }
}

/// Counts the install's first rows past their send expiry as gaps and deletes them in one
/// transaction; answers the other rows.
pub(crate) fn expire_rows(install_id: Ulid, now_ms: u64) -> ExpireRows {
    ExpireRows {
        install_id,
        now_ms,
        phase: ExpirePhase::Reading,
    }
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

impl Operation for ExpireRows {
    type Output = Rows;
    type Error = StoreError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<Rows>, StoreError> {
        Ok(Next::Call(page_call(
            PLUGIN_OUTBOX_KEYSPACE,
            self.install_id.to_bytes().to_vec(),
            None,
            BATCH,
        )))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Rows>, StoreError> {
        match std::mem::replace(&mut self.phase, ExpirePhase::Deleting(Vec::new())) {
            ExpirePhase::Reading => {
                let (rows, _) = page_of(answer)?;
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
                Ok(Next::Call(ask))
            }
            ExpirePhase::Gaps(outbox, kept, expired) => {
                let values = all_values(answer)?;
                self.phase = ExpirePhase::Writing(kept, expired);
                Ok(Next::Call(IoCall::Write(outbox.rows(values)?)))
            }
            ExpirePhase::Writing(kept, expired) => {
                done(answer)?;
                self.phase = ExpirePhase::Deleting(kept);
                Ok(Next::Call(delete_call(expired)))
            }
            ExpirePhase::Deleting(kept) => {
                done(answer)?;
                Ok(Next::Done(kept))
            }
        }
    }
}

/// What this node holds about a dataset event: the document's status, the event's own record
/// and whether the document is deleted.
pub(crate) fn dataset_facts(document: Ulid, event_id: Ulid, updated: bool) -> DatasetFacts {
    DatasetFacts {
        document,
        event_id,
        updated,
    }
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
pub struct Facts {
    status: Option<MaterializationStatusRecord>,
    /// Whether the event's record is in the event log.
    pub(crate) logged: bool,
    /// The graph and batch of a logged change that has one.
    pub(crate) batch: Option<(String, MetadataBatch)>,
    pub(crate) deleted: bool,
}

impl Facts {
    /// Whether the document's status is materialized at an event id that `covers` accepts.
    pub(crate) fn materialized(&self, covers: impl Fn(Ulid) -> bool) -> bool {
        self.status.as_ref().is_some_and(|status| {
            status.state == MaterializationState::Materialized && covers(status.event_id)
        })
    }
}

impl Operation for DatasetFacts {
    type Output = Facts;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<Facts>, StoreError> {
        let graph = graph_lifecycle_key(&MetadataRegistryRecord::graph_iri_for(self.document));
        Ok(Next::Call(read_call(vec![
            (
                MATERIALIZATION_STATUS_KEYSPACE,
                materialization_status_key(self.document).to_vec(),
            ),
            (
                EVENT_LOG_KEYSPACE,
                event_log_key(self.document, self.event_id).to_vec(),
            ),
            (GRAPH_LIFECYCLE_KEYSPACE, graph.to_vec()),
        ])))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Facts>, StoreError> {
        let [status, logged, lifecycle] = values::<3>(answer)?;
        let status = status.and_then(|bytes| decode::<MaterializationStatusRecord>(&bytes).ok());
        let logged = logged.filter(|_| self.updated);
        let record = (logged.as_ref())
            .map(|bytes| decode::<MetadataEventRecord>(bytes))
            .transpose()?;
        let batch = record.and_then(|record| match record.payload {
            MetadataEventPayload::ApplyBatch { batch, .. } => {
                Some((record.record.graph_iri, batch))
            }
            _ => None,
        });
        let lifecycle = lifecycle.and_then(|bytes| decode::<GraphLifecycleRecord>(&bytes).ok());
        Ok(Next::Done(Facts {
            status,
            logged: logged.is_some(),
            batch,
            deleted: lifecycle.is_some_and(|record| record.is_deleted()),
        }))
    }
}

/// Deletes the decided rows and records the acknowledged gap sequences in one transaction;
/// returns how many rows were decided and whether any was busy.
pub(crate) fn apply(keys: &BTreeMap<(Ulid, String), Vec<u8>>, decisions: Decisions) -> Apply {
    let busy = (decisions.events.iter()).any(|(.., outcome)| *outcome == Outcome::Busy);
    let decided: Vec<_> = (decisions.events.into_iter())
        .filter(|(.., outcome)| *outcome != Outcome::Busy)
        .filter_map(|(install, source, _)| keys.get(&(install, source)).cloned())
        .map(|key| (PLUGIN_OUTBOX_KEYSPACE, key))
        .collect();
    let acked = (decisions.gaps.into_iter())
        .map(|(install_id, kind, reason, sequence)| (gap_key(install_id, kind, reason), sequence))
        .collect();
    Apply {
        count: decided.len(),
        busy,
        decided: Some(decided),
        acked,
        phase: ApplyPhase::Deleting,
    }
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

impl Operation for Apply {
    type Output = (usize, bool);
    type Error = StoreError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<Self::Output>, StoreError> {
        Ok(Next::Call(delete_call(
            self.decided.take().unwrap_or_default(),
        )))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, StoreError> {
        match self.phase {
            ApplyPhase::Deleting => {
                done(answer)?;
                self.phase = ApplyPhase::Reading;
                let reads = (self.acked.iter()).map(|(key, _)| (PLUGIN_GAPS_KEYSPACE, key.clone()));
                Ok(Next::Call(read_call(reads.collect())))
            }
            ApplyPhase::Reading => {
                let values = all_values(answer)?;
                let mut rows = Vec::new();
                for ((key, sequence), value) in
                    std::mem::take(&mut self.acked).into_iter().zip(values)
                {
                    let Some(bytes) = value else {
                        continue;
                    };
                    let mut row: GapRow = decode(&bytes)?;
                    row.acked = row.acked.max(sequence.min(row.sequence));
                    rows.push(row_of(PLUGIN_GAPS_KEYSPACE, key, encode(&row)?));
                }
                self.phase = ApplyPhase::Writing;
                Ok(Next::Call(IoCall::Write(rows)))
            }
            ApplyPhase::Writing => {
                done(answer)?;
                Ok(Next::Done((self.count, self.busy)))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum ReadablePhase {
    Facts,
    Dot { deleted: bool },
}

/// Whether a dataset event is not readable here yet: its own change is not materialized and the
/// document is not deleted. A parked job keeps the row until its send expiry counts a gap.
#[derive(Debug, PartialEq)]
pub(crate) struct Unreadable {
    kind: SourceKind,
    ids: Option<(Ulid, Ulid)>,
    /// Whether this node has a metadata store that can be asked about a change.
    metadata: bool,
    phase: ReadablePhase,
}

impl Unreadable {
    pub(crate) fn new(row: &ForwardRow, metadata: bool) -> Self {
        let event = &row.event;
        let ids = (event.resource.parse().ok()).zip(event.revision.parse().ok());
        Self {
            kind: event.kind,
            ids,
            metadata,
            phase: ReadablePhase::Facts,
        }
    }
}

impl Operation for Unreadable {
    type Output = bool;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<bool>, StoreError> {
        let Some((document, event_id)) = self.ids else {
            return Ok(Next::Done(false));
        };
        let updated = match self.kind {
            SourceKind::DataUploaded
            | SourceKind::SyncCompleted
            | SourceKind::SyncFailed
            | SourceKind::JobFinished => return Ok(Next::Done(false)),
            SourceKind::DatasetCreated => false,
            SourceKind::DatasetUpdated => true,
        };
        let facts = dataset_facts(document, event_id, updated);
        Ok(Next::Call(IoCall::child(facts)))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<bool>, StoreError> {
        if let ReadablePhase::Dot { deleted } = self.phase {
            let applied = match answer.event()? {
                Event::Metadata(MetadataEvent::ContainsDotResult { contains, .. }) => contains,
                Event::Metadata(MetadataEvent::Error { error, .. }) => {
                    return Err(StoreError(error.to_string()));
                }
                other => return Err(StoreError(format!("unexpected {}", other.label()))),
            };
            return Ok(Next::Done(!applied && !deleted));
        }
        let mut facts = answer.child::<Facts, StoreError>()??;
        let event_id = self.ids.map(|(_, event_id)| event_id).unwrap_or_default();
        let applied = match (self.kind, facts.batch.take()) {
            (SourceKind::DatasetCreated, _) => facts.materialized(|id| id >= event_id),
            (_, Some((graph_iri, batch))) => {
                if !self.metadata {
                    return Ok(Next::Done(false));
                }
                self.phase = ReadablePhase::Dot {
                    deleted: facts.deleted,
                };
                let dot = MetadataEffect::ContainsDot {
                    graph_iri,
                    actor: batch.actor,
                    counter: batch.counter,
                };
                return Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(dot)))));
            }
            (_, None) => facts.logged && facts.materialized(|id| id == event_id),
        };
        Ok(Next::Done(!applied && !facts.deleted))
    }
}

/// What the outbox pass is waiting for.
#[derive(Debug, PartialEq)]
enum CollectPhase {
    NextInstall,
    Dropping,
    Expiring {
        install_id: Ulid,
        receiver: Option<NodeId>,
    },
    Reading {
        install_id: Ulid,
        receiver: NodeId,
        rows: VecDeque<(Vec<u8>, Vec<u8>)>,
        current: Box<(Vec<u8>, ForwardRow, usize)>,
    },
    ScanningGaps(Scan),
    DroppingGap(VecDeque<(Vec<u8>, Vec<u8>)>),
}

/// The requests of one outbox pass by receiver. Rows of removed installs are dropped, rows past
/// their send expiry become gaps, rows and unacknowledged gaps of `open` receivers join a request
/// unless their dataset is not readable here yet.
#[derive(Debug, PartialEq)]
pub(crate) struct CollectOutbox {
    config: Arc<RealmConfigDocument>,
    open: BTreeSet<NodeId>,
    now_ms: u64,
    bound: usize,
    metadata: bool,
    after: Option<Ulid>,
    batches: BTreeMap<NodeId, Requests>,
    phase: CollectPhase,
}

impl CollectOutbox {
    pub(crate) fn new(
        config: Arc<RealmConfigDocument>,
        open: BTreeSet<NodeId>,
        (now_ms, bound): (u64, usize),
        metadata: bool,
    ) -> Self {
        Self {
            config,
            open,
            now_ms,
            bound,
            metadata,
            after: None,
            batches: BTreeMap::new(),
            phase: CollectPhase::NextInstall,
        }
    }

    /// The install of `install_id` while it is not being removed.
    fn kept(&self, install_id: Ulid) -> Option<NodeId> {
        (self.config.installs.iter())
            .find(|install| install.install_id == install_id)
            .filter(|install| install.state != InstallState::Removing)
            .map(|install| install.receiver)
    }

    fn next_install(&mut self) -> IoCall {
        self.phase = CollectPhase::NextInstall;
        IoCall::child(next_install(self.after))
    }

    fn scan_gaps(&mut self) -> IoCall {
        let scan = Scan::new(PLUGIN_GAPS_KEYSPACE, vec![SENDER_GAP]);
        let call = scan.ask();
        self.phase = CollectPhase::ScanningGaps(scan);
        call
    }

    /// The readability check of the next row, or the next install.
    fn next_row(
        &mut self,
        install_id: Ulid,
        receiver: NodeId,
        mut rows: VecDeque<(Vec<u8>, Vec<u8>)>,
    ) -> Result<IoCall, StoreError> {
        let Some((key, value)) = rows.pop_front() else {
            return Ok(self.next_install());
        };
        let row: ForwardRow = decode(&value)?;
        let check = Unreadable::new(&row, self.metadata);
        self.phase = CollectPhase::Reading {
            install_id,
            receiver,
            rows,
            current: Box::new((key, row, value.len())),
        };
        Ok(IoCall::child(check))
    }

    /// Adds the gaps of `rows`; stops at a gap of a removed install, which is dropped first.
    fn take_gaps(
        &mut self,
        mut rows: VecDeque<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Next<BTreeMap<NodeId, Requests>>, StoreError> {
        while let Some((key, value)) = rows.pop_front() {
            let install_id = Ulid::from_bytes(key[1..17].try_into().unwrap_or_default());
            let Some(receiver) = self.kept(install_id) else {
                self.phase = CollectPhase::DroppingGap(rows);
                return Ok(Next::Call(IoCall::child(drop_rows(
                    PLUGIN_GAPS_KEYSPACE,
                    key,
                ))));
            };
            let row: GapRow = decode(&value)?;
            if row.sequence > row.acked && self.open.contains(&receiver) {
                let requests = receiver_requests(&mut self.batches, receiver, self.bound);
                let (batch, _) = requests.room(value.len() + 16);
                batch.gaps.push((install_id, row));
            }
        }
        Ok(Next::Done(std::mem::take(&mut self.batches)))
    }
}

impl Operation for CollectOutbox {
    type Output = BTreeMap<NodeId, Requests>;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, StoreError> {
        Ok(Next::Call(self.next_install()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, StoreError> {
        match std::mem::replace(&mut self.phase, CollectPhase::Dropping) {
            CollectPhase::NextInstall => {
                let Some(install_id) = answer.child::<Option<Ulid>, StoreError>()?? else {
                    return Ok(Next::Call(self.scan_gaps()));
                };
                self.after = Some(install_id);
                let Some(receiver) = self.kept(install_id) else {
                    let prefix = install_id.to_bytes().to_vec();
                    let drop = drop_rows(PLUGIN_OUTBOX_KEYSPACE, prefix);
                    return Ok(Next::Call(IoCall::child(drop)));
                };
                let receiver = self.open.contains(&receiver).then_some(receiver);
                if let Some(receiver) = receiver {
                    receiver_requests(&mut self.batches, receiver, self.bound);
                }
                self.phase = CollectPhase::Expiring {
                    install_id,
                    receiver,
                };
                Ok(Next::Call(IoCall::child(expire_rows(
                    install_id,
                    self.now_ms,
                ))))
            }
            CollectPhase::Dropping => {
                answer.child::<(), StoreError>()??;
                Ok(Next::Call(self.next_install()))
            }
            CollectPhase::Expiring {
                install_id,
                receiver,
            } => {
                let kept = answer.child::<Rows, StoreError>()??;
                Ok(Next::Call(match receiver {
                    Some(receiver) => self.next_row(install_id, receiver, kept.into())?,
                    None => self.next_install(),
                }))
            }
            CollectPhase::Reading {
                install_id,
                receiver,
                rows,
                current,
            } => {
                let (key, row, size) = *current;
                if !answer.child::<bool, StoreError>()?? {
                    let requests = receiver_requests(&mut self.batches, receiver, self.bound);
                    let (batch, keys) = requests.room(size);
                    keys.insert((install_id, row.event.source.clone()), key);
                    batch.events.push(row);
                }
                Ok(Next::Call(self.next_row(install_id, receiver, rows)?))
            }
            CollectPhase::ScanningGaps(mut scan) => match scan.take(answer)? {
                Some(rows) => self.take_gaps(rows.into()),
                None => {
                    let call = scan.ask();
                    self.phase = CollectPhase::ScanningGaps(scan);
                    Ok(Next::Call(call))
                }
            },
            CollectPhase::DroppingGap(rows) => {
                answer.child::<(), StoreError>()??;
                self.take_gaps(rows)
            }
        }
    }
}

/// Hands a batch of events to its receiver over the metadata transport.
#[derive(Debug, PartialEq)]
pub(crate) struct SendEvents {
    node: NodeId,
    batch: EventBatch,
}

impl SendEvents {
    pub(crate) fn new(node: NodeId, batch: EventBatch) -> Self {
        Self { node, batch }
    }
}

impl Operation for SendEvents {
    type Output = Decisions;
    type Error = MetadataReadError;

    fn first(&mut self) -> Result<Next<Decisions>, MetadataReadError> {
        let batch = std::mem::take(&mut self.batch);
        let forward = MetadataTransportMessage::ForwardEvents { batch };
        let ask = ask_peer(self.node, &forward).ok_or(MetadataReadError::Unavailable)?;
        Ok(Next::Call(ask))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Decisions>, MetadataReadError> {
        match peer_answer(self.node, result)? {
            Some(MetadataTransportMessage::EventsDecided { result }) => Ok(Next::Done(result?)),
            _ => Err(MetadataReadError::Unavailable),
        }
    }
}

/// Which receivers one outbox pass serves. This node's own batches are decided under the
/// admission hold; the others wait for the network without it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OutboxRoute {
    Local,
    Remote,
}

/// What one outbox pass did.
#[derive(Debug, PartialEq)]
pub(crate) struct OutboxPass {
    pub decided: usize,
    pub backoff: Backoff,
    pub failure: Option<StoreError>,
}

/// The requests of one receiver still to send.
#[derive(Debug, PartialEq)]
struct ReceiverQueue {
    node: NodeId,
    requests: VecDeque<Pending>,
}

#[derive(Debug, PartialEq)]
enum OutboxPhase {
    Collect,
    Sent(BTreeMap<(Ulid, String), Vec<u8>>),
    Applied,
}

/// Sends each reachable receiver its batches and applies their decisions. Rows of removed
/// installs are dropped and rows past their send expiry become gaps. A receiver that fails or
/// is busy waits; one that took every request is tried at once next time.
#[derive(Debug, PartialEq)]
pub(crate) struct SendOutbox {
    config: Arc<RealmConfigDocument>,
    route: OutboxRoute,
    local: NodeId,
    now_ms: u64,
    bound: usize,
    metadata: bool,
    backoff: Backoff,
    receivers: VecDeque<ReceiverQueue>,
    current: Option<ReceiverQueue>,
    decided: usize,
    phase: OutboxPhase,
}

impl SendOutbox {
    pub(crate) fn new(
        config: Arc<RealmConfigDocument>,
        (route, local): (OutboxRoute, NodeId),
        (now_ms, metadata): (u64, bool),
        backoff: Backoff,
    ) -> Self {
        Self {
            config,
            route,
            local,
            now_ms,
            bound: REQUEST_BYTES,
            metadata,
            backoff,
            receivers: VecDeque::new(),
            current: None,
            decided: 0,
            phase: OutboxPhase::Collect,
        }
    }

    fn served(&self, node: NodeId) -> bool {
        (node == self.local) == (self.route == OutboxRoute::Local)
    }

    fn collect(&self) -> CollectOutbox {
        let open = (self.config.installs.iter())
            .map(|install| install.receiver)
            .filter(|node| self.served(*node))
            .filter(|node| lost_reason(&self.config, *node).is_none())
            .filter(|node| self.backoff.ready(*node, self.now_ms))
            .collect();
        let pass = (self.now_ms, self.bound);
        CollectOutbox::new(self.config.clone(), open, pass, self.metadata)
    }

    fn finish(&mut self, failure: Option<StoreError>) -> Next<OutboxPass> {
        Next::Done(OutboxPass {
            decided: self.decided,
            backoff: std::mem::take(&mut self.backoff),
            failure,
        })
    }

    /// Sends the next request, moving on to the next receiver when one has none left.
    fn advance(&mut self) -> Next<OutboxPass> {
        loop {
            let Some(mut receiver) = self.current.take().or_else(|| self.receivers.pop_front())
            else {
                return self.finish(None);
            };
            let Some((batch, keys)) = receiver.requests.pop_front() else {
                self.backoff.cleared(receiver.node);
                continue;
            };
            let node = receiver.node;
            self.current = Some(receiver);
            self.phase = OutboxPhase::Sent(keys);
            return Next::Call(match self.route {
                OutboxRoute::Local => {
                    let nodes = (self.local, self.local);
                    let config = self.config.clone();
                    IoCall::child(DecideBatch::new(config, nodes, batch, self.now_ms))
                }
                OutboxRoute::Remote => IoCall::child(SendEvents::new(node, batch)),
            });
        }
    }

    /// The receiver waits, and what it still had stays in the outbox.
    fn receiver_failed(&mut self) -> Next<OutboxPass> {
        if let Some(receiver) = self.current.take() {
            self.backoff.failed(receiver.node, self.now_ms);
        }
        self.advance()
    }

    fn decisions(&self, result: IoResult) -> Result<Option<Decisions>, IoError> {
        Ok(match self.route {
            OutboxRoute::Local => result.child::<Decisions, StoreError>()?.ok(),
            OutboxRoute::Remote => result.child::<Decisions, MetadataReadError>()?.ok(),
        })
    }

    fn step(&mut self, result: IoResult) -> Result<Next<OutboxPass>, StoreError> {
        match std::mem::replace(&mut self.phase, OutboxPhase::Applied) {
            OutboxPhase::Collect => {
                let batches = result.child::<BTreeMap<NodeId, Requests>, StoreError>()??;
                self.receivers = (batches.into_iter())
                    .map(|(node, requests)| ReceiverQueue {
                        node,
                        requests: (requests.open.into_iter())
                            .map(|(pending, _)| pending)
                            .collect(),
                    })
                    .collect();
                Ok(self.advance())
            }
            OutboxPhase::Sent(keys) => match self.decisions(result)? {
                Some(decisions) => Ok(Next::Call(IoCall::child(apply(&keys, decisions)))),
                None => Ok(self.receiver_failed()),
            },
            OutboxPhase::Applied => {
                let (count, busy) = result.child::<(usize, bool), StoreError>()??;
                self.decided += count;
                Ok(match busy {
                    true => self.receiver_failed(),
                    false => self.advance(),
                })
            }
        }
    }
}

impl Operation for SendOutbox {
    type Output = OutboxPass;
    type Error = StoreError;

    fn hold_scope(&self) -> Option<HoldScope> {
        (self.route == OutboxRoute::Local).then_some(HoldScope::PluginAdmissions)
    }

    fn first(&mut self) -> Result<Next<OutboxPass>, StoreError> {
        Ok(Next::Call(IoCall::child(self.collect())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<OutboxPass>, StoreError> {
        Ok(match self.step(result) {
            Ok(next) => next,
            Err(error) => self.finish(Some(error)),
        })
    }
}

#[cfg(test)]
#[path = "forward_tests.rs"]
mod tests;
