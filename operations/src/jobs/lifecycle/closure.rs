//! Closes what was sent and never taken up: a plugin task's submission past its deadline and a
//! launch without a receipt past its window, answered from stored rows by the node it went to.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, VecDeque};
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, JobRecordFrame, LaunchFrame, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::jobs::{ClosureAnswer, LaunchClosureAnswer};
use aruna_core::keyspaces::{
    FAMILY_RECORD_KEYSPACE, FAMILY_TOMBSTONE_KEYSPACE, LAUNCH_CLOSURE_KEYSPACE,
    SUBMISSION_CLOSURE_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::job::family::{
    CLOCK_MARGIN_MS, JobFamilyId, JobFamilyRecord, JobRecordEnvelope, JobRecordKind, LaunchIntent,
    SubmissionId,
};
use aruna_core::structs::execution::job::prune::FamilyTombstone;
use aruna_core::structs::execution::job::record_keys::{family_prefix, submission_prefix};
use aruna_core::structs::execution::job::record_rows::{from_bytes, to_bytes};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{Key, Value};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, warn};
use ulid::Ulid;

use super::LifecycleError;
use super::launch_admit::existing_receipt;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::jobs::records::append::{
    AppendOutcome, AppendRecordConfig, AppendRecordOperation, RecordOrigin,
};
use crate::jobs::records::family::{FamilyReadError, LoadFamilyOperation};
use crate::jobs::records::{RECORD_PAGE_SIZE, RecordStoreError};
use aruna_core::metadata::protocol::{
    MetadataTransportMessage, decode_message, encode_message, metadata_request,
};

/// How long one closure or record question to a node may take.
pub(crate) const ASK_DEADLINE: Duration = Duration::from_secs(10);

/// Closure rows one sweep pass reads at a time.
const SWEEP_PAGE: usize = 500;

/// Closure rows one page of the launch pass reads.
const CLOSURE_PAGE: usize = 512;

/// Whether the realm config `result` names `peer` as a member; an unreadable config does not.
fn member(result: IoResult, peer: NodeId) -> Result<bool, IoError> {
    let config = result.child::<RealmConfigDocument, GetConfigError>()?;
    let eligible = (config.ok()).and_then(|config| config.sync_eligible_nodes().ok());
    Ok(eligible.is_some_and(|nodes| nodes.contains(&peer)))
}

/// The call that sends `message` to `node` within the ask deadline; `None` when it does not
/// encode.
fn ask_call(node: NodeId, message: &MetadataTransportMessage) -> Option<IoCall> {
    let frame = encode_message(message).ok()?;
    let request = metadata_request(node, frame, ASK_DEADLINE);
    let effect = Effect::Net(NetEffect::MetadataRequest(Box::new(request)));
    Some(IoCall::Effect(Box::new(effect)))
}

/// The answer message of one ask; `None` when the node was not reached or its answer is garbage.
fn reply_of(result: IoResult) -> Option<MetadataTransportMessage> {
    let IoResult::Event(event) = result else {
        return None;
    };
    let Event::Net(NetEvent::MetadataAnswer(answer)) = *event else {
        return None;
    };
    decode_message(&answer.result.ok()?).ok()
}

/// The closure row of one submission: it names the deadline, after which it may go.
pub fn closure_key(submission_id: SubmissionId) -> Key {
    Key::from(submission_id.0.as_slice())
}

/// Whether a closure row may go: past its deadline and the margin on this node's clock, the
/// admission deadline check refuses every fresh admission, and an open one read the row.
pub fn closure_expired(admit_by: &[u8], now_ms: u64) -> bool {
    let Ok(admit_by) = <[u8; 8]>::try_from(admit_by) else {
        return false;
    };
    u64::from_be_bytes(admit_by).saturating_add(CLOCK_MARGIN_MS) < now_ms
}

/// Reads the claims of one submission and, without one, writes its closure row, in one
/// transaction. Admission reads that row, so whichever commits first decides.
#[derive(Debug, PartialEq)]
pub struct CloseSubmissionOperation {
    submission_id: SubmissionId,
    admit_by: u64,
    claim: Option<ClosureAnswer>,
    written: bool,
}

impl CloseSubmissionOperation {
    pub fn new(submission_id: SubmissionId, admit_by: u64) -> Self {
        Self {
            submission_id,
            admit_by,
            claim: None,
            written: false,
        }
    }

    fn scan(&self, after: Option<Key>) -> Next<ClosureAnswer> {
        Next::Call(IoCall::Page {
            space: FAMILY_RECORD_KEYSPACE.to_string(),
            prefix: submission_prefix(self.submission_id),
            after,
            limit: RECORD_PAGE_SIZE,
        })
    }

    /// Keeps the first claim of the page; claims of other request families count too.
    fn keep_claim(&mut self, rows: Vec<(Key, Value)>) -> Result<(), LifecycleError> {
        for (_, value) in rows {
            let envelope = from_bytes::<JobRecordEnvelope>(&value)?;
            if let (None, JobFamilyRecord::Claim(claim)) = (&self.claim, &envelope.record) {
                self.claim = Some(ClosureAnswer::Claimed {
                    family: envelope.family(),
                    job_id: claim.job_id,
                });
            }
        }
        Ok(())
    }
}

impl Operation for CloseSubmissionOperation {
    type Output = ClosureAnswer;
    type Error = LifecycleError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<ClosureAnswer>, LifecycleError> {
        Ok(self.scan(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ClosureAnswer>, LifecycleError> {
        if self.written {
            return match result {
                IoResult::Done => Ok(Next::Done(ClosureAnswer::Absent)),
                other => Err(LifecycleError::unexpected(
                    &"closure",
                    "Done",
                    other.label(),
                )),
            };
        }
        let IoResult::Page { rows, next } = result else {
            let got = result.label();
            return Err(LifecycleError::unexpected(&"claim scan", "Page", got));
        };
        self.keep_claim(rows)?;
        if let Some(claim) = self.claim {
            return Ok(Next::Done(claim));
        }
        if let Some(after) = next {
            return Ok(self.scan(Some(after)));
        }
        self.written = true;
        let row = (
            SUBMISSION_CLOSURE_KEYSPACE.to_string(),
            closure_key(self.submission_id),
            Value::from(self.admit_by.to_be_bytes().as_slice()),
        );
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

/// Answers a realm node about one submission from stored rows only, with no holder check: a
/// node keeps the claims it admitted after it stopped holding the placement. A conflict or any
/// storage failure decides nothing, and the asker asks again.
#[derive(Debug, PartialEq)]
pub struct ServeClosureOperation {
    realm_id: RealmId,
    peer: NodeId,
    question: (SubmissionId, u64),
    configured: bool,
}

impl ServeClosureOperation {
    pub fn new(realm_id: RealmId, peer: NodeId, question: (SubmissionId, u64)) -> Self {
        Self {
            realm_id,
            peer,
            question,
            configured: false,
        }
    }
}

impl Operation for ServeClosureOperation {
    type Output = ClosureAnswer;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<ClosureAnswer>, IoError> {
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ClosureAnswer>, IoError> {
        if self.configured {
            let closed = result.child::<ClosureAnswer, LifecycleError>()?;
            return Ok(Next::Done(closed.unwrap_or(ClosureAnswer::Undecided)));
        }
        if !member(result, self.peer)? {
            return Ok(Next::Done(ClosureAnswer::Undecided));
        }
        self.configured = true;
        let (submission_id, admit_by) = self.question;
        let close = CloseSubmissionOperation::new(submission_id, admit_by);
        Ok(Next::Call(IoCall::child(close)))
    }
}

/// Asks `node` whether it admitted the submission; any failure decides nothing.
#[derive(Debug, PartialEq)]
pub struct AskClosureOperation {
    node: NodeId,
    question: (SubmissionId, u64),
}

impl AskClosureOperation {
    pub fn new(node: NodeId, question: (SubmissionId, u64)) -> Self {
        Self { node, question }
    }
}

impl Operation for AskClosureOperation {
    type Output = ClosureAnswer;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<ClosureAnswer>, IoError> {
        let (submission_id, admit_by) = self.question;
        let message = MetadataTransportMessage::QuerySubmissionClaim {
            submission_id,
            admit_by,
        };
        Ok(match ask_call(self.node, &message) {
            Some(call) => Next::Call(call),
            None => Next::Done(ClosureAnswer::Undecided),
        })
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ClosureAnswer>, IoError> {
        Ok(Next::Done(match reply_of(result) {
            Some(MetadataTransportMessage::SubmissionClaimAnswered { answer }) => answer,
            _ => ClosureAnswer::Undecided,
        }))
    }
}

/// Deletes the expired closure rows of this node, a page at a time; answers how many went.
#[derive(Debug, PartialEq)]
pub struct SweepClosuresOperation {
    now_ms: u64,
    after: Option<Key>,
    dropped: usize,
    deleting: bool,
}

impl SweepClosuresOperation {
    pub fn new(now_ms: u64) -> Self {
        Self {
            now_ms,
            after: None,
            dropped: 0,
            deleting: false,
        }
    }

    fn page(&self) -> Next<usize> {
        Next::Call(IoCall::Page {
            space: SUBMISSION_CLOSURE_KEYSPACE.to_string(),
            prefix: Key::from(&[][..]),
            after: self.after.clone(),
            limit: SWEEP_PAGE,
        })
    }
}

impl Operation for SweepClosuresOperation {
    type Output = usize;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<usize>, IoError> {
        Ok(self.page())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, IoError> {
        if std::mem::take(&mut self.deleting) {
            return Ok(match self.after {
                Some(_) => self.page(),
                None => Next::Done(self.dropped),
            });
        }
        let IoResult::Page { rows, next } = result else {
            let got = result.label();
            return Err(IoError::Unexpected {
                expected: "IoResult::Page",
                got,
            });
        };
        self.after = next;
        let expired: Vec<_> = (rows.into_iter())
            .filter(|(_, value)| closure_expired(value, self.now_ms))
            .map(|(key, _)| (SUBMISSION_CLOSURE_KEYSPACE.to_string(), key))
            .collect();
        self.dropped += expired.len();
        self.deleting = true;
        Ok(Next::Call(IoCall::Delete(expired)))
    }
}

/// Why one closed launch is closed on this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClosureKind {
    NeverAccepted,
    Pruned,
}

/// One closed launch, deleted with its family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClosureRow {
    pub kind: ClosureKind,
    pub closed_at_ms: u64,
}

/// The closure row of one launch, under its family's prefix so it goes with the family.
pub fn launch_closure_key(family: &JobFamilyId, launch_id: Ulid) -> Key {
    let mut key = family_prefix(family).to_vec();
    key.extend_from_slice(&launch_id.to_bytes());
    Key::from(key.as_slice())
}

/// Why a target gives no closure answer.
#[derive(Debug, Error, PartialEq)]
pub enum ClosureError {
    #[error("closure query from outside the realm")]
    Outsider,
    #[error("closure answer undecided: {0}")]
    Undecided(String),
}

impl From<IoError> for ClosureError {
    fn from(error: IoError) -> Self {
        Self::Undecided(error.to_string())
    }
}

#[derive(Debug, PartialEq)]
enum AnswerStep {
    Config,
    Receipts,
    Tombstone,
}

/// The answer this target gives for one signed launch, from its stored rows only: no fetch, no
/// append and no holder check. A peer must be a realm member; this node's own query is not checked.
#[derive(Debug, PartialEq)]
pub struct AnswerClosureOperation {
    launch: LaunchFrame,
    serving: Option<(NodeId, RealmId)>,
    peer: Option<NodeId>,
    now_ms: u64,
    step: AnswerStep,
}

impl AnswerClosureOperation {
    pub fn new(
        launch: LaunchFrame,
        serving: Option<(NodeId, RealmId)>,
        peer: Option<NodeId>,
    ) -> Self {
        Self {
            launch,
            serving,
            peer,
            now_ms: 0,
            step: AnswerStep::Config,
        }
    }

    fn undecided(message: &str) -> ClosureError {
        ClosureError::Undecided(message.to_string())
    }

    /// The signed launch, when it names this node as its target and verifies.
    fn intent(&self) -> Result<&LaunchIntent, ClosureError> {
        let (local, realm_id) = self.serving.ok_or_else(|| Self::undecided("no network"))?;
        let envelope = self.launch.envelope();
        let JobFamilyRecord::Launch(intent) = &envelope.record else {
            return Err(Self::undecided("not a launch"));
        };
        if intent.target.node_id != local
            || envelope.realm_id != realm_id
            || envelope.published_by != intent.scheduler_node_id
            || envelope.verify_signature().is_err()
        {
            return Err(Self::undecided("the launch names another target"));
        }
        Ok(intent)
    }

    fn receipts(&mut self) -> Result<Next<LaunchClosureAnswer>, ClosureError> {
        self.intent()?;
        let family = self.launch.envelope().family();
        self.step = AnswerStep::Receipts;
        let read = LoadFamilyOperation::kind(family, JobRecordKind::Receipt);
        Ok(Next::Call(IoCall::child(read)))
    }

    /// The receipt of the launch, else the tombstone read that tells a pruned family.
    fn answer(
        &mut self,
        receipts: &[JobRecordEnvelope],
    ) -> Result<Next<LaunchClosureAnswer>, ClosureError> {
        let intent = self.intent()?;
        match existing_receipt(receipts, intent) {
            Some(Ok(receipt)) => {
                return Ok(Next::Done(LaunchClosureAnswer::Receipt(Box::new(receipt))));
            }
            // Another launch holds this launch id here, so this one stays open.
            Some(Err(_)) => return Err(Self::undecided("another launch holds this launch id")),
            None => {}
        }
        let family = self.launch.envelope().family();
        self.step = AnswerStep::Tombstone;
        let row = (
            FAMILY_TOMBSTONE_KEYSPACE.to_string(),
            family_prefix(&family),
        );
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn unreceipted(&self, tombstone: Option<Value>) -> Result<LaunchClosureAnswer, ClosureError> {
        let intent = self.intent()?;
        let tombstone = tombstone
            .map(|value| from_bytes::<FamilyTombstone>(&value))
            .transpose();
        let tombstone = tombstone.map_err(|error| ClosureError::Undecided(error.to_string()))?;
        // This node pruned the family only after every execution it ran there had stopped.
        if tombstone.is_some_and(|tombstone| tombstone.last_record_ms >= intent.created_at_ms) {
            return Ok(LaunchClosureAnswer::Pruned);
        }
        Ok(match intent.expired(self.now_ms) {
            true => LaunchClosureAnswer::NeverAccepted,
            false => LaunchClosureAnswer::TooEarly,
        })
    }
}

impl Operation for AnswerClosureOperation {
    type Output = LaunchClosureAnswer;
    type Error = ClosureError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<LaunchClosureAnswer>, ClosureError> {
        let Some((_, realm_id)) = self.serving.filter(|_| self.peer.is_some()) else {
            return self.receipts();
        };
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<LaunchClosureAnswer>, ClosureError> {
        match self.step {
            AnswerStep::Config => {
                let peer = self.peer.ok_or(IoError::Unfinished)?;
                if !member(result, peer)? {
                    return Err(ClosureError::Outsider);
                }
                self.receipts()
            }
            AnswerStep::Receipts => {
                let receipts = result.child::<Vec<JobRecordEnvelope>, FamilyReadError>()?;
                let receipts = receipts.map_err(|error| {
                    warn!(error = %error, "Receipt read failed; closure undecided");
                    ClosureError::Undecided(error.to_string())
                })?;
                self.answer(&receipts)
            }
            AnswerStep::Tombstone => Ok(Next::Done(self.unreceipted(result.value()?)?)),
        }
    }
}

/// What one closure query left behind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClosureOutcome {
    /// The target holds a receipt; the caller appends it.
    Receipt(JobRecordFrame),
    /// A closure row is stored.
    Closed,
    /// Nothing is stored; the query runs again later.
    Later,
}

#[derive(Debug, PartialEq)]
enum CloseStep {
    Ask,
    Write,
}

/// Asks the launch's target and stores a closure row when it answers never accepted or pruned. A
/// launch this node is the target of is answered here; no transport dials itself.
#[derive(Debug, PartialEq)]
pub struct CloseLaunchOperation {
    family: JobFamilyId,
    launch_id: Ulid,
    target: NodeId,
    launch: Option<LaunchFrame>,
    serving: (NodeId, RealmId),
    now_ms: u64,
    step: CloseStep,
}

type CloseNext = Result<Next<ClosureOutcome>, LifecycleError>;

impl CloseLaunchOperation {
    pub fn new(
        (family, launch): (JobFamilyId, LaunchFrame),
        serving: (NodeId, RealmId),
    ) -> Option<Self> {
        let JobFamilyRecord::Launch(intent) = &launch.envelope().record else {
            return None;
        };
        Some(Self {
            family,
            launch_id: intent.launch_id,
            target: intent.target.node_id,
            launch: Some(launch),
            serving,
            now_ms: 0,
            step: CloseStep::Ask,
        })
    }

    fn close(&mut self, kind: ClosureKind) -> CloseNext {
        let row = ClosureRow {
            kind,
            closed_at_ms: self.now_ms,
        };
        let value = Value::from(to_bytes(&row)?.as_slice());
        let key = launch_closure_key(&self.family, self.launch_id);
        self.step = CloseStep::Write;
        Ok(Next::Call(IoCall::Write(vec![(
            LAUNCH_CLOSURE_KEYSPACE.to_string(),
            key,
            value,
        )])))
    }

    fn answered(&mut self, answer: LaunchClosureAnswer) -> CloseNext {
        match answer {
            LaunchClosureAnswer::Receipt(receipt) => {
                match JobRecordFrame::new(receipt.into_inner()) {
                    Ok(frame) => Ok(Next::Done(ClosureOutcome::Receipt(frame))),
                    Err(error) => Err(LifecycleError::Store(error.into())),
                }
            }
            LaunchClosureAnswer::NeverAccepted => self.close(ClosureKind::NeverAccepted),
            LaunchClosureAnswer::Pruned => self.close(ClosureKind::Pruned),
            LaunchClosureAnswer::TooEarly => Ok(Next::Done(ClosureOutcome::Later)),
        }
    }

    fn later(message: impl std::fmt::Display) -> CloseNext {
        debug!(%message, "Launch target did not answer the closure query");
        Ok(Next::Done(ClosureOutcome::Later))
    }
}

impl Operation for CloseLaunchOperation {
    type Output = ClosureOutcome;
    type Error = LifecycleError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> CloseNext {
        let launch = self.launch.take().ok_or(IoError::Unfinished)?;
        if self.target == self.serving.0 {
            let answer = AnswerClosureOperation::new(launch, Some(self.serving), None);
            return Ok(Next::Call(IoCall::child(answer)));
        }
        let message = MetadataTransportMessage::QueryLaunchClosure {
            launch: Box::new(launch),
        };
        match ask_call(self.target, &message) {
            Some(call) => Ok(Next::Call(call)),
            None => Self::later("the closure query does not encode"),
        }
    }

    fn next(&mut self, result: IoResult) -> CloseNext {
        match self.step {
            CloseStep::Write => {
                result.done()?;
                Ok(Next::Done(ClosureOutcome::Closed))
            }
            CloseStep::Ask if self.target == self.serving.0 => {
                match result.child::<LaunchClosureAnswer, ClosureError>()? {
                    Ok(answer) => self.answered(answer),
                    Err(error) => Self::later(error),
                }
            }
            CloseStep::Ask => match reply_of(result) {
                Some(MetadataTransportMessage::LaunchClosureAnswered { answer }) => {
                    self.answered(answer)
                }
                Some(other) => Self::later(aruna_core::metadata::protocol::transport_message_kind(
                    &other,
                )),
                None => Self::later("no answer"),
            },
        }
    }
}

#[derive(Debug, PartialEq)]
enum PassStep {
    Closed,
    Ask,
    Append,
}

/// Asks the target of every launch of one family that has no receipt and no closure row and
/// whose closure time passed on this node's clock. Answers the launches closed here afterwards;
/// a node without a network asks nobody and answers the closures it holds.
#[derive(Debug, PartialEq)]
pub struct CloseLaunchesOperation {
    family: JobFamilyId,
    records: Vec<JobRecordEnvelope>,
    serving: Option<(NodeId, RealmId)>,
    now_ms: u64,
    closed: BTreeSet<Ulid>,
    open: VecDeque<LaunchFrame>,
    asking: Option<Ulid>,
    step: PassStep,
}

type PassNext = Result<Next<BTreeSet<Ulid>>, LifecycleError>;

impl CloseLaunchesOperation {
    pub fn new(
        family: JobFamilyId,
        records: Vec<JobRecordEnvelope>,
        serving: Option<(NodeId, RealmId)>,
    ) -> Self {
        Self {
            family,
            records,
            serving,
            now_ms: 0,
            closed: BTreeSet::new(),
            open: VecDeque::new(),
            asking: None,
            step: PassStep::Closed,
        }
    }

    fn page(&mut self, after: Option<Key>) -> PassNext {
        self.step = PassStep::Closed;
        Ok(Next::Call(IoCall::Page {
            space: LAUNCH_CLOSURE_KEYSPACE.to_string(),
            prefix: family_prefix(&self.family),
            after,
            limit: CLOSURE_PAGE,
        }))
    }

    /// The launches still open once every closure row is read.
    fn collect_open(&mut self) {
        let received: BTreeSet<Ulid> = (self.records.iter())
            .filter_map(|envelope| match &envelope.record {
                JobFamilyRecord::Receipt(receipt) => Some(receipt.launch_id),
                _ => None,
            })
            .collect();
        for envelope in &self.records {
            let JobFamilyRecord::Launch(launch) = &envelope.record else {
                continue;
            };
            let id = launch.launch_id;
            let open = !received.contains(&id) && !self.closed.contains(&id);
            if !open || launch.closure_due() > self.now_ms {
                continue;
            }
            if let Ok(frame) = LaunchFrame::new(envelope.clone()) {
                self.open.push_back(frame);
            }
        }
    }

    fn ask_next(&mut self) -> PassNext {
        let Some(serving) = self.serving else {
            return Ok(Next::Done(std::mem::take(&mut self.closed)));
        };
        while let Some(frame) = self.open.pop_front() {
            if let Some(close) = CloseLaunchOperation::new((self.family, frame), serving) {
                self.asking = Some(close.launch_id);
                self.step = PassStep::Ask;
                return Ok(Next::Call(IoCall::child(close)));
            }
        }
        Ok(Next::Done(std::mem::take(&mut self.closed)))
    }

    fn asked(&mut self, outcome: ClosureOutcome) -> PassNext {
        let (local, realm_id) = self.serving.ok_or(IoError::Unfinished)?;
        match outcome {
            ClosureOutcome::Closed => self.closed.extend(self.asking.take()),
            ClosureOutcome::Receipt(receipt) => {
                let target = receipt.envelope().published_by;
                let append = AppendRecordOperation::new(AppendRecordConfig {
                    realm_id,
                    local_node_id: local,
                    record: receipt,
                    local: None,
                    origin: RecordOrigin::Peer(target),
                    now_ms: self.now_ms,
                });
                self.step = PassStep::Append;
                return Ok(Next::Call(IoCall::child(append)));
            }
            ClosureOutcome::Later => {}
        }
        self.ask_next()
    }
}

impl Operation for CloseLaunchesOperation {
    type Output = BTreeSet<Ulid>;
    type Error = LifecycleError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> PassNext {
        self.page(None)
    }

    fn next(&mut self, result: IoResult) -> PassNext {
        match self.step {
            PassStep::Closed => {
                let IoResult::Page { rows, next } = result else {
                    let got = result.label();
                    let expected = "IoResult::Page";
                    return Err(IoError::Unexpected { expected, got }.into());
                };
                let skip = family_prefix(&self.family).len();
                for (key, value) in rows {
                    let id = key.get(skip..).and_then(|id| <[u8; 16]>::try_from(id).ok());
                    if let (Some(id), Ok(_)) = (id, from_bytes::<ClosureRow>(&value)) {
                        self.closed.insert(Ulid::from_bytes(id));
                    }
                }
                if let Some(after) = next {
                    return self.page(Some(after));
                }
                self.collect_open();
                self.ask_next()
            }
            PassStep::Ask => match result.child::<ClosureOutcome, LifecycleError>()? {
                Ok(outcome) => self.asked(outcome),
                Err(error) => {
                    warn!(error = %error, "Launch closure query failed");
                    self.ask_next()
                }
            },
            PassStep::Append => {
                if let Err(error) = result.child::<AppendOutcome, RecordStoreError>()? {
                    warn!(error = %error, "Receipt from a closure answer was not appended");
                }
                self.ask_next()
            }
        }
    }
}

#[cfg(test)]
#[path = "closure_tests.rs"]
mod tests;
