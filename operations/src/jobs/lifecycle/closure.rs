//! Closes launches without a receipt: the target's read-only answer, the holder's query and the
//! pass that asks the target of every open launch of one family.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, VecDeque};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, JobRecordFrame, LaunchFrame, NetEffect};
use aruna_core::events::{ClosureAnswer, Event, MetadataAnswer, NetEvent};
use aruna_core::keyspaces::LAUNCH_CLOSURE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::record_keys::family_prefix;
use aruna_core::structs::execution::job::record_rows::{from_bytes, to_bytes};
use aruna_core::structs::execution::job::{
    JobFamilyId, JobFamilyRecord, JobRecordEnvelope, JobRecordKind, LaunchIntent,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{Key, Value};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, warn};
use ulid::Ulid;

use super::LifecycleError;
use super::launch_admit::existing_receipt;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::jobs::records::RecordStoreError;
use crate::jobs::records::append::{
    AppendOutcome, AppendRecordConfig, AppendRecordOperation, RecordOrigin,
};
use crate::jobs::records::family::{FamilyReadError, LoadFamilyOperation};
use crate::metadata::protocol::{
    MetadataTransportMessage, decode_message, encode_message, metadata_request,
};

/// Wall-clock budget of one closure query round trip.
const QUERY_DEADLINE: Duration = Duration::from_secs(10);

/// Closure rows one page of the holder pass reads.
const CLOSURE_PAGE: usize = 512;

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

pub fn closure_key(family: &JobFamilyId, launch_id: Ulid) -> Key {
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

    fn receipts(&mut self) -> Result<Next<ClosureAnswer>, ClosureError> {
        self.intent()?;
        let family = self.launch.envelope().family();
        self.step = AnswerStep::Receipts;
        let read = LoadFamilyOperation::kind(family, JobRecordKind::Receipt);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn answer(&self, receipts: &[JobRecordEnvelope]) -> Result<ClosureAnswer, ClosureError> {
        let intent = self.intent()?;
        match existing_receipt(receipts, intent) {
            Some(Ok(receipt)) => return Ok(ClosureAnswer::Receipt(Box::new(receipt))),
            // Another launch holds this launch id here, so this one stays open.
            Some(Err(_)) => return Err(Self::undecided("another launch holds this launch id")),
            None => {}
        }
        Ok(match intent.expired(self.now_ms) {
            true => ClosureAnswer::NeverAccepted,
            false => ClosureAnswer::TooEarly,
        })
    }
}

impl Operation for AnswerClosureOperation {
    type Output = ClosureAnswer;
    type Error = ClosureError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<ClosureAnswer>, ClosureError> {
        let Some((_, realm_id)) = self.serving.filter(|_| self.peer.is_some()) else {
            return self.receipts();
        };
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ClosureAnswer>, ClosureError> {
        match self.step {
            AnswerStep::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|error| ClosureError::Undecided(error.to_string()))?;
                let members = config.sync_eligible_nodes().unwrap_or_default();
                if !self.peer.is_some_and(|peer| members.contains(&peer)) {
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
                Ok(Next::Done(self.answer(&receipts)?))
            }
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
        let key = closure_key(&self.family, self.launch_id);
        self.step = CloseStep::Write;
        Ok(Next::Call(IoCall::Write(vec![(
            LAUNCH_CLOSURE_KEYSPACE.to_string(),
            key,
            value,
        )])))
    }

    fn answered(&mut self, answer: ClosureAnswer) -> CloseNext {
        match answer {
            ClosureAnswer::Receipt(receipt) => match JobRecordFrame::new(receipt.into_inner()) {
                Ok(frame) => Ok(Next::Done(ClosureOutcome::Receipt(frame))),
                Err(error) => Err(LifecycleError::Store(error.into())),
            },
            ClosureAnswer::NeverAccepted => self.close(ClosureKind::NeverAccepted),
            ClosureAnswer::Pruned => self.close(ClosureKind::Pruned),
            ClosureAnswer::TooEarly => Ok(Next::Done(ClosureOutcome::Later)),
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
        let message = MetadataTransportMessage::ForwardClosureQuery {
            launch: Box::new(launch),
        };
        let Ok(frame) = encode_message(&message) else {
            return Self::later("the closure query does not encode");
        };
        let request = metadata_request(self.target, frame, QUERY_DEADLINE);
        let effect = Effect::Net(NetEffect::MetadataRequest(Box::new(request)));
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }

    fn next(&mut self, result: IoResult) -> CloseNext {
        match self.step {
            CloseStep::Write => {
                result.done()?;
                Ok(Next::Done(ClosureOutcome::Closed))
            }
            CloseStep::Ask if self.target == self.serving.0 => {
                match result.child::<ClosureAnswer, ClosureError>()? {
                    Ok(answer) => self.answered(answer),
                    Err(error) => Self::later(error),
                }
            }
            CloseStep::Ask => {
                let Event::Net(NetEvent::MetadataAnswer(answer)) = result.event()? else {
                    return Self::later("another event");
                };
                let MetadataAnswer { result, .. } = *answer;
                let reply = match result {
                    Ok(bytes) => decode_message(&bytes).map_err(|error| error.to_string()),
                    Err(error) => Err(format!("{error:?}")),
                };
                match reply {
                    Ok(MetadataTransportMessage::ForwardedClosureQuery { answer }) => {
                        self.answered(answer)
                    }
                    Ok(other) => Self::later(crate::metadata::transport_message_kind(&other)),
                    Err(message) => Self::later(message),
                }
            }
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
/// whose closure time passed on this node's clock. Answers how many launches it closed.
#[derive(Debug, PartialEq)]
pub struct CloseLaunchesOperation {
    family: JobFamilyId,
    records: Vec<JobRecordEnvelope>,
    serving: (NodeId, RealmId),
    now_ms: u64,
    closed: BTreeSet<Ulid>,
    open: VecDeque<LaunchFrame>,
    count: usize,
    step: PassStep,
}

type PassNext = Result<Next<usize>, LifecycleError>;

impl CloseLaunchesOperation {
    pub fn new(
        family: JobFamilyId,
        records: Vec<JobRecordEnvelope>,
        serving: (NodeId, RealmId),
    ) -> Self {
        Self {
            family,
            records,
            serving,
            now_ms: 0,
            closed: BTreeSet::new(),
            open: VecDeque::new(),
            count: 0,
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
        while let Some(frame) = self.open.pop_front() {
            if let Some(close) = CloseLaunchOperation::new((self.family, frame), self.serving) {
                self.step = PassStep::Ask;
                return Ok(Next::Call(IoCall::child(close)));
            }
        }
        Ok(Next::Done(self.count))
    }

    fn asked(&mut self, outcome: ClosureOutcome) -> PassNext {
        match outcome {
            ClosureOutcome::Closed => self.count += 1,
            ClosureOutcome::Receipt(receipt) => {
                let target = receipt.envelope().published_by;
                let append = AppendRecordOperation::new(AppendRecordConfig {
                    realm_id: self.serving.1,
                    local_node_id: self.serving.0,
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
    type Output = usize;
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
