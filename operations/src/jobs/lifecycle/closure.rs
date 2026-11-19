//! Answers whether a plugin task's submission was admitted here after its deadline: a stored
//! claim, or no claim together with a closure row that fences every later admission.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, MetadataAnswer, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::jobs::ClosureAnswer;
use aruna_core::keyspaces::{FAMILY_RECORD_KEYSPACE, SUBMISSION_CLOSURE_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::job::record_keys::submission_prefix;
use aruna_core::structs::execution::job::record_rows::from_bytes;
use aruna_core::structs::execution::job::{JobFamilyRecord, JobRecordEnvelope, SubmissionId};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{Key, Value};

use super::LifecycleError;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::jobs::records::RECORD_PAGE_SIZE;
use crate::metadata::protocol::{
    MetadataTransportMessage, decode_message, encode_message, metadata_request,
};

/// How far apart node clocks may be; a closure row stays this long past its deadline, and the
/// receiver asks only this long after it.
pub const CLOCK_MARGIN_MS: u64 = 10 * 60 * 1000;

/// How long one closure or record question to a node may take.
pub(crate) const ASK_DEADLINE: Duration = Duration::from_secs(10);

/// Closure rows one sweep pass reads at a time.
const SWEEP_PAGE: usize = 500;

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
        let config = result.child::<RealmConfigDocument, GetConfigError>()?;
        let eligible = config
            .ok()
            .and_then(|config| config.sync_eligible_nodes().ok());
        if !eligible.is_some_and(|nodes| nodes.contains(&self.peer)) {
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
        let Ok(frame) = encode_message(&message) else {
            return Ok(Next::Done(ClosureAnswer::Undecided));
        };
        let request = metadata_request(self.node, frame, ASK_DEADLINE);
        let effect = Effect::Net(NetEffect::MetadataRequest(Box::new(request)));
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ClosureAnswer>, IoError> {
        let answer = match result {
            IoResult::Event(event) => match *event {
                Event::Net(NetEvent::MetadataAnswer(answer)) => answered(*answer),
                _ => ClosureAnswer::Undecided,
            },
            _ => ClosureAnswer::Undecided,
        };
        Ok(Next::Done(answer))
    }
}

fn answered(answer: MetadataAnswer) -> ClosureAnswer {
    let reply = (answer.result.ok()).and_then(|bytes| decode_message(&bytes).ok());
    match reply {
        Some(MetadataTransportMessage::SubmissionClaimAnswered { answer }) => answer,
        _ => ClosureAnswer::Undecided,
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

#[cfg(test)]
#[path = "closure_tests.rs"]
mod tests;
