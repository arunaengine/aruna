//! The rows plugin installs keep about their native work in the work keyspace: the work row by
//! install and identity with its index by work id, and a task's submission facts beside it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::calls::{IoCall, IoError, IoResult};
use crate::id::NodeId;
use crate::job_request::SubmissionRequest;
use crate::keyspaces::PLUGIN_WORK_KEYSPACE;
use crate::plugin_installs::PluginInstall;
use crate::rows::{decode, encode, values};
use crate::structs::execution::job::family::JobFamilyId;
use crate::structs::execution::job::identity::JobId;
use crate::structs::execution::job::local::OutputObject;
use crate::types::Key;
use crate::user::id::UserId;

/// Row kinds of the work keyspace: the work by install and identity, and its identity by work id.
pub const WORK_ROW: u8 = b'w';
pub const WORK_INDEX: u8 = b'i';

/// What names one work across calls: the run and the ordinal of the call that started it, so a
/// rerun of that run gets the same work back.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Identity {
    Call { run_id: Ulid, ordinal: u32 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkKind {
    Copy,
    Download,
    Upload,
    Recipe,
}

/// What the work was approved under; a later manifest digest stops it before it runs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Binding {
    pub digest: String,
    pub revision: u64,
    pub recipe: Option<String>,
}

/// A transfer's request as resolved when the work started, without any secret, so the work
/// still runs after its run was pruned.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestContext {
    pub connection: String,
    /// The connection's base URL resolved from the run's saved settings.
    pub base: String,
    pub method: String,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
}

/// `Reserved` until its job is known, then `Dispatched`, and `Settled` once the summary is kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkState {
    Reserved,
    Dispatched,
    Settled,
}

/// How far a sending attempt came; an attempt that finds `Sending` never sends again.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SendBoundary {
    Sending {
        attempt: u32,
    },
    /// The answer: its status and its reply as exposed when stored, absent when too large, and
    /// the bytes and version sent.
    Sent {
        status: u16,
        reply: Option<String>,
        bytes: u64,
        version: Ulid,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FinalState {
    Succeeded,
    Failed,
    Cancelled,
    /// The outcome cannot be known; nothing is tried again.
    Unknown,
}

/// What a settled work answers after its job record is gone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    pub state: FinalState,
    /// The object path the work reads: a copy's source or an upload's object.
    pub source: Option<String>,
    /// The object path or prefix the work writes: a copy or download target, a recipe's outputs.
    pub target: Option<String>,
    pub message: Option<String>,
    pub bytes: Option<u64>,
    pub version_id: Option<Ulid>,
    /// The answer status of an upload, and its reply as exposed when it was stored.
    pub status: Option<u16>,
    pub reply: Option<String>,
    pub exit_code: Option<i32>,
    pub outputs: Vec<OutputObject>,
    pub report: Option<String>,
}

impl Summary {
    /// A summary of `state` with nothing else known yet.
    pub fn of(state: FinalState) -> Self {
        Self {
            state,
            source: None,
            target: None,
            message: None,
            bytes: None,
            version_id: None,
            status: None,
            reply: None,
            exit_code: None,
            outputs: Vec::new(),
            report: None,
        }
    }
}

/// One work of an install on its receiver.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkRow {
    /// The account of the install that wrote it; a reinstall under the same id has another one.
    pub account: UserId,
    pub work_id: Ulid,
    pub kind: WorkKind,
    /// The object path the work reads: a copy's source or an upload's object.
    pub source: Option<String>,
    /// The object path or prefix the work writes: a copy or download target, a recipe's outputs.
    pub target: Option<String>,
    /// BLAKE3 of the stable arguments, never of generated ids.
    pub digest: [u8; 32],
    pub binding: Binding,
    pub context: Option<RequestContext>,
    /// The run that started it; an audit reference only, the run may be pruned first.
    pub run_id: Ulid,
    /// The version a copy or download writes, minted once.
    pub version_id: Option<Ulid>,
    pub job: Option<JobId>,
    pub state: WorkState,
    pub boundary: Option<SendBoundary>,
    pub summary: Option<Summary>,
    pub notified: bool,
    pub cancel: bool,
    pub created_ms: u64,
    pub settled_ms: Option<u64>,
}

impl WorkRow {
    /// A new row `work_id` of `install` in state `Reserved`.
    pub fn reserved(
        install: &PluginInstall,
        kind: WorkKind,
        (run_id, work_id): (Ulid, Ulid),
        now_ms: u64,
    ) -> Self {
        Self {
            account: install.account,
            work_id,
            kind,
            source: None,
            target: None,
            digest: [0; 32],
            binding: Binding {
                digest: install.digest.clone(),
                revision: install.revision,
                recipe: None,
            },
            context: None,
            run_id,
            version_id: None,
            job: None,
            state: WorkState::Reserved,
            boundary: None,
            summary: None,
            notified: false,
            cancel: false,
            created_ms: now_ms,
            settled_ms: None,
        }
    }
}

pub fn row_key(install_id: Ulid, identity: &Identity) -> Result<Vec<u8>, IoError> {
    Ok([
        &[WORK_ROW][..],
        &install_id.to_bytes(),
        encode(identity)?.as_ref(),
    ]
    .concat())
}

pub fn index_key(install_id: Ulid, work_id: Ulid) -> Vec<u8> {
    [
        &[WORK_INDEX][..],
        &install_id.to_bytes(),
        &work_id.to_bytes(),
    ]
    .concat()
}

/// A read of one row of the work keyspace.
fn work_read(key: Vec<u8>) -> IoCall {
    IoCall::Read(vec![(PLUGIN_WORK_KEYSPACE.to_string(), Key::from(key))])
}

/// A sub-plan that reads the row of work `work_id` of an install and its key, through the index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkLookup {
    install_id: Ulid,
    work_id: Ulid,
    /// The row key the index named, once read.
    key: Option<Vec<u8>>,
}

/// What a lookup needs next, or the row it found.
#[derive(Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum Found {
    Ask(IoCall),
    Row(Option<(Vec<u8>, WorkRow)>),
}

impl WorkLookup {
    pub fn new(install_id: Ulid, work_id: Ulid) -> Self {
        Self {
            install_id,
            work_id,
            key: None,
        }
    }

    pub fn ask(&self) -> IoCall {
        let index = index_key(self.install_id, self.work_id);
        work_read(index)
    }

    pub fn take(&mut self, answer: IoResult) -> Result<Found, IoError> {
        let [value] = values::<1>(answer)?;
        let Some(value) = value.map(|value| value.to_vec()) else {
            return Ok(Found::Row(None));
        };
        let Some(key) = self.key.take() else {
            self.key = Some(value.clone());
            return Ok(Found::Ask(work_read(value)));
        };
        let row: WorkRow = decode(&value)?;
        Ok(Found::Row(
            (row.work_id == self.work_id).then_some((key, row)),
        ))
    }
}

/// Row kind of a task's submission facts in the work keyspace, beside its work row.
pub const TASK_ROW: u8 = b't';

/// How long after the first send holders may admit a task.
pub const ADMIT_WINDOW_MS: u64 = 60 * 60 * 1000;

/// Reports a task keeps, at most; a larger one is left out.
pub const MAX_REPORT: usize = 256 * 1024;

/// What the receiver knows about one task's submission, kept until the work row goes.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRow {
    /// Unix ms after which no holder admits the submission; fixed by the first send.
    pub admit_by: u64,
    /// Every node a send went to, in order; only these can hold an original claim.
    pub targets: Vec<NodeId>,
    /// A send ended without a definite answer, so a later refusal proves nothing.
    pub uncertain: bool,
    /// The request family of the adopted claim.
    pub family: Option<JobFamilyId>,
    /// The request of the first send, prepared and paged once, and its digest; every later
    /// send sends it unchanged, so no input resolves anew.
    pub request: Option<SubmissionRequest>,
    pub digest: Option<[u8; 32]>,
    /// The task's report, read from the container before any export; never in a bucket.
    pub report: Option<String>,
}

/// The key of work `work_id`'s task row.
pub fn task_key(install_id: Ulid, work_id: Ulid) -> Vec<u8> {
    [&[TASK_ROW][..], &install_id.to_bytes(), &work_id.to_bytes()].concat()
}

/// Where a task row is read and written.
pub fn task_place(install_id: Ulid, work_id: Ulid) -> (&'static str, Vec<u8>) {
    (PLUGIN_WORK_KEYSPACE, task_key(install_id, work_id))
}

/// The report bytes as text, `None` when there are too many of them or they are no UTF-8.
pub fn report_text(bytes: &[u8]) -> Option<String> {
    (bytes.len() <= MAX_REPORT)
        .then(|| String::from_utf8(bytes.to_vec()).ok())
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_needs_text() {
        assert_eq!(
            report_text(b"mapped 12 reads"),
            Some("mapped 12 reads".into())
        );
        assert_eq!(report_text(&[0xff, 0xfe]), None);
        assert_eq!(report_text(&vec![b'a'; MAX_REPORT + 1]), None);
    }

    #[test]
    fn keys_beside_work() {
        let (install, work) = (Ulid::from_parts(1, 1), Ulid::from_parts(2, 2));
        let key = task_key(install, work);
        assert_eq!(key[0], TASK_ROW);
        assert_eq!(&key[1..17], &install.to_bytes());
        assert_eq!(&key[17..], &work.to_bytes());
    }
}
