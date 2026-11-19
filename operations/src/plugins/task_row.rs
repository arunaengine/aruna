//! What the receiver keeps about one plugin task's submission beside its work row: the deadline,
//! the nodes a send went to, the adopted family, the stored request and the task's report.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::keyspaces::PLUGIN_WORK_KEYSPACE;
use aruna_core::structs::execution::job::JobFamilyId;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::jobs::lifecycle::ids::SubmissionRequest;

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
