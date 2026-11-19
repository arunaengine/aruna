//! Reads this node's applied realm config clock and node intervals, and applies the not-run rule.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::AdminDocumentTarget;
use aruna_core::calls::{IoError, IoResult, Next};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::execution::job::{
    JobProjection, JobRecordEnvelope, LogicalJobState, NotRun, not_run,
};
use aruna_core::structs::identity::realm::RealmId;
use tracing::warn;

use crate::secrets::owner::WriterView;

/// The realm config clock and node history this node has applied. `None` when the reducer
/// state is unreadable; a node without any state has an empty view.
#[derive(Debug, PartialEq)]
pub struct MembershipViewOperation {
    realm_id: RealmId,
}

impl MembershipViewOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self { realm_id }
    }
}

impl Operation for MembershipViewOperation {
    type Output = Option<WriterView>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, IoError> {
        let target = AdminDocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        Ok(Next::Call(read_row(
            DOCUMENT_STATE_KEYSPACE,
            reducer_state_key(&target),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, IoError> {
        let value = result.value()?;
        match WriterView::read(value.as_deref()) {
            Ok(view) => Ok(Next::Done(Some(view))),
            Err(error) => {
                warn!(error = %error, "Realm config state is unreadable; membership unknown");
                Ok(Next::Done(None))
            }
        }
    }
}

/// The executions and launches of a family that are not run in `view`.
pub fn family_not_run(records: &[JobRecordEnvelope], view: &WriterView) -> NotRun {
    not_run(records, &view.intervals, &view.clock)
}

/// Shows an undecided family whose open work all belongs to nodes that left as not run. A
/// success, a signed failure or a cancel stays the outcome.
pub fn mark_not_run(projection: &mut JobProjection, not_run: &NotRun) {
    let undecided = matches!(
        projection.state,
        LogicalJobState::Queued | LogicalJobState::Running | LogicalJobState::Indeterminate
    );
    if undecided && not_run.family {
        projection.state = LogicalJobState::NotRun;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn projection(state: LogicalJobState) -> JobProjection {
        use aruna_core::structs::execution::job::{JobId, OutputSet, SubmissionId};
        JobProjection {
            submission_id: SubmissionId([1u8; 32]),
            request_digest: [2u8; 32],
            canonical_job_id: JobId::from_bytes([3u8; 16]),
            aliases: Vec::new(),
            state,
            canonical_execution_id: None,
            executions: Vec::new(),
            outputs: OutputSet::new(Vec::new()).expect("empty outputs"),
            cancel_requested: false,
            expiry_reason: None,
        }
    }

    // Only an undecided family reads as not run; an outcome or a cancel stays as it is.
    #[test]
    fn marks_undecided_only() {
        let derived = NotRun {
            family: true,
            ..NotRun::default()
        };
        for (state, shown) in [
            (LogicalJobState::Indeterminate, LogicalJobState::NotRun),
            (LogicalJobState::Running, LogicalJobState::NotRun),
            (LogicalJobState::Succeeded, LogicalJobState::Succeeded),
            (LogicalJobState::Cancelled, LogicalJobState::Cancelled),
        ] {
            let mut read = projection(state);
            mark_not_run(&mut read, &derived);
            assert_eq!(read.state, shown, "{state:?}");
        }
        let mut read = projection(LogicalJobState::Running);
        mark_not_run(&mut read, &NotRun::default());
        assert_eq!(read.state, LogicalJobState::Running);
    }

    // No state yet is the empty view; an unreadable state leaves membership unknown.
    #[test]
    fn view_of_state() {
        let mut empty = MembershipViewOperation::new(RealmId([1; 32]));
        assert!(matches!(empty.first(), Ok(Next::Call(_))));
        let none = empty.next(IoResult::Values(vec![None]));
        assert_eq!(none, Ok(Next::Done(Some(WriterView::default()))));
        let mut broken = MembershipViewOperation::new(RealmId([1; 32]));
        broken.first().unwrap();
        let garbage = IoResult::Values(vec![Some(b"garbage".to_vec().into())]);
        assert_eq!(broken.next(garbage), Ok(Next::Done(None)));
    }
}
