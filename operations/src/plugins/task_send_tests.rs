//! Tests what a task submission's sends prove: the decision per answer, a send past its deadline,
//! and a resend of the stored request that marks, sends and records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::structs::execution::job::{JobId, LogicalJobState, SubmissionId};
use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
use aruna_core::structured_id::{BucketId, PlacementHandle};

use super::*;
use crate::jobs::lifecycle::ids::{SubmissionRequest, SubmissionScope};
use crate::plugins::rows::encode;
use crate::plugins::rows::tests::values_result;
use crate::plugins::work::tests::sample_install;
use crate::tests::records::{Family, local_ingress, node, payload, user};

fn family() -> JobFamilyId {
    JobFamilyId {
        submission_id: SubmissionId([1; 32]),
        request_digest: [2; 32],
    }
}

fn alias() -> JobId {
    let handle = PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap();
    JobId::from_parts(1_700_000_000_000, handle, BucketId::new(0).unwrap(), 7).unwrap()
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

#[test]
fn first_send_refusal() {
    let refused = || Sent::Refused("quota".to_string());
    assert_eq!(
        decide(1, false, refused()),
        SendOutcome::Fail("quota".to_string())
    );
    // A second send, or an earlier lost answer, makes any refusal uncertain.
    assert_eq!(decide(2, false, refused()), SendOutcome::Keep);
    assert_eq!(decide(1, true, refused()), SendOutcome::Keep);
    let lost = Sent::Uncertain("lost".to_string());
    assert_eq!(decide(1, false, lost), SendOutcome::Keep);
    let accepted = Sent::Accepted {
        alias: alias(),
        family: family(),
    };
    let dispatch = SendOutcome::Dispatch {
        alias: alias(),
        family: family(),
    };
    assert_eq!(decide(3, true, accepted), dispatch);
}

#[test]
fn classifies_answers() {
    let sorted = |error| classify(Err(error), family());
    assert!(matches!(
        sorted(SubmitJobError::AuthorityDenied),
        Sent::Refused(_)
    ));
    let invalid = SubmitJobError::InvalidWorkspace("past its deadline".to_string());
    assert!(matches!(sorted(invalid), Sent::Refused(_)));
    let lost = SubmitJobError::PlacementUnavailable("no answer".to_string());
    assert!(matches!(sorted(lost), Sent::Uncertain(_)));
    // Another request already holds the key: something was admitted.
    let conflict = SubmitJobError::JobPlanConflict {
        existing_job_id: alias(),
    };
    assert!(matches!(sorted(conflict), Sent::Uncertain(_)));
    let accepted = AcceptedSubmission {
        job_id: alias(),
        created: true,
        submission_id: family().submission_id,
        state: LogicalJobState::Queued,
    };
    let sent = classify(Ok(accepted), family());
    let expected = Sent::Accepted {
        alias: alias(),
        family: family(),
    };
    assert_eq!(sent, expected);
}

fn send(now_ms: u64) -> SendTask {
    let install = sample_install(Ulid::from_parts(1, 1));
    let mut node = local_ingress();
    node.now_ms = now_ms;
    SendTask::new(&install, Ulid::from_parts(2, 2), None, node)
}

fn stored_task(admit_by: u64) -> IoResult {
    let request = SubmissionRequest {
        created_by: user(),
        spec: payload(),
        scope: SubmissionScope::Keyed("plugin-work/1".to_string()),
        retention_ms: 1,
        ingress_node_id: node(1),
        captured_inputs: Vec::new(),
        output_policies: Vec::new(),
        input_pages: None,
        admit_by: Some(admit_by),
    };
    let task = TaskRow {
        admit_by,
        request: Some(request),
        digest: Some([7; 32]),
        ..TaskRow::default()
    };
    values_result(vec![Some(encode(&task).unwrap())])
}

#[test]
fn late_send_skipped() {
    // Past the deadline nothing is sent; the settle pass proves the outcome.
    let mut late = send(10_000);
    assert!(matches!(late.first(), Ok(Next::Call(IoCall::Read(_)))));
    assert_eq!(late.next(stored_task(5_000)), Ok(Next::Done(())));
}

#[test]
fn resend_marks_first() {
    let mut config = Family::new([4; 32]).config;
    config.seed_job_control(node(1), 0);
    let mut resend = send(1_000);
    resend.first().unwrap();
    assert!(matches!(
        resend.next(stored_task(5_000)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let read = child(Ok::<_, GetConfigError>(config));
    assert!(matches!(
        resend.next(read),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert!(matches!(resend.phase, SendPhase::Mark(_)));
    // A row that is no longer reserved is not sent to.
    let unchanged = child(Ok::<_, StoreError>(Changed::default()));
    assert_eq!(resend.next(unchanged), Ok(Next::Done(())));
}
