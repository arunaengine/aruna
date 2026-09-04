//! The reducer must be a function of the record set alone.

use aruna_core::structs::{
    ExecutionRole, JobFamilyRecord, JobId, JobProjection, JobRecordBody, JobRecordEnvelope,
    LogicalJobState, PhysicalExecutionState, canonical_execution_key,
};

use super::fixture::Family;
use crate::jobs::records::reduce::{canonical_binding, reduce_family, submission_families};

/// Deterministic shuffle: an index rotation plus every record duplicated once,
/// so replay and batching are covered without unseeded randomness.
fn shuffled(records: &[JobRecordEnvelope], rotation: usize) -> Vec<JobRecordEnvelope> {
    let mut shuffled: Vec<JobRecordEnvelope> = Vec::with_capacity(records.len() * 2);
    for index in 0..records.len() {
        shuffled.push(records[(index + rotation) % records.len()].clone());
    }
    shuffled.extend(records.iter().rev().cloned());
    shuffled
}

fn project(family: &Family, records: &[JobRecordEnvelope]) -> JobProjection {
    reduce_family(family.family(), records)
        .expect("records reduce")
        .expect("family has an accepted alias")
}

#[test]
fn reduces_any_order() {
    // Every rotation, reversal, and duplication of one record set must reduce
    // to the same projection and the same revision digest.
    let family = Family::new([1u8; 32]);
    let records = family.run(1, 0, PhysicalExecutionState::Succeeded);
    let expected = project(&family, &records);

    for rotation in 0..records.len() {
        let reduced = project(&family, &shuffled(&records, rotation));
        assert_eq!(reduced, expected);
        assert_eq!(reduced.digest(), expected.digest());
    }
    assert_eq!(expected.state, LogicalJobState::Succeeded);
    assert_eq!(expected.outputs.as_slice().len(), 1);
}

#[test]
fn keeps_start_time() {
    // The first running update is the start; without one the acceptance of the
    // launch is the closest replicated moment.
    let family = Family::new([8u8; 32]);
    let spec = family.spec();
    let launch = family.launch(&spec, family.holder.public(), 0);
    let receipt = family.receipt(&launch, 1);
    let running = family.update(
        &receipt,
        0,
        receipt.digest().expect("receipt digest"),
        PhysicalExecutionState::Running,
        None,
    );
    let accepted = vec![
        family.sign(
            &family.holder,
            JobFamilyRecord::Spec(Box::new(spec.clone())),
        ),
        family.sign(&family.holder, JobFamilyRecord::Claim(family.claim(&spec))),
        family.sign(
            &family.target,
            JobFamilyRecord::Receipt(Box::new(receipt.clone())),
        ),
    ];
    let mut started = accepted.clone();
    started.push(family.sign(
        &family.target,
        JobFamilyRecord::Update(Box::new(running.clone())),
    ));

    assert_eq!(
        project(&family, &accepted).executions[0].started_at_ms,
        Some(receipt.accepted_at_ms)
    );
    assert_eq!(
        project(&family, &started).executions[0].started_at_ms,
        Some(running.observed_at_ms)
    );
}

#[test]
fn carries_log_tails() {
    // The reduced result must carry the bounded stdout and stderr tails, so a
    // node that never ran the container still answers a status read with them.
    let family = Family::new([7u8; 32]);
    let records = family.run(1, 0, PhysicalExecutionState::Succeeded);
    let projection = project(&family, &records);

    let canonical = projection
        .canonical_execution_id
        .expect("a success is canonical");
    let result = projection
        .executions
        .iter()
        .find(|execution| execution.execution_id == canonical)
        .and_then(|execution| execution.result.as_ref())
        .expect("the canonical execution has a result");
    assert_eq!(
        result.stdout.as_ref().map(|tail| tail.as_str()),
        Some("out tail")
    );
    assert_eq!(
        result.stderr.as_ref().map(|tail| tail.as_str()),
        Some("err tail")
    );
}

#[test]
fn picks_canonical_success() {
    // Two successful executions of one family: the smallest canonical key wins
    // and the other stays visible as a duplicate success.
    let family = Family::new([2u8; 32]);
    let mut records = family.run(1, 0, PhysicalExecutionState::Succeeded);
    records.extend(family.run(2, 1, PhysicalExecutionState::Succeeded));
    let projection = project(&family, &records);

    let canonical = projection
        .canonical_execution_id
        .expect("a success is canonical");
    let expected = projection
        .executions
        .iter()
        .map(|execution| execution.execution_id)
        .min_by_key(|execution_id| {
            canonical_execution_key(family.submission_id, family.request_digest, *execution_id)
        })
        .expect("executions are projected");
    assert_eq!(canonical, expected);
    assert_eq!(projection.executions.len(), 2);
    assert_eq!(
        projection
            .executions
            .iter()
            .filter(|execution| execution.role == ExecutionRole::DuplicateSuccess)
            .count(),
        1
    );
    assert_eq!(project(&family, &shuffled(&records, 3)), projection);
}

#[test]
fn merges_partitions() {
    // Two partitions that each saw one execution converge on one canonical
    // result whichever way their records merge, and keep both executions.
    let family = Family::new([3u8; 32]);
    let left = family.run(1, 0, PhysicalExecutionState::Succeeded);
    let right = family.run(2, 1, PhysicalExecutionState::Succeeded);

    let mut forward = left.clone();
    forward.extend(right.clone());
    let mut backward = right;
    backward.extend(left);

    let merged = project(&family, &forward);
    assert_eq!(merged, project(&family, &backward));
    assert_eq!(merged.executions.len(), 2);
    assert_eq!(merged.state, LogicalJobState::Succeeded);
}

#[test]
fn keeps_state_indeterminate() {
    // A terminal infrastructure error does not prove a realm-wide failure.
    let family = Family::new([4u8; 32]);
    let records = family.run(1, 0, PhysicalExecutionState::Error);
    let projection = project(&family, &records);
    assert_eq!(projection.state, LogicalJobState::Indeterminate);
    assert!(projection.canonical_execution_id.is_none());
    assert!(projection.outputs.as_slice().is_empty());
}

#[test]
fn projects_permanent_failure() {
    // A signed permanent job failure is terminal and suppresses retries.
    let family = Family::new([8u8; 32]);
    let records = family.run(1, 0, PhysicalExecutionState::Failed);
    let projection = project(&family, &records);

    assert_eq!(projection.state, LogicalJobState::Failed);
    assert!(projection.canonical_execution_id.is_some());
    assert!(projection.outputs.as_slice().is_empty());
}

#[test]
fn projects_cancellation() {
    // A cancel suppresses scheduling and projects cancelled once no execution
    // is active; a success observed later still wins with the flag set.
    let family = Family::new([5u8; 32]);
    let spec = family.spec();
    let claim = family.claim(&spec);
    let cancel = family.cancel(&spec);
    let records = vec![
        family.sign(&family.holder, JobFamilyRecord::Spec(Box::new(spec))),
        family.sign(&family.holder, JobFamilyRecord::Claim(claim)),
        family.sign(&family.holder, JobFamilyRecord::Cancel(cancel)),
    ];
    let projection = project(&family, &records);
    assert_eq!(projection.state, LogicalJobState::Cancelled);
    assert!(projection.cancel_requested);

    let mut late = records.clone();
    late.extend(family.run(1, 0, PhysicalExecutionState::Succeeded));
    let projection = project(&family, &late);
    assert_eq!(projection.state, LogicalJobState::Succeeded);
    assert!(projection.cancel_requested);
}

#[test]
fn unions_same_request() {
    // Two holders accepted the same normalized request while partitioned: both
    // aliases belong to one projection, in claim-key order.
    let family = Family::new([6u8; 32]);
    let mine = family.spec();
    let other_id = JobId::from_bytes([12u8; 16]);
    let other = family.spec_for(other_id, family.holder.public());
    let records = vec![
        family.sign(
            &family.holder,
            JobFamilyRecord::Spec(Box::new(mine.clone())),
        ),
        family.sign(&family.holder, JobFamilyRecord::Claim(family.claim(&mine))),
        family.sign(
            &family.holder,
            JobFamilyRecord::Spec(Box::new(other.clone())),
        ),
        family.sign(&family.holder, JobFamilyRecord::Claim(family.claim(&other))),
    ];
    let projection = project(&family, &records);
    assert_eq!(projection.aliases.len(), 2);
    assert!(projection.aliases.contains(&other_id));
    assert_eq!(projection.canonical_job_id, projection.aliases[0]);
    assert_eq!(project(&family, &shuffled(&records, 2)), projection);
}

#[test]
fn separates_conflicts() {
    // A second request digest under one submission is its own family: it never
    // contributes to the canonical request's projection.
    let canonical = Family::new([1u8; 32]);
    let conflict = Family::new([2u8; 32]);
    let mut records = canonical.run(1, 0, PhysicalExecutionState::Succeeded);
    records.extend(conflict.run(2, 0, PhysicalExecutionState::Succeeded));

    let projection = project(&canonical, &records);
    assert_eq!(projection.executions.len(), 1);
    assert_eq!(projection.request_digest, canonical.request_digest);

    let families = submission_families(&records);
    assert_eq!(families.len(), 2);
    assert_eq!(canonical_binding(&records), Some(families[0]));
    let mut reversed: Vec<JobRecordEnvelope> = records.clone();
    reversed.reverse();
    assert_eq!(submission_families(&reversed), families);
}
