//! Witness ranking, launch suppression, stored budgets, and staging refusals.

use aruna_core::structs::{JobErrorKind, JobFamilyRecord, PhysicalExecutionState};
use std::collections::BTreeSet;

use crate::jobs::lifecycle::stage::stage_error;
use crate::jobs::lifecycle::witness::{suppressed, witness_rank};
use crate::jobs::records::tests::fixture::{Family, node};
use crate::replication::bao_read::BaoReadError;

#[test]
fn ranks_witnesses() {
    // Rank is a property of the identity and the holder set, not of the order
    // the holders were observed in, and every holder gets a distinct position.
    let family = Family::new([1u8; 32]);
    let holders: Vec<_> = (1..=4u8).map(node).collect();
    let mut shuffled = holders.clone();
    shuffled.reverse();

    let ranks: Vec<_> = holders
        .iter()
        .map(|holder| {
            witness_rank(&holders, &family.family(), *holder, None).expect("holder ranks")
        })
        .collect();
    let reversed: Vec<_> = holders
        .iter()
        .map(|holder| {
            witness_rank(&shuffled, &family.family(), *holder, None).expect("holder ranks")
        })
        .collect();

    assert_eq!(ranks, reversed);
    let mut sorted = ranks.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), holders.len());
    assert_eq!(
        witness_rank(&holders, &family.family(), node(9), None),
        None
    );
}

#[test]
fn admitting_ranks_first() {
    // The admitting node plans first and the remaining witnesses keep their
    // digest order behind it; a non-holder never shifts anyone.
    let family = Family::new([1u8; 32]);
    let holders: Vec<_> = (1..=4u8).map(node).collect();
    let admitting = Some(node(3));

    let ranks: Vec<_> = holders
        .iter()
        .map(|holder| {
            witness_rank(&holders, &family.family(), *holder, admitting).expect("holder ranks")
        })
        .collect();

    assert_eq!(
        witness_rank(&holders, &family.family(), node(3), admitting),
        Some(0)
    );
    let mut sorted = ranks.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, vec![0, 1, 2, 3]);
    let outsider = Some(node(9));
    for holder in &holders {
        assert_eq!(
            witness_rank(&holders, &family.family(), *holder, outsider),
            witness_rank(&holders, &family.family(), *holder, None)
        );
    }
}

#[test]
fn suppresses_after_receipt() {
    // Permanent failure stops retries; infrastructure error remains retryable.
    let family = Family::new([1u8; 32]);
    let running = family.run(1, 0, PhysicalExecutionState::Running);
    let succeeded = family.run(1, 0, PhysicalExecutionState::Succeeded);
    let failed = family.run(1, 0, PhysicalExecutionState::Failed);
    let error = family.run(1, 0, PhysicalExecutionState::Error);
    let live = BTreeSet::new();

    assert!(suppressed(family.family(), &running, &live));
    assert!(suppressed(family.family(), &succeeded, &live));
    assert!(suppressed(family.family(), &failed, &live));
    assert!(!suppressed(family.family(), &error, &live));
}

#[test]
fn silent_node_replans() {
    // An unfinished execution on a node that stopped reporting stops
    // suppressing, while its terminal outcomes still do.
    let family = Family::new([1u8; 32]);
    let running = family.run(1, 0, PhysicalExecutionState::Running);
    let succeeded = family.run(1, 0, PhysicalExecutionState::Succeeded);
    let failed = family.run(1, 0, PhysicalExecutionState::Failed);
    let silent = BTreeSet::from([family.target.public()]);

    assert!(!suppressed(family.family(), &running, &silent));
    assert!(suppressed(family.family(), &succeeded, &silent));
    assert!(suppressed(family.family(), &failed, &silent));
}

#[test]
fn live_node_suppresses() {
    // Another node going silent says nothing about the node that runs the work.
    let family = Family::new([1u8; 32]);
    let running = family.run(1, 0, PhysicalExecutionState::Running);
    let elsewhere = BTreeSet::from([node(2)]);

    assert!(suppressed(family.family(), &running, &elsewhere));
}

#[test]
fn cancel_suppresses_launch() {
    // A cancellation stops new launches even while an execution is running,
    // and the receipted execution itself is not erased from the family.
    let family = Family::new([1u8; 32]);
    let mut records = family.run(1, 0, PhysicalExecutionState::Running);
    let spec = family.spec();
    records.push(family.sign(
        &family.holder,
        JobFamilyRecord::Cancel(family.cancel(&spec)),
    ));

    assert!(suppressed(family.family(), &records, &BTreeSet::new()));
    assert!(
        records
            .iter()
            .any(|envelope| matches!(&envelope.record, JobFamilyRecord::Receipt(_)))
    );
}

#[test]
fn bounds_launch_sequence() {
    // A launch outside the stored budget is never admitted, whatever the
    // scheduler claims about its own sequence.
    let family = Family::new([1u8; 32]);
    let spec = family.spec();
    let budget = family.budget(&spec, family.holder.public());

    assert!(
        budget
            .admits(&family.launch(&spec, family.holder.public(), 1))
            .is_ok()
    );
    assert!(
        budget
            .admits(&family.launch(&spec, family.holder.public(), 2))
            .is_err()
    );
    assert!(budget.admits(&family.launch(&spec, node(2), 0)).is_err());
}

#[test]
fn denied_stage_fails() {
    // A policy refusal fails this attempt permanently; an unreachable holder is
    // retryable, so a transient outage never terminalizes an execution.
    assert_eq!(
        stage_error("data", "reads.fastq", Some(BaoReadError::NoDestination)).kind,
        JobErrorKind::Permanent
    );
    assert_eq!(
        stage_error(
            "data",
            "reads.fastq",
            Some(BaoReadError::PolicyRequired { refs: Vec::new() })
        )
        .kind,
        JobErrorKind::Permanent
    );
    assert_eq!(
        stage_error("data", "reads.fastq", None).kind,
        JobErrorKind::Retryable
    );
}
