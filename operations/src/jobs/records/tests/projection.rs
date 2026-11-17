//! Tests family projection truncation, cache reuse, and corrupt or outdated cache rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{FAMILY_PROJECTION_KEYSPACE, JOB_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    JobPayload, JobRecord, JobRecordEnvelope, JobState, LogicalJobState, PhysicalExecutionState,
};
use aruna_core::types::{Key, Value};

use crate::jobs::records::project::{
    FamilyRef, ProjectFamilyConfig, ProjectFamilyOperation, ProjectedFamily,
};
use crate::jobs::records::reduce::reduce_family;
use crate::jobs::records::{MAX_PROJECTION_RECORDS, RecordStoreError};
use crate::tests::records::{Family, payload, user};
use aruna_core::structs::execution::job::JobWrites;
use aruna_core::structs::execution::job::record_keys::record_key;
use aruna_core::structs::execution::job::record_rows::{
    PROJECTION_CACHE_VERSION, ProjectionCache, to_bytes,
};

/// What one sans-I/O run of the projection did, with no storage behind it.
struct Run {
    outcome: Result<ProjectedFamily, RecordStoreError>,
    writes: JobWrites,
    /// True when the run read local job rows to bridge the projection into.
    bridged: bool,
}

impl Run {
    fn projected(&self) -> &ProjectedFamily {
        self.outcome.as_ref().expect("projection completes")
    }

    fn cache_row(&self) -> Option<ProjectionCache> {
        self.writes
            .iter()
            .find(|(key_space, _, _)| key_space.as_str() == FAMILY_PROJECTION_KEYSPACE)
            .and_then(|(_, _, value)| ProjectionCache::decode(value))
    }

    fn job_rows(&self) -> usize {
        self.writes
            .iter()
            .filter(|(key_space, _, _)| key_space.as_str() == JOB_KEYSPACE)
            .count()
    }
}

/// Answers one projection's calls: `rows` are the stored family records, `cache` the stored
/// cache row, and `local` the job row every alias read answers with.
fn run(family: &Family, rows: &[(Key, Value)], cache: Option<Value>, local: Option<Value>) -> Run {
    let mut operation = ProjectFamilyOperation::new(ProjectFamilyConfig {
        family: FamilyRef::Family(family.family()),
        now_ms: 3_100,
        rebuild: false,
    });
    let mut writes: JobWrites = Vec::new();
    let mut bridged = false;
    let mut offset = 0usize;
    let mut turn = operation.first();
    let outcome = loop {
        let call = match turn {
            Ok(Next::Call(call)) => call,
            Ok(Next::Done(projected)) => break Ok(projected),
            Ok(Next::Wait) => panic!("the projection never waits"),
            Ok(Next::Then { .. }) => panic!("the projection has no calls after its commit"),
            Err(error) => break Err(error),
        };
        let result = match call {
            IoCall::Effect(effect) => match *effect {
                Effect::Storage(StorageEffect::Read { key, .. }) => {
                    IoResult::Event(Box::new(Event::Storage(StorageEvent::ReadResult {
                        key,
                        value: cache.clone(),
                    })))
                }
                other => panic!("unexpected effect: {other:?}"),
            },
            IoCall::Page { limit, .. } => {
                let page: Vec<(Key, Value)> =
                    rows.iter().skip(offset).take(limit).cloned().collect();
                offset += page.len();
                IoResult::Page {
                    rows: page,
                    next: None,
                }
            }
            IoCall::Read(reads) => {
                bridged = true;
                IoResult::Values(reads.iter().map(|_| local.clone()).collect())
            }
            IoCall::Write(batch) => {
                writes.extend(batch);
                IoResult::Done
            }
            other => panic!("unexpected call: {other:?}"),
        };
        turn = operation.next(result);
    };
    Run {
        outcome,
        writes,
        bridged,
    }
}

/// `count` stored rows, cycling `records` and keying each one by its position.
fn rows(records: &[JobRecordEnvelope], count: usize) -> Vec<(Key, Value)> {
    (0..count)
        .map(|index| {
            let envelope = &records[index % records.len()];
            let mut key = envelope.key();
            key.sequence = index as u64;
            let value = to_bytes(envelope).expect("record encodes");
            (record_key(&key), Value::from(value.as_slice()))
        })
        .collect()
}

fn job_row(family: &Family, state: JobState) -> Value {
    let mut record = JobRecord::new(
        family.job_id,
        JobPayload::Execution(payload()),
        user(),
        family.holder.public(),
        1_000,
        1_000,
        None,
    );
    record.state = state;
    Value::from(record.to_bytes().expect("job row encodes").as_slice())
}

fn encoded(cache: &ProjectionCache) -> Value {
    Value::from(to_bytes(cache).expect("cache row encodes").as_slice())
}

#[test]
fn truncation_survives_cache() {
    // A family too large to reduce at once is never cached, so every later read
    // rebuilds it and reports the same truncation instead of a complete view.
    let family = Family::new([1u8; 32]);
    let records = family.run(1, 0, PhysicalExecutionState::Succeeded);
    let stored = rows(&records, MAX_PROJECTION_RECORDS);
    let stale = encoded(&ProjectionCache::invalidated(None));

    for _ in 0..2 {
        let run = run(&family, &stored, Some(stale.clone()), None);
        assert!(run.projected().truncated);
        assert!(!run.projected().cached);
        assert!(run.cache_row().is_none());
    }
}

#[test]
fn truncated_skips_bridge() {
    // A partial projection may be inspected but must never settle a mutable
    // local job row, so it neither reads nor writes one.
    let family = Family::new([2u8; 32]);
    let records = family.run(1, 0, PhysicalExecutionState::Succeeded);
    let stored = rows(&records, MAX_PROJECTION_RECORDS);
    let run = run(
        &family,
        &stored,
        None,
        Some(job_row(&family, JobState::Queued)),
    );

    assert!(run.projected().truncated);
    assert!(!run.bridged);
    assert!(run.writes.is_empty());
}

#[test]
fn full_projection_bridges() {
    // A complete projection still settles its local rows and caches itself.
    let family = Family::new([3u8; 32]);
    let records = family.run(1, 0, PhysicalExecutionState::Succeeded);
    let stored = rows(&records, records.len());
    let run = run(
        &family,
        &stored,
        None,
        Some(job_row(&family, JobState::Queued)),
    );

    assert!(!run.projected().truncated);
    assert_eq!(
        run.projected()
            .projection
            .as_ref()
            .expect("family projects")
            .state,
        LogicalJobState::Succeeded
    );
    assert!(run.bridged);
    assert_eq!(run.job_rows(), 1);
    let cached = run.cache_row().expect("cache row written");
    assert!(!cached.stale);
    assert!(cached.projection.is_some());
}

#[test]
fn malformed_row_errors() {
    // A corrupt key or envelope fails the rebuild instead of reducing a set
    // that is silently short, so the runner rolls back and nothing is written.
    let family = Family::new([4u8; 32]);
    let records = family.run(1, 0, PhysicalExecutionState::Succeeded);
    let local = job_row(&family, JobState::Queued);

    let mut corrupt_key = rows(&records, records.len());
    let value = corrupt_key[0].1.clone();
    corrupt_key.push((Key::from([9u8; 8].as_slice()), value));
    let corrupt = run(&family, &corrupt_key, None, Some(local.clone()));
    assert!(matches!(corrupt.outcome, Err(RecordStoreError::Record(_))));
    assert!(corrupt.writes.is_empty());

    let mut corrupt_value = rows(&records, records.len());
    let key = corrupt_value[0].0.clone();
    corrupt_value.push((key, Value::from(b"corrupt".as_slice())));
    let corrupt = run(&family, &corrupt_value, None, Some(local));
    assert!(matches!(
        corrupt.outcome,
        Err(RecordStoreError::Conversion(_))
    ));
    assert!(corrupt.writes.is_empty());
}

#[test]
fn old_version_rebuilds() {
    // A cache row this build did not write is discarded, never answered as a
    // fresh projection, and the rebuild replaces it with a current row.
    let family = Family::new([5u8; 32]);
    let records = family.run(1, 0, PhysicalExecutionState::Succeeded);
    let stored = rows(&records, records.len());
    let current = ProjectionCache {
        version: PROJECTION_CACHE_VERSION,
        revision: 3,
        stale: false,
        projection: reduce_family(family.family(), &records).expect("records reduce"),
    };

    let control = run(&family, &stored, Some(encoded(&current)), None);
    assert!(control.projected().cached);
    assert!(control.writes.is_empty());

    let old = ProjectionCache {
        version: PROJECTION_CACHE_VERSION - 1,
        ..current
    };
    let discarded = run(
        &family,
        &stored,
        Some(encoded(&old)),
        Some(job_row(&family, JobState::Queued)),
    );
    assert!(!discarded.projected().cached);
    let rebuilt = discarded.cache_row().expect("cache row written");
    assert_eq!(rebuilt.version, PROJECTION_CACHE_VERSION);
    assert_eq!(rebuilt.revision, 1);
}

#[test]
fn reads_cache_outside() {
    // The cache read is a plain storage effect, so a fresh cache opens no transaction.
    let family = Family::new([6u8; 32]);
    let mut operation = ProjectFamilyOperation::new(ProjectFamilyConfig {
        family: FamilyRef::Family(family.family()),
        now_ms: 3_100,
        rebuild: false,
    });
    assert!(operation.transaction());
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Effect(effect)))
            if matches!(*effect, Effect::Storage(StorageEffect::Read { txn_id: None, .. }))
    ));
    assert!(matches!(
        operation.next(IoResult::Done),
        Err(RecordStoreError::UnexpectedEvent { .. })
    ));
}
