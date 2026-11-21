//! Tests the prune index backfill: a family alpha.159 stored without index rows is scheduled one
//! retention ahead, a scheduled family keeps its rows, and a second run writes nothing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::FAMILY_RECORD_KEYSPACE;
use aruna_core::structs::execution::job::family::{
    JobFamilyRecord, JobRecordEnvelope, SubmissionClaim,
};
use aruna_core::structs::execution::job::identity::{JobId, RETENTION_MS};
use aruna_core::structs::execution::job::record_keys::{
    parse_prune_due, prune_rows_at, record_key,
};
use aruna_core::structs::identity::realm::RealmId;

use super::super::migrate_output;
use super::super::tests::{read, write};
use super::*;

/// A claim row of family `seed`, which alpha.159 stored without index rows.
fn claim(seed: u8) -> (JobFamilyId, Vec<u8>, Vec<u8>) {
    let secret = iroh::SecretKey::from_bytes(&[7u8; 32]);
    let claim = SubmissionClaim {
        submission_id: SubmissionId([seed; 32]),
        job_id: JobId::from_bytes([seed; 16]),
        request_digest: [seed; 32],
        spec_digest: [9; 32],
        committing_node_id: secret.public(),
        accepted_at_ms: 1_000,
    };
    let family = JobFamilyId {
        submission_id: claim.submission_id,
        request_digest: claim.request_digest,
    };
    let record = JobFamilyRecord::Claim(claim);
    let envelope = JobRecordEnvelope::sign(RealmId([4u8; 32]), record, &secret).unwrap();
    let key = record_key(&envelope.key()).to_vec();
    (family, key, postcard::to_allocvec(&envelope).unwrap())
}

#[test]
fn schedules_unindexed_families() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let (bare, bare_key, bare_row) = claim(1);
    let (indexed, indexed_key, indexed_row) = claim(2);
    write(
        &path,
        FAMILY_RECORD_KEYSPACE,
        vec![(&bare_key[..], bare_row), (&indexed_key[..], indexed_row)],
    );
    let scheduled = prune_rows_at(&indexed, 5);
    let rows = (scheduled.iter())
        .map(|(_, key, value)| (&key[..], value.to_vec()))
        .collect();
    write(&path, FAMILY_PRUNE_KEYSPACE, rows);
    let before = aruna_clock::unix_timestamp_millis();

    let output = migrate_output(path.to_str().unwrap()).unwrap();

    assert_eq!(output.families_scheduled, 1);
    let index = read(&path, FAMILY_PRUNE_KEYSPACE);
    assert_eq!(index.len(), 4);
    let slot = &index[&prune_slot_key(&bare).to_vec()];
    let due = u64::from_be_bytes(slot.as_slice().try_into().unwrap());
    assert!(due >= before + RETENTION_MS);
    let dues: Vec<_> = (index.keys())
        .filter_map(|key| parse_prune_due(key))
        .collect();
    assert!(dues.contains(&(due, bare)));
    assert!(dues.contains(&(5, indexed)));
    let again = migrate_output(path.to_str().unwrap()).unwrap();
    assert_eq!(again.families_scheduled, 0);
    assert_eq!(read(&path, FAMILY_PRUNE_KEYSPACE), index);
}
