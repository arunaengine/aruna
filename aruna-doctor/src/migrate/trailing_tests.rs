//! Tests the batched rewrite of rows whose record gained trailing fields.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::migrate_output;
use super::super::tests::{read, write};
use super::*;
use aruna_core::UserId;
use aruna_core::secrets::{SecretName, SecretScope};
use aruna_core::structs::execution::harvest::{HarvestSelector, HarvestSource};
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord, SecretAudit};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::types::GroupId;
use fjall::KeyspaceCreateOptions;
use serde::Deserialize;
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};
use ulid::Ulid;

const REALM: RealmId = RealmId([1; 32]);

fn user(seed: u64) -> UserId {
    UserId::local(Ulid::from_parts(seed, seed.into()), REALM)
}

/// An admin audit row as stored before `secret`.
#[derive(Serialize)]
struct OldAudit {
    audit_id: Ulid,
    action: AdminAuditAction,
    actor: UserId,
    target: UserId,
    group_id: Option<GroupId>,
    session_id: Option<String>,
}

fn old_audit(seed: u64) -> Vec<u8> {
    postcard::to_allocvec(&OldAudit {
        audit_id: Ulid::from_parts(seed, 2),
        action: AdminAuditAction::RestrictedTokenIssued,
        actor: user(1),
        target: user(1),
        group_id: Some(Ulid::from_parts(3, 3)),
        session_id: Some("session".to_string()),
    })
    .expect("old row encodes")
}

fn secret_audit(write_id: Option<Ulid>) -> AdminAuditRecord {
    let secret = SecretAudit {
        scope: SecretScope::User(user(1)),
        name: SecretName::try_from("catalog-key".to_string()).expect("name"),
        incarnation: Some(Ulid::from_parts(4, 4)),
        write_id,
    };
    AdminAuditRecord::for_secret(AdminAuditAction::SecretRead, user(1), secret)
}

#[test]
fn audit_gains_secret() {
    let current = secret_audit(Some(Ulid::from_parts(5, 5)))
        .to_bytes()
        .expect("current row encodes");
    let ended = secret_audit(None).to_bytes().expect("row encodes");
    // A current row cut before its last `write_id`, and one with a byte too many.
    let cut = ended[..ended.len() - 1].to_vec();
    let longer = [current.as_slice(), &[0]].concat();
    let temp = tempfile::tempdir().expect("temporary directory");
    let path = temp.path().join("db");
    let database = path.to_str().expect("path");
    let rows = vec![
        (b"old".as_slice(), old_audit(1)),
        (b"current".as_slice(), current.clone()),
        (b"cut".as_slice(), cut.clone()),
        (b"longer".as_slice(), longer.clone()),
    ];
    write(&path, ADMIN_AUDIT_KEYSPACE, rows);
    let output = migrate_output(database).expect("migration runs");
    assert_eq!(
        (output.audit_rows_scanned, output.audit_rows_rewritten),
        (4, 1)
    );
    assert_eq!(output.unknown_rows.len(), 2);
    assert!(output.unknown_rows[0].starts_with("admin_audit/"));
    let stored = read(&path, ADMIN_AUDIT_KEYSPACE);
    let migrated = AdminAuditRecord::from_bytes(&stored[b"old".as_slice()]).expect("decodes");
    assert_eq!(migrated.session_id.as_deref(), Some("session"));
    assert_eq!(migrated.secret, None);
    assert_eq!(stored[b"current".as_slice()], current);
    assert_eq!(
        (&stored[b"cut".as_slice()], &stored[b"longer".as_slice()]),
        (&cut, &longer)
    );
    let again = migrate_output(database).expect("migration repeats");
    assert_eq!(again.audit_rows_rewritten, 0);
}

#[test]
fn harvest_gains_job() {
    let mut source = HarvestSource::new(
        Ulid::from_parts(1, 1),
        Ulid::from_parts(2, 2),
        Ulid::from_parts(3, 3),
        "oai".to_string(),
        "/harvest".to_string(),
        HarvestSelector {
            set: Some("set".to_string()),
            metadata_prefix: None,
        },
        Some(60_000),
        UNIX_EPOCH + Duration::from_secs(5),
        user(4),
    );
    let ended = source.to_bytes().expect("source encodes");
    // The row ends with `last_job: None`, one zero byte the old shape lacked.
    let old = ended[..ended.len() - 1].to_vec();
    let mut running = source.clone();
    let handle = PlacementHandle::new(FIRST_GRANTABLE_HANDLE).expect("handle");
    let bucket = BucketId::new(0).expect("bucket");
    running.last_job = Some(JobId::from_parts(1, handle, bucket, 9).expect("job id"));
    let current = running.to_bytes().expect("source encodes");
    let temp = tempfile::tempdir().expect("temporary directory");
    let path = temp.path().join("db");
    let database = path.to_str().expect("path");
    let rows = vec![
        (b"old".as_slice(), old),
        (b"current".as_slice(), current.clone()),
    ];
    write(&path, HARVEST_SOURCE_KEYSPACE, rows);
    let output = migrate_output(database).expect("migration runs");
    assert_eq!(output.harvest_sources_rewritten, 1);
    let stored = read(&path, HARVEST_SOURCE_KEYSPACE);
    let migrated = HarvestSource::from_bytes(&stored[b"old".as_slice()]).expect("decodes");
    source.last_job = None;
    assert_eq!(migrated, source);
    assert_eq!(stored[b"current".as_slice()], current);
}

fn audit_rows(path: &Path) -> (OptimisticTxDatabase, OptimisticTxKeyspace) {
    let db = OptimisticTxDatabase::builder(path)
        .open()
        .expect("database opens");
    let keyspace = db
        .keyspace(ADMIN_AUDIT_KEYSPACE, KeyspaceCreateOptions::default)
        .expect("keyspace opens");
    (db, keyspace)
}

#[test]
fn resumes_partial_run() {
    let temp = tempfile::tempdir().expect("temporary directory");
    let path = temp.path().join("db");
    let keys: Vec<[u8; 1]> = (0..5).map(|key| [key]).collect();
    let rows = keys
        .iter()
        .zip(1..)
        .map(|(key, seed)| (key.as_slice(), old_audit(seed)))
        .collect();
    write(&path, ADMIN_AUDIT_KEYSPACE, rows);
    let mut unknown = Vec::new();
    {
        // One committed batch stands for a run stopped after its first commit.
        let (db, keyspace) = audit_rows(&path);
        let mut first = Counts::default();
        let after = next_batch::<AdminAuditRecord>(
            &db,
            &keyspace,
            AUDIT,
            (None, 2),
            &mut first,
            &mut unknown,
        )
        .expect("batch runs");
        assert_eq!((after, first.rewritten), (Some(vec![1]), 2));
        let rest = batched::<AdminAuditRecord>(&db, &keyspace, AUDIT, 2, &mut unknown)
            .expect("rerun runs");
        assert_eq!(
            rest,
            Counts {
                scanned: 5,
                rewritten: 3
            }
        );
    }
    assert!(unknown.is_empty());
    let stored = read(&path, ADMIN_AUDIT_KEYSPACE);
    assert!(
        stored
            .values()
            .all(|value| AdminAuditRecord::from_bytes(value).is_ok())
    );
    let output = migrate_output(path.to_str().expect("path")).expect("migration repeats");
    assert_eq!(
        (output.audit_rows_scanned, output.audit_rows_rewritten),
        (5, 0)
    );
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Grown {
    id: u8,
    older: Option<u8>,
    newest: Option<u8>,
}

#[test]
fn older_values_kept() {
    const LAYOUTS: &[Layout] = &[&[("Grown", 2)], &[("Grown", 1)]];
    let older = postcard::to_allocvec(&(1u8, Some(7u8))).expect("row encodes");
    let grown = |older, newest| {
        Row::Older(Grown {
            id: 1,
            older,
            newest,
        })
    };
    assert_eq!(classify::<Grown>(&older, LAYOUTS), grown(Some(7), None));
    let first = postcard::to_allocvec(&1u8).expect("row encodes");
    assert_eq!(classify::<Grown>(&first, LAYOUTS), grown(None, None));
    let current = postcard::to_allocvec(&(1u8, Some(7u8), None::<u8>)).expect("row encodes");
    assert_eq!(classify::<Grown>(&current, LAYOUTS), Row::Current);
    let longer = [current.as_slice(), &[0]].concat();
    assert_eq!(classify::<Grown>(&longer, LAYOUTS), Row::Unknown);
}
