//! Tests the batched rewrite of rows whose record gained trailing fields.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::tests::{read, write};
use super::super::{migrate, migrate_output};
use super::*;
use crate::error::CliError;
use aruna_core::secrets::{SecretName, SecretScope};
use aruna_core::structs::execution::harvest::{HarvestSelector, HarvestSource};
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::identity::admin_audit::{
    AdminAuditAction, AdminAuditRecord, PluginAudit, SecretAudit,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
use aruna_core::structs::storage::replication::ArunaArn;
use aruna_core::structs::{SyncMode, SyncRelationship, SyncState, SyncStatusSnapshot};
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::types::GroupId;
use aruna_core::{NodeId, UserId};
use aruna_operations::sync::mirror_repair::{SyncMirrorIntent, SyncMirrorRecord};
use fjall::KeyspaceCreateOptions;
use serde::Deserialize;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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

/// An admin audit row as stored before `plugin`, with its `secret`.
#[derive(Serialize)]
struct SecretOnly {
    audit_id: Ulid,
    action: AdminAuditAction,
    actor: UserId,
    target: UserId,
    group_id: Option<GroupId>,
    session_id: Option<String>,
    secret: Option<SecretAudit>,
}

fn secret_only() -> Vec<u8> {
    let record = secret_audit(Some(Ulid::from_parts(6, 6)));
    postcard::to_allocvec(&SecretOnly {
        audit_id: record.audit_id,
        action: record.action,
        actor: record.actor,
        target: record.target,
        group_id: record.group_id,
        session_id: record.session_id,
        secret: record.secret,
    })
    .expect("row before plugin encodes")
}

/// An admin audit row as alpha.188 stored it: the secret row, then its `plugin`.
fn plugin_row() -> Vec<u8> {
    let plugin = PluginAudit {
        plugin: "announcer".to_string(),
        version: "0.1.0".to_string(),
        install_id: Ulid::from_parts(7, 7),
    };
    let record = secret_audit(Some(Ulid::from_parts(8, 8)));
    let head = SecretOnly {
        audit_id: Ulid::from_parts(9, 9),
        action: AdminAuditAction::PluginInstalled,
        actor: record.actor,
        target: record.target,
        group_id: record.group_id,
        session_id: record.session_id,
        secret: record.secret,
    };
    postcard::to_allocvec(&(head, Some(plugin))).expect("row before secret_use encodes")
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
    // A current row cut inside its `secret`, before `write_id`, and one with a byte too many.
    let cut = ended[..ended.len() - 3].to_vec();
    let longer = [current.as_slice(), &[0]].concat();
    let temp = tempfile::tempdir().expect("temporary directory");
    let path = temp.path().join("db");
    let database = path.to_str().expect("path");
    let rows = vec![
        (b"old".as_slice(), old_audit(1)),
        (b"secret".as_slice(), secret_only()),
        (b"plugin".as_slice(), plugin_row()),
        (b"current".as_slice(), current.clone()),
        (b"cut".as_slice(), cut.clone()),
        (b"longer".as_slice(), longer.clone()),
    ];
    write(&path, ADMIN_AUDIT_KEYSPACE, rows);
    let output = migrate_output(database).expect("migration runs");
    assert_eq!(
        (output.audit_rows_scanned, output.audit_rows_rewritten),
        (6, 3)
    );
    assert_eq!(output.unknown_rows.len(), 2);
    assert!(output.unknown_rows[0].starts_with("admin_audit/"));
    let stored = read(&path, ADMIN_AUDIT_KEYSPACE);
    let migrated = AdminAuditRecord::from_bytes(&stored[b"old".as_slice()]).expect("decodes");
    assert_eq!(migrated.session_id.as_deref(), Some("session"));
    assert_eq!(migrated.secret, None);
    // A row missing only `plugin` keeps its secret.
    let kept = AdminAuditRecord::from_bytes(&stored[b"secret".as_slice()]).expect("decodes");
    assert_eq!(
        kept.secret.and_then(|secret| secret.write_id),
        Some(Ulid::from_parts(6, 6))
    );
    assert_eq!((kept.plugin, kept.secret_use), (None, None));
    // An alpha.188 row keeps both its secret and its plugin and gains no use.
    let both = AdminAuditRecord::from_bytes(&stored[b"plugin".as_slice()]).expect("decodes");
    assert_eq!(
        both.plugin.map(|plugin| plugin.install_id),
        Some(Ulid::from_parts(7, 7))
    );
    let write_id = both.secret.and_then(|secret| secret.write_id);
    assert_eq!(
        (write_id, both.secret_use),
        (Some(Ulid::from_parts(8, 8)), None)
    );
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

/// A pending mirror repair as stored before `rollback`.
#[derive(Serialize)]
struct OldMirror {
    relationship: SyncRelationship,
    intent: SyncMirrorIntent,
    due_at_ms: u64,
    attempts: u32,
    last_error: Option<String>,
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn relationship(seed: u8) -> SyncRelationship {
    SyncRelationship {
        id: Ulid::from_bytes([seed; 16]),
        source: ArunaArn::s3_bucket(REALM, node(3), "source").expect("source"),
        target: ArunaArn::s3_bucket(REALM, node(4), "target").expect("target"),
        mode: SyncMode::Continuous,
        reference_handling: Default::default(),
        reference_serving: false,
        replicate_deletes: true,
        created_by: user(5),
        created_at: SystemTime::UNIX_EPOCH,
        state: SyncState::Enabled,
        status: SyncStatusSnapshot::default(),
    }
}

#[test]
fn mirror_gains_rollback() {
    let old = OldMirror {
        relationship: relationship(2),
        intent: SyncMirrorIntent::Delete,
        due_at_ms: 9,
        attempts: 3,
        last_error: Some("target unreachable".to_string()),
    };
    let rolled = SyncMirrorRecord {
        relationship: relationship(6),
        intent: SyncMirrorIntent::Reconcile,
        due_at_ms: 1,
        attempts: 0,
        last_error: None,
        rollback: true,
    };
    let current = postcard::to_allocvec(&rolled).expect("current row encodes");
    let temp = tempfile::tempdir().expect("temporary directory");
    let path = temp.path().join("db");
    let database = path.to_str().expect("path");
    let rows = vec![
        (
            [2; 16].as_slice(),
            postcard::to_allocvec(&old).expect("old row encodes"),
        ),
        ([6; 16].as_slice(), current.clone()),
    ];
    write(&path, MIRROR_REPAIR_KEYSPACE, rows);
    let output = migrate_output(database).expect("migration runs");
    assert_eq!(
        (
            output.mirror_repairs_scanned,
            output.mirror_repairs_rewritten
        ),
        (2, 1)
    );
    let stored = read(&path, MIRROR_REPAIR_KEYSPACE);
    let migrated: SyncMirrorRecord =
        postcard::from_bytes(&stored[[2; 16].as_slice()]).expect("decodes");
    let expected = SyncMirrorRecord {
        relationship: old.relationship,
        intent: old.intent,
        due_at_ms: old.due_at_ms,
        attempts: old.attempts,
        last_error: old.last_error,
        rollback: false,
    };
    assert_eq!(migrated, expected);
    assert_eq!(stored[[6; 16].as_slice()], current);
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

/// Writes five admin audit rows in the shape before `secret`.
fn old_rows(path: &Path) {
    let keys: Vec<[u8; 1]> = (0..5).map(|key| [key]).collect();
    let rows = keys
        .iter()
        .zip(1..)
        .map(|(key, seed)| (key.as_slice(), old_audit(seed)))
        .collect();
    write(path, ADMIN_AUDIT_KEYSPACE, rows);
}

#[test]
fn resumes_partial_run() {
    let temp = tempfile::tempdir().expect("temporary directory");
    let path = temp.path().join("db");
    old_rows(&path);
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
    }
    assert!(unknown.is_empty());
    let database = path.to_str().expect("path");
    let output = migrate_output(database).expect("migration resumes");
    assert_eq!(
        (output.audit_rows_scanned, output.audit_rows_rewritten),
        (5, 3)
    );
    let stored = read(&path, ADMIN_AUDIT_KEYSPACE);
    assert!(
        stored
            .values()
            .all(|value| AdminAuditRecord::from_bytes(value).is_ok())
    );
    let again = migrate_output(database).expect("migration repeats");
    assert_eq!(
        (again.audit_rows_scanned, again.audit_rows_rewritten),
        (5, 0)
    );
}

#[test]
fn spans_several_batches() {
    let temp = tempfile::tempdir().expect("temporary directory");
    let path = temp.path().join("db");
    old_rows(&path);
    let mut unknown = Vec::new();
    let (db, keyspace) = audit_rows(&path);
    let counts =
        batched::<AdminAuditRecord>(&db, &keyspace, AUDIT, 2, &mut unknown).expect("batches run");
    assert_eq!(
        counts,
        Counts {
            scanned: 5,
            rewritten: 5
        }
    );
    assert!(unknown.is_empty());
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

#[tokio::test]
async fn unknown_rows_fail() {
    let temp = tempfile::tempdir().expect("temporary directory");
    let path = temp.path().join("db");
    write(
        &path,
        ADMIN_AUDIT_KEYSPACE,
        vec![(b"broken".as_slice(), vec![0xff; 3])],
    );
    let database = path.to_str().expect("path").to_string();
    let result = migrate(database, None).await;
    assert!(matches!(result, Err(CliError::UnknownRows(1))));
    assert_eq!(
        read(&path, ADMIN_AUDIT_KEYSPACE)[b"broken".as_slice()],
        vec![0xff; 3]
    );
}
