//! Records a completed storage purge in the delete audit, with its trail row while the purged
//! bucket incarnation still stands.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{
    BUCKET_CREATION_KEYSPACE, DELETE_AUDIT_KEYSPACE, DELETE_TRAIL_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, values};
use aruna_core::structs::storage::blob::incarnation;
use aruna_core::structs::storage::delete_audit::{
    BlobAuditKind, BlobAuditRecord, BlobPurgeKind, delete_audit_key, trail_key,
};
use aruna_core::structs::storage::storage_purge::{StoragePurgeScope, StoragePurgeSpec};
use aruna_core::types::{Key, Value};
use ulid::Ulid;

pub(super) fn purge_audit_record(spec: &StoragePurgeSpec, occurred_at_ms: u64) -> BlobAuditRecord {
    let (scope, key) = match &spec.scope {
        StoragePurgeScope::File { key, .. } => (BlobPurgeKind::File, key.clone()),
        StoragePurgeScope::Prefix { prefix, .. } => (BlobPurgeKind::Prefix, prefix.clone()),
        StoragePurgeScope::Bucket { .. } => (BlobPurgeKind::Bucket, String::new()),
    };
    BlobAuditRecord {
        realm_id: spec.auth_context.realm_id,
        group_id: spec.group_id,
        node_id: spec.node_id,
        user_id: spec.auth_context.user_id,
        kind: BlobAuditKind::Purge(scope),
        bucket: spec.scope.bucket().to_string(),
        key,
        version_id: None,
        occurred_at_ms,
    }
}

/// The audit row of a purge and its trail row in the bucket incarnation it purged, if that
/// bucket still stands. A purge that deleted its bucket, or one whose bucket was replaced,
/// leaves nothing to protect, and a bucket created again must not meet its trail.
pub(super) fn audit_rows(
    record: &BlobAuditRecord,
    audit_id: ulid::Ulid,
    value: Value,
    incarnation: Option<u128>,
) -> Vec<(String, Key, Value)> {
    let audit = delete_audit_key(record.group_id, audit_id).into();
    let mut writes = vec![(DELETE_AUDIT_KEYSPACE.to_string(), audit, value.clone())];
    if let Some(incarnation) = incarnation {
        let trail = trail_key(record, incarnation, audit_id).into();
        writes.push((DELETE_TRAIL_KEYSPACE.to_string(), trail, value));
    }
    writes
}

/// Writes the audit row of a purge once. The job id is the record id, so a re-driven job
/// overwrites its own row instead of adding a second one. The bucket is read in the same write
/// transaction, so its removal or replacement meanwhile conflicts.
#[derive(Debug, PartialEq)]
pub(super) struct WriteAuditOperation {
    record: BlobAuditRecord,
    audit_id: Ulid,
    value: Value,
    incarnation: Option<u128>,
    written: bool,
}

impl WriteAuditOperation {
    pub(super) fn new(
        record: BlobAuditRecord,
        audit_id: Ulid,
        incarnation: Option<u128>,
    ) -> Result<Self, ConversionError> {
        let value = Value::from(record.to_bytes()?);
        Ok(Self {
            record,
            audit_id,
            value,
            incarnation,
            written: false,
        })
    }
}

impl Operation for WriteAuditOperation {
    type Output = ();
    type Error = IoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let key = Key::from(self.record.bucket.as_bytes().to_vec());
        Ok(Next::Call(IoCall::Read(vec![
            (S3_BUCKET_KEYSPACE.to_string(), key.clone()),
            (BUCKET_CREATION_KEYSPACE.to_string(), key),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.written {
            done(result)?;
            return Ok(Next::Done(()));
        }
        let [bucket, creation] = values::<2>(result)?;
        let current = bucket.map(|_| incarnation(creation.as_deref()));
        let kept = self.incarnation.filter(|kept| current == Some(*kept));
        let rows = audit_rows(&self.record, self.audit_id, self.value.clone(), kept);
        self.written = true;
        Ok(Next::Call(IoCall::Write(rows)))
    }
}

#[cfg(test)]
mod pure_tests {
    use super::purge_audit_record;
    use aruna_core::UserId;
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::delete_audit::{BlobAuditKind, BlobPurgeKind};
    use aruna_core::structs::storage::storage_purge::{StoragePurgeScope, StoragePurgeSpec};
    use ulid::Ulid;

    fn spec(scope: StoragePurgeScope) -> StoragePurgeSpec {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        StoragePurgeSpec {
            scope,
            group_id: Ulid::from_bytes([2u8; 16]),
            auth_context: AuthContext {
                user_id: UserId::local(Ulid::from_bytes([3u8; 16]), realm_id),
                realm_id,
                path_restrictions: None,
                session: None,
            },
            node_id: iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
        }
    }

    #[test]
    fn audits_purge_scope() {
        let file = purge_audit_record(
            &spec(StoragePurgeScope::File {
                bucket: "bucket".to_string(),
                key: "reports/a.csv".to_string(),
            }),
            7,
        );
        assert_eq!(file.kind, BlobAuditKind::Purge(BlobPurgeKind::File));
        assert_eq!(file.bucket, "bucket");
        assert_eq!(file.key, "reports/a.csv");
        assert_eq!(file.version_id, None);
        assert_eq!(file.occurred_at_ms, 7);

        let prefix = purge_audit_record(
            &spec(StoragePurgeScope::Prefix {
                bucket: "bucket".to_string(),
                prefix: "reports/".to_string(),
            }),
            8,
        );
        assert_eq!(prefix.kind, BlobAuditKind::Purge(BlobPurgeKind::Prefix));
        assert_eq!(prefix.key, "reports/");

        let bucket = purge_audit_record(
            &spec(StoragePurgeScope::Bucket {
                bucket: "bucket".to_string(),
            }),
            9,
        );
        assert_eq!(bucket.kind, BlobAuditKind::Purge(BlobPurgeKind::Bucket));
        assert!(bucket.key.is_empty());
    }
}

#[cfg(test)]
mod audit_tests {
    use super::*;

    #[test]
    fn omits_bucket_trail() {
        let record = BlobAuditRecord {
            realm_id: aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]),
            group_id: ulid::Ulid::from_parts(1, 1),
            node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
            user_id: Default::default(),
            kind: BlobAuditKind::Purge(BlobPurgeKind::Bucket),
            bucket: "b".to_string(),
            key: String::new(),
            version_id: None,
            occurred_at_ms: 1,
        };
        let id = ulid::Ulid::from_parts(3, 3);
        let spaces = |incarnation| {
            let rows = audit_rows(&record, id, vec![1].into(), incarnation);
            rows.into_iter()
                .map(|(space, ..)| space)
                .collect::<Vec<_>>()
        };
        // The audit always stays; the trail only while the purged bucket incarnation does.
        assert_eq!(
            spaces(Some(7)),
            [DELETE_AUDIT_KEYSPACE, DELETE_TRAIL_KEYSPACE]
        );
        assert_eq!(spaces(None), [DELETE_AUDIT_KEYSPACE]);
    }
}

#[cfg(test)]
mod operation_tests {
    use super::*;
    use crate::jobs::store::mutation::{Rows, run_in_memory};

    fn record() -> BlobAuditRecord {
        BlobAuditRecord {
            realm_id: aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]),
            group_id: Ulid::from_parts(1, 1),
            node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
            user_id: Default::default(),
            kind: BlobAuditKind::Purge(BlobPurgeKind::Bucket),
            bucket: "b".to_string(),
            key: String::new(),
            version_id: None,
            occurred_at_ms: 1,
        }
    }

    fn spaces(rows: &Rows) -> Vec<&str> {
        rows.keys().map(|(space, _)| space.as_str()).collect()
    }

    // The trail row is written only while the bucket stands in the purged incarnation.
    #[test]
    fn trails_standing_incarnation() {
        let id = Ulid::from_parts(3, 3);
        let standing = incarnation(None);
        let bucket = (S3_BUCKET_KEYSPACE.to_string(), b"b".to_vec());
        let operation = WriteAuditOperation::new(record(), id, Some(standing)).unwrap();
        assert_eq!(operation.transaction(), TransactionMode::Write);
        let mut rows = Rows::from([(bucket.clone(), vec![1])]);
        assert_eq!(run_in_memory(operation, &mut rows), Ok(()));
        let written = spaces(&rows);
        assert!(
            written.contains(&DELETE_AUDIT_KEYSPACE) && written.contains(&DELETE_TRAIL_KEYSPACE)
        );

        let mut gone = Rows::new();
        let operation = WriteAuditOperation::new(record(), id, Some(standing)).unwrap();
        assert_eq!(run_in_memory(operation, &mut gone), Ok(()));
        assert_eq!(spaces(&gone), vec![DELETE_AUDIT_KEYSPACE]);
    }
}
