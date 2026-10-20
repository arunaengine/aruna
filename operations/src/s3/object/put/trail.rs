//! Reads the delete trail of a group inside the version transaction of an import.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::effects::IterStart;
use aruna_core::keyspaces::DELETE_AUDIT_KEYSPACE;
use aruna_core::structs::storage::delete_audit::{BlobAuditKind, BlobAuditRecord, BlobPurgeKind};
use aruna_core::types::Key;

const TRAIL_PAGE: usize = 500;

/// What the delete trail says about one version of a key.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Trail {
    Clean,
    /// Some version of the key was deleted, so its head may have been removed on purpose.
    Touched,
    /// The version was deleted, or a purge after its creation covered the key.
    Removed,
}

impl Trail {
    /// Adds one record about `key` in `bucket` to what is known about version `id`.
    pub(crate) fn add(self, record: &BlobAuditRecord, bucket: &str, key: &str, id: Ulid) -> Self {
        if self == Self::Removed || record.bucket != bucket {
            return self;
        }
        match record.kind {
            BlobAuditKind::Purge(scope) => {
                let covers = match scope {
                    BlobPurgeKind::File => record.key == key,
                    BlobPurgeKind::Prefix => key.starts_with(&record.key),
                    BlobPurgeKind::Bucket => true,
                };
                match covers && record.occurred_at_ms >= id.timestamp_ms() {
                    true => Self::Removed,
                    false => self,
                }
            }
            BlobAuditKind::DeleteVersion | BlobAuditKind::DeleteMarker if record.key == key => {
                match record.version_id == Some(id) {
                    true => Self::Removed,
                    false => Self::Touched,
                }
            }
            _ => self,
        }
    }
}

/// The trail scan of one version transaction. Its range read makes a delete recorded meanwhile
/// conflict with the commit, so the retried transaction reads it.
#[derive(Debug, PartialEq)]
pub(super) struct TrailScan {
    version_id: Ulid,
    after: Option<Key>,
    trail: Trail,
    done: bool,
}

impl TrailScan {
    pub(super) fn new(version_id: Ulid) -> Self {
        Self {
            version_id,
            after: None,
            trail: Trail::Clean,
            done: false,
        }
    }

    /// A retried transaction reads the whole trail again.
    pub(super) fn restart(&mut self) {
        *self = Self::new(self.version_id);
    }
}

impl PutObjectOperation {
    /// The next page of the trail, or `None` once the whole trail is read.
    pub(super) fn trail_page(&mut self, txn_id: Ulid) -> Option<Effects> {
        let scan = self.trail_scan.as_ref().filter(|scan| !scan.done)?;
        self.state = PutObjectState::ReadTrail;
        Some(smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: DELETE_AUDIT_KEYSPACE.to_string(),
            prefix: Some(self.config.group_id.to_bytes().to_vec().into()),
            start: scan.after.clone().map(IterStart::After),
            limit: TRAIL_PAGE,
            txn_id: Some(txn_id),
        })])
    }

    pub(super) fn trail_read(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(PutObjectError::NoTransactionFound);
        };
        let (values, next) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return self.emit_error(error.into()),
            _ => return self.emit_error(PutObjectError::InvalidOperationState),
        };
        let request = &self.config.request;
        let Some(scan) = self.trail_scan.as_mut() else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        for (_, value) in values {
            match BlobAuditRecord::from_bytes(&value) {
                Ok(record) => {
                    let id = scan.version_id;
                    scan.trail = scan.trail.add(&record, &request.bucket, &request.key, id);
                }
                Err(error) => return self.emit_error(error.into()),
            }
        }
        scan.done = next.is_none();
        scan.after = next;
        let trail = scan.trail;
        if trail == Trail::Removed || (scan.done && trail == Trail::Touched && !self.has_head()) {
            return self.emit_error(PutObjectError::HistoryDeleted);
        }
        self.write_cleanup_row(txn_id)
    }

    fn has_head(&self) -> bool {
        self.existing_pointer.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;

    fn audit(kind: BlobAuditKind, key: &str, version_id: Option<Ulid>, at: u64) -> BlobAuditRecord {
        BlobAuditRecord {
            realm_id: RealmId::from_bytes([1; 32]),
            group_id: Ulid::from_parts(3, 3),
            node_id: iroh::SecretKey::from_bytes(&[65; 32]).public(),
            user_id: Default::default(),
            kind,
            bucket: "bucket".to_string(),
            key: key.to_string(),
            version_id,
            occurred_at_ms: at,
        }
    }

    fn trail(records: &[BlobAuditRecord], id: Ulid) -> Trail {
        let add = |trail: Trail, record| trail.add(record, "bucket", "in/a.csv", id);
        records.iter().fold(Trail::Clean, add)
    }

    #[test]
    fn respects_delete_trail() {
        let id = Ulid::from_parts(1_000, 1);
        assert_eq!(trail(&[], id), Trail::Clean);
        let kind = BlobAuditKind::DeleteVersion;
        let other = audit(kind, "in/a.csv", Some(Ulid::nil()), 2_000);
        assert_eq!(trail(std::slice::from_ref(&other), id), Trail::Touched);
        let exact = audit(BlobAuditKind::DeleteVersion, "in/a.csv", Some(id), 2_000);
        assert_eq!(trail(&[exact, other], id), Trail::Removed);
        // A purge removes what existed when it ran, not what was written after it.
        let purge = |at| audit(BlobAuditKind::Purge(BlobPurgeKind::Prefix), "in/", None, at);
        assert_eq!(trail(&[purge(2_000)], id), Trail::Removed);
        assert_eq!(trail(&[purge(500)], id), Trail::Clean);
        let bucket = audit(BlobAuditKind::Purge(BlobPurgeKind::Bucket), "", None, 2_000);
        assert_eq!(trail(std::slice::from_ref(&bucket), id), Trail::Removed);
        let elsewhere = BlobAuditRecord {
            bucket: "other".to_string(),
            ..bucket
        };
        let file = BlobAuditKind::Purge(BlobPurgeKind::File);
        let file = audit(file, "in/b.csv", None, 2_000);
        assert_eq!(trail(&[elsewhere, file], id), Trail::Clean);
    }
}
