//! Reads the delete trail of a group inside the version transaction of an import.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::effects::IterStart;
use aruna_core::keyspaces::DELETE_TRAIL_KEYSPACE;
use aruna_core::structs::storage::delete_audit::{
    BlobAuditKind, BlobAuditRecord, BlobPurgeKind, delete_prefix, purge_prefix,
};
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
            // A delete of another version before this one was written says nothing about it.
            BlobAuditKind::DeleteVersion | BlobAuditKind::DeleteMarker if record.key == key => {
                match record.version_id == Some(id) {
                    true => Self::Removed,
                    false if record.occurred_at_ms >= id.timestamp_ms() => Self::Touched,
                    false => self,
                }
            }
            _ => self,
        }
    }
}

/// The deletes of one object key, then the purges of its bucket, in the bucket incarnation the
/// write goes to: an earlier bucket of the name has its own trail.
pub(crate) fn trail_ranges(
    group_id: GroupId,
    (bucket, incarnation): (&str, u128),
    key: &str,
) -> [Key; 2] {
    let deletes = delete_prefix(group_id, bucket, incarnation, key);
    [
        deletes.into(),
        purge_prefix(group_id, bucket, incarnation).into(),
    ]
}

/// The trail scan of one version transaction. Its range read makes a delete recorded meanwhile
/// conflict with the commit, so the retried transaction reads it.
#[derive(Debug, PartialEq)]
pub(crate) struct TrailScan {
    pub(crate) version_id: Ulid,
    pub(crate) after: Option<Key>,
    pub(crate) trail: Trail,
    /// The next of the two trail ranges to read; both read means done.
    pub(crate) range: usize,
    /// The original write of the version, which only an exact removal refuses.
    pub(crate) writer: bool,
}

impl TrailScan {
    pub(crate) fn new(version_id: Ulid) -> Self {
        Self {
            version_id,
            after: None,
            trail: Trail::Clean,
            range: 0,
            writer: false,
        }
    }

    /// Where a page of `prefix` starts. The original write needs only the deletes of its key
    /// recorded since its version was created, as their audit ids are time ordered. A purge
    /// is keyed by its job, which may predate the write, so all purges are read.
    pub(crate) fn start(&self, prefix: &Key) -> Option<IterStart> {
        if let Some(after) = &self.after {
            return Some(IterStart::After(after.clone()));
        }
        let floor = Ulid::from_parts(self.version_id.timestamp_ms(), 0);
        let at = [prefix.as_ref(), &floor.to_bytes()[..]].concat();
        (self.writer && self.range == 0).then(|| IterStart::At(at.into()))
    }

    /// A retried transaction reads the whole trail again.
    pub(super) fn restart(&mut self) {
        let writer = self.writer;
        *self = Self::new(self.version_id);
        self.writer = writer;
    }
}

impl PutObjectOperation {
    /// The next page of the trail, or `None` once the whole trail is read.
    pub(super) fn trail_page(&mut self, txn_id: Ulid) -> Option<Effects> {
        let scan = self.trail_scan.as_ref()?;
        let request = &self.config.request;
        let bucket = (request.bucket.as_str(), self.incarnation());
        let ranges = trail_ranges(self.config.group_id, bucket, &request.key);
        let prefix = ranges.get(scan.range)?.clone();
        self.state = PutObjectState::ReadTrail;
        Some(smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: DELETE_TRAIL_KEYSPACE.to_string(),
            start: scan.start(&prefix),
            prefix: Some(prefix),
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
        scan.range += usize::from(next.is_none());
        scan.after = next;
        let (trail, done, writer) = (scan.trail, scan.range >= 2, scan.writer);
        let headless = !writer && done && trail == Trail::Touched && !self.has_head();
        if trail == Trail::Removed || headless {
            return self.emit_error(PutObjectError::HistoryDeleted);
        }
        if writer {
            return self.writer_trail(txn_id);
        }
        self.write_cleanup_row(txn_id)
    }

    /// A listed write whose version an import registered and a user then deleted or purged
    /// must not publish it again, so its unregistered version reads the trail before the head.
    pub(super) fn writer_trail(&mut self, txn_id: Ulid) -> Effects {
        let Some(version_id) = self.version_id else {
            return self.emit_error(PutObjectError::PutObjectFailed);
        };
        let scan = self
            .trail_scan
            .get_or_insert_with(|| TrailScan::new(version_id));
        scan.writer = true;
        if let Some(effects) = self.trail_page(txn_id) {
            return effects;
        }
        let existing = self.existing_pointer.clone();
        self.write_head(existing.as_ref(), version_id)
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
        // A delete before the version was written does not block it, unless it names it.
        let kind = BlobAuditKind::DeleteMarker;
        let older = audit(kind, "in/a.csv", Some(Ulid::nil()), 500);
        assert_eq!(trail(std::slice::from_ref(&older), id), Trail::Clean);
        let named = audit(BlobAuditKind::DeleteVersion, "in/a.csv", Some(id), 500);
        assert_eq!(trail(&[older, named], id), Trail::Removed);
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
