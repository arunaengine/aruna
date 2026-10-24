//! Composes a completed upload at its path in the tenant's folders under a backend import rule.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::object::put::fenced::{Fenced, FencedWrite};
use crate::s3::object::put::{
    PlainRecord, Trail, TrailScan, intent_deletes, intent_keys, intent_rows, owner_read, owns_path,
    same_name_path, trail_ranges, version_row,
};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, DELETE_TRAIL_KEYSPACE,
    IMPORT_INTENT_KEYSPACE, IMPORTED_OBJECT_KEYSPACE,
};
use aruna_core::structs::storage::delete_audit::BlobAuditRecord;

/// Trail entries one read of the finalize transaction returns.
const TRAIL_PAGE: usize = 500;
use aruna_core::structs::storage::backend_import::{
    WriteIntent, import_key, intent_key, object_key, tagged_path,
};
use aruna_core::structs::storage::multipart::BackendUpload;

impl CompleteUploadOperation {
    /// Routing inputs that may name a plain path for the completed object.
    pub fn with_routing(mut self, routing: RoutingSnapshot) -> Self {
        self.routing = Some(routing);
        self
    }

    /// A plain path only for raw parts composed on the rule's write backend. A same-name
    /// object first looks at the path; no lock is held.
    pub(super) fn plain_compose(&mut self) -> Option<Effects> {
        if let Some(location) = self.plain_upload().map(|upload| upload.location.clone()) {
            return Some(self.stat_in_place(location));
        }
        let upload = self.upload_record.as_ref()?;
        if self.compression != Compression::Off || upload.backend_upload.is_some() {
            return None;
        }
        let id = self.version_id.unwrap_or_else(Ulid::generate);
        let (bucket, key) = (&self.input.bucket, &self.input.key);
        let target = self
            .routing
            .as_ref()?
            .plain_target(bucket, key, id)
            .filter(|target| target.backend == upload.backend)?;
        self.version_id = Some(id);
        let stat_at = (target.backend.clone(), target.path.clone());
        let (same_name, path) = (target.same_name, target.path.clone());
        self.plain = Some(target);
        if !same_name {
            return Some(self.compose_plain(path, false));
        }
        Some(self.read_owner(stat_at))
    }

    /// The rule's record of the path decides first whether the file there may be replaced.
    /// The record is read at the path the object completes at. Without the rule nothing proves
    /// an owner, so the file there is kept.
    fn read_owner(&mut self, stat_at: (BackendRef, String)) -> Effects {
        let at = self.plain.clone().map(|target| PlainTarget {
            path: stat_at.1.clone(),
            ..target
        });
        self.plain_stat_at = Some(stat_at);
        match at.as_ref().and_then(owner_read) {
            Some(read) => {
                self.state = CompleteUploadState::ReadPlainOwner;
                smallvec![read]
            }
            None => {
                // The rules may have changed since the upload began, so nothing proves an owner.
                self.plain_owned = false;
                self.stat_plain()
            }
        }
    }

    pub(super) fn plain_owner(&mut self, event: Event) -> Effects {
        match owns_path(event, &self.input.key) {
            Ok(owned) => {
                self.plain_owned = owned;
                self.stat_plain()
            }
            Err(error) => self.schedule_error(error.into()),
        }
    }

    fn stat_plain(&mut self) -> Effects {
        let Some((backend, path)) = self.plain_stat_at.clone() else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };
        self.state = CompleteUploadState::StatPlain;
        smallvec![Effect::Blob(BlobEffect::StatPlain { backend, path })]
    }

    /// The provider upload of an upload that opened at its plain path.
    pub(super) fn plain_upload(&self) -> Option<&BackendUpload> {
        let upload = self.upload_record.as_ref()?.backend_upload.as_ref()?;
        upload.location.is_external().then_some(upload)
    }

    /// The object completes where its parts streamed to; the version id is the one the path
    /// was chosen with, also after a pattern change. The provider completes in place, so the
    /// path is looked at again.
    fn stat_in_place(&mut self, location: BackendLocation) -> Effects {
        let id = location.ulid;
        let (bucket, key, path) = (&self.input.bucket, &self.input.key, &location.backend_path);
        self.plain = self
            .routing
            .as_ref()
            .and_then(|routing| routing.saved_target(bucket, key, &location.backend, path));
        self.version_id = Some(id);
        self.read_owner((location.backend, location.backend_path))
    }

    pub(super) fn plain_stat(&mut self, event: Event) -> Effects {
        // Only a backend with versions keeps a file that appeared meanwhile; else it conflicts.
        if self.plain_upload().is_some() {
            return match event {
                Event::Blob(BlobEvent::PlainStat { exists, version }) => {
                    let replaces = exists && version.is_some() && self.plain_owned;
                    let path = self
                        .plain_upload()
                        .map(|upload| upload.location.backend_path.clone());
                    match (self.complete_provider(!replaces), path) {
                        // A provider upload completes at its own path only.
                        (Some(effects), Some(path)) => self.intent_first(&path, false, effects),
                        _ => self.schedule_error(CompleteUploadError::InvalidOperationState),
                    }
                }
                Event::Blob(BlobEvent::Error(error)) => self.schedule_error(error.into()),
                _ => self.schedule_error(CompleteUploadError::InvalidOperationState),
            };
        }
        let (Some(target), Some(id)) = (self.plain.as_ref(), self.version_id) else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };
        match event {
            Event::Blob(BlobEvent::PlainStat { exists, version }) => {
                let (path, replaces) = (&target.path, version.is_some() && self.plain_owned);
                let (path, if_not_exists) = same_name_path(path, id, exists, replaces);
                self.compose_plain(path, if_not_exists)
            }
            Event::Blob(BlobEvent::Error(error)) => self.schedule_error(error.into()),
            _ => self.schedule_error(CompleteUploadError::InvalidOperationState),
        }
    }

    /// Another writer took the free path first; the parts are still there, so the object
    /// takes its tagged path instead.
    pub(super) fn plain_taken(&mut self) -> Effects {
        // The refused compose moved no byte, so its intents must not claim the path.
        if !self.written_intent.is_empty() {
            self.state = CompleteUploadState::DropIntent;
            return smallvec![intent_deletes(&self.written_intent)];
        }
        // Parts in a provider upload cannot move to another path; the client uploads again.
        if self.plain_upload().is_some() {
            return self.schedule_error(BlobError::PathTaken.into());
        }
        let (Some(target), Some(id)) = (self.plain.as_ref(), self.version_id) else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };
        let tagged = tagged_path(&target.path, id);
        if self
            .plain_path
            .as_ref()
            .is_some_and(|plain| plain.path == tagged)
        {
            return self.schedule_error(BlobError::PathTaken.into());
        }
        self.compose_plain(tagged, true)
    }

    fn compose_plain(&mut self, path: String, if_not_exists: bool) -> Effects {
        let (Some(upload), Some(ulid)) = (self.upload_record.as_ref(), self.version_id) else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };
        let resolved = ResolvedBackend::new(upload.backend.clone(), upload.storage_class.clone());
        let plain = PlainPath {
            path,
            ulid,
            version_id: ulid,
            if_not_exists,
            exact: false,
        };
        self.plain_path = Some(plain.clone());
        self.state = CompleteUploadState::ComposeBlob;
        let path = plain.path.clone();
        let compose = smallvec![Effect::Blob(BlobEffect::Compose {
            bucket: self.input.bucket.clone(),
            key: self.input.key.clone(),
            resolved,
            created_by: self.input.created_by,
            parts: self
                .resolved_parts
                .iter()
                .map(|part| part.location.clone())
                .collect(),
            plain: Some(plain),
        })];
        self.intent_first(&path, if_not_exists, compose)
    }

    /// A listed plain object stores its identity before `compose` moves any byte to `path`.
    fn intent_first(&mut self, path: &str, if_not_exists: bool, compose: Effects) -> Effects {
        let (Some(target), Some(version_id)) = (self.plain.as_ref(), self.version_id) else {
            return compose;
        };
        if !target.listed {
            return compose;
        }
        let intent = WriteIntent {
            key: self.input.key.clone(),
            version_id,
            backend: target.backend.clone(),
            created_by: self.input.created_by,
            written: None,
            mover: None,
        };
        let keys = intent_keys(target.import_id, path, version_id, if_not_exists);
        let (rows, Some(rule)) = (intent_rows(&keys, &intent), self.rule_key()) else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };
        match rows {
            Ok(rows) => {
                self.written_intent = keys;
                self.pending_compose = Some(compose);
                self.state = CompleteUploadState::WriteIntent;
                self.fence.insert(FencedWrite::new(rule, rows)).start()
            }
            Err(error) => self.schedule_error(error.into()),
        }
    }

    /// The key of the import rule of a listed object.
    fn rule_key(&self) -> Option<Key> {
        let target = self.plain.as_ref().filter(|target| target.listed)?;
        let group_id = self.upload_record.as_ref()?.group_id;
        Some(import_key(group_id, target.import_id))
    }

    /// The refused compose goes on only once its intents are gone for good; a failed delete or
    /// sync keeps them and fails the upload with that error.
    pub(super) fn intent_dropped(&mut self, event: Event) -> Effects {
        let syncing = self.state == CompleteUploadState::SyncDroppedIntent;
        match event {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) if !syncing => {
                self.state = CompleteUploadState::SyncDroppedIntent;
                smallvec![Effect::Storage(StorageEffect::SyncAll)]
            }
            Event::Storage(StorageEvent::SyncAllFinished) if syncing => {
                self.written_intent.clear();
                self.plain_taken()
            }
            Event::Storage(StorageEvent::Error { error }) => self.schedule_error(error.into()),
            _ => self.schedule_error(CompleteUploadError::InvalidOperationState),
        }
    }

    /// Records the hash of the composed bytes in the intent of the path they reached. A lost
    /// record leaves the intent unproven, which an import reports instead of trusting.
    pub(super) fn write_evidence(&mut self) -> Option<Effects> {
        let target = self
            .plain
            .as_ref()
            .filter(|_| !self.written_intent.is_empty())?;
        let written = self.composed_location.as_ref()?;
        let intent = WriteIntent {
            key: self.input.key.clone(),
            version_id: self.version_id?,
            backend: target.backend.clone(),
            created_by: self.input.created_by,
            written: Some(written.get_blake3()?.try_into().ok()?),
            mover: None,
        };
        let path = &written.backend_path;
        let key = intent_key(target.import_id, path, intent.version_id);
        let (rows, rule) = (intent_rows(&[key], &intent), self.rule_key()?);
        let rows = match rows {
            Ok(rows) => rows,
            Err(error) => return Some(self.schedule_error(error.into())),
        };
        self.state = CompleteUploadState::WriteEvidence;
        Some(self.fence.insert(FencedWrite::new(rule, rows)).start())
    }

    /// The hash is stored with a read of the rule; a removed rule gets none, and the upload
    /// goes on as one without its rule.
    pub(super) fn evidence_written(&mut self, event: Event) -> Effects {
        let Some(fence) = self.fence.as_mut() else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };
        let effects = fence.step(event);
        match fence.outcome().cloned() {
            None => effects,
            Some(Fenced::Written | Fenced::RuleGone) => self.start_finalize(),
            Some(Fenced::Failed(Some(error))) => self.schedule_error(error.into()),
            Some(Fenced::Failed(None)) => {
                self.schedule_error(CompleteUploadError::InvalidOperationState)
            }
        }
    }

    /// The compose waits until its intent is stored with a read of the rule and synced to
    /// disk; a removed rule stores nothing and fails the upload.
    pub(super) fn intent_written(&mut self, event: Event) -> Effects {
        if self.state == CompleteUploadState::SyncIntent {
            return match (event, self.pending_compose.take()) {
                (Event::Storage(StorageEvent::SyncAllFinished), Some(compose)) => {
                    self.state = CompleteUploadState::ComposeBlob;
                    compose
                }
                (Event::Storage(StorageEvent::Error { error }), _) => {
                    self.schedule_error(error.into())
                }
                _ => self.schedule_error(CompleteUploadError::InvalidOperationState),
            };
        }
        let Some(fence) = self.fence.as_mut() else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };
        let effects = fence.step(event);
        match fence.outcome().cloned() {
            None => effects,
            Some(Fenced::Written) => {
                self.state = CompleteUploadState::SyncIntent;
                smallvec![Effect::Storage(StorageEffect::SyncAll)]
            }
            Some(Fenced::RuleGone) => {
                self.written_intent.clear();
                self.schedule_error(CompleteUploadError::ImportRemoved)
            }
            Some(Fenced::Failed(Some(error))) => self.schedule_error(error.into()),
            Some(Fenced::Failed(None)) => {
                self.schedule_error(CompleteUploadError::InvalidOperationState)
            }
        }
    }

    /// The committed version holds the identity its intent recorded, so the intent goes.
    pub(super) fn retire_intent(&mut self) -> Option<Effects> {
        if self.written_intent.is_empty() {
            return None;
        }
        let space = IMPORT_INTENT_KEYSPACE.to_string();
        let deletes = self
            .written_intent
            .iter()
            .map(|key| (space.clone(), key.clone()));
        self.state = CompleteUploadState::RetireIntent;
        Some(smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: deletes.collect(),
            txn_id: self.txn_id,
        })])
    }

    pub(super) fn intent_retired(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => self.write_obligation(),
            Event::Storage(StorageEvent::Error { error }) => self.schedule_error(error.into()),
            _ => self.schedule_error(CompleteUploadError::InvalidOperationState),
        }
    }

    /// Reads the version in the finalize transaction, so a registration meanwhile conflicts.
    pub(super) fn read_registered(&mut self, version_id: Ulid) -> Effects {
        let key = VersionKey::new(&self.input.bucket, &self.input.key, version_id).to_bytes();
        let key = match key {
            Ok(key) => key.into(),
            Err(error) => return self.schedule_error(error.into()),
        };
        self.state = CompleteUploadState::ReadRegistered;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    /// The same bytes an import registered finish the upload without a second head move or
    /// charge; other content under the version id is a conflict.
    pub(super) fn registered_read(&mut self, event: Event) -> Effects {
        let value = match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            Event::Storage(StorageEvent::Error { error }) => {
                return self.schedule_error(error.into());
            }
            _ => return self.schedule_error(CompleteUploadError::InvalidOperationState),
        };
        let Some(value) = value else {
            self.registered = false;
            return self.writer_trail();
        };
        let stored = BlobVersion::from_bytes(value.as_ref()).ok();
        let hash = stored.as_ref().and_then(|version| version.blob_hash());
        let written = self
            .final_location
            .as_ref()
            .and_then(|location| location.get_blake3());
        if hash.is_none() || hash.map(|hash| &hash[..]) != written {
            return self.schedule_error(CompleteUploadError::VersionExists);
        }
        self.registered = true;
        // The import's own location reads the file, so a second one would count it twice.
        let staged = self.final_location.as_ref().filter(|_| self.new_blob);
        let Some(key) = staged.and_then(|location| location.location_key().ok()) else {
            return self.delete_upload_records();
        };
        self.new_blob = false;
        self.state = CompleteUploadState::DropStagedLocation;
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
            key: key.to_bytes().into(),
            txn_id: self.txn_id,
        })]
    }

    pub(super) fn staged_dropped(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::DeleteResult { .. }) => self.delete_upload_records(),
            Event::Storage(StorageEvent::Error { error }) => self.schedule_error(error.into()),
            _ => self.schedule_error(CompleteUploadError::InvalidOperationState),
        }
    }

    /// An upload whose version an import registered and a user then deleted or purged must not
    /// publish it again, so its unregistered version reads the trail before the head moves.
    fn writer_trail(&mut self) -> Effects {
        let (Some(version_id), Some(record)) = (self.version_id, self.upload_record.as_ref())
        else {
            return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
        };
        let group_id = record.group_id;
        let scan = self
            .trail_scan
            .get_or_insert_with(|| TrailScan::new(version_id));
        scan.writer = true;
        let Some(incarnation) = scan.incarnation else {
            self.state = CompleteUploadState::ReadTrail;
            return smallvec![TrailScan::creation_read(&self.input.bucket, self.txn_id)];
        };
        let bucket = (self.input.bucket.as_str(), incarnation);
        let ranges = trail_ranges(group_id, bucket, &self.input.key);
        let Some(prefix) = ranges.get(scan.range).cloned() else {
            let existing = self.existing_pointer.clone();
            return self.write_head(existing.as_ref(), version_id);
        };
        let start = scan.start(&prefix);
        self.state = CompleteUploadState::ReadTrail;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: DELETE_TRAIL_KEYSPACE.to_string(),
            prefix: Some(prefix),
            start,
            limit: TRAIL_PAGE,
            txn_id: self.txn_id,
        })]
    }

    pub(super) fn trail_read(&mut self, event: Event) -> Effects {
        let creation = self
            .trail_scan
            .as_mut()
            .map(|scan| scan.take_creation(&event));
        if creation == Some(true) {
            return self.writer_trail();
        }
        let (values, next) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => {
                return self.schedule_error(error.into());
            }
            _ => return self.schedule_error(CompleteUploadError::InvalidOperationState),
        };
        let (bucket, key) = (&self.input.bucket, &self.input.key);
        let Some(scan) = self.trail_scan.as_mut() else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };
        for (_, value) in values {
            match BlobAuditRecord::from_bytes(&value) {
                Ok(record) => scan.trail = scan.trail.add(&record, bucket, key, scan.version_id),
                Err(error) => return self.schedule_error(error.into()),
            }
        }
        scan.range += usize::from(next.is_none());
        scan.after = next;
        if scan.trail == Trail::Removed {
            return self.schedule_error(CompleteUploadError::HistoryDeleted);
        }
        self.writer_trail()
    }

    /// Reads the rule of a listed plain object in the finalize transaction, so its import
    /// row is written only while the rule exists.
    pub(super) fn plain_fence(&mut self) -> Option<Effects> {
        let target = self.plain.as_ref().filter(|target| target.listed)?;
        let group_id = self.upload_record.as_ref()?.group_id;
        // Which other rules read the composed path is read in this transaction first.
        if !self.readers.done() {
            self.state = CompleteUploadState::ReadReaders;
            return Some(smallvec![self.readers.page(group_id, self.txn_id)]);
        }
        let rule = (
            BACKEND_IMPORT_KEYSPACE.to_string(),
            import_key(group_id, target.import_id),
        );
        self.state = CompleteUploadState::ReadImportRule;
        Some(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: std::iter::once(rule).chain(self.shared_rows()).collect(),
            txn_id: self.txn_id,
        })])
    }

    pub(super) fn readers_read(&mut self, event: Event) -> Effects {
        let path = self
            .composed_location
            .as_ref()
            .map(|written| written.backend_path.clone());
        let group_id = self.upload_record.as_ref().map(|record| record.group_id);
        let (Some(target), Some(path), Some(group_id)) = (self.plain.as_mut(), path, group_id)
        else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };
        let reading = (target.import_id, &target.backend, path.as_str());
        match self.readers.read(event, reading) {
            Ok(Some(readers)) => {
                target.shared = readers;
                let invalid = CompleteUploadError::InvalidOperationState;
                self.plain_fence()
                    .unwrap_or_else(|| self.schedule_error(invalid))
            }
            Ok(None) => smallvec![self.readers.page(group_id, self.txn_id)],
            Err(_) => self.schedule_error(CompleteUploadError::InvalidOperationState),
        }
    }

    /// The records other rules sharing the folder would keep of the composed path.
    fn shared_rows(&self) -> Vec<(String, Key)> {
        let (Some(target), Some(written)) = (self.plain.as_ref(), self.composed_location.as_ref())
        else {
            return Vec::new();
        };
        let path = &written.backend_path;
        let rows = target
            .shared
            .iter()
            .map(|import_id| object_key(*import_id, path));
        rows.map(|key| (IMPORTED_OBJECT_KEYSPACE.to_string(), key))
            .collect()
    }

    /// The rule must exist, and no other rule sharing the folder may hold the path.
    pub(super) fn import_rule_read(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::BatchReadResult { values }) => {
                let Some(((_, rule), others)) = values.split_first() else {
                    return self.schedule_error(CompleteUploadError::InvalidOperationState);
                };
                if others.len() != self.shared_rows().len() {
                    return self.schedule_error(CompleteUploadError::InvalidOperationState);
                }
                if others.iter().any(|(_, row)| row.is_some()) {
                    return self.schedule_error(BlobError::PathTaken.into());
                }
                self.write_cleanup_records(rule.is_some())
            }
            Event::Storage(StorageEvent::Error { error }) => self.schedule_error(error.into()),
            _ => self.schedule_error(CompleteUploadError::InvalidOperationState),
        }
    }

    /// The backend version and import row of a plain object, written with its version.
    pub(super) fn plain_records(
        &self,
        rule_exists: bool,
    ) -> Result<Vec<(String, Key, Value)>, aruna_core::errors::ConversionError> {
        let (Some(written), Some(output), Some(version_id)) = (
            self.composed_location.as_ref(),
            self.final_location.as_ref(),
            self.version_id,
        ) else {
            return Ok(Vec::new());
        };
        // A deduplicated version reads the older location, so it needs no version row.
        let version = self.plain_version.as_deref();
        let version = version.filter(|_| output.ulid == written.ulid);
        // An object completed in place after its rule was removed still keeps its version.
        let Some(target) = self.plain.as_ref() else {
            let row = version.map(|version| version_row(written, version));
            return Ok(row.into_iter().collect());
        };
        let record = PlainRecord {
            target,
            written,
            version,
            etag: self.plain_etag.as_deref(),
            version_id,
            key: &self.input.key,
        };
        let mut writes = record.writes(rule_exists)?;
        writes.extend(self.dedup_release().transpose()?);
        Ok(writes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::keyspaces::{BACKEND_VERSION_KEYSPACE, IMPORTED_OBJECT_KEYSPACE};
    use aruna_core::structs::storage::backend_import::{
        BackendImport, DEFAULT_PATTERN, ImportedObject, object_key,
    };
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::multipart::BackendUpload;
    use aruna_core::structs::storage::routing::{BackendCatalog, GroupRoutingInputs};
    use std::collections::BTreeSet;

    fn backend_id() -> Ulid {
        Ulid::from_bytes([5u8; 16])
    }

    fn operation(pattern: &str) -> CompleteUploadOperation {
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let input = CompleteUploadInput {
            bucket: "bucket".to_string(),
            key: "in/object".to_string(),
            upload_id: Ulid::from_parts(1, 1),
            realm_id,
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
            completed_parts: vec![],
            expected_checksums: vec![],
            checksum_algorithm: None,
            checksum_type: MultipartChecksumType::FullObject,
            checksum_type_explicit: false,
            object_size: None,
            created_by: UserId::local(Ulid::from_parts(2, 2), realm_id),
            quota_ceiling: None,
            now_ms: 0,
        };
        let record = MultipartUpload {
            upload_id: input.upload_id,
            backend: BackendRef::Group(backend_id()),
            storage_class: None,
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            group_id: Ulid::from_parts(3, 3),
            created_by: input.created_by,
            created_at: SystemTime::UNIX_EPOCH,
            status: MultipartUploadStatus::Completing,
            checksum_hint: None,
            metadata: HashMap::new(),
            placement_policies: Vec::new(),
            subject_generation: 0,
            completing_since_ms: None,
            backend_upload: None,
            encryption: None,
        };
        let rule = BackendImport {
            import_id: Ulid::from_bytes([4u8; 16]),
            group_id: Ulid::from_parts(3, 3),
            backend_id: backend_id(),
            backend_prefix: "raw/".to_string(),
            bucket: "bucket".to_string(),
            key_prefix: "in/".to_string(),
            schedule: None,
            created_by: UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: pattern.to_string(),
        };
        let catalog = BackendCatalog::new("default").with_backend("default", None);
        let routing = RoutingSnapshot::new(Ulid::from_parts(3, 3), catalog).with_group_inputs(
            GroupRoutingInputs {
                backend_ids: BTreeSet::from([backend_id()]),
                imports: vec![rule],
                ..Default::default()
            },
        );
        let mut operation = CompleteUploadOperation::new(input).with_routing(routing);
        operation.upload_record = Some(record);
        operation
    }

    fn record_writes(exists: bool, committed: Option<Ulid>) -> Vec<(String, Key, Value)> {
        recorded(exists, committed).1
    }

    fn recorded(
        exists: bool,
        committed: Option<Ulid>,
    ) -> (CompleteUploadOperation, Vec<(String, Key, Value)>) {
        let mut operation = operation("{dir}{name}");
        operation.compose_blob();
        operation.plain_version = Some("v1".to_string());
        let id = operation.version_id.unwrap();
        operation.composed_location = Some(BackendLocation {
            backend: BackendRef::Group(backend_id()),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: "raw/object".to_string(),
            ulid: id,
            format: aruna_core::structs::storage::format::StoredFormat::default(),
            created_by: UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 3,
            hashes: HashMap::from([(
                aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
                vec![7u8; 32],
            )]),
        });
        operation.final_location = operation.composed_location.clone();
        if let (Some(location), Some(ulid)) = (operation.final_location.as_mut(), committed) {
            location.ulid = ulid;
        }
        let writes = finalize_writes(&mut operation, exists);
        (operation, writes)
    }

    /// Drives a composed upload through its finalize records, with or without the rule.
    fn finalize_writes(
        operation: &mut CompleteUploadOperation,
        exists: bool,
    ) -> Vec<(String, Key, Value)> {
        operation.txn_id = Some(Ulid::nil());
        operation.state = CompleteUploadState::DeleteUploadRecords;
        let effects = operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        let effects = fence_read(operation, effects, exists, None);
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected the records, got {effects:?}")
        };
        writes.clone()
    }

    /// Answers the read of the rule and of the rows other rules keep of the path.
    /// Answers the scan of the rules that read the path with `readers`.
    fn past_readers(
        operation: &mut CompleteUploadOperation,
        effects: Effects,
        readers: &[BackendImport],
    ) -> Effects {
        let [Effect::Storage(StorageEffect::Iter { key_space, .. })] = effects.as_slice() else {
            return effects;
        };
        assert_eq!(key_space, BACKEND_IMPORT_KEYSPACE);
        let values = readers
            .iter()
            .map(|rule| (rule.key(), rule.to_bytes().unwrap().into()))
            .collect();
        operation.step(Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        }))
    }

    fn fence_read(
        operation: &mut CompleteUploadOperation,
        effects: Effects,
        exists: bool,
        other: Option<Value>,
    ) -> Effects {
        let effects = past_readers(operation, effects, &[]);
        let [Effect::Storage(StorageEffect::BatchRead { reads, .. })] = effects.as_slice() else {
            panic!("expected the rule read, got {effects:?}")
        };
        assert_eq!(reads[0].0, BACKEND_IMPORT_KEYSPACE);
        let mut values = vec![(Key::from(Vec::new()), exists.then(|| vec![0u8].into()))];
        values.extend(
            reads[1..]
                .iter()
                .map(|(_, key)| (key.clone(), other.clone())),
        );
        operation.step(Event::Storage(StorageEvent::BatchReadResult { values }))
    }

    #[test]
    fn drops_orphan_record() {
        // A removed rule gets no record: the transaction that read it writes nothing.
        let mut operation = operation(DEFAULT_PATTERN);
        operation.compose_blob();
        let txn_id = Ulid::from_parts(77, 7);
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        operation.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert!(operation.written_intent.is_empty());
        assert_eq!(
            operation.cleanup.take_error(),
            Some(CompleteUploadError::ImportRemoved)
        );
    }

    #[test]
    fn keeps_shared_path() {
        // Another rule of the folder recorded the path, so the upload does not take it.
        let (mut operation, _) = recorded(true, None);
        let other = Ulid::from_parts(9, 9);
        // The rule reads the folder, created after the upload was routed.
        let reader = BackendImport {
            import_id: other,
            group_id: Ulid::from_parts(3, 3),
            backend_id: backend_id(),
            backend_prefix: "raw/".to_string(),
            bucket: "other".to_string(),
            key_prefix: String::new(),
            schedule: None,
            created_by: UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: DEFAULT_PATTERN.to_string(),
        };
        operation.readers = Default::default();
        operation.state = CompleteUploadState::DeleteUploadRecords;
        let effects = operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        let effects = past_readers(&mut operation, effects, &[reader]);
        let [Effect::Storage(StorageEffect::BatchRead { reads, .. })] = effects.as_slice() else {
            panic!("expected the fence read, got {effects:?}")
        };
        assert_eq!(reads[1].1, object_key(other, "raw/object"));
        fence_read(&mut operation, effects, true, Some(vec![1].into()));
        let error = operation.cleanup.take_error();
        assert_eq!(
            error,
            Some(CompleteUploadError::BlobError(BlobError::PathTaken))
        );
    }

    fn count(writes: &[(String, Key, Value)], keyspace: &str) -> usize {
        writes
            .iter()
            .filter(|(key_space, _, _)| key_space == keyspace)
            .count()
    }

    #[test]
    fn adopts_registered_version() {
        let found = |stored: Option<[u8; 32]>, staged: bool| {
            let (mut operation, _) = recorded(true, None);
            operation.new_blob = staged;
            operation.state = CompleteUploadState::ReadLivenessVersion;
            // The version is read in the finalize transaction before the head moves.
            let effects = operation.write_current_lookup(None);
            let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice()
            else {
                panic!("expected the version read, got {effects:?}")
            };
            assert_eq!(key_space, aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE);
            let version = stored.map(|hash| {
                let version = BlobVersion::materialized(
                    hash,
                    BackendRef::Group(backend_id()),
                    aruna_core::structs::storage::format::EncodingClass::Raw,
                    SystemTime::UNIX_EPOCH,
                    UserId::default(),
                    None,
                );
                Value::from(version.to_bytes().unwrap())
            });
            let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
                key: Vec::new().into(),
                value: version,
            }));
            (operation, effects)
        };
        // An unregistered version reads the delete trail before its head moves.
        let (mut operation, _) = found(None, false);
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { key_space, .. })]
                if key_space == DELETE_TRAIL_KEYSPACE
        ));
        let page = |values| {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after: None,
            })
        };
        operation.step(page(Vec::new()));
        let effects = operation.step(page(Vec::new()));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { key_space, .. })]
                if key_space == aruna_core::keyspaces::BLOB_HEAD_KEYSPACE
        ));
        // A user deleted the version an import had registered: it is not published again.
        let (mut operation, _) = found(None, false);
        let record = BlobAuditRecord {
            realm_id: RealmId::from_bytes([3u8; 32]),
            group_id: Ulid::nil(),
            node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            user_id: UserId::default(),
            kind: aruna_core::structs::storage::delete_audit::BlobAuditKind::DeleteVersion,
            bucket: "bucket".to_string(),
            key: "in/object".to_string(),
            version_id: operation.version_id,
            occurred_at_ms: 1,
        };
        let entry = (Vec::new().into(), record.to_bytes().unwrap().into());
        operation.step(page(vec![entry]));
        assert_eq!(
            operation.cleanup.take_error(),
            Some(CompleteUploadError::HistoryDeleted)
        );
        // The same bytes registered by an import: only the upload is cleared, then committed.
        let (mut operation, _) = found(Some([7u8; 32]), false);
        assert_eq!(operation.state, CompleteUploadState::ReadUploadRecord);
        // A location the upload staged for that file goes, so the file counts once.
        let (mut staged, effects) = found(Some([7u8; 32]), true);
        let [Effect::Storage(StorageEffect::Delete { key_space, .. })] = effects.as_slice() else {
            panic!("expected the staged location delete, got {effects:?}")
        };
        assert_eq!(key_space, BLOB_LOCATIONS_KEYSPACE);
        staged.step(Event::Storage(StorageEvent::DeleteResult {
            key: Vec::new().into(),
        }));
        assert_eq!(staged.state, CompleteUploadState::ReadUploadRecord);
        let effects = operation.write_obligation();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { .. })]
        ));
        let (mut operation, _) = found(Some([8u8; 32]), false);
        assert_eq!(
            operation.cleanup.take_error(),
            Some(CompleteUploadError::VersionExists)
        );
    }

    #[test]
    fn retires_intent() {
        let (mut operation, _) = recorded(true, None);
        let id = operation.version_id.unwrap();
        let keys = intent_keys(Ulid::from_bytes([4u8; 16]), "raw/object", id, true);
        operation.written_intent = keys.clone();
        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        // The intent goes in the version transaction, before the replication obligation.
        let [Effect::Storage(StorageEffect::BatchDelete { deletes, txn_id })] = effects.as_slice()
        else {
            panic!("expected the intent delete, got {effects:?}")
        };
        // Every intent the compose recorded goes, the tagged path's as well.
        let found: Vec<_> = deletes.iter().map(|(_, key)| key.clone()).collect();
        assert_eq!(found, keys);
        assert_eq!(*txn_id, Some(Ulid::nil()));
        let effects = operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        assert!(!effects.is_empty());
        assert_ne!(operation.state, CompleteUploadState::RetireIntent);
    }

    #[test]
    fn fences_import_row() {
        let rows = |exists: bool| count(&record_writes(exists, None), IMPORTED_OBJECT_KEYSPACE);
        assert_eq!(rows(true), 1);
        // A rule removed meanwhile gets no row its removal would miss.
        assert_eq!(rows(false), 0);
    }

    #[test]
    fn drops_deduplicated_version() {
        let versions = |committed| count(&record_writes(true, committed), BACKEND_VERSION_KEYSPACE);
        assert_eq!(versions(None), 1);
        // Dedup keeps an older location at the same path, so the new one gets no version row.
        assert_eq!(versions(Some(Ulid::from_parts(6, 6))), 0);
    }

    /// Answers the intent delete that comes after a refused compose, and returns its key.
    fn past_drop(operation: &mut CompleteUploadOperation, effects: Effects) -> (Key, Effects) {
        let [Effect::Storage(StorageEffect::BatchDelete { deletes, txn_id })] = effects.as_slice()
        else {
            panic!("expected the intent delete, got {effects:?}")
        };
        assert_eq!(
            (deletes[0].0.as_str(), *txn_id),
            (IMPORT_INTENT_KEYSPACE, None)
        );
        let key = deletes[0].1.clone();
        let event = Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        });
        // The compose goes on only once the delete is on disk.
        let effects = operation.step(event);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::SyncAll)]
        ));
        (
            key,
            operation.step(Event::Storage(StorageEvent::SyncAllFinished)),
        )
    }

    /// Answers the read of the rule after a record or its hash is stored.
    /// Answers the transaction and rule read of a fenced record write, up to its rows.
    fn past_fence(operation: &mut CompleteUploadOperation, effects: Effects) -> Effects {
        let starting = operation.fence.as_ref().is_some_and(FencedWrite::starting);
        let [Effect::Storage(StorageEffect::StartTransaction { .. })] = effects.as_slice() else {
            return effects;
        };
        if !starting {
            return effects;
        }
        let txn_id = Ulid::from_parts(77, 7);
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice() else {
            panic!("expected the rule read, got {effects:?}")
        };
        assert_eq!(key_space, BACKEND_IMPORT_KEYSPACE);
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(vec![1].into()),
        }))
    }

    /// Answers the rows and the commit of a fenced record write.
    fn fence_done(operation: &mut CompleteUploadOperation) -> Effects {
        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: Ulid::from_parts(77, 7),
        }))
    }

    /// Answers the stored and synced write intent that comes before a plain compose.
    fn past_intent(operation: &mut CompleteUploadOperation, effects: Effects) -> Effects {
        let effects = past_fence(operation, effects);
        match effects.as_slice() {
            [Effect::Storage(StorageEffect::BatchWrite { writes, .. })]
                if writes[0].0 == aruna_core::keyspaces::IMPORT_INTENT_KEYSPACE =>
            {
                let effects = fence_done(operation);
                assert!(matches!(
                    effects.as_slice(),
                    [Effect::Storage(StorageEffect::SyncAll)]
                ));
                operation.step(Event::Storage(StorageEvent::SyncAllFinished))
            }
            _ => effects,
        }
    }

    #[test]
    fn orders_intent_acks() {
        // The compose waits for the intent write and then its sync, each answered once.
        let mut operation = operation(DEFAULT_PATTERN);
        let effects = operation.compose_blob();
        let effects = past_fence(&mut operation, effects);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchWrite { .. })]
        ));
        let effects = operation.step(Event::Storage(StorageEvent::SyncAllFinished));
        let compose = |effect: &Effect| matches!(effect, Effect::Blob(BlobEffect::Compose { .. }));
        assert!(!effects.iter().any(compose), "{effects:?}");
        assert_ne!(operation.state, CompleteUploadState::ComposeBlob);
    }

    #[test]
    fn composes_plain_path() {
        let mut operation = operation(DEFAULT_PATTERN);
        let effects = operation.compose_blob();
        let effects = past_fence(&mut operation, effects);

        // No byte moves before the identity of the object is stored.
        let id = operation.version_id.expect("the path names the version");
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected the write intent, got {effects:?}")
        };
        let [(_, key, value)] = writes.as_slice() else {
            panic!("a unique path gets one intent, got {writes:?}")
        };
        let path = format!("raw/object~{id}");
        let import_id = Ulid::from_bytes([4u8; 16]);
        let intent_key = aruna_core::structs::storage::backend_import::intent_key;
        assert_eq!(*key, intent_key(import_id, &path, id));
        let intent = WriteIntent::from_bytes(value).unwrap();
        assert_eq!((intent.key.as_str(), intent.version_id), ("in/object", id));
        let effects = past_intent(&mut operation, effects);
        let [
            Effect::Blob(BlobEffect::Compose {
                plain: Some(plain), ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected a plain compose, got {effects:?}")
        };
        assert_eq!(plain.path, format!("raw/object~{id}"));
        assert_eq!(plain.ulid, id);
    }

    /// Answers the read of the rule's record at the path with a row for `key`, or none.
    fn owner_answer(operation: &mut CompleteUploadOperation, key: Option<String>) -> Effects {
        let row = key.map(|key| {
            let row = ImportedObject::written(Ulid::from_parts(8, 8), key, 1, None);
            row.to_bytes().unwrap().into()
        });
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: row,
        }))
    }

    #[test]
    fn falls_back_tagged() {
        let mut operation = operation("{dir}{name}");
        operation.compose_blob();
        let id = operation.version_id.unwrap();
        let effects = owner_answer(&mut operation, None);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::StatPlain { path, .. })] if path == "raw/object"
        ));
        let effects = operation.step(Event::Blob(BlobEvent::PlainStat {
            exists: false,
            version: None,
        }));
        let composed = |effects: &Effects| match effects.as_slice() {
            [
                Effect::Blob(BlobEffect::Compose {
                    plain: Some(plain), ..
                }),
            ] => plain.clone(),
            _ => panic!("expected a plain compose, got {effects:?}"),
        };
        let plain = composed(&past_intent(&mut operation, effects));
        assert_eq!(plain.path, "raw/object");
        assert!(plain.if_not_exists);

        // Another upload took the free path first; the parts still compose next to it.
        let taken = || Event::Blob(BlobEvent::Error(BlobError::PathTaken));
        let effects = operation.step(taken());
        // The refused compose moved no byte, so the intent of the free path goes.
        let (key, effects) = past_drop(&mut operation, effects);
        assert_eq!(
            key,
            intent_key(Ulid::from_bytes([4u8; 16]), "raw/object", id)
        );
        let plain = composed(&past_intent(&mut operation, effects));
        assert_eq!(plain.path, format!("raw/object~{id}"));
        assert!(plain.if_not_exists);

        let effects = operation.step(taken());
        let (_, effects) = past_drop(&mut operation, effects);
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::Blob(BlobEffect::Compose { .. }))),
            "a taken tagged path is not tried again: {effects:?}"
        );
        assert_eq!(
            operation.cleanup.take_error(),
            Some(CompleteUploadError::BlobError(BlobError::PathTaken))
        );
    }

    #[test]
    fn keeps_foreign_file() {
        // Another key's file with versions is kept; the object composes at its tagged path.
        let mut operation = operation("{dir}{name}");
        operation.compose_blob();
        let id = operation.version_id.unwrap();
        owner_answer(&mut operation, Some("other".to_string()));
        let effects = operation.step(Event::Blob(BlobEvent::PlainStat {
            exists: true,
            version: Some("v0".to_string()),
        }));
        let effects = past_intent(&mut operation, effects);
        let [
            Effect::Blob(BlobEffect::Compose {
                plain: Some(plain), ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected a plain compose, got {effects:?}")
        };
        assert_eq!(plain.path, format!("raw/object~{id}"));
        assert!(plain.if_not_exists);
    }

    #[test]
    fn proves_unlisted_owner() {
        // The rules may change during an upload, so a path without a record is never replaced.
        let mut operation = operation("{dir}{name}");
        let routing = operation.routing.as_mut().expect("the upload's routing");
        routing.imports[0].write_prefix = Some("out/".to_string());
        let effects = operation.compose_blob();
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::Blob(BlobEffect::StatPlain { path, .. })] if path == "out/object"
            ),
            "expected the path stat, got {effects:?}"
        );
        let id = operation.version_id.unwrap();
        operation.step(Event::Blob(BlobEvent::PlainStat {
            exists: true,
            version: Some("v0".to_string()),
        }));
        let plain = operation.plain_path.as_ref().expect("the compose path");
        assert_eq!(plain.path, format!("out/object~{id}"));
    }

    /// An upload whose provider upload opened at `raw/object` as version `id`, a path the
    /// rule recorded for `owner`.
    fn in_place_for(id: Ulid, owner: Option<String>) -> CompleteUploadOperation {
        let mut operation = operation("{dir}{name}");
        open_at(&mut operation, "raw/object", id, owner);
        operation
    }

    /// Opens the provider upload at `path` as version `id`, recorded for `owner`, and starts
    /// its completion.
    fn open_at(
        operation: &mut CompleteUploadOperation,
        path: &str,
        id: Ulid,
        owner: Option<String>,
    ) {
        let location = BackendLocation {
            backend: BackendRef::Group(backend_id()),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: path.to_string(),
            ulid: id,
            format: Default::default(),
            created_by: UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 0,
            hashes: HashMap::new(),
        };
        if let Some(upload) = operation.upload_record.as_mut() {
            upload.backend_upload = Some(BackendUpload {
                location,
                upload_id: "provider".to_string(),
                record_id: Ulid::from_parts(1, 1),
            });
        }
        operation.compose_blob();
        let effects = owner_answer(operation, owner);
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::Blob(BlobEffect::StatPlain { path: stat, .. })] if stat == path
            ),
            "expected the path stat, got {effects:?}"
        );
        assert_eq!(operation.version_id, Some(id));
    }

    fn in_place(id: Ulid) -> CompleteUploadOperation {
        let key = operation("{dir}{name}").input.key;
        in_place_for(id, Some(key))
    }

    fn provider_condition(effects: &Effects) -> bool {
        match effects.as_slice() {
            [Effect::Blob(BlobEffect::CompleteUpload { if_not_exists, .. })] => *if_not_exists,
            _ => panic!("expected the provider completion, got {effects:?}"),
        }
    }

    #[test]
    fn completes_in_place() {
        // Only a backend with versions keeps a file that appeared meanwhile at the path.
        let id = Ulid::from_parts(6, 6);
        for (exists, version, if_not_exists) in [
            (false, None, true),
            (true, None, true),
            (true, Some("v0".to_string()), false),
        ] {
            let mut operation = in_place(id);
            let effects = operation.step(Event::Blob(BlobEvent::PlainStat { exists, version }));
            let effects = past_intent(&mut operation, effects);
            assert_eq!(provider_condition(&effects), if_not_exists);
        }
        // A file the rule recorded for another key, or never, is kept even with versions.
        for owner in [Some("other".to_string()), None] {
            let mut operation = in_place_for(id, owner);
            let effects = operation.step(Event::Blob(BlobEvent::PlainStat {
                exists: true,
                version: Some("v0".to_string()),
            }));
            let effects = past_intent(&mut operation, effects);
            assert!(provider_condition(&effects));
        }

        // The object written in place gets the records of a plain write.
        let mut operation = in_place(id);
        written_in_place(&mut operation);
        let writes = finalize_writes(&mut operation, true);
        assert_eq!(count(&writes, BACKEND_VERSION_KEYSPACE), 1);
        assert_eq!(count(&writes, IMPORTED_OBJECT_KEYSPACE), 1);
    }

    #[test]
    fn keeps_saved_identity() {
        // The upload opened under a suffix pattern; the plain pattern cannot name this key.
        let id = Ulid::from_parts(6, 6);
        let path = format!("raw/report ~{id}");
        let routing = operation("{dir}{name}").routing.unwrap();
        assert_eq!(routing.plain_target("bucket", "in/report ", id), None);
        let opened = || {
            let mut operation = operation("{dir}{name}");
            operation.input.key = "in/report ".to_string();
            let key = operation.input.key.clone();
            open_at(&mut operation, &path, id, Some(key));
            operation
        };
        let mut operation = opened();
        let effects = operation.step(Event::Blob(BlobEvent::PlainStat {
            exists: false,
            version: None,
        }));
        let effects = past_fence(&mut operation, effects);
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected the write intent, got {effects:?}")
        };
        let intent_key = aruna_core::structs::storage::backend_import::intent_key;
        assert_eq!(
            writes[0].1,
            intent_key(Ulid::from_bytes([4u8; 16]), &path, id)
        );
        let mut operation = opened();
        written_in_place(&mut operation);
        let writes = finalize_writes(&mut operation, true);
        assert_eq!(count(&writes, IMPORTED_OBJECT_KEYSPACE), 1);
    }

    #[test]
    fn keeps_unclaimed_file() {
        // The rule was removed before completion, so nothing proves the file there is ours.
        let id = Ulid::from_parts(6, 6);
        let mut operation = in_place_for(id, None);
        operation.plain = None;
        operation.read_owner((BackendRef::Group(backend_id()), "raw/object".to_string()));
        let effects = operation.step(Event::Blob(BlobEvent::PlainStat {
            exists: true,
            version: Some("v0".to_string()),
        }));
        assert!(provider_condition(&effects));
    }

    #[test]
    fn reads_path_owner() {
        // An upload opened at a tagged path asks who owns that path, not the plain one.
        let mut operation = in_place(Ulid::from_parts(6, 6));
        let effects = operation.read_owner((
            BackendRef::Group(backend_id()),
            "raw/object~tag".to_string(),
        ));
        let [Effect::Storage(StorageEffect::Read { key, .. })] = effects.as_slice() else {
            panic!("expected the owner read, got {effects:?}")
        };
        let import_id = operation.plain.as_ref().unwrap().import_id;
        assert_eq!(*key, object_key(import_id, "raw/object~tag"));
    }

    #[test]
    fn checks_ruleless_replay() {
        // An upload opened at a provider path is checked for a registered or deleted version
        // also after its rule was removed.
        let mut operation = in_place(Ulid::from_parts(6, 6));
        operation.plain = None;
        written_in_place(&mut operation);
        operation.txn_id = Some(Ulid::nil());
        let effects = operation.write_current_lookup(None);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE
        ));
    }

    #[test]
    fn pins_without_rule() {
        // The rule was removed before completion: no import row, but the version stays pinned.
        let mut operation = in_place(Ulid::from_parts(6, 6));
        operation.plain = None;
        written_in_place(&mut operation);
        operation.txn_id = Some(Ulid::nil());
        operation.state = CompleteUploadState::DeleteUploadRecords;
        let effects = operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected the records, got {effects:?}")
        };
        assert_eq!(count(writes, BACKEND_VERSION_KEYSPACE), 1);
        assert_eq!(count(writes, IMPORTED_OBJECT_KEYSPACE), 0);
    }

    /// Completes a stat-checked in-place upload with backend version `v1`.
    fn written_in_place(operation: &mut CompleteUploadOperation) {
        let effects = operation.step(Event::Blob(BlobEvent::PlainStat {
            exists: false,
            version: None,
        }));
        past_intent(operation, effects);
        let mut location = operation.plain_upload().unwrap().location.clone();
        location.blob_size = 3;
        location.hashes = HashMap::from([(
            aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
            vec![7u8; 32],
        )]);
        let effects = operation.step(Event::Blob(BlobEvent::PlainWritten {
            location: location.clone(),
            version: Some("v1".to_string()),
            etag: None,
        }));
        // A listed object's intent records the hash of the bytes before the finalize.
        let effects = past_fence(operation, effects);
        let effects = match effects.as_slice() {
            [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] => {
                let intent = WriteIntent::from_bytes(&writes[0].2).unwrap();
                assert_eq!(intent.written, Some([7u8; 32]));
                fence_done(operation)
            }
            _ => effects,
        };
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::StartTransaction { .. })]
            ),
            "expected the finalize transaction, got {effects:?}"
        );
        assert_eq!(operation.composed_location.as_ref(), Some(&location));
        operation.final_location = operation.composed_location.clone();
    }

    #[test]
    fn taken_in_place() {
        // A file that took the free path first fails the upload; its parts cannot move.
        let mut operation = in_place(Ulid::from_parts(6, 6));
        let effects = operation.step(Event::Blob(BlobEvent::PlainStat {
            exists: false,
            version: None,
        }));
        past_intent(&mut operation, effects);
        let effects = operation.step(Event::Blob(BlobEvent::Error(BlobError::PathTaken)));
        let (_, effects) = past_drop(&mut operation, effects);
        assert!(
            !effects.iter().any(|effect| matches!(
                effect,
                Effect::Blob(BlobEffect::Compose { .. } | BlobEffect::CompleteUpload { .. })
            )),
            "a taken path is not tried again: {effects:?}"
        );
        assert_eq!(
            operation.cleanup.take_error(),
            Some(CompleteUploadError::BlobError(BlobError::PathTaken))
        );
    }

    #[test]
    fn keeps_provider_layout() {
        // An upload opened before the rule assembles in Aruna's own layout.
        let mut operation = operation(DEFAULT_PATTERN);
        let location = BackendLocation {
            backend: BackendRef::Group(backend_id()),
            storage_class: None,
            root: String::new(),
            storage_bucket: "data".to_string(),
            backend_path: "bucket/in/object".to_string(),
            ulid: Ulid::from_parts(6, 6),
            format: Default::default(),
            created_by: UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 0,
            hashes: HashMap::new(),
        };
        if let Some(upload) = operation.upload_record.as_mut() {
            upload.backend_upload = Some(BackendUpload {
                location,
                upload_id: "provider".to_string(),
                record_id: Ulid::from_parts(1, 1),
            });
        }
        let effects = operation.compose_blob();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::CompleteUpload { .. })]
        ));
    }

    fn read(value: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: value.map(Into::into),
        })
    }

    #[test]
    fn reuses_current_file() {
        use aruna_core::keyspaces::{BLOB_CLEANUP_KEYSPACE, BLOB_HEAD_KEYSPACE};
        use aruna_core::structs::storage::blob::{
            BlobCleanupWork, BlobVersion, CurrentVersionPointer,
        };
        use aruna_core::structs::storage::format::StoredLayout;
        // A completed upload of the current version's bytes reads that version's file.
        let (mut operation, _) = recorded(false, None);
        let written = operation.composed_location.clone().unwrap();
        let mut old = written.clone();
        (old.ulid, old.backend_path) = (Ulid::from_parts(8, 8), "raw/object~old".to_string());
        old.format.layout = StoredLayout::Plain(old.ulid);
        operation.plain_version = Some("v2".to_string());
        (operation.dedup, operation.dedup_checked) = (None, false);
        operation.txn_id = Some(Ulid::nil());
        let effects = operation.check_hash_lookup();
        let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice() else {
            panic!("expected the head read, got {effects:?}")
        };
        assert_eq!(key_space, BLOB_HEAD_KEYSPACE);
        let head = CurrentVersionPointer::new(Ulid::from_parts(6, 6));
        operation.step(read(Some(head.to_bytes().unwrap())));
        let version = BlobVersion::materialized(
            [7u8; 32],
            BackendRef::Group(backend_id()),
            old.format.encoding(),
            SystemTime::UNIX_EPOCH,
            UserId::default(),
            None,
        );
        operation.step(read(Some(version.to_bytes().unwrap())));
        operation.step(read(Some(old.to_bytes().unwrap())));
        operation.step(read(Some(b"v1".to_vec())));
        operation.step(read(None));
        assert_eq!(operation.final_location.as_ref(), Some(&old));
        // The assembled file is released after the commit, at its exact backend version.
        let writes = operation.plain_records(true).unwrap();
        let release = writes
            .iter()
            .find(|(space, ..)| space == BLOB_CLEANUP_KEYSPACE)
            .map(|(_, _, value)| BlobCleanupWork::from_bytes(value).unwrap());
        let Some(BlobCleanupWork::ReleaseRenamed {
            location, version, ..
        }) = release
        else {
            panic!("expected the release of the assembled file, got {writes:?}")
        };
        assert_eq!((location, version.as_str()), (written, "v2"));
        // Without a backend version the assembled file stays the version's own.
        let (mut operation, _) = recorded(false, None);
        (operation.plain_version, operation.dedup_checked) = (None, false);
        operation.check_hash_lookup();
        assert_eq!(operation.state, CompleteUploadState::CheckHashLookup);
    }
}
