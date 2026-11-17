//! Composes a completed upload at its path in the tenant's folders under a backend import rule.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::object::put::fenced::{Fenced, FencedWrite};
use crate::s3::object::put::{
    PlainRecord, Trail, TrailScan, intent_keys, intent_rows, owner_call, path_owned,
    same_name_path, trail_ranges, version_row,
};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BUCKET_CREATION_KEYSPACE, DELETE_TRAIL_KEYSPACE,
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
    pub(super) fn plain_compose(&mut self) -> Option<Turn> {
        if let Some(location) = self.plain_upload().map(|upload| upload.location.clone()) {
            return Some(self.stat_in_place(location));
        }
        let upload = self.upload_record.as_ref()?;
        if self.compression != Compression::Off || upload.backend_upload.is_some() {
            return None;
        }
        let id = self
            .version_id
            .unwrap_or_else(|| mint_version(self.set_hint));
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
    fn read_owner(&mut self, stat_at: (BackendRef, String)) -> Turn {
        let at = self.plain.clone().map(|target| PlainTarget {
            path: stat_at.1.clone(),
            ..target
        });
        self.plain_stat_at = Some(stat_at);
        match at.as_ref().and_then(owner_call) {
            Some(read) => self.call(CompleteUploadState::ReadPlainOwner, read),
            None => {
                // The rules may have changed since the upload began, so nothing proves an owner.
                self.plain_owned = false;
                self.stat_plain()
            }
        }
    }

    pub(super) fn plain_owner(&mut self, value: Option<Value>) -> Turn {
        self.plain_owned = path_owned(value.as_deref(), &self.input.key)?;
        self.stat_plain()
    }

    fn stat_plain(&mut self) -> Turn {
        let stat_at = self.plain_stat_at.clone();
        let (backend, path) = stat_at.ok_or(CompleteUploadError::InvalidOperationState)?;
        let stat = BlobEffect::StatPlain { backend, path };
        self.effect(CompleteUploadState::StatPlain, Effect::Blob(stat))
    }

    /// The provider upload of an upload that opened at its plain path.
    pub(super) fn plain_upload(&self) -> Option<&BackendUpload> {
        let upload = self.upload_record.as_ref()?.backend_upload.as_ref()?;
        upload.location.is_external().then_some(upload)
    }

    /// The object completes where its parts streamed to; the version id is the one the path
    /// was chosen with, also after a pattern change. The provider completes in place, so the
    /// path is looked at again.
    fn stat_in_place(&mut self, location: BackendLocation) -> Turn {
        let id = location.ulid;
        let (bucket, key, path) = (&self.input.bucket, &self.input.key, &location.backend_path);
        self.plain = self
            .routing
            .as_ref()
            .and_then(|routing| routing.saved_target(bucket, key, &location.backend, path));
        self.version_id = Some(id);
        self.read_owner((location.backend, location.backend_path))
    }

    pub(super) fn plain_stat(&mut self, event: Event) -> Turn {
        let (exists, version) = match event {
            Event::Blob(BlobEvent::PlainStat { exists, version }) => (exists, version),
            Event::Blob(BlobEvent::Error(error)) => return Err(error.into()),
            _ => return Err(CompleteUploadError::InvalidOperationState),
        };
        // Only a backend with versions keeps a file that appeared meanwhile; else it conflicts.
        if self.plain_upload().is_some() {
            let replaces = exists && version.is_some() && self.plain_owned;
            let path = (self.plain_upload()).map(|upload| upload.location.backend_path.clone());
            return match (self.complete_provider(!replaces), path) {
                // A provider upload completes at its own path only.
                (Some(call), Some(path)) => self.intent_first(&path, false, call),
                _ => Err(CompleteUploadError::InvalidOperationState),
            };
        }
        let (Some(target), Some(id)) = (self.plain.as_ref(), self.version_id) else {
            return Err(CompleteUploadError::InvalidOperationState);
        };
        let (path, replaces) = (&target.path, version.is_some() && self.plain_owned);
        let (path, if_not_exists) = same_name_path(path, id, exists, replaces);
        self.compose_plain(path, if_not_exists)
    }

    /// Another writer took the free path first; the parts are still there, so the object
    /// takes its tagged path instead.
    pub(super) fn plain_taken(&mut self) -> Turn {
        // The refused compose moved no byte, so its intents must not claim the path.
        if !self.written_intent.is_empty() {
            let space = IMPORT_INTENT_KEYSPACE.to_string();
            let deletes = (self.written_intent.iter()).map(|key| (space.clone(), key.clone()));
            let call = IoCall::Delete(deletes.collect());
            return self.call(CompleteUploadState::DropIntent, call);
        }
        // Parts in a provider upload cannot move to another path; the client uploads again.
        if self.plain_upload().is_some() {
            return Err(BlobError::PathTaken.into());
        }
        let (Some(target), Some(id)) = (self.plain.as_ref(), self.version_id) else {
            return Err(CompleteUploadError::InvalidOperationState);
        };
        let tagged = tagged_path(&target.path, id);
        if (self.plain_path.as_ref()).is_some_and(|plain| plain.path == tagged) {
            return Err(BlobError::PathTaken.into());
        }
        self.compose_plain(tagged, true)
    }

    fn compose_plain(&mut self, path: String, if_not_exists: bool) -> Turn {
        let (Some(upload), Some(ulid)) = (self.upload_record.as_ref(), self.version_id) else {
            return Err(CompleteUploadError::InvalidOperationState);
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
        let path = plain.path.clone();
        let compose = BlobEffect::Compose {
            bucket: self.input.bucket.clone(),
            key: self.input.key.clone(),
            resolved,
            created_by: self.input.created_by,
            parts: (self.resolved_parts.iter())
                .map(|part| part.location.clone())
                .collect(),
            plain: Some(plain),
        };
        self.intent_first(&path, if_not_exists, Effect::Blob(compose).into())
    }

    /// A listed plain object stores its identity before `compose` moves any byte to `path`.
    fn intent_first(&mut self, path: &str, if_not_exists: bool, compose: IoCall) -> Turn {
        let (Some(target), Some(version_id)) = (self.plain.as_ref(), self.version_id) else {
            return self.compose(compose);
        };
        if !target.listed {
            return self.compose(compose);
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
        let rule = self.rule_key();
        let rule = rule.ok_or(CompleteUploadError::InvalidOperationState)?;
        let rows = intent_rows(&keys, &intent)?;
        self.written_intent = keys;
        self.pending_compose = Some(compose);
        let fence = IoCall::child(FencedWrite::new(rule, rows));
        self.call(CompleteUploadState::WriteIntent, fence)
    }

    /// The key of the import rule of a listed object.
    fn rule_key(&self) -> Option<Key> {
        let target = self.plain.as_ref().filter(|target| target.listed)?;
        let group_id = self.upload_record.as_ref()?.group_id;
        Some(import_key(group_id, target.import_id))
    }

    /// The refused compose goes on only once its intents are gone for good; a failed delete or
    /// sync keeps them and fails the upload with that error.
    pub(super) fn intent_dropped(&mut self, result: IoResult) -> Turn {
        result.done()?;
        self.call(CompleteUploadState::SyncDroppedIntent, IoCall::Sync)
    }

    pub(super) fn dropped_synced(&mut self, result: IoResult) -> Turn {
        result.done()?;
        self.written_intent.clear();
        self.plain_taken()
    }

    /// Records the hash of the composed bytes in the intent of the path they reached. A lost
    /// record leaves the intent unproven, which an import reports instead of trusting.
    pub(super) fn write_evidence(&mut self) -> Option<Turn> {
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
            Err(error) => return Some(Err(error.into())),
        };
        let fence = IoCall::child(FencedWrite::new(rule, rows));
        Some(self.call(CompleteUploadState::WriteEvidence, fence))
    }

    /// The hash is stored with a read of the rule; a removed rule gets none, and the upload
    /// goes on as one without its rule.
    pub(super) fn evidence_written(&mut self, result: IoResult) -> Turn {
        match result.child::<Fenced, StorageError>()? {
            Ok(Fenced::Written | Fenced::RuleGone) => self.start_finalize(),
            Ok(Fenced::Failed(error)) => {
                Err(error.map_or(CompleteUploadError::InvalidOperationState, Into::into))
            }
            Err(error) => Err(error.into()),
        }
    }

    /// The compose waits until its intent is stored with a read of the rule and synced to
    /// disk; a removed rule stores nothing and fails the upload.
    pub(super) fn intent_written(&mut self, result: IoResult) -> Turn {
        match result.child::<Fenced, StorageError>()? {
            Ok(Fenced::Written) => self.call(CompleteUploadState::SyncIntent, IoCall::Sync),
            Ok(Fenced::RuleGone) => {
                self.written_intent.clear();
                Err(CompleteUploadError::ImportRemoved)
            }
            Ok(Fenced::Failed(error)) => {
                Err(error.map_or(CompleteUploadError::InvalidOperationState, Into::into))
            }
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn intent_synced(&mut self, result: IoResult) -> Turn {
        result.done()?;
        let compose = self.pending_compose.take();
        let compose = compose.ok_or(CompleteUploadError::InvalidOperationState)?;
        self.compose(compose)
    }

    /// The committed version holds the identity its intent recorded, so the intent goes.
    pub(super) fn retire_intent(&mut self) -> Option<Turn> {
        if self.written_intent.is_empty() {
            return None;
        }
        let space = IMPORT_INTENT_KEYSPACE.to_string();
        let deletes = (self.written_intent.iter()).map(|key| (space.clone(), key.clone()));
        let call = IoCall::Delete(deletes.collect());
        Some(self.call(CompleteUploadState::RetireIntent, call))
    }

    /// Reads the version in the finalize transaction, so a registration meanwhile conflicts.
    pub(super) fn read_registered(&mut self, version_id: Ulid) -> Turn {
        let key = VersionKey::new(&self.input.bucket, &self.input.key, version_id).to_bytes()?;
        let read = read_row(BLOB_VERSIONS_KEYSPACE, key);
        self.call(CompleteUploadState::ReadRegistered, read)
    }

    /// The same bytes an import registered finish the upload without a second head move or
    /// charge; other content under the version id is a conflict.
    pub(super) fn registered_read(&mut self, value: Option<Value>) -> Turn {
        let Some(value) = value else {
            self.registered = false;
            return self.writer_trail();
        };
        let stored = BlobVersion::from_bytes(value.as_ref()).ok();
        let hash = stored.as_ref().and_then(|version| version.blob_hash());
        let written = (self.final_location.as_ref()).and_then(|location| location.get_blake3());
        if hash.is_none() || hash.map(|hash| &hash[..]) != written {
            return Err(CompleteUploadError::VersionExists);
        }
        self.registered = true;
        // The import's own location reads the file, so a second one would count it twice.
        let staged = self.final_location.as_ref().filter(|_| self.new_blob);
        let Some(key) = staged.and_then(|location| location.location_key().ok()) else {
            return self.delete_upload_records();
        };
        self.new_blob = false;
        let row = (BLOB_LOCATIONS_KEYSPACE.to_string(), key.to_bytes().into());
        self.call(
            CompleteUploadState::DropStagedLocation,
            IoCall::Delete(vec![row]),
        )
    }

    /// An upload whose version an import registered and a user then deleted or purged must not
    /// publish it again, so its unregistered version reads the trail before the head moves.
    fn writer_trail(&mut self) -> Turn {
        let (Some(version_id), Some(record)) = (self.version_id, self.upload_record.as_ref())
        else {
            return Err(CompleteUploadError::CompleteUploadFailed);
        };
        let group_id = record.group_id;
        let scan = self
            .trail_scan
            .get_or_insert_with(|| TrailScan::new(version_id));
        scan.writer = true;
        let Some(incarnation) = scan.incarnation else {
            let bucket = self.input.bucket.as_bytes().to_vec();
            let read = read_row(BUCKET_CREATION_KEYSPACE, bucket);
            return self.call(CompleteUploadState::ReadTrail, read);
        };
        let bucket = (self.input.bucket.as_str(), incarnation);
        let ranges = trail_ranges(group_id, bucket, &self.input.key);
        let Some(prefix) = ranges.get(scan.range).cloned() else {
            let existing = self.existing_pointer.clone();
            return self.write_head(existing.as_ref(), version_id);
        };
        let page = IoCall::Page {
            space: DELETE_TRAIL_KEYSPACE.to_string(),
            after: scan.after(&prefix),
            prefix,
            limit: TRAIL_PAGE,
        };
        self.call(CompleteUploadState::ReadTrail, page)
    }

    pub(super) fn trail_read(&mut self, result: IoResult) -> Turn {
        let (bucket, key) = (&self.input.bucket, &self.input.key);
        let scan = self.trail_scan.as_mut();
        let scan = scan.ok_or(CompleteUploadError::InvalidOperationState)?;
        if scan.incarnation.is_none() {
            scan.set_creation(result.value()?.as_deref());
            return self.writer_trail();
        }
        let (values, next) = result.page()?;
        for (_, value) in values {
            let record = BlobAuditRecord::from_bytes(&value)?;
            scan.trail = scan.trail.add(&record, bucket, key, scan.version_id);
        }
        scan.range += usize::from(next.is_none());
        scan.after = next;
        if scan.trail == Trail::Removed {
            return Err(CompleteUploadError::HistoryDeleted);
        }
        self.writer_trail()
    }

    /// Reads the rule of a listed plain object in the finalize transaction, so its import
    /// row is written only while the rule exists.
    pub(super) fn plain_fence(&mut self) -> Option<Turn> {
        let target = self.plain.as_ref().filter(|target| target.listed)?;
        let group_id = self.upload_record.as_ref()?.group_id;
        // Which other rules read the composed path is read in this transaction first.
        if !self.readers.done() {
            let page = self.readers.page_call(group_id);
            return Some(self.call(CompleteUploadState::ReadReaders, page));
        }
        let rule = (
            BACKEND_IMPORT_KEYSPACE.to_string(),
            import_key(group_id, target.import_id),
        );
        let reads = std::iter::once(rule).chain(self.shared_rows()).collect();
        Some(self.call(CompleteUploadState::ReadImportRule, IoCall::Read(reads)))
    }

    pub(super) fn readers_read(&mut self, result: IoResult) -> Turn {
        let path = (self.composed_location.as_ref()).map(|written| written.backend_path.clone());
        let group_id = self.upload_record.as_ref().map(|record| record.group_id);
        let (Some(target), Some(path), Some(group_id)) = (self.plain.as_mut(), path, group_id)
        else {
            return Err(CompleteUploadError::InvalidOperationState);
        };
        let reading = (target.import_id, &target.backend, path.as_str());
        let read = self.readers.read_page(result.page()?, reading);
        match read.map_err(|_| CompleteUploadError::InvalidOperationState)? {
            Some(readers) => {
                target.shared = readers;
                let fence = self.plain_fence();
                fence.unwrap_or(Err(CompleteUploadError::InvalidOperationState))
            }
            None => {
                let page = self.readers.page_call(group_id);
                Ok(Next::Call(page))
            }
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
    pub(super) fn import_rule_read(&mut self, values: Vec<Option<Value>>) -> Turn {
        let Some((rule, others)) = values.split_first() else {
            return Err(CompleteUploadError::InvalidOperationState);
        };
        if others.len() != self.shared_rows().len() {
            return Err(CompleteUploadError::InvalidOperationState);
        }
        if others.iter().any(Option::is_some) {
            return Err(BlobError::PathTaken.into());
        }
        self.write_cleanup_records(rule.is_some())
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
