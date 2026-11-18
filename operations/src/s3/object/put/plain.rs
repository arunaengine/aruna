//! Writes a version to its path in the tenant's folders under a backend import rule.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::fenced::{Fenced, FencedWrite};
use super::*;
use aruna_core::calls::IoCall;
use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BACKEND_VERSION_KEYSPACE, IMPORT_INTENT_KEYSPACE,
    IMPORTED_OBJECT_KEYSPACE,
};
use aruna_core::structs::storage::backend_import::{
    ImportedObject, PlainPath, WriteIntent, import_key, intent_key, object_key, tagged_path,
    version_key,
};
use aruna_core::structs::storage::blob::ResolvedBackend;
use aruna_core::structs::storage::distribution::hint::mint_version;
use aruna_core::structs::storage::routing::PlainTarget;
use aruna_core::types::{Key, Value};

/// The path a write takes, the backend it resolved to and the backend version it got.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct PlainWrite {
    pub(super) target: PlainTarget,
    resolved: ResolvedBackend,
    pub(super) version: Option<String>,
    pub(super) etag: Option<String>,
    /// The file at the path may be replaced: the rule recorded it for this key.
    owned: bool,
}

/// Rows an import commits with its version; the rule must still exist in that transaction.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct ImportRows {
    import_id: Ulid,
    writes: Vec<(String, Key, Value)>,
    /// Values the transaction must still read, or the write fails with `HeadChanged`.
    guards: Vec<(String, Key, Option<Value>)>,
    /// Write intents the committed version makes obsolete.
    retired: Vec<Key>,
}

impl PutObjectOperation {
    /// A plain path only where routing sends the key to the rule's write backend, and only
    /// for raw bytes; an encrypting bucket keeps Aruna's own layout.
    pub(super) fn plain_target(&mut self, resolved: &ResolvedBackend) -> Option<PlainTarget> {
        let request = &self.config.request;
        let id = self
            .version_id
            .unwrap_or_else(|| mint_version(self.set_hint));
        let target = self
            .config
            .routing
            .plain_target(&request.bucket, &request.key, id)
            .filter(|target| target.backend == resolved.backend)?;
        if self.seal_plan.is_some() {
            warn!(
                import_id = %target.import_id,
                bucket = %request.bucket,
                "Encrypting bucket keeps Aruna's layout for an import folder"
            );
            return None;
        }
        self.version_id = Some(id);
        Some(target)
    }

    /// Nothing at a tenant path is ever deleted, so the write counts as adopted from here on.
    /// A same-name write first looks at the path; no lock is held.
    pub(super) fn start_plain(&mut self, target: PlainTarget, resolved: ResolvedBackend) -> Turn {
        self.adopted = true;
        let (same_name, path) = (target.same_name, target.path.clone());
        let owner = owner_call(&target);
        self.plain = Some(PlainWrite {
            // Only a record proves a mapped path the key's own.
            owned: owner.is_none() && !target.mapped,
            target,
            resolved,
            version: None,
            etag: None,
        });
        if !same_name {
            return self.write_plain(path, false);
        }
        match owner {
            Some(read) => self.call(PutObjectState::ReadPlainOwner, read),
            None => self.stat_plain(),
        }
    }

    fn stat_plain(&mut self) -> Turn {
        let plain = self.plain.as_ref();
        let plain = plain.ok_or(PutObjectError::InvalidOperationState)?;
        let stat = BlobEffect::StatPlain {
            backend: plain.target.backend.clone(),
            path: plain.target.path.clone(),
        };
        self.effect(PutObjectState::StatPlain, Effect::Blob(stat))
    }

    pub(super) fn plain_owner(&mut self, value: Option<Value>) -> Turn {
        let owned = path_owned(value.as_deref(), &self.config.request.key);
        let owned = owned.map_err(PutObjectError::BlobWriteFailed)?;
        let plain = self.plain.as_mut();
        plain.ok_or(PutObjectError::InvalidOperationState)?.owned = owned;
        self.stat_plain()
    }

    pub(super) fn plain_stat(&mut self, event: Event) -> Turn {
        let (Some(plain), Some(id)) = (self.plain.as_ref(), self.version_id) else {
            return Err(PutObjectError::InvalidOperationState);
        };
        match event {
            Event::Blob(BlobEvent::PlainStat { exists, version }) => {
                let (path, replaces) = (&plain.target.path, version.is_some() && plain.owned);
                let (path, if_not_exists) = same_name_path(path, id, exists, replaces);
                self.write_plain(path, if_not_exists)
            }
            Event::Blob(BlobEvent::Error(error)) => Err(PutObjectError::BlobWriteFailed(error)),
            _ => Err(PutObjectError::InvalidOperationState),
        }
    }

    fn write_plain(&mut self, path: String, if_not_exists: bool) -> Turn {
        let (Some(plain), Some(ulid)) = (self.plain.as_ref(), self.version_id) else {
            return Err(PutObjectError::InvalidOperationState);
        };
        let blob = self.config.request.body.take();
        let blob = blob.ok_or(PutObjectError::MissingBody)?;
        let intent = self.plain_intent();
        let import_id = plain.target.import_id;
        let keys = intent_keys(import_id, &path, ulid, if_not_exists);
        let write = Effect::Blob(BlobEffect::WritePlain {
            resolved: plain.resolved.clone(),
            plain: PlainPath {
                path,
                ulid,
                version_id: ulid,
                if_not_exists,
                exact: false,
            },
            created_by: self.config.user_id,
            blob,
        });
        let Some(intent) = intent else {
            return self.effect(PutObjectState::WriteBlob, write);
        };
        let rule = self.rule_fence();
        let rule = rule.ok_or(PutObjectError::InvalidOperationState)?;
        let rows = intent_rows(&keys, &intent)?;
        self.written_intent = keys;
        self.pending_write = Some(write.into());
        let fence = IoCall::child(FencedWrite::new(rule, rows));
        self.call(PutObjectState::WriteIntent, fence)
    }

    /// Whether this is a plain write to the folder of an import rule.
    pub(super) fn listed_write(&self) -> bool {
        self.plain.as_ref().is_some_and(|plain| plain.target.listed)
    }

    /// The identity of a listed plain write, without its written bytes.
    fn plain_intent(&self) -> Option<WriteIntent> {
        let plain = self.plain.as_ref().filter(|plain| plain.target.listed)?;
        Some(WriteIntent {
            key: self.config.request.key.clone(),
            version_id: self.version_id?,
            backend: plain.target.backend.clone(),
            created_by: self.config.user_id,
            written: None,
            mover: None,
        })
    }

    /// Records the hash of the written bytes in the intent of the path they reached. A lost
    /// record leaves the intent unproven, which an import reports instead of trusting.
    pub(super) fn write_evidence(&mut self) -> Option<Turn> {
        let mut intent = self
            .plain_intent()
            .filter(|_| !self.written_intent.is_empty())?;
        let (plain, written) = (self.plain.as_ref()?, self.get_written_location()?);
        intent.written = Some(written.get_blake3()?.try_into().ok()?);
        let key = intent_key(
            plain.target.import_id,
            &written.backend_path,
            intent.version_id,
        );
        let (rows, rule) = (intent_rows(&[key], &intent), self.rule_fence()?);
        let rows = match rows {
            Ok(rows) => rows,
            Err(error) => return Some(Err(error.into())),
        };
        let fence = IoCall::child(FencedWrite::new(rule, rows));
        Some(self.call(PutObjectState::WriteEvidence, fence))
    }

    /// The hash is stored with a read of the rule; a removed rule gets none, and the write
    /// goes on as one without its rule.
    pub(super) fn evidence_written(&mut self, result: IoResult) -> Turn {
        match result.child::<Fenced, StorageError>()? {
            Ok(Fenced::Written | Fenced::RuleGone) => self.start_publish(),
            Ok(Fenced::Failed(error)) => {
                Err(error.map_or(PutObjectError::InvalidOperationState, Into::into))
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Bytes move only once their identity is stored with a read of the rule and synced to
    /// disk; a removed rule stores nothing and refuses the write.
    pub(super) fn intent_written(&mut self, result: IoResult) -> Turn {
        match result.child::<Fenced, StorageError>()? {
            Ok(Fenced::Written) => self.call(PutObjectState::SyncIntent, IoCall::Sync),
            Ok(Fenced::RuleGone) => {
                self.written_intent.clear();
                Err(PutObjectError::ImportRemoved)
            }
            Ok(Fenced::Failed(error)) => {
                Err(error.map_or(PutObjectError::InvalidOperationState, Into::into))
            }
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn intent_synced(&mut self, result: IoResult) -> Turn {
        result.done()?;
        let write = self.pending_write.take();
        let write = write.ok_or(PutObjectError::InvalidOperationState)?;
        self.call(PutObjectState::WriteBlob, write)
    }

    /// A refused conditional write moved no byte, so its intent must not claim the path; the
    /// write fails as taken once the intents are gone for good.
    pub(super) fn drop_intent(&mut self) -> Turn {
        if self.written_intent.is_empty() {
            return Err(PutObjectError::BlobWriteFailed(BlobError::PathTaken));
        }
        let space = IMPORT_INTENT_KEYSPACE.to_string();
        let deletes = (self.written_intent.iter()).map(|key| (space.clone(), key.clone()));
        let call = IoCall::Delete(deletes.collect());
        self.call(PutObjectState::DropIntent, call)
    }

    /// Rows committed with the version only while the import rule `import_id` exists.
    pub fn with_import_rows(mut self, import_id: Ulid, writes: Vec<(String, Key, Value)>) -> Self {
        let guards = Vec::new();
        self.import_rows = Some(ImportRows {
            import_id,
            writes,
            guards,
            retired: Vec::new(),
        });
        self
    }

    /// Deletes the write intent `key` in the version transaction that registers its file.
    pub fn with_retired_intent(mut self, key: Key) -> Self {
        if let Some(rows) = self.import_rows.as_mut() {
            rows.retired.push(key);
        }
        self
    }

    /// The intents of this write and its import rows, gone once the version commits.
    fn retired_intents(&self) -> Vec<(String, Key)> {
        let rows = self
            .import_rows
            .iter()
            .flat_map(|rows| rows.retired.clone());
        let keys = rows.chain(self.written_intent.iter().cloned());
        keys.map(|key| (IMPORT_INTENT_KEYSPACE.to_string(), key))
            .collect()
    }

    /// The import rows are written; the obsolete intents go in the same transaction.
    pub(super) fn plain_written(&mut self) -> Turn {
        let deletes = self.retired_intents();
        if deletes.is_empty() {
            return self.finish_publish();
        }
        self.call(PutObjectState::RetireIntents, IoCall::Delete(deletes))
    }

    /// Refuses the version while the delete trail shows `version_id`, or the key without a
    /// head, removed on purpose. The trail is read in the version transaction.
    pub fn with_trail_check(mut self, version_id: Ulid) -> Self {
        self.trail_scan = Some(super::trail::TrailScan::new(version_id));
        self
    }

    /// Values the version transaction must still read before it writes the import rows.
    pub fn with_import_guards(mut self, guards: Vec<(String, Key, Option<Value>)>) -> Self {
        if let Some(rows) = self.import_rows.as_mut() {
            rows.guards = guards;
        }
        self
    }

    /// Values the version transaction reads with the rule: the import guards, and for a
    /// listed write the records other rules sharing the folder would keep of the written path.
    fn fence_guards(&self) -> Vec<(String, Key, Option<Value>)> {
        let mut guards = self
            .import_rows
            .as_ref()
            .map_or_else(Vec::new, |rows| rows.guards.clone());
        let plain = self.plain.as_ref().filter(|plain| plain.target.listed);
        if let (Some(plain), Some(written)) = (plain, self.get_written_location()) {
            let path = &written.backend_path;
            guards.extend(plain.target.shared.iter().map(|import_id| {
                let key = object_key(*import_id, path);
                (IMPORTED_OBJECT_KEYSPACE.to_string(), key, None)
            }));
        }
        guards
    }

    /// The rule whose existence the version transaction must read before its rows.
    fn rule_fence(&self) -> Option<Key> {
        let import_id = match (&self.import_rows, &self.plain) {
            (Some(rows), _) => rows.import_id,
            (None, Some(plain)) if plain.target.listed => plain.target.import_id,
            _ => return None,
        };
        Some(import_key(self.config.group_id, import_id))
    }

    pub(super) fn plain_records(&mut self) -> Option<Turn> {
        // Which other rules read the written path is read in this transaction.
        if self.listed_write() && self.rule_exists.is_none() && !self.readers.done() {
            let page = self.readers.page_call(self.config.group_id);
            return Some(self.call(PutObjectState::ReadReaders, page));
        }
        if self.rule_exists.is_none()
            && let Some(key) = self.rule_fence()
        {
            let rule = (BACKEND_IMPORT_KEYSPACE.to_string(), key);
            let guards = self.fence_guards();
            let guards = guards.into_iter().map(|(space, key, _)| (space, key));
            let reads = std::iter::once(rule).chain(guards).collect();
            return Some(self.call(PutObjectState::ReadImportRule, IoCall::Read(reads)));
        }
        if self.import_rows.is_some()
            && self.rule_exists == Some(true)
            && let Some(turn) = self.trail_page().or_else(|| self.rule_page())
        {
            return Some(turn);
        }
        let mut writes = Vec::new();
        // A deduplicated version reads the older location, so it needs no version row.
        let (output, written) = (self.get_output(), self.get_written_location());
        let fresh = output
            .zip(written)
            .is_some_and(|(output, written)| output.ulid == written.ulid);
        if let Some(rows) = self.import_rows.as_ref() {
            if self.rule_exists != Some(true) {
                return Some(Err(PutObjectError::ImportRemoved));
            }
            let rows = rows.writes.iter();
            writes.extend(
                rows.filter(|(space, ..)| fresh || space != BACKEND_VERSION_KEYSPACE)
                    .cloned(),
            );
        }
        if let Some(plain) = self.plain.as_ref() {
            let (Some(_), Some(written)) = (output, written) else {
                return None;
            };
            let version = plain.version.as_deref().filter(|_| fresh);
            let record = PlainRecord {
                target: &plain.target,
                written,
                version,
                etag: plain.etag.as_deref(),
                version_id: self.version_id?,
                key: &self.config.request.key,
            };
            match record.writes(self.rule_exists == Some(true)) {
                Ok(rows) => writes.extend(rows),
                Err(error) => return Some(Err(error.into())),
            }
            match self.dedup_release().transpose() {
                Ok(release) => writes.extend(release),
                Err(error) => return Some(Err(error.into())),
            }
        }
        if writes.is_empty() {
            // A write without records, as after its rule went, still retires its intents.
            let deletes = self.retired_intents();
            if self.plain.is_none() || deletes.is_empty() {
                return None;
            }
            return Some(self.call(PutObjectState::RetireIntents, IoCall::Delete(deletes)));
        }
        Some(self.call(PutObjectState::WritePlainRecords, IoCall::Write(writes)))
    }

    pub(super) fn readers_read(&mut self, page: (Vec<(Key, Value)>, Option<Key>)) -> Turn {
        let path = (self.get_written_location()).map(|written| written.backend_path.clone());
        let (Some(plain), Some(path)) = (self.plain.as_mut(), path) else {
            return Err(PutObjectError::InvalidOperationState);
        };
        let target = (plain.target.import_id, &plain.target.backend, path.as_str());
        let read = self.readers.read_page(page, target);
        match read.map_err(|_| PutObjectError::InvalidOperationState)? {
            Some(readers) => {
                plain.target.shared = readers;
                self.write_cleanup_row()
            }
            None => {
                let page = self.readers.page_call(self.config.group_id);
                Ok(Next::Call(page))
            }
        }
    }

    pub(super) fn import_rule_read(&mut self, values: Vec<Option<Value>>) -> Turn {
        let guards = self.fence_guards();
        let Some((rule, seen)) = values.split_first() else {
            return Err(PutObjectError::InvalidOperationState);
        };
        let expected = guards.iter().map(|(_, _, value)| value);
        if seen.len() != guards.len() || !seen.iter().eq(expected) {
            // Another rule recorded the written path, so the file there is its own.
            return Err(match self.import_rows.is_some() {
                true => PutObjectError::HeadChanged,
                false => PutObjectError::BlobWriteFailed(BlobError::PathTaken),
            });
        }
        self.rule_exists = Some(rule.is_some());
        self.write_cleanup_row()
    }
}

/// Where a same-name write goes and whether it must find the path free. A free path takes
/// the file; a backend with versions keeps the replaced bytes; else a tagged path is used.
pub(crate) fn same_name_path(
    path: &str,
    id: Ulid,
    exists: bool,
    versioned: bool,
) -> (String, bool) {
    match (exists, versioned) {
        (false, _) => (path.to_string(), true),
        (true, true) => (path.to_string(), false),
        (true, false) => (tagged_path(path, id), true),
    }
}

/// Reads the rule's record of the path, where the rule records its files; another rule's or
/// an unknown file there is never replaced.
pub(crate) fn owner_read(target: &PlainTarget) -> Option<Effect> {
    target.listed.then(|| {
        Effect::Storage(StorageEffect::Read {
            key_space: IMPORTED_OBJECT_KEYSPACE.to_string(),
            key: object_key(target.import_id, &target.path),
            txn_id: None,
        })
    })
}

/// Whether the record read by `owner_read` names `key`. A record without a key proves no
/// owner, since several keys can give the same path.
pub(crate) fn owns_path(event: Event, key: &str) -> Result<bool, BlobError> {
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => path_owned(value.as_deref(), key),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(BlobError::ReadError(error.to_string()))
        }
        _ => Err(BlobError::InvalidEffect),
    }
}

/// The call form of [`owner_read`].
pub(crate) fn owner_call(target: &PlainTarget) -> Option<IoCall> {
    let key = object_key(target.import_id, &target.path);
    target
        .listed
        .then(|| aruna_core::rows::read_row(IMPORTED_OBJECT_KEYSPACE, key))
}

/// Whether the record row `owner_call` read names `key`.
pub(crate) fn path_owned(value: Option<&[u8]>, key: &str) -> Result<bool, BlobError> {
    let Some(value) = value else {
        return Ok(false);
    };
    let row = ImportedObject::from_bytes(value)?;
    Ok(row.key.as_deref() == Some(key))
}

/// What a plain write commits with its version.
pub(crate) struct PlainRecord<'a> {
    pub target: &'a PlainTarget,
    pub written: &'a BackendLocation,
    pub version: Option<&'a str>,
    /// The backend's ETag of the written file, so its first listing needs no read.
    pub etag: Option<&'a str>,
    pub version_id: Ulid,
    pub key: &'a str,
}

impl PlainRecord<'_> {
    /// The backend version lets older versions read their own bytes after an overwrite; the
    /// import row, written only while the rule exists, keeps its importer from importing them.
    pub(crate) fn writes(
        &self,
        rule_exists: bool,
    ) -> Result<Vec<(String, Key, Value)>, ConversionError> {
        let mut writes: Vec<_> = self
            .version
            .map(|version| version_row(self.written, version))
            .into_iter()
            .collect();
        if self.target.listed && rule_exists {
            let row = ImportedObject::written(
                self.version_id,
                self.key.to_string(),
                self.written.blob_size,
                self.etag.map(str::to_string),
            );
            let row_key = object_key(self.target.import_id, &self.written.backend_path);
            writes.push((
                IMPORTED_OBJECT_KEYSPACE.to_string(),
                row_key,
                row.to_bytes()?.into(),
            ));
        }
        Ok(writes)
    }
}

/// Where the intent of a write to `path` goes. A backend without conditional writes moves a
/// write that must find its path free to its tagged path, so that path gets the intent too.
pub(crate) fn intent_keys(import_id: Ulid, path: &str, id: Ulid, if_not_exists: bool) -> Vec<Key> {
    let mut keys = vec![intent_key(import_id, path, id)];
    if if_not_exists && !path.ends_with(&format!("~{id}")) {
        keys.push(intent_key(import_id, &tagged_path(path, id), id));
    }
    keys
}

/// The intent rows of `keys`, for a write in a transaction.
pub(crate) fn intent_rows(
    keys: &[Key],
    intent: &WriteIntent,
) -> Result<Vec<(String, Key, Value)>, ConversionError> {
    let value: Value = intent.to_bytes()?.into();
    let space = IMPORT_INTENT_KEYSPACE.to_string();
    Ok(keys
        .iter()
        .map(|key| (space.clone(), key.clone(), value.clone()))
        .collect())
}

/// Deletes the intents of a write that moved no byte.
pub(crate) fn intent_deletes(keys: &[Key]) -> Effect {
    let space = IMPORT_INTENT_KEYSPACE.to_string();
    Effect::Storage(StorageEffect::BatchDelete {
        deletes: keys
            .iter()
            .map(|key| (space.clone(), key.clone()))
            .collect(),
        txn_id: None,
    })
}

/// Pins a plain object to the backend version it was written as.
pub(crate) fn version_row(written: &BackendLocation, version: &str) -> (String, Key, Value) {
    let value = version.as_bytes().to_vec().into();
    (
        BACKEND_VERSION_KEYSPACE.to_string(),
        version_key(written.ulid),
        value,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::object::put::test::{asked, event, read_spaces, values, written};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::keyspaces::DELETE_TRAIL_KEYSPACE;
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::storage::backend_import::{BackendImport, DEFAULT_PATTERN};
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::format::StoredFormat;
    use aruna_core::structs::storage::routing::{
        BackendCatalog, GroupRoutingInputs, RoutingSnapshot, RoutingTarget,
    };
    use std::collections::BTreeSet;
    use std::time::SystemTime;

    fn backend_id() -> Ulid {
        Ulid::from_bytes([5u8; 16])
    }

    fn rule(import_id: Ulid, bucket: &str, key_prefix: &str, pattern: &str) -> BackendImport {
        BackendImport {
            import_id,
            group_id: Ulid::from_parts(2, 2),
            backend_id: backend_id(),
            backend_prefix: "raw/".to_string(),
            bucket: bucket.to_string(),
            key_prefix: key_prefix.to_string(),
            schedule: None,
            created_by: UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: pattern.to_string(),
        }
    }

    fn operation(pattern: &str) -> PutObjectOperation {
        let rule = rule(Ulid::from_bytes([4u8; 16]), "bucket", "in/", pattern);
        let catalog = BackendCatalog::new("default").with_backend("default", None);
        let routing = RoutingSnapshot::new(Ulid::from_parts(2, 2), catalog).with_group_inputs(
            GroupRoutingInputs {
                default_target: Some(RoutingTarget::Backend(BackendRef::Group(backend_id()))),
                backend_ids: BTreeSet::from([backend_id()]),
                imports: vec![rule],
                ..Default::default()
            },
        );
        let realm_id = RealmId::from_bytes([1u8; 32]);
        PutObjectOperation::new(PutObjectConfig {
            user_id: UserId::local(Ulid::from_parts(1, 1), realm_id),
            group_id: routing.group_id,
            realm_id,
            node_id: iroh::SecretKey::from_bytes(&[65; 32]).public(),
            request: PutObjectInput {
                bucket: "bucket".to_string(),
                key: "in/x/one.csv".to_string(),
                content_length: Some(3),
                body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                    &b"abc"[..],
                ))),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing,
        })
    }

    fn row(value: Option<Vec<u8>>) -> IoResult {
        values(vec![value.map(Into::into)])
    }

    fn fenced(result: Result<Fenced, StorageError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    /// Runs the gate reads of a bucket without policies, fence or encryption.
    fn begin(operation: &mut PutObjectOperation) -> Turn {
        operation.first().unwrap();
        operation.next(row(None)).unwrap();
        operation.next(row(None)).unwrap();
        operation.next(row(None))
    }

    /// Answers the stored intent and its sync; the write waits for both.
    fn past_intent(operation: &mut PutObjectOperation) -> Turn {
        assert_eq!(operation.state, PutObjectState::WriteIntent);
        let turn = operation.next(fenced(Ok(Fenced::Written)));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Sync))), "{turn:?}");
        operation.next(IoResult::Done)
    }

    fn written_at(path: String, ulid: Ulid) -> BackendLocation {
        BackendLocation {
            backend: BackendRef::Group(backend_id()),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: path,
            ulid,
            format: StoredFormat::default(),
            created_by: UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 3,
            hashes: HashMap::from([(HASH_BLAKE3.to_string(), vec![7u8; 32])]),
        }
    }

    fn plain_write(turn: &Turn) -> &PlainPath {
        match asked(turn) {
            Effect::Blob(BlobEffect::WritePlain { plain, .. }) => plain,
            other => panic!("expected a plain write, got {other:?}"),
        }
    }

    #[test]
    fn mapped_needs_path() {
        let mapped = |key: &str| {
            let mut operation = operation("{dir}{name}");
            operation.config.routing.imports[0].key_prefix = String::new();
            operation.config.request.key = key.to_string();
            let turn = begin(&mut operation);
            (operation, turn)
        };
        let (mut operation, _) = mapped("data/a.csv");
        // The same-name write first looks at the exact key in the provider bucket.
        let turn = operation.next(row(None));
        let Effect::Blob(BlobEffect::StatPlain { path, .. }) = asked(&turn) else {
            panic!("expected a plain path, got {turn:?}")
        };
        assert_eq!(path, "raw/data/a.csv");
        // A folder key is written at its exact key, without looking first.
        let (mut operation, _) = mapped("reports/");
        let turn = past_intent(&mut operation);
        assert_eq!(plain_write(&turn).path, "raw/reports/");
        // Keys without a plain path are refused before any byte is written.
        for key in ["reports//a.csv", "reports//"] {
            let (_, turn) = mapped(key);
            assert_eq!(turn.err(), Some(PutObjectError::Unmapped), "{key}");
        }
    }

    /// The version transaction's records, after the scan of the rules that read the path
    /// finds `readers`.
    fn records(operation: &mut PutObjectOperation, readers: &[BackendImport]) -> Option<Turn> {
        let turn = operation.plain_records()?;
        let Ok(Next::Call(IoCall::Page { space, .. })) = &turn else {
            return Some(turn);
        };
        if space != BACKEND_IMPORT_KEYSPACE {
            return Some(turn);
        }
        let rows = readers
            .iter()
            .map(|rule| (rule.key(), rule.to_bytes().unwrap().into()))
            .collect();
        Some(operation.next(IoResult::Page { rows, next: None }))
    }

    /// Another rule that reads the folder, created after the write was routed.
    fn reader() -> BackendImport {
        rule(Ulid::from_parts(9, 9), "other", "", DEFAULT_PATTERN)
    }

    #[test]
    fn keeps_shared_path() {
        // Another rule of the folder recorded the written path: the version is refused.
        let mut put = operation(DEFAULT_PATTERN);
        begin(&mut put).unwrap();
        let id = put.version_id.unwrap();
        // The rule that reads the folder is found in the version transaction.
        let other = reader().import_id;
        let path = format!("raw/x/one.csv~{id}");
        put.written_location = Some(written_at(path.clone(), id));
        put.output = Some(written_at(path.clone(), id));
        let turn = records(&mut put, &[reader()]).unwrap();
        let Ok(Next::Call(IoCall::Read(reads))) = &turn else {
            panic!("expected the fence read, got {turn:?}")
        };
        assert_eq!(reads[1].1, object_key(other, &path));
        let turn = put.import_rule_read(vec![Some(vec![1].into()), Some(vec![1].into())]);
        assert_eq!(
            turn.err(),
            Some(PutObjectError::BlobWriteFailed(BlobError::PathTaken))
        );
    }

    #[test]
    fn orders_intent_acks() {
        // The write waits for the stored intent and then its sync, each answered once.
        let mut operation = operation(DEFAULT_PATTERN);
        begin(&mut operation).unwrap();
        let turn = operation.next(fenced(Ok(Fenced::Written)));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Sync))));
        assert_eq!(operation.state, PutObjectState::SyncIntent);
        let turn = operation.next(IoResult::Done);
        plain_write(&turn);
        assert_eq!(operation.state, PutObjectState::WriteBlob);
    }

    #[test]
    fn writes_unique_path() {
        let mut operation = operation(DEFAULT_PATTERN);
        let turn = begin(&mut operation);

        // The identity of the write is stored before any byte moves.
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let id = operation.version_id.expect("the path names the version");
        let path = format!("raw/x/one.csv~{id}");
        assert_eq!(
            operation.written_intent,
            [intent_key(Ulid::from_bytes([4u8; 16]), &path, id)],
            "a unique path gets one intent"
        );
        let turn = past_intent(&mut operation);
        let plain = plain_write(&turn);
        assert_eq!(plain.path, path);
        assert_eq!(plain.ulid, id);
        assert!(!plain.if_not_exists);

        // A failed write leaves the tenant path alone.
        let written = BlobEvent::PlainWritten {
            location: written_at(plain.path.clone(), id),
            version: None,
            etag: None,
        };
        operation.next(event(Event::Blob(written))).unwrap();
        operation.state = PutObjectState::Error;
        assert!(operation.abort().is_empty());
    }

    #[test]
    fn guards_import_rows() {
        let guarded = |found: Option<Vec<u8>>| {
            let mut operation = operation(DEFAULT_PATTERN)
                .with_import_rows(Ulid::from_bytes([4u8; 16]), Vec::new())
                .with_import_guards(vec![(
                    IMPORTED_OBJECT_KEYSPACE.to_string(),
                    Key::from(vec![1u8]),
                    Some(Value::from(vec![2u8])),
                )]);
            let turn = records(&mut operation, &[]).expect("the guarded rule read");
            assert_eq!(read_spaces(&turn).len(), 2);
            let turn = operation
                .import_rule_read(vec![Some(Value::from(vec![0u8])), found.map(Value::from)]);
            (operation, turn)
        };
        // A row that changed since the import read it refuses the write.
        let (_, turn) = guarded(Some(vec![3u8]));
        assert_eq!(turn.err(), Some(PutObjectError::HeadChanged));
        let (_, turn) = guarded(None);
        assert_eq!(turn.err(), Some(PutObjectError::HeadChanged));
        let (kept, turn) = guarded(Some(vec![2u8]));
        assert_eq!(kept.rule_exists, Some(true));
        assert_ne!(turn.err(), Some(PutObjectError::HeadChanged));
    }

    #[test]
    fn checks_delete_trail() {
        use aruna_core::structs::storage::delete_audit::{BlobAuditKind, BlobAuditRecord};
        let id = Ulid::from_parts(1_000, 1);
        let checked = |kind: BlobAuditKind, head: bool| {
            let mut operation = operation(DEFAULT_PATTERN)
                .with_import_rows(Ulid::from_bytes([4u8; 16]), Vec::new())
                .with_trail_check(id);
            operation.rule_exists = Some(true);
            if head {
                operation.existing_pointer = Some(CurrentVersionPointer::new(Ulid::nil()));
            }
            // The trail is read in the version transaction, so a later delete conflicts. The
            // bucket's creation row comes first and names the incarnation.
            let turn = records(&mut operation, &[]).expect("the trail read");
            assert_eq!(
                read_spaces(&turn),
                [aruna_core::keyspaces::BUCKET_CREATION_KEYSPACE]
            );
            let turn = operation.next(row(None));
            let Ok(Next::Call(IoCall::Page { space, .. })) = turn else {
                panic!("expected one trail page, got {turn:?}")
            };
            assert_eq!(space, DELETE_TRAIL_KEYSPACE);
            let record = BlobAuditRecord {
                realm_id: RealmId::from_bytes([1; 32]),
                group_id: operation.config.group_id,
                node_id: iroh::SecretKey::from_bytes(&[65; 32]).public(),
                user_id: Default::default(),
                kind,
                bucket: operation.config.request.bucket.clone(),
                key: operation.config.request.key.clone(),
                version_id: Some(Ulid::nil()),
                occurred_at_ms: 2_000,
            };
            let rows = vec![(
                Key::from(vec![1u8]),
                Value::from(record.to_bytes().unwrap()),
            )];
            let turn = operation.next(IoResult::Page { rows, next: None });
            // The purges of the group are the second range, read in the same transaction.
            let Ok(Next::Call(IoCall::Page { prefix, .. })) = turn else {
                panic!("expected the purge range, got {turn:?}")
            };
            let purges = aruna_core::structs::storage::delete_audit::purge_prefix;
            let group = operation.config.group_id;
            assert_eq!(prefix, Key::from(purges(group, "bucket", 0)));
            operation.next(IoResult::Page {
                rows: Vec::new(),
                next: None,
            })
        };
        let deleted = |turn: &Turn| matches!(turn, Err(PutObjectError::HistoryDeleted));
        // A key whose head was deleted on purpose gets no version back.
        assert!(deleted(&checked(BlobAuditKind::DeleteMarker, false)));
        // Below a current head older history may join.
        assert!(!deleted(&checked(BlobAuditKind::DeleteVersion, true)));
    }

    #[test]
    fn records_own_write() {
        let records = |rule: bool, deduplicated: bool| {
            let mut operation = operation("{dir}{name}");
            begin(&mut operation).unwrap();
            let id = operation.version_id.unwrap();
            let location = written_at("raw/x/one.csv".to_string(), id);
            operation.written_location = Some(location.clone());
            operation.output = Some(match deduplicated {
                // An identical earlier upload to the same name keeps its own location.
                true => written_at("raw/x/one.csv".to_string(), Ulid::from_parts(3, 3)),
                false => location,
            });
            let plain = operation.plain.as_mut().unwrap();
            (plain.version, plain.etag) = (Some("v1".to_string()), Some("\"e1\"".to_string()));

            // The import row needs the rule, read in the version transaction.
            let turn = records(&mut operation, &[]).expect("the rule read");
            assert_eq!(read_spaces(&turn), [BACKEND_IMPORT_KEYSPACE]);
            let turn = operation.next(row(rule.then(|| vec![0u8])));
            (id, written(&turn).to_vec())
        };

        let (id, writes) = records(true, false);
        let [
            (version_space, version_row, version),
            (row_space, row_key, row),
        ] = writes.as_slice()
        else {
            panic!("expected the backend version and the import row")
        };
        assert_eq!(version_space, BACKEND_VERSION_KEYSPACE);
        assert_eq!(*version_row, version_key(id));
        assert_eq!(version.as_ref(), b"v1");
        assert_eq!(row_space, IMPORTED_OBJECT_KEYSPACE);
        assert_eq!(
            *row_key,
            object_key(Ulid::from_bytes([4u8; 16]), "raw/x/one.csv")
        );
        let row = ImportedObject::from_bytes(row).unwrap();
        assert_eq!(row.key.as_deref(), Some("in/x/one.csv"));
        assert_eq!(row.version_id, id);
        // The backend's ETag lets the first listing compare without reading the file.
        assert_eq!(row.etag.as_deref(), Some("\"e1\""));
        assert!(!row.unlisted());

        // Without the rule only the backend version is written; the version still commits.
        let (_, writes) = records(false, false);
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, BACKEND_VERSION_KEYSPACE);

        // A deduplicated version reads an older location; the written path still gets its row.
        let (_, writes) = records(true, true);
        let [(row_space, row_key, _)] = writes.as_slice() else {
            panic!("expected only the import row, got {writes:?}")
        };
        assert_eq!(row_space, IMPORTED_OBJECT_KEYSPACE);
        assert_eq!(
            *row_key,
            object_key(Ulid::from_bytes([4u8; 16]), "raw/x/one.csv")
        );
    }
}
