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
