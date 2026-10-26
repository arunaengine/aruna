//! Writes a version to its path in the tenant's folders under a backend import rule.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::fenced::{Fenced, FencedWrite};
use super::*;
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
#[derive(Debug, PartialEq)]
pub(super) struct PlainWrite {
    pub(super) target: PlainTarget,
    resolved: ResolvedBackend,
    pub(super) version: Option<String>,
    pub(super) etag: Option<String>,
    /// The file at the path may be replaced: the rule recorded it for this key.
    owned: bool,
}

/// Rows an import commits with its version; the rule must still exist in that transaction.
#[derive(Debug, PartialEq)]
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
    pub(super) fn start_plain(
        &mut self,
        target: PlainTarget,
        resolved: ResolvedBackend,
    ) -> Effects {
        self.adopted = true;
        let (same_name, path) = (target.same_name, target.path.clone());
        let owner = owner_read(&target);
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
            Some(read) => {
                self.state = PutObjectState::ReadPlainOwner;
                smallvec![read]
            }
            None => self.stat_plain(),
        }
    }

    fn stat_plain(&mut self) -> Effects {
        let Some(plain) = self.plain.as_ref() else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        let stat = BlobEffect::StatPlain {
            backend: plain.target.backend.clone(),
            path: plain.target.path.clone(),
        };
        self.state = PutObjectState::StatPlain;
        smallvec![Effect::Blob(stat)]
    }

    pub(super) fn plain_owner(&mut self, event: Event) -> Effects {
        let owned = owns_path(event, &self.config.request.key);
        match (owned, self.plain.as_mut()) {
            (Ok(owned), Some(plain)) => {
                plain.owned = owned;
                self.stat_plain()
            }
            (Err(error), _) => self.emit_error(PutObjectError::BlobWriteFailed(error)),
            (_, None) => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    pub(super) fn plain_stat(&mut self, event: Event) -> Effects {
        let (Some(plain), Some(id)) = (self.plain.as_ref(), self.version_id) else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        match event {
            Event::Blob(BlobEvent::PlainStat { exists, version }) => {
                let (path, replaces) = (&plain.target.path, version.is_some() && plain.owned);
                let (path, if_not_exists) = same_name_path(path, id, exists, replaces);
                self.write_plain(path, if_not_exists)
            }
            Event::Blob(BlobEvent::Error(error)) => {
                self.emit_error(PutObjectError::BlobWriteFailed(error))
            }
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    fn write_plain(&mut self, path: String, if_not_exists: bool) -> Effects {
        let (Some(plain), Some(ulid)) = (self.plain.as_ref(), self.version_id) else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        let Some(blob) = self.config.request.body.take() else {
            return self.emit_error(PutObjectError::MissingBody);
        };
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
        self.state = PutObjectState::WriteBlob;
        let Some(intent) = intent else {
            return smallvec![write];
        };
        let (rows, Some(rule)) = (intent_rows(&keys, &intent), self.rule_fence()) else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        match rows {
            Ok(rows) => {
                self.written_intent = keys;
                self.pending_write = Some(smallvec![write]);
                self.state = PutObjectState::WriteIntent;
                let fence = self.fence.insert(FencedWrite::new(rule, rows));
                fence.start()
            }
            Err(error) => self.emit_error(error.into()),
        }
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
    pub(super) fn write_evidence(&mut self) -> Option<Effects> {
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
            Err(error) => return Some(self.emit_error(error.into())),
        };
        self.state = PutObjectState::WriteEvidence;
        Some(self.fence.insert(FencedWrite::new(rule, rows)).start())
    }

    /// The hash is stored with a read of the rule; a removed rule gets none, and the write
    /// goes on as one without its rule.
    pub(super) fn evidence_written(&mut self, event: Event) -> Effects {
        let Some(fence) = self.fence.as_mut() else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        let effects = fence.step(event);
        match fence.outcome().cloned() {
            None => effects,
            Some(Fenced::Written | Fenced::RuleGone) => self.start_transaction(),
            Some(Fenced::Failed(Some(error))) => {
                self.cleanup_failed_write(PutObjectError::StorageError(error))
            }
            Some(Fenced::Failed(None)) => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    /// Bytes move only once their identity is stored with a read of the rule and synced to
    /// disk; a removed rule stores nothing and refuses the write.
    pub(super) fn intent_written(&mut self, event: Event) -> Effects {
        if self.state == PutObjectState::SyncIntent {
            return match (event, self.pending_write.take()) {
                (Event::Storage(StorageEvent::SyncAllFinished), Some(write)) => {
                    self.state = PutObjectState::WriteBlob;
                    write
                }
                (Event::Storage(StorageEvent::Error { error }), _) => self.emit_error(error.into()),
                _ => self.emit_error(PutObjectError::InvalidOperationState),
            };
        }
        let Some(fence) = self.fence.as_mut() else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        let effects = fence.step(event);
        match fence.outcome().cloned() {
            None => effects,
            Some(Fenced::Written) => {
                self.state = PutObjectState::SyncIntent;
                smallvec![Effect::Storage(StorageEffect::SyncAll)]
            }
            Some(Fenced::RuleGone) => {
                self.written_intent.clear();
                self.emit_error(PutObjectError::ImportRemoved)
            }
            Some(Fenced::Failed(Some(error))) => self.emit_error(error.into()),
            Some(Fenced::Failed(None)) => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    /// A refused conditional write moved no byte, so its intent must not claim the path.
    pub(super) fn drop_intent(&mut self) -> Effects {
        let taken = PutObjectError::BlobWriteFailed(BlobError::PathTaken);
        if self.written_intent.is_empty() {
            return self.cleanup_failed_write(taken);
        }
        self.state = PutObjectState::DropIntent;
        smallvec![intent_deletes(&self.written_intent)]
    }

    /// The write fails as taken only once its intents are gone for good; a failed delete or
    /// sync keeps them and fails the write with that error instead.
    pub(super) fn intent_dropped(&mut self, event: Event) -> Effects {
        let syncing = self.state == PutObjectState::SyncDroppedIntent;
        match event {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) if !syncing => {
                self.state = PutObjectState::SyncDroppedIntent;
                smallvec![Effect::Storage(StorageEffect::SyncAll)]
            }
            Event::Storage(StorageEvent::SyncAllFinished) if syncing => {
                self.written_intent.clear();
                let taken = PutObjectError::BlobWriteFailed(BlobError::PathTaken);
                self.cleanup_failed_write(taken)
            }
            Event::Storage(StorageEvent::Error { error }) => {
                self.cleanup_failed_write(PutObjectError::StorageError(error))
            }
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
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
    pub(super) fn plain_written(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(PutObjectError::NoTransactionFound);
        };
        let retiring = self.state == PutObjectState::RetireIntents;
        let deletes = match event {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) if !retiring => {
                self.retired_intents()
            }
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) if retiring => Vec::new(),
            Event::Storage(StorageEvent::Error { error }) => return self.emit_error(error.into()),
            _ => return self.emit_error(PutObjectError::InvalidOperationState),
        };
        if !deletes.is_empty() {
            self.state = PutObjectState::RetireIntents;
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes,
                txn_id: Some(txn_id),
            })];
        }
        self.state = PutObjectState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
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

    pub(super) fn plain_records(&mut self, txn_id: Ulid) -> Option<Effects> {
        // Which other rules read the written path is read in this transaction.
        if self.listed_write() && self.rule_exists.is_none() && !self.readers.done() {
            self.state = PutObjectState::ReadReaders;
            let page = self.readers.page(self.config.group_id, Some(txn_id));
            return Some(smallvec![page]);
        }
        if self.rule_exists.is_none()
            && let Some(key) = self.rule_fence()
        {
            self.state = PutObjectState::ReadImportRule;
            let guards = self.fence_guards();
            if !guards.is_empty() {
                let rule = (BACKEND_IMPORT_KEYSPACE.to_string(), key);
                let reads = guards
                    .iter()
                    .map(|(space, key, _)| (space.clone(), key.clone()));
                return Some(smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: std::iter::once(rule).chain(reads).collect(),
                    txn_id: Some(txn_id),
                })]);
            }
            return Some(smallvec![Effect::Storage(StorageEffect::Read {
                key_space: BACKEND_IMPORT_KEYSPACE.to_string(),
                key,
                txn_id: Some(txn_id),
            })]);
        }
        if self.import_rows.is_some()
            && self.rule_exists == Some(true)
            && let Some(effects) = self.trail_page(txn_id).or_else(|| self.rule_page(txn_id))
        {
            return Some(effects);
        }
        let mut writes = Vec::new();
        // A deduplicated version reads the older location, so it needs no version row.
        let (output, written) = (self.get_output(), self.get_written_location());
        let fresh = output
            .zip(written)
            .is_some_and(|(output, written)| output.ulid == written.ulid);
        if let Some(rows) = self.import_rows.as_ref() {
            if self.rule_exists != Some(true) {
                return Some(self.emit_error(PutObjectError::ImportRemoved));
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
                Err(error) => return Some(self.emit_error(error.into())),
            }
            match self.dedup_release().transpose() {
                Ok(release) => writes.extend(release),
                Err(error) => return Some(self.emit_error(error.into())),
            }
        }
        if writes.is_empty() {
            // A write without records, as after its rule went, still retires its intents.
            let deletes = self.retired_intents();
            if self.plain.is_none() || deletes.is_empty() {
                return None;
            }
            self.state = PutObjectState::RetireIntents;
            return Some(smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes,
                txn_id: Some(txn_id),
            })]);
        }
        self.state = PutObjectState::WritePlainRecords;
        Some(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    pub(super) fn readers_read(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(PutObjectError::NoTransactionFound);
        };
        let path = self
            .get_written_location()
            .map(|written| written.backend_path.clone());
        let (Some(plain), Some(path)) = (self.plain.as_mut(), path) else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        let target = (plain.target.import_id, &plain.target.backend, path.as_str());
        match self.readers.read(event, target) {
            Ok(Some(readers)) => {
                plain.target.shared = readers;
                self.write_cleanup_row(txn_id)
            }
            Ok(None) => smallvec![self.readers.page(self.config.group_id, Some(txn_id))],
            Err(_) => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    pub(super) fn import_rule_read(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(PutObjectError::NoTransactionFound);
        };
        // Only the answer to the read that was sent counts.
        let guards = self.fence_guards();
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) if guards.is_empty() => {
                self.rule_exists = Some(value.is_some());
                self.write_cleanup_row(txn_id)
            }
            Event::Storage(StorageEvent::BatchReadResult { values }) if !guards.is_empty() => {
                let Some(((_, rule), seen)) = values.split_first() else {
                    return self.emit_error(PutObjectError::InvalidOperationState);
                };
                let expected = guards.iter().map(|(_, _, value)| value);
                if seen.len() != guards.len() || !seen.iter().map(|(_, v)| v).eq(expected) {
                    // Another rule recorded the written path, so the file there is its own.
                    let error = match self.import_rows.is_some() {
                        true => PutObjectError::HeadChanged,
                        false => PutObjectError::BlobWriteFailed(BlobError::PathTaken),
                    };
                    return self.emit_error(error);
                }
                self.rule_exists = Some(rule.is_some());
                self.write_cleanup_row(txn_id)
            }
            Event::Storage(StorageEvent::Error { error }) => self.emit_error(error.into()),
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
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
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(false),
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            let row = ImportedObject::from_bytes(&value)?;
            Ok(row.key.as_deref() == Some(key))
        }
        Event::Storage(StorageEvent::Error { error }) => {
            Err(BlobError::ReadError(error.to_string()))
        }
        _ => Err(BlobError::InvalidEffect),
    }
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
    use aruna_core::keyspaces::BLOB_LOCATIONS_KEYSPACE;
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::storage::backend_import::{BackendImport, DEFAULT_PATTERN};
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::format::{StoredFormat, StoredLayout};
    use aruna_core::structs::storage::routing::{
        BackendCatalog, GroupRoutingInputs, RoutingSnapshot, RoutingTarget,
    };
    use std::collections::BTreeSet;
    use std::time::SystemTime;

    fn backend_id() -> Ulid {
        Ulid::from_bytes([5u8; 16])
    }

    fn operation(pattern: &str) -> PutObjectOperation {
        let rule = BackendImport {
            import_id: Ulid::from_bytes([4u8; 16]),
            group_id: Ulid::from_parts(2, 2),
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

    fn read(value: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: value.map(Into::into),
        })
    }

    /// Runs the gate reads of a bucket without policies, fence or encryption.
    /// Starts the write up to the intent rows its transaction writes after reading the rule.
    fn begin(operation: &mut PutObjectOperation) -> Effects {
        operation.start();
        operation.step(read(None));
        operation.step(read(None));
        let effects = operation.step(read(None));
        past_fence(operation, effects)
    }

    /// Answers the transaction and rule read of a fenced intent write, up to its rows.
    fn past_fence(operation: &mut PutObjectOperation, effects: Effects) -> Effects {
        let [Effect::Storage(StorageEffect::StartTransaction { .. })] = effects.as_slice() else {
            return effects;
        };
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_parts(77, 7),
        }));
        let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice() else {
            panic!("expected the rule read, got {effects:?}")
        };
        assert_eq!(key_space, BACKEND_IMPORT_KEYSPACE);
        operation.step(read(Some(vec![1])))
    }

    /// Answers the rows and the commit of a fenced intent write.
    fn fence_done(operation: &mut PutObjectOperation) -> Effects {
        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: Ulid::from_parts(77, 7),
        }))
    }

    fn written(path: String, ulid: Ulid) -> BackendLocation {
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
            hashes: HashMap::from([(
                aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
                vec![7u8; 32],
            )]),
        }
    }

    #[test]
    fn mapped_needs_path() {
        let mapped = |key: &str| {
            let mut operation = operation("{dir}{name}");
            let rule = &mut operation.config.routing.imports[0];
            rule.key_prefix = String::new();
            operation.config.request.key = key.to_string();
            let effects = begin(&mut operation);
            (operation, effects)
        };
        let (mut operation, _) = mapped("data/a.csv");
        // The same-name write first looks at the exact key in the provider bucket.
        let effects = operation.step(read(None));
        let [Effect::Blob(BlobEffect::StatPlain { path, .. })] = effects.as_slice() else {
            panic!("expected a plain path, got {effects:?}")
        };
        assert_eq!(path, "raw/data/a.csv");
        // A folder key is written at its exact key, without looking first.
        let (mut operation, _) = mapped("reports/");
        let effects = past_intent(&mut operation);
        let [Effect::Blob(BlobEffect::WritePlain { plain, .. })] = effects.as_slice() else {
            panic!("expected a folder write, got {effects:?}")
        };
        assert_eq!(plain.path, "raw/reports/");
        // Keys without a plain path are refused before any byte is written.
        for key in ["reports//a.csv", "reports//"] {
            let (operation, effects) = mapped(key);
            assert!(effects.is_empty(), "{key}: {effects:?}");
            assert!(
                matches!(operation.finalize(), Err(PutObjectError::Unmapped)),
                "{key}"
            );
        }
    }

    /// Answers the intent write and its sync; the write waits for both.
    fn past_intent(operation: &mut PutObjectOperation) -> Effects {
        if operation.fence.as_ref().is_some_and(FencedWrite::starting) {
            let effects = smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })];
            past_fence(operation, effects);
        }
        let effects = fence_done(operation);
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::SyncAll)]
            ),
            "expected the intent sync, got {effects:?}"
        );
        operation.step(Event::Storage(StorageEvent::SyncAllFinished))
    }

    /// The version transaction's records, after the scan of the rules that read the path
    /// finds `readers`.
    fn records(operation: &mut PutObjectOperation, readers: &[BackendImport]) -> Option<Effects> {
        let effects = operation.plain_records(Ulid::nil())?;
        let [Effect::Storage(StorageEffect::Iter { key_space, .. })] = effects.as_slice() else {
            return Some(effects);
        };
        if key_space != BACKEND_IMPORT_KEYSPACE {
            return Some(effects);
        }
        operation.txn_id = Some(Ulid::nil());
        let values = readers
            .iter()
            .map(|rule| (rule.key(), rule.to_bytes().unwrap().into()))
            .collect();
        Some(operation.step(Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        })))
    }

    /// Another rule that reads the folder, created after the write was routed.
    fn reader() -> BackendImport {
        BackendImport {
            import_id: Ulid::from_parts(9, 9),
            group_id: Ulid::from_parts(2, 2),
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
        }
    }

    #[test]
    fn keeps_shared_path() {
        // Another rule of the folder recorded the written path: the version is refused.
        let mut put = operation(DEFAULT_PATTERN);
        begin(&mut put);
        let id = put.version_id.unwrap();
        // The rule that reads the folder is found in the version transaction.
        let other = reader().import_id;
        let path = format!("raw/x/one.csv~{id}");
        put.written_location = Some(written(path.clone(), id));
        put.output = Some(Ok(written(path.clone(), id)));
        let effects = records(&mut put, &[reader()]).unwrap();
        let [Effect::Storage(StorageEffect::BatchRead { reads, .. })] = effects.as_slice() else {
            panic!("expected the fence read, got {effects:?}")
        };
        assert_eq!(reads[1].1, object_key(other, &path));
        put.txn_id = Some(Ulid::nil());
        let rows = vec![
            (Key::from(Vec::new()), Some(Value::from(vec![1]))),
            (Key::from(Vec::new()), Some(Value::from(vec![1]))),
        ];
        put.import_rule_read(Event::Storage(StorageEvent::BatchReadResult {
            values: rows,
        }));
        let taken = PutObjectError::BlobWriteFailed(BlobError::PathTaken);
        assert!(matches!(&put.output, Some(Err(error)) if *error == taken));

        // A single read answer does not count for the batch that was sent.
        let mut early = operation(DEFAULT_PATTERN);
        begin(&mut early);
        if let Some(plain) = early.plain.as_mut() {
            plain.target.shared = vec![other];
        }
        early.written_location = Some(written(path, id));
        early.txn_id = Some(Ulid::nil());
        early.import_rule_read(read(None));
        let invalid = PutObjectError::InvalidOperationState;
        assert!(matches!(&early.output, Some(Err(error)) if *error == invalid));
    }

    #[test]
    fn orders_intent_acks() {
        // A sync answer before the intent write, or a second write answer, is refused.
        let aborted = || {
            Event::Storage(StorageEvent::TransactionAborted {
                txn_id: Ulid::from_parts(77, 7),
            })
        };
        let mut early = operation(DEFAULT_PATTERN);
        begin(&mut early);
        early.step(Event::Storage(StorageEvent::SyncAllFinished));
        early.step(aborted());
        let error = early.finalize().unwrap_err();
        assert_eq!(error, PutObjectError::InvalidOperationState);
        let mut twice = operation(DEFAULT_PATTERN);
        begin(&mut twice);
        let ack = || {
            Event::Storage(StorageEvent::BatchWriteResult {
                entries: Vec::new(),
            })
        };
        twice.step(ack());
        let effects = twice.step(ack());
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::Blob(_)))
        );
        twice.step(aborted());
        let error = twice.finalize().unwrap_err();
        assert_eq!(error, PutObjectError::InvalidOperationState);
    }

    #[test]
    fn writes_unique_path() {
        let mut operation = operation(DEFAULT_PATTERN);
        let effects = begin(&mut operation);

        // The identity of the write is stored before any byte moves.
        let id = operation.version_id.expect("the path names the version");
        let [Effect::Storage(StorageEffect::BatchWrite { writes, txn_id })] = effects.as_slice()
        else {
            panic!("expected the write intent, got {effects:?}")
        };
        let [(key_space, key, value)] = writes.as_slice() else {
            panic!("a unique path gets one intent, got {writes:?}")
        };
        let path = format!("raw/x/one.csv~{id}");
        assert_eq!(key_space, IMPORT_INTENT_KEYSPACE);
        assert_eq!(*key, intent_key(Ulid::from_bytes([4u8; 16]), &path, id));
        // The rows go in the transaction that read the rule.
        assert!(txn_id.is_some());
        let intent = WriteIntent::from_bytes(value).unwrap();
        assert_eq!(
            (intent.key.as_str(), intent.version_id),
            ("in/x/one.csv", id)
        );
        assert_eq!(intent.backend, BackendRef::Group(backend_id()));
        let effects = past_intent(&mut operation);
        let [Effect::Blob(BlobEffect::WritePlain { plain, .. })] = effects.as_slice() else {
            panic!("expected a plain write, got {effects:?}")
        };
        assert_eq!(plain.path, path);
        assert_eq!(plain.ulid, id);
        assert!(!plain.if_not_exists);

        // A failed write leaves the tenant path alone.
        operation.step(Event::Blob(BlobEvent::PlainWritten {
            location: written(plain.path.clone(), id),
            version: None,
            etag: None,
        }));
        let effects = operation.abort();
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::Blob(BlobEffect::Delete { .. }))),
            "{effects:?}"
        );
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
            operation.txn_id = Some(Ulid::nil());
            let Some(effects) = records(&mut operation, &[]) else {
                panic!("expected the guarded rule read")
            };
            let [Effect::Storage(StorageEffect::BatchRead { reads, .. })] = effects.as_slice()
            else {
                panic!("expected one batch read, got {effects:?}")
            };
            assert_eq!(reads.len(), 2);
            operation.step(Event::Storage(StorageEvent::BatchReadResult {
                values: vec![
                    (Key::from(vec![0u8]), Some(Value::from(vec![0u8]))),
                    (Key::from(vec![1u8]), found.map(Value::from)),
                ],
            }));
            operation
        };
        // A row that changed since the import read it refuses the write.
        let changed = guarded(Some(vec![3u8]));
        assert!(matches!(
            changed.output,
            Some(Err(PutObjectError::HeadChanged))
        ));
        assert!(matches!(
            guarded(None).output,
            Some(Err(PutObjectError::HeadChanged))
        ));
        let kept = guarded(Some(vec![2u8]));
        assert_eq!(kept.rule_exists, Some(true));
        assert!(!matches!(
            kept.output,
            Some(Err(PutObjectError::HeadChanged))
        ));
    }

    #[test]
    fn checks_delete_trail() {
        use aruna_core::keyspaces::DELETE_TRAIL_KEYSPACE;
        use aruna_core::structs::storage::delete_audit::{BlobAuditKind, BlobAuditRecord};
        let id = Ulid::from_parts(1_000, 1);
        let checked = |kind: BlobAuditKind, head: bool| {
            let mut operation = operation(DEFAULT_PATTERN)
                .with_import_rows(Ulid::from_bytes([4u8; 16]), Vec::new())
                .with_trail_check(id);
            operation.txn_id = Some(Ulid::nil());
            operation.rule_exists = Some(true);
            if head {
                operation.existing_pointer = Some(CurrentVersionPointer::new(Ulid::nil()));
            }
            // The trail is read in the version transaction, so a later delete conflicts.
            let Some(effects) = records(&mut operation, &[]) else {
                panic!("expected the trail read")
            };
            // The bucket's creation row comes first and names the incarnation.
            let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice()
            else {
                panic!("expected the creation read, got {effects:?}")
            };
            assert_eq!(key_space, aruna_core::keyspaces::BUCKET_CREATION_KEYSPACE);
            let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
                key: Vec::new().into(),
                value: None,
            }));
            let [
                Effect::Storage(StorageEffect::Iter {
                    key_space, txn_id, ..
                }),
            ] = effects.as_slice()
            else {
                panic!("expected one trail page, got {effects:?}")
            };
            assert_eq!(key_space, DELETE_TRAIL_KEYSPACE);
            assert_eq!(*txn_id, Some(Ulid::nil()));
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
            let value = Value::from(record.to_bytes().unwrap());
            let effects = operation.step(Event::Storage(StorageEvent::IterResult {
                values: vec![(Key::from(vec![1u8]), value)],
                next_start_after: None,
            }));
            // The purges of the group are the second range, read in the same transaction.
            let [Effect::Storage(StorageEffect::Iter { prefix, .. })] = effects.as_slice() else {
                panic!("expected the purge range, got {effects:?}")
            };
            let purges = aruna_core::structs::storage::delete_audit::purge_prefix;
            let group = operation.config.group_id;
            assert_eq!(*prefix, Some(purges(group, "bucket", 0).into()));
            operation.step(Event::Storage(StorageEvent::IterResult {
                values: Vec::new(),
                next_start_after: None,
            }));
            operation.output
        };
        let deleted = |output: &Option<Result<_, PutObjectError>>| {
            matches!(output, Some(Err(PutObjectError::HistoryDeleted)))
        };
        // A key whose head was deleted on purpose gets no version back.
        assert!(deleted(&checked(BlobAuditKind::DeleteMarker, false)));
        // Below a current head older history may join.
        assert!(!deleted(&checked(BlobAuditKind::DeleteVersion, true)));
    }

    #[test]
    fn records_own_write() {
        let records = |rule: bool, deduplicated: bool| {
            let mut operation = operation("{dir}{name}");
            begin(&mut operation);
            let id = operation.version_id.unwrap();
            let location = written("raw/x/one.csv".to_string(), id);
            operation.written_location = Some(location.clone());
            let output = match deduplicated {
                // An identical earlier upload to the same name keeps its own location.
                true => written("raw/x/one.csv".to_string(), Ulid::from_parts(3, 3)),
                false => location,
            };
            operation.output = Some(Ok(output));
            let plain = operation.plain.as_mut().unwrap();
            (plain.version, plain.etag) = (Some("v1".to_string()), Some("\"e1\"".to_string()));
            operation.txn_id = Some(Ulid::nil());

            // The import row needs the rule, read in the version transaction.
            let Some(effects) = records(&mut operation, &[]) else {
                panic!("expected the rule read")
            };
            assert!(matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::Read { key_space, .. })] if key_space == BACKEND_IMPORT_KEYSPACE
            ));
            let effects = operation.step(read(rule.then(|| vec![0u8])));
            let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice()
            else {
                panic!("expected one batch write, got {effects:?}")
            };
            (id, writes.clone())
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

    #[test]
    fn retires_own_intent() {
        let mut put = operation("{dir}{name}");
        begin(&mut put);
        let id = put.version_id.unwrap();
        let location = written("raw/x/one.csv".to_string(), id);
        put.written_location = Some(location.clone());
        put.output = Some(Ok(location));
        put.txn_id = Some(Ulid::nil());
        let keys = intent_keys(Ulid::from_bytes([4u8; 16]), "raw/x/one.csv", id, true);
        put.written_intent = keys.clone();
        records(&mut put, &[]);
        put.step(read(Some(vec![0u8])));

        // The committed version makes its intent obsolete in the same transaction.
        let effects = put.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        let [Effect::Storage(StorageEffect::BatchDelete { deletes, txn_id })] = effects.as_slice()
        else {
            panic!("expected the intent delete, got {effects:?}")
        };
        // Both paths the write could have taken lose their intent.
        let space = IMPORT_INTENT_KEYSPACE.to_string();
        let expected: Vec<_> = keys.into_iter().map(|key| (space.clone(), key)).collect();
        assert_eq!(*deletes, expected);
        assert_eq!(*txn_id, Some(Ulid::nil()));
        // A second write answer is not what the delete waits for.
        let mut stray = operation(DEFAULT_PATTERN);
        stray.state = PutObjectState::RetireIntents;
        stray.txn_id = Some(Ulid::nil());
        stray.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert!(matches!(
            stray.output,
            Some(Err(PutObjectError::InvalidOperationState))
        ));
        let effects = put.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { .. })]
        ));
    }

    #[test]
    fn fences_import_rows() {
        let import_id = Ulid::from_bytes([4u8; 16]);
        let row = (
            IMPORTED_OBJECT_KEYSPACE.to_string(),
            Key::from(vec![1u8]),
            Value::from(vec![2u8]),
        );
        let fenced = |exists: bool| {
            let mut operation =
                operation(DEFAULT_PATTERN).with_import_rows(import_id, vec![row.clone()]);
            operation.txn_id = Some(Ulid::nil());
            let effects = records(&mut operation, &[]).expect("a rule read");
            let [
                Effect::Storage(StorageEffect::Read {
                    key_space,
                    key,
                    txn_id,
                    ..
                }),
            ] = effects.as_slice()
            else {
                panic!("expected the rule read, got {effects:?}")
            };
            assert_eq!(key_space, BACKEND_IMPORT_KEYSPACE);
            assert_eq!(*key, import_key(Ulid::from_parts(2, 2), import_id));
            assert_eq!(*txn_id, Some(Ulid::nil()));
            let value = exists.then(|| vec![0u8]);
            let effects = operation.step(read(value));
            (operation, effects)
        };

        let (_, effects) = fenced(true);
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected the rows, got {effects:?}")
        };
        assert_eq!(writes, &vec![row.clone()]);

        // A removed rule refuses the version, so no row outlives it.
        let (operation, effects) = fenced(false);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert_eq!(
            operation.finalize().unwrap_err(),
            PutObjectError::ImportRemoved
        );
    }

    #[test]
    fn drops_deduplicated_version() {
        let version_row = (
            BACKEND_VERSION_KEYSPACE.to_string(),
            version_key(Ulid::from_parts(5, 5)),
            Value::from(b"v1".to_vec()),
        );
        let rows = |committed: Ulid| {
            let import_id = Ulid::from_bytes([4u8; 16]);
            let mut operation =
                operation(DEFAULT_PATTERN).with_import_rows(import_id, vec![version_row.clone()]);
            operation.written_location =
                Some(written("raw/a.csv".to_string(), Ulid::from_parts(5, 5)));
            operation.output = Some(Ok(written("raw/a.csv".to_string(), committed)));
            operation.txn_id = Some(Ulid::nil());
            records(&mut operation, &[]).expect("a rule read");
            operation.step(read(Some(vec![0u8])))
        };

        let effects = rows(Ulid::from_parts(5, 5));
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected the version row, got {effects:?}")
        };
        assert_eq!(writes, &vec![version_row.clone()]);
        // Dedup keeps an older location, so the new one gets no version row.
        let effects = rows(Ulid::from_parts(6, 6));
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::Storage(StorageEffect::BatchWrite { .. }))),
            "{effects:?}"
        );
    }

    #[test]
    fn checks_expected_head() {
        let head = Ulid::from_parts(9, 9);
        let pointer = |id| {
            Some(
                CurrentVersionPointer::new_with_generation(id, 1)
                    .to_bytes()
                    .unwrap(),
            )
        };
        let lookup = |expected: Option<Ulid>, found: Option<Vec<u8>>| {
            let mut operation = operation(DEFAULT_PATTERN).with_expected_head(expected);
            operation.txn_id = Some(Ulid::nil());
            operation.state = PutObjectState::ReadObjectLookup;
            let effects = operation.step(read(found));
            (operation, effects)
        };

        let (_, effects) = lookup(Some(head), pointer(head));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));
        let (_, effects) = lookup(None, None);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { .. })]
        ));

        // A write or a delete marker since the caller looked keeps the key.
        for (expected, found) in [
            (Some(head), pointer(Ulid::from_parts(9, 10))),
            (None, pointer(head)),
            (Some(head), None),
        ] {
            let (operation, effects) = lookup(expected, found);
            assert!(matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::AbortTransaction { .. })]
            ));
            assert_eq!(
                operation.finalize().unwrap_err(),
                PutObjectError::HeadChanged
            );
        }
    }

    #[test]
    fn picks_same_name() {
        let id = Ulid::from_parts(7, 7);
        let path = "raw/a.csv";
        assert_eq!(
            same_name_path(path, id, false, false),
            (path.to_string(), true)
        );
        assert_eq!(
            same_name_path(path, id, false, true),
            (path.to_string(), true)
        );
        // A backend with versions keeps the replaced bytes, so the write overwrites.
        assert_eq!(
            same_name_path(path, id, true, true),
            (path.to_string(), false)
        );
        // Without versions the write goes next to the file, never over it.
        assert_eq!(
            same_name_path(path, id, true, false),
            (format!("raw/a.csv~{id}"), true)
        );
    }

    /// Runs the gates and the read of the rule's record at the path, answered with `row`.
    fn owner_answered(operation: &mut PutObjectOperation, row: Option<&str>) -> Effects {
        let effects = begin(operation);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == IMPORTED_OBJECT_KEYSPACE
        ));
        let row = row.map(|key| {
            let id = Ulid::from_parts(8, 8);
            ImportedObject::written(id, key.to_string(), 3, None)
                .to_bytes()
                .unwrap()
        });
        operation.step(read(row))
    }

    #[test]
    fn ignores_keyless_rows() {
        // An imported `raw/a.csv` maps to `in/a.csv`, yet `in/x/a.csv` can give the same path.
        let listed = aruna_core::structs::storage::backend_import::ExternalObject {
            key: "raw/a.csv".to_string(),
            size: 3,
            etag: None,
            modified_ms: None,
        };
        let row = ImportedObject::new(Ulid::from_parts(8, 8), &listed);
        assert_eq!(row.key, None);
        let event = read(Some(row.to_bytes().unwrap()));
        assert_eq!(owns_path(event, "in/x/a.csv"), Ok(false));
    }

    #[test]
    fn keeps_foreign_files() {
        // Only the file the rule recorded for this key is replaced on a backend with versions.
        let cases = [
            (Some("in/x/one.csv"), false),
            (Some("other/one.csv"), true),
            (None, true),
        ];
        for (row, tagged) in cases {
            let mut operation = operation("{dir}{name}");
            owner_answered(&mut operation, row);
            let id = operation.version_id.unwrap();
            operation.step(Event::Blob(BlobEvent::PlainStat {
                exists: true,
                version: Some("v1".to_string()),
            }));
            let effects = past_intent(&mut operation);
            let [Effect::Blob(BlobEffect::WritePlain { plain, .. })] = effects.as_slice() else {
                panic!("expected a plain write, got {effects:?}")
            };
            let expected = match tagged {
                true => (format!("raw/x/one.csv~{id}"), true),
                false => ("raw/x/one.csv".to_string(), false),
            };
            assert_eq!(
                (plain.path.clone(), plain.if_not_exists),
                expected,
                "{row:?}"
            );
        }
    }

    #[test]
    fn stats_same_name() {
        let mut operation = operation("{dir}{name}");
        let effects = owner_answered(&mut operation, None);
        let id = operation.version_id.unwrap();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::StatPlain { path, .. })] if path == "raw/x/one.csv"
        ));

        operation.step(Event::Blob(BlobEvent::PlainStat {
            exists: true,
            version: None,
        }));
        let effects = past_intent(&mut operation);
        let [Effect::Blob(BlobEffect::WritePlain { plain, .. })] = effects.as_slice() else {
            panic!("expected a plain write, got {effects:?}")
        };
        assert_eq!(plain.path, format!("raw/x/one.csv~{id}"));
        assert!(plain.if_not_exists);
    }

    #[test]
    fn adopts_registered_version() {
        let found = |stored: Option<[u8; 32]>| {
            let mut put = operation("{dir}{name}");
            begin(&mut put);
            let id = put.version_id.unwrap();
            put.output = Some(Ok(written("raw/x/one.csv".to_string(), id)));
            put.txn_id = Some(Ulid::nil());
            // The version is read in the transaction before the head moves.
            let effects = put.write_current_lookup(None);
            let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice()
            else {
                panic!("expected the version read, got {effects:?}")
            };
            assert_eq!(key_space, BLOB_VERSIONS_KEYSPACE);
            let version = stored.map(|hash| {
                let version = BlobVersion::materialized(
                    hash,
                    BackendRef::Group(backend_id()),
                    aruna_core::structs::storage::format::EncodingClass::Raw,
                    SystemTime::UNIX_EPOCH,
                    UserId::default(),
                    None,
                );
                version.to_bytes().unwrap()
            });
            let effects = put.step(read(version));
            (put, effects)
        };
        // Nothing registered yet: the delete trail is read, then the head moves as usual.
        let (mut put, _) = found(None);
        let effects = put.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { key_space, .. })]
                if key_space == aruna_core::keyspaces::DELETE_TRAIL_KEYSPACE
        ));
        let page = || {
            Event::Storage(StorageEvent::IterResult {
                values: Vec::new(),
                next_start_after: None,
            })
        };
        put.step(page());
        let effects = put.step(page());
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { key_space, .. })] if key_space == BLOB_HEAD_KEYSPACE
        ));
        // An import registered the same bytes first: no head move and no second charge.
        let (mut put, effects) = found(Some([7u8; 32]));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        let effects = put.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: Ulid::nil(),
        }));
        assert!(!effects.iter().any(|effect| matches!(
            effect,
            Effect::Storage(StorageEffect::Write { .. } | StorageEffect::AddUsage { .. })
        )));
        assert!(put.output.as_ref().is_some_and(Result::is_ok));
        // Other bytes under the same version id are a conflict.
        let (put, _) = found(Some([8u8; 32]));
        assert!(matches!(
            put.output,
            Some(Err(PutObjectError::VersionExists))
        ));
    }

    #[test]
    fn records_written_hash() {
        let mut operation = operation("{dir}{name}");
        owner_answered(&mut operation, None);
        operation.step(Event::Blob(BlobEvent::PlainStat {
            exists: false,
            version: None,
        }));
        past_intent(&mut operation);
        // A backend without conditional writes put the bytes at the tagged path instead.
        let id = operation.version_id.unwrap();
        let tagged = format!("raw/x/one.csv~{id}");
        let effects = operation.step(Event::Blob(BlobEvent::PlainWritten {
            location: written(tagged.clone(), id),
            version: None,
            etag: None,
        }));
        let effects = past_fence(&mut operation, effects);
        let [Effect::Storage(StorageEffect::BatchWrite { writes, txn_id })] = effects.as_slice()
        else {
            panic!("expected the written hash, got {effects:?}")
        };
        let [(space, key, value)] = writes.as_slice() else {
            panic!("only the reached path gets the hash, got {writes:?}")
        };
        assert_eq!(space.as_str(), IMPORT_INTENT_KEYSPACE);
        assert!(txn_id.is_some());
        assert_eq!(*key, intent_key(Ulid::from_bytes([4u8; 16]), &tagged, id));
        let intent = WriteIntent::from_bytes(value).unwrap();
        assert_eq!(intent.written, Some([7u8; 32]));
        // The version transaction starts only after the hash is stored with the rule read.
        let effects = fence_done(&mut operation);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ));
    }

    #[test]
    fn stores_with_rule() {
        // A removed rule gets no intent: the transaction that read it writes nothing.
        let mut operation = operation(DEFAULT_PATTERN);
        operation.start();
        operation.step(read(None));
        operation.step(read(None));
        operation.step(read(None));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_parts(77, 7),
        }));
        let effects = operation.step(read(None));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        operation.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: Ulid::from_parts(77, 7),
        }));
        assert!(operation.written_intent.is_empty());
        assert_eq!(
            operation.finalize().unwrap_err(),
            PutObjectError::ImportRemoved
        );
    }

    #[test]
    fn keeps_failed_drop() {
        let mut operation = operation("{dir}{name}");
        owner_answered(&mut operation, None);
        operation.step(Event::Blob(BlobEvent::PlainStat {
            exists: false,
            version: None,
        }));
        past_intent(&mut operation);
        operation.step(Event::Blob(BlobEvent::Error(BlobError::PathTaken)));
        // An intent that could not be deleted stays unresolved; the write reports why.
        let error = aruna_core::errors::StorageError::WriteError("disk".to_string());
        operation.step(Event::Storage(StorageEvent::Error {
            error: error.clone(),
        }));
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize().unwrap_err(),
            PutObjectError::StorageError(error)
        );
    }

    #[test]
    fn taken_path_fails() {
        let mut operation = operation("{dir}{name}");
        owner_answered(&mut operation, None);
        operation.step(Event::Blob(BlobEvent::PlainStat {
            exists: false,
            version: None,
        }));
        past_intent(&mut operation);

        // Another upload took the free path first; nothing at the path is deleted.
        let effects = operation.step(Event::Blob(BlobEvent::Error(BlobError::PathTaken)));
        // The refused write moved no byte, so its intent goes first.
        let id = operation.version_id.unwrap();
        let [Effect::Storage(StorageEffect::BatchDelete { deletes, txn_id })] = effects.as_slice()
        else {
            panic!("expected the intent delete, got {effects:?}")
        };
        // A free path may go to its tagged path on a backend without conditional writes.
        let keys = intent_keys(Ulid::from_bytes([4u8; 16]), "raw/x/one.csv", id, true);
        assert_eq!(keys.len(), 2);
        let found: Vec<_> = deletes.iter().map(|(_, key)| key.clone()).collect();
        assert_eq!((found, *txn_id), (keys, None));
        let effects = operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        // The write fails as taken only once the delete is on disk.
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::SyncAll)]
        ));
        assert!(!operation.is_complete());
        let effects = operation.step(Event::Storage(StorageEvent::SyncAllFinished));
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::Blob(BlobEffect::Delete { .. }))),
            "{effects:?}"
        );
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize().unwrap_err(),
            PutObjectError::BlobWriteFailed(BlobError::PathTaken)
        );
    }

    /// A write whose provider version is known, at its tagged path, before its transaction.
    fn rewritten(hash: u8) -> PutObjectOperation {
        let mut operation = operation(DEFAULT_PATTERN);
        operation.version_id = Some(Ulid::from_parts(9, 1));
        let resolved = ResolvedBackend::new(BackendRef::Group(backend_id()), None);
        let target = operation.plain_target(&resolved).unwrap();
        operation.plain = Some(PlainWrite {
            target,
            resolved,
            version: Some("v2".to_string()),
            etag: None,
            owned: true,
        });
        let mut new = written("raw/x/one.csv~new".to_string(), Ulid::from_parts(5, 5));
        new.hashes.insert(HASH_BLAKE3.to_string(), vec![hash; 32]);
        operation.written_location = Some(new);
        operation.txn_id = Some(Ulid::nil());
        operation
    }

    /// Answers the reads of the current version, whose file holds bytes with hash 7.
    fn current(operation: &mut PutObjectOperation, old: &BackendLocation) -> Effects {
        let effects = operation.start_hash_lookup();
        let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice() else {
            panic!("expected the head read, got {effects:?}")
        };
        assert_eq!(key_space, BLOB_HEAD_KEYSPACE);
        let head = CurrentVersionPointer::new(Ulid::from_parts(8, 1));
        operation.step(read(Some(head.to_bytes().unwrap())));
        let version = BlobVersion::materialized(
            [7; 32],
            BackendRef::Group(backend_id()),
            old.format.encoding(),
            SystemTime::UNIX_EPOCH,
            UserId::default(),
            None,
        );
        operation.step(read(Some(version.to_bytes().unwrap())))
    }

    #[test]
    fn reuses_same_file() {
        let mut old = written("raw/x/one.csv~old".to_string(), Ulid::from_parts(4, 4));
        old.format.layout = StoredLayout::Plain(old.ulid);
        let mut operation = rewritten(7);
        let effects = current(&mut operation, &old);
        let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice() else {
            panic!("expected the location read, got {effects:?}")
        };
        assert_eq!(key_space, BLOB_LOCATIONS_KEYSPACE);
        operation.step(read(Some(old.to_bytes().unwrap())));
        operation.step(read(Some(b"v1".to_vec())));
        // The older file's version is not being deleted, so it takes the new version.
        assert_eq!(operation.state, PutObjectState::ReadDedupClaim);
        operation.step(read(None));
        // The version reads the older file; the written one is released after the commit.
        assert_eq!(operation.output, Some(Ok(old.clone())));
        let (space, _, value) = operation.dedup_release().unwrap().unwrap();
        assert_eq!(space, BLOB_CLEANUP_KEYSPACE);
        let work = BlobCleanupWork::from_bytes(&value).unwrap();
        let BlobCleanupWork::ReleaseRenamed {
            location,
            import_id,
            version_id,
            version,
        } = work
        else {
            panic!("expected a release, got {work:?}")
        };
        assert_eq!(location.backend_path, "raw/x/one.csv~new");
        assert_eq!(
            (import_id, version_id, version.as_str()),
            (Ulid::from_bytes([4u8; 16]), Ulid::from_parts(9, 1), "v2")
        );
    }

    #[test]
    fn keeps_other_bytes() {
        let mut old = written("raw/x/one.csv~old".to_string(), Ulid::from_parts(4, 4));
        old.format.layout = StoredLayout::Plain(old.ulid);
        // Other bytes than the current version's go the usual way and release nothing.
        let mut operation = rewritten(8);
        let effects = current(&mut operation, &old);
        assert_eq!(
            operation.state,
            PutObjectState::CheckHashLookup,
            "{effects:?}"
        );
        assert!(operation.dedup_release().is_none());
        // Without a backend version the written file cannot go, so it is kept as well.
        let mut operation = rewritten(7);
        if let Some(plain) = operation.plain.as_mut() {
            plain.version = None;
        }
        operation.start_hash_lookup();
        assert_eq!(operation.state, PutObjectState::CheckHashLookup);
    }

    #[test]
    fn reuses_older_version() {
        // A same-name re-upload on a versioned storage gives way to the older backend version.
        let reuse = |stored: &[u8]| {
            let mut old = written("raw/x/one.csv~new".to_string(), Ulid::from_parts(4, 4));
            old.format.layout = StoredLayout::Plain(old.ulid);
            let mut operation = rewritten(7);
            current(&mut operation, &old);
            let effects = operation.step(read(Some(old.to_bytes().unwrap())));
            let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice()
            else {
                panic!("expected the backend version read, got {effects:?}")
            };
            assert_eq!(key_space, BACKEND_VERSION_KEYSPACE);
            operation.step(read(Some(stored.to_vec())));
            if operation.state == PutObjectState::ReadDedupClaim {
                operation.step(read(None));
            }
            (operation, old)
        };
        let (operation, old) = reuse(b"v1");
        assert_eq!(operation.output, Some(Ok(old)));
        assert!(operation.dedup_release().is_some());
        // The same backend version is the same file; nothing is released.
        let (operation, _) = reuse(b"v2");
        assert_eq!(operation.state, PutObjectState::CheckHashLookup);
        assert!(operation.dedup_release().is_none());
    }

    #[test]
    fn skips_claimed_file() {
        // A backend version a release is deleting takes no new reader.
        let mut old = written("raw/x/one.csv~old".to_string(), Ulid::from_parts(4, 4));
        old.format.layout = StoredLayout::Plain(old.ulid);
        let mut operation = rewritten(7);
        current(&mut operation, &old);
        operation.step(read(Some(old.to_bytes().unwrap())));
        operation.step(read(Some(b"v1".to_vec())));
        operation.step(read(Some(Vec::new())));
        assert_eq!(operation.state, PutObjectState::CheckHashLookup);
        assert!(operation.dedup_release().is_none());
    }
}
