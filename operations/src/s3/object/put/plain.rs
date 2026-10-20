//! Writes a version to its path in the tenant's folders under a backend import rule.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

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
use aruna_core::structs::storage::routing::PlainTarget;
use aruna_core::types::{Key, Value};

/// The path a write takes, the backend it resolved to and the backend version it got.
#[derive(Debug, PartialEq)]
pub(super) struct PlainWrite {
    target: PlainTarget,
    resolved: ResolvedBackend,
    pub(super) version: Option<String>,
    pub(super) etag: Option<String>,
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
        let id = self.version_id.unwrap_or_else(Ulid::generate);
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
        let stat = BlobEffect::StatPlain {
            backend: target.backend.clone(),
            path: target.path.clone(),
        };
        let (same_name, path) = (target.same_name, target.path.clone());
        self.plain = Some(PlainWrite {
            target,
            resolved,
            version: None,
            etag: None,
        });
        if !same_name {
            return self.write_plain(path, false);
        }
        self.state = PutObjectState::StatPlain;
        smallvec![Effect::Blob(stat)]
    }

    pub(super) fn plain_stat(&mut self, event: Event) -> Effects {
        let (Some(plain), Some(id)) = (self.plain.as_ref(), self.version_id) else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        match event {
            Event::Blob(BlobEvent::PlainStat { exists, version }) => {
                let path = &plain.target.path;
                let (path, if_not_exists) = same_name_path(path, id, exists, version.is_some());
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
        let intent = plain.target.listed.then(|| WriteIntent {
            key: self.config.request.key.clone(),
            version_id: ulid,
            backend: plain.target.backend.clone(),
            created_by: self.config.user_id,
        });
        let (import_id, intent_path) = (plain.target.import_id, path.clone());
        let write = Effect::Blob(BlobEffect::WritePlain {
            resolved: plain.resolved.clone(),
            plain: PlainPath {
                path,
                ulid,
                if_not_exists,
            },
            created_by: self.config.user_id,
            blob,
        });
        self.state = PutObjectState::WriteBlob;
        let Some(intent) = intent else {
            return smallvec![write];
        };
        match intent_write(import_id, &intent_path, &intent) {
            Ok(effect) => {
                self.written_intent = Some(intent_key(import_id, &intent_path, ulid));
                self.pending_write = Some(smallvec![write]);
                self.state = PutObjectState::WriteIntent;
                smallvec![effect]
            }
            Err(error) => self.emit_error(error.into()),
        }
    }

    /// Bytes move only once their identity is stored and synced to disk.
    pub(super) fn intent_written(&mut self, event: Event) -> Effects {
        match (event, self.pending_write.take()) {
            (Event::Storage(StorageEvent::WriteResult { .. }), Some(write)) => {
                self.pending_write = Some(write);
                smallvec![Effect::Storage(StorageEffect::SyncAll)]
            }
            (Event::Storage(StorageEvent::SyncAllFinished), Some(write)) => {
                self.state = PutObjectState::WriteBlob;
                write
            }
            (Event::Storage(StorageEvent::Error { error }), _) => self.emit_error(error.into()),
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    /// A refused conditional write moved no byte, so its intent must not claim the path.
    pub(super) fn drop_intent(&mut self) -> Effects {
        let Some(key) = self.written_intent.take() else {
            return self
                .cleanup_failed_write(PutObjectError::BlobWriteFailed(BlobError::PathTaken));
        };
        self.state = PutObjectState::DropIntent;
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: IMPORT_INTENT_KEYSPACE.to_string(),
            key,
            txn_id: None,
        })]
    }

    /// A failed delete leaves the intent; the import then finds no committed version for it.
    pub(super) fn intent_dropped(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::DeleteResult { .. } | StorageEvent::Error { .. }) => {
                self.cleanup_failed_write(PutObjectError::BlobWriteFailed(BlobError::PathTaken))
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
        let own = self.plain.as_ref().zip(self.get_written_location());
        let own = own.zip(self.version_id).and_then(|((plain, written), id)| {
            let listed = plain.target.listed;
            listed.then(|| intent_key(plain.target.import_id, &written.backend_path, id))
        });
        let keys = rows.chain(own);
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
        if self.rule_exists.is_none()
            && let Some(key) = self.rule_fence()
        {
            self.state = PutObjectState::ReadImportRule;
            let guards = self
                .import_rows
                .as_ref()
                .map_or(&[][..], |rows| &rows.guards);
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
            && let Some(effects) = self.trail_page(txn_id)
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
        }
        if writes.is_empty() {
            return None;
        }
        self.state = PutObjectState::WritePlainRecords;
        Some(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    pub(super) fn import_rule_read(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(PutObjectError::NoTransactionFound);
        };
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                self.rule_exists = Some(value.is_some());
                self.write_cleanup_row(txn_id)
            }
            Event::Storage(StorageEvent::BatchReadResult { values }) => {
                let guards = self
                    .import_rows
                    .as_ref()
                    .map_or(&[][..], |rows| &rows.guards);
                let Some(((_, rule), seen)) = values.split_first() else {
                    return self.emit_error(PutObjectError::InvalidOperationState);
                };
                let expected = guards.iter().map(|(_, _, value)| value);
                if seen.len() != guards.len() || !seen.iter().map(|(_, v)| v).eq(expected) {
                    return self.emit_error(PutObjectError::HeadChanged);
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

/// Stores the identity of a write to the import folder of `import_id` at `path`.
pub(crate) fn intent_write(
    import_id: Ulid,
    path: &str,
    intent: &WriteIntent,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: IMPORT_INTENT_KEYSPACE.to_string(),
        key: intent_key(import_id, path, intent.version_id),
        value: intent.to_bytes()?.into(),
        txn_id: None,
    }))
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
    use aruna_core::stream::BackendStream;
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
    fn begin(operation: &mut PutObjectOperation) -> Effects {
        operation.start();
        operation.step(read(None));
        operation.step(read(None));
        operation.step(read(None))
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
        let (_, effects) = mapped("data/a.csv");
        // The same-name write first looks at the exact key in the provider bucket.
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
        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        }));
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::SyncAll)]
            ),
            "expected the intent sync, got {effects:?}"
        );
        operation.step(Event::Storage(StorageEvent::SyncAllFinished))
    }

    #[test]
    fn writes_unique_path() {
        let mut operation = operation(DEFAULT_PATTERN);
        let effects = begin(&mut operation);

        // The identity of the write is stored before any byte moves.
        let id = operation.version_id.expect("the path names the version");
        let [
            Effect::Storage(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id,
            }),
        ] = effects.as_slice()
        else {
            panic!("expected the write intent, got {effects:?}")
        };
        let path = format!("raw/x/{id}_one.csv");
        assert_eq!(key_space, IMPORT_INTENT_KEYSPACE);
        assert_eq!(*key, intent_key(Ulid::from_bytes([4u8; 16]), &path, id));
        assert_eq!(*txn_id, None);
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
            let Some(effects) = operation.plain_records(Ulid::nil()) else {
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
            let Some(effects) = operation.plain_records(Ulid::nil()) else {
                panic!("expected the trail read")
            };
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
            assert_eq!(*prefix, Some(purges(operation.config.group_id).into()));
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
            let Some(effects) = operation.plain_records(Ulid::nil()) else {
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
        put.plain_records(Ulid::nil());
        put.step(read(Some(vec![0u8])));

        // The committed version makes its intent obsolete in the same transaction.
        let effects = put.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        let [Effect::Storage(StorageEffect::BatchDelete { deletes, txn_id })] = effects.as_slice()
        else {
            panic!("expected the intent delete, got {effects:?}")
        };
        let key = intent_key(Ulid::from_bytes([4u8; 16]), "raw/x/one.csv", id);
        assert_eq!(*deletes, vec![(IMPORT_INTENT_KEYSPACE.to_string(), key)]);
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
            let effects = operation.plain_records(Ulid::nil()).expect("a rule read");
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
            operation.plain_records(Ulid::nil()).expect("a rule read");
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

    #[test]
    fn stats_same_name() {
        let mut operation = operation("{dir}{name}");
        let effects = begin(&mut operation);
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
    fn taken_path_fails() {
        let mut operation = operation("{dir}{name}");
        begin(&mut operation);
        operation.step(Event::Blob(BlobEvent::PlainStat {
            exists: false,
            version: None,
        }));
        past_intent(&mut operation);

        // Another upload took the free path first; nothing at the path is deleted.
        let effects = operation.step(Event::Blob(BlobEvent::Error(BlobError::PathTaken)));
        // The refused write moved no byte, so its intent goes first.
        let id = operation.version_id.unwrap();
        let [Effect::Storage(StorageEffect::Delete { key_space, key, .. })] = effects.as_slice()
        else {
            panic!("expected the intent delete, got {effects:?}")
        };
        assert_eq!(key_space, IMPORT_INTENT_KEYSPACE);
        let intent = intent_key(Ulid::from_bytes([4u8; 16]), "raw/x/one.csv", id);
        assert_eq!(*key, intent);
        let effects = operation.step(Event::Storage(StorageEvent::DeleteResult {
            key: intent.clone(),
        }));
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
}
