//! Writes a version to its path in the tenant's folders under a backend import rule.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BACKEND_VERSION_KEYSPACE, IMPORTED_OBJECT_KEYSPACE,
};
use aruna_core::structs::storage::backend_import::{
    ImportedObject, PlainPath, import_key, object_key, tagged_path, version_key,
};
use aruna_core::structs::storage::blob::ResolvedBackend;
use aruna_core::structs::storage::group_backend::path_bucket;
use aruna_core::structs::storage::routing::PlainTarget;
use aruna_core::types::{Key, Value};

/// The path a write takes, the backend it resolved to and the backend version it got.
#[derive(Debug, PartialEq)]
pub(super) struct PlainWrite {
    target: PlainTarget,
    resolved: ResolvedBackend,
    pub(super) version: Option<String>,
}

/// Rows an import commits with its version; the rule must still exist in that transaction.
#[derive(Debug, PartialEq)]
pub(super) struct ImportRows {
    import_id: Ulid,
    writes: Vec<(String, Key, Value)>,
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
        self.state = PutObjectState::WriteBlob;
        smallvec![Effect::Blob(BlobEffect::WritePlain {
            resolved: plain.resolved.clone(),
            plain: PlainPath {
                path,
                ulid,
                if_not_exists,
            },
            created_by: self.config.user_id,
            blob,
        })]
    }

    /// A plain write never takes over a copy in another provider bucket; its own copy then
    /// replaces that one as the copy of the content hash on this backend.
    pub(super) fn keeps_own(&self, existing: &BackendLocation, written: &BackendLocation) -> bool {
        let bucket = |location: &BackendLocation| match location.storage_bucket.as_str() {
            "" => path_bucket(&location.backend_path).map(str::to_string),
            bucket => Some(bucket.to_string()),
        };
        self.plain.is_some() && bucket(existing) != bucket(written)
    }

    /// Rows committed with the version only while the import rule `import_id` exists.
    pub fn with_import_rows(mut self, import_id: Ulid, writes: Vec<(String, Key, Value)>) -> Self {
        self.import_rows = Some(ImportRows { import_id, writes });
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
            return Some(smallvec![Effect::Storage(StorageEffect::Read {
                key_space: BACKEND_IMPORT_KEYSPACE.to_string(),
                key,
                txn_id: Some(txn_id),
            })]);
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
        let mut writes = Vec::new();
        if let Some(version) = self.version {
            let key = version_key(self.written.ulid);
            let value = version.as_bytes().to_vec().into();
            writes.push((BACKEND_VERSION_KEYSPACE.to_string(), key, value));
        }
        if self.target.listed && rule_exists {
            let row = ImportedObject::written(
                self.version_id,
                self.key.to_string(),
                self.written.blob_size,
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
    fn writes_unique_path() {
        let mut operation = operation(DEFAULT_PATTERN);
        let effects = begin(&mut operation);

        let id = operation.version_id.expect("the path names the version");
        let [Effect::Blob(BlobEffect::WritePlain { plain, .. })] = effects.as_slice() else {
            panic!("expected a plain write, got {effects:?}")
        };
        assert_eq!(plain.path, format!("raw/x/{id}_one.csv"));
        assert_eq!(plain.ulid, id);
        assert!(!plain.if_not_exists);

        // A failed write leaves the tenant path alone.
        operation.step(Event::Blob(BlobEvent::PlainWritten {
            location: written(plain.path.clone(), id),
            version: None,
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
            operation.plain.as_mut().unwrap().version = Some("v1".to_string());
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
        assert!(row.unlisted());

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

        let effects = operation.step(Event::Blob(BlobEvent::PlainStat {
            exists: true,
            version: None,
        }));
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

        // Another upload took the free path first; nothing at the path is deleted.
        let effects = operation.step(Event::Blob(BlobEvent::Error(BlobError::PathTaken)));
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
