//! Composes a completed upload at its path in the tenant's folders under a backend import rule.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::object::put::{PlainRecord, intent_write, same_name_path, version_row};
use aruna_core::keyspaces::BACKEND_IMPORT_KEYSPACE;
use aruna_core::structs::storage::backend_import::{WriteIntent, import_key, tagged_path};
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
        let stat = BlobEffect::StatPlain {
            backend: target.backend.clone(),
            path: target.path.clone(),
        };
        let (same_name, path) = (target.same_name, target.path.clone());
        self.plain = Some(target);
        if !same_name {
            return Some(self.compose_plain(path, false));
        }
        self.state = CompleteUploadState::StatPlain;
        Some(smallvec![Effect::Blob(stat)])
    }

    /// The provider upload of an upload that opened at its plain path.
    fn plain_upload(&self) -> Option<&BackendUpload> {
        let upload = self.upload_record.as_ref()?.backend_upload.as_ref()?;
        upload.location.is_external().then_some(upload)
    }

    /// The object completes where its parts streamed to; the version id is the one the path
    /// was chosen with. The path is looked at again, since the provider completes in place.
    fn stat_in_place(&mut self, location: BackendLocation) -> Effects {
        let id = location.ulid;
        let (bucket, key) = (&self.input.bucket, &self.input.key);
        self.plain = self
            .routing
            .as_ref()
            .and_then(|routing| routing.plain_target(bucket, key, id))
            .filter(|target| target.backend == location.backend);
        self.version_id = Some(id);
        self.state = CompleteUploadState::StatPlain;
        smallvec![Effect::Blob(BlobEffect::StatPlain {
            backend: location.backend,
            path: location.backend_path,
        })]
    }

    pub(super) fn plain_stat(&mut self, event: Event) -> Effects {
        // Only a backend with versions keeps a file that appeared meanwhile; else it conflicts.
        if self.plain_upload().is_some() {
            return match event {
                Event::Blob(BlobEvent::PlainStat { exists, version }) => {
                    let replaces = exists && version.is_some();
                    let path = self
                        .plain_upload()
                        .map(|upload| upload.location.backend_path.clone());
                    match (self.complete_provider(!replaces), path) {
                        (Some(effects), Some(path)) => self.intent_first(&path, effects),
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
                let path = &target.path;
                let (path, if_not_exists) = same_name_path(path, id, exists, version.is_some());
                self.compose_plain(path, if_not_exists)
            }
            Event::Blob(BlobEvent::Error(error)) => self.schedule_error(error.into()),
            _ => self.schedule_error(CompleteUploadError::InvalidOperationState),
        }
    }

    /// Another writer took the free path first; the parts are still there, so the object
    /// takes its tagged path instead.
    pub(super) fn plain_taken(&mut self) -> Effects {
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
            if_not_exists,
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
        self.intent_first(&path, compose)
    }

    /// A listed plain object stores its identity before `compose` moves any byte to `path`.
    fn intent_first(&mut self, path: &str, compose: Effects) -> Effects {
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
        };
        match intent_write(target.import_id, path, &intent) {
            Ok(effect) => {
                self.pending_compose = Some(compose);
                self.state = CompleteUploadState::WriteIntent;
                smallvec![effect]
            }
            Err(error) => self.schedule_error(error.into()),
        }
    }

    pub(super) fn intent_written(&mut self, event: Event) -> Effects {
        match (event, self.pending_compose.take()) {
            (Event::Storage(StorageEvent::WriteResult { .. }), Some(compose)) => {
                self.state = CompleteUploadState::ComposeBlob;
                compose
            }
            (Event::Storage(StorageEvent::Error { error }), _) => self.schedule_error(error.into()),
            _ => self.schedule_error(CompleteUploadError::InvalidOperationState),
        }
    }

    /// Reads the rule of a listed plain object in the finalize transaction, so its import
    /// row is written only while the rule exists.
    pub(super) fn plain_fence(&mut self) -> Option<Effects> {
        let target = self.plain.as_ref().filter(|target| target.listed)?;
        let group_id = self.upload_record.as_ref()?.group_id;
        let key = import_key(group_id, target.import_id);
        self.state = CompleteUploadState::ReadImportRule;
        Some(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BACKEND_IMPORT_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })])
    }

    pub(super) fn import_rule_read(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                self.write_cleanup_records(value.is_some())
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
        record.writes(rule_exists)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::keyspaces::{BACKEND_VERSION_KEYSPACE, IMPORTED_OBJECT_KEYSPACE};
    use aruna_core::structs::storage::backend_import::{BackendImport, DEFAULT_PATTERN};
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
        finalize_writes(&mut operation, exists)
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
        let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice() else {
            panic!("expected the rule read, got {effects:?}")
        };
        assert_eq!(key_space, BACKEND_IMPORT_KEYSPACE);
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: exists.then(|| vec![0u8].into()),
        }));
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected the records, got {effects:?}")
        };
        writes.clone()
    }

    fn count(writes: &[(String, Key, Value)], keyspace: &str) -> usize {
        writes
            .iter()
            .filter(|(key_space, _, _)| key_space == keyspace)
            .count()
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

    /// Answers the stored write intent that comes before a plain compose.
    fn past_intent(operation: &mut CompleteUploadOperation, effects: Effects) -> Effects {
        match effects.as_slice() {
            [Effect::Storage(StorageEffect::Write { key_space, .. })]
                if key_space == aruna_core::keyspaces::IMPORT_INTENT_KEYSPACE =>
            {
                operation.step(Event::Storage(StorageEvent::WriteResult {
                    key: Vec::new().into(),
                }))
            }
            _ => effects,
        }
    }

    #[test]
    fn composes_plain_path() {
        let mut operation = operation(DEFAULT_PATTERN);
        let effects = operation.compose_blob();

        // No byte moves before the identity of the object is stored.
        let id = operation.version_id.expect("the path names the version");
        let [Effect::Storage(StorageEffect::Write { key, value, .. })] = effects.as_slice() else {
            panic!("expected the write intent, got {effects:?}")
        };
        let path = format!("raw/{id}_object");
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
        assert_eq!(plain.path, format!("raw/{id}_object"));
        assert_eq!(plain.ulid, id);
    }

    #[test]
    fn falls_back_tagged() {
        let mut operation = operation("{dir}{name}");
        let effects = operation.compose_blob();
        let id = operation.version_id.unwrap();
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
        let plain = composed(&past_intent(&mut operation, effects));
        assert_eq!(plain.path, format!("raw/object~{id}"));
        assert!(plain.if_not_exists);

        let effects = operation.step(taken());
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

    /// An upload whose provider upload opened at `raw/object` as version `id`.
    fn in_place(id: Ulid) -> CompleteUploadOperation {
        let mut operation = operation("{dir}{name}");
        let location = BackendLocation {
            backend: BackendRef::Group(backend_id()),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: "raw/object".to_string(),
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
        let effects = operation.compose_blob();
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::Blob(BlobEffect::StatPlain { path, .. })] if path == "raw/object"
            ),
            "expected the path stat, got {effects:?}"
        );
        assert_eq!(operation.version_id, Some(id));
        operation
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

        // The object written in place gets the records of a plain write.
        let mut operation = in_place(id);
        written_in_place(&mut operation);
        let writes = finalize_writes(&mut operation, true);
        assert_eq!(count(&writes, BACKEND_VERSION_KEYSPACE), 1);
        assert_eq!(count(&writes, IMPORTED_OBJECT_KEYSPACE), 1);
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
}
