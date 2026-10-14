//! Composes a completed upload at its path in the tenant's folders under a backend import rule.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::object::put::{PlainRecord, same_name_path};
use aruna_core::keyspaces::BACKEND_IMPORT_KEYSPACE;
use aruna_core::structs::storage::backend_import::{import_key, tagged_path};

impl CompleteUploadOperation {
    /// Routing inputs that may name a plain path for the completed object.
    pub fn with_routing(mut self, routing: RoutingSnapshot) -> Self {
        self.routing = Some(routing);
        self
    }

    /// A plain path only for raw parts composed on the rule's write backend. A same-name
    /// object first looks at the path; no lock is held.
    pub(super) fn plain_compose(&mut self) -> Option<Effects> {
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

    pub(super) fn plain_stat(&mut self, event: Event) -> Effects {
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
        smallvec![Effect::Blob(BlobEffect::Compose {
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
        })]
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
        let (Some(target), Some(written), Some(output), Some(version_id)) = (
            self.plain.as_ref(),
            self.composed_location.as_ref(),
            self.final_location.as_ref(),
            self.version_id,
        ) else {
            return Ok(Vec::new());
        };
        // A deduplicated version reads the older location, so it needs no version row.
        let version = self.plain_version.as_deref();
        let record = PlainRecord {
            target,
            written,
            version: version.filter(|_| output.ulid == written.ulid),
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

    #[test]
    fn composes_plain_path() {
        let mut operation = operation(DEFAULT_PATTERN);
        let effects = operation.compose_blob();

        let id = operation.version_id.expect("the path names the version");
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
        let plain = composed(&effects);
        assert_eq!(plain.path, "raw/object");
        assert!(plain.if_not_exists);

        // Another upload took the free path first; the parts still compose next to it.
        let taken = || Event::Blob(BlobEvent::Error(BlobError::PathTaken));
        let plain = composed(&operation.step(taken()));
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
