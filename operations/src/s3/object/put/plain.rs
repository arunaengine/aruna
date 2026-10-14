//! Writes a version to its path in the tenant's folders under a backend import rule.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::aside::set_aside;
use super::*;
use aruna_core::keyspaces::{IMPORTED_OBJECT_KEYSPACE, PLAIN_NAME_KEYSPACE};
use aruna_core::structs::storage::backend_import::{
    ImportedObject, PlainPath, object_key, plain_name_key,
};
use aruna_core::structs::storage::blob::ResolvedBackend;
use aruna_core::structs::storage::routing::PlainTarget;
use aruna_core::types::{Key, Value};

/// The path a write takes and the backend it resolved to.
#[derive(Debug, PartialEq)]
pub(super) struct PlainWrite {
    target: PlainTarget,
    resolved: ResolvedBackend,
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
    pub(super) fn start_plain(
        &mut self,
        target: PlainTarget,
        resolved: ResolvedBackend,
    ) -> Effects {
        let Some(id) = self.version_id else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        self.adopted = true;
        let effect = BlobEffect::ClaimPlain {
            backend: target.backend.clone(),
            path: target.path.clone(),
            id,
        };
        let same_name = target.same_name;
        self.plain = Some(PlainWrite { target, resolved });
        if !same_name {
            return self.write_plain();
        }
        self.plain_claim = Some(id);
        self.state = PutObjectState::ClaimPlain;
        smallvec![Effect::Blob(effect)]
    }

    pub(super) fn plain_claimed(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::PlainClaimed { claimed: true }) => {
                let Some(plain) = self.plain.as_ref() else {
                    return self.emit_error(PutObjectError::InvalidOperationState);
                };
                let target = &plain.target;
                self.state = PutObjectState::SetAside;
                smallvec![set_aside(target.backend.clone(), target.path.clone())]
            }
            Event::Blob(BlobEvent::PlainClaimed { claimed: false }) => {
                self.emit_error(PutObjectError::PlainBusy)
            }
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    pub(super) fn plain_set_aside(&mut self, event: Event) -> Effects {
        match event {
            Event::SubOperation(SubOperationEvent::PlainSetAside { result: Ok(()) }) => {
                self.write_plain()
            }
            Event::SubOperation(SubOperationEvent::PlainSetAside { result: Err(error) }) => {
                self.emit_error(PutObjectError::PlainAside(error))
            }
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    fn write_plain(&mut self) -> Effects {
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
                path: plain.target.path.clone(),
                ulid,
            },
            created_by: self.config.user_id,
            blob,
            size: self.config.request.content_length,
        })]
    }

    pub(super) fn plain_records(&mut self, txn_id: Ulid) -> Option<Effects> {
        let target = &self.plain.as_ref()?.target;
        let (location, version_id) = (self.get_output()?, self.version_id?);
        let key = &self.config.request.key;
        let writes = match plain_writes(target, location, version_id, key) {
            Ok(writes) if writes.is_empty() => return None,
            Ok(writes) => writes,
            Err(error) => return Some(self.emit_error(error.into())),
        };
        self.state = PutObjectState::WritePlainRecords;
        Some(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }
}

/// Rows a plain write commits with its version. The hint lets the next same-name write find
/// the bytes; the import row keeps the rule's importer from importing them again.
pub(crate) fn plain_writes(
    target: &PlainTarget,
    location: &BackendLocation,
    version_id: Ulid,
    key: &str,
) -> Result<Vec<(String, Key, Value)>, ConversionError> {
    let mut writes = Vec::new();
    if target.same_name {
        let name = plain_name_key(&target.backend, &target.path);
        let hint = location.location_key()?.to_bytes();
        writes.push((PLAIN_NAME_KEYSPACE.to_string(), name, hint.into()));
    }
    if target.listed {
        let row = ImportedObject::written(version_id, key.to_string(), location.blob_size);
        let row_key = object_key(target.import_id, &target.path);
        let row = row.to_bytes()?;
        writes.push((IMPORTED_OBJECT_KEYSPACE.to_string(), row_key, row.into()));
    }
    Ok(writes)
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

        // A failed write leaves the tenant path alone.
        operation.step(Event::Blob(BlobEvent::WriteFinished {
            location: written(plain.path.clone(), id),
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
        let mut operation = operation("{dir}{name}");
        begin(&mut operation);
        let id = operation.version_id.unwrap();
        operation.output = Some(Ok(written("raw/x/one.csv".to_string(), id)));

        let Some(effects) = operation.plain_records(Ulid::nil()) else {
            panic!("expected the plain records")
        };
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected one batch write, got {effects:?}")
        };
        let [(hint_space, _, _), (row_space, row_key, row)] = writes.as_slice() else {
            panic!("expected the hint and the import row")
        };
        assert_eq!(hint_space, PLAIN_NAME_KEYSPACE);
        assert_eq!(row_space, IMPORTED_OBJECT_KEYSPACE);
        assert_eq!(
            *row_key,
            object_key(Ulid::from_bytes([4u8; 16]), "raw/x/one.csv")
        );
        let row = ImportedObject::from_bytes(row).unwrap();
        assert_eq!(row.key.as_deref(), Some("in/x/one.csv"));
        assert_eq!(row.version_id, id);
        assert!(row.unlisted());
    }

    #[test]
    fn claims_same_name() {
        let mut operation = operation("{dir}{name}");
        let effects = begin(&mut operation);
        let id = operation.version_id.unwrap();
        let [
            Effect::Blob(BlobEffect::ClaimPlain {
                path, id: claim, ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected a claim, got {effects:?}")
        };
        assert_eq!((path.as_str(), *claim), ("raw/x/one.csv", id));

        let effects = operation.step(Event::Blob(BlobEvent::PlainClaimed { claimed: true }));
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        let effects = operation.step(Event::SubOperation(SubOperationEvent::PlainSetAside {
            result: Ok(()),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::WritePlain { .. })]
        ));
    }

    #[test]
    fn busy_path_conflicts() {
        let mut operation = operation("{dir}{name}");
        begin(&mut operation);

        operation.step(Event::Blob(BlobEvent::PlainClaimed { claimed: false }));
        assert!(operation.is_complete());
        assert_eq!(operation.finalize().unwrap_err(), PutObjectError::PlainBusy);

        // A failed move releases the claim the write holds.
        let mut operation = self::operation("{dir}{name}");
        begin(&mut operation);
        let id = operation.version_id.unwrap();
        operation.step(Event::Blob(BlobEvent::PlainClaimed { claimed: true }));
        let effects = operation.step(Event::SubOperation(SubOperationEvent::PlainSetAside {
            result: Err("gone".to_string()),
        }));
        assert!(
            effects.iter().any(|effect| matches!(
                effect,
                Effect::Blob(BlobEffect::ReleaseReservation { id: released }) if *released == id
            )),
            "{effects:?}"
        );
    }
}
