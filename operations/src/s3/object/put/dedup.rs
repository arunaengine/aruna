//! Lets a linked re-upload with the bytes of the key's current version read that version's
//! file, and queues the release of the file the upload just wrote.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::{BACKEND_VERSION_KEYSPACE, DELETE_CLAIM_KEYSPACE};
use aruna_core::structs::storage::backend_import::{claim_key, version_key};
use aruna_core::types::{Key, Value};

impl PutObjectOperation {
    /// Only Aruna's own linked write with a known backend version may give way: an import
    /// registers a file it did not write, and without the version no exact delete exists.
    fn dedups(&self) -> bool {
        self.import_rows.is_none()
            && !self.dedup_checked
            && self.seal_plan.is_none()
            && (self.plain.as_ref()).is_some_and(|plain| plain.version.is_some())
    }

    /// Reads the key's current version in the version transaction, before the usual lookup.
    pub(super) fn read_current(&mut self) -> Option<Effects> {
        if !self.dedups() {
            return None;
        }
        self.dedup_checked = true;
        let request = &self.config.request;
        let head = BlobHeadKey::new(request.bucket.clone(), request.key.clone()).to_bytes();
        let Ok(head) = head else {
            return Some(self.start_hash_lookup());
        };
        self.state = PutObjectState::ReadDedupHead;
        Some(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: head.into(),
            txn_id: self.txn_id,
        })])
    }

    pub(super) fn dedup_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        match (&self.state, value) {
            (PutObjectState::ReadDedupHead, Some(value)) => {
                let pointer = match CurrentVersionPointer::from_bytes(&value) {
                    Ok(pointer) => pointer,
                    Err(error) => return self.emit_error(error.into()),
                };
                let request = &self.config.request;
                let key = VersionKey::new(&request.bucket, &request.key, pointer.version_id);
                match key.to_bytes() {
                    Ok(key) => {
                        self.state = PutObjectState::ReadDedupVersion;
                        smallvec![Effect::Storage(StorageEffect::Read {
                            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                            key: key.into(),
                            txn_id: self.txn_id,
                        })]
                    }
                    Err(error) => self.emit_error(error.into()),
                }
            }
            (PutObjectState::ReadDedupVersion, Some(value)) => {
                let version = match BlobVersion::from_bytes(&value) {
                    Ok(version) => version,
                    Err(error) => return self.emit_error(error.into()),
                };
                let written = self.get_written_location();
                let same = written.is_some_and(|written| {
                    let hash = written.get_blake3();
                    version.blob_hash().map(|hash| &hash[..]) == hash
                        && version.blob_backend() == Some(&written.backend)
                });
                match version
                    .location_key()
                    .filter(|key| same && plain_class(key))
                {
                    Some(key) => {
                        self.state = PutObjectState::ReadDedupLocation;
                        smallvec![blob_location_read(&key, self.txn_id)]
                    }
                    None => self.start_hash_lookup(),
                }
            }
            (PutObjectState::ReadDedupLocation, Some(value)) => {
                let existing = match BackendLocation::from_bytes(&value) {
                    Ok(existing) => existing,
                    Err(error) => return self.emit_error(error.into()),
                };
                let Some(written) = self.get_written_location().cloned() else {
                    return self.emit_error(PutObjectError::MissingOutput);
                };
                let usable = existing.is_external()
                    && !existing.staging
                    && !existing.partial
                    && existing.blob_size == written.blob_size;
                if !usable {
                    return self.start_hash_lookup();
                }
                // The kept file's backend version decides a same-path reuse and its claim.
                self.state = PutObjectState::ReadDedupProvider;
                self.output = Some(Ok(existing.clone()));
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: BACKEND_VERSION_KEYSPACE.to_string(),
                    key: version_key(existing.ulid),
                    txn_id: self.txn_id,
                })]
            }
            (PutObjectState::ReadDedupProvider, value) => {
                let kept = self.get_output().cloned();
                let (Some(kept), Some(written)) = (kept, self.get_written_location().cloned())
                else {
                    return self.emit_error(PutObjectError::MissingOutput);
                };
                let fresh = self.plain.as_ref().and_then(|plain| plain.version.clone());
                let other = value.map(|value| String::from_utf8_lossy(&value).into_owned());
                // At the same path only another backend version of it is another file.
                let other_bytes = other.is_some() && other != fresh;
                if kept.same_object(&written) && !other_bytes {
                    self.output = None;
                    return self.start_hash_lookup();
                }
                let Some(other) = other else {
                    return self.reuse(written, kept);
                };
                // A version a release is deleting takes no new reader.
                match claim_key(&kept, &other) {
                    Ok(claim) => {
                        self.state = PutObjectState::ReadDedupClaim;
                        smallvec![Effect::Storage(StorageEffect::Read {
                            key_space: DELETE_CLAIM_KEYSPACE.to_string(),
                            key: claim,
                            txn_id: self.txn_id,
                        })]
                    }
                    Err(error) => self.emit_error(error.into()),
                }
            }
            (PutObjectState::ReadDedupClaim, None) => {
                let kept = self.get_output().cloned();
                let (Some(kept), Some(written)) = (kept, self.get_written_location().cloned())
                else {
                    return self.emit_error(PutObjectError::MissingOutput);
                };
                self.reuse(written, kept)
            }
            (PutObjectState::ReadDedupClaim, Some(_)) => {
                self.output = None;
                self.start_hash_lookup()
            }
            _ => self.start_hash_lookup(),
        }
    }

    fn reuse(&mut self, written: BackendLocation, existing: BackendLocation) -> Effects {
        self.dedup = Some(written);
        self.output = Some(Ok(existing));
        self.create_blob_location()
    }

    /// The release of the written file, committed with the version that reads the older one.
    pub(super) fn dedup_release(&self) -> Option<Result<(String, Key, Value), ConversionError>> {
        let written = self.dedup.as_ref()?;
        let plain = self.plain.as_ref()?;
        let work = BlobCleanupWork::ReleaseRenamed {
            location: written.clone(),
            import_id: plain.target.import_id,
            version_id: self.version_id?,
            version: plain.version.clone()?,
        };
        let key = Ulid::generate().to_bytes().to_vec().into();
        Some(work.to_bytes().map(|value| {
            let space = BLOB_CLEANUP_KEYSPACE.to_string();
            (space, key, value.into())
        }))
    }
}

/// A linked file has its own location row; only such a row is shared this way.
fn plain_class(key: &BlobLocationKey) -> bool {
    matches!(key.encoding, EncodingClass::Plain { .. })
}
