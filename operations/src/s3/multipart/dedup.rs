//! Lets a completed linked upload with the bytes of the key's current version read that
//! version's file, and queues the release of the file the upload just assembled.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::{BACKEND_VERSION_KEYSPACE, DELETE_CLAIM_KEYSPACE};
use aruna_core::structs::storage::backend_import::{claim_key, version_key};

impl CompleteUploadOperation {
    /// Only Aruna's own linked upload with a known backend version may give way: without the
    /// version no exact delete exists, and bytes an import registered are not Aruna's.
    fn dedups(&self) -> bool {
        let written = self.composed_location.as_ref();
        !self.dedup_checked
            && !self.registered
            && self.plain.is_some()
            && self.plain_version.is_some()
            && written.is_some_and(BackendLocation::is_external)
    }

    /// Reads the key's current version in the finalize transaction, before the usual lookup.
    pub(super) fn read_current(&mut self) -> Option<Turn> {
        if !self.dedups() {
            return None;
        }
        self.dedup_checked = true;
        let head = BlobHeadKey::new(self.input.bucket.clone(), self.input.key.clone());
        let Ok(head) = head.to_bytes() else {
            return Some(self.check_hash_lookup());
        };
        let read = read_row(BLOB_HEAD_KEYSPACE, head);
        Some(self.call(CompleteUploadState::ReadDedupHead, read))
    }

    pub(super) fn dedup_read(&mut self, value: Option<Value>) -> Turn {
        let written = self.composed_location.clone();
        let written = written.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        match (&self.state, value) {
            (CompleteUploadState::ReadDedupHead, Some(value)) => {
                let pointer = CurrentVersionPointer::from_bytes(&value)?;
                let key = VersionKey::new(&self.input.bucket, &self.input.key, pointer.version_id);
                let read = read_row(BLOB_VERSIONS_KEYSPACE, key.to_bytes()?);
                self.call(CompleteUploadState::ReadDedupVersion, read)
            }
            (CompleteUploadState::ReadDedupVersion, Some(value)) => {
                let version = BlobVersion::from_bytes(&value)?;
                let same = version.blob_hash().map(|hash| &hash[..]) == written.get_blake3()
                    && version.blob_backend() == Some(&written.backend);
                let plain =
                    |key: &BlobLocationKey| matches!(key.encoding, EncodingClass::Plain { .. });
                match version.location_key().filter(|key| same && plain(key)) {
                    Some(key) => {
                        let read = read_row(BLOB_LOCATIONS_KEYSPACE, key.to_bytes());
                        self.call(CompleteUploadState::ReadDedupLocation, read)
                    }
                    None => self.check_hash_lookup(),
                }
            }
            (CompleteUploadState::ReadDedupLocation, Some(value)) => {
                let existing = BackendLocation::from_bytes(&value)?;
                let usable = existing.is_external()
                    && !existing.staging
                    && !existing.partial
                    && existing.blob_size == written.blob_size;
                if !usable {
                    return self.check_hash_lookup();
                }
                // The kept file's backend version decides a same-path reuse and its claim.
                let read = read_row(BACKEND_VERSION_KEYSPACE, version_key(existing.ulid));
                self.final_location = Some(existing);
                self.call(CompleteUploadState::ReadDedupProvider, read)
            }
            (CompleteUploadState::ReadDedupProvider, value) => {
                let kept = self.final_location.clone();
                let kept = kept.ok_or(CompleteUploadError::CompleteUploadFailed)?;
                let other = value.map(|value| String::from_utf8_lossy(&value).into_owned());
                // At the same path only another backend version of it is another file.
                let other_bytes = other.is_some() && other != self.plain_version;
                if kept.same_object(&written) && !other_bytes {
                    self.final_location = None;
                    return self.check_hash_lookup();
                }
                let Some(other) = other else {
                    return self.reuse(written, kept);
                };
                // A version a release is deleting takes no new reader.
                let read = read_row(DELETE_CLAIM_KEYSPACE, claim_key(&kept, &other)?);
                self.call(CompleteUploadState::ReadDedupClaim, read)
            }
            (CompleteUploadState::ReadDedupClaim, None) => {
                let kept = self.final_location.clone();
                let kept = kept.ok_or(CompleteUploadError::CompleteUploadFailed)?;
                self.reuse(written, kept)
            }
            (CompleteUploadState::ReadDedupClaim, Some(_)) => {
                self.final_location = None;
                self.check_hash_lookup()
            }
            _ => self.check_hash_lookup(),
        }
    }

    fn reuse(&mut self, written: BackendLocation, existing: BackendLocation) -> Turn {
        self.dedup = Some(written);
        self.final_location = Some(existing);
        self.write_blob_location()
    }

    /// The release of the assembled file, committed with the version that reads the older one.
    pub(super) fn dedup_release(
        &self,
    ) -> Option<Result<(String, Key, Value), aruna_core::errors::ConversionError>> {
        let written = self.dedup.as_ref()?;
        let target = self.plain.as_ref()?;
        let work = BlobCleanupWork::ReleaseRenamed {
            location: written.clone(),
            import_id: target.import_id,
            version_id: self.version_id?,
            version: self.plain_version.clone()?,
        };
        let key = Ulid::generate().to_bytes().to_vec().into();
        Some(work.to_bytes().map(|value| {
            let space = BLOB_CLEANUP_KEYSPACE.to_string();
            (space, key, value.into())
        }))
    }
}
