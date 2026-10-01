//! Deletes multipart uploads the current format cannot read, with their part records, and
//! queues the stored part blobs for deletion.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::explorer::ExplorerError;
use aruna_core::structs::storage::blob::{BackendLocation, BlobCleanupWork};
use aruna_core::structs::storage::multipart::{MultipartPart, MultipartPartKey, MultipartUpload};
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::time::SystemTime;
use ulid::Ulid;

/// Upload and part rows to remove, and a delete row per stored part blob: the reconcile rows
/// that once named those blobs may have drained already.
pub(super) struct UploadCleanup {
    pub(super) scanned: usize,
    pub(super) uploads: Vec<Vec<u8>>,
    pub(super) parts: Vec<Vec<u8>>,
    pub(super) blob_deletes: Vec<(Vec<u8>, Vec<u8>)>,
}

/// A part record from before parts kept their provider ETag.
#[derive(Deserialize)]
struct LegacyPart {
    _part_number: u16,
    location: BackendLocation,
    _created_at: SystemTime,
}

/// The part's own blob; an in-place part has none, its provider upload held the bytes.
fn part_blob(value: &[u8]) -> Option<BackendLocation> {
    let location = match MultipartPart::from_bytes(value) {
        Ok(part) => part.location,
        Err(_) => postcard::from_bytes::<LegacyPart>(value).ok()?.location,
    };
    (!location.partial).then_some(location)
}

pub(super) fn stale_uploads(
    db: &OptimisticTxDatabase,
    uploads: &OptimisticTxKeyspace,
    parts: &OptimisticTxKeyspace,
) -> Result<UploadCleanup, ExplorerError> {
    let read = db.read_tx();
    let mut cleanup = UploadCleanup {
        scanned: 0,
        uploads: Vec::new(),
        parts: Vec::new(),
        blob_deletes: Vec::new(),
    };
    let mut live = BTreeSet::new();
    for entry in read.iter(uploads) {
        let (key, value) = entry.into_inner()?;
        cleanup.scanned += 1;
        match MultipartUpload::from_bytes(&value) {
            Ok(upload) if upload.upload_id.to_bytes() == key.as_ref() => {
                live.insert(upload.upload_id);
            }
            _ => cleanup.uploads.push(key.to_vec()),
        }
    }
    for entry in read.iter(parts) {
        let (key, value) = entry.into_inner()?;
        let owned = MultipartPartKey::from_bytes(&key)
            .is_ok_and(|part| live.contains(&part.upload_id))
            && MultipartPart::from_bytes(&value).is_ok();
        if !owned {
            cleanup.parts.push(key.to_vec());
            if let Some(location) = part_blob(&value) {
                let row = BlobCleanupWork::DeleteBlob { location }
                    .to_bytes()
                    .map_err(|error| ExplorerError::Decode(error.to_string()))?;
                cleanup
                    .blob_deletes
                    .push((Ulid::generate().to_bytes().to_vec(), row));
            }
        }
    }
    Ok(cleanup)
}

#[cfg(test)]
mod tests {
    use super::super::migrate_output;
    use super::super::tests::{read, write};
    use aruna_core::UserId;
    use aruna_core::keyspaces::{BLOB_CLEANUP_KEYSPACE, UPLOAD_KEYSPACE, UPLOAD_PART_KEYSPACE};
    use aruna_core::structs::storage::blob::{BackendLocation, BackendRef, BlobCleanupWork};
    use aruna_core::structs::storage::format::StoredFormat;
    use aruna_core::structs::storage::multipart::{
        MultipartPartKey, MultipartUpload, MultipartUploadStatus,
    };
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    #[test]
    fn deletes_old_uploads() {
        let (live, old) = (Ulid::from(1), Ulid::from(2));
        let upload = MultipartUpload {
            upload_id: live,
            backend: BackendRef::node_default(),
            storage_class: None,
            bucket: "bucket".to_string(),
            key: "object".to_string(),
            group_id: Ulid::from(3),
            created_by: UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            status: MultipartUploadStatus::Open,
            checksum_hint: None,
            metadata: HashMap::new(),
            placement_policies: Vec::new(),
            subject_generation: 0,
            completing_since_ms: None,
            backend_upload: None,
        };
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().join("db");
        let database = path.to_str().expect("path");
        // Rows from before the format change no longer decode.
        write(
            &path,
            UPLOAD_KEYSPACE,
            vec![
                (&live.to_bytes(), upload.to_bytes().expect("encodes")),
                (&old.to_bytes(), vec![1, 2, 3]),
            ],
        );
        let part_key = |upload_id, number| {
            MultipartPartKey::new(upload_id, number)
                .to_bytes()
                .expect("encodes")
        };
        let (live_part, old_part) = (part_key(live, 1), part_key(old, 1));
        let blob = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "parts".to_string(),
            backend_path: "_parts/old/00001".to_string(),
            ulid: Ulid::from(5),
            format: StoredFormat::default(),
            created_by: UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 4,
            hashes: HashMap::new(),
        };
        // The layout before parts kept their provider ETag.
        let legacy =
            postcard::to_allocvec(&(1u16, blob.clone(), SystemTime::UNIX_EPOCH)).expect("encodes");
        write(
            &path,
            UPLOAD_PART_KEYSPACE,
            vec![(&live_part, vec![4, 5]), (&old_part, legacy)],
        );

        let output = migrate_output(database).expect("migration runs");

        assert_eq!((output.uploads_scanned, output.uploads_deleted), (2, 1));
        // An unreadable part of a readable upload goes too.
        assert_eq!(output.upload_parts_deleted, 2);
        let uploads = read(&path, UPLOAD_KEYSPACE);
        assert_eq!(uploads.keys().collect::<Vec<_>>(), [&live.to_bytes()]);
        assert!(read(&path, UPLOAD_PART_KEYSPACE).is_empty());
        // The old part's blob is queued even if its reconcile row drained long ago.
        assert_eq!(output.upload_blobs_queued, 1);
        let queued: Vec<_> = read(&path, BLOB_CLEANUP_KEYSPACE)
            .into_values()
            .map(|value| BlobCleanupWork::from_bytes(&value).expect("row decodes"))
            .collect();
        assert_eq!(queued, [BlobCleanupWork::DeleteBlob { location: blob }]);
        let again = migrate_output(database).expect("migration repeats");
        assert_eq!(
            (
                again.uploads_deleted,
                again.upload_parts_deleted,
                again.upload_blobs_queued
            ),
            (0, 0, 0)
        );
    }
}
