//! Deletes multipart uploads the current format cannot read, with their part records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::explorer::ExplorerError;
use aruna_core::structs::storage::multipart::{MultipartPart, MultipartPartKey, MultipartUpload};
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use std::collections::BTreeSet;

/// Upload and part rows to remove. The cleanup drain then deletes the blobs of removed parts,
/// whose reconcile rows name the part record as owner.
pub(super) struct UploadCleanup {
    pub(super) scanned: usize,
    pub(super) uploads: Vec<Vec<u8>>,
    pub(super) parts: Vec<Vec<u8>>,
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
        }
    }
    Ok(cleanup)
}

#[cfg(test)]
mod tests {
    use super::super::migrate_output;
    use super::super::tests::{read, write};
    use aruna_core::UserId;
    use aruna_core::keyspaces::{UPLOAD_KEYSPACE, UPLOAD_PART_KEYSPACE};
    use aruna_core::structs::storage::blob::BackendRef;
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
        write(
            &path,
            UPLOAD_PART_KEYSPACE,
            vec![(&live_part, vec![4, 5]), (&old_part, vec![6])],
        );

        let output = migrate_output(database).expect("migration runs");

        assert_eq!((output.uploads_scanned, output.uploads_deleted), (2, 1));
        // An unreadable part of a readable upload goes too.
        assert_eq!(output.upload_parts_deleted, 2);
        let uploads = read(&path, UPLOAD_KEYSPACE);
        assert_eq!(uploads.keys().collect::<Vec<_>>(), [&live.to_bytes()]);
        assert!(read(&path, UPLOAD_PART_KEYSPACE).is_empty());
        let again = migrate_output(database).expect("migration repeats");
        assert_eq!((again.uploads_deleted, again.upload_parts_deleted), (0, 0));
    }
}
