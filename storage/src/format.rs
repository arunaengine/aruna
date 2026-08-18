use aruna_core::keyspaces::STORAGE_FORMAT_KEYSPACE;
use aruna_core::storage_format::{STORAGE_FORMAT_KEY, StorageFormatError, StorageFormatMarker};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, PersistMode, Readable};
use tracing::info;

use crate::errors::StorageLibError;

/// Admits one database root: a fresh root is stamped with the current format
/// marker, a matching root passes, and any other epoch fails here, before the
/// first record is read.
pub fn ensure_format(db: &OptimisticTxDatabase, root: &str) -> Result<(), StorageLibError> {
    // Any keyspace besides the marker means the root already holds records, so
    // a missing marker identifies a predecessor root rather than a fresh one.
    let populated = db
        .list_keyspace_names()
        .iter()
        .any(|name| name.as_ref() != STORAGE_FORMAT_KEYSPACE);
    let keyspace = db.keyspace(STORAGE_FORMAT_KEYSPACE, KeyspaceCreateOptions::default)?;
    let stored = db.read_tx().get(&keyspace, STORAGE_FORMAT_KEY)?;
    if StorageFormatMarker::verify(stored.as_deref())?.is_some() {
        return Ok(());
    }
    if populated {
        return Err(StorageFormatError::Unmarked.into());
    }

    let marker = StorageFormatMarker::default();
    keyspace.insert(STORAGE_FORMAT_KEY, marker.to_bytes()?)?;
    db.persist(PersistMode::SyncAll)?;
    info!(
        root,
        epoch = marker.epoch,
        "Initialized the storage format marker on a fresh database root"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ensure_format;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::STORAGE_FORMAT_KEYSPACE;
    use aruna_core::storage_format::{
        STORAGE_FORMAT_EPOCH, STORAGE_FORMAT_KEY, StorageFormatMarker,
    };
    use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, PersistMode, Readable};
    use tempfile::tempdir;

    fn open(path: &std::path::Path) -> OptimisticTxDatabase {
        OptimisticTxDatabase::builder(path)
            .open()
            .expect("database opens")
    }

    #[test]
    fn stamps_fresh_root() {
        let dir = tempdir().expect("temp dir");
        let db = open(dir.path());

        ensure_format(&db, "fresh").expect("fresh root is admitted");

        let keyspace = db
            .keyspace(STORAGE_FORMAT_KEYSPACE, KeyspaceCreateOptions::default)
            .expect("marker keyspace");
        let stored = db
            .read_tx()
            .get(&keyspace, STORAGE_FORMAT_KEY)
            .expect("marker reads");
        assert_eq!(
            StorageFormatMarker::verify(stored.as_deref()),
            Ok(Some(StorageFormatMarker::default()))
        );
    }

    #[test]
    fn admits_matching_root() {
        let dir = tempdir().expect("temp dir");
        let db = open(dir.path());

        ensure_format(&db, "first").expect("fresh root is admitted");
        ensure_format(&db, "second").expect("matching root is admitted");
    }

    #[test]
    fn rejects_other_epoch() {
        // A root from another format generation must fail before any record is
        // decoded, so the marker is the only row this root ever holds.
        let dir = tempdir().expect("temp dir");
        let db = open(dir.path());
        let keyspace = db
            .keyspace(STORAGE_FORMAT_KEYSPACE, KeyspaceCreateOptions::default)
            .expect("marker keyspace");
        let older = StorageFormatMarker {
            epoch: STORAGE_FORMAT_EPOCH - 1,
            ..StorageFormatMarker::default()
        };
        keyspace
            .insert(
                STORAGE_FORMAT_KEY,
                older.to_bytes().expect("marker encodes"),
            )
            .expect("marker writes");

        assert!(ensure_format(&db, "stale").is_err());
    }

    #[test]
    fn rejects_unmarked_root() {
        // A predecessor root carries records but no marker; stamping it would
        // silently adopt bytes this build cannot decode.
        let dir = tempdir().expect("temp dir");
        let db = open(dir.path());
        let records = db
            .keyspace("s3_buckets", KeyspaceCreateOptions::default)
            .expect("record keyspace");
        records
            .insert(b"bucket".as_slice(), b"record".as_slice())
            .expect("record writes");

        assert!(ensure_format(&db, "predecessor").is_err());
    }

    #[test]
    fn startup_rejects_stale() {
        // Startup opens through FjallStorage, so a stale root must fail there
        // rather than at the first record read.
        let dir = tempdir().expect("temp dir");
        let path = dir.path().to_str().expect("utf-8 path").to_string();
        {
            let db = open(dir.path());
            let keyspace = db
                .keyspace(STORAGE_FORMAT_KEYSPACE, KeyspaceCreateOptions::default)
                .expect("marker keyspace");
            let older = StorageFormatMarker {
                epoch: STORAGE_FORMAT_EPOCH - 1,
                ..StorageFormatMarker::default()
            };
            keyspace
                .insert(
                    STORAGE_FORMAT_KEY,
                    older.to_bytes().expect("marker encodes"),
                )
                .expect("marker writes");
            db.persist(PersistMode::SyncAll).expect("marker persists");
        }

        assert!(crate::FjallStorage::open(&path).is_err());
    }

    #[tokio::test]
    async fn startup_stamps_fresh() {
        let dir = tempdir().expect("temp dir");
        let handle = crate::FjallStorage::open(dir.path().to_str().expect("utf-8 path"))
            .expect("fresh root opens");

        let event = handle
            .send_storage_effect(StorageEffect::Read {
                key_space: STORAGE_FORMAT_KEYSPACE.to_string(),
                key: STORAGE_FORMAT_KEY.to_vec().into(),
                txn_id: None,
            })
            .await;

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            panic!("expected a marker read result, got {event:?}");
        };
        assert_eq!(
            StorageFormatMarker::verify(value.as_deref()),
            Ok(Some(StorageFormatMarker::default()))
        );
    }
}
