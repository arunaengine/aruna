use crate::error::CliError;
use crate::explorer::ExplorerError;
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_RECLAIM_KEYSPACE,
};
use aruna_core::structs::{
    BackendLocation, BackendRef, BlobCleanupWork, BlobLocationKey, ReclaimCandidate,
    ReclaimCandidateKey,
};
use chrono::{DateTime, Utc};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, Readable};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::SystemTime;
use ulid::Ulid;

#[derive(Debug, Serialize)]
pub struct SeedOutput {
    pub database_path: String,
    pub backend: String,
    pub scanned: usize,
    pub queued: usize,
}

#[derive(Debug, Default, Serialize)]
pub struct BackendReclaim {
    pub candidates: usize,
    pub queued_cleanups: usize,
    pub oldest_enqueued_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct ReclaimStatusOutput {
    pub database_path: String,
    pub backends: BTreeMap<String, BackendReclaim>,
}

/// Queues every stored copy on one backend. Only for garbage that predates a
/// switch to reclaim: deletes queue their own candidates.
pub async fn seed_backend(database_path: String, backend: String) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking({
        let database_path = database_path.clone();
        move || seed_output(&database_path, &backend)
    })
    .await
    .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

pub async fn print_status(database_path: String) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking({
        let database_path = database_path.clone();
        move || status_output(&database_path)
    })
    .await
    .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn fold_oldest(oldest: &mut Option<DateTime<Utc>>, time: SystemTime) {
    let time = DateTime::<Utc>::from(time);
    *oldest = Some(oldest.map_or(time, |current| current.min(time)));
}

fn parse_backend(backend: &str) -> Result<BackendRef, ExplorerError> {
    BackendRef::from_key_bytes(backend.as_bytes())
        .map_err(|error| ExplorerError::Decode(error.to_string()))
}

fn seed_output(database_path: &str, backend: &str) -> Result<SeedOutput, ExplorerError> {
    let wanted = parse_backend(backend)?;
    let db = OptimisticTxDatabase::builder(Path::new(database_path)).open()?;
    let locations = db.keyspace(BLOB_LOCATIONS_KEYSPACE, KeyspaceCreateOptions::default)?;
    let candidates = db.keyspace(BLOB_RECLAIM_KEYSPACE, KeyspaceCreateOptions::default)?;

    let enqueued_at = SystemTime::now();
    let mut scanned = 0;
    let mut queued = Vec::new();
    for entry in db.read_tx().iter(&locations) {
        let (key, value) = entry.into_inner()?;
        let location = BackendLocation::from_bytes(value.as_ref())
            .map_err(|error| ExplorerError::Decode(error.to_string()))?;
        scanned += 1;
        if location.backend != wanted || location.staging || location.partial {
            continue;
        }
        let hash = BlobLocationKey::from_bytes(key.as_ref())
            .map_err(|error| ExplorerError::Decode(error.to_string()))?
            .blake3_hash;
        queued.push(ReclaimCandidateKey::new(wanted.clone(), hash).to_bytes());
    }

    let value = ReclaimCandidate { enqueued_at }
        .to_bytes()
        .map_err(|error| ExplorerError::Decode(error.to_string()))?;
    let mut txn = db.write_tx()?;
    for key in &queued {
        txn.insert(candidates.clone(), key.clone(), value.clone());
    }
    txn.commit()?.map_err(|_| {
        ExplorerError::Decode("reclaim seeding conflicted with a running node".to_string())
    })?;

    Ok(SeedOutput {
        database_path: database_path.to_string(),
        backend: wanted.to_string(),
        scanned,
        queued: queued.len(),
    })
}

/// Queue depth and stuck physical deletes per backend. A backend whose
/// `oldest_enqueued_at` never advances is the reclaim-blocked signal.
fn status_output(database_path: &str) -> Result<ReclaimStatusOutput, ExplorerError> {
    let db = OptimisticTxDatabase::builder(Path::new(database_path)).open()?;
    let mut backends: BTreeMap<String, BackendReclaim> = BTreeMap::new();

    let candidates = db.keyspace(BLOB_RECLAIM_KEYSPACE, KeyspaceCreateOptions::default)?;
    for entry in db.read_tx().iter(&candidates) {
        let (key, value) = entry.into_inner()?;
        let Ok(key) = ReclaimCandidateKey::from_bytes(key.as_ref()) else {
            continue;
        };
        let row = backends.entry(key.backend.to_string()).or_default();
        row.candidates += 1;
        if let Ok(candidate) = ReclaimCandidate::from_bytes(value.as_ref()) {
            fold_oldest(&mut row.oldest_enqueued_at, candidate.enqueued_at);
        }
    }

    let cleanups = db.keyspace(BLOB_CLEANUP_KEYSPACE, KeyspaceCreateOptions::default)?;
    for entry in db.read_tx().iter(&cleanups) {
        let (key, value) = entry.into_inner()?;
        if let Ok(
            BlobCleanupWork::DeleteBlob { location }
            | BlobCleanupWork::ReconcileWrite { location, .. },
        ) = BlobCleanupWork::from_bytes(value.as_ref())
        {
            let row = backends.entry(location.backend.to_string()).or_default();
            row.queued_cleanups += 1;
            // Cleanup keys are generated ULIDs, so a stalled physical delete
            // dates itself even after its candidate row is gone.
            if let Ok(bytes) = <[u8; 16]>::try_from(key.as_ref()) {
                fold_oldest(
                    &mut row.oldest_enqueued_at,
                    Ulid::from_bytes(bytes).datetime(),
                );
            }
        }
    }

    Ok(ReclaimStatusOutput {
        database_path: database_path.to_string(),
        backends,
    })
}

#[cfg(test)]
mod tests {
    use super::{seed_output, status_output};
    use aruna_core::keyspaces::{BLOB_LOCATIONS_KEYSPACE, BLOB_RECLAIM_KEYSPACE};
    use aruna_core::structs::{BackendLocation, BackendRef, BlobLocationKey};
    use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, Readable};
    use std::collections::HashMap;
    use std::path::Path;
    use std::time::SystemTime;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn location(backend: BackendRef, staging: bool) -> BackendLocation {
        BackendLocation {
            backend,
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "storage".to_string(),
            backend_path: "bucket/key_01".to_string(),
            ulid: Ulid::from_bytes([1u8; 16]),
            compressed: false,
            encrypted: false,
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging,
            partial: false,
            blob_size: 10,
            hashes: HashMap::new(),
        }
    }

    #[test]
    fn seeds_one_backend() {
        // Copies on other backends and staged copies stay out of the queue.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        {
            let db = OptimisticTxDatabase::builder(Path::new(&path))
                .open()
                .unwrap();
            let locations = db
                .keyspace(BLOB_LOCATIONS_KEYSPACE, KeyspaceCreateOptions::default)
                .unwrap();
            let mut txn = db.write_tx().unwrap();
            for (seed, backend, staging) in [
                (1u8, BackendRef::node_default(), false),
                (2u8, BackendRef::Node("cold".to_string()), false),
                (3u8, BackendRef::node_default(), true),
            ] {
                txn.insert(
                    locations.clone(),
                    BlobLocationKey::new([seed; 32], location(backend.clone(), staging).backend)
                        .to_bytes(),
                    location(backend, staging).to_bytes().unwrap(),
                );
            }
            let _ = txn.commit().unwrap();
        }

        let output = seed_output(path.to_str().unwrap(), "n:default").unwrap();

        assert_eq!(output.scanned, 3);
        assert_eq!(output.queued, 1);

        let status = status_output(path.to_str().unwrap()).unwrap();
        let row = status.backends.get("node:default").unwrap();
        assert_eq!(row.candidates, 1);
        assert_eq!(row.queued_cleanups, 0);
        assert!(row.oldest_enqueued_at.is_some());

        let db = OptimisticTxDatabase::builder(Path::new(&path))
            .open()
            .unwrap();
        let candidates = db
            .keyspace(BLOB_RECLAIM_KEYSPACE, KeyspaceCreateOptions::default)
            .unwrap();
        assert_eq!(db.read_tx().iter(&candidates).count(), 1);
    }

    #[test]
    fn rejects_unknown_backend() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");

        assert!(seed_output(path.to_str().unwrap(), "cold").is_err());
    }
}
