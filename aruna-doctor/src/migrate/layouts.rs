//! Rows stored before distributed buckets: shapes that gained trailing fields, replication jobs
//! whose key gained the member target, and queue and index rows a new key layout replaced.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::session_refs::{self, Layout};
use super::vault::Moves;
use super::{Rewrites, decode_error, relaid, rewrite_rows};
use crate::explorer::ExplorerError;
use aruna_core::structs::storage::blob::BlobCleanupWork;
use aruna_operations::replication::queue::{BlobJobRecord, blob_job_key};
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};

/// Rows to write or keys to delete in one keyspace.
pub(super) type KeyspaceRows<'k, R = (Vec<u8>, Vec<u8>)> =
    (&'k OptimisticTxKeyspace, &'static str, Vec<R>);

/// Heads and the policy records that hold one, before the original epoch.
pub(super) const HEAD: Layout = &[("CurrentVersionPointer", 2)];
/// Node info before the download slots and the storage capacity.
pub(super) const NODE_INFO: Layout = &[("NodeUtilization", 4)];
/// Source connectors before the namespace.
pub(super) const CONNECTOR: Layout = &[("SourceConnector", 8)];
/// Live replication obligations before the member publications.
pub(super) const OBLIGATION: Layout = &[("LiveReplicationContinuation", 2)];
/// Replication jobs before the member target.
const REPLICATION_JOB: Layout = &[("BlobJobRecord", 11)];
/// Key prefix of the reference refresh jobs before one job per version.
const OLD_REFRESH: &[u8] = b"v1";
/// IRI index keys before the document cursor moved into the value.
const OLD_IRI_KEY: usize = 96;

/// Rows of `T` stored in `layout`, which is its only older shape, with their keyspace.
pub(super) fn padded<'k, T>(
    db: &OptimisticTxDatabase,
    keyspace: &'k OptimisticTxKeyspace,
    name: &'static str,
    layout: Layout,
) -> Result<KeyspaceRows<'k>, ExplorerError>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    let rows = rewrite_rows(db, keyspace, name, |value| relaid::<T, T>(value, layout))?;
    Ok((keyspace, name, rows.rows))
}

/// Replication jobs before the member target, moved to the key their record now has.
pub(super) fn replication_jobs(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    name: &str,
) -> Result<Moves, ExplorerError> {
    let mut moves = Moves {
        scanned: 0,
        rows: Vec::new(),
        removed: Vec::new(),
    };
    for entry in db.read_tx().iter(keyspace) {
        let (key, value) = entry.into_inner()?;
        moves.scanned += 1;
        if BlobJobRecord::from_bytes(&value).is_ok() {
            continue;
        }
        let record = session_refs::decode::<BlobJobRecord>(&value, REPLICATION_JOB)
            .map_err(|error| decode_error(name, &key, error))?;
        let moved = blob_job_key(&record).map_err(|error| decode_error(name, &key, error))?;
        let bytes =
            postcard::to_allocvec(&record).map_err(|error| decode_error(name, &key, error))?;
        if moved.as_ref() != key.as_ref() {
            moves.removed.push(key.to_vec());
        }
        moves.rows.push((moved.to_vec(), bytes));
    }
    Ok(moves)
}

/// Uncertain writes of a blob from before owners named whether the copy is announced in the
/// DHT. Every copy was announced then, so the owner gains `true` as its last value.
pub(super) fn cleanup_rows(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
) -> Result<Rewrites, ExplorerError> {
    let mut rows = Vec::new();
    let mut scanned = 0;
    for entry in db.read_tx().iter(keyspace) {
        let (key, value) = entry.into_inner()?;
        scanned += 1;
        if BlobCleanupWork::from_bytes(&value).is_ok() {
            continue;
        }
        let announced = [value.as_ref(), &[1]].concat();
        let Ok(work @ BlobCleanupWork::ReconcileWrite { .. }) =
            BlobCleanupWork::from_bytes(&announced)
        else {
            continue;
        };
        if work.to_bytes().is_ok_and(|bytes| bytes == announced) {
            rows.push((key.to_vec(), announced));
        }
    }
    Ok(Rewrites { scanned, rows })
}

/// Reference refresh jobs keyed by their refresh time; the queue now keeps one per version
/// behind a due index, so these are never read again.
pub(super) fn old_refreshes(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
) -> Result<Vec<Vec<u8>>, ExplorerError> {
    let mut keys = Vec::new();
    for entry in db.read_tx().prefix(keyspace, OLD_REFRESH) {
        keys.push(entry.into_inner()?.0.to_vec());
    }
    Ok(keys)
}

/// IRI index rows in the old key layout; the index rebuild at startup writes them anew.
pub(super) fn old_iri_rows(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
) -> Result<Vec<Vec<u8>>, ExplorerError> {
    let mut keys = Vec::new();
    for entry in db.read_tx().iter(keyspace) {
        let key = entry.into_inner()?.0;
        if key.len() == OLD_IRI_KEY {
            keys.push(key.to_vec());
        }
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::super::migrate_output;
    use super::super::tests::{read, write};
    use super::*;
    use aruna_core::UserId;
    use aruna_core::keyspaces::{
        BLOB_CLEANUP_KEYSPACE, BLOB_HEAD_KEYSPACE, IRI_INDEX_KEYSPACE, REFRESH_JOB_KEYSPACE,
        REPLICATION_JOB_KEYSPACE,
    };
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::{
        BackendLocation, BackendRef, CurrentVersionPointer, WriteOwner,
    };
    use aruna_core::structs::storage::format::StoredFormat;
    use aruna_operations::replication::protocol::ReplicationMode;
    use aruna_operations::replication::version_replication::{
        ReplicateScopeInput, ReplicateScopeTarget,
    };
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    #[test]
    fn relays_distributed_rows() {
        // Each row stored before distributed buckets becomes its current encoding, a
        // replication job moves to its current key, and replaced queue and index rows go.
        let realm_id = RealmId([2; 32]);
        let head = CurrentVersionPointer::new(Ulid::from(1))
            .to_bytes()
            .unwrap();
        let job = BlobJobRecord::new(
            ReplicateScopeInput {
                bucket: "data".to_string(),
                target: ReplicateScopeTarget::Bucket,
                target_node_id: iroh::SecretKey::from_bytes(&[3; 32]).public(),
                auth_context: AuthContext {
                    user_id: UserId::nil(realm_id),
                    realm_id,
                    path_restrictions: None,
                    session: None,
                },
                replicate_delete_markers: true,
                mode: ReplicationMode::Live,
            },
            None,
            5,
        );
        let job_key = blob_job_key(&job).unwrap().to_vec();
        let job_bytes = job.to_bytes().unwrap();
        let work = BlobCleanupWork::ReconcileWrite {
            location: BackendLocation {
                backend: BackendRef::node_default(),
                storage_class: None,
                root: "/data".to_string(),
                storage_bucket: "b".to_string(),
                backend_path: "k".to_string(),
                ulid: Ulid::from(5),
                format: StoredFormat::default(),
                created_by: UserId::default(),
                created_at: SystemTime::UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 4,
                hashes: HashMap::new(),
            },
            owner: WriteOwner::Blob {
                blake3: [4; 32],
                realm_id,
                ttl_ms: 9,
                announce: true,
            },
        }
        .to_bytes()
        .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("db");
        write(
            &path,
            BLOB_HEAD_KEYSPACE,
            vec![(b"head", head[..head.len() - 1].to_vec())],
        );
        let old_key = &job_key[..job_key.len() - 1];
        let old_job = job_bytes[..job_bytes.len() - 1].to_vec();
        write(&path, REPLICATION_JOB_KEYSPACE, vec![(old_key, old_job)]);
        write(
            &path,
            BLOB_CLEANUP_KEYSPACE,
            vec![(b"work", work[..work.len() - 1].to_vec())],
        );
        let refreshes = vec![
            (b"v1job".as_slice(), vec![1]),
            (b"v2job".as_slice(), vec![2]),
        ];
        write(&path, REFRESH_JOB_KEYSPACE, refreshes);
        let iri = vec![
            ([7; 96].as_slice(), Vec::new()),
            ([8; 80].as_slice(), vec![9]),
        ];
        write(&path, IRI_INDEX_KEYSPACE, iri);

        let output = migrate_output(path.to_str().unwrap()).unwrap();

        assert_eq!(read(&path, BLOB_HEAD_KEYSPACE)[b"head".as_slice()], head);
        let jobs = read(&path, REPLICATION_JOB_KEYSPACE);
        assert_eq!(
            jobs.into_iter().collect::<Vec<_>>(),
            vec![(job_key, job_bytes)]
        );
        assert_eq!(read(&path, BLOB_CLEANUP_KEYSPACE)[b"work".as_slice()], work);
        let refreshes = read(&path, REFRESH_JOB_KEYSPACE);
        assert_eq!(refreshes.keys().collect::<Vec<_>>(), vec![b"v2job"]);
        let iri = read(&path, IRI_INDEX_KEYSPACE);
        assert_eq!(iri.keys().map(Vec::len).collect::<Vec<_>>(), vec![80]);
        assert_eq!(output.relaid_rows.values().sum::<usize>(), 3);
        assert_eq!(output.stale_rows_deleted.values().sum::<usize>(), 3);
        let again = migrate_output(path.to_str().unwrap()).unwrap();
        assert!(again.relaid_rows.is_empty() && again.stale_rows_deleted.is_empty());
    }
}
