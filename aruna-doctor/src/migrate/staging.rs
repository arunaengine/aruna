//! Re-encodes staging jobs from before a job named its destination bucket's identity, and
//! their checkpoints from before the durable directory queue.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{Rewrites, decode_error};
use crate::explorer::ExplorerError;
use aruna_core::structs::execution::job::{
    StagingJobCheckpoint, StagingJobError, StagingJobItem, StagingJobPhase, StagingJobPrefix,
    StagingJobSpec,
};
use aruna_core::structs::execution::staging::StagingStrategy;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::{BucketIdentity, BucketInfo};
use aruna_core::types::GroupId;
use aruna_core::{NodeId, UserId};
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::SystemTime;
use ulid::Ulid;

/// Previous shape of `StagingJobSpec`, before the bucket identity and the checksum manifest.
#[derive(Serialize, Deserialize)]
pub(super) struct LegacyStaging {
    pub(super) auth_context: AuthContext,
    pub(super) group_id: GroupId,
    pub(super) node_id: NodeId,
    pub(super) connector_id: Ulid,
    pub(super) bucket: String,
    pub(super) strategy: StagingStrategy,
    pub(super) items: Vec<StagingJobItem>,
    pub(super) prefixes: Vec<StagingJobPrefix>,
}

/// The identity is a placeholder that matches no bucket; `bucket_identity` sets the real one.
impl From<LegacyStaging> for StagingJobSpec {
    fn from(old: LegacyStaging) -> Self {
        Self {
            bucket_identity: (old.group_id, SystemTime::UNIX_EPOCH, UserId::default()),
            auth_context: old.auth_context,
            group_id: old.group_id,
            node_id: old.node_id,
            connector_id: old.connector_id,
            bucket: old.bucket,
            strategy: old.strategy,
            items: old.items,
            prefixes: old.prefixes,
            checksum_manifest: None,
        }
    }
}

/// Local bucket identities by name, reading `rewrites` over the stored rows.
pub(super) fn identities(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    rewrites: &[(Vec<u8>, Vec<u8>)],
    name: &str,
) -> Result<BTreeMap<String, BucketIdentity>, ExplorerError> {
    let rewrites: BTreeMap<&[u8], &[u8]> = rewrites
        .iter()
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect();
    let mut identities = BTreeMap::new();
    for entry in db.read_tx().iter(keyspace) {
        let (key, stored) = entry.into_inner()?;
        let value = rewrites.get(key.as_ref()).copied().unwrap_or(&stored);
        let bucket =
            BucketInfo::from_bytes(value).map_err(|error| decode_error(name, &key, error))?;
        identities.insert(
            String::from_utf8_lossy(&key).into_owned(),
            bucket.identity(),
        );
    }
    Ok(identities)
}

/// The identity of the local bucket a migrated job writes to. Without one the job keeps the
/// placeholder, so a running job ends as it would for a bucket recreated under its name.
pub(super) fn bucket_identity(
    spec: &mut StagingJobSpec,
    buckets: &BTreeMap<String, BucketIdentity>,
) {
    if let Some(identity) = buckets.get(&spec.bucket) {
        spec.bucket_identity = *identity;
    }
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
struct LegacyItem {
    _source_path: String,
    _target_key: String,
    _size: Option<u64>,
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
struct LegacyDirectory {
    _source_path: String,
    _target_prefix: String,
    _offset: usize,
}

/// Previous shape of `StagingJobCheckpoint`, which kept its pending work inline.
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
struct LegacyCheckpoint {
    phase: StagingJobPhase,
    _pending_items: Vec<LegacyItem>,
    _pending_directories: Vec<LegacyDirectory>,
    items_current: u64,
    items_succeeded: u64,
    items_failed: u64,
    items_total: Option<u64>,
    bytes_current: u64,
    bytes_total: Option<u64>,
    bytes_discovered: u64,
    _unknown_sizes: u64,
    current_path: Option<String>,
    errors: Vec<StagingJobError>,
}

impl From<LegacyCheckpoint> for StagingJobCheckpoint {
    fn from(old: LegacyCheckpoint) -> Self {
        Self {
            phase: old.phase,
            flat: None,
            items_done: 0,
            prefixes_done: 0,
            walk: None,
            manifest: None,
            queue_head: 0,
            queue_tail: 0,
            items_current: old.items_current,
            items_succeeded: old.items_succeeded,
            items_failed: old.items_failed,
            items_total: old.items_total,
            bytes_current: old.bytes_current,
            bytes_total: old.bytes_total,
            bytes_discovered: old.bytes_discovered,
            current_path: old.current_path,
            errors: old.errors,
        }
    }
}

/// Checkpoints to rewrite and to remove.
pub(super) struct Checkpoints {
    pub(super) rewrites: Rewrites,
    pub(super) restarted: Vec<Vec<u8>>,
}

/// Finished checkpoints keep their counts. The pending work of an unfinished one has no place
/// in the new queue, so its row is removed and the job starts its walk again.
pub(super) fn checkpoint_rows(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    name: &str,
) -> Result<Checkpoints, ExplorerError> {
    let mut rows = Vec::new();
    let mut restarted = Vec::new();
    let mut scanned = 0;
    for entry in db.read_tx().iter(keyspace) {
        let (key, value) = entry.into_inner()?;
        scanned += 1;
        if postcard::from_bytes::<StagingJobCheckpoint>(&value)
            .is_ok_and(|current| postcard::to_allocvec(&current).is_ok_and(|bytes| bytes == *value))
        {
            continue;
        }
        let (old, rest) = postcard::take_from_bytes::<LegacyCheckpoint>(&value)
            .map_err(|error| decode_error(name, &key, error))?;
        if !rest.is_empty() {
            return Err(decode_error(name, &key, "unknown checkpoint shape"));
        }
        if !matches!(
            old.phase,
            StagingJobPhase::Completed | StagingJobPhase::Failed
        ) {
            restarted.push(key.to_vec());
            continue;
        }
        let bytes = postcard::to_allocvec(&StagingJobCheckpoint::from(old))
            .map_err(|error| decode_error(name, &key, error))?;
        rows.push((key.to_vec(), bytes));
    }
    Ok(Checkpoints {
        rewrites: Rewrites { scanned, rows },
        restarted,
    })
}

#[cfg(test)]
mod tests {
    use super::super::migrate_output;
    use super::super::tests::{read, write};
    use super::*;
    use aruna_core::keyspaces::{JOB_KEYSPACE, S3_BUCKET_KEYSPACE, STAGING_STATE_KEYSPACE};
    use aruna_core::structs::execution::job::{JobId, JobPayload, JobRecord, job_record_key};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::format::Compression;
    use std::time::Duration;

    fn checkpoint(phase: StagingJobPhase) -> LegacyCheckpoint {
        LegacyCheckpoint {
            phase,
            _pending_items: Vec::new(),
            _pending_directories: Vec::new(),
            items_current: 2,
            items_succeeded: 1,
            items_failed: 1,
            items_total: Some(2),
            bytes_current: 8,
            bytes_total: None,
            bytes_discovered: 8,
            _unknown_sizes: 0,
            current_path: None,
            errors: Vec::new(),
        }
    }

    #[test]
    fn rewrites_staging_jobs() {
        // A staging job takes its local bucket's identity and the plan digest of its new spec;
        // a finished checkpoint keeps its counts and an unfinished one goes, so its walk restarts.
        let realm_id = RealmId([5; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[6; 32]).public();
        let bucket = BucketInfo {
            group_id: Ulid::from(4),
            created_at: SystemTime::UNIX_EPOCH + Duration::from_secs(9),
            created_by: UserId::nil(realm_id),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        let old = LegacyStaging {
            auth_context: AuthContext {
                user_id: UserId::nil(realm_id),
                realm_id,
                path_restrictions: None,
                session: None,
            },
            group_id: Ulid::from(4),
            node_id,
            connector_id: Ulid::from(7),
            bucket: "target".to_string(),
            strategy: StagingStrategy::Reference,
            items: vec![StagingJobItem {
                source_path: "a.csv".to_string(),
                target_key: "a.csv".to_string(),
            }],
            prefixes: Vec::new(),
        };
        let old_spec = postcard::to_allocvec(&old).unwrap();
        let mut spec = StagingJobSpec::from(old);
        spec.bucket_identity = bucket.identity();
        let new_spec = postcard::to_allocvec(&spec).unwrap();
        let (done, running) = (JobId::from_bytes([1; 16]), JobId::from_bytes([2; 16]));
        let payload = JobPayload::Staging(spec);
        let mut record = JobRecord::new(done, payload, UserId::nil(realm_id), node_id, 5, 5, None);
        record.plan_digest = Some([3; 32]);
        let bytes = record.to_bytes().unwrap();
        let at = bytes
            .windows(new_spec.len())
            .position(|part| part == new_spec)
            .unwrap();
        let stored = [&bytes[..at], &old_spec, &bytes[at + new_spec.len()..]].concat();
        record.plan_digest = Some(record.payload.plan_digest());
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("db");
        write(
            &path,
            S3_BUCKET_KEYSPACE,
            vec![(b"target", bucket.to_bytes().unwrap())],
        );
        write(&path, JOB_KEYSPACE, vec![(&job_record_key(done), stored)]);
        let finished = postcard::to_allocvec(&checkpoint(StagingJobPhase::Completed)).unwrap();
        let unfinished = postcard::to_allocvec(&checkpoint(StagingJobPhase::Writing)).unwrap();
        let rows = [
            (done.to_bytes(), finished),
            (running.to_bytes(), unfinished),
        ];
        let rows = rows
            .iter()
            .map(|(key, value)| (key.as_slice(), value.clone()))
            .collect();
        write(&path, STAGING_STATE_KEYSPACE, rows);

        let output = migrate_output(path.to_str().unwrap()).unwrap();

        assert_eq!(output.jobs_rewritten, 1);
        let jobs = read(&path, JOB_KEYSPACE);
        let migrated = JobRecord::from_bytes(&jobs[job_record_key(done).as_ref()]).unwrap();
        assert_eq!(migrated, record);
        let states = read(&path, STAGING_STATE_KEYSPACE);
        assert_eq!(states.len(), 1);
        let kept: StagingJobCheckpoint =
            postcard::from_bytes(&states[&done.to_bytes()[..]]).unwrap();
        assert_eq!(
            (kept.phase, kept.items_succeeded),
            (StagingJobPhase::Completed, 1)
        );
        let again = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!(
            (again.jobs_rewritten, again.staging_checkpoints_rewritten),
            (0, 0)
        );
    }
}
