//! Hands a range's open imports to its new coordinator, which fetches the named job specs and
//! runs them again for its own ranges once it coordinates the range.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DISTRIBUTION_KEYSPACE, JOB_KEYSPACE, NODE_STATE_KEYSPACE};
use aruna_core::structs::execution::job::{
    JobId, JobPayload, RECORD_KEY_PREFIX, StagingJobSpec, job_record_key,
};
use aruna_core::structs::storage::distribution::DistributionView;
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::jobs::JOB_RETENTION_MS;
use crate::jobs::service::submit_staging_job;
use crate::jobs::store::decode_job_record;
use crate::metadata::protocol::MetadataReadError;
use crate::s3::bucket::get::GetBucketOperation;

/// Job rows one scan page reads.
const JOB_PAGE: usize = 256;
const HANDOVER_PREFIX: &[u8] = b"import_handover/";

/// The open staging jobs of this node that import into `bucket`.
pub(crate) async fn open_imports(
    context: &DriverContext,
    bucket: &str,
) -> Result<Vec<JobId>, String> {
    let mut start = None;
    let mut jobs = Vec::new();
    loop {
        let prefix = Some(RECORD_KEY_PREFIX.to_vec());
        let (rows, next) = page(context, JOB_KEYSPACE, prefix, start, JOB_PAGE).await?;
        for (_, value) in rows {
            let Ok(record) = decode_job_record(&value) else {
                continue;
            };
            if let JobPayload::Staging(spec) = &record.payload
                && spec.bucket == bucket
                && !record.state.is_terminal()
            {
                jobs.push(record.job_id);
            }
        }
        match next {
            Some(next) => start = Some(IterStart::After(next)),
            None => return Ok(jobs),
        }
    }
}

/// The spec of a staging job of this node, for a member of the set its bucket is bound to.
pub(crate) async fn staging_spec(
    context: &DriverContext,
    peer: NodeId,
    job_id: JobId,
) -> Result<StagingJobSpec, MetadataReadError> {
    let unavailable = |_| MetadataReadError::Unavailable;
    let value = read_value(context, JOB_KEYSPACE, job_record_key(job_id))
        .await
        .map_err(unavailable)?
        .ok_or(MetadataReadError::NotFound)?;
    let record = decode_job_record(&value).map_err(|_| MetadataReadError::Unavailable)?;
    let JobPayload::Staging(spec) = record.payload else {
        return Err(MetadataReadError::NotFound);
    };
    let bucket = drive(GetBucketOperation::new(spec.bucket.clone()), context)
        .await
        .map_err(|_| MetadataReadError::NotFound)?;
    let binding = bucket.distribution.ok_or(MetadataReadError::Forbidden)?;
    let record = read_value(
        context,
        DISTRIBUTION_KEYSPACE,
        binding.set_id.to_bytes().to_vec().into(),
    )
    .await
    .map_err(unavailable)?
    .ok_or(MetadataReadError::Forbidden)?;
    let view = DistributionView::from_bytes(&record).map_err(|_| MetadataReadError::Unavailable)?;
    let members = &view.snapshot().config.members;
    if !members.iter().any(|member| member.node_id == peer) {
        return Err(MetadataReadError::Forbidden);
    }
    Ok(spec)
}

fn handover_key(set_id: Ulid, range_id: Ulid) -> Vec<u8> {
    [HANDOVER_PREFIX, &set_id.to_bytes(), &range_id.to_bytes()].concat()
}

/// Keeps the former coordinator's open jobs of a range until this node coordinates it; a
/// kept row is never replaced, so a repeated take over keeps what is left of it.
pub(crate) async fn keep_imports(
    context: &DriverContext,
    (set_id, range_id): (Ulid, Ulid),
    from: NodeId,
    jobs: &[JobId],
) -> Result<(), String> {
    let key: Key = handover_key(set_id, range_id).into();
    if read_value(context, NODE_STATE_KEYSPACE, key.clone())
        .await?
        .is_some()
    {
        return Ok(());
    }
    write_kept(context, key, &(from, jobs.to_vec())).await
}

async fn write_kept(
    context: &DriverContext,
    key: Key,
    kept: &(NodeId, Vec<JobId>),
) -> Result<(), String> {
    let value = postcard::to_allocvec(kept).map_err(|error| error.to_string())?;
    let write = StorageEffect::Write {
        key_space: NODE_STATE_KEYSPACE.to_string(),
        key,
        value: value.into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(write).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        other => Err(format!("handed over imports not kept: {other:?}")),
    }
}

/// Fetches each kept job's spec from the former coordinator and runs it here as its own job;
/// a started job leaves the row at once. Stops while the former coordinator does not answer.
async fn start_kept(
    context: &DriverContext,
    key: Key,
    (from, mut jobs): (NodeId, Vec<JobId>),
    bucket: &aruna_core::structs::storage::blob::BucketInfo,
) -> Result<(), String> {
    let Some(handle) = context.metadata_handle.as_ref() else {
        return Ok(());
    };
    let local = context.net_handle.as_ref().map(|net| net.node_id());
    let Some(local) = local else {
        return Ok(());
    };
    while let Some(job_id) = jobs.first().copied() {
        match handle.request_staging_spec(from, job_id).await {
            Ok(spec) => {
                let spec = StagingJobSpec {
                    node_id: local,
                    bucket_identity: bucket.identity(),
                    ..spec
                };
                submit_staging_job(context, spec, local, JOB_RETENTION_MS)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            // A job that finished or is gone meanwhile has nothing left to hand over.
            Err(MetadataReadError::NotFound) => {}
            Err(_) => return Ok(()),
        }
        jobs.remove(0);
        write_kept(context, key.clone(), &(from, jobs.clone())).await?;
    }
    delete(context, key).await
}

/// Starts the kept imports of every range this node now coordinates, as its own jobs; a
/// transfer that ended without this node drops them.
pub(crate) async fn continue_imports(context: &DriverContext) -> Result<(), String> {
    let Some(net) = context.net_handle.as_ref() else {
        return Ok(());
    };
    let local = net.node_id();
    let prefix = Some(HANDOVER_PREFIX.to_vec());
    let (rows, _) = page(context, NODE_STATE_KEYSPACE, prefix, None, JOB_PAGE).await?;
    for (key, value) in rows {
        let Some((set_id, range_id)) = handover_parts(&key) else {
            continue;
        };
        let set_key = set_id.to_bytes().to_vec().into();
        let record = read_value(context, DISTRIBUTION_KEYSPACE, set_key).await?;
        let view = (record.as_deref())
            .map(DistributionView::from_bytes)
            .transpose();
        let Some(view) = view.map_err(|error| error.to_string())? else {
            delete(context, key).await?;
            continue;
        };
        let snapshot = view.snapshot();
        let member = (snapshot.config.members.iter()).find(|member| member.node_id == local);
        let range = (snapshot.config.ranges.iter()).find(|range| range.range_id == range_id);
        let arriving = (snapshot.lifecycle.transfers.iter()).any(|open| {
            open.range_id == range_id && member.is_some_and(|m| open.to == m.member_id)
        });
        match (member, range) {
            (Some(member), Some(range)) if range.coordinator == member.member_id => {
                let kept = postcard::from_bytes(&value).map_err(|error| error.to_string())?;
                let bucket = &snapshot.config.bucket;
                let info = drive(GetBucketOperation::new(bucket.clone()), context)
                    .await
                    .map_err(|error| error.to_string())?;
                start_kept(context, key, kept, &info).await?;
            }
            // The transfer is still on its way here; the kept jobs wait for it.
            _ if arriving => {}
            _ => delete(context, key).await?,
        }
    }
    Ok(())
}

fn handover_parts(key: &[u8]) -> Option<(Ulid, Ulid)> {
    let rest = key.strip_prefix(HANDOVER_PREFIX)?;
    let set: [u8; 16] = rest.get(..16)?.try_into().ok()?;
    let range: [u8; 16] = rest.get(16..32)?.try_into().ok()?;
    Some((Ulid::from_bytes(set), Ulid::from_bytes(range)))
}

async fn delete(context: &DriverContext, key: Key) -> Result<(), String> {
    let delete = StorageEffect::Delete {
        key_space: NODE_STATE_KEYSPACE.to_string(),
        key,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(delete).await {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        other => Err(format!("handed over imports not dropped: {other:?}")),
    }
}

async fn read_value(
    context: &DriverContext,
    key_space: &str,
    key: Key,
) -> Result<Option<Value>, String> {
    let read = StorageEffect::Read {
        key_space: key_space.to_string(),
        key,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        other => Err(format!("unexpected read answer {other:?}")),
    }
}

async fn page(
    context: &DriverContext,
    key_space: &str,
    prefix: Option<Vec<u8>>,
    start: Option<IterStart>,
    limit: usize,
) -> Result<(Vec<(Key, Value)>, Option<Key>), String> {
    let iter = StorageEffect::Iter {
        key_space: key_space.to_string(),
        prefix: prefix.map(Into::into),
        start,
        limit,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(iter).await {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Ok((values, next_start_after)),
        other => Err(format!("unexpected page answer {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::execution::job::{JobRecord, JobState, StagingJobPrefix};
    use aruna_core::structs::execution::staging::StagingStrategy;
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;

    fn spec(bucket: &str) -> StagingJobSpec {
        let realm_id = RealmId::from_bytes([1; 32]);
        StagingJobSpec {
            auth_context: AuthContext {
                user_id: aruna_core::UserId::local(Ulid::from_bytes([2; 16]), realm_id),
                realm_id,
                path_restrictions: None,
                session: None,
            },
            group_id: Ulid::from_bytes([3; 16]),
            node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
            connector_id: Ulid::from_bytes([5; 16]),
            bucket: bucket.to_string(),
            bucket_identity: (
                Ulid::nil(),
                std::time::SystemTime::UNIX_EPOCH,
                Default::default(),
            ),
            strategy: StagingStrategy::Reference,
            items: Vec::new(),
            prefixes: vec![StagingJobPrefix {
                source_prefix: "data".to_string(),
                target_prefix: String::new(),
            }],
            checksum_manifest: None,
        }
    }

    // Only unfinished imports into the transferred bucket are handed over.
    #[tokio::test]
    async fn open_bucket_imports() {
        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let node_id = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let jobs = [
            (1, "spire", JobState::Queued),
            (2, "spire", JobState::Succeeded),
            (3, "other", JobState::Running),
        ];
        for (seed, bucket, state) in jobs {
            let job_id = JobId::from_bytes([seed; 16]);
            let payload = JobPayload::Staging(spec(bucket));
            let mut record =
                JobRecord::new(job_id, payload, Default::default(), node_id, 1, 1, None);
            record.state = state;
            crate::jobs::store::insert_job(&storage, &record)
                .await
                .unwrap();
        }
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let open = open_imports(&context, "spire").await.unwrap();
        assert_eq!(open, [JobId::from_bytes([1; 16])]);
    }
}
