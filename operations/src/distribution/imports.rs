//! Hands a range's open imports to its new coordinator, which fetches the named jobs with their
//! source roots and positions before it completes, and runs them once it coordinates the range.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DISTRIBUTION_KEYSPACE, JOB_KEYSPACE, NODE_STATE_KEYSPACE};
use aruna_core::structs::execution::job::{
    JobId, JobPayload, RECORD_KEY_PREFIX, StagingJobCheckpoint, StagingJobSpec,
};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::distribution::DistributionView;
use aruna_core::structs::storage::distribution::source_root::SourceRoot;
use aruna_core::types::{GroupId, Key, Value};
use serde::{Deserialize, Serialize};
use tracing::warn;
use ulid::Ulid;

use super::purge::clear_prefix;
use crate::connectors::get_connector::{GetSourceInput, GetSourceOperation};
use crate::connectors::list_connectors::{ListSourceInput, ListSourceOperation};
use crate::driver::{DriverContext, drive};
use crate::jobs::JOB_RETENTION_MS;
use crate::jobs::service::submit_handed_job;
use crate::jobs::staging::{handed_checkpoint, read_staging_checkpoint};
use crate::jobs::store::decode_job_record;
use crate::metadata::protocol::MetadataReadError;
use crate::s3::bucket::get::GetBucketOperation;

/// Job rows one scan page reads.
const JOB_PAGE: usize = 256;
const HANDOVER_PREFIX: &[u8] = b"import_handover/";
const RESUME_PREFIX: &[u8] = b"import_resume/";
const JOBS_PREFIX: &[u8] = b"import_jobs/";

/// A staging job a range's former coordinator hands over: its spec, the source root its
/// connector reads, and where the job stood before the range was fenced.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandedImport {
    pub spec: StagingJobSpec,
    pub root: SourceRoot,
    pub resume: Option<StagingJobCheckpoint>,
}

/// A fenced job as its former coordinator keeps it until the transfer is no longer open: its
/// spec, so pruning its job record loses nothing, and where it stood before the fence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct FencedJob {
    spec: StagingJobSpec,
    at: Option<StagingJobCheckpoint>,
}

/// What a new coordinator keeps of a range's imports: jobs still to fetch from the former
/// coordinator, and jobs ready to run here with their connector and position.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Kept {
    from: NodeId,
    pending: Vec<JobId>,
    ready: Vec<(StagingJobSpec, Option<StagingJobCheckpoint>)>,
}

/// The open staging jobs of this node that import into `bucket`, with their specs.
pub(crate) async fn open_imports(
    context: &DriverContext,
    bucket: &str,
) -> Result<Vec<(JobId, StagingJobSpec)>, String> {
    let mut start = None;
    let mut jobs = Vec::new();
    loop {
        let prefix = Some(RECORD_KEY_PREFIX.to_vec());
        let (rows, next) = page(context, JOB_KEYSPACE, prefix, start, JOB_PAGE).await?;
        for (_, value) in rows {
            let Ok(record) = decode_job_record(&value) else {
                continue;
            };
            if let JobPayload::Staging(spec) = record.payload
                && spec.bucket == bucket
                && !record.state.is_terminal()
            {
                jobs.push((record.job_id, spec));
            }
        }
        match next {
            Some(next) => start = Some(IterStart::After(next)),
            None => return Ok(jobs),
        }
    }
}

fn resume_key(transfer_id: Ulid, job_id: JobId) -> Vec<u8> {
    [RESUME_PREFIX, &transfer_id.to_bytes(), &job_id.to_bytes()].concat()
}

fn jobs_key(transfer_id: Ulid) -> Vec<u8> {
    [JOBS_PREFIX, &transfer_id.to_bytes()].concat()
}

/// Keeps the open imports of `bucket` and where each stands, before the range of `transfer_id`
/// is fenced: pages committed after it only repeat work the new coordinator does again.
pub(crate) async fn keep_resume(
    context: &DriverContext,
    bucket: &str,
    transfer_id: Ulid,
) -> Result<(), String> {
    let mut jobs = Vec::new();
    for (job_id, spec) in open_imports(context, bucket).await? {
        let at = read_staging_checkpoint(context, job_id).await?;
        keep_job(context, (transfer_id, job_id), FencedJob { spec, at }).await?;
        jobs.push(job_id);
    }
    let value = postcard::to_allocvec(&jobs).map_err(|error| error.to_string())?;
    write_row(context, jobs_key(transfer_id).into(), value).await
}

async fn keep_job(
    context: &DriverContext,
    (transfer_id, job_id): (Ulid, JobId),
    job: FencedJob,
) -> Result<(), String> {
    let value = postcard::to_allocvec(&job).map_err(|error| error.to_string())?;
    write_row(context, resume_key(transfer_id, job_id).into(), value).await
}

/// The imports of `bucket` to hand over: those open now and those kept before the fence of
/// `transfer_id`, finished ones too, since a job may end while it skips the fenced range's keys.
pub(crate) async fn fenced_imports(
    context: &DriverContext,
    bucket: &str,
    transfer_id: Ulid,
) -> Result<Vec<JobId>, String> {
    let stored = read_value(context, NODE_STATE_KEYSPACE, jobs_key(transfer_id).into()).await?;
    let kept = stored.as_deref().map(postcard::from_bytes).transpose();
    let mut jobs: Vec<JobId> = kept.map_err(|error| error.to_string())?.unwrap_or_default();
    for (job_id, spec) in open_imports(context, bucket).await? {
        if !jobs.contains(&job_id) {
            keep_job(context, (transfer_id, job_id), FencedJob { spec, at: None }).await?;
            jobs.push(job_id);
        }
    }
    Ok(jobs)
}

/// Drops what [`keep_resume`] kept for `transfer_id` once the transfer is no longer open.
pub(crate) async fn drop_resume(context: &DriverContext, transfer_id: Ulid) -> Result<(), String> {
    let key: Key = jobs_key(transfer_id).into();
    if read_value(context, NODE_STATE_KEYSPACE, key.clone())
        .await?
        .is_none()
    {
        return Ok(());
    }
    let prefix = [RESUME_PREFIX, &transfer_id.to_bytes()].concat();
    clear_prefix(context, NODE_STATE_KEYSPACE, prefix).await?;
    delete(context, key).await
}

/// A staging job of this node as it hands it over to a member of the set its bucket is bound
/// to, with its connector's source root and its position before the fence of `transfer_id`.
pub(crate) async fn staging_spec(
    context: &DriverContext,
    peer: NodeId,
    (transfer_id, job_id): (Ulid, JobId),
) -> Result<HandedImport, MetadataReadError> {
    let unavailable = |_| MetadataReadError::Unavailable;
    let key = resume_key(transfer_id, job_id).into();
    let value = read_value(context, NODE_STATE_KEYSPACE, key)
        .await
        .map_err(unavailable)?
        .ok_or(MetadataReadError::NotFound)?;
    let FencedJob { spec, at } =
        postcard::from_bytes(&value).map_err(|_| MetadataReadError::Unavailable)?;
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
    let input = GetSourceInput {
        group_id: spec.group_id,
        connector_id: spec.connector_id,
    };
    let connector = drive(GetSourceOperation::new(input), context)
        .await
        .map_err(|_| MetadataReadError::Unavailable)?
        .connector;
    let root = SourceRoot {
        kind: connector.kind,
        public_config: connector.public_config.into_iter().collect(),
        namespace: connector.namespace,
    };
    let resume = at.map(|at| handed_checkpoint(&spec, &at));
    Ok(HandedImport { spec, root, resume })
}

fn handover_key(set_id: Ulid, range_id: Ulid) -> Vec<u8> {
    [HANDOVER_PREFIX, &set_id.to_bytes(), &range_id.to_bytes()].concat()
}

/// Fetches the former coordinator's open jobs of a range, bound to this node's connector of each
/// source root, and keeps them until this node coordinates the range. `true` once all are
/// fetched; an unanswered request or a root no connector here reads waits for the next pass.
pub(crate) async fn fetch_imports(
    context: &DriverContext,
    (set_id, range_id, transfer_id): (Ulid, Ulid, Ulid),
    from: NodeId,
    jobs: &[JobId],
) -> Result<bool, String> {
    let key: Key = handover_key(set_id, range_id).into();
    let stored = read_value(context, NODE_STATE_KEYSPACE, key.clone()).await?;
    let mut kept = match stored {
        Some(value) => postcard::from_bytes(&value).map_err(|error| error.to_string())?,
        None => Kept {
            from,
            pending: jobs.to_vec(),
            ready: Vec::new(),
        },
    };
    let (Some(handle), Some(net)) = (
        context.metadata_handle.as_ref(),
        context.net_handle.as_ref(),
    ) else {
        return Ok(false);
    };
    while let Some(job_id) = kept.pending.first().copied() {
        match handle
            .request_staging_spec(kept.from, transfer_id, job_id)
            .await
        {
            Ok(handed) => {
                let group_id = handed.spec.group_id;
                let Some(connector_id) = local_connector(context, group_id, &handed.root).await?
                else {
                    warn!(%set_id, %job_id, kind = ?handed.root.kind, "No connector here reads a handed-over import's source; the transfer waits");
                    break;
                };
                let spec = StagingJobSpec {
                    node_id: net.node_id(),
                    connector_id,
                    ..handed.spec
                };
                kept.ready.push((spec, handed.resume));
            }
            // The former coordinator keeps each handed job while the transfer is open.
            Err(MetadataReadError::NotFound) => {
                warn!(%set_id, %job_id, "The former coordinator holds no handed-over import; it is dropped");
            }
            Err(_) => break,
        }
        kept.pending.remove(0);
        write_kept(context, key.clone(), &kept).await?;
    }
    write_kept(context, key, &kept).await?;
    Ok(kept.pending.is_empty())
}

/// This node's connector of `group_id` with the same source root: kind, public settings and
/// login namespace.
async fn local_connector(
    context: &DriverContext,
    group_id: GroupId,
    root: &SourceRoot,
) -> Result<Option<Ulid>, String> {
    let listed = drive(
        ListSourceOperation::new(ListSourceInput { group_id }),
        context,
    )
    .await
    .map_err(|error| error.to_string())?;
    let matched = listed.connectors.into_iter().find(|connector| {
        let config: BTreeMap<String, String> =
            connector.public_config.clone().into_iter().collect();
        connector.kind == root.kind
            && config == root.public_config
            && connector.namespace == root.namespace
    });
    Ok(matched.map(|connector| connector.connector_id))
}

async fn write_kept(context: &DriverContext, key: Key, kept: &Kept) -> Result<(), String> {
    let value = postcard::to_allocvec(kept).map_err(|error| error.to_string())?;
    write_row(context, key, value).await
}

async fn write_row(context: &DriverContext, key: Key, value: Vec<u8>) -> Result<(), String> {
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

/// Runs each fetched job here as its own job from its handed position; a started job leaves
/// the row at once.
async fn start_kept(
    context: &DriverContext,
    key: Key,
    mut kept: Kept,
    bucket: &BucketInfo,
) -> Result<(), String> {
    let Some(local) = context.net_handle.as_ref().map(|net| net.node_id()) else {
        return Ok(());
    };
    while let Some((spec, resume)) = kept.ready.first().cloned() {
        let spec = StagingJobSpec {
            bucket_identity: bucket.identity(),
            ..spec
        };
        submit_handed_job(context, spec, resume.as_ref(), local, JOB_RETENTION_MS).await?;
        kept.ready.remove(0);
        write_kept(context, key.clone(), &kept).await?;
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
                let kept: Kept = postcard::from_bytes(&value).map_err(|e| e.to_string())?;
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

    // A handed-over job names its connector's source root and its position before the fence;
    // the new coordinator binds it to its own connector of that root, and only members ask.
    #[tokio::test]
    async fn hands_over_portable() {
        use crate::distribution::test_record::{binding, record, secret, store_record};
        use crate::jobs::staging::seed_checkpoint;
        use crate::tests::staging::{create_http_connector, setup_driver_context};
        use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
        use aruna_core::structs::execution::job::{JobRecord, StagingJobPhase, StagingWalk};
        let test = setup_driver_context().await;
        let context = &test.driver_context;
        let mut spec = spec("spire");
        let connector = create_http_connector(context, spec.group_id, "https://a.example").await;
        spec.connector_id = connector.connector_id;
        let job_id = JobId::from_bytes([1; 16]);
        let payload = JobPayload::Staging(spec.clone());
        let job = JobRecord::new(
            job_id,
            payload,
            Default::default(),
            spec.node_id,
            1,
            1,
            None,
        );
        crate::jobs::store::insert_job(&context.storage_handle, &job)
            .await
            .unwrap();
        let (snapshot, bytes) = record(true);
        store_record(&context.storage_handle, snapshot.set_id(), bytes).await;
        let bucket = BucketInfo {
            group_id: spec.group_id,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: spec.auth_context.user_id,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: aruna_core::structs::storage::format::Compression::Off,
            distribution: Some(binding(&snapshot, 1, 5)),
        };
        let write = StorageEffect::Write {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: b"spire".to_vec().into(),
            value: bucket.to_bytes().unwrap().into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(write).await;
        let at = StagingJobCheckpoint {
            phase: StagingJobPhase::Registering,
            flat: Some(true),
            items_done: 0,
            prefixes_done: 1,
            walk: Some(StagingWalk {
                source_path: "data/".to_string(),
                target_prefix: String::new(),
                cursor: None,
                queue_row: None,
                from: Some("k".to_string()),
            }),
            manifest: None,
            queue_head: 0,
            queue_tail: 0,
            items_current: 7,
            items_succeeded: 7,
            items_failed: 0,
            items_total: None,
            bytes_current: 0,
            bytes_total: None,
            bytes_discovered: 70,
            current_path: None,
            errors: Vec::new(),
        };
        seed_checkpoint(context, job_id, &at).await.unwrap();
        let transfer_id = Ulid::from_bytes([8; 16]);
        keep_resume(context, "spire", transfer_id).await.unwrap();
        // The fenced job is kept with its spec, so pruning its record loses nothing.
        let prune = StorageEffect::Delete {
            key_space: JOB_KEYSPACE.to_string(),
            key: aruna_core::structs::execution::job::job_record_key(job_id),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(prune).await;

        let member = secret(2).public();
        let handed = staging_spec(context, member, (transfer_id, job_id))
            .await
            .unwrap();
        assert_eq!(handed.root.kind, connector.kind);
        assert_eq!(
            handed
                .root
                .public_config
                .get("endpoint")
                .map(String::as_str),
            Some("https://a.example")
        );
        assert_eq!(
            handed.resume,
            Some(crate::jobs::staging::handed_checkpoint(&spec, &at))
        );
        let local = local_connector(context, spec.group_id, &handed.root).await;
        assert_eq!(local, Ok(Some(connector.connector_id)));
        let mut other = handed.root.clone();
        other
            .public_config
            .insert("endpoint".to_string(), "https://b.example".to_string());
        assert_eq!(
            local_connector(context, spec.group_id, &other).await,
            Ok(None)
        );
        let stranger = secret(7).public();
        assert_eq!(
            staging_spec(context, stranger, (transfer_id, job_id)).await,
            Err(MetadataReadError::Forbidden)
        );

        // Once the transfer is no longer open, its kept jobs and job list go.
        drop_resume(context, transfer_id).await.unwrap();
        let handed = staging_spec(context, member, (transfer_id, job_id)).await;
        assert_eq!(handed, Err(MetadataReadError::NotFound));
        let jobs = read_value(context, NODE_STATE_KEYSPACE, jobs_key(transfer_id).into()).await;
        assert_eq!(jobs, Ok(None));
    }

    // A transfer away that opens while a staging job is admitted refuses the job: the admission
    // retries once the record changed under it, so no job lands after the transfer's job scan.
    #[tokio::test]
    async fn refuses_moving_admission() {
        use crate::distribution::test_record::store_record;
        use crate::distribution::test_record::{binding, member, realm, record, secret};
        use crate::jobs::submit::{SubmitJobError, SubmitJobOperation, SubmitJobSpec};
        use aruna_core::effects::Effect;
        use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
        use aruna_core::operation::Operation;
        use aruna_core::structs::execution::job::WorkspaceMode;
        use aruna_core::structs::storage::distribution::snapshot::OpenTransfer;
        use aruna_core::structs::storage::distribution::{SignedSnapshot, TransferRole};
        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let (mut snapshot, bytes) = record(true);
        let set_id = snapshot.set_id();
        store_record(&storage, set_id, bytes).await;
        let spec = spec("spire");
        let bucket = BucketInfo {
            group_id: spec.group_id,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: spec.auth_context.user_id,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: aruna_core::structs::storage::format::Compression::Off,
            distribution: Some(binding(&snapshot, 1, 5)),
        };
        let write = StorageEffect::Write {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: b"spire".to_vec().into(),
            value: bucket.to_bytes().unwrap().into(),
            txn_id: None,
        };
        storage.send_storage_effect(write).await;
        snapshot.lifecycle.transfers.push(OpenTransfer {
            transfer_id: Ulid::from_bytes([8; 16]),
            range_id: snapshot.config.ranges[0].range_id,
            from: member(1),
            to: member(2),
            epoch: 2,
            role: TransferRole::Replica,
            fenced: None,
            completed: None,
        });
        let head = SignedSnapshot::sign(snapshot, realm(), &secret(1)).unwrap();
        let moving = DistributionView {
            head,
            conflict: None,
        };
        let submission = SubmitJobSpec {
            created_by: spec.auth_context.user_id,
            owner_node_id: spec.node_id,
            payload: JobPayload::Staging(spec),
            dedup_key: None,
            now_ms: 1,
            retention_ms: 1,
            workspace_mode: WorkspaceMode::default(),
            workspace_bucket: None,
            active_cap: None,
        };
        let mut operation = SubmitJobOperation::new(submission, JobId::from_bytes([1; 16]));
        let mut effects = operation.start().into_vec();
        let mut raced = false;
        while let Some(Effect::Storage(effect)) = effects.pop() {
            // The transfer opens after the admission read the record, before it commits.
            if !raced && matches!(effect, StorageEffect::CommitTransaction { .. }) {
                store_record(&storage, set_id, moving.to_bytes().unwrap()).await;
                raced = true;
            }
            let event = storage.send_storage_effect(effect).await;
            if !operation.is_complete() {
                effects = operation.step(event).into_vec();
            }
        }
        assert!(raced);
        assert_eq!(
            operation.finalize(),
            Err(SubmitJobError::RangeMoving(set_id))
        );
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        assert!(open_imports(&context, "spire").await.unwrap().is_empty());
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
        let open: Vec<JobId> = open.into_iter().map(|(job_id, _)| job_id).collect();
        assert_eq!(open, [JobId::from_bytes([1; 16])]);
    }

    // A job open at the fence is handed over even when it ended afterwards, skipping the fenced
    // range's keys; a transfer that kept nothing hands over only open jobs.
    #[tokio::test]
    async fn keeps_fenced_jobs() {
        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let node_id = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let job_id = JobId::from_bytes([1; 16]);
        let payload = JobPayload::Staging(spec("spire"));
        let mut record = JobRecord::new(job_id, payload, Default::default(), node_id, 1, 1, None);
        crate::jobs::store::insert_job(&storage, &record)
            .await
            .unwrap();
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let transfer_id = Ulid::from_bytes([8; 16]);
        keep_resume(&context, "spire", transfer_id).await.unwrap();
        record.state = JobState::Succeeded;
        crate::jobs::store::insert_job(&context.storage_handle, &record)
            .await
            .unwrap();

        assert!(open_imports(&context, "spire").await.unwrap().is_empty());
        let handed = fenced_imports(&context, "spire", transfer_id).await;
        assert_eq!(handed.unwrap(), [job_id]);
        let other = Ulid::from_bytes([9; 16]);
        let kept = fenced_imports(&context, "spire", other).await;
        assert!(kept.unwrap().is_empty());
    }
}
