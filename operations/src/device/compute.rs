//! Owner-initiated local execution on a user device.
//!
//! The device runs its owner's jobs against device-local data and nothing else.
//! The realm never dispatches here, no record of a local run is forwarded or
//! replicated, and outputs stay in the node-local workspace bucket until the
//! owner publishes them. A realm input is copied onto the device first, as an
//! ordinary local object and never as a reference to the realm version.

use aruna_core::compute::{ExecutorKind, ResourceEnvelope};
use aruna_core::structs::{
    AuthContext, BucketInfo, ExecutionSpec, InputMode, InputSource, JobPayload, JobState,
    RealmConfigDocument, VersionedObjectArn, WorkspaceMode,
};
use aruna_core::types::{GroupId, NodeId, UserId};
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::{DriverContext, drive, routing_snapshot};
use crate::jobs::service::{list_owned_jobs, submit_execution_job};
use crate::jobs::submit::{SubmitJobError, SubmitJobResult};
use crate::metadata::api::load_realm_config;
use crate::mutate_realm_placement::node_kind;
use crate::node_info::read_operator_drain;
use crate::replication::bao_read::{BaoReadOutput, managed_read};
use crate::replication::protocol::{BaoReadRequest, BaoReadTarget};
use crate::s3::create_bucket::{CreateBucketError, CreateBucketOperation};
use crate::s3::get_bucket_info::GetBucketInfoOperation;
use crate::s3::head_object::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
use crate::s3::put_object::{PutObjectConfig, PutObjectInput, PutObjectOperation};

/// Runs one scan of the owner's job index reports on. A device queues its
/// owner's work, not a realm's, so this bounds the count rather than paging it.
const MAX_RUN_SCAN: usize = 64;

/// One owner-initiated run of the local executor.
pub struct LocalExecutionConfig {
    pub spec: ExecutionSpec,
    /// The user this device is enrolled for; every local run is theirs.
    pub owner: UserId,
    /// This device, which owns the job it is about to accept.
    pub node_id: NodeId,
    pub idempotency_key: Option<String>,
    pub workspace_mode: WorkspaceMode,
    pub retention_ms: u64,
}

/// Why a device refused a local run.
#[derive(Debug, Error)]
pub enum LocalExecutionError {
    #[error("this node is not a user device")]
    NotADevice,
    #[error("this device runs jobs for its owner only")]
    NotOwner,
    #[error("this device's compute plane is paused")]
    Paused,
    #[error("this device has no compute backend for the request")]
    NoExecutor,
    #[error("{0} needs an S3 endpoint a device does not expose")]
    Unsupported(&'static str),
    #[error("this device already runs {limit} jobs")]
    AtCapacity { limit: u32 },
    #[error("input {bucket}/{key} is not on this device")]
    InputNotLocal { bucket: String, key: String },
    #[error("input {bucket}/{key} could not be copied onto this device: {reason}")]
    InputCopy {
        bucket: String,
        key: String,
        reason: String,
    },
    #[error(transparent)]
    Submit(#[from] SubmitJobError),
    #[error("device state unavailable: {0}")]
    Unavailable(String),
}

/// Accepts one local run on behalf of the device owner.
///
/// Every refusal is decided before the job exists: the caller is the enrolled
/// owner, the plane is not paused, the request needs nothing but file staging,
/// the device is below its configured concurrency, and every input either sits
/// on this device or was copied onto it here.
pub async fn submit_local_execution(
    context: &DriverContext,
    mut config: LocalExecutionConfig,
) -> Result<SubmitJobResult, LocalExecutionError> {
    let realm = load_realm_config(context, config.owner.realm_id)
        .await
        .ok_or_else(|| LocalExecutionError::Unavailable("realm configuration".to_string()))?;
    let owner = node_kind(&realm, config.node_id)
        .and_then(|kind| kind.owner())
        .ok_or(LocalExecutionError::NotADevice)?;
    if owner != config.owner {
        return Err(LocalExecutionError::NotOwner);
    }
    if read_operator_drain(context)
        .await
        .map_err(LocalExecutionError::Unavailable)?
    {
        return Err(LocalExecutionError::Paused);
    }
    local_staging(&config.spec, config.workspace_mode)?;
    let limits = backend_limits(context, config.spec.executor_constraint.as_deref())?;
    if let Some(limit) = limits.max_concurrent {
        let runs = count_runs(context, config.owner).await?;
        if runs.active() >= limit {
            return Err(LocalExecutionError::AtCapacity { limit });
        }
    }
    resolve_inputs(context, &mut config, &realm).await?;
    submit_execution_job(
        context,
        config.spec,
        config.owner,
        config.node_id,
        config.idempotency_key,
        config.workspace_mode,
        None,
        config.retention_ms,
    )
    .await
    .map_err(LocalExecutionError::Submit)
}

/// This device's compute plane as the owner's tools see it.
#[derive(Debug, Clone, PartialEq)]
pub struct ComputeStatus {
    pub enabled: bool,
    /// Wire kind of the backend a local run would select.
    pub backend: Option<String>,
    pub healthy: bool,
    /// A paused plane accepts no local run at all.
    pub paused: bool,
    pub limits: ResourceEnvelope,
    pub running: u32,
    pub queued: u32,
}

/// Reports the local compute plane and the owner's runs on it.
///
/// The counters come from the durable job records rather than the in-process
/// jobs runtime, which counts every job class on the node and starts empty
/// after a restart. They are bounded by one scan, so a device holding more
/// unfinished runs than that reports the bound.
pub async fn compute_status(
    context: &DriverContext,
    owner: UserId,
) -> Result<ComputeStatus, LocalExecutionError> {
    let backend = context
        .compute_handle
        .as_ref()
        .and_then(|registry| registry.select(None).cloned());
    let runs = count_runs(context, owner).await?;
    let paused = read_operator_drain(context)
        .await
        .map_err(LocalExecutionError::Unavailable)?;
    let (kind, limits, healthy) = match &backend {
        Some(backend) => (
            Some(backend.kind().as_wire()),
            backend.capabilities().limits,
            backend.health().await.is_ok(),
        ),
        None => (None, ResourceEnvelope::default(), false),
    };
    Ok(ComputeStatus {
        enabled: backend.is_some(),
        backend: kind,
        healthy,
        paused,
        limits,
        running: runs.running,
        queued: runs.queued,
    })
}

/// The owner's runs this device has not finished.
struct LocalRuns {
    running: u32,
    queued: u32,
}

impl LocalRuns {
    fn active(&self) -> u32 {
        self.running + self.queued
    }
}

/// Only file staging runs on a device: mounted inputs and Direct-S3 both hand
/// the container an S3 endpoint, and a device exposes none.
fn local_staging(spec: &ExecutionSpec, mode: WorkspaceMode) -> Result<(), LocalExecutionError> {
    if mode == WorkspaceMode::None {
        return Err(LocalExecutionError::Unsupported("workspace mode none"));
    }
    if spec
        .inputs
        .iter()
        .any(|input| input.mode == InputMode::Mount)
    {
        return Err(LocalExecutionError::Unsupported("a mounted input"));
    }
    Ok(())
}

/// Static ceilings of the backend this request would run on. A device without
/// one refuses the run instead of queueing work nothing will claim.
fn backend_limits(
    context: &DriverContext,
    constraint: Option<&str>,
) -> Result<ResourceEnvelope, LocalExecutionError> {
    let registry = context
        .compute_handle
        .as_ref()
        .ok_or(LocalExecutionError::NoExecutor)?;
    let constraint = constraint.map(ExecutorKind::from_wire);
    let backend = registry
        .select(constraint.as_ref())
        .ok_or(LocalExecutionError::NoExecutor)?;
    Ok(backend.capabilities().limits)
}

async fn count_runs(
    context: &DriverContext,
    owner: UserId,
) -> Result<LocalRuns, LocalExecutionError> {
    let (records, _) = list_owned_jobs(context, owner, None, MAX_RUN_SCAN, |record| {
        matches!(record.payload, JobPayload::Execution(_)) && !record.state.is_terminal()
    })
    .await
    .map_err(LocalExecutionError::Unavailable)?;
    let queued = records
        .iter()
        .filter(|record| record.state == JobState::Queued)
        .count();
    Ok(LocalRuns {
        running: (records.len() - queued) as u32,
        queued: queued as u32,
    })
}

/// Makes every declared input readable on this device.
///
/// An input naming another node is a realm object: its exact version is copied
/// here and the selection is rewritten to that local copy, so the run reads
/// device-local bytes and no reference to the realm version is ever created.
async fn resolve_inputs(
    context: &DriverContext,
    config: &mut LocalExecutionConfig,
    realm: &RealmConfigDocument,
) -> Result<(), LocalExecutionError> {
    let group_id = config.spec.group_id;
    let quota_ceiling = realm.quota.effective_group_ceiling(&group_id);
    for index in 0..config.spec.inputs.len() {
        let input = &config.spec.inputs[index];
        let InputSource::S3 {
            bucket,
            key,
            version_id,
        } = &input.source;
        let (bucket, key, version_id) = (bucket.clone(), key.clone(), version_id.clone());
        match input
            .source_node_id
            .filter(|source| *source != config.node_id)
        {
            Some(source) => {
                let copied = copy_input(
                    context,
                    config,
                    CopyInput {
                        source,
                        bucket: bucket.clone(),
                        key: key.clone(),
                        version_id,
                        group_id,
                        quota_ceiling,
                    },
                )
                .await?;
                let input = &mut config.spec.inputs[index];
                input.source = InputSource::S3 {
                    bucket,
                    key,
                    version_id: Some(copied.to_string()),
                };
                input.source_node_id = None;
            }
            None => resolve_local(context, &bucket, &key, version_id.as_deref()).await?,
        }
    }
    Ok(())
}

/// Refuses at submit time what staging could only discover mid-run.
async fn resolve_local(
    context: &DriverContext,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<(), LocalExecutionError> {
    let version_id = version_id.map(Ulid::from_string).transpose().map_err(|_| {
        LocalExecutionError::InputNotLocal {
            bucket: bucket.to_string(),
            key: key.to_string(),
        }
    })?;
    let missing = LocalExecutionError::InputNotLocal {
        bucket: bucket.to_string(),
        key: key.to_string(),
    };
    match drive(
        HeadObjectOperation::new(HeadObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id,
        }),
        context,
    )
    .await
    .and_then(|result| result.transpose())
    {
        Ok(Some(_)) => Ok(()),
        Ok(None)
        | Err(
            HeadObjectError::NoSuchKey
            | HeadObjectError::NoSuchVersion
            | HeadObjectError::DeleteMarker,
        ) => Err(missing),
        Err(error) => Err(LocalExecutionError::Unavailable(error.to_string())),
    }
}

struct CopyInput {
    source: NodeId,
    bucket: String,
    key: String,
    version_id: Option<String>,
    group_id: GroupId,
    quota_ceiling: Option<u64>,
}

/// Copies one exact realm version onto this device and returns the local
/// version it created. The bytes are read with the owner's own authorization
/// and written through the ordinary local write path, so the copy is an
/// ordinary device-local object that the realm neither owns nor tracks.
async fn copy_input(
    context: &DriverContext,
    config: &LocalExecutionConfig,
    input: CopyInput,
) -> Result<Ulid, LocalExecutionError> {
    let refuse = |reason: String| LocalExecutionError::InputCopy {
        bucket: input.bucket.clone(),
        key: input.key.clone(),
        reason,
    };
    let version = input
        .version_id
        .as_deref()
        .map(Ulid::from_string)
        .transpose()
        .map_err(|_| refuse("the version is not a version id".to_string()))?
        .ok_or_else(|| refuse("a realm input needs an exact version".to_string()))?;
    let realm_id = config.owner.realm_id;
    let request = BaoReadRequest {
        auth_context: AuthContext {
            user_id: config.owner,
            realm_id,
            path_restrictions: None,
        },
        realm_id,
        target: BaoReadTarget::ExactVersion(VersionedObjectArn {
            realm_id,
            node_id: input.source,
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            version,
        }),
        expected_blake3: None,
        metadata_only: false,
        // A device is never a managed-copy destination; the read is the
        // owner-bound device read, which refuses governed content outright.
        destination: None,
        known_refs: Vec::new(),
    };
    let (blob, size) = match managed_read(context, input.source, request).await {
        Ok(BaoReadOutput::Stream { blob, size, .. }) => (blob, size),
        Ok(BaoReadOutput::Metadata { .. }) => {
            return Err(refuse("the holder returned no bytes".to_string()));
        }
        Err(error) => return Err(refuse(error.to_string())),
    };
    claim_bucket(context, config, &input).await?;
    let routing = routing_snapshot(context, input.group_id, &input.bucket)
        .await
        .map_err(|error| refuse(error.to_string()))?;
    let result = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: config.owner,
            group_id: input.group_id,
            realm_id,
            node_id: config.node_id,
            request: PutObjectInput {
                bucket: input.bucket.clone(),
                key: input.key.clone(),
                content_length: Some(size),
                body: Some(blob),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: input.quota_ceiling,
            routing,
        }),
        context,
    )
    .await
    .and_then(|result| result.transpose())
    .map_err(|error| refuse(error.to_string()))?
    .ok_or_else(|| refuse("the copy did not finish".to_string()))?;
    Ok(result.version_id)
}

/// The device-local bucket the copy lands in: the source name under the
/// execution's own group. A name another group already holds is refused rather
/// than written into.
async fn claim_bucket(
    context: &DriverContext,
    config: &LocalExecutionConfig,
    input: &CopyInput,
) -> Result<(), LocalExecutionError> {
    let refuse = |reason: String| LocalExecutionError::InputCopy {
        bucket: input.bucket.clone(),
        key: input.key.clone(),
        reason,
    };
    match drive(GetBucketInfoOperation::new(input.bucket.clone()), context)
        .await
        .and_then(|result| result.transpose())
        .map_err(|error| refuse(error.to_string()))?
    {
        Some(existing) if existing.group_id == input.group_id => return Ok(()),
        Some(_) => {
            return Err(refuse(
                "a local bucket of that name is another group's".to_string(),
            ));
        }
        None => {}
    }
    match drive(
        CreateBucketOperation::new(
            input.bucket.clone(),
            BucketInfo {
                group_id: input.group_id,
                created_at: SystemTime::now(),
                created_by: config.owner,
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
            },
        ),
        context,
    )
    .await
    .and_then(|result| result.transpose())
    {
        Ok(_) | Err(CreateBucketError::BucketAlreadyExists) => Ok(()),
        Err(error) => Err(refuse(error.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::records::tests::fixture::payload;
    use aruna_core::document::DocumentSyncTarget;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::structs::{Actor, InputSelection, RealmId, RealmNodeKind};
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;

    const REALM: RealmId = RealmId([3u8; 32]);

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn owner(seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), REALM)
    }

    fn test_ctx(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    fn realm_config(local: NodeId, kind: RealmNodeKind) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::default_for_realm(REALM, Vec::new());
        config.seed_default_placement();
        config.ensure_node(local, kind);
        config
    }

    async fn write_config(ctx: &DriverContext, config: &RealmConfigDocument, local: NodeId) {
        let actor = Actor {
            node_id: local,
            user_id: UserId::nil(REALM),
            realm_id: REALM,
        };
        let target = DocumentSyncTarget::RealmConfig { realm_id: REALM };
        let event = ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    fn run_config(local: NodeId, caller: UserId) -> LocalExecutionConfig {
        LocalExecutionConfig {
            spec: payload(),
            owner: caller,
            node_id: local,
            idempotency_key: None,
            workspace_mode: WorkspaceMode::Kept,
            retention_ms: 60_000,
        }
    }

    fn input(bucket: &str, source: Option<NodeId>, version: Option<Ulid>) -> InputSelection {
        InputSelection {
            source: InputSource::S3 {
                bucket: bucket.to_string(),
                key: "reads.fastq".to_string(),
                version_id: version.map(|version| version.to_string()),
            },
            source_node_id: source,
            dest_key: "reads.fastq".to_string(),
            mode: InputMode::Snapshot,
            container_path: None,
            name: None,
            description: None,
        }
    }

    #[tokio::test]
    async fn refuses_a_server_node() {
        // Only a device has an owner to run local jobs for.
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let local = node(1);
        write_config(&ctx, &realm_config(local, RealmNodeKind::Server), local).await;

        let error = submit_local_execution(&ctx, run_config(local, owner(2)))
            .await
            .expect_err("a server node runs no local job");

        assert!(matches!(error, LocalExecutionError::NotADevice));
    }

    #[tokio::test]
    async fn refuses_a_foreign_caller() {
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let local = node(1);
        let config = realm_config(local, RealmNodeKind::User { owner: owner(2) });
        write_config(&ctx, &config, local).await;

        let error = submit_local_execution(&ctx, run_config(local, owner(3)))
            .await
            .expect_err("only the enrolled owner runs jobs here");

        assert!(matches!(error, LocalExecutionError::NotOwner));
    }

    #[tokio::test]
    async fn refuses_while_paused() {
        // Pause stops local compute like it stops serving and syncing.
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let local = node(1);
        let config = realm_config(local, RealmNodeKind::User { owner: owner(2) });
        write_config(&ctx, &config, local).await;
        crate::node_info::set_operator_drain(&ctx, local, REALM, true)
            .await
            .unwrap();

        let error = submit_local_execution(&ctx, run_config(local, owner(2)))
            .await
            .expect_err("a paused plane accepts nothing");

        assert!(matches!(error, LocalExecutionError::Paused));
    }

    #[test]
    fn refuses_foreign_staging() {
        // Both modes hand the container an S3 endpoint a device does not expose.
        let mut spec = payload();
        assert!(matches!(
            local_staging(&spec, WorkspaceMode::None),
            Err(LocalExecutionError::Unsupported(_))
        ));

        spec.inputs.push(input("project", None, None));
        spec.inputs[0].mode = InputMode::Mount;
        assert!(matches!(
            local_staging(&spec, WorkspaceMode::Kept),
            Err(LocalExecutionError::Unsupported(_))
        ));

        spec.inputs[0].mode = InputMode::Snapshot;
        assert!(local_staging(&spec, WorkspaceMode::Kept).is_ok());
    }

    #[tokio::test]
    async fn refuses_a_missing_input() {
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let local = node(1);
        let realm = realm_config(local, RealmNodeKind::User { owner: owner(2) });
        let mut config = run_config(local, owner(2));
        config.spec.inputs.push(input("project", None, None));

        let error = resolve_inputs(&ctx, &mut config, &realm)
            .await
            .expect_err("an input the device does not hold is refused");

        assert!(matches!(
            error,
            LocalExecutionError::InputNotLocal { bucket, .. } if bucket == "project"
        ));
    }

    #[tokio::test]
    async fn refuses_a_floating_copy() {
        // A realm input is copied by exact version or not at all.
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let local = node(1);
        let realm = realm_config(local, RealmNodeKind::User { owner: owner(2) });
        let mut config = run_config(local, owner(2));
        config
            .spec
            .inputs
            .push(input("project", Some(node(4)), None));

        let error = resolve_inputs(&ctx, &mut config, &realm)
            .await
            .expect_err("a realm input without a version is refused");

        assert!(matches!(
            error,
            LocalExecutionError::InputCopy { reason, .. } if reason.contains("exact version")
        ));
    }
}
