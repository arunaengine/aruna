//! Owner-initiated local execution on a user device.
//!
//! The device runs its owner's jobs against device-local data and nothing else.
//! The realm never dispatches here and no record of a local run is forwarded or
//! replicated. An input the device does not hold must name the realm version
//! that holds it, which staging then materializes in the run's own workspace as
//! an ordinary local object, never as a reference.

use aruna_core::compute::{ExecutorKind, ResourceEnvelope};
use aruna_core::structs::{
    ExecutionSpec, InputMode, InputSource, JobPayload, JobState, WorkspaceMode,
};
use aruna_core::types::{NodeId, UserId};
use thiserror::Error;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::jobs::service::{list_owned_jobs, submit_execution_job};
use crate::jobs::submit::{SubmitJobError, SubmitJobResult};
use crate::metadata::api::load_realm_config;
use crate::mutate_realm_placement::node_kind;
use crate::node_info::read_operator_drain;
use crate::s3::head_object::{HeadObjectError, HeadObjectInput, HeadObjectOperation};

/// Unfinished runs one status scan counts before it stops. A device queues its
/// owner's work, not a realm's, so the count is bounded instead of paged.
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
    #[error("this device does not run the request: {0}")]
    Unsupported(&'static str),
    #[error("input {bucket}/{key} is not on this device")]
    InputNotLocal { bucket: String, key: String },
    /// The request itself asks for a copy this device may never make.
    #[error("input {bucket}/{key} cannot be copied onto this device: {reason}")]
    InputRefused {
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
/// Every refusal it can decide is decided before the job exists; the run
/// ceiling is counted by the admitting transaction, so two concurrent
/// submissions cannot both pass it.
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
    resolve_inputs(context, &config).await?;
    pin_outputs(&mut config.spec, config.node_id);
    submit_execution_job(
        context,
        config.spec,
        config.owner,
        config.node_id,
        config.idempotency_key,
        config.workspace_mode,
        None,
        config.retention_ms,
        limits.max_concurrent,
    )
    .await
    .map_err(LocalExecutionError::Submit)
}

/// Pins every declared output to this device, the only node that will ever
/// write them. A workspace output is pinned by the run itself once its bucket
/// exists.
fn pin_outputs(spec: &mut ExecutionSpec, node_id: NodeId) {
    for output in &mut spec.file_outputs {
        output.destination_node_id = Some(node_id);
    }
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
    /// Why a local run would be refused right now, if it would be.
    pub message: Option<String>,
}

/// Reports the local compute plane and the owner's runs on it.
///
/// The counters come from the durable job records, so they survive a restart,
/// and stop at [`MAX_RUN_SCAN`] unfinished runs.
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
    let (kind, limits, failure) = match &backend {
        Some(backend) => (
            Some(backend.kind().as_wire()),
            backend.capabilities().limits,
            backend.health().await.err().map(|error| error.to_string()),
        ),
        None => (
            None,
            ResourceEnvelope::default(),
            Some("no compute backend is configured on this device".to_string()),
        ),
    };
    Ok(ComputeStatus {
        enabled: backend.is_some(),
        backend: kind,
        healthy: backend.is_some() && failure.is_none(),
        paused,
        limits,
        running: runs.running,
        queued: runs.queued,
        message: match paused {
            true => Some(LocalExecutionError::Paused.to_string()),
            false => failure,
        },
    })
}

/// The owner's runs this device has not finished.
struct LocalRuns {
    running: u32,
    queued: u32,
}

/// Only file staging into the run's own workspace happens on a device: the
/// other modes hand the container an S3 endpoint a device does not expose.
fn local_staging(spec: &ExecutionSpec, mode: WorkspaceMode) -> Result<(), LocalExecutionError> {
    match mode {
        WorkspaceMode::None => {
            return Err(LocalExecutionError::Unsupported(
                "workspace mode none needs an S3 endpoint a device does not expose",
            ));
        }
        WorkspaceMode::Existing => {
            return Err(LocalExecutionError::Unsupported(
                "a local run keeps its outputs in its own workspace bucket",
            ));
        }
        WorkspaceMode::Temporary | WorkspaceMode::Kept => {}
    }
    if spec
        .inputs
        .iter()
        .any(|input| input.mode == InputMode::Mount)
    {
        return Err(LocalExecutionError::Unsupported(
            "a mounted input needs an S3 endpoint a device does not expose",
        ));
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

/// Refuses at submit time what staging could only discover mid-run: an input
/// naming another node is fetched by exact version, anything else must already
/// be readable here.
async fn resolve_inputs(
    context: &DriverContext,
    config: &LocalExecutionConfig,
) -> Result<(), LocalExecutionError> {
    for input in &config.spec.inputs {
        let InputSource::S3 {
            bucket,
            key,
            version_id,
        } = &input.source;
        match input
            .source_node_id
            .filter(|source| *source != config.node_id)
        {
            Some(_) => remote_version(bucket, key, version_id.as_deref())?,
            None => resolve_local(context, bucket, key, version_id.as_deref()).await?,
        }
    }
    Ok(())
}

/// A realm input is fetched by exact version or not at all: a device plans
/// nothing, so the request is the only thing that can name the version.
fn remote_version(
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<(), LocalExecutionError> {
    let refuse = |reason: &str| LocalExecutionError::InputRefused {
        bucket: bucket.to_string(),
        key: key.to_string(),
        reason: reason.to_string(),
    };
    match version_id.map(Ulid::from_string) {
        Some(Ok(_)) => Ok(()),
        Some(Err(_)) => Err(refuse("the version is not a version id")),
        None => Err(refuse("a realm input needs an exact version")),
    }
}

async fn resolve_local(
    context: &DriverContext,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<(), LocalExecutionError> {
    let missing = || LocalExecutionError::InputNotLocal {
        bucket: bucket.to_string(),
        key: key.to_string(),
    };
    let version_id = version_id
        .map(Ulid::from_string)
        .transpose()
        .map_err(|_| missing())?;
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
        ) => Err(missing()),
        Err(error) => Err(LocalExecutionError::Unavailable(error.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::records::tests::fixture::payload;
    use aruna_core::compute::{AttemptStatus, BackendError, FenceContext};
    use aruna_core::document::DocumentSyncTarget;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::LaunchDecline;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        DOCUMENT_SYNC_OUTBOX_KEYSPACE, JOB_FAMILY_OUTBOX_KEYSPACE, JOB_FAMILY_RECORD_KEYSPACE,
    };
    use aruna_core::structs::{
        Actor, InputSelection, NodeUrls, OutputDestination, OutputSelection, RealmConfigDocument,
        RealmId, RealmNodeKind,
    };
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

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
    async fn refuses_server_node() {
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
    async fn refuses_foreign_caller() {
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
        // Only file staging into the run's own workspace happens on a device.
        let mut spec = payload();
        for mode in [WorkspaceMode::None, WorkspaceMode::Existing] {
            assert!(matches!(
                local_staging(&spec, mode),
                Err(LocalExecutionError::Unsupported(_))
            ));
        }

        spec.inputs.push(input("project", None, None));
        spec.inputs[0].mode = InputMode::Mount;
        assert!(matches!(
            local_staging(&spec, WorkspaceMode::Kept),
            Err(LocalExecutionError::Unsupported(_))
        ));

        spec.inputs[0].mode = InputMode::Snapshot;
        assert!(local_staging(&spec, WorkspaceMode::Kept).is_ok());
    }

    struct StubBackend;

    #[async_trait::async_trait]
    impl aruna_compute::ExecutorBackend for StubBackend {
        fn kind(&self) -> ExecutorKind {
            ExecutorKind::Docker
        }
        fn capabilities(&self) -> aruna_compute::executor::BackendCaps {
            aruna_compute::executor::BackendCaps {
                file_staging: true,
                local_site: true,
                limits: ResourceEnvelope {
                    max_concurrent: Some(1),
                    ..ResourceEnvelope::default()
                },
                ..Default::default()
            }
        }
        fn run_identity(&self) -> aruna_core::compute::UserSpec {
            aruna_core::compute::NOBODY
        }
        async fn health(&self) -> Result<(), BackendError> {
            Ok(())
        }
        async fn resolve_image(
            &self,
            image: &str,
            _cancel: &CancellationToken,
        ) -> Result<String, BackendError> {
            Ok(image.to_string())
        }
        async fn fence(&self, _context: &FenceContext) -> Result<(), BackendError> {
            unimplemented!()
        }
        async fn submit(
            &self,
            _context: &FenceContext,
            _spec: &aruna_core::compute::TaskSpec,
            _cancel: &CancellationToken,
        ) -> Result<AttemptStatus, BackendError> {
            unimplemented!()
        }
        async fn status(&self, _context: &FenceContext) -> Result<AttemptStatus, BackendError> {
            unimplemented!()
        }
        async fn cancel(
            &self,
            _context: &FenceContext,
        ) -> Result<aruna_core::compute::CancelEvidence, BackendError> {
            unimplemented!()
        }
        async fn fetch_logs(
            &self,
            _context: &FenceContext,
            _limits: &aruna_core::compute::LogLimits,
        ) -> Result<aruna_core::compute::LogTails, BackendError> {
            unimplemented!()
        }
        async fn fetch_output(
            &self,
            _context: &FenceContext,
            _path: &str,
        ) -> Result<aruna_core::compute::TaskOutput, BackendError> {
            unimplemented!()
        }
        async fn reconcile(
            &self,
            _context: &FenceContext,
        ) -> aruna_core::compute::ReconcileEvidence {
            unimplemented!()
        }
        async fn cleanup(&self, _context: &FenceContext) -> Result<(), BackendError> {
            unimplemented!()
        }
    }

    fn device_ctx(root: &str) -> DriverContext {
        DriverContext {
            compute_handle: Some(std::sync::Arc::new(
                aruna_compute::ExecutorRegistry::new()
                    .with_backend(std::sync::Arc::new(StubBackend)),
            )),
            ..test_ctx(root)
        }
    }

    async fn rows(ctx: &DriverContext, key_space: &str) -> usize {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: key_space.to_string(),
                prefix: None,
                start: None,
                limit: 16,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values.len(),
            other => panic!("unexpected iter result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn keeps_runs_local() {
        // The device owns the job and tells nobody: no outbox row, no family
        // record, and a second run is refused by the owner's own ceiling.
        let dir = tempdir().unwrap();
        let ctx = device_ctx(dir.path().to_str().unwrap());
        let local = node(1);
        let caller = owner(2);
        let config = realm_config(local, RealmNodeKind::User { owner: caller });
        write_config(&ctx, &config, local).await;

        let accepted = submit_local_execution(&ctx, run_config(local, caller))
            .await
            .expect("the owner's run is accepted");

        assert!(accepted.created);
        let (records, _) = list_owned_jobs(&ctx, caller, None, 8, |_| true)
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].owner_node_id, local);
        assert_eq!(records[0].created_by, caller);
        assert_eq!(rows(&ctx, DOCUMENT_SYNC_OUTBOX_KEYSPACE).await, 0);
        assert_eq!(rows(&ctx, JOB_FAMILY_RECORD_KEYSPACE).await, 0);
        assert_eq!(rows(&ctx, JOB_FAMILY_OUTBOX_KEYSPACE).await, 0);

        let error = submit_local_execution(&ctx, run_config(local, caller))
            .await
            .expect_err("the second run exceeds the device ceiling");
        assert!(matches!(
            error,
            LocalExecutionError::Submit(SubmitJobError::ActiveJobLimit { limit }) if limit == 1
        ));
    }

    #[tokio::test]
    async fn refuses_device_launch() {
        // The realm's own launch path declines a device whatever it advertises,
        // so local compute never becomes a dispatch target.
        use crate::jobs::records::tests::fixture::Family;
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let local = node(1);
        let config = realm_config(local, RealmNodeKind::User { owner: owner(2) });
        write_config(&ctx, &config, local).await;
        crate::node_info::seed_node_info_document(
            &ctx,
            local,
            REALM,
            NodeUrls {
                api: None,
                s3: None,
            },
        )
        .await
        .unwrap();

        let family = Family::new([8u8; 32]);
        let spec = family.spec();
        let intent = family.launch(&spec, local, 0);
        let declined =
            crate::jobs::lifecycle::target::local_capability(&ctx, &config, local, &intent, &spec)
                .await
                .expect_err("a device is never a launch target");

        assert!(matches!(declined, LaunchDecline::Unauthorized));
    }

    #[tokio::test]
    async fn pins_output_destination() {
        // A declared output with no endpoint fails capture mid-run, so the
        // device names itself as the writer before the job exists.
        let dir = tempdir().unwrap();
        let ctx = device_ctx(dir.path().to_str().unwrap());
        let local = node(1);
        let caller = owner(2);
        let config = realm_config(local, RealmNodeKind::User { owner: caller });
        write_config(&ctx, &config, local).await;
        let mut run = run_config(local, caller);
        run.spec.file_outputs.push(OutputSelection {
            container_path: "/out/report.txt".to_string(),
            path_prefix: None,
            destination_node_id: None,
            destination: OutputDestination::S3 {
                bucket: "results".to_string(),
                key: "report.txt".to_string(),
            },
            name: None,
            description: None,
        });

        submit_local_execution(&ctx, run)
            .await
            .expect("the owner's run is accepted");

        let (records, _) = list_owned_jobs(&ctx, caller, None, 8, |_| true)
            .await
            .unwrap();
        let JobPayload::Execution(spec) = &records[0].payload else {
            panic!("expected an execution payload");
        };
        assert_eq!(spec.file_outputs[0].destination_node_id, Some(local));
    }

    #[tokio::test]
    async fn refuses_missing_input() {
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let local = node(1);
        let mut config = run_config(local, owner(2));
        config.spec.inputs.push(input("project", None, None));

        let error = resolve_inputs(&ctx, &config)
            .await
            .expect_err("an input the device does not hold is refused");

        assert!(matches!(
            error,
            LocalExecutionError::InputNotLocal { bucket, .. } if bucket == "project"
        ));
    }

    #[tokio::test]
    async fn refuses_floating_copy() {
        // A realm input is copied by exact version or not at all.
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let local = node(1);
        let mut config = run_config(local, owner(2));
        config
            .spec
            .inputs
            .push(input("project", Some(node(4)), None));

        let error = resolve_inputs(&ctx, &config)
            .await
            .expect_err("a realm input without a version is refused");

        assert!(matches!(
            error,
            LocalExecutionError::InputRefused { reason, .. } if reason.contains("exact version")
        ));
    }
}
