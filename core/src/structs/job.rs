use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use byteview::ByteView;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::NodeId;
use crate::compute::ExecutionTargetId;
use crate::errors::ConversionError;
use crate::structs::invert_timestamp_ms;
use crate::structs::{
    AuthContext, BackendLocation, HarvestJobSpec, HiddenBlobKey, MintPersistentIdSpec,
    PlacementPolicyRef, PlacementRef, RealmId, StagingStrategy, StoragePurgeResult,
    StoragePurgeScope, StoragePurgeSpec,
};
use crate::structured_id::{
    BucketId, FieldError, JobId as RoutableJobId, PlacementHandle, StructuredId,
};
use crate::types::{GroupId, Key, UserId};
use crate::util::tail_str;

/// Version prefix keeping the record wrappable in a version envelope later (#286).
pub const JOB_RECORD_KEY_PREFIX: &[u8] = b"jobs-v1/";

pub const JOB_DUE_INDEX_PREFIX: &[u8] = b"due/";
pub const JOB_LEASE_INDEX_PREFIX: &[u8] = b"lease/";
pub const JOB_PRUNE_INDEX_PREFIX: &[u8] = b"prune/";
/// Invalid UTF-8 byte separating generated report rows from user paths.
pub const JOB_SYSTEM_ENTRY_PREFIX: u8 = u8::MAX;
pub const DEFAULT_JOB_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// Creation-ordered job identifier stored at API and persistence boundaries.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct JobId(Ulid);

impl JobId {
    pub fn from_routable(job_id: RoutableJobId) -> Self {
        Self(job_id.as_ulid())
    }

    pub fn try_from_bytes(bytes: [u8; 16]) -> Result<Self, FieldError> {
        RoutableJobId::from_bytes(bytes).map(Self::from_routable)
    }

    /// Constructs an id from trusted structured bytes.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self::try_from_bytes(bytes).expect("job id bytes carry a structured placement handle")
    }

    pub fn from_parts(
        timestamp_ms: u64,
        handle: PlacementHandle,
        bucket: BucketId,
        nonce: u64,
    ) -> Result<Self, FieldError> {
        RoutableJobId::from_parts(timestamp_ms, handle, bucket, nonce).map(Self::from_routable)
    }

    pub fn as_routable(self) -> Result<RoutableJobId, FieldError> {
        RoutableJobId::from_bytes(self.to_bytes())
    }

    pub fn as_ulid(self) -> Ulid {
        self.0
    }

    pub fn to_bytes(&self) -> [u8; 16] {
        self.0.to_bytes()
    }

    pub fn timestamp_ms(&self) -> u64 {
        self.0.timestamp_ms()
    }
}

impl<'de> Deserialize<'de> for JobId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let ulid = Ulid::deserialize(deserializer)?;
        Self::try_from_bytes(ulid.to_bytes()).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JobId({})", self.0)
    }
}

impl FromStr for JobId {
    type Err = ConversionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        RoutableJobId::parse(value)
            .map(Self::from_routable)
            .map_err(|error| ConversionError::FromStrError(error.to_string()))
    }
}

/// Whether a payload runs in-process (idempotent, safe to requeue) or drives an
/// external attempt (a container that MUST NOT run twice). The lease sweep and
/// restart recovery branch on this to route external attempts to reconciliation
/// instead of a blind requeue.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JobExecutionClass {
    InProcess,
    ExternalAttempt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JobState {
    Queued,
    Claimed,
    Preparing,
    Ready,
    Running,
    Cancelling,
    Indeterminate,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobState::Succeeded | JobState::Failed | JobState::Cancelled
        )
    }

    /// Stable machine-readable name for API payloads. Never change an existing mapping.
    pub fn name(&self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Claimed => "claimed",
            JobState::Preparing => "preparing",
            JobState::Ready => "ready",
            JobState::Running => "running",
            JobState::Cancelling => "cancelling",
            JobState::Indeterminate => "indeterminate",
            JobState::Succeeded => "succeeded",
            JobState::Failed => "failed",
            JobState::Cancelled => "cancelled",
        }
    }
}

/// How an input is exposed to the task. Reference modes stage a copy today; the
/// non-copying durable binding waits on native reference reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InputMode {
    Snapshot,
    Mount,
    /// Membership is fixed while the source content may advance.
    FloatingReference,
    /// Bound to the exact source version the request names.
    ExactReference,
}

/// How a composition resolves a destination key that is already claimed, either
/// by another input or by an object already in the destination bucket.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollisionPolicy {
    #[default]
    Reject,
    Replace,
    KeepExisting,
}

impl CollisionPolicy {
    /// Stable wire and log spelling; never change an existing mapping.
    pub const fn name(self) -> &'static str {
        match self {
            CollisionPolicy::Reject => "reject",
            CollisionPolicy::Replace => "replace",
            CollisionPolicy::KeepExisting => "keep_existing",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CompositionError {
    #[error("composition key conflict on `{0}`")]
    KeyConflict(String),
    #[error("exact_reference input `{0}` requires a source version")]
    MissingVersion(String),
    #[error("floating_reference input `{0}` must not pin a source version")]
    PinnedVersion(String),
}

/// Resolve one destination key per input under `policy`. `Replace` keeps the last
/// claim, `KeepExisting` the first, and `Reject` fails the whole composition.
pub fn plan_composition(
    inputs: Vec<InputSelection>,
    policy: CollisionPolicy,
) -> Result<Vec<InputSelection>, CompositionError> {
    let mut planned: Vec<InputSelection> = Vec::with_capacity(inputs.len());
    for input in inputs {
        validate_input_version(&input)?;
        match planned
            .iter()
            .position(|existing| existing.dest_key == input.dest_key)
        {
            None => planned.push(input),
            Some(_) if policy == CollisionPolicy::KeepExisting => {}
            Some(index) if policy == CollisionPolicy::Replace => planned[index] = input,
            Some(_) => return Err(CompositionError::KeyConflict(input.dest_key)),
        }
    }
    Ok(planned)
}

fn validate_input_version(input: &InputSelection) -> Result<(), CompositionError> {
    let InputSource::S3 { version_id, .. } = &input.source;
    match input.mode {
        InputMode::ExactReference if version_id.is_none() => {
            Err(CompositionError::MissingVersion(input.dest_key.clone()))
        }
        InputMode::FloatingReference if version_id.is_some() => {
            Err(CompositionError::PinnedVersion(input.dest_key.clone()))
        }
        InputMode::Snapshot
        | InputMode::Mount
        | InputMode::FloatingReference
        | InputMode::ExactReference => Ok(()),
    }
}

/// Where an input comes from. v1 supports internal S3 objects only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InputSource {
    S3 {
        bucket: String,
        key: String,
        version_id: Option<String>,
    },
}

/// One declared input and where it lands in the workspace bucket.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputSelection {
    pub source: InputSource,
    /// Node-local endpoint that owns the source object once the job is stored.
    pub source_node_id: Option<crate::NodeId>,
    /// Input name, unique across the run; nothing copies it into a bucket.
    pub dest_key: String,
    pub mode: InputMode,
    /// Absolute path inside the container; submission refuses an input without one.
    pub container_path: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputDestination {
    S3 { bucket: String, key: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputSelection {
    /// Absolute container path, which may carry POSIX 12.13 wildcards.
    pub container_path: String,
    /// Literal ancestor stripped from every matched path to build the
    /// destination key. Required with wildcards, absent otherwise.
    pub path_prefix: Option<String>,
    /// Node-local endpoint that owns the destination once the job is stored.
    pub destination_node_id: Option<crate::NodeId>,
    pub destination: OutputDestination,
    pub name: Option<String>,
    pub description: Option<String>,
}

/// A native output intent bound to the workspace bucket, which is derived from
/// the `JobId` and therefore unknown at submit time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceOutput {
    pub container_path: String,
    /// Destination key inside the workspace bucket.
    pub dest_key: String,
}

/// Resource ceilings requested for the container. `None` fills from backend defaults.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputeResources {
    pub cpu_cores: Option<u32>,
    pub ram_bytes: Option<u64>,
    pub disk_bytes: Option<u64>,
    pub max_walltime_ms: Option<u64>,
    pub preemptible: bool,
}

/// The container plan carried by a `JobPayload::Execution`. Bounded per spec 16.2.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionSpec {
    /// Workspace parent group; also the credential/crate authorization scope.
    pub group_id: GroupId,
    pub name: Option<String>,
    pub description: Option<String>,
    pub tags: BTreeMap<String, String>,
    pub image: String,
    /// Overrides the image ENTRYPOINT when set.
    pub entrypoint: Option<Vec<String>>,
    pub command: Vec<String>,
    pub workdir: Option<String>,
    pub env: BTreeMap<String, String>,
    pub resources: ComputeResources,
    /// Pin a backend wire kind (`docker`); `None` runs on any enabled backend.
    pub executor_constraint: Option<String>,
    pub inputs: Vec<InputSelection>,
    pub file_outputs: Vec<OutputSelection>,
    /// Native output intents, materialized into `file_outputs` by
    /// `resolve_outputs` once the workspace bucket name exists.
    pub workspace_outputs: Vec<WorkspaceOutput>,
    /// Declared output prefixes in the workspace, inventoried at completion.
    pub output_prefixes: Vec<String>,
    /// How a claimed destination key is resolved while composing the workspace.
    pub collision_policy: CollisionPolicy,
}

/// Exact source details resolved at ingress and carried into the stored job
/// spec, so a forwarded planner never reinterprets a bucket on its own node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapturedInput {
    pub destination_key: String,
    pub source_node_id: crate::NodeId,
    pub version_id: Ulid,
    pub blake3: [u8; 32],
    pub bytes: u64,
    pub policies: Vec<PlacementPolicyRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagingJobItem {
    pub source_path: String,
    pub target_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagingJobPrefix {
    pub source_prefix: String,
    pub target_prefix: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagingJobSpec {
    pub auth_context: AuthContext,
    pub group_id: GroupId,
    pub node_id: NodeId,
    pub connector_id: Ulid,
    pub bucket: String,
    pub strategy: StagingStrategy,
    pub items: Vec<StagingJobItem>,
    pub prefixes: Vec<StagingJobPrefix>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StagingJobPhase {
    Queued,
    Discovering,
    Inspecting,
    Registering,
    Downloading,
    Writing,
    Completed,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagingJobError {
    pub source_path: String,
    pub target_key: String,
    pub error: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagingJobDirectory {
    pub source_path: String,
    pub target_prefix: String,
    pub offset: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagingPendingItem {
    pub source_path: String,
    pub target_key: String,
    pub size: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagingJobCheckpoint {
    pub phase: StagingJobPhase,
    pub pending_items: Vec<StagingPendingItem>,
    pub pending_directories: Vec<StagingJobDirectory>,
    pub items_current: u64,
    pub items_succeeded: u64,
    pub items_failed: u64,
    pub items_total: Option<u64>,
    pub bytes_current: u64,
    pub bytes_total: Option<u64>,
    pub bytes_discovered: u64,
    pub unknown_sizes: u64,
    pub current_path: Option<String>,
    pub errors: Vec<StagingJobError>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImportRoCrateSource {
    Upload {
        upload_id: Ulid,
    },
    Object {
        bucket: String,
        key: String,
        version: Option<Ulid>,
    },
    Connector {
        group_id: GroupId,
        connector_id: Ulid,
        path: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportRoCrateTarget {
    pub bucket: String,
    pub prefix: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportMetadataTarget {
    pub group_id: GroupId,
    pub path: String,
    pub public: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoCrateLimits {
    pub direct_upload_bytes: u64,
    pub import_source_bytes: u64,
    pub expanded_import_bytes: u64,
    pub export_artifact_bytes: u64,
    pub max_entries: u64,
    pub metadata_bytes: u64,
    pub key_bytes: u64,
    pub upload_retention_ms: u64,
    pub artifact_retention_ms: u64,
    pub max_active_jobs: u32,
    pub holder_ttl_ms: u64,
    pub holder_refresh_ms: u64,
}

impl Default for RoCrateLimits {
    fn default() -> Self {
        const GIB: u64 = 1024 * 1024 * 1024;
        const DAY_MS: u64 = 24 * 60 * 60 * 1000;
        Self {
            direct_upload_bytes: 8 * GIB,
            import_source_bytes: 100 * GIB,
            expanded_import_bytes: 100 * GIB,
            export_artifact_bytes: 100 * GIB,
            max_entries: 100_000,
            metadata_bytes: 16 * 1024 * 1024,
            key_bytes: 1024,
            upload_retention_ms: DAY_MS,
            artifact_retention_ms: DEFAULT_JOB_RETENTION_MS,
            max_active_jobs: 4,
            holder_ttl_ms: DAY_MS,
            holder_refresh_ms: 8 * 60 * 60 * 1000,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportRoCrateSpec {
    pub auth_context: AuthContext,
    pub source: ImportRoCrateSource,
    pub target: ImportRoCrateTarget,
    pub metadata: ImportMetadataTarget,
    pub limits: RoCrateLimits,
    pub document_id: Ulid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportRoCrateSpec {
    pub auth_context: AuthContext,
    pub document_id: Ulid,
    pub limits: RoCrateLimits,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasonCode {
    Imported,
    Unlisted,
    Failed,
    NotAttempted,
    Included,
    External,
    Denied,
    Missing,
    Offline,
    Unsupported,
    PathSynthesized,
    UnrewrittenReference,
    SignatureDropped,
    UnsupportedCrateVersion,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobReportRow<T> {
    pub entry_key: String,
    pub code: ReasonCode,
    pub message: Option<String>,
    pub detail: T,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportReportDetail {
    pub archive_path: String,
    pub target_key: Option<String>,
    pub version_id: Option<Ulid>,
    pub blake3: Option<String>,
    pub size: Option<u64>,
    pub arn: Option<String>,
    pub w3id: Option<String>,
    pub validation: Option<crate::metadata::MetadataValidationViolation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportReportSource {
    Local,
    Remote,
    Hash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportReportDetail {
    pub entity_id: String,
    pub zip_path: Option<String>,
    pub source: Option<ExportReportSource>,
    pub resolved_version: Option<Ulid>,
    pub validation: Option<crate::metadata::MetadataValidationViolation>,
}

pub type ImportReportRow = JobReportRow<ImportReportDetail>;
pub type ExportReportRow = JobReportRow<ExportReportDetail>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub location: BackendLocation,
    pub blake3: [u8; 32],
    pub size: u64,
    pub expires_at_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoCrateMediaType {
    Zip,
    Eln,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoCrateUploadRecord {
    pub upload_id: Ulid,
    pub owner: UserId,
    pub location: BackendLocation,
    pub blake3: [u8; 32],
    pub size: u64,
    pub media_type: RoCrateMediaType,
    pub expires_at_ms: u64,
    pub claimed_by: Option<JobId>,
}

/// Durable compensation for a hidden upload whose record cleanup was ambiguous.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoCrateUploadCleanup {
    pub upload_id: Ulid,
    pub hidden_key: HiddenBlobKey,
}

impl RoCrateUploadCleanup {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoCrateCheckpointRefs {
    pub hidden_locations: Vec<BackendLocation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportRoCrateResult {
    pub document_id: Option<Ulid>,
    pub entries_total: u64,
    pub imported: u64,
    pub unlisted: u64,
    pub failed: u64,
    pub report_digest: [u8; 32],
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportOmissionCounts {
    pub external: u64,
    pub denied: u64,
    pub missing: u64,
    pub offline: u64,
    pub unsupported: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportRoCrateResult {
    pub artifact: Option<ArtifactRef>,
    pub included: u64,
    pub omitted: ExportOmissionCounts,
    pub report_digest: [u8; 32],
}

impl ExecutionSpec {
    /// Materialize workspace output intents against the resolved bucket.
    /// Deterministic across retries: the bucket name derives from the `JobId`.
    pub fn resolve_outputs(&mut self, bucket: &str, node_id: crate::NodeId) {
        for output in std::mem::take(&mut self.workspace_outputs) {
            self.file_outputs.push(OutputSelection {
                container_path: output.container_path,
                path_prefix: None,
                destination_node_id: Some(node_id),
                destination: OutputDestination::S3 {
                    bucket: bucket.to_string(),
                    key: output.dest_key,
                },
                name: None,
                description: None,
            });
        }
    }
}

/// Closed job payload enum, keeping the typed-queue discipline of `TaskKey` and
/// `DocumentSyncOutboxEvent`. Additive-only until a version envelope lands (#286).
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobPayload {
    /// Test-only executor. Idempotency key: the `cleanup_marker` file, which a
    /// re-driven or cancelled Probe removes so re-running from scratch is safe.
    Probe {
        steps: u32,
        step_sleep_ms: u64,
        fail_at: Option<u32>,
        panic_at: Option<u32>,
        cleanup_marker: Option<String>,
    },
    /// Run a container against an S3 workspace; the sole `ExternalAttempt` payload.
    Execution(ExecutionSpec),
    /// Follow-on internal obligation: write the run crate for a finished execution
    /// job. Idempotent by dedup key `run-crate/{JobId}`; a failure never affects
    /// the parent job.
    WriteRunCrate {
        for_job: JobId,
    },
    /// Durable internal obligation to revoke the workspace credential and remove
    /// the terminal backend attempt.
    TerminalCleanup {
        for_job: JobId,
        attempt: Option<AttemptIntent>,
        access_key: String,
    },
    Staging(StagingJobSpec),
    ImportRoCrate(ImportRoCrateSpec),
    ExportRoCrate(ExportRoCrateSpec),
    /// One run of a repository harvest source. Idempotent by harvest provenance
    /// keyed on `(namespace, source record id)`; safe to requeue.
    Harvest(HarvestJobSpec),
    /// Idempotent w3id persistent-identifier registration for a document.
    /// Idempotency key is the document id; a re-mint returns the same PID.
    MintPersistentId(MintPersistentIdSpec),
    /// One server-side permanent purge family, scoped to a file, prefix, or bucket.
    StoragePurge(StoragePurgeSpec),
}

impl JobPayload {
    /// Stable discriminant string. Payload internals are never echoed verbatim.
    pub fn kind(&self) -> &'static str {
        match self {
            JobPayload::Probe { .. } => "probe",
            JobPayload::Execution(_) => "execution",
            JobPayload::Staging(_) => "staging",
            JobPayload::ImportRoCrate(_) => "import_rocrate",
            JobPayload::ExportRoCrate(_) => "export_rocrate",
            JobPayload::WriteRunCrate { .. } => "write_run_crate",
            JobPayload::TerminalCleanup { .. } => "terminal_cleanup",
            JobPayload::Harvest(_) => "harvest",
            JobPayload::MintPersistentId(_) => "mint_persistent_id",
            JobPayload::StoragePurge(_) => "storage_purge",
        }
    }

    /// Default progress unit for a freshly submitted job of this kind.
    pub fn progress_unit(&self) -> &'static str {
        match self {
            JobPayload::Probe { .. } => "steps",
            JobPayload::Execution(_) => "phases",
            JobPayload::Staging(_)
            | JobPayload::ImportRoCrate(_)
            | JobPayload::ExportRoCrate(_) => "items",
            JobPayload::Harvest(_) => "records",
            JobPayload::StoragePurge(_) => "entries",
            JobPayload::MintPersistentId(_)
            | JobPayload::WriteRunCrate { .. }
            | JobPayload::TerminalCleanup { .. } => "steps",
        }
    }

    /// Execution class. Internal payloads are safe to requeue; external attempts
    /// are not. Only `Execution` drives an external container.
    pub fn execution_class(&self) -> JobExecutionClass {
        match self {
            JobPayload::Probe { .. }
            | JobPayload::Staging(_)
            | JobPayload::ImportRoCrate(_)
            | JobPayload::ExportRoCrate(_)
            | JobPayload::Harvest(_)
            | JobPayload::StoragePurge(_)
            | JobPayload::MintPersistentId(_)
            | JobPayload::WriteRunCrate { .. }
            | JobPayload::TerminalCleanup { .. } => JobExecutionClass::InProcess,
            JobPayload::Execution(_) => JobExecutionClass::ExternalAttempt,
        }
    }

    pub fn is_internal(&self) -> bool {
        matches!(
            self,
            JobPayload::WriteRunCrate { .. } | JobPayload::TerminalCleanup { .. }
        )
    }

    pub fn is_rocrate(&self) -> bool {
        matches!(
            self,
            JobPayload::ImportRoCrate(_) | JobPayload::ExportRoCrate(_)
        )
    }

    /// Whether the dedup row is reclaimed when the job is pruned rather than when
    /// it reaches a terminal state. Replaying the request while the job is still
    /// retained must resolve to the same job identity and the same result.
    pub fn dedup_until_prune(&self) -> bool {
        self.is_rocrate()
            || matches!(
                self,
                JobPayload::MintPersistentId(_) | JobPayload::StoragePurge(_)
            )
    }

    pub fn rocrate_limits(&self) -> Option<&RoCrateLimits> {
        match self {
            JobPayload::ImportRoCrate(spec) => Some(&spec.limits),
            JobPayload::ExportRoCrate(spec) => Some(&spec.limits),
            _ => None,
        }
    }

    /// Canonical plan digest: BLAKE3 over the logical postcard payload.
    /// The same idempotency identity with a matching digest is an idempotent
    /// create; a differing digest is a `JobPlanConflict`.
    pub fn plan_digest(&self) -> [u8; 32] {
        let bytes = match self {
            JobPayload::ImportRoCrate(spec) => {
                let mut spec = spec.clone();
                spec.document_id = Ulid::nil();
                postcard::to_allocvec(&JobPayload::ImportRoCrate(spec))
            }
            // Idempotency is the document id alone: a re-mint by a different user
            // must match, not conflict, so the minter is excluded from the digest.
            JobPayload::MintPersistentId(spec) => {
                let mut spec = spec.clone();
                spec.minted_by = UserId::default();
                postcard::to_allocvec(&JobPayload::MintPersistentId(spec))
            }
            _ => postcard::to_allocvec(self),
        }
        .expect("payload postcard is infallible");
        *blake3::hash(&bytes).as_bytes()
    }
}

const ATTEMPT_FENCE_DOMAIN: &[u8] = b"aruna-attempt-fence-v1";

/// Deterministic external identity of one attempt, recorded write-ahead before any
/// external submit so a lost attempt can be adopted by name on reconcile.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptIntent {
    pub attempt_no: u32,
    pub external_name: String,
    pub executor_kind: String,
    pub pinned_image: String,
    pub attempt_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptControl {
    pub attempt_epoch: u64,
    /// Physical execution this fenced attempt is, minted with the control row and
    /// never reused by another attempt.
    pub execution_id: Ulid,
    pub controller_generation: u64,
    pub bound_token: Option<Ulid>,
    pub tombstone_ref: Option<String>,
    /// Write-ahead output commit identities of this physical execution.
    pub output_commits: Vec<OutputCommitIntent>,
    /// Digest of this execution's durable signed [`ExecutionOutputRecord`].
    /// Terminal success reads it in its own transaction, so an execution can
    /// never succeed before its exact output set is durable.
    pub output_record: Option<[u8; 32]>,
}

impl AttemptControl {
    /// Local fence this execution's output record binds itself to until the
    /// family rounds publish a real [`ExecutionReceipt`] to bind instead.
    pub fn fence_digest(&self, job_id: JobId) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(ATTEMPT_FENCE_DOMAIN);
        hasher.update(&job_id.to_bytes());
        hasher.update(&self.attempt_epoch.to_be_bytes());
        hasher.update(&self.execution_id.to_bytes());
        *hasher.finalize().as_bytes()
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// Reserve one VersionId per destination this execution has not committed
    /// yet, keeping every existing reservation so a replayed capture reuses it
    /// instead of creating a second version. Reports whether anything was added.
    pub fn reserve_outputs<F>(
        &mut self,
        destinations: &[(crate::NodeId, String, String)],
        mut mint: F,
    ) -> bool
    where
        F: FnMut() -> Ulid,
    {
        let mut reserved: BTreeSet<(crate::NodeId, String, String)> = self
            .output_commits
            .iter()
            .map(|commit| (commit.node_id, commit.bucket.clone(), commit.key.clone()))
            .collect();
        let mut changed = false;
        for (node_id, bucket, key) in destinations {
            if reserved.insert((*node_id, bucket.clone(), key.clone())) {
                self.output_commits.push(OutputCommitIntent {
                    node_id: *node_id,
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: mint(),
                });
                changed = true;
            }
        }
        changed
    }
}

/// Write-ahead identity of one output commit, persisted before the write so a
/// replayed capture reuses this VersionId instead of creating a second version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputCommitIntent {
    pub node_id: crate::NodeId,
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
}

pub fn attempt_control_key(job_id: JobId, attempt_epoch: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(24);
    key.extend_from_slice(&job_id.to_bytes());
    key.extend_from_slice(&attempt_epoch.to_be_bytes());
    key
}

/// Reconciliation key: the container name, K8s Job name, or Slurm job-name an
/// attempt deterministically owns.
pub fn attempt_external_name(job_id: JobId, attempt_no: u32) -> String {
    format!("aruna-{}-a{attempt_no}", job_id.to_string().to_lowercase())
}

/// Encode a `job_dedup_index` value: `job_id (16) || plan_digest (32)`.
pub fn encode_job_dedup_value(job_id: JobId, plan_digest: [u8; 32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(48);
    bytes.extend_from_slice(&job_id.to_bytes());
    bytes.extend_from_slice(&plan_digest);
    bytes
}

pub fn parse_job_dedup_value(bytes: &[u8]) -> Result<(JobId, [u8; 32]), ConversionError> {
    if bytes.len() != 48 {
        return Err(ConversionError::InvalidLength(format!(
            "expected 48-byte dedup value, got {}",
            bytes.len()
        )));
    }
    let job_id = JobId::try_from_bytes(bytes[..16].try_into()?)
        .map_err(|error| ConversionError::FromStrError(error.to_string()))?;
    let plan_digest: [u8; 32] = bytes[16..48].try_into()?;
    Ok((job_id, plan_digest))
}

/// One output object captured at completion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputObject {
    /// Node-local endpoint that owns this exact version.
    pub node_id: crate::NodeId,
    pub bucket: String,
    pub key: String,
    /// Exact version this write created. Two executions writing one key keep two
    /// retrievable versions, so the identity may never be discarded.
    pub version_id: Ulid,
    /// Physical execution that produced the object.
    pub execution_id: Ulid,
    pub container_path: String,
    pub size: u64,
    /// Hex BLAKE3 digest when known.
    pub digest: Option<String>,
}

/// Closed result enum parallel to `JobPayload`.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobResultPayload {
    Probe {
        completed_steps: u32,
    },
    Execution {
        /// Container exit code; `None` when the outcome is evidence-free.
        exit_code: Option<i32>,
        workspace_bucket: Option<String>,
        outputs: Vec<OutputObject>,
        stdout: String,
        stderr: String,
        /// Digest of the durable signed output record. Present exactly on a
        /// terminal success, which cannot commit without it.
        output_digest: Option<[u8; 32]>,
    },
    RunCrate {
        resource: String,
    },
    Cleanup,
    Staging {
        completed_items: u64,
        failed_items: u64,
    },
    ImportRoCrate(ImportRoCrateResult),
    ExportRoCrate(ExportRoCrateResult),
    Harvest {
        minted: u64,
        updated: u64,
        tombstoned: u64,
        skipped: u64,
    },
    PersistentId {
        pid: String,
        newly_minted: bool,
    },
    StoragePurge(StoragePurgeResult),
}

impl JobResultPayload {
    pub fn kind(&self) -> &'static str {
        match self {
            JobResultPayload::Probe { .. } => "probe",
            JobResultPayload::Execution { .. } => "execution",
            JobResultPayload::RunCrate { .. } => "run_crate",
            JobResultPayload::Cleanup => "cleanup",
            JobResultPayload::Staging { .. } => "staging",
            JobResultPayload::ImportRoCrate(_) => "import_rocrate",
            JobResultPayload::ExportRoCrate(_) => "export_rocrate",
            JobResultPayload::Harvest { .. } => "harvest",
            JobResultPayload::PersistentId { .. } => "persistent_id",
            JobResultPayload::StoragePurge(_) => "storage_purge",
        }
    }

    /// Terminal success requires every captured output to name the exact version
    /// it created and the physical execution that produced it, so a duplicate
    /// execution stays attributable and a replay stays idempotent.
    pub fn check_outputs(&self, execution_id: Ulid) -> Result<(), JobRecordError> {
        let JobResultPayload::Execution { outputs, .. } = self else {
            return Ok(());
        };
        if execution_id.is_nil() {
            return Err(JobRecordError::OutputIdentity);
        }
        let mut seen = BTreeSet::new();
        for output in outputs {
            if output.version_id.is_nil() || output.execution_id != execution_id {
                return Err(JobRecordError::OutputIdentity);
            }
            if !seen.insert((
                output.node_id,
                &output.bucket,
                &output.key,
                output.version_id,
            )) {
                return Err(JobRecordError::OutputIdentity);
            }
        }
        Ok(())
    }

    /// The storage-level success invariant: every output names a version this
    /// execution reserved before writing, and the exact immutable output record
    /// is already durable under the digest the result names.
    pub fn proves_outputs(&self, control: &AttemptControl) -> Result<(), JobRecordError> {
        let JobResultPayload::Execution {
            outputs,
            output_digest,
            ..
        } = self
        else {
            return Err(JobRecordError::OutputIdentity);
        };
        self.check_outputs(control.execution_id)?;
        let reserved: BTreeSet<(crate::NodeId, &str, &str, Ulid)> = control
            .output_commits
            .iter()
            .map(|commit| {
                (
                    commit.node_id,
                    commit.bucket.as_str(),
                    commit.key.as_str(),
                    commit.version_id,
                )
            })
            .collect();
        if outputs.iter().any(|output| {
            !reserved.contains(&(
                output.node_id,
                output.bucket.as_str(),
                output.key.as_str(),
                output.version_id,
            ))
        }) {
            return Err(JobRecordError::OutputIdentity);
        }
        match control.output_record.is_some() && *output_digest == control.output_record {
            true => Ok(()),
            false => Err(JobRecordError::MissingEvidence(JobRecordKind::Output)),
        }
    }

    /// Payload-specific public projection returned by the REST surface.
    pub fn to_public_json(&self) -> serde_json::Value {
        match self {
            JobResultPayload::Probe { completed_steps } => {
                serde_json::json!({ "completed_steps": completed_steps })
            }
            JobResultPayload::Execution {
                exit_code,
                workspace_bucket,
                outputs,
                stdout,
                stderr,
                output_digest,
            } => serde_json::json!({
                "exit_code": exit_code,
                "workspace_bucket": workspace_bucket,
                "stdout": stdout,
                "stderr": stderr,
                "output_record": output_digest.map(hex::encode),
                "outputs": outputs
                    .iter()
                    .map(|output| serde_json::json!({
                        "bucket": output.bucket,
                        "key": output.key,
                        "version_id": output.version_id.to_string(),
                        "execution_id": output.execution_id.to_string(),
                        "container_path": output.container_path,
                        "size": output.size,
                        "digest": output.digest,
                    }))
                    .collect::<Vec<_>>(),
            }),
            JobResultPayload::RunCrate { resource } => {
                serde_json::json!({ "resource": resource })
            }
            JobResultPayload::Cleanup => serde_json::json!({}),
            JobResultPayload::Staging {
                completed_items,
                failed_items,
            } => serde_json::json!({
                "completed_items": completed_items,
                "failed_items": failed_items,
            }),
            JobResultPayload::ImportRoCrate(result) => serde_json::json!({
                "document_id": result.document_id.map(|id| id.to_string()),
                "entries_total": result.entries_total,
                "imported": result.imported,
                "unlisted": result.unlisted,
                "failed": result.failed,
                "report_digest": hex::encode(result.report_digest),
            }),
            JobResultPayload::ExportRoCrate(result) => serde_json::json!({
                "artifact": result.artifact.as_ref().map(|artifact| serde_json::json!({
                    "blake3": hex::encode(artifact.blake3),
                    "size": artifact.size,
                    "expires_at_ms": artifact.expires_at_ms,
                })),
                "included": result.included,
                "omitted": {
                    "external": result.omitted.external,
                    "denied": result.omitted.denied,
                    "missing": result.omitted.missing,
                    "offline": result.omitted.offline,
                    "unsupported": result.omitted.unsupported,
                },
                "report_digest": hex::encode(result.report_digest),
            }),
            JobResultPayload::Harvest {
                minted,
                updated,
                tombstoned,
                skipped,
            } => serde_json::json!({
                "minted": minted,
                "updated": updated,
                "tombstoned": tombstoned,
                "skipped": skipped,
            }),
            JobResultPayload::PersistentId { pid, newly_minted } => serde_json::json!({
                "pid": pid,
                "newly_minted": newly_minted,
            }),
            JobResultPayload::StoragePurge(result) => {
                let scope = match &result.scope {
                    StoragePurgeScope::File { bucket, key } => {
                        serde_json::json!({"kind": "file", "bucket": bucket, "key": key})
                    }
                    StoragePurgeScope::Prefix { bucket, prefix } => serde_json::json!({
                        "kind": "prefix",
                        "bucket": bucket,
                        "prefix": prefix,
                    }),
                    StoragePurgeScope::Bucket { bucket } => {
                        serde_json::json!({"kind": "bucket", "bucket": bucket})
                    }
                };
                serde_json::json!({
                    "scope": scope,
                    "versions_removed": result.versions_removed,
                    "multipart_uploads_removed": result.multipart_uploads_removed,
                    "batches_completed": result.batches_completed,
                    "bucket_deleted": result.bucket_deleted,
                    "emptiness_proven": result.emptiness_proven,
                })
            }
        }
    }
}

/// Terminal outcome of the run-crate obligation, stored in a side keyspace so the
/// immutable terminal parent record is never rewritten. Surfaced on the job.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunCrateStatus {
    Pending,
    Minted { document_id: Ulid },
    Written { resource: String },
    Denied { message: String },
    Failed { message: String },
}

impl RunCrateStatus {
    pub fn name(&self) -> &'static str {
        match self {
            RunCrateStatus::Pending => "pending",
            RunCrateStatus::Minted { .. } => "pending",
            RunCrateStatus::Written { .. } => "written",
            RunCrateStatus::Denied { .. } => "denied",
            RunCrateStatus::Failed { .. } => "failed",
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn to_public_json(&self) -> serde_json::Value {
        match self {
            RunCrateStatus::Pending => serde_json::json!({ "status": "pending" }),
            RunCrateStatus::Minted { .. } => serde_json::json!({ "status": "pending" }),
            RunCrateStatus::Written { resource } => {
                serde_json::json!({ "status": "written", "resource": resource })
            }
            RunCrateStatus::Denied { message } => {
                serde_json::json!({ "status": "denied", "message": message })
            }
            RunCrateStatus::Failed { message } => {
                serde_json::json!({ "status": "failed", "message": message })
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobErrorKind {
    Retryable,
    Permanent,
}

impl JobErrorKind {
    pub fn name(&self) -> &'static str {
        match self {
            JobErrorKind::Retryable => "retryable",
            JobErrorKind::Permanent => "permanent",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobError {
    pub message: String,
    pub kind: JobErrorKind,
}

impl JobError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: JobErrorKind::Retryable,
        }
    }

    pub fn permanent(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: JobErrorKind::Permanent,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobProgress {
    pub current: u64,
    pub total: Option<u64>,
    pub unit: String,
}

impl JobProgress {
    pub fn new(unit: impl Into<String>) -> Self {
        Self {
            current: 0,
            total: None,
            unit: unit.into(),
        }
    }
}

/// Lease on the job; `claim_token` fences zombie executors on every write.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobClaim {
    pub holder_node_id: NodeId,
    pub claim_token: Ulid,
    pub lease_expires_at_ms: u64,
}

/// Which bucket a run writes into: none of its own, or one the caller owns.
/// Stored records changed shape here, because the per-run `ws-` workspace
/// variants are gone and their encodings no longer decode.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkspaceMode {
    #[default]
    None,
    Existing,
}

impl WorkspaceMode {
    pub fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Existing => "existing",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRecord {
    pub job_id: JobId,
    pub payload: JobPayload,
    pub state: JobState,
    pub created_by: UserId,
    pub owner_node_id: NodeId,
    pub created_at_ms: u64,
    pub started_at_ms: Option<u64>,
    pub updated_at_ms: u64,
    pub due_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub attempts: u32,
    pub next_attempt_epoch: u64,
    pub has_run: bool,
    pub last_error: Option<JobError>,
    pub progress: JobProgress,
    pub cancel_requested: bool,
    pub claim: Option<JobClaim>,
    pub dedup_key: Option<Vec<u8>>,
    pub result: Option<JobResultPayload>,
    pub execution_class: JobExecutionClass,
    pub plan_digest: Option<[u8; 32]>,
    pub attempt_intent: Option<AttemptIntent>,
    /// The caller's bucket an `Existing`-mode run works inside.
    pub workspace_bucket: Option<String>,
    pub workspace_mode: WorkspaceMode,
    /// Resolved source details copied from the stored family for physical staging.
    pub captured_inputs: Vec<CapturedInput>,
    pub report_digest: Option<[u8; 32]>,
    pub retention_ms: u64,
    /// Local attempts are spent without job-specific evidence: no further attempt
    /// runs here, yet the distributed outcome stays `Indeterminate`.
    pub locally_exhausted: bool,
}

impl JobRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        job_id: JobId,
        payload: JobPayload,
        created_by: UserId,
        owner_node_id: NodeId,
        created_at_ms: u64,
        due_at_ms: u64,
        dedup_key: Option<Vec<u8>>,
    ) -> Self {
        let unit = payload.progress_unit();
        let execution_class = payload.execution_class();
        let plan_digest = Some(payload.plan_digest());
        Self {
            job_id,
            payload,
            state: JobState::Queued,
            created_by,
            owner_node_id,
            created_at_ms,
            started_at_ms: None,
            updated_at_ms: created_at_ms,
            due_at_ms,
            finished_at_ms: None,
            attempts: 0,
            next_attempt_epoch: 1,
            has_run: false,
            last_error: None,
            progress: JobProgress::new(unit),
            cancel_requested: false,
            claim: None,
            dedup_key,
            result: None,
            execution_class,
            plan_digest,
            attempt_intent: None,
            workspace_bucket: None,
            workspace_mode: WorkspaceMode::default(),
            captured_inputs: Vec::new(),
            report_digest: None,
            retention_ms: DEFAULT_JOB_RETENTION_MS,
            locally_exhausted: false,
        }
    }

    /// No further attempt will be started here: either the job is terminal or it
    /// spent its local attempts without evidence.
    pub fn is_settled(&self) -> bool {
        self.state.is_terminal() || self.locally_exhausted
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("illegal job transition {from:?} -> {to:?}")]
pub struct JobTransitionError {
    pub from: JobState,
    pub to: JobState,
}

/// Pure state-machine guard, guarded by execution class so in-process jobs keep the
/// original graph exactly and only external attempts use the extended states.
/// Terminal states absorb nothing: any transition out of a terminal state is rejected.
pub fn validate_transition(
    class: JobExecutionClass,
    from: JobState,
    to: JobState,
) -> Result<(), JobTransitionError> {
    let legal = match class {
        JobExecutionClass::InProcess => in_process_transition(from, to),
        JobExecutionClass::ExternalAttempt => external_attempt_transition(from, to),
    };
    if legal {
        Ok(())
    } else {
        Err(JobTransitionError { from, to })
    }
}

fn in_process_transition(from: JobState, to: JobState) -> bool {
    use JobState::*;
    matches!(
        (from, to),
        (Queued, Claimed)
            | (Queued, Cancelled)
            | (Claimed, Running)
            | (Claimed, Queued)
            | (Claimed, Cancelled)
            | (Claimed, Failed)
            | (Running, Succeeded)
            | (Running, Failed)
            | (Running, Cancelled)
            | (Running, Queued)
            // Local retry exhaustion without a job-specific verdict parks here too.
            | (Claimed, Indeterminate)
            | (Running, Indeterminate)
    )
}

/// The fenced execution graph (spec 16.7): a requeue is legal only before an attempt
/// is submitted; `Indeterminate` exits only on evidence. `Ready -> Indeterminate`
/// parks a submit whose outcome is unknowable after the intent was written;
/// `Preparing/Ready -> Failed` terminalizes a permanent pre-attempt failure.
fn external_attempt_transition(from: JobState, to: JobState) -> bool {
    use JobState::*;
    matches!(
        (from, to),
        (Queued, Claimed)
            | (Queued, Cancelled)
            | (Claimed, Preparing)
            | (Claimed, Queued)
            | (Claimed, Cancelled)
            | (Claimed, Failed)
            // Pre-submit exhaustion parks: no container exists, so nothing is proven.
            | (Claimed, Indeterminate)
            | (Preparing, Indeterminate)
            | (Preparing, Ready)
            | (Preparing, Queued)
            | (Preparing, Failed)
            // Pre-attempt cancels: only ever taken while `attempt_intent` is still None,
            // so no container can exist. Without these a cancel cannot terminalize until
            // the job reaches Running, and TES sticks in CANCELING.
            | (Preparing, Cancelled)
            | (Ready, Cancelled)
            | (Ready, Running)
            | (Ready, Queued)
            | (Ready, Failed)
            | (Ready, Indeterminate)
            | (Ready, Cancelling)
            | (Running, Succeeded)
            | (Running, Failed)
            | (Running, Cancelling)
            | (Running, Indeterminate)
            | (Cancelling, Cancelled)
            | (Cancelling, Succeeded)
            | (Cancelling, Failed)
            | (Cancelling, Indeterminate)
            | (Indeterminate, Running)
            | (Indeterminate, Cancelling)
            | (Indeterminate, Succeeded)
            | (Indeterminate, Failed)
            | (Indeterminate, Cancelled)
    )
}

pub fn job_record_key(job_id: JobId) -> Key {
    let mut bytes = Vec::with_capacity(JOB_RECORD_KEY_PREFIX.len() + 16);
    bytes.extend_from_slice(JOB_RECORD_KEY_PREFIX);
    bytes.extend_from_slice(&job_id.to_bytes());
    ByteView::from(bytes)
}

/// Side-row key holding the run-crate obligation status for an execution job.
pub fn job_run_crate_key(job_id: JobId) -> Key {
    ByteView::from(job_id.to_bytes().to_vec())
}

/// Dedup key of the follow-on `WriteRunCrate` obligation for `job_id`. Internal
/// obligation keys live in the `internal/` subspace, disjoint from user keys.
pub fn run_crate_dedup_key(job_id: JobId) -> Vec<u8> {
    format!("internal/run-crate/{job_id}").into_bytes()
}

/// Deterministic child-job id that remains on its parent's JobControl bucket.
fn child_job_id(job_id: JobId, domain: &[u8]) -> JobId {
    let parent = job_id
        .as_routable()
        .expect("JobId preserves its structured-id invariant");
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(&job_id.to_bytes());
    let nonce = u64::from_be_bytes(
        hasher.finalize().as_bytes()[..8]
            .try_into()
            .expect("8 bytes"),
    ) & ((1u64 << 48) - 1);
    JobId::from_parts(
        job_id.timestamp_ms(),
        parent.placement_handle(),
        parent.bucket(),
        nonce,
    )
    .expect("structured child job id")
}

/// Stable child-job identity for the durable run-crate obligation.
pub fn crate_job_id(job_id: JobId) -> JobId {
    child_job_id(job_id, b"aruna/run-crate-job/v1")
}

pub fn cleanup_dedup_key(job_id: JobId) -> Vec<u8> {
    format!("internal/terminal-cleanup/{job_id}").into_bytes()
}

pub fn cleanup_job_id(job_id: JobId) -> JobId {
    child_job_id(job_id, b"aruna/terminal-cleanup-job/v1")
}

pub fn workspace_credential_id(job_id: JobId) -> String {
    format!("ws{job_id}")
}

/// Marker of a dedup key whose scope is the subject it names rather than the
/// submitting user. `job_dedup_index_key` leaves these unprefixed, so two users
/// asking for the same thing join one job identity. `user_dedup_key` always
/// namespaces under `user/`, so a caller can never reach this subspace.
pub const GLOBAL_DEDUP_PREFIX: &[u8] = b"global/";

/// Dedup key of a PID mint: the document alone, so a concurrent mint by another
/// user joins the same job rather than creating a second one.
pub fn pid_dedup_key(document_id: Ulid) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(GLOBAL_DEDUP_PREFIX.len() + 4 + 16);
    bytes.extend_from_slice(GLOBAL_DEDUP_PREFIX);
    bytes.extend_from_slice(b"pid/");
    bytes.extend_from_slice(&document_id.to_bytes());
    bytes
}

/// Dedup key of a user-supplied idempotency key: namespaced under `user/` and
/// scoped to the submitting user (fixed-width id), so a caller can neither
/// suppress an internal obligation nor squat another user's key.
pub fn user_dedup_key(created_by: UserId, idempotency_key: &str) -> Vec<u8> {
    let user = created_by.to_bytes();
    let mut bytes = Vec::with_capacity(5 + user.len() + 1 + idempotency_key.len());
    bytes.extend_from_slice(b"user/");
    bytes.extend_from_slice(&user);
    bytes.push(b'/');
    bytes.extend_from_slice(idempotency_key.as_bytes());
    bytes
}

fn schedule_index_key(prefix: &[u8], timestamp_ms: u64, job_id: JobId) -> Key {
    let mut bytes = Vec::with_capacity(prefix.len() + 8 + 16);
    bytes.extend_from_slice(prefix);
    bytes.extend_from_slice(&timestamp_ms.to_be_bytes());
    bytes.extend_from_slice(&job_id.to_bytes());
    ByteView::from(bytes)
}

pub fn job_due_index_key(due_at_ms: u64, job_id: JobId) -> Key {
    schedule_index_key(JOB_DUE_INDEX_PREFIX, due_at_ms, job_id)
}

pub fn job_lease_index_key(lease_expires_at_ms: u64, job_id: JobId) -> Key {
    schedule_index_key(JOB_LEASE_INDEX_PREFIX, lease_expires_at_ms, job_id)
}

pub fn job_prune_index_key(retention_expiry_ms: u64, job_id: JobId) -> Key {
    schedule_index_key(JOB_PRUNE_INDEX_PREFIX, retention_expiry_ms, job_id)
}

/// Extract `(timestamp_ms, job_id)` from a `due/`, `lease/`, or `prune/` schedule
/// index key.
pub fn parse_job_schedule_index_key(key: &[u8]) -> Result<(u64, JobId), ConversionError> {
    for prefix in [
        JOB_DUE_INDEX_PREFIX,
        JOB_LEASE_INDEX_PREFIX,
        JOB_PRUNE_INDEX_PREFIX,
    ] {
        if let Some(rest) = key.strip_prefix(prefix) {
            if rest.len() != 24 {
                return Err(ConversionError::InvalidLength(format!(
                    "expected 24-byte schedule index suffix, got {}",
                    rest.len()
                )));
            }
            let timestamp_ms = u64::from_be_bytes(rest[..8].try_into()?);
            let job_id = JobId::try_from_bytes(rest[8..24].try_into()?)
                .map_err(|error| ConversionError::FromStrError(error.to_string()))?;
            return Ok((timestamp_ms, job_id));
        }
    }
    Err(ConversionError::InvalidLength(
        "unknown job schedule index prefix".to_string(),
    ))
}

pub fn job_owner_index_key(created_by: UserId, created_at_ms: u64, job_id: JobId) -> Key {
    let mut bytes = Vec::with_capacity(72);
    bytes.extend_from_slice(&created_by.to_storage_key());
    bytes.extend_from_slice(&invert_timestamp_ms(created_at_ms).to_be_bytes());
    bytes.extend_from_slice(&job_id.to_bytes());
    ByteView::from(bytes)
}

pub fn job_owner_index_prefix(created_by: UserId) -> Key {
    ByteView::from(created_by.to_storage_key())
}

/// The slot a running job occupies in its submitter's active index. Slots are
/// scoped per kind, so an execution ceiling and an RO-Crate limit never count
/// each other's work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActiveJobKind {
    RoCrate,
    Execution,
}

impl ActiveJobKind {
    /// The slot this payload holds while it runs, if it holds one.
    pub fn of(payload: &JobPayload) -> Option<Self> {
        if payload.is_rocrate() {
            return Some(Self::RoCrate);
        }
        matches!(payload, JobPayload::Execution(_)).then_some(Self::Execution)
    }

    fn tag(self) -> u8 {
        match self {
            Self::RoCrate => 0,
            Self::Execution => 1,
        }
    }
}

pub fn job_active_key(created_by: UserId, kind: ActiveJobKind, job_id: JobId) -> Key {
    let mut bytes = job_active_prefix(created_by, kind).to_vec();
    bytes.extend_from_slice(&job_id.to_bytes());
    ByteView::from(bytes)
}

pub fn job_active_prefix(created_by: UserId, kind: ActiveJobKind) -> Key {
    let mut bytes = created_by.to_storage_key();
    bytes.push(kind.tag());
    ByteView::from(bytes)
}

pub fn job_entry_key(job_id: JobId, entry_key: &[u8]) -> Key {
    let mut bytes = Vec::with_capacity(16 + entry_key.len());
    bytes.extend_from_slice(&job_id.to_bytes());
    bytes.extend_from_slice(entry_key);
    ByteView::from(bytes)
}

pub fn job_entry_prefix(job_id: JobId) -> Key {
    ByteView::from(job_id.to_bytes().to_vec())
}

pub fn rocrate_plan_key(job_id: JobId) -> Key {
    let mut bytes = Vec::with_capacity(17);
    bytes.extend_from_slice(&job_id.to_bytes());
    bytes.push(b'p');
    ByteView::from(bytes)
}

pub fn parse_entry_key(job_id: JobId, key: &[u8]) -> Result<Vec<u8>, ConversionError> {
    if key.len() < 16 || key[..16] != job_id.to_bytes() {
        return Err(ConversionError::InvalidLength(
            "job entry key does not match its job prefix".to_string(),
        ));
    }
    Ok(key[16..].to_vec())
}

pub fn job_owner_cursor(created_at_ms: u64, job_id: JobId) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(24);
    bytes.extend_from_slice(&invert_timestamp_ms(created_at_ms).to_be_bytes());
    bytes.extend_from_slice(&job_id.to_bytes());
    bytes
}

pub fn parse_job_owner_index_key(key: &[u8]) -> Result<(UserId, u64, JobId), ConversionError> {
    if key.len() != 72 {
        return Err(ConversionError::InvalidLength(format!(
            "expected 72-byte job owner index key, got {} bytes",
            key.len()
        )));
    }
    let created_by = UserId::from_storage_key(&key[..48])?;
    let created_at_ms = invert_timestamp_ms(u64::from_be_bytes(key[48..56].try_into()?));
    let job_id = JobId::try_from_bytes(key[56..72].try_into()?)
        .map_err(|error| ConversionError::FromStrError(error.to_string()))?;
    Ok((created_by, created_at_ms, job_id))
}

const SUBMISSION_KEYED_DOMAIN: &[u8] = b"aruna-submission-id-v1";
const SUBMISSION_UNKEYED_DOMAIN: &[u8] = b"aruna-submission-nonce-v1";
const SUBMISSION_CLAIM_DOMAIN: &[u8] = b"aruna-submission-claim-v1";
const CANONICAL_EXECUTION_DOMAIN: &[u8] = b"aruna-canonical-execution-v1";

/// Opaque replicated identity of one keyed or unkeyed submission family. Every
/// ingress derives it identically, and the raw idempotency key never replicates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SubmissionId(pub [u8; 32]);

impl SubmissionId {
    /// The caller storage key is fixed width and the idempotency key is length
    /// prefixed, so no caller can shift bytes into another family's identity.
    pub fn keyed(created_by: UserId, idempotency_key: &[u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(SUBMISSION_KEYED_DOMAIN);
        hasher.update(&created_by.to_storage_key());
        hasher.update(&(idempotency_key.len() as u64).to_be_bytes());
        hasher.update(idempotency_key);
        Self(*hasher.finalize().as_bytes())
    }

    /// Unkeyed submissions never merge, so a fresh ingress nonce is the subject.
    pub fn unkeyed(nonce: Ulid) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(SUBMISSION_UNKEYED_DOMAIN);
        hasher.update(&nonce.to_bytes());
        Self(*hasher.finalize().as_bytes())
    }
}

/// Domain tags of the canonical record encodings. Each tag is distinct and no
/// tag is a prefix of another, so `tag || postcard(body)` is unambiguous.
pub const JOB_SPEC_DOMAIN: &[u8] = b"aruna-job-spec-v1";
pub const JOB_CLAIM_DOMAIN: &[u8] = b"aruna-job-claim-v1";
pub const JOB_BUDGET_DOMAIN: &[u8] = b"aruna-job-budget-v1";
pub const JOB_LAUNCH_DOMAIN: &[u8] = b"aruna-job-launch-v1";
pub const JOB_RECEIPT_DOMAIN: &[u8] = b"aruna-job-receipt-v1";
pub const JOB_UPDATE_DOMAIN: &[u8] = b"aruna-job-update-v1";
pub const JOB_OUTPUT_DOMAIN: &[u8] = b"aruna-job-output-v1";
pub const JOB_CANCEL_DOMAIN: &[u8] = b"aruna-job-cancel-v1";
pub const JOB_ENVELOPE_DOMAIN: &[u8] = b"aruna-job-envelope-v1";

/// Objects one execution may publish in its immutable output record.
pub const MAX_EXECUTION_OUTPUTS: usize = 1024;

/// Bytes of the free-text diagnostic on a terminal execution update.
pub const MAX_RESULT_MESSAGE_BYTES: usize = 4096;

/// Encoded width of a [`JobRecordKey`]: family, kind, subject, sequence.
pub const JOB_RECORD_KEY_BYTES: usize = 105;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum JobContractError {
    #[error("retry policy must allow at least one launch")]
    EmptyRetry,
    #[error("launch belongs to another witness budget")]
    BudgetMismatch,
    #[error("launch sequence {sequence} is outside the stored budget of {max_launches}")]
    BudgetExhausted { sequence: u32, max_launches: u32 },
    #[error("launch spec digest does not match the stored source spec digest")]
    SpecMismatch,
}

/// Why a replicated job record is not authentic. An unverifiable record is never
/// appended, projected, or relayed as authentic.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum JobRecordError {
    #[error(transparent)]
    Encoding(#[from] postcard::Error),
    #[error(transparent)]
    Contract(#[from] JobContractError),
    #[error("publisher signature does not verify for the claimed publisher")]
    BadSignature,
    #[error("record realm does not match the ingesting holder's realm")]
    RealmMismatch,
    #[error("record does not belong to the job family being ingested")]
    FamilyMismatch,
    #[error("spec placement is not the family placement derived from the submission")]
    PlacementMismatch,
    #[error("a {0:?} record may only be published by its one permitted author")]
    WrongPublisher(JobRecordKind),
    #[error("a {0:?} record requires a publisher the local view still holds authority for")]
    NotHolder(JobRecordKind),
    #[error("record digest does not reproduce from its own canonical bytes")]
    DigestMismatch,
    /// Local proof gate only: a replicated record whose predecessor is absent
    /// yields the [`RecordVerdict::MissingEvidence`] verdict instead.
    #[error("a verified {0:?} record is required to prove this result")]
    MissingEvidence(JobRecordKind),
    #[error("record contradicts the verified {0:?} record it refers to")]
    EvidenceMismatch(JobRecordKind),
    #[error("record's own embedded fields contradict each other")]
    Inconsistent,
    #[error("caller is not authorized against the stored job spec")]
    Unauthorized,
    #[error("job record key must be {JOB_RECORD_KEY_BYTES} bytes naming a known kind")]
    MalformedKey,
    #[error("outputs must be at most {MAX_EXECUTION_OUTPUTS} canonically ordered exact objects")]
    OutputOrder,
    #[error("every output must name its exact version and its producing execution once")]
    OutputIdentity,
    #[error("result message must be at most {MAX_RESULT_MESSAGE_BYTES} bytes")]
    MessageBytes,
    #[error("two different updates claim sequence {sequence}")]
    ChainConflict { sequence: u64 },
}

/// Per-witness launch bound stored in the immutable spec.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRetryPolicy {
    /// Includes the initial launch and is at least one.
    pub max_launches_per_witness: u32,
}

impl JobRetryPolicy {
    pub fn validate(&self) -> Result<(), JobContractError> {
        match self.max_launches_per_witness {
            0 => Err(JobContractError::EmptyRetry),
            _ => Ok(()),
        }
    }
}

/// Ceilings normalized once at submission. No field is optional, so comparing a
/// request against a static executor envelope is total.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveResources {
    pub cpu_cores: u32,
    pub ram_bytes: u64,
    /// Zero is the absence of a disk request, not a zero-byte ceiling.
    pub disk_bytes: u64,
    pub max_walltime_ms: u64,
    pub preemptible: bool,
}

/// Immutable logical admission committed with the spec. Later quota convergence
/// never revokes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobAdmissionRecord {
    pub submission_id: SubmissionId,
    pub request_digest: [u8; 32],
    pub job_id: JobId,
    pub group_id: GroupId,
    pub admitting_node_id: NodeId,
    pub membership_generation: u64,
    pub resources: EffectiveResources,
    pub admitted_at_ms: u64,
}

/// The immutable spec of one accepted claim. `request_digest` covers the
/// normalized caller plan; `spec_digest` is this record's own digest, the one
/// self-referential field: zeroed, never omitted, while its bytes are computed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalJobSpec {
    pub submission_id: SubmissionId,
    pub job_id: JobId,
    pub origin_node_id: NodeId,
    /// Ingress node whose node-local object names were resolved at submission.
    pub ingress_node_id: NodeId,
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub created_by: UserId,
    pub created_at_ms: u64,
    pub payload: ExecutionSpec,
    pub request_digest: [u8; 32],
    pub spec_digest: [u8; 32],
    pub resources: EffectiveResources,
    pub retention_ms: u64,
    pub retry: JobRetryPolicy,
    pub admission: JobAdmissionRecord,
    pub captured_inputs: Vec<CapturedInput>,
    pub output_policies: Vec<PlacementPolicyRef>,
    /// Family placement derived from `submission_id`, never from the alias bucket.
    pub placement: PlacementRef,
}

impl LogicalJobSpec {
    /// Fills the self-referential `spec_digest` from the record's canonical bytes.
    pub fn store_digest(mut self) -> Result<Self, JobRecordError> {
        self.spec_digest = self.digest()?;
        Ok(self)
    }

    /// Fails when the stored digest does not reproduce from the record itself.
    pub fn verify_digest(&self) -> Result<(), JobRecordError> {
        match self.spec_digest == self.digest()? {
            true => Ok(()),
            false => Err(JobRecordError::DigestMismatch),
        }
    }

    /// The admission committed with the spec must describe this exact claim.
    fn admission_binds(&self) -> Result<(), JobRecordError> {
        let admission = &self.admission;
        match admission.submission_id == self.submission_id
            && admission.request_digest == self.request_digest
            && admission.job_id == self.job_id
            && admission.group_id == self.group_id
            && admission.resources == self.resources
        {
            true => Ok(()),
            false => Err(JobRecordError::Inconsistent),
        }
    }
}

/// Union member keyed by `(submission_id, job_id)`, so two partitioned accepts
/// of one idempotency key contribute two claims instead of conflicting.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionClaim {
    pub submission_id: SubmissionId,
    pub job_id: JobId,
    pub request_digest: [u8; 32],
    pub spec_digest: [u8; 32],
    pub committing_node_id: NodeId,
    pub accepted_at_ms: u64,
}

impl SubmissionClaim {
    /// Reduction order of the union: the smallest key is the canonical alias, so
    /// arrival order, clocks, and scheduler rank never select it.
    pub fn order_key(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(SUBMISSION_CLAIM_DOMAIN);
        hasher.update(&self.submission_id.0);
        hasher.update(&self.request_digest);
        hasher.update(&self.job_id.to_bytes());
        *hasher.finalize().as_bytes()
    }
}

/// Lifetime launch bound one scheduler stores before it first plans a request.
/// A later realm-config or alias change can never reset or widen it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WitnessBudgetRecord {
    pub submission_id: SubmissionId,
    pub request_digest: [u8; 32],
    pub scheduler_node_id: NodeId,
    pub source_spec_digest: [u8; 32],
    pub max_launches: u32,
}

impl WitnessBudgetRecord {
    /// A launch is actionable only inside the budget its own scheduler stored.
    pub fn admits(&self, launch: &LaunchIntent) -> Result<(), JobContractError> {
        if self.submission_id != launch.submission_id
            || self.request_digest != launch.request_digest
            || self.scheduler_node_id != launch.scheduler_node_id
        {
            return Err(JobContractError::BudgetMismatch);
        }
        if self.source_spec_digest != launch.spec_digest {
            return Err(JobContractError::SpecMismatch);
        }
        if launch.scheduler_seq >= self.max_launches {
            return Err(JobContractError::BudgetExhausted {
                sequence: launch.scheduler_seq,
                max_launches: self.max_launches,
            });
        }
        Ok(())
    }
}

/// One scheduler's durable decision to launch. Replaying the same `launch_id`
/// against the same target is idempotent; another id is another execution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchIntent {
    pub launch_id: Ulid,
    pub submission_id: SubmissionId,
    pub request_digest: [u8; 32],
    pub job_id: JobId,
    pub scheduler_node_id: NodeId,
    pub scheduler_seq: u32,
    pub witness_placement: PlacementRef,
    /// Audit and ranking evidence; it never proves historical holder authority.
    pub holder_generation: u64,
    pub target: ExecutionTargetId,
    pub inputs: Vec<crate::scheduling::PlannedInput>,
    pub output_policies: Vec<PlacementPolicyRef>,
    pub plan_digest: [u8; 32],
    pub spec_digest: [u8; 32],
    pub created_at_ms: u64,
}

/// The target's acceptance of one exact launch, authenticated by the executor's
/// envelope signature. It binds the subject it was accepted under, so a later
/// placement change cannot rewrite that history or authorize unrelated work.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionReceipt {
    pub execution_id: Ulid,
    pub launch_id: Ulid,
    pub launch_digest: [u8; 32],
    pub submission_id: SubmissionId,
    pub request_digest: [u8; 32],
    pub job_id: JobId,
    pub executor_node_id: NodeId,
    pub target: ExecutionTargetId,
    pub spec_digest: [u8; 32],
    pub membership_generation: u64,
    pub subject_generation: u64,
    pub subject_digest: [u8; 32],
    pub accepted_at_ms: u64,
}

/// State of one physical execution. Terminal here means terminal for this
/// `ExecutionId` only, never for the logical job.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PhysicalExecutionState {
    Accepted,
    Preparing,
    Running,
    Succeeded,
    /// Permanent job-specific failure that suppresses further execution.
    Failed,
    Cancelled,
    /// Infrastructure or retryable execution error without a logical outcome.
    Error,
}

impl PhysicalExecutionState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            PhysicalExecutionState::Succeeded
                | PhysicalExecutionState::Failed
                | PhysicalExecutionState::Cancelled
                | PhysicalExecutionState::Error
        )
    }

    /// Stable machine-readable name for API payloads.
    pub fn name(&self) -> &'static str {
        match self {
            PhysicalExecutionState::Accepted => "accepted",
            PhysicalExecutionState::Preparing => "preparing",
            PhysicalExecutionState::Running => "running",
            PhysicalExecutionState::Succeeded => "succeeded",
            PhysicalExecutionState::Failed => "failed",
            PhysicalExecutionState::Cancelled => "cancelled",
            PhysicalExecutionState::Error => "error",
        }
    }
}

/// Bounded free-text diagnostic carried by a terminal update.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct ResultMessage(String);

impl ResultMessage {
    pub fn new(message: String) -> Result<Self, JobRecordError> {
        match message.len() <= MAX_RESULT_MESSAGE_BYTES {
            true => Ok(Self(message)),
            false => Err(JobRecordError::MessageBytes),
        }
    }

    /// The last bytes that fit the cap, cut on a char boundary. `None` for an
    /// empty stream, so nothing captured stays distinguishable from a blank one.
    pub fn tail(text: &str) -> Option<Self> {
        if text.is_empty() {
            return None;
        }
        Some(Self(tail_str(text, MAX_RESULT_MESSAGE_BYTES).to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ResultMessage {
    type Error = JobRecordError;

    fn try_from(message: String) -> Result<Self, Self::Error> {
        Self::new(message)
    }
}

/// Ordering of one output inside a canonical set.
fn output_order(object: &OutputObject) -> (crate::NodeId, &str, &str, Ulid) {
    (
        object.node_id,
        &object.bucket,
        &object.key,
        object.version_id,
    )
}

/// The exact output objects of one execution, in one canonical order. Decoding
/// rejects an unordered, duplicated, oversized, or identity-free set, so a peer
/// can never reorder a signed record into different canonical bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<OutputObject>")]
pub struct OutputSet(Vec<OutputObject>);

impl OutputSet {
    pub fn new(outputs: Vec<OutputObject>) -> Result<Self, JobRecordError> {
        if outputs.len() > MAX_EXECUTION_OUTPUTS {
            return Err(JobRecordError::OutputOrder);
        }
        if outputs
            .iter()
            .any(|object| object.version_id.is_nil() || object.execution_id.is_nil())
        {
            return Err(JobRecordError::OutputOrder);
        }
        if outputs
            .windows(2)
            .any(|pair| output_order(&pair[0]) >= output_order(&pair[1]))
        {
            return Err(JobRecordError::OutputOrder);
        }
        Ok(Self(outputs))
    }

    /// Producer-side constructor: sorts into the one canonical order first.
    pub fn canonical(mut outputs: Vec<OutputObject>) -> Result<Self, JobRecordError> {
        outputs.sort_by(|left, right| output_order(left).cmp(&output_order(right)));
        Self::new(outputs)
    }

    pub fn as_slice(&self) -> &[OutputObject] {
        &self.0
    }

    pub fn into_inner(self) -> Vec<OutputObject> {
        self.0
    }
}

impl TryFrom<Vec<OutputObject>> for OutputSet {
    type Error = JobRecordError;

    fn try_from(outputs: Vec<OutputObject>) -> Result<Self, Self::Error> {
        Self::new(outputs)
    }
}

/// Terminal result of one physical execution. Outputs are not embedded here: a
/// success names the digest of its separately published output record, so that
/// record must already be durable before success can be projected.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalExecutionResult {
    pub exit_code: Option<i32>,
    pub output_digest: Option<[u8; 32]>,
    pub message: Option<ResultMessage>,
    /// Bounded tail of the run's stdout: its last bytes, never the whole stream.
    pub stdout: Option<ResultMessage>,
    /// Bounded tail of the run's stderr: its last bytes, never the whole stream.
    pub stderr: Option<ResultMessage>,
}

/// Monotonic state publication by the fenced executor. `previous_digest` roots
/// the chain at the receipt, so a gap cannot silently skip a state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionUpdate {
    pub execution_id: Ulid,
    pub submission_id: SubmissionId,
    pub request_digest: [u8; 32],
    pub executor_node_id: NodeId,
    pub sequence: u64,
    pub previous_digest: [u8; 32],
    pub state: PhysicalExecutionState,
    pub observed_at_ms: u64,
    pub result: Option<PhysicalExecutionResult>,
}

/// The exact output set of one physical execution, published as its own
/// immutable record. Its durability is the prerequisite for terminal success.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionOutputRecord {
    pub execution_id: Ulid,
    pub submission_id: SubmissionId,
    pub request_digest: [u8; 32],
    pub job_id: JobId,
    pub executor_node_id: NodeId,
    pub spec_digest: [u8; 32],
    pub receipt_digest: [u8; 32],
    pub outputs: OutputSet,
    pub committed_at_ms: u64,
}

/// Token-free evidence of how the publishing node authorized the caller against
/// the stored spec. Bearer tokens never replicate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CancelAuthority {
    /// The caller is the stored submitter; every holder rechecks this alone.
    Submitter,
    /// The publishing node checked cancel permission on the stored group when it
    /// published the record, and its envelope signature is that statement.
    GroupAdmin,
}

/// Replicated cancellation intent for one `(submission_id, request_digest)`
/// family. It suppresses new launches; a partitioned executor may still finish.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCancelRecord {
    pub cancel_id: Ulid,
    pub submission_id: SubmissionId,
    pub request_digest: [u8; 32],
    pub job_id: JobId,
    /// Stored spec the caller's authorization was evaluated against.
    pub spec_digest: [u8; 32],
    pub requested_by: UserId,
    pub authority: CancelAuthority,
    pub requested_at_ms: u64,
}

/// Content-independent success order. The smallest key in one request family is
/// canonical, so no publisher can bias selection with a timestamp.
pub fn canonical_execution_key(
    submission_id: SubmissionId,
    request_digest: [u8; 32],
    execution_id: Ulid,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CANONICAL_EXECUTION_DOMAIN);
    hasher.update(&submission_id.0);
    hasher.update(&request_digest);
    hasher.update(&execution_id.to_bytes());
    *hasher.finalize().as_bytes()
}

/// Replication family every job record belongs to. Two different requests under
/// one submission are separate families and never merge.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct JobFamilyId {
    pub submission_id: SubmissionId,
    pub request_digest: [u8; 32],
}

impl JobFamilyId {
    pub fn to_bytes(&self) -> [u8; 64] {
        let mut bytes = [0u8; 64];
        bytes[..32].copy_from_slice(&self.submission_id.0);
        bytes[32..].copy_from_slice(&self.request_digest);
        bytes
    }
}

/// The immutable record kinds of one job family. Declaration order is the byte
/// order of [`JobRecordKey`], so key ordering and struct ordering agree.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum JobRecordKind {
    Spec,
    Claim,
    Budget,
    Launch,
    Receipt,
    Update,
    Output,
    Cancel,
}

impl JobRecordKind {
    pub fn as_byte(self) -> u8 {
        match self {
            JobRecordKind::Spec => 0,
            JobRecordKind::Claim => 1,
            JobRecordKind::Budget => 2,
            JobRecordKind::Launch => 3,
            JobRecordKind::Receipt => 4,
            JobRecordKind::Update => 5,
            JobRecordKind::Output => 6,
            JobRecordKind::Cancel => 7,
        }
    }

    pub fn from_byte(byte: u8) -> Result<Self, JobRecordError> {
        match byte {
            0 => Ok(JobRecordKind::Spec),
            1 => Ok(JobRecordKind::Claim),
            2 => Ok(JobRecordKind::Budget),
            3 => Ok(JobRecordKind::Launch),
            4 => Ok(JobRecordKind::Receipt),
            5 => Ok(JobRecordKind::Update),
            6 => Ok(JobRecordKind::Output),
            7 => Ok(JobRecordKind::Cancel),
            _ => Err(JobRecordError::MalformedKey),
        }
    }
}

/// Stable typed key of one immutable job record. The encoded form is the storage
/// and paging order: family prefix first, then kind, subject, and sequence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct JobRecordKey {
    pub family: JobFamilyId,
    pub kind: JobRecordKind,
    /// Identity discriminating the record inside its kind, zero-extended.
    pub subject: [u8; 32],
    /// Position inside the subject; zero for every single-instance kind.
    pub sequence: u64,
}

impl JobRecordKey {
    pub fn to_bytes(&self) -> [u8; JOB_RECORD_KEY_BYTES] {
        let mut bytes = [0u8; JOB_RECORD_KEY_BYTES];
        bytes[..64].copy_from_slice(&self.family.to_bytes());
        bytes[64] = self.kind.as_byte();
        bytes[65..97].copy_from_slice(&self.subject);
        bytes[97..].copy_from_slice(&self.sequence.to_be_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, JobRecordError> {
        if bytes.len() != JOB_RECORD_KEY_BYTES {
            return Err(JobRecordError::MalformedKey);
        }
        let read = |range: std::ops::Range<usize>| -> Result<[u8; 32], JobRecordError> {
            bytes[range]
                .try_into()
                .map_err(|_| JobRecordError::MalformedKey)
        };
        let sequence: [u8; 8] = bytes[97..]
            .try_into()
            .map_err(|_| JobRecordError::MalformedKey)?;
        Ok(Self {
            family: JobFamilyId {
                submission_id: SubmissionId(read(0..32)?),
                request_digest: read(32..64)?,
            },
            kind: JobRecordKind::from_byte(bytes[64])?,
            subject: read(65..97)?,
            sequence: u64::from_be_bytes(sequence),
        })
    }
}

/// Domain-tagged canonical encoding shared by every record kind.
fn tagged_bytes(domain: &[u8], value: &impl Serialize) -> Result<Vec<u8>, JobRecordError> {
    let body = postcard::to_allocvec(value)?;
    let mut bytes = Vec::with_capacity(domain.len() + body.len());
    bytes.extend_from_slice(domain);
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

/// Zero-extends a 16-byte identity into a record key subject.
fn subject_bytes(id: [u8; 16]) -> [u8; 32] {
    let mut subject = [0u8; 32];
    subject[..16].copy_from_slice(&id);
    subject
}

/// Canonical identity of one immutable job record kind. The digest is
/// `blake3(DOMAIN || postcard(canonical form))`; a record carrying its own digest
/// zeroes that one field there and nowhere else, hashing every other as it stands.
pub trait JobRecordBody: Serialize + Sized {
    const DOMAIN: &'static [u8];
    const KIND: JobRecordKind;

    fn family(&self) -> JobFamilyId;

    fn subject(&self) -> [u8; 32];

    fn sequence(&self) -> u64 {
        0
    }

    fn canonical_bytes(&self) -> Result<Vec<u8>, JobRecordError> {
        tagged_bytes(Self::DOMAIN, self)
    }

    fn digest(&self) -> Result<[u8; 32], JobRecordError> {
        Ok(*blake3::hash(&self.canonical_bytes()?).as_bytes())
    }
}

impl JobRecordBody for LogicalJobSpec {
    const DOMAIN: &'static [u8] = JOB_SPEC_DOMAIN;
    const KIND: JobRecordKind = JobRecordKind::Spec;

    fn family(&self) -> JobFamilyId {
        JobFamilyId {
            submission_id: self.submission_id,
            request_digest: self.request_digest,
        }
    }

    fn subject(&self) -> [u8; 32] {
        subject_bytes(self.job_id.to_bytes())
    }

    /// The only self-referential field in the family is zeroed here.
    fn canonical_bytes(&self) -> Result<Vec<u8>, JobRecordError> {
        let mut canonical = self.clone();
        canonical.spec_digest = [0u8; 32];
        tagged_bytes(Self::DOMAIN, &canonical)
    }
}

impl JobRecordBody for SubmissionClaim {
    const DOMAIN: &'static [u8] = JOB_CLAIM_DOMAIN;
    const KIND: JobRecordKind = JobRecordKind::Claim;

    fn family(&self) -> JobFamilyId {
        JobFamilyId {
            submission_id: self.submission_id,
            request_digest: self.request_digest,
        }
    }

    fn subject(&self) -> [u8; 32] {
        subject_bytes(self.job_id.to_bytes())
    }
}

impl JobRecordBody for WitnessBudgetRecord {
    const DOMAIN: &'static [u8] = JOB_BUDGET_DOMAIN;
    const KIND: JobRecordKind = JobRecordKind::Budget;

    fn family(&self) -> JobFamilyId {
        JobFamilyId {
            submission_id: self.submission_id,
            request_digest: self.request_digest,
        }
    }

    fn subject(&self) -> [u8; 32] {
        *self.scheduler_node_id.as_bytes()
    }
}

impl JobRecordBody for LaunchIntent {
    const DOMAIN: &'static [u8] = JOB_LAUNCH_DOMAIN;
    const KIND: JobRecordKind = JobRecordKind::Launch;

    fn family(&self) -> JobFamilyId {
        JobFamilyId {
            submission_id: self.submission_id,
            request_digest: self.request_digest,
        }
    }

    fn subject(&self) -> [u8; 32] {
        subject_bytes(self.launch_id.to_bytes())
    }
}

impl JobRecordBody for ExecutionReceipt {
    const DOMAIN: &'static [u8] = JOB_RECEIPT_DOMAIN;
    const KIND: JobRecordKind = JobRecordKind::Receipt;

    fn family(&self) -> JobFamilyId {
        JobFamilyId {
            submission_id: self.submission_id,
            request_digest: self.request_digest,
        }
    }

    fn subject(&self) -> [u8; 32] {
        subject_bytes(self.execution_id.to_bytes())
    }
}

impl JobRecordBody for ExecutionUpdate {
    const DOMAIN: &'static [u8] = JOB_UPDATE_DOMAIN;
    const KIND: JobRecordKind = JobRecordKind::Update;

    fn family(&self) -> JobFamilyId {
        JobFamilyId {
            submission_id: self.submission_id,
            request_digest: self.request_digest,
        }
    }

    fn subject(&self) -> [u8; 32] {
        subject_bytes(self.execution_id.to_bytes())
    }

    fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl JobRecordBody for ExecutionOutputRecord {
    const DOMAIN: &'static [u8] = JOB_OUTPUT_DOMAIN;
    const KIND: JobRecordKind = JobRecordKind::Output;

    fn family(&self) -> JobFamilyId {
        JobFamilyId {
            submission_id: self.submission_id,
            request_digest: self.request_digest,
        }
    }

    fn subject(&self) -> [u8; 32] {
        subject_bytes(self.execution_id.to_bytes())
    }
}

impl JobRecordBody for JobCancelRecord {
    const DOMAIN: &'static [u8] = JOB_CANCEL_DOMAIN;
    const KIND: JobRecordKind = JobRecordKind::Cancel;

    fn family(&self) -> JobFamilyId {
        JobFamilyId {
            submission_id: self.submission_id,
            request_digest: self.request_digest,
        }
    }

    fn subject(&self) -> [u8; 32] {
        subject_bytes(self.cancel_id.to_bytes())
    }
}

/// One immutable record of a job family. Every variant has exactly one permitted
/// author and is never rewritten under the same key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobFamilyRecord {
    Spec(Box<LogicalJobSpec>),
    Claim(SubmissionClaim),
    Budget(WitnessBudgetRecord),
    Launch(Box<LaunchIntent>),
    Receipt(Box<ExecutionReceipt>),
    Update(Box<ExecutionUpdate>),
    Output(Box<ExecutionOutputRecord>),
    Cancel(JobCancelRecord),
}

impl JobFamilyRecord {
    pub fn kind(&self) -> JobRecordKind {
        match self {
            JobFamilyRecord::Spec(_) => JobRecordKind::Spec,
            JobFamilyRecord::Claim(_) => JobRecordKind::Claim,
            JobFamilyRecord::Budget(_) => JobRecordKind::Budget,
            JobFamilyRecord::Launch(_) => JobRecordKind::Launch,
            JobFamilyRecord::Receipt(_) => JobRecordKind::Receipt,
            JobFamilyRecord::Update(_) => JobRecordKind::Update,
            JobFamilyRecord::Output(_) => JobRecordKind::Output,
            JobFamilyRecord::Cancel(_) => JobRecordKind::Cancel,
        }
    }

    /// Family the record binds itself to. It is part of the signed canonical
    /// bytes, so a relay cannot move a record into another family.
    pub fn family(&self) -> JobFamilyId {
        match self {
            JobFamilyRecord::Spec(spec) => spec.family(),
            JobFamilyRecord::Claim(claim) => claim.family(),
            JobFamilyRecord::Budget(budget) => budget.family(),
            JobFamilyRecord::Launch(launch) => launch.family(),
            JobFamilyRecord::Receipt(receipt) => receipt.family(),
            JobFamilyRecord::Update(update) => update.family(),
            JobFamilyRecord::Output(output) => output.family(),
            JobFamilyRecord::Cancel(cancel) => cancel.family(),
        }
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, JobRecordError> {
        match self {
            JobFamilyRecord::Spec(spec) => spec.canonical_bytes(),
            JobFamilyRecord::Claim(claim) => claim.canonical_bytes(),
            JobFamilyRecord::Budget(budget) => budget.canonical_bytes(),
            JobFamilyRecord::Launch(launch) => launch.canonical_bytes(),
            JobFamilyRecord::Receipt(receipt) => receipt.canonical_bytes(),
            JobFamilyRecord::Update(update) => update.canonical_bytes(),
            JobFamilyRecord::Output(output) => output.canonical_bytes(),
            JobFamilyRecord::Cancel(cancel) => cancel.canonical_bytes(),
        }
    }

    pub fn digest(&self) -> Result<[u8; 32], JobRecordError> {
        Ok(*blake3::hash(&self.canonical_bytes()?).as_bytes())
    }

    pub fn key(&self) -> JobRecordKey {
        let (subject, sequence) = match self {
            JobFamilyRecord::Spec(spec) => (spec.subject(), spec.sequence()),
            JobFamilyRecord::Claim(claim) => (claim.subject(), claim.sequence()),
            JobFamilyRecord::Budget(budget) => (budget.subject(), budget.sequence()),
            JobFamilyRecord::Launch(launch) => (launch.subject(), launch.sequence()),
            JobFamilyRecord::Receipt(receipt) => (receipt.subject(), receipt.sequence()),
            JobFamilyRecord::Update(update) => (update.subject(), update.sequence()),
            JobFamilyRecord::Output(output) => (output.subject(), output.sequence()),
            JobFamilyRecord::Cancel(cancel) => (cancel.subject(), cancel.sequence()),
        };
        JobRecordKey {
            family: self.family(),
            kind: self.kind(),
            subject,
            sequence,
        }
    }
}

/// The verifying node's authenticated local view of who may author records of
/// this family: its current realm membership and the unconflicted holders of the
/// family placement. A valid realm key proves identity, never holder authority.
///
/// A node whose local placement view is missing or conflicted must defer
/// verification rather than present an empty view: an empty view grants nothing,
/// so every holder-authored record would be refused instead of retried.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HolderView<'a> {
    /// Sync-eligible nodes of the verifier's current replicated realm config.
    pub members: &'a [NodeId],
    /// Current holders of the family placement, resolved over its activated map.
    pub holders: &'a [NodeId],
}

impl HolderView<'_> {
    /// Both are required: an activated candidate map may still rank a node that
    /// has since left the realm.
    fn grants(&self, node: NodeId) -> bool {
        self.holders.contains(&node) && self.members.contains(&node)
    }
}

/// A node's evidence about its own locally fenced execution, presented while no
/// replicated launch chain exists. It authorizes that node's own output record
/// and nothing else, and every field is a documented stand-in for the record the
/// distributed rounds publish instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalExecution {
    /// The verifying node itself; a caller never fills this in for a peer.
    pub node_id: NodeId,
    pub execution_id: Ulid,
    /// Local attempt fence digest standing in for the receipt digest.
    pub fence_digest: [u8; 32],
    /// Local plan digest standing in for the stored spec digest.
    pub spec_digest: [u8; 32],
}

/// Verified evidence a holder already retains, plus the local view authority is
/// judged against. Every field comes from the verifier's own state, never from
/// the peer that relayed the record under check.
#[derive(Clone, Copy, Debug)]
pub struct JobRecordContext<'a> {
    pub realm_id: RealmId,
    pub family: JobFamilyId,
    /// Family placement derived from the submission id and the realm strategy.
    pub placement: PlacementRef,
    pub view: HolderView<'a>,
    pub spec: Option<&'a LogicalJobSpec>,
    pub budget: Option<&'a WitnessBudgetRecord>,
    pub launch: Option<&'a LaunchIntent>,
    pub receipt: Option<&'a ExecutionReceipt>,
    pub previous_update: Option<&'a ExecutionUpdate>,
    pub local: Option<&'a LocalExecution>,
}

impl<'a> JobRecordContext<'a> {
    /// Fail-closed: the view starts empty, so a caller that forgets to resolve
    /// holders proves no authority instead of accepting every publisher.
    pub fn new(realm_id: RealmId, family: JobFamilyId, placement: PlacementRef) -> Self {
        Self {
            realm_id,
            family,
            placement,
            view: HolderView::default(),
            spec: None,
            budget: None,
            launch: None,
            receipt: None,
            previous_update: None,
            local: None,
        }
    }
}

/// Outcome of verifying one record against the local view. Only `Authentic` may
/// be appended, projected, and relayed as replicated authority; `MissingEvidence`
/// belongs in the bounded pending path until its predecessor is verified.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordVerdict {
    Authentic,
    /// Proven only against this node's own fenced execution: no replicated
    /// launch chain backs it, so it stays local and never relays as authority.
    LocalEvidence,
    /// The named predecessor is not verified locally yet. The record is neither
    /// authentic nor forged, so it is retained pending, never projected.
    MissingEvidence(JobRecordKind),
}

/// The authenticated envelope every replicated job record travels in, kept
/// byte-identical end to end: after a relay the publisher signature is the only
/// proof of authorship, and key, kind, family, and digest all derive from it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRecordEnvelope {
    pub realm_id: RealmId,
    pub record: JobFamilyRecord,
    /// The node that created the record, not the holder that relayed it.
    pub published_by: NodeId,
    pub signature: iroh::Signature,
}

/// The tuple a publisher signs: realm plus the record's canonical digest, which
/// already covers kind and family. A record therefore cannot be replayed into
/// another realm, family, or kind.
fn claim_bytes(realm_id: RealmId, record: &JobFamilyRecord) -> Result<[u8; 32], JobRecordError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(JOB_ENVELOPE_DOMAIN);
    hasher.update(realm_id.as_bytes());
    hasher.update(&record.digest()?);
    Ok(*hasher.finalize().as_bytes())
}

impl JobRecordEnvelope {
    pub fn sign(
        realm_id: RealmId,
        record: JobFamilyRecord,
        secret: &iroh::SecretKey,
    ) -> Result<Self, JobRecordError> {
        Self::signed_with(realm_id, record, secret.public(), |message| {
            secret.sign(message)
        })
    }

    /// Signs with the node's own signer, for publishers that hold a handle
    /// rather than the key itself.
    pub fn signed_with(
        realm_id: RealmId,
        record: JobFamilyRecord,
        published_by: NodeId,
        sign: impl FnOnce(&[u8]) -> iroh::Signature,
    ) -> Result<Self, JobRecordError> {
        let signature = sign(&claim_bytes(realm_id, &record)?);
        Ok(Self {
            realm_id,
            record,
            published_by,
            signature,
        })
    }

    pub fn key(&self) -> JobRecordKey {
        self.record.key()
    }

    pub fn kind(&self) -> JobRecordKind {
        self.record.kind()
    }

    pub fn family(&self) -> JobFamilyId {
        self.record.family()
    }

    pub fn digest(&self) -> Result<[u8; 32], JobRecordError> {
        self.record.digest()
    }

    pub fn signing_bytes(&self) -> Result<[u8; 32], JobRecordError> {
        claim_bytes(self.realm_id, &self.record)
    }

    /// The single ingest gate: realm and family binding, publisher signature, the
    /// record kind's exact author rule, and the authority the local view grants
    /// that author. A holder that only relays a record satisfies no author rule.
    pub fn verify(&self, context: &JobRecordContext<'_>) -> Result<RecordVerdict, JobRecordError> {
        if self.realm_id != context.realm_id {
            return Err(JobRecordError::RealmMismatch);
        }
        if self.record.family() != context.family {
            return Err(JobRecordError::FamilyMismatch);
        }
        self.verify_signature()?;
        match &self.record {
            JobFamilyRecord::Spec(spec) => self.verify_spec(spec, context),
            JobFamilyRecord::Claim(claim) => self.verify_claim(claim, context),
            JobFamilyRecord::Budget(budget) => self.verify_budget(budget, context),
            JobFamilyRecord::Launch(launch) => self.verify_launch(launch, context),
            JobFamilyRecord::Receipt(receipt) => self.verify_receipt(receipt, context),
            JobFamilyRecord::Update(update) => self.verify_update(update, context),
            JobFamilyRecord::Output(output) => self.verify_output(output, context),
            JobFamilyRecord::Cancel(cancel) => self.verify_cancel(cancel, context),
        }
    }

    pub fn verify_signature(&self) -> Result<(), JobRecordError> {
        self.published_by
            .verify(&self.signing_bytes()?, &self.signature)
            .map_err(|_| JobRecordError::BadSignature)
    }

    fn author(&self, expected: NodeId, kind: JobRecordKind) -> Result<(), JobRecordError> {
        match self.published_by == expected {
            true => Ok(()),
            false => Err(JobRecordError::WrongPublisher(kind)),
        }
    }

    /// Holder-authored kinds require a publisher the accepting node's own view
    /// still ranks as a family holder. Identity alone authorizes nothing.
    fn holder(
        &self,
        kind: JobRecordKind,
        context: &JobRecordContext<'_>,
    ) -> Result<(), JobRecordError> {
        match context.view.grants(self.published_by) {
            true => Ok(()),
            false => Err(JobRecordError::NotHolder(kind)),
        }
    }

    /// Only the committing family holder that minted the alias publishes a spec.
    fn verify_spec(
        &self,
        spec: &LogicalJobSpec,
        context: &JobRecordContext<'_>,
    ) -> Result<RecordVerdict, JobRecordError> {
        self.author(spec.origin_node_id, JobRecordKind::Spec)?;
        self.holder(JobRecordKind::Spec, context)?;
        if spec.realm_id != context.realm_id {
            return Err(JobRecordError::RealmMismatch);
        }
        // The stored submitter is the authority every later round re-checks, so
        // it must belong to the realm the record was published in.
        if spec.created_by.realm_id != spec.realm_id {
            return Err(JobRecordError::Unauthorized);
        }
        if spec.placement != context.placement {
            return Err(JobRecordError::PlacementMismatch);
        }
        spec.verify_digest()?;
        spec.admission_binds()?;
        spec.retry.validate()?;
        Ok(RecordVerdict::Authentic)
    }

    fn verify_claim(
        &self,
        claim: &SubmissionClaim,
        context: &JobRecordContext<'_>,
    ) -> Result<RecordVerdict, JobRecordError> {
        self.author(claim.committing_node_id, JobRecordKind::Claim)?;
        self.holder(JobRecordKind::Claim, context)?;
        let Some(spec) = context.spec else {
            return Ok(RecordVerdict::MissingEvidence(JobRecordKind::Spec));
        };
        match claim.job_id == spec.job_id
            && claim.spec_digest == spec.spec_digest
            && claim.committing_node_id == spec.origin_node_id
        {
            true => Ok(RecordVerdict::Authentic),
            false => Err(JobRecordError::EvidenceMismatch(JobRecordKind::Spec)),
        }
    }

    fn verify_budget(
        &self,
        budget: &WitnessBudgetRecord,
        context: &JobRecordContext<'_>,
    ) -> Result<RecordVerdict, JobRecordError> {
        self.author(budget.scheduler_node_id, JobRecordKind::Budget)?;
        self.holder(JobRecordKind::Budget, context)?;
        if budget.max_launches == 0 {
            return Err(JobRecordError::Contract(JobContractError::EmptyRetry));
        }
        let Some(spec) = context.spec else {
            return Ok(RecordVerdict::MissingEvidence(JobRecordKind::Spec));
        };
        match budget.source_spec_digest == spec.spec_digest
            && budget.max_launches <= spec.retry.max_launches_per_witness
        {
            true => Ok(RecordVerdict::Authentic),
            false => Err(JobRecordError::EvidenceMismatch(JobRecordKind::Spec)),
        }
    }

    /// An unreceipted launch is actionable only while its scheduler is a current
    /// holder here. Once the target signed its exact receipt, that receipt is the
    /// historical authority and a later placement change cannot revoke it.
    fn verify_launch(
        &self,
        launch: &LaunchIntent,
        context: &JobRecordContext<'_>,
    ) -> Result<RecordVerdict, JobRecordError> {
        self.author(launch.scheduler_node_id, JobRecordKind::Launch)?;
        let digest = launch.digest()?;
        let receipted = context.receipt.is_some_and(|receipt| {
            receipt.launch_id == launch.launch_id && receipt.launch_digest == digest
        });
        if !receipted {
            self.holder(JobRecordKind::Launch, context)?;
            // The witness must have planned under the placement this family is
            // judged in; a receipt is its own historical authority.
            if launch.witness_placement != context.placement {
                return Err(JobRecordError::PlacementMismatch);
            }
        }
        let Some(spec) = context.spec else {
            return Ok(RecordVerdict::MissingEvidence(JobRecordKind::Spec));
        };
        let Some(budget) = context.budget else {
            return Ok(RecordVerdict::MissingEvidence(JobRecordKind::Budget));
        };
        budget.admits(launch)?;
        match launch.spec_digest == spec.spec_digest && launch.job_id == spec.job_id {
            true => Ok(RecordVerdict::Authentic),
            false => Err(JobRecordError::EvidenceMismatch(JobRecordKind::Spec)),
        }
    }

    fn verify_receipt(
        &self,
        receipt: &ExecutionReceipt,
        context: &JobRecordContext<'_>,
    ) -> Result<RecordVerdict, JobRecordError> {
        self.author(receipt.executor_node_id, JobRecordKind::Receipt)?;
        if receipt.executor_node_id != receipt.target.node_id {
            return Err(JobRecordError::Inconsistent);
        }
        let Some(launch) = context.launch else {
            return Ok(RecordVerdict::MissingEvidence(JobRecordKind::Launch));
        };
        match receipt.launch_id == launch.launch_id
            && receipt.launch_digest == launch.digest()?
            && receipt.target == launch.target
            && receipt.job_id == launch.job_id
            && receipt.spec_digest == launch.spec_digest
        {
            true => Ok(RecordVerdict::Authentic),
            false => Err(JobRecordError::EvidenceMismatch(JobRecordKind::Launch)),
        }
    }

    fn verify_update(
        &self,
        update: &ExecutionUpdate,
        context: &JobRecordContext<'_>,
    ) -> Result<RecordVerdict, JobRecordError> {
        self.author(update.executor_node_id, JobRecordKind::Update)?;
        let Some(receipt) = context.receipt else {
            return Ok(RecordVerdict::MissingEvidence(JobRecordKind::Receipt));
        };
        if update.execution_id != receipt.execution_id
            || update.executor_node_id != receipt.executor_node_id
        {
            return Err(JobRecordError::EvidenceMismatch(JobRecordKind::Receipt));
        }
        match update.sequence {
            0 if update.previous_digest == receipt.digest()? => Ok(RecordVerdict::Authentic),
            0 => Err(JobRecordError::EvidenceMismatch(JobRecordKind::Receipt)),
            sequence => match context.previous_update {
                None => Ok(RecordVerdict::MissingEvidence(JobRecordKind::Update)),
                Some(previous)
                    if previous.execution_id == update.execution_id
                        && previous.sequence == sequence - 1
                        && previous.digest()? == update.previous_digest =>
                {
                    Ok(RecordVerdict::Authentic)
                }
                Some(_) => Err(JobRecordError::EvidenceMismatch(JobRecordKind::Update)),
            },
        }
    }

    fn verify_output(
        &self,
        output: &ExecutionOutputRecord,
        context: &JobRecordContext<'_>,
    ) -> Result<RecordVerdict, JobRecordError> {
        self.author(output.executor_node_id, JobRecordKind::Output)?;
        if output
            .outputs
            .as_slice()
            .iter()
            .any(|object| object.execution_id != output.execution_id)
        {
            return Err(JobRecordError::Inconsistent);
        }
        let Some(receipt) = context.receipt else {
            return Ok(self.local_output(output, context));
        };
        // The exact receipt digest covers the target's membership and subject
        // generations, so binding to it binds the epoch the work was accepted in.
        match output.execution_id == receipt.execution_id
            && output.executor_node_id == receipt.executor_node_id
            && output.job_id == receipt.job_id
            && output.receipt_digest == receipt.digest()?
            && output.spec_digest == receipt.spec_digest
        {
            true => Ok(RecordVerdict::Authentic),
            false => Err(JobRecordError::EvidenceMismatch(JobRecordKind::Receipt)),
        }
    }

    /// Until the launch and receipt rounds exist, a node's own fenced attempt is
    /// the only evidence behind its output record. It proves nothing about any
    /// other publisher, so anything else stays pending on its receipt.
    fn local_output(
        &self,
        output: &ExecutionOutputRecord,
        context: &JobRecordContext<'_>,
    ) -> RecordVerdict {
        match context.local {
            Some(local)
                if local.node_id == output.executor_node_id
                    && local.execution_id == output.execution_id
                    && local.fence_digest == output.receipt_digest
                    && local.spec_digest == output.spec_digest =>
            {
                RecordVerdict::LocalEvidence
            }
            _ => RecordVerdict::MissingEvidence(JobRecordKind::Receipt),
        }
    }

    /// Cancellation authority is defined against the stored spec, so the spec is
    /// required evidence, the publisher is the family holder that checked the
    /// caller's permission, and no bearer token ever replicates.
    fn verify_cancel(
        &self,
        cancel: &JobCancelRecord,
        context: &JobRecordContext<'_>,
    ) -> Result<RecordVerdict, JobRecordError> {
        self.holder(JobRecordKind::Cancel, context)?;
        let Some(spec) = context.spec else {
            return Ok(RecordVerdict::MissingEvidence(JobRecordKind::Spec));
        };
        if cancel.spec_digest != spec.spec_digest || cancel.job_id != spec.job_id {
            return Err(JobRecordError::EvidenceMismatch(JobRecordKind::Spec));
        }
        if cancel.requested_by.is_nil() || cancel.requested_by.realm_id != spec.realm_id {
            return Err(JobRecordError::Unauthorized);
        }
        match cancel.authority {
            CancelAuthority::Submitter => match cancel.requested_by == spec.created_by {
                true => Ok(RecordVerdict::Authentic),
                false => Err(JobRecordError::Unauthorized),
            },
            // The publishing holder's signature is its statement that it checked
            // group cancel permission; the payload field alone grants nothing.
            CancelAuthority::GroupAdmin => Ok(RecordVerdict::Authentic),
        }
    }
}

/// Longest receipt-rooted contiguous update chain, in sequence order. A gap, a
/// broken link, a state after a terminal one, or a success whose exact output
/// record is not durable truncates the projection instead of extending it.
pub fn verify_update_chain(
    receipt_digest: [u8; 32],
    output_digest: Option<[u8; 32]>,
    updates: &[ExecutionUpdate],
) -> Result<Vec<&ExecutionUpdate>, JobRecordError> {
    let mut ordered: Vec<&ExecutionUpdate> = updates.iter().collect();
    ordered.sort_by_key(|update| update.sequence);
    let mut chain: Vec<&ExecutionUpdate> = Vec::new();
    let mut previous = receipt_digest;
    let mut expected = 0u64;
    for index in 0..ordered.len() {
        let update = ordered[index];
        if index > 0 && ordered[index - 1].sequence == update.sequence {
            if ordered[index - 1] == update {
                continue;
            }
            return Err(JobRecordError::ChainConflict {
                sequence: update.sequence,
            });
        }
        if update.sequence != expected || update.previous_digest != previous {
            break;
        }
        if update.state == PhysicalExecutionState::Succeeded {
            let claimed = update
                .result
                .as_ref()
                .and_then(|result| result.output_digest);
            if claimed.is_none() || claimed != output_digest {
                break;
            }
        }
        chain.push(update);
        previous = update.digest()?;
        expected += 1;
        if update.state.is_terminal() {
            break;
        }
    }
    Ok(chain)
}

/// Replicated logical state of one request family. `Failed` requires a signed
/// permanent job failure; local exhaustion and silence stay `Indeterminate`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalJobState {
    Queued,
    Running,
    Indeterminate,
    Succeeded,
    Cancelled,
    Failed,
}

impl LogicalJobState {
    /// Stable machine-readable name for API payloads.
    pub fn name(&self) -> &'static str {
        match self {
            LogicalJobState::Queued => "queued",
            LogicalJobState::Running => "running",
            LogicalJobState::Indeterminate => "indeterminate",
            LogicalJobState::Succeeded => "succeeded",
            LogicalJobState::Cancelled => "cancelled",
            LogicalJobState::Failed => "failed",
        }
    }
}

/// How one physical execution relates to the canonical terminal result.
/// Redundant executions stay visible instead of being erased.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionRole {
    Canonical,
    DuplicateSuccess,
    Redundant,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectedExecution {
    pub execution_id: Ulid,
    pub executor_node_id: NodeId,
    pub state: PhysicalExecutionState,
    pub role: ExecutionRole,
    pub observed_at_ms: Option<u64>,
    pub result: Option<PhysicalExecutionResult>,
}

/// Deterministic reduction of one request family. It is derived from immutable
/// records only: local retry tasks, reachability, and the responder's clock are
/// never inputs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobProjection {
    pub submission_id: SubmissionId,
    pub request_digest: [u8; 32],
    pub canonical_job_id: JobId,
    /// Every same-request alias in `SubmissionClaim::order_key` order.
    pub aliases: Vec<JobId>,
    pub state: LogicalJobState,
    /// Canonical success, or canonical signed permanent failure when no success exists.
    pub canonical_execution_id: Option<Ulid>,
    pub executions: Vec<ProjectedExecution>,
    /// Outputs of the canonical execution, in their one canonical order.
    pub outputs: OutputSet,
    pub cancel_requested: bool,
}

impl JobProjection {
    /// Revision a client compares to detect that its view changed. Responder-local
    /// diagnostics stay outside it by construction.
    pub fn digest(&self) -> Result<[u8; 32], ConversionError> {
        Ok(*blake3::hash(&postcard::to_allocvec(self)?).as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret(seed: u8) -> iroh::SecretKey {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        iroh::SecretKey::from_bytes(&seed_bytes)
    }

    fn node_id(seed: u8) -> NodeId {
        secret(seed).public()
    }

    fn user(realm: u8, byte: u8) -> UserId {
        UserId::new(Ulid::from_bytes([byte; 16]), RealmId([realm; 32]))
    }

    #[test]
    fn digest_ignores_minter() {
        let document_id = Ulid::from_bytes([1; 16]);
        let first = JobPayload::MintPersistentId(MintPersistentIdSpec {
            document_id,
            minted_by: user(1, 2),
        });
        let second = JobPayload::MintPersistentId(MintPersistentIdSpec {
            document_id,
            minted_by: user(3, 4),
        });
        assert_eq!(first.plan_digest(), second.plan_digest());
    }

    fn probe_record(job_id: JobId, created_at_ms: u64) -> JobRecord {
        JobRecord::new(
            job_id,
            JobPayload::Probe {
                steps: 3,
                step_sleep_ms: 0,
                fail_at: None,
                panic_at: None,
                cleanup_marker: Some("/tmp/probe-marker".to_string()),
            },
            user(1, 2),
            node_id(7),
            created_at_ms,
            created_at_ms,
            Some(b"dedup".to_vec()),
        )
    }

    #[test]
    fn legal_transitions() {
        let legal = [
            (JobState::Queued, JobState::Claimed),
            (JobState::Queued, JobState::Cancelled),
            (JobState::Claimed, JobState::Running),
            (JobState::Claimed, JobState::Queued),
            (JobState::Claimed, JobState::Cancelled),
            (JobState::Claimed, JobState::Failed),
            (JobState::Running, JobState::Succeeded),
            (JobState::Running, JobState::Failed),
            (JobState::Running, JobState::Cancelled),
            (JobState::Running, JobState::Queued),
        ];
        for (from, to) in legal {
            assert!(
                validate_transition(JobExecutionClass::InProcess, from, to).is_ok(),
                "{from:?} -> {to:?}"
            );
        }
    }

    #[test]
    fn illegal_transitions() {
        let illegal = [
            (JobState::Queued, JobState::Running),
            (JobState::Queued, JobState::Succeeded),
            (JobState::Queued, JobState::Failed),
            (JobState::Queued, JobState::Queued),
            (JobState::Claimed, JobState::Succeeded),
            (JobState::Claimed, JobState::Claimed),
            (JobState::Running, JobState::Claimed),
            (JobState::Running, JobState::Running),
        ];
        for (from, to) in illegal {
            assert_eq!(
                validate_transition(JobExecutionClass::InProcess, from, to),
                Err(JobTransitionError { from, to }),
                "{from:?} -> {to:?}"
            );
        }
    }

    #[test]
    fn internal_rejects_external() {
        // External-only states must be rejected for an in-process job. Parking on
        // local exhaustion is the one Indeterminate edge both classes share.
        let external_only = [
            (JobState::Claimed, JobState::Preparing),
            (JobState::Preparing, JobState::Ready),
            (JobState::Ready, JobState::Running),
            (JobState::Running, JobState::Cancelling),
            (JobState::Cancelling, JobState::Cancelled),
            (JobState::Indeterminate, JobState::Running),
        ];
        for (from, to) in external_only {
            assert_eq!(
                validate_transition(JobExecutionClass::InProcess, from, to),
                Err(JobTransitionError { from, to }),
                "in-process must reject {from:?} -> {to:?}"
            );
        }
    }

    #[test]
    fn external_graph_legal() {
        // The fenced execution graph is accepted for external attempts.
        let legal = [
            (JobState::Claimed, JobState::Preparing),
            (JobState::Preparing, JobState::Ready),
            (JobState::Preparing, JobState::Queued),
            (JobState::Ready, JobState::Running),
            (JobState::Ready, JobState::Queued),
            // A permanent pre-attempt failure terminalizes without a container.
            (JobState::Preparing, JobState::Failed),
            (JobState::Ready, JobState::Failed),
            // A cancel before the attempt intent is written terminalizes without a
            // container; without these a cancel cannot land until the job is Running.
            (JobState::Preparing, JobState::Cancelled),
            (JobState::Ready, JobState::Cancelled),
            // A submit with an unknowable outcome parks after the intent write.
            (JobState::Ready, JobState::Indeterminate),
            (JobState::Ready, JobState::Cancelling),
            (JobState::Running, JobState::Cancelling),
            (JobState::Running, JobState::Indeterminate),
            (JobState::Cancelling, JobState::Cancelled),
            (JobState::Cancelling, JobState::Succeeded),
            (JobState::Cancelling, JobState::Failed),
            (JobState::Cancelling, JobState::Indeterminate),
            (JobState::Indeterminate, JobState::Running),
            (JobState::Indeterminate, JobState::Succeeded),
        ];
        for (from, to) in legal {
            assert!(
                validate_transition(JobExecutionClass::ExternalAttempt, from, to).is_ok(),
                "external must accept {from:?} -> {to:?}"
            );
        }
        // Ready cannot skip straight to Succeeded.
        assert!(
            validate_transition(
                JobExecutionClass::ExternalAttempt,
                JobState::Ready,
                JobState::Succeeded,
            )
            .is_err()
        );
    }

    #[test]
    fn terminal_absorbs() {
        for class in [
            JobExecutionClass::InProcess,
            JobExecutionClass::ExternalAttempt,
        ] {
            for from in [JobState::Succeeded, JobState::Failed, JobState::Cancelled] {
                for to in [
                    JobState::Queued,
                    JobState::Claimed,
                    JobState::Preparing,
                    JobState::Ready,
                    JobState::Running,
                    JobState::Cancelling,
                    JobState::Indeterminate,
                    JobState::Succeeded,
                    JobState::Failed,
                    JobState::Cancelled,
                ] {
                    assert_eq!(
                        validate_transition(class, from, to),
                        Err(JobTransitionError { from, to }),
                        "terminal {from:?} must reject -> {to:?}"
                    );
                }
            }
        }
    }

    fn input(key: &str, mode: InputMode, version: Option<&str>) -> InputSelection {
        InputSelection {
            source: InputSource::S3 {
                bucket: "src".to_string(),
                key: key.to_string(),
                version_id: version.map(str::to_string),
            },
            source_node_id: None,
            dest_key: format!("in/{key}"),
            mode,
            container_path: Some(format!("/inputs/{key}")),
            name: None,
            description: None,
        }
    }

    fn colliding() -> Vec<InputSelection> {
        let mut second = input("a", InputMode::Snapshot, None);
        second.source = InputSource::S3 {
            bucket: "other".to_string(),
            key: "a".to_string(),
            version_id: None,
        };
        vec![input("a", InputMode::Snapshot, None), second]
    }

    #[test]
    fn rejects_key_conflict() {
        assert_eq!(
            plan_composition(colliding(), CollisionPolicy::Reject),
            Err(CompositionError::KeyConflict("in/a".to_string()))
        );
    }

    #[test]
    fn resolves_key_conflict() {
        // Replace keeps the last claim on a key, KeepExisting the first.
        let replaced = plan_composition(colliding(), CollisionPolicy::Replace).unwrap();
        assert_eq!(replaced.len(), 1);
        assert_eq!(replaced[0].source, colliding()[1].source);

        let kept = plan_composition(colliding(), CollisionPolicy::KeepExisting).unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].source, colliding()[0].source);
    }

    #[test]
    fn enforces_reference_versions() {
        assert_eq!(
            plan_composition(
                vec![input("a", InputMode::ExactReference, None)],
                CollisionPolicy::Reject
            ),
            Err(CompositionError::MissingVersion("in/a".to_string()))
        );
        assert_eq!(
            plan_composition(
                vec![input("a", InputMode::FloatingReference, Some("01ARZ"))],
                CollisionPolicy::Reject
            ),
            Err(CompositionError::PinnedVersion("in/a".to_string()))
        );
        assert!(
            plan_composition(
                vec![
                    input("a", InputMode::ExactReference, Some("01ARZ")),
                    input("b", InputMode::FloatingReference, None),
                ],
                CollisionPolicy::Reject
            )
            .is_ok()
        );
    }

    #[test]
    fn resolves_workspace_outputs() {
        // Intents materialize against the derived bucket and drain once.
        let node_id = iroh::SecretKey::from_bytes(&[3u8; 32]).public();
        let mut spec = ExecutionSpec {
            group_id: Ulid::from_bytes([2u8; 16]),
            name: None,
            description: None,
            tags: Default::default(),
            image: "alpine".to_string(),
            entrypoint: None,
            command: Vec::new(),
            workdir: None,
            env: Default::default(),
            resources: Default::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: vec![WorkspaceOutput {
                container_path: "/out/report.txt".to_string(),
                dest_key: "outputs/report.txt".to_string(),
            }],
            output_prefixes: vec!["outputs/".to_string()],
            collision_policy: Default::default(),
        };

        spec.resolve_outputs("ws-job", node_id);

        assert!(spec.workspace_outputs.is_empty());
        assert_eq!(
            spec.file_outputs,
            vec![OutputSelection {
                container_path: "/out/report.txt".to_string(),
                path_prefix: None,
                destination_node_id: Some(node_id),
                destination: OutputDestination::S3 {
                    bucket: "ws-job".to_string(),
                    key: "outputs/report.txt".to_string(),
                },
                name: None,
                description: None,
            }]
        );

        spec.resolve_outputs("ws-job", node_id);
        assert_eq!(spec.file_outputs.len(), 1);
    }

    #[test]
    fn external_name_deterministic() {
        let id = JobId::from_bytes([0xAB; 16]);
        let name = attempt_external_name(id, 2);
        assert!(name.starts_with("aruna-"));
        assert!(name.ends_with("-a2"));
        assert_eq!(name, name.to_lowercase());
        assert_eq!(name, attempt_external_name(id, 2));
    }

    #[test]
    fn dedup_key_namespaces() {
        let job = JobId::from_bytes([1u8; 16]);
        let user_a = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
        let user_b = UserId::new(Ulid::from_bytes([3u8; 16]), RealmId([1u8; 32]));

        assert!(run_crate_dedup_key(job).starts_with(b"internal/"));
        assert!(cleanup_dedup_key(job).starts_with(b"internal/"));
        assert_ne!(cleanup_dedup_key(job), run_crate_dedup_key(job));
        assert!(user_dedup_key(user_a, "k").starts_with(b"user/"));
        // A caller cannot forge an internal obligation key through their
        // idempotency key, and users cannot squat each other's keys.
        assert_ne!(
            user_dedup_key(user_a, &format!("internal/run-crate/{job}")),
            run_crate_dedup_key(job)
        );
        assert_ne!(user_dedup_key(user_a, "k"), user_dedup_key(user_b, "k"));
        assert_eq!(user_dedup_key(user_a, "k"), user_dedup_key(user_a, "k"));
        assert_eq!(cleanup_job_id(job), cleanup_job_id(job));
        assert_ne!(cleanup_job_id(job), crate_job_id(job));
        assert_eq!(workspace_credential_id(job), format!("ws{job}"));
    }

    #[test]
    fn routable_child_ids() {
        // Child obligations remain on a non-default, high parent bucket.
        let parent = JobId::from_parts(
            1,
            PlacementHandle::new(crate::structs::FIRST_GRANTABLE_HANDLE).unwrap(),
            BucketId::new(3_000).unwrap(),
            4,
        )
        .unwrap();
        assert_eq!(crate_job_id(parent), crate_job_id(parent));
        assert_eq!(cleanup_job_id(parent), cleanup_job_id(parent));
        assert_ne!(crate_job_id(parent), cleanup_job_id(parent));
        for child in [crate_job_id(parent), cleanup_job_id(parent)] {
            let routed = child.as_routable().unwrap();
            assert_eq!(
                routed.placement_handle(),
                parent.as_routable().unwrap().placement_handle()
            );
            assert_eq!(routed.bucket(), parent.as_routable().unwrap().bucket());
        }
    }

    #[test]
    fn rejects_plain_ids() {
        let plain = Ulid::from_parts(1, 0);
        assert!(JobId::try_from_bytes(plain.to_bytes()).is_err());
        assert!(plain.to_string().parse::<JobId>().is_err());
        let encoded = postcard::to_allocvec(&plain).unwrap();
        assert!(postcard::from_bytes::<JobId>(&encoded).is_err());
    }

    #[test]
    fn dedup_value_roundtrips() {
        let id = JobId::from_bytes([7u8; 16]);
        let digest = [9u8; 32];
        let encoded = encode_job_dedup_value(id, digest);
        assert_eq!(encoded.len(), 48);
        assert_eq!(parse_job_dedup_value(&encoded).unwrap(), (id, digest));
        assert!(parse_job_dedup_value(&encoded[..16]).is_err());
    }

    #[test]
    fn record_roundtrips() {
        let record = probe_record(JobId::from_bytes([5u8; 16]), 1_700_000_000_000);
        let bytes = record.to_bytes().unwrap();
        assert_eq!(JobRecord::from_bytes(&bytes).unwrap(), record);
    }

    #[test]
    fn rocrate_record_roundtrips() {
        let owner = user(1, 2);
        let record = JobRecord::new(
            JobId::from_bytes([5u8; 16]),
            JobPayload::ImportRoCrate(ImportRoCrateSpec {
                auth_context: AuthContext {
                    user_id: owner,
                    realm_id: RealmId([1u8; 32]),
                    path_restrictions: None,
                    session: None,
                },
                source: ImportRoCrateSource::Upload {
                    upload_id: Ulid::from_bytes([3u8; 16]),
                },
                target: ImportRoCrateTarget {
                    bucket: "target".to_string(),
                    prefix: "crate".to_string(),
                },
                metadata: ImportMetadataTarget {
                    group_id: Ulid::from_bytes([4u8; 16]),
                    path: "crate".to_string(),
                    public: false,
                },
                limits: RoCrateLimits::default(),
                document_id: Ulid::from_bytes([5u8; 16]),
            }),
            owner,
            node_id(7),
            1_700_000_000_000,
            1_700_000_000_000,
            Some(b"dedup".to_vec()),
        );
        let bytes = record.to_bytes().unwrap();
        assert_eq!(JobRecord::from_bytes(&bytes).unwrap(), record);
    }

    #[test]
    fn rejects_short_record() {
        // A predecessor-format record must fail loudly, never decode with defaults.
        let record = probe_record(JobId::from_bytes([6u8; 16]), 1_700_000_000_000);
        let bytes = record.to_bytes().unwrap();
        let retention = postcard::to_allocvec(&record.retention_ms).unwrap();
        let digest = postcard::to_allocvec(&Option::<[u8; 32]>::None).unwrap();
        let mode = postcard::to_allocvec(&WorkspaceMode::Existing).unwrap();

        for trim in [
            retention.len(),
            retention.len() + digest.len(),
            retention.len() + digest.len() + mode.len(),
        ] {
            assert!(JobRecord::from_bytes(&bytes[..bytes.len() - trim]).is_err());
        }
    }

    fn submission() -> SubmissionId {
        SubmissionId([3u8; 32])
    }

    fn target() -> ExecutionTargetId {
        ExecutionTargetId {
            node_id: node_id(9),
            executor_kind: "docker".to_string(),
        }
    }

    fn family() -> JobFamilyId {
        JobFamilyId {
            submission_id: submission(),
            request_digest: [1u8; 32],
        }
    }

    fn placement() -> PlacementRef {
        PlacementRef {
            strategy_id: Ulid::from_bytes([9u8; 16]),
            shard: 5,
        }
    }

    /// Owns what a borrowed [`HolderView`] points at: node 1 holds the family,
    /// node 2 is a member that never held it, node 9 is the execution target.
    struct LocalView {
        members: Vec<NodeId>,
        holders: Vec<NodeId>,
    }

    impl LocalView {
        fn new() -> Self {
            Self {
                members: vec![node_id(1), node_id(2), node_id(9)],
                holders: vec![node_id(1)],
            }
        }

        /// The view after the family moved off its former holder.
        fn moved() -> Self {
            Self {
                holders: vec![node_id(2)],
                ..Self::new()
            }
        }

        fn context(&self) -> JobRecordContext<'_> {
            JobRecordContext {
                view: HolderView {
                    members: &self.members,
                    holders: &self.holders,
                },
                ..JobRecordContext::new(RealmId([8u8; 32]), family(), placement())
            }
        }
    }

    fn envelope(record: JobFamilyRecord, seed: u8) -> JobRecordEnvelope {
        JobRecordEnvelope::sign(RealmId([8u8; 32]), record, &secret(seed)).expect("record signs")
    }

    fn sample_output() -> OutputObject {
        OutputObject {
            node_id: node_id(1),
            bucket: "dest".to_string(),
            key: "out/report.txt".to_string(),
            version_id: Ulid::from_bytes([4u8; 16]),
            execution_id: Ulid::from_bytes([13u8; 16]),
            container_path: "/out/report.txt".to_string(),
            size: 12,
            digest: Some("aa".repeat(32)),
        }
    }

    fn sample_resources() -> EffectiveResources {
        EffectiveResources {
            cpu_cores: 4,
            ram_bytes: 8 * 1024 * 1024 * 1024,
            disk_bytes: 32 * 1024 * 1024 * 1024,
            max_walltime_ms: 3_600_000,
            preemptible: false,
        }
    }

    fn sample_admission() -> JobAdmissionRecord {
        JobAdmissionRecord {
            submission_id: submission(),
            request_digest: [1u8; 32],
            job_id: JobId::from_bytes([6u8; 16]),
            group_id: Ulid::from_bytes([7u8; 16]),
            admitting_node_id: node_id(1),
            membership_generation: 4,
            resources: sample_resources(),
            admitted_at_ms: 1_700_000_000_000,
        }
    }

    fn sample_spec() -> LogicalJobSpec {
        LogicalJobSpec {
            submission_id: submission(),
            job_id: JobId::from_bytes([6u8; 16]),
            origin_node_id: node_id(1),
            ingress_node_id: node_id(1),
            realm_id: RealmId([8u8; 32]),
            group_id: Ulid::from_bytes([7u8; 16]),
            created_by: user(8, 2),
            created_at_ms: 1_700_000_000_000,
            retention_ms: DEFAULT_JOB_RETENTION_MS,
            payload: ExecutionSpec {
                group_id: Ulid::from_bytes([7u8; 16]),
                name: None,
                description: None,
                tags: Default::default(),
                image: "alpine".to_string(),
                entrypoint: None,
                command: vec!["true".to_string()],
                workdir: None,
                env: Default::default(),
                resources: Default::default(),
                executor_constraint: None,
                inputs: Vec::new(),
                file_outputs: Vec::new(),
                workspace_outputs: Vec::new(),
                output_prefixes: Vec::new(),
                collision_policy: Default::default(),
            },
            request_digest: [1u8; 32],
            spec_digest: [0u8; 32],
            resources: sample_resources(),
            retry: JobRetryPolicy {
                max_launches_per_witness: 3,
            },
            admission: sample_admission(),
            captured_inputs: Vec::new(),
            output_policies: Vec::new(),
            placement: placement(),
        }
        .store_digest()
        .expect("spec digest stored")
    }

    fn spec_digest() -> [u8; 32] {
        sample_spec().spec_digest
    }

    fn sample_claim() -> SubmissionClaim {
        SubmissionClaim {
            submission_id: submission(),
            job_id: JobId::from_bytes([6u8; 16]),
            request_digest: [1u8; 32],
            spec_digest: spec_digest(),
            committing_node_id: node_id(1),
            accepted_at_ms: 1_700_000_000_000,
        }
    }

    fn sample_budget() -> WitnessBudgetRecord {
        WitnessBudgetRecord {
            submission_id: submission(),
            request_digest: [1u8; 32],
            scheduler_node_id: node_id(1),
            source_spec_digest: spec_digest(),
            max_launches: 3,
        }
    }

    fn sample_launch() -> LaunchIntent {
        LaunchIntent {
            launch_id: Ulid::from_bytes([10u8; 16]),
            submission_id: submission(),
            request_digest: [1u8; 32],
            job_id: JobId::from_bytes([6u8; 16]),
            scheduler_node_id: node_id(1),
            scheduler_seq: 0,
            witness_placement: placement(),
            holder_generation: 11,
            target: target(),
            inputs: Vec::new(),
            output_policies: Vec::new(),
            plan_digest: [12u8; 32],
            spec_digest: spec_digest(),
            created_at_ms: 1_700_000_000_000,
        }
    }

    fn sample_receipt() -> ExecutionReceipt {
        ExecutionReceipt {
            execution_id: Ulid::from_bytes([13u8; 16]),
            launch_id: Ulid::from_bytes([10u8; 16]),
            launch_digest: sample_launch().digest().expect("launch digests"),
            submission_id: submission(),
            request_digest: [1u8; 32],
            job_id: JobId::from_bytes([6u8; 16]),
            executor_node_id: node_id(9),
            target: target(),
            spec_digest: spec_digest(),
            membership_generation: 4,
            subject_generation: 2,
            subject_digest: [15u8; 32],
            accepted_at_ms: 1_700_000_000_000,
        }
    }

    fn output_record() -> ExecutionOutputRecord {
        ExecutionOutputRecord {
            execution_id: Ulid::from_bytes([13u8; 16]),
            submission_id: submission(),
            request_digest: [1u8; 32],
            job_id: JobId::from_bytes([6u8; 16]),
            executor_node_id: node_id(9),
            spec_digest: spec_digest(),
            receipt_digest: sample_receipt().digest().expect("receipt digests"),
            outputs: OutputSet::canonical(vec![sample_output()]).expect("canonical outputs"),
            committed_at_ms: 1_700_000_001_000,
        }
    }

    fn sample_update() -> ExecutionUpdate {
        ExecutionUpdate {
            execution_id: Ulid::from_bytes([13u8; 16]),
            submission_id: submission(),
            request_digest: [1u8; 32],
            executor_node_id: node_id(9),
            sequence: 0,
            previous_digest: sample_receipt().digest().expect("receipt digests"),
            state: PhysicalExecutionState::Running,
            observed_at_ms: 1_700_000_001_000,
            result: None,
        }
    }

    /// Terminal success naming the durable output record it depends on.
    fn success_update() -> ExecutionUpdate {
        ExecutionUpdate {
            sequence: 1,
            previous_digest: sample_update().digest().expect("update digests"),
            state: PhysicalExecutionState::Succeeded,
            observed_at_ms: 1_700_000_002_000,
            result: Some(PhysicalExecutionResult {
                exit_code: Some(0),
                output_digest: Some(output_record().digest().expect("output digests")),
                message: None,
                stdout: None,
                stderr: None,
            }),
            ..sample_update()
        }
    }

    fn sample_cancel() -> JobCancelRecord {
        JobCancelRecord {
            cancel_id: Ulid::from_bytes([17u8; 16]),
            submission_id: submission(),
            request_digest: [1u8; 32],
            job_id: JobId::from_bytes([6u8; 16]),
            spec_digest: spec_digest(),
            requested_by: user(8, 2),
            authority: CancelAuthority::Submitter,
            requested_at_ms: 1_700_000_002_000,
        }
    }

    fn sample_projection() -> JobProjection {
        JobProjection {
            submission_id: submission(),
            request_digest: [1u8; 32],
            canonical_job_id: JobId::from_bytes([6u8; 16]),
            aliases: vec![JobId::from_bytes([6u8; 16]), JobId::from_bytes([18u8; 16])],
            state: LogicalJobState::Succeeded,
            canonical_execution_id: Some(Ulid::from_bytes([13u8; 16])),
            executions: vec![ProjectedExecution {
                execution_id: Ulid::from_bytes([13u8; 16]),
                executor_node_id: node_id(9),
                state: PhysicalExecutionState::Succeeded,
                role: ExecutionRole::Canonical,
                observed_at_ms: Some(1_700_000_001_000),
                result: Some(PhysicalExecutionResult {
                    exit_code: Some(0),
                    output_digest: Some([16u8; 32]),
                    message: None,
                    stdout: None,
                    stderr: None,
                }),
            }],
            outputs: OutputSet::canonical(vec![sample_output()]).expect("canonical outputs"),
            cancel_requested: false,
        }
    }

    fn reencode<T: Serialize + serde::de::DeserializeOwned>(value: &T) -> T {
        postcard::from_bytes(&postcard::to_allocvec(value).unwrap()).unwrap()
    }

    fn wire_digest<T: Serialize>(value: &T) -> String {
        let bytes = postcard::to_allocvec(value).unwrap();
        hex::encode(blake3::hash(&bytes).as_bytes())
    }

    #[test]
    fn roundtrips_records() {
        assert_eq!(reencode(&sample_output()), sample_output());
        assert_eq!(reencode(&sample_spec()), sample_spec());
        assert_eq!(reencode(&sample_admission()), sample_admission());
        assert_eq!(reencode(&sample_claim()), sample_claim());
        assert_eq!(reencode(&sample_budget()), sample_budget());
        assert_eq!(reencode(&sample_launch()), sample_launch());
        assert_eq!(reencode(&sample_receipt()), sample_receipt());
        assert_eq!(reencode(&sample_update()), sample_update());
        assert_eq!(reencode(&output_record()), output_record());
        assert_eq!(reencode(&sample_cancel()), sample_cancel());
        assert_eq!(reencode(&sample_projection()), sample_projection());
        let signed = envelope(JobFamilyRecord::Cancel(sample_cancel()), 1);
        assert_eq!(reencode(&signed), signed);
    }

    #[test]
    fn canonical_encodings() {
        // Golden wire digests: a change here must be a deliberate format change.
        let digests = [
            wire_digest(&sample_output()),
            wire_digest(&sample_spec()),
            wire_digest(&sample_admission()),
            wire_digest(&sample_claim()),
            wire_digest(&sample_budget()),
            wire_digest(&sample_launch()),
            wire_digest(&sample_receipt()),
            wire_digest(&sample_update()),
            wire_digest(&output_record()),
            wire_digest(&sample_cancel()),
            wire_digest(&sample_projection()),
        ];
        assert_eq!(
            digests,
            [
                "2e3adfb3440145e2f0acf7dcac006c00cdbccb0fa8df37b935fc0ba4055993d2",
                "77468e0780d1ea7a34f8191e280a3b6102255a5bdb05b9890cc0f03b40695979",
                "1c4dc854b3931565ff75fafec7a24673cc03c1989516f9b46dc93a093ec2bfeb",
                "1ca48ad6fc1652c190e1808e565f7794fe08f32ac87b4d9e440447d87e2c3b93",
                "db3657f9c0d342c3291b32f42bbadf757a7356030786a173cf8c569f38695c86",
                "ded9dc94e1d65f2502e30dbcf8a5c4d2e588922d39790e451b6849f6f14ecd62",
                "ec4c067595ea40647f1d6b293f7f9daf60fa8113be1885825360294271df4c6e",
                "15a1c2404110cfc66720b7a7bb8f77f33d199665e2c028229efa3c708b66917a",
                "6a2c88bab322eae973d142690b0a2fb0ba7a5bf5c417df9cc515de498b26635d",
                "f69a68ef56b007a54533ebdb696b806e43b4cf04a75fdb453449d22190853310",
                "f2f6b9fb023ed033aade2c1af131943e1243b82a3ca58469b5fad70a52c013f6",
            ]
        );
        assert_eq!(postcard::to_allocvec(&submission()).unwrap(), vec![3u8; 32]);
    }

    #[test]
    fn derives_submission_id() {
        let caller = user(1, 2);
        let other = user(1, 3);
        assert_eq!(
            SubmissionId::keyed(caller, b"key"),
            SubmissionId::keyed(caller, b"key")
        );
        assert_ne!(
            SubmissionId::keyed(caller, b"key"),
            SubmissionId::keyed(other, b"key")
        );
        // A different realm under one user ulid is a different family.
        assert_ne!(
            SubmissionId::keyed(caller, b"key"),
            SubmissionId::keyed(user(9, 2), b"key")
        );
        // Length prefixing keeps a shifted key from colliding with a longer one.
        assert_ne!(
            SubmissionId::keyed(caller, b"ab"),
            SubmissionId::keyed(caller, b"a")
        );
        assert_ne!(
            SubmissionId::unkeyed(Ulid::from_bytes([1u8; 16])),
            SubmissionId::unkeyed(Ulid::from_bytes([2u8; 16]))
        );
    }

    #[test]
    fn rejects_empty_retry() {
        assert_eq!(
            JobRetryPolicy {
                max_launches_per_witness: 0
            }
            .validate(),
            Err(JobContractError::EmptyRetry)
        );
        assert!(
            JobRetryPolicy {
                max_launches_per_witness: 1
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn admits_stored_launch() {
        let budget = sample_budget();
        assert_eq!(budget.admits(&sample_launch()), Ok(()));

        let mut exhausted = sample_launch();
        exhausted.scheduler_seq = budget.max_launches;
        assert_eq!(
            budget.admits(&exhausted),
            Err(JobContractError::BudgetExhausted {
                sequence: budget.max_launches,
                max_launches: budget.max_launches,
            })
        );

        let mut drifted = sample_launch();
        drifted.spec_digest = [99u8; 32];
        assert_eq!(budget.admits(&drifted), Err(JobContractError::SpecMismatch));

        let mut foreign = sample_launch();
        foreign.scheduler_node_id = node_id(2);
        assert_eq!(
            budget.admits(&foreign),
            Err(JobContractError::BudgetMismatch)
        );
    }

    #[test]
    fn orders_canonical_records() {
        // Selection must not move with a timestamp or the committing node.
        let mut restamped = sample_claim();
        restamped.accepted_at_ms = u64::MAX;
        restamped.committing_node_id = node_id(3);
        assert_eq!(restamped.order_key(), sample_claim().order_key());

        let mut alias = sample_claim();
        alias.job_id = JobId::from_bytes([2u8; 16]);
        assert_ne!(alias.order_key(), sample_claim().order_key());

        let execution = Ulid::from_bytes([13u8; 16]);
        assert_eq!(
            canonical_execution_key(submission(), [1u8; 32], execution),
            canonical_execution_key(submission(), [1u8; 32], execution)
        );
        assert_ne!(
            canonical_execution_key(submission(), [1u8; 32], execution),
            canonical_execution_key(submission(), [1u8; 32], Ulid::from_bytes([14u8; 16]))
        );
        assert_ne!(
            canonical_execution_key(submission(), [1u8; 32], execution),
            canonical_execution_key(submission(), [2u8; 32], execution)
        );
    }

    #[test]
    fn digest_excludes_self() {
        // The one self-referential field is zeroed, never omitted.
        let spec = sample_spec();
        let mut restamped = spec.clone();
        restamped.spec_digest = [77u8; 32];
        assert_eq!(spec.digest().unwrap(), restamped.digest().unwrap());
        assert_eq!(spec.verify_digest(), Ok(()));
        assert_eq!(
            restamped.verify_digest(),
            Err(JobRecordError::DigestMismatch)
        );
        let mut moved = spec.clone();
        moved.created_at_ms += 1;
        assert_ne!(moved.digest().unwrap(), spec.digest().unwrap());
    }

    #[test]
    fn domains_stay_distinct() {
        // `tag || body` is only unambiguous while no tag prefixes another.
        let domains = [
            JOB_SPEC_DOMAIN,
            JOB_CLAIM_DOMAIN,
            JOB_BUDGET_DOMAIN,
            JOB_LAUNCH_DOMAIN,
            JOB_RECEIPT_DOMAIN,
            JOB_UPDATE_DOMAIN,
            JOB_OUTPUT_DOMAIN,
            JOB_CANCEL_DOMAIN,
            JOB_ENVELOPE_DOMAIN,
        ];
        for (index, left) in domains.iter().enumerate() {
            for right in domains.iter().skip(index + 1) {
                assert!(!left.starts_with(right) && !right.starts_with(left));
            }
        }
    }

    #[test]
    fn rejects_forged_author() {
        let view = LocalView::new();
        let record = JobFamilyRecord::Spec(Box::new(sample_spec()));
        assert_eq!(
            envelope(record.clone(), 1).verify(&view.context()),
            Ok(RecordVerdict::Authentic)
        );
        // A relay restating the record signs with its own key and is refused.
        assert_eq!(
            envelope(record.clone(), 2).verify(&view.context()),
            Err(JobRecordError::WrongPublisher(JobRecordKind::Spec))
        );
        let mut restated = envelope(record, 2);
        restated.published_by = node_id(1);
        assert_eq!(
            restated.verify(&view.context()),
            Err(JobRecordError::BadSignature)
        );
    }

    #[test]
    fn rejects_forged_fields() {
        let view = LocalView::new();
        let mut forged = envelope(JobFamilyRecord::Claim(sample_claim()), 1);
        let JobFamilyRecord::Claim(claim) = &mut forged.record else {
            unreachable!("claim record")
        };
        claim.accepted_at_ms = 0;
        assert_eq!(
            forged.verify(&view.context()),
            Err(JobRecordError::BadSignature)
        );
    }

    #[test]
    fn relay_keeps_publisher() {
        // Replication is transport: the envelope crosses a holder unchanged.
        let view = LocalView::new();
        let spec = sample_spec();
        let checked = JobRecordContext {
            spec: Some(&spec),
            ..view.context()
        };
        let signed = envelope(JobFamilyRecord::Claim(sample_claim()), 1);
        let relayed = reencode(&signed);
        assert_eq!(relayed.published_by, node_id(1));
        assert_eq!(relayed.verify(&checked), Ok(RecordVerdict::Authentic));
        assert_eq!(relayed.key(), signed.key());
    }

    #[test]
    fn replay_is_identical() {
        let first = envelope(JobFamilyRecord::Budget(sample_budget()), 1);
        let second = envelope(JobFamilyRecord::Budget(sample_budget()), 1);
        assert_eq!(first.key(), second.key());
        assert_eq!(first.digest().unwrap(), second.digest().unwrap());
    }

    #[test]
    fn detects_byte_conflict() {
        let first = sample_update();
        let mut second = sample_update();
        second.observed_at_ms += 1;
        assert_eq!(
            JobFamilyRecord::Update(Box::new(first.clone())).key(),
            JobFamilyRecord::Update(Box::new(second.clone())).key()
        );
        assert_ne!(first.digest().unwrap(), second.digest().unwrap());
    }

    #[test]
    fn rejects_wrong_family() {
        let view = LocalView::new();
        let mut foreign = sample_claim();
        foreign.request_digest = [44u8; 32];
        assert_eq!(
            envelope(JobFamilyRecord::Claim(foreign), 1).verify(&view.context()),
            Err(JobRecordError::FamilyMismatch)
        );
    }

    #[test]
    fn rejects_wrong_realm() {
        let view = LocalView::new();
        let record = JobFamilyRecord::Claim(sample_claim());
        let elsewhere = JobRecordEnvelope::sign(RealmId([99u8; 32]), record, &secret(1)).unwrap();
        assert_eq!(
            elsewhere.verify(&view.context()),
            Err(JobRecordError::RealmMismatch)
        );
        let mut spec = sample_spec();
        spec.realm_id = RealmId([99u8; 32]);
        let spec = spec.store_digest().unwrap();
        assert_eq!(
            envelope(JobFamilyRecord::Spec(Box::new(spec)), 1).verify(&view.context()),
            Err(JobRecordError::RealmMismatch)
        );
    }

    #[test]
    fn rejects_foreign_placement() {
        let view = LocalView::new();
        let mut spec = sample_spec();
        spec.placement = PlacementRef::NIL;
        let spec = spec.store_digest().unwrap();
        assert_eq!(
            envelope(JobFamilyRecord::Spec(Box::new(spec)), 1).verify(&view.context()),
            Err(JobRecordError::PlacementMismatch)
        );
    }

    #[test]
    fn binds_receipt_launch() {
        let view = LocalView::new();
        let launch = sample_launch();
        let spec = sample_spec();
        let checked = JobRecordContext {
            spec: Some(&spec),
            launch: Some(&launch),
            ..view.context()
        };
        let signed = envelope(JobFamilyRecord::Receipt(Box::new(sample_receipt())), 9);
        assert_eq!(signed.verify(&checked), Ok(RecordVerdict::Authentic));

        let mut drifted = sample_receipt();
        drifted.launch_digest = [1u8; 32];
        assert_eq!(
            envelope(JobFamilyRecord::Receipt(Box::new(drifted)), 9).verify(&checked),
            Err(JobRecordError::EvidenceMismatch(JobRecordKind::Launch))
        );
    }

    #[test]
    fn bounds_stored_budget() {
        let view = LocalView::new();
        let budget = sample_budget();
        let spec = sample_spec();
        let checked = JobRecordContext {
            spec: Some(&spec),
            budget: Some(&budget),
            ..view.context()
        };
        assert_eq!(
            envelope(JobFamilyRecord::Launch(Box::new(sample_launch())), 1).verify(&checked),
            Ok(RecordVerdict::Authentic)
        );
        let mut beyond = sample_launch();
        beyond.scheduler_seq = budget.max_launches;
        assert_eq!(
            envelope(JobFamilyRecord::Launch(Box::new(beyond)), 1).verify(&checked),
            Err(JobRecordError::Contract(
                JobContractError::BudgetExhausted {
                    sequence: budget.max_launches,
                    max_launches: budget.max_launches,
                }
            ))
        );
    }

    #[test]
    fn cancel_needs_spec() {
        // Cancellation authority is defined only against the stored spec.
        let view = LocalView::new();
        let signed = envelope(JobFamilyRecord::Cancel(sample_cancel()), 1);
        assert_eq!(
            signed.verify(&view.context()),
            Ok(RecordVerdict::MissingEvidence(JobRecordKind::Spec))
        );
        let spec = sample_spec();
        let checked = JobRecordContext {
            spec: Some(&spec),
            ..view.context()
        };
        assert_eq!(signed.verify(&checked), Ok(RecordVerdict::Authentic));

        let mut stranger = sample_cancel();
        stranger.requested_by = user(8, 5);
        assert_eq!(
            envelope(JobFamilyRecord::Cancel(stranger), 1).verify(&checked),
            Err(JobRecordError::Unauthorized)
        );
        stranger.authority = CancelAuthority::GroupAdmin;
        assert_eq!(
            envelope(JobFamilyRecord::Cancel(stranger), 1).verify(&checked),
            Ok(RecordVerdict::Authentic)
        );
        stranger.requested_by = user(9, 5);
        assert_eq!(
            envelope(JobFamilyRecord::Cancel(stranger), 1).verify(&checked),
            Err(JobRecordError::Unauthorized)
        );
        let mut replayed = sample_cancel();
        replayed.spec_digest = [1u8; 32];
        assert_eq!(
            envelope(JobFamilyRecord::Cancel(replayed), 1).verify(&checked),
            Err(JobRecordError::EvidenceMismatch(JobRecordKind::Spec))
        );
    }

    /// Every kind whose author must be a current family holder, signed by the
    /// node its own payload names as author.
    fn holder_records() -> Vec<(JobRecordKind, JobRecordEnvelope)> {
        vec![
            (
                JobRecordKind::Spec,
                envelope(JobFamilyRecord::Spec(Box::new(sample_spec())), 1),
            ),
            (
                JobRecordKind::Claim,
                envelope(JobFamilyRecord::Claim(sample_claim()), 1),
            ),
            (
                JobRecordKind::Budget,
                envelope(JobFamilyRecord::Budget(sample_budget()), 1),
            ),
            (
                JobRecordKind::Launch,
                envelope(JobFamilyRecord::Launch(Box::new(sample_launch())), 1),
            ),
            (
                JobRecordKind::Cancel,
                envelope(JobFamilyRecord::Cancel(sample_cancel()), 1),
            ),
        ]
    }

    #[test]
    fn rejects_non_holder() {
        // A valid realm key proves identity; only the local view grants authority.
        let view = LocalView::new();
        let spec = sample_spec();
        let checked = JobRecordContext {
            spec: Some(&spec),
            ..view.context()
        };
        for (kind, signed) in holder_records() {
            assert_eq!(signed.verify(&checked).map(|_| kind), Ok(kind));
        }

        // Node 2 is a realm member that never held this family.
        let stranger = LocalView {
            holders: vec![node_id(2)],
            ..LocalView::new()
        };
        let outsider = JobRecordContext {
            spec: Some(&spec),
            ..stranger.context()
        };
        for (kind, signed) in holder_records() {
            assert_eq!(
                signed.verify(&outsider),
                Err(JobRecordError::NotHolder(kind))
            );
        }

        // A caller that resolved no view at all grants nothing.
        let unresolved = JobRecordContext::new(RealmId([8u8; 32]), family(), placement());
        for (kind, signed) in holder_records() {
            assert_eq!(
                signed.verify(&unresolved),
                Err(JobRecordError::NotHolder(kind))
            );
        }
    }

    #[test]
    fn rejects_former_holder() {
        // A node the map still ranks but the realm no longer lists holds nothing.
        let departed = LocalView {
            members: vec![node_id(2), node_id(9)],
            holders: vec![node_id(1)],
        };
        let spec = sample_spec();
        let checked = JobRecordContext {
            spec: Some(&spec),
            ..departed.context()
        };
        for (kind, signed) in holder_records() {
            assert_eq!(
                signed.verify(&checked),
                Err(JobRecordError::NotHolder(kind))
            );
        }
    }

    #[test]
    fn keeps_receipted_launch() {
        // The target's signed receipt is historical authority: the launch stays
        // valid after the family moved off the scheduler that published it.
        let moved = LocalView::moved();
        let spec = sample_spec();
        let budget = sample_budget();
        let receipt = sample_receipt();
        let signed = envelope(JobFamilyRecord::Launch(Box::new(sample_launch())), 1);
        let unreceipted = JobRecordContext {
            spec: Some(&spec),
            budget: Some(&budget),
            ..moved.context()
        };
        assert_eq!(
            signed.verify(&unreceipted),
            Err(JobRecordError::NotHolder(JobRecordKind::Launch))
        );
        let checked = JobRecordContext {
            receipt: Some(&receipt),
            ..unreceipted
        };
        assert_eq!(signed.verify(&checked), Ok(RecordVerdict::Authentic));

        // A receipt for another launch is not that authority.
        let mut other = sample_receipt();
        other.launch_id = Ulid::from_bytes([20u8; 16]);
        assert_eq!(
            signed.verify(&JobRecordContext {
                receipt: Some(&other),
                ..unreceipted
            }),
            Err(JobRecordError::NotHolder(JobRecordKind::Launch))
        );
    }

    #[test]
    fn holds_missing_evidence() {
        // A dependent record arriving before its predecessor is pending, never
        // authentic and never an error that would drop it.
        let view = LocalView::new();
        let bare = view.context();
        let cases = [
            (
                envelope(JobFamilyRecord::Claim(sample_claim()), 1),
                JobRecordKind::Spec,
            ),
            (
                envelope(JobFamilyRecord::Budget(sample_budget()), 1),
                JobRecordKind::Spec,
            ),
            (
                envelope(JobFamilyRecord::Launch(Box::new(sample_launch())), 1),
                JobRecordKind::Spec,
            ),
            (
                envelope(JobFamilyRecord::Receipt(Box::new(sample_receipt())), 9),
                JobRecordKind::Launch,
            ),
            (
                envelope(JobFamilyRecord::Update(Box::new(sample_update())), 9),
                JobRecordKind::Receipt,
            ),
            (
                envelope(JobFamilyRecord::Output(Box::new(output_record())), 9),
                JobRecordKind::Receipt,
            ),
            (
                envelope(JobFamilyRecord::Cancel(sample_cancel()), 1),
                JobRecordKind::Spec,
            ),
        ];
        for (signed, missing) in cases {
            assert_eq!(
                signed.verify(&bare),
                Ok(RecordVerdict::MissingEvidence(missing)),
                "{:?}",
                signed.kind()
            );
        }
        // The launch's own budget is required even once the spec is verified.
        let spec = sample_spec();
        assert_eq!(
            envelope(JobFamilyRecord::Launch(Box::new(sample_launch())), 1).verify(
                &JobRecordContext {
                    spec: Some(&spec),
                    ..bare
                }
            ),
            Ok(RecordVerdict::MissingEvidence(JobRecordKind::Budget))
        );
    }

    #[test]
    fn verdicts_ignore_order() {
        // Every arrival order of the same valid records ends in the same verdict:
        // absent evidence is pending, complete evidence is authentic.
        let view = LocalView::new();
        let spec = sample_spec();
        let budget = sample_budget();
        let launch = sample_launch();
        let receipt = sample_receipt();
        let signed = [
            envelope(JobFamilyRecord::Claim(sample_claim()), 1),
            envelope(JobFamilyRecord::Launch(Box::new(launch.clone())), 1),
            envelope(JobFamilyRecord::Receipt(Box::new(receipt.clone())), 9),
            envelope(JobFamilyRecord::Update(Box::new(sample_update())), 9),
            envelope(JobFamilyRecord::Output(Box::new(output_record())), 9),
        ];
        for mask in 0..16u8 {
            let context = JobRecordContext {
                spec: (mask & 1 != 0).then_some(&spec),
                budget: (mask & 2 != 0).then_some(&budget),
                launch: (mask & 4 != 0).then_some(&launch),
                receipt: (mask & 8 != 0).then_some(&receipt),
                ..view.context()
            };
            for record in &signed {
                let verdict = record.verify(&context).expect("valid records never error");
                let complete = match record.kind() {
                    JobRecordKind::Claim => mask & 1 != 0,
                    JobRecordKind::Launch => mask & 3 == 3,
                    JobRecordKind::Receipt => mask & 4 != 0,
                    JobRecordKind::Update | JobRecordKind::Output => mask & 8 != 0,
                    kind => unreachable!("{kind:?} is not part of this set"),
                };
                assert_eq!(
                    verdict == RecordVerdict::Authentic,
                    complete,
                    "{:?} at mask {mask}",
                    record.kind()
                );
            }
        }
    }

    #[test]
    fn rejects_wrong_target() {
        // Only the node the scheduler named as target may receipt that launch.
        let view = LocalView::new();
        let spec = sample_spec();
        let launch = sample_launch();
        let checked = JobRecordContext {
            spec: Some(&spec),
            launch: Some(&launch),
            ..view.context()
        };
        let mut elsewhere = sample_receipt();
        elsewhere.executor_node_id = node_id(2);
        elsewhere.target = ExecutionTargetId {
            node_id: node_id(2),
            executor_kind: "docker".to_string(),
        };
        assert_eq!(
            envelope(JobFamilyRecord::Receipt(Box::new(elsewhere.clone())), 2).verify(&checked),
            Err(JobRecordError::EvidenceMismatch(JobRecordKind::Launch))
        );
        // A receipt naming another node as its own target contradicts itself.
        let mut mismatched = sample_receipt();
        mismatched.executor_node_id = node_id(2);
        assert_eq!(
            envelope(JobFamilyRecord::Receipt(Box::new(mismatched)), 2).verify(&checked),
            Err(JobRecordError::Inconsistent)
        );
        assert_eq!(
            envelope(JobFamilyRecord::Receipt(Box::new(elsewhere)), 9).verify(&checked),
            Err(JobRecordError::WrongPublisher(JobRecordKind::Receipt))
        );
    }

    #[test]
    fn roots_update_chain() {
        let view = LocalView::new();
        let receipt = sample_receipt();
        let checked = JobRecordContext {
            receipt: Some(&receipt),
            ..view.context()
        };
        assert_eq!(
            envelope(JobFamilyRecord::Update(Box::new(sample_update())), 9).verify(&checked),
            Ok(RecordVerdict::Authentic)
        );
        // The first update must root at the exact receipt it claims to follow.
        let mut unrooted = sample_update();
        unrooted.previous_digest = [5u8; 32];
        assert_eq!(
            envelope(JobFamilyRecord::Update(Box::new(unrooted)), 9).verify(&checked),
            Err(JobRecordError::EvidenceMismatch(JobRecordKind::Receipt))
        );
        let mut foreign = sample_update();
        foreign.executor_node_id = node_id(2);
        assert_eq!(
            envelope(JobFamilyRecord::Update(Box::new(foreign)), 2).verify(&checked),
            Err(JobRecordError::EvidenceMismatch(JobRecordKind::Receipt))
        );
    }

    #[test]
    fn accepts_local_output() {
        // A node's own fenced attempt validates its own output record while no
        // launch chain exists; the same record from anyone else stays pending.
        let view = LocalView::new();
        let local = LocalExecution {
            node_id: node_id(9),
            execution_id: output_record().execution_id,
            fence_digest: output_record().receipt_digest,
            spec_digest: output_record().spec_digest,
        };
        let signed = envelope(JobFamilyRecord::Output(Box::new(output_record())), 9);
        assert_eq!(
            signed.verify(&JobRecordContext {
                local: Some(&local),
                ..view.context()
            }),
            Ok(RecordVerdict::LocalEvidence)
        );
        let other = LocalExecution {
            execution_id: Ulid::from_bytes([21u8; 16]),
            ..local
        };
        assert_eq!(
            signed.verify(&JobRecordContext {
                local: Some(&other),
                ..view.context()
            }),
            Ok(RecordVerdict::MissingEvidence(JobRecordKind::Receipt))
        );
        // A published receipt outranks the local stand-in.
        let receipt = sample_receipt();
        assert_eq!(
            signed.verify(&JobRecordContext {
                local: Some(&local),
                receipt: Some(&receipt),
                ..view.context()
            }),
            Ok(RecordVerdict::Authentic)
        );
    }

    #[test]
    fn rejects_forged_cancel() {
        // Selecting GroupAdmin or naming a submitter cannot create authority.
        let view = LocalView::new();
        let spec = sample_spec();
        let checked = JobRecordContext {
            spec: Some(&spec),
            ..view.context()
        };
        let mut forged = sample_cancel();
        forged.authority = CancelAuthority::GroupAdmin;
        forged.requested_by = user(8, 9);
        assert_eq!(
            envelope(JobFamilyRecord::Cancel(forged), 2).verify(&checked),
            Err(JobRecordError::NotHolder(JobRecordKind::Cancel))
        );
        let mut anonymous = sample_cancel();
        anonymous.requested_by = UserId::new(Ulid::nil(), RealmId([8u8; 32]));
        anonymous.authority = CancelAuthority::GroupAdmin;
        assert_eq!(
            envelope(JobFamilyRecord::Cancel(anonymous), 1).verify(&checked),
            Err(JobRecordError::Unauthorized)
        );
    }

    #[test]
    fn rejects_family_replay() {
        // One authentic record replayed into another realm or family is refused
        // even when every local view would otherwise grant its publisher.
        let view = LocalView::new();
        let spec = sample_spec();
        let signed = envelope(JobFamilyRecord::Claim(sample_claim()), 1);
        let elsewhere = JobRecordContext {
            spec: Some(&spec),
            family: JobFamilyId {
                submission_id: SubmissionId([7u8; 32]),
                request_digest: [1u8; 32],
            },
            ..view.context()
        };
        assert_eq!(
            signed.verify(&elsewhere),
            Err(JobRecordError::FamilyMismatch)
        );
        let other_realm = JobRecordContext {
            spec: Some(&spec),
            realm_id: RealmId([99u8; 32]),
            ..view.context()
        };
        assert_eq!(
            signed.verify(&other_realm),
            Err(JobRecordError::RealmMismatch)
        );
    }

    #[test]
    fn outputs_sort_canonically() {
        let first = sample_output();
        let mut second = sample_output();
        second.key = "out/a.txt".to_string();
        let sorted = OutputSet::canonical(vec![second.clone(), first.clone()]).unwrap();
        assert_eq!(
            sorted,
            OutputSet::canonical(vec![first.clone(), second.clone()]).unwrap()
        );
        assert_eq!(sorted.as_slice()[0], second);

        // A peer may not reorder or duplicate what its publisher signed.
        let unsorted = postcard::to_allocvec(&vec![first.clone(), second.clone()]).unwrap();
        assert!(postcard::from_bytes::<OutputSet>(&unsorted).is_err());
        assert_eq!(
            OutputSet::canonical(vec![first.clone(), first.clone()]),
            Err(JobRecordError::OutputOrder)
        );
        let mut anonymous = sample_output();
        anonymous.version_id = Ulid::nil();
        assert_eq!(
            OutputSet::canonical(vec![anonymous]),
            Err(JobRecordError::OutputOrder)
        );
        let many = (0..=MAX_EXECUTION_OUTPUTS)
            .map(|index| OutputObject {
                key: format!("out/{index:08}"),
                ..sample_output()
            })
            .collect();
        assert_eq!(OutputSet::canonical(many), Err(JobRecordError::OutputOrder));
    }

    fn execution_result(outputs: Vec<OutputObject>) -> JobResultPayload {
        JobResultPayload::Execution {
            exit_code: Some(0),
            workspace_bucket: Some("ws".to_string()),
            outputs,
            stdout: String::new(),
            stderr: String::new(),
            output_digest: None,
        }
    }

    #[test]
    fn success_needs_identity() {
        let execution_id = sample_output().execution_id;
        assert!(
            execution_result(vec![sample_output()])
                .check_outputs(execution_id)
                .is_ok()
        );

        let mut anonymous = sample_output();
        anonymous.version_id = Ulid::nil();
        assert_eq!(
            execution_result(vec![anonymous]).check_outputs(execution_id),
            Err(JobRecordError::OutputIdentity)
        );

        // An output produced by another execution may not ride this result.
        let mut foreign = sample_output();
        foreign.execution_id = Ulid::from_bytes([14u8; 16]);
        assert_eq!(
            execution_result(vec![foreign]).check_outputs(execution_id),
            Err(JobRecordError::OutputIdentity)
        );
        assert_eq!(
            execution_result(vec![sample_output()]).check_outputs(Ulid::nil()),
            Err(JobRecordError::OutputIdentity)
        );
        assert_eq!(
            execution_result(vec![sample_output(), sample_output()]).check_outputs(execution_id),
            Err(JobRecordError::OutputIdentity)
        );
    }

    fn sample_control() -> AttemptControl {
        AttemptControl {
            attempt_epoch: 3,
            execution_id: Ulid::from_bytes([13u8; 16]),
            controller_generation: 1,
            bound_token: None,
            tombstone_ref: None,
            output_commits: Vec::new(),
            output_record: None,
        }
    }

    #[test]
    fn success_needs_record() {
        // Success must observe a durable output record and a reserved version
        // for every object, so an unproven result can never commit.
        let mut control = sample_control();
        let output = sample_output();
        let mut result = execution_result(vec![output.clone()]);

        assert_eq!(
            result.proves_outputs(&control),
            Err(JobRecordError::OutputIdentity)
        );

        control.output_commits.push(OutputCommitIntent {
            node_id: output.node_id,
            bucket: output.bucket.clone(),
            key: output.key.clone(),
            version_id: output.version_id,
        });
        assert_eq!(
            result.proves_outputs(&control),
            Err(JobRecordError::MissingEvidence(JobRecordKind::Output))
        );

        let digest = [7u8; 32];
        control.output_record = Some(digest);
        assert_eq!(
            result.proves_outputs(&control),
            Err(JobRecordError::MissingEvidence(JobRecordKind::Output))
        );

        if let JobResultPayload::Execution { output_digest, .. } = &mut result {
            *output_digest = Some(digest);
        }
        assert_eq!(result.proves_outputs(&control), Ok(()));
    }

    #[test]
    fn success_rejects_foreign() {
        // A version another writer produced under the same key is not this
        // execution's output, even when the key was reserved.
        let mut control = sample_control();
        let output = sample_output();
        control.output_commits.push(OutputCommitIntent {
            node_id: output.node_id,
            bucket: output.bucket.clone(),
            key: output.key.clone(),
            version_id: Ulid::from_bytes([9u8; 16]),
        });
        control.output_record = Some([7u8; 32]);
        let mut result = execution_result(vec![output]);
        if let JobResultPayload::Execution { output_digest, .. } = &mut result {
            *output_digest = Some([7u8; 32]);
        }

        assert_eq!(
            result.proves_outputs(&control),
            Err(JobRecordError::OutputIdentity)
        );
    }

    #[test]
    fn empty_success_needs_record() {
        // Even an empty output set is a claim: it needs its durable record.
        let mut control = sample_control();
        let mut result = execution_result(Vec::new());
        assert_eq!(
            result.proves_outputs(&control),
            Err(JobRecordError::MissingEvidence(JobRecordKind::Output))
        );
        control.output_record = Some([3u8; 32]);
        if let JobResultPayload::Execution { output_digest, .. } = &mut result {
            *output_digest = Some([3u8; 32]);
        }
        assert_eq!(result.proves_outputs(&control), Ok(()));
    }

    #[test]
    fn reservation_is_stable() {
        // A replayed capture must reuse the reserved VersionId, never mint a second.
        let mut control = sample_control();
        let output_node = node_id(1);
        let destinations = vec![
            (output_node, "dest".to_string(), "out/a.txt".to_string()),
            (output_node, "dest".to_string(), "out/b.txt".to_string()),
        ];
        assert!(control.reserve_outputs(&destinations, Ulid::generate));
        let reserved = control.output_commits.clone();
        assert_eq!(reserved.len(), 2);
        assert!(!control.reserve_outputs(&destinations, Ulid::generate));
        assert_eq!(control.output_commits, reserved);

        let grown = vec![(output_node, "dest".to_string(), "out/c.txt".to_string())];
        assert!(control.reserve_outputs(&grown, Ulid::generate));
        assert_eq!(control.output_commits[..2], reserved[..]);
        let remote = vec![(node_id(2), "dest".to_string(), "out/a.txt".to_string())];
        assert!(control.reserve_outputs(&remote, Ulid::generate));
    }

    #[test]
    fn binds_output_receipt() {
        let view = LocalView::new();
        let receipt = sample_receipt();
        let checked = JobRecordContext {
            receipt: Some(&receipt),
            ..view.context()
        };
        assert_eq!(
            envelope(JobFamilyRecord::Output(Box::new(output_record())), 9).verify(&checked),
            Ok(RecordVerdict::Authentic)
        );
        let mut foreign = output_record();
        foreign.outputs = OutputSet::canonical(vec![OutputObject {
            execution_id: Ulid::from_bytes([1u8; 16]),
            ..sample_output()
        }])
        .unwrap();
        assert_eq!(
            envelope(JobFamilyRecord::Output(Box::new(foreign)), 9).verify(&checked),
            Err(JobRecordError::Inconsistent)
        );
    }

    #[test]
    fn chain_stops_gap() {
        let root = sample_receipt().digest().unwrap();
        let outputs = output_record().digest().unwrap();
        let mut gapped = success_update();
        gapped.sequence = 2;
        let records = [gapped, sample_update()];
        let chain = verify_update_chain(root, Some(outputs), &records).unwrap();
        assert_eq!(chain, vec![&sample_update()]);

        let mut unrooted = sample_update();
        unrooted.previous_digest = [0u8; 32];
        assert!(
            verify_update_chain(root, Some(outputs), &[unrooted])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn chain_stops_terminal() {
        let root = sample_receipt().digest().unwrap();
        let outputs = output_record().digest().unwrap();
        let mut trailing = sample_update();
        trailing.sequence = 2;
        trailing.previous_digest = success_update().digest().unwrap();
        let records = [sample_update(), success_update(), trailing];
        let chain = verify_update_chain(root, Some(outputs), &records).unwrap();
        assert_eq!(chain, vec![&sample_update(), &success_update()]);
    }

    #[test]
    fn chain_requires_outputs() {
        // A success is projected only once its exact output record is durable.
        let root = sample_receipt().digest().unwrap();
        let records = [sample_update(), success_update()];
        assert_eq!(
            verify_update_chain(root, None, &records).unwrap(),
            vec![&sample_update()]
        );
        assert_eq!(
            verify_update_chain(root, Some([3u8; 32]), &records).unwrap(),
            vec![&sample_update()]
        );
        let outputs = output_record().digest().unwrap();
        assert_eq!(
            verify_update_chain(root, Some(outputs), &records)
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn chain_rejects_conflict() {
        let root = sample_receipt().digest().unwrap();
        let mut rival = sample_update();
        rival.observed_at_ms += 1;
        assert_eq!(
            verify_update_chain(root, None, &[sample_update(), rival]),
            Err(JobRecordError::ChainConflict { sequence: 0 })
        );
        // An exact replay of one record is a no-op, not a conflict.
        assert_eq!(
            verify_update_chain(root, None, &[sample_update(), sample_update()])
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn key_bytes_ordered() {
        let mut keys = vec![
            JobFamilyRecord::Cancel(sample_cancel()).key(),
            JobFamilyRecord::Spec(Box::new(sample_spec())).key(),
            JobFamilyRecord::Update(Box::new(success_update())).key(),
            JobFamilyRecord::Update(Box::new(sample_update())).key(),
            JobFamilyRecord::Receipt(Box::new(sample_receipt())).key(),
        ];
        keys.sort();
        let mut encoded: Vec<[u8; JOB_RECORD_KEY_BYTES]> =
            keys.iter().map(JobRecordKey::to_bytes).collect();
        encoded.sort();
        assert_eq!(
            encoded,
            keys.iter()
                .map(JobRecordKey::to_bytes)
                .collect::<Vec<[u8; JOB_RECORD_KEY_BYTES]>>()
        );
        for key in keys {
            assert_eq!(JobRecordKey::from_bytes(&key.to_bytes()), Ok(key));
        }
        assert_eq!(
            JobRecordKey::from_bytes(&[0u8; JOB_RECORD_KEY_BYTES - 1]),
            Err(JobRecordError::MalformedKey)
        );
    }

    #[test]
    fn bounds_result_message() {
        assert!(ResultMessage::new("m".repeat(MAX_RESULT_MESSAGE_BYTES)).is_ok());
        assert_eq!(
            ResultMessage::new("m".repeat(MAX_RESULT_MESSAGE_BYTES + 1)),
            Err(JobRecordError::MessageBytes)
        );
        let over = postcard::to_allocvec(&"m".repeat(MAX_RESULT_MESSAGE_BYTES + 1)).unwrap();
        assert!(postcard::from_bytes::<ResultMessage>(&over).is_err());
    }

    #[test]
    fn keeps_message_tail() {
        // An overlong stream keeps its end, and a cut inside a character moves
        // forward to the next boundary rather than producing invalid UTF-8.
        assert_eq!(ResultMessage::tail(""), None);
        assert_eq!(
            ResultMessage::tail("short").map(|tail| tail.as_str().to_string()),
            Some("short".to_string())
        );
        let text = format!("{}end", "m".repeat(MAX_RESULT_MESSAGE_BYTES));
        let tail = ResultMessage::tail(&text).expect("non-empty tail");
        assert_eq!(tail.as_str().len(), MAX_RESULT_MESSAGE_BYTES);
        assert!(tail.as_str().ends_with("end"));
        // 6000 bytes of three-byte characters: the cut at 1904 is inside one.
        let wide = "€".repeat(2000);
        let tail = ResultMessage::tail(&wide).expect("non-empty tail");
        assert_eq!(tail.as_str().len(), MAX_RESULT_MESSAGE_BYTES - 1);
        assert!(wide.ends_with(tail.as_str()));
    }

    #[test]
    fn digest_tracks_projection() {
        let projection = sample_projection();
        assert_eq!(
            projection.digest().unwrap(),
            sample_projection().digest().unwrap()
        );
        let mut succeeded = projection.clone();
        succeeded.state = LogicalJobState::Cancelled;
        assert_ne!(succeeded.digest().unwrap(), projection.digest().unwrap());
    }

    #[test]
    fn record_key_versioned() {
        let key = job_record_key(JobId::from_bytes([9u8; 16]));
        assert!(key.starts_with(JOB_RECORD_KEY_PREFIX));
        assert_eq!(key.len(), JOB_RECORD_KEY_PREFIX.len() + 16);
    }

    #[test]
    fn due_index_ordered() {
        let id = JobId::from_bytes([1u8; 16]);
        assert!(job_due_index_key(1_000, id) < job_due_index_key(2_000, id));
        let (ts, parsed) = parse_job_schedule_index_key(&job_due_index_key(1_234, id)).unwrap();
        assert_eq!(ts, 1_234);
        assert_eq!(parsed, id);
    }

    #[test]
    fn prefixes_disjoint() {
        let id = JobId::from_bytes([1u8; 16]);
        let due = job_due_index_key(5, id);
        let lease = job_lease_index_key(5, id);
        let prune = job_prune_index_key(5, id);
        assert!(due < lease);
        assert!(lease < prune);
        assert!(!due.starts_with(JOB_LEASE_INDEX_PREFIX));
        assert!(!lease.starts_with(JOB_PRUNE_INDEX_PREFIX));
    }

    #[test]
    fn owner_index_newest_first() {
        let u = user(1, 2);
        let id = JobId::from_bytes([3u8; 16]);
        assert!(job_owner_index_key(u, 2_000, id) < job_owner_index_key(u, 1_000, id));
        assert!(job_owner_index_key(u, 1_000, id).starts_with(job_owner_index_prefix(u).as_ref()));
    }

    #[test]
    fn owner_key_roundtrips() {
        let u = user(5, 9);
        let ts = 1_700_000_000_000u64;
        let id = JobId::from_bytes([8u8; 16]);
        let key = job_owner_index_key(u, ts, id);
        assert_eq!(parse_job_owner_index_key(&key).unwrap(), (u, ts, id));
        assert_eq!(job_owner_cursor(ts, id).as_slice(), &key[48..72]);
    }

    #[test]
    fn owner_index_scoped() {
        let a = user(1, 2);
        let b = user(1, 3);
        let id = JobId::from_bytes([4u8; 16]);
        let ka = job_owner_index_key(a, 1_000, id);
        assert!(!ka.starts_with(job_owner_index_prefix(b).as_ref()));
    }
}
