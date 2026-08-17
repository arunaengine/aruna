use std::collections::BTreeMap;
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
    PlacementRef, RealmId, StagingStrategy,
};
use crate::structured_id::{
    BucketId, FieldError, JobId as RoutableJobId, PlacementHandle, StructuredId,
};
use crate::types::{GroupId, Key, UserId};

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
        _ => Ok(()),
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
    /// Destination key inside the workspace bucket (16.4 non-overlapping).
    pub dest_key: String,
    pub mode: InputMode,
    /// Absolute path exposed to a TES executor; native workspace inputs omit it.
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
    pub fn resolve_outputs(&mut self, bucket: &str) {
        for output in std::mem::take(&mut self.workspace_outputs) {
            self.file_outputs.push(OutputSelection {
                container_path: output.container_path,
                path_prefix: None,
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
        self.is_rocrate() || matches!(self, JobPayload::MintPersistentId(_))
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
    pub controller_generation: u64,
    pub bound_token: Option<Ulid>,
    pub tombstone_ref: Option<String>,
}

impl AttemptControl {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
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
            } => serde_json::json!({
                "exit_code": exit_code,
                "workspace_bucket": workspace_bucket,
                "stdout": stdout,
                "stderr": stderr,
                "outputs": outputs
                    .iter()
                    .map(|output| serde_json::json!({
                        "bucket": output.bucket,
                        "key": output.key,
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkspaceMode {
    Temporary,
    #[default]
    Kept,
    Existing,
    None,
}

impl WorkspaceMode {
    pub fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Temporary => "temporary",
            Self::Kept => "kept",
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
    /// Durable workspace/run bucket name (`ws-{jobid}`) for execution jobs.
    pub workspace_bucket: Option<String>,
    #[serde(default)]
    pub workspace_mode: WorkspaceMode,
    pub report_digest: Option<[u8; 32]>,
    pub retention_ms: u64,
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
            report_digest: None,
            retention_ms: DEFAULT_JOB_RETENTION_MS,
        }
    }

    /// The run bucket name for an execution job, deterministic from the id.
    pub fn workspace_bucket_name(job_id: JobId) -> String {
        format!("ws-{}", job_id.to_string().to_lowercase())
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        match postcard::from_bytes(bytes) {
            Ok(record) => Ok(record),
            Err(postcard::Error::DeserializeUnexpectedEnd) => {
                let mut previous = bytes.to_vec();
                previous.extend(postcard::to_allocvec(&Option::<[u8; 32]>::None)?);
                previous.extend(postcard::to_allocvec(&DEFAULT_JOB_RETENTION_MS)?);
                if let Ok(record) = postcard::from_bytes(&previous) {
                    return Ok(record);
                }
                let mut legacy = bytes.to_vec();
                legacy.extend(postcard::to_allocvec(&WorkspaceMode::default())?);
                legacy.extend(postcard::to_allocvec(&Option::<[u8; 32]>::None)?);
                legacy.extend(postcard::to_allocvec(&DEFAULT_JOB_RETENTION_MS)?);
                Ok(postcard::from_bytes(&legacy)?)
            }
            Err(error) => Err(error.into()),
        }
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

pub fn job_active_key(created_by: UserId, job_id: JobId) -> Key {
    let mut bytes = created_by.to_storage_key();
    bytes.extend_from_slice(&job_id.to_bytes());
    ByteView::from(bytes)
}

pub fn job_active_prefix(created_by: UserId) -> Key {
    ByteView::from(created_by.to_storage_key())
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum JobContractError {
    #[error("retry policy must allow at least one launch")]
    EmptyRetry,
    #[error("launch belongs to another witness budget")]
    BudgetMismatch,
    #[error("launch sequence {sequence} is outside the sealed budget of {max_launches}")]
    BudgetExhausted { sequence: u32, max_launches: u32 },
    #[error("launch spec digest does not match the sealed source spec digest")]
    SpecMismatch,
}

/// Per-witness launch bound sealed into the immutable spec.
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
/// normalized caller plan only; `spec_digest` covers this whole record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalJobSpec {
    pub submission_id: SubmissionId,
    pub job_id: JobId,
    pub origin_node_id: NodeId,
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub created_by: UserId,
    pub created_at_ms: u64,
    pub payload: ExecutionSpec,
    pub request_digest: [u8; 32],
    pub spec_digest: [u8; 32],
    pub resources: EffectiveResources,
    pub retry: JobRetryPolicy,
    pub admission: JobAdmissionRecord,
    /// Family placement derived from `submission_id`, never from the alias bucket.
    pub placement: PlacementRef,
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

/// Lifetime launch bound one scheduler seals before it first plans a request.
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
    /// A launch is actionable only inside the budget its own scheduler sealed.
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
    pub plan_digest: [u8; 32],
    pub spec_digest: [u8; 32],
    pub created_at_ms: u64,
}

/// The target's signed acceptance. It binds one exact launch and the subject it
/// was accepted under, so a later placement change cannot rewrite that history
/// or authorize unrelated new work.
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
    Failed,
    Cancelled,
}

impl PhysicalExecutionState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            PhysicalExecutionState::Succeeded
                | PhysicalExecutionState::Failed
                | PhysicalExecutionState::Cancelled
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
        }
    }
}

/// Terminal facts of one physical execution. A success is valid only when every
/// output carries its exact VersionId.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalExecutionResult {
    pub exit_code: Option<i32>,
    pub outputs: Vec<OutputObject>,
    pub message: Option<String>,
}

/// Monotonic state publication by the executor. `previous_digest` roots the
/// chain at the receipt so a gap cannot silently skip a state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionUpdate {
    pub execution_id: Ulid,
    pub sequence: u64,
    pub previous_digest: Option<[u8; 32]>,
    pub state: PhysicalExecutionState,
    pub observed_at_ms: u64,
    pub result: Option<PhysicalExecutionResult>,
}

/// Replicated cancellation intent for one `(submission_id, request_digest)`
/// family. It suppresses new launches; a partitioned executor may still finish.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCancelRecord {
    pub cancel_id: Ulid,
    pub submission_id: SubmissionId,
    pub request_digest: [u8; 32],
    pub job_id: JobId,
    pub requested_by: UserId,
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

/// Replicated logical state of one request family. There is deliberately no
/// `Failed`: realm-wide failure may never be inferred from local exhaustion or
/// silence, so an unsuccessful family stays `Indeterminate`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalJobState {
    Queued,
    Running,
    Indeterminate,
    Succeeded,
    Cancelled,
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
        }
    }
}

/// How one physical execution relates to the canonical success. Redundant
/// executions stay visible instead of being erased.
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
    pub canonical_execution_id: Option<Ulid>,
    pub executions: Vec<ProjectedExecution>,
    /// Outputs of the canonical execution.
    pub outputs: Vec<OutputObject>,
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

    fn node_id(seed: u8) -> NodeId {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        iroh::SecretKey::from_bytes(&seed_bytes).public()
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

    // External-only states must be rejected for an in-process job.
    #[test]
    fn internal_rejects_external() {
        let external_only = [
            (JobState::Claimed, JobState::Preparing),
            (JobState::Preparing, JobState::Ready),
            (JobState::Ready, JobState::Running),
            (JobState::Running, JobState::Cancelling),
            (JobState::Running, JobState::Indeterminate),
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

    // The fenced execution graph is accepted for external attempts.
    #[test]
    fn external_graph_legal() {
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

        spec.resolve_outputs("ws-job");

        assert!(spec.workspace_outputs.is_empty());
        assert_eq!(
            spec.file_outputs,
            vec![OutputSelection {
                container_path: "/out/report.txt".to_string(),
                path_prefix: None,
                destination: OutputDestination::S3 {
                    bucket: "ws-job".to_string(),
                    key: "outputs/report.txt".to_string(),
                },
                name: None,
                description: None,
            }]
        );

        spec.resolve_outputs("ws-job");
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
    fn legacy_record_defaults() {
        let record = probe_record(JobId::from_bytes([6u8; 16]), 1_700_000_000_000);
        let mut bytes = record.to_bytes().unwrap();
        let retention = postcard::to_allocvec(&record.retention_ms).unwrap();
        bytes.truncate(bytes.len() - retention.len());
        let digest = postcard::to_allocvec(&Option::<[u8; 32]>::None).unwrap();
        bytes.truncate(bytes.len() - digest.len());
        let mode = postcard::to_allocvec(&WorkspaceMode::Kept).unwrap();
        bytes.truncate(bytes.len() - mode.len());

        let decoded = JobRecord::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.workspace_mode, WorkspaceMode::Kept);
        assert_eq!(decoded.workspace_bucket, record.workspace_bucket);
        assert_eq!(decoded.report_digest, None);
        assert_eq!(decoded.retention_ms, DEFAULT_JOB_RETENTION_MS);
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

    fn sample_output() -> OutputObject {
        OutputObject {
            bucket: "dest".to_string(),
            key: "out/report.txt".to_string(),
            version_id: Ulid::from_bytes([4u8; 16]),
            execution_id: Ulid::from_bytes([5u8; 16]),
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
            realm_id: RealmId([8u8; 32]),
            group_id: Ulid::from_bytes([7u8; 16]),
            created_by: user(8, 2),
            created_at_ms: 1_700_000_000_000,
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
            spec_digest: [2u8; 32],
            resources: sample_resources(),
            retry: JobRetryPolicy {
                max_launches_per_witness: 3,
            },
            admission: sample_admission(),
            placement: PlacementRef {
                strategy_id: Ulid::from_bytes([9u8; 16]),
                shard: 5,
            },
        }
    }

    fn sample_claim() -> SubmissionClaim {
        SubmissionClaim {
            submission_id: submission(),
            job_id: JobId::from_bytes([6u8; 16]),
            request_digest: [1u8; 32],
            spec_digest: [2u8; 32],
            committing_node_id: node_id(1),
            accepted_at_ms: 1_700_000_000_000,
        }
    }

    fn sample_budget() -> WitnessBudgetRecord {
        WitnessBudgetRecord {
            submission_id: submission(),
            request_digest: [1u8; 32],
            scheduler_node_id: node_id(1),
            source_spec_digest: [2u8; 32],
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
            witness_placement: PlacementRef {
                strategy_id: Ulid::from_bytes([9u8; 16]),
                shard: 5,
            },
            holder_generation: 11,
            target: target(),
            plan_digest: [12u8; 32],
            spec_digest: [2u8; 32],
            created_at_ms: 1_700_000_000_000,
        }
    }

    fn sample_receipt() -> ExecutionReceipt {
        ExecutionReceipt {
            execution_id: Ulid::from_bytes([13u8; 16]),
            launch_id: Ulid::from_bytes([10u8; 16]),
            launch_digest: [14u8; 32],
            submission_id: submission(),
            request_digest: [1u8; 32],
            job_id: JobId::from_bytes([6u8; 16]),
            executor_node_id: node_id(9),
            target: target(),
            spec_digest: [2u8; 32],
            membership_generation: 4,
            subject_generation: 2,
            subject_digest: [15u8; 32],
            accepted_at_ms: 1_700_000_000_000,
        }
    }

    fn sample_update() -> ExecutionUpdate {
        ExecutionUpdate {
            execution_id: Ulid::from_bytes([13u8; 16]),
            sequence: 3,
            previous_digest: Some([16u8; 32]),
            state: PhysicalExecutionState::Succeeded,
            observed_at_ms: 1_700_000_001_000,
            result: Some(PhysicalExecutionResult {
                exit_code: Some(0),
                outputs: vec![sample_output()],
                message: None,
            }),
        }
    }

    fn sample_cancel() -> JobCancelRecord {
        JobCancelRecord {
            cancel_id: Ulid::from_bytes([17u8; 16]),
            submission_id: submission(),
            request_digest: [1u8; 32],
            job_id: JobId::from_bytes([6u8; 16]),
            requested_by: user(8, 2),
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
            }],
            outputs: vec![sample_output()],
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
        assert_eq!(reencode(&sample_cancel()), sample_cancel());
        assert_eq!(reencode(&sample_projection()), sample_projection());
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
            wire_digest(&sample_cancel()),
            wire_digest(&sample_projection()),
        ];
        assert_eq!(
            digests,
            [
                "b98e0af7c920302f38cd6f87c1617543d64e1f0b54ab7d8d895dc3137147c889",
                "e8187e6ea96c933f2bb870f1b94860dedfd7f8397dcf0ec3485f5ca8190b064d",
                "1c4dc854b3931565ff75fafec7a24673cc03c1989516f9b46dc93a093ec2bfeb",
                "045a9878c7942dee314a991bec25d526785ac4d1d0bf0a4d2953735562115a33",
                "0ac17924f2f58a054126d6fb22c6a0de5e2493ab93eb5a072631f4e63003accc",
                "f5d1d8d4278b2fac2505ff0f4b72842b081aad77208abed4a8ef9e9f8fb8d2f3",
                "70bcabaf26ea387f46f09219e24713ed2f30a41ddc49a28505c0c61128278e19",
                "7169d9822b6b5f5d10c475c5ddbbbfee854120e48cb51a58fcf087978d42a788",
                "1ce897cfd6c39414bd7266e6f09b21ff823cd5468a2b0e15b40387a31e00e1e6",
                "eb62502ff6af0d549869e048e24f2a6239a256db3a9e5012eff7cc4b82fd7d76",
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
    fn admits_sealed_launch() {
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
