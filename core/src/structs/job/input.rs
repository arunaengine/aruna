use super::*;

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

/// What one line of a session job's report says.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionReportDetail {
    /// One object staged into the workspace bucket while the session ran.
    Input {
        dest_key: String,
        bytes: u64,
        blake3: String,
        source_node_id: String,
        version_id: String,
    },
    /// One object the session's own credential read or wrote.
    Touched {
        bucket: String,
        key: String,
        operation: String,
    },
    /// Why the session stopped: `ended`, `idle`, `walltime`, `cancelled` or
    /// `kernel_exit`.
    End { reason: String },
}

/// One line of a session job's report. It records what the session brought in
/// and why it stopped; cell traffic is never recorded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionReportRow {
    pub entry_key: String,
    pub detail: SessionReportDetail,
}

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
