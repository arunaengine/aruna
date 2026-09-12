use super::*;

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
pub fn encode_dedup_value(job_id: JobId, plan_digest: [u8; 32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(48);
    bytes.extend_from_slice(&job_id.to_bytes());
    bytes.extend_from_slice(&plan_digest);
    bytes
}

pub fn parse_dedup_value(bytes: &[u8]) -> Result<(JobId, [u8; 32]), ConversionError> {
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

/// Fenced execution permits requeue only before attempt submission. `Indeterminate` requires evidence;
/// permanent pre-attempt failures terminalize from Preparing or Ready.
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
            // Pre-attempt cancellation is safe only while no intent exists, so no container can exist.
            // It must terminalize there to prevent TES from remaining in CANCELING.
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
pub fn run_crate_key(job_id: JobId) -> Key {
    ByteView::from(job_id.to_bytes().to_vec())
}

/// Dedup key of the follow-on `WriteRunCrate` obligation for `job_id`. Internal
/// obligation keys live in the `internal/` subspace, disjoint from user keys.
pub fn crate_dedup_key(job_id: JobId) -> Vec<u8> {
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

/// The job a workspace credential's access key belongs to. `None` for every
/// other credential, so an ordinary request is never attributed to a job.
pub fn credential_job_id(access_key: &str) -> Option<JobId> {
    access_key
        .strip_prefix("ws")
        .and_then(|id| JobId::from_str(id).ok())
}

/// Marks dedup keys scoped by their subject instead of user. Global keys join equivalent requests;
/// `user_dedup_key` namespaces callers so they cannot enter this subspace.
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

pub fn due_index_key(due_at_ms: u64, job_id: JobId) -> Key {
    schedule_index_key(JOB_DUE_INDEX_PREFIX, due_at_ms, job_id)
}

pub fn lease_index_key(lease_expires_at_ms: u64, job_id: JobId) -> Key {
    schedule_index_key(JOB_LEASE_INDEX_PREFIX, lease_expires_at_ms, job_id)
}

pub fn job_prune_key(retention_expiry_ms: u64, job_id: JobId) -> Key {
    schedule_index_key(JOB_PRUNE_INDEX_PREFIX, retention_expiry_ms, job_id)
}

/// Extract `(timestamp_ms, job_id)` from a `due/`, `lease/`, or `prune/` schedule
/// index key.
pub fn parse_schedule_key(key: &[u8]) -> Result<(u64, JobId), ConversionError> {
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

pub fn owner_index_key(created_by: UserId, created_at_ms: u64, job_id: JobId) -> Key {
    let mut bytes = Vec::with_capacity(72);
    bytes.extend_from_slice(&created_by.to_storage_key());
    bytes.extend_from_slice(&invert_timestamp_ms(created_at_ms).to_be_bytes());
    bytes.extend_from_slice(&job_id.to_bytes());
    ByteView::from(bytes)
}

pub fn owner_index_prefix(created_by: UserId) -> Key {
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

pub fn parse_owner_key(key: &[u8]) -> Result<(UserId, u64, JobId), ConversionError> {
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
