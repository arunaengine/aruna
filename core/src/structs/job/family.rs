use super::*;

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
    pub physical_job_id: JobId,
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

/// Last `max_bytes` of `text`, cut on a char boundary so the result stays
/// valid UTF-8. Used where the end of a message carries the evidence.
pub fn tail_str(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut start = text.len() - max_bytes;
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    &text[start..]
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

/// Verification uses current realm membership and unconflicted family holders; a realm key proves
/// identity only. Missing or conflicted placement defers instead of rejecting with an empty view.
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

/// Evidence for the node's fenced execution before a replicated launch chain exists.
/// It authorizes only that node's output, with fields matching the later distributed record.
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
    /// When the work itself started: the first `Running` update, else the
    /// moment the target accepted the launch.
    pub started_at_ms: Option<u64>,
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
