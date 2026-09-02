//! Request normalization and the identities derived from it.
//!
//! The same bytes must produce the same identity on every node, so nothing
//! assigned locally enters a digest: no job id, no origin, no timestamp, no
//! resolved server default and no current topology.

use aruna_core::errors::ConversionError;
use aruna_core::structs::{
    EffectiveResources, ExecutionSpec, JobFamilyId, JobInputFact, LabelMatch, MAX_SELECTOR_LABELS,
    PlacementPolicyRef, SubmissionId, WorkspaceMode,
};
use aruna_core::types::NodeId;
use aruna_core::types::UserId;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

/// Domain tag of the normalized request digest.
pub const REQUEST_DIGEST_DOMAIN: &[u8] = b"aruna-job-request-v1";

/// Reserved tag namespace of the scheduling directives an execution spec seals.
/// The spec is the complete request, so these travel inside it rather than
/// beside it, in the tag namespace the engine already owns.
pub const WORKSPACE_MODE_TAG: &str = "aruna-engine.org/workspace-mode";
pub const WORKSPACE_BUCKET_TAG: &str = "aruna-engine.org/workspace-bucket";
/// `aruna-engine.org/label/<key> = <value>` is one required target label.
pub const LABEL_TAG_PREFIX: &str = "aruna-engine.org/label/";

/// Cores assumed when a request declares none.
pub const DEFAULT_CPU_CORES: u32 = 1;
/// Memory assumed when a request declares none: 1 GiB.
pub const DEFAULT_RAM_BYTES: u64 = 1024 * 1024 * 1024;
/// Walltime assumed when a request declares none, matching the executor default.
pub const DEFAULT_WALLTIME_MS: u64 = 24 * 60 * 60 * 1000;

#[derive(Debug, PartialEq, Error)]
pub enum RequestError {
    #[error("tag {0} is reserved for engine scheduling directives")]
    ReservedTag(String),
    #[error("a request declares at most {MAX_SELECTOR_LABELS} required labels")]
    LabelCount,
    #[error("required label keys and values must be non-empty")]
    InvalidLabel,
    #[error("existing workspace mode requires a bucket")]
    MissingBucket,
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

/// How a submission is keyed. A keyed request merges with its own retries; an
/// unkeyed one is a fresh family and never merges.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmissionScope {
    Keyed(String),
    Unkeyed(Ulid),
}

/// The complete external request, normalized once at ingress and carried
/// unchanged across the one forwarding hop.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionRequest {
    pub created_by: UserId,
    pub spec: ExecutionSpec,
    pub scope: SubmissionScope,
    pub retention_ms: u64,
    /// The first node that resolved node-local object names for this request.
    pub ingress_node_id: NodeId,
    pub input_facts: Vec<JobInputFact>,
    pub output_policies: Vec<PlacementPolicyRef>,
}

/// The replicated identity of one request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestIdentity {
    pub submission_id: SubmissionId,
    pub request_digest: [u8; 32],
}

impl RequestIdentity {
    pub fn family(&self) -> JobFamilyId {
        JobFamilyId {
            submission_id: self.submission_id,
            request_digest: self.request_digest,
        }
    }
}

impl SubmissionRequest {
    /// Derives the opaque submission identity and the normalized request digest.
    /// Both sides of a forwarding hop run this and compare.
    pub fn identity(&self) -> Result<RequestIdentity, ConversionError> {
        let submission_id = match &self.scope {
            SubmissionScope::Keyed(key) => SubmissionId::keyed(self.created_by, key.as_bytes()),
            SubmissionScope::Unkeyed(nonce) => SubmissionId::unkeyed(*nonce),
        };
        Ok(RequestIdentity {
            submission_id,
            request_digest: request_digest(
                self.created_by,
                &self.spec,
                self.ingress_node_id,
                &self.input_facts,
                &self.output_policies,
            )?,
        })
    }
}

fn request_digest(
    created_by: UserId,
    spec: &ExecutionSpec,
    ingress_node_id: NodeId,
    input_facts: &[JobInputFact],
    output_policies: &[PlacementPolicyRef],
) -> Result<[u8; 32], ConversionError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(REQUEST_DIGEST_DOMAIN);
    hasher.update(&created_by.to_storage_key());
    hasher.update(&postcard::to_allocvec(spec)?);
    hasher.update(ingress_node_id.as_bytes());
    hasher.update(&postcard::to_allocvec(input_facts)?);
    hasher.update(&postcard::to_allocvec(output_policies)?);
    Ok(*hasher.finalize().as_bytes())
}

/// Seals the workspace choice into the spec. A caller that already set a
/// reserved tag is refused rather than silently overridden.
pub fn seal_workspace(
    spec: &mut ExecutionSpec,
    mode: WorkspaceMode,
    bucket: Option<String>,
) -> Result<(), RequestError> {
    if spec.tags.contains_key(WORKSPACE_MODE_TAG) || spec.tags.contains_key(WORKSPACE_BUCKET_TAG) {
        return Err(RequestError::ReservedTag(WORKSPACE_MODE_TAG.to_string()));
    }
    spec.tags
        .insert(WORKSPACE_MODE_TAG.to_string(), mode.name().to_string());
    match (mode, bucket) {
        (WorkspaceMode::Existing, None) => return Err(RequestError::MissingBucket),
        (_, Some(bucket)) => {
            spec.tags.insert(WORKSPACE_BUCKET_TAG.to_string(), bucket);
        }
        (_, None) => {}
    }
    Ok(())
}

/// The workspace the sealed spec asks for. An unknown or absent mode owns no
/// bucket, which is what a run without a named bucket always meant.
pub fn workspace_of(spec: &ExecutionSpec) -> (WorkspaceMode, Option<String>) {
    let mode = match spec.tags.get(WORKSPACE_MODE_TAG).map(String::as_str) {
        Some("existing") => WorkspaceMode::Existing,
        _ => WorkspaceMode::None,
    };
    (mode, spec.tags.get(WORKSPACE_BUCKET_TAG).cloned())
}

/// Required target labels the spec seals, in canonical order.
pub fn required_labels(spec: &ExecutionSpec) -> Result<Vec<LabelMatch>, RequestError> {
    let mut labels: Vec<LabelMatch> = Vec::new();
    for (key, value) in &spec.tags {
        let Some(key) = key.strip_prefix(LABEL_TAG_PREFIX) else {
            continue;
        };
        if key.trim().is_empty() || value.trim().is_empty() {
            return Err(RequestError::InvalidLabel);
        }
        labels.push(LabelMatch {
            key: key.trim().to_string(),
            value: value.trim().to_string(),
        });
    }
    if labels.len() > MAX_SELECTOR_LABELS {
        return Err(RequestError::LabelCount);
    }
    Ok(labels)
}

/// Normalizes the requested ceilings once. Every field is filled, so comparing
/// the request against a static executor envelope is total.
pub fn effective_resources(spec: &ExecutionSpec) -> EffectiveResources {
    EffectiveResources {
        cpu_cores: spec.resources.cpu_cores.unwrap_or(DEFAULT_CPU_CORES).max(1),
        ram_bytes: spec.resources.ram_bytes.unwrap_or(DEFAULT_RAM_BYTES),
        disk_bytes: spec.resources.disk_bytes.unwrap_or_default(),
        max_walltime_ms: spec
            .resources
            .max_walltime_ms
            .unwrap_or(DEFAULT_WALLTIME_MS),
        preemptible: spec.resources.preemptible,
    }
}
