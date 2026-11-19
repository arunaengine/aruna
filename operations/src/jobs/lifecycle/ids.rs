//! Normalizes a submission request and derives the identities that follow from it.
//! The same bytes must give the same identity on every node, so nothing local enters a digest.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::compute::SessionMount;
use aruna_core::compute::runtimes::{
    MOUNT_PATH_TAG, MOUNT_PREFIX_TAG, SESSION_EXPIRY_TAG, SESSION_IDLE_TAG, SESSION_RUNTIME_TAG,
    SESSION_TAG, SESSION_TAG_NOTEBOOK,
};
use aruna_core::errors::ConversionError;
use aruna_core::id::NodeId;
use aruna_core::structs::execution::job::{
    CapturedInput, EffectiveResources, ExecutionSpec, JobFamilyId, SubmissionId, WorkspaceMode,
};
use aruna_core::structs::placement::policy::{MAX_SELECTOR_LABELS, PlacementPolicyRef};
use aruna_core::structs::placement::record::LabelMatch;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

/// Domain tag of the normalized request digest.
const REQUEST_DIGEST_DOMAIN: &[u8] = b"aruna-job-request-v1";

/// Reserved tag namespace of the scheduling directives an execution spec stores.
/// The spec is the complete request, so these travel inside it rather than
/// beside it, in the tag namespace the engine already owns.
const WORKSPACE_MODE_TAG: &str = "aruna-engine.org/workspace-mode";
const WORKSPACE_BUCKET_TAG: &str = "aruna-engine.org/workspace-bucket";
/// `aruna-engine.org/label/<key> = <value>` is one required target label.
const LABEL_TAG_PREFIX: &str = "aruna-engine.org/label/";

/// Cores assumed when a request declares none.
const DEFAULT_CPU_CORES: u32 = 1;
/// Memory assumed when a request declares none: 1 GiB.
const DEFAULT_RAM_BYTES: u64 = 1024 * 1024 * 1024;
/// Walltime assumed when a request declares none, matching the executor default.
const DEFAULT_WALLTIME_MS: u64 = 24 * 60 * 60 * 1000;

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
    pub captured_inputs: Vec<CapturedInput>,
    pub output_policies: Vec<PlacementPolicyRef>,
    /// Inputs too large to travel inline: the lists are empty and the sender keeps the pages.
    /// The identity is always computed from the full lists.
    pub input_pages: Option<aruna_core::structs::execution::job::PageManifest>,
    /// Unix ms after which no holder admits this request anew; only plugin tasks carry it.
    pub admit_by: Option<u64>,
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
        let submission_id = submission_of(self.created_by, &self.scope);
        Ok(RequestIdentity {
            submission_id,
            request_digest: request_digest(
                self.created_by,
                &self.spec,
                self.ingress_node_id,
                (&self.captured_inputs, &self.output_policies),
                self.admit_by,
            )?,
        })
    }
}

/// The submission id `created_by` asks under `scope`, known before any input resolves.
pub fn submission_of(created_by: UserId, scope: &SubmissionScope) -> SubmissionId {
    match scope {
        SubmissionScope::Keyed(key) => SubmissionId::keyed(created_by, key.as_bytes()),
        SubmissionScope::Unkeyed(nonce) => SubmissionId::unkeyed(*nonce),
    }
}

fn request_digest(
    created_by: UserId,
    spec: &ExecutionSpec,
    ingress_node_id: NodeId,
    (captured_inputs, output_policies): (&[CapturedInput], &[PlacementPolicyRef]),
    admit_by: Option<u64>,
) -> Result<[u8; 32], ConversionError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(REQUEST_DIGEST_DOMAIN);
    hasher.update(&created_by.to_storage_key());
    hasher.update(&postcard::to_allocvec(spec)?);
    hasher.update(ingress_node_id.as_bytes());
    // Only the exact pins identify the request; metadata, grants and public reads may change.
    let pins: Vec<_> = captured_inputs
        .iter()
        .map(|captured| {
            (
                &captured.destination_key,
                captured.source_node_id.as_bytes(),
                captured.version_id,
                &captured.content,
                captured.bytes,
                &captured.policies,
                &captured.link,
            )
        })
        .collect();
    hasher.update(&postcard::to_allocvec(&pins)?);
    hasher.update(&postcard::to_allocvec(output_policies)?);
    // Absent for every request but a plugin task's, so other digests keep their bytes.
    if let Some(admit_by) = admit_by {
        hasher.update(&admit_by.to_be_bytes());
    }
    Ok(*hasher.finalize().as_bytes())
}

/// Stores the workspace choice in the spec. A caller that already set a
/// reserved tag is refused rather than silently overridden.
pub fn store_workspace(
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

/// The workspace the stored spec asks for. An unknown or absent mode owns no
/// bucket, which is what a run without a named bucket always meant.
pub fn workspace_of(spec: &ExecutionSpec) -> (WorkspaceMode, Option<String>) {
    let mode = match spec.tags.get(WORKSPACE_MODE_TAG).map(String::as_str) {
        Some("existing") => WorkspaceMode::Existing,
        _ => WorkspaceMode::None,
    };
    (mode, spec.tags.get(WORKSPACE_BUCKET_TAG).cloned())
}

/// What a session job carries next to its ordinary execution spec.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionSpec {
    pub runtime: String,
    /// The idle wait the submitter asked for. The executing node clamps it to
    /// the realm value.
    pub idle_after_ms: Option<u64>,
    /// When the submitter's bearer expires. The credential never outlives it.
    pub expires_at_ms: Option<u64>,
    /// Where the workspace bucket is mounted, when the submission recorded it.
    pub mount: Option<SessionMount>,
}

/// The interactive session the stored spec asks for. `None` is an ordinary run.
pub fn session_of(spec: &ExecutionSpec) -> Option<SessionSpec> {
    if spec.tags.get(SESSION_TAG).map(String::as_str) != Some(SESSION_TAG_NOTEBOOK) {
        return None;
    }
    let mount = spec.tags.get(MOUNT_PATH_TAG).map(|path| SessionMount {
        prefix: spec.tags.get(MOUNT_PREFIX_TAG).cloned().unwrap_or_default(),
        path: path.clone(),
    });
    Some(SessionSpec {
        runtime: spec
            .tags
            .get(SESSION_RUNTIME_TAG)
            .cloned()
            .unwrap_or_default(),
        idle_after_ms: spec
            .tags
            .get(SESSION_IDLE_TAG)
            .and_then(|value| value.parse().ok()),
        expires_at_ms: spec
            .tags
            .get(SESSION_EXPIRY_TAG)
            .and_then(|value| value.parse().ok()),
        mount,
    })
}

/// Required target labels the spec stores, in canonical order.
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

#[cfg(test)]
mod tests {
    use aruna_core::structs::execution::job::ComputeResources;

    use super::*;

    fn session_spec() -> ExecutionSpec {
        let mut tags = std::collections::BTreeMap::new();
        tags.insert(SESSION_TAG.to_string(), SESSION_TAG_NOTEBOOK.to_string());
        tags.insert(
            SESSION_RUNTIME_TAG.to_string(),
            "python-notebook".to_string(),
        );
        ExecutionSpec {
            group_id: Ulid::from_bytes([3u8; 16]),
            name: None,
            description: None,
            tags,
            image: "session:latest".to_string(),
            entrypoint: None,
            command: Vec::new(),
            workdir: Some("/work".to_string()),
            env: Default::default(),
            resources: ComputeResources::default(),
            executor_constraint: None,
            backend: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        }
    }

    #[test]
    fn inline_changes_digest() {
        // The bytes are part of the request, so other bytes are another request.
        use aruna_core::structs::execution::job::{InputMode, InputSelection, InputSource};
        let request = |bytes: &[u8]| {
            let mut spec = session_spec();
            spec.inputs.push(InputSelection {
                source: InputSource::Inline {
                    bytes: bytes.to_vec(),
                },
                source_node_id: None,
                dest_key: "/in/manifest.txt".to_string(),
                mode: InputMode::Snapshot,
                container_path: Some("/in/manifest.txt".to_string()),
                name: None,
                description: None,
            });
            SubmissionRequest {
                created_by: UserId::nil(aruna_core::structs::identity::realm::RealmId([1; 32])),
                spec,
                scope: SubmissionScope::Keyed("plugin-work/1".to_string()),
                retention_ms: 1,
                ingress_node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
                captured_inputs: Vec::new(),
                output_policies: Vec::new(),
                input_pages: None,
                admit_by: None,
            }
        };
        let (first, second) = (request(b"STUDY A"), request(b"STUDY B"));
        let (first, second) = (first.identity().unwrap(), second.identity().unwrap());
        assert_eq!(first.submission_id, second.submission_id);
        assert_ne!(first.request_digest, second.request_digest);
        // The admission deadline is part of the request; a resend keeps the same one.
        let (mut bounded, mut again) = (request(b"STUDY A"), request(b"STUDY A"));
        (bounded.admit_by, again.admit_by) = (Some(5), Some(5));
        let bounded = bounded.identity().unwrap().request_digest;
        assert_ne!(bounded, first.request_digest);
        assert_eq!(bounded, again.identity().unwrap().request_digest);
    }

    #[test]
    fn session_reads_mount() {
        // The recorded mount tags name the bucket slice and its folder; a spec
        // stored without them mounts nothing rather than guessing a layout.
        let mut spec = session_spec();
        assert_eq!(session_of(&spec).unwrap().mount, None);

        spec.tags
            .insert(MOUNT_PATH_TAG.to_string(), "/work/raw".to_string());
        assert_eq!(
            session_of(&spec).unwrap().mount,
            Some(SessionMount {
                prefix: String::new(),
                path: "/work/raw".to_string(),
            })
        );

        spec.tags
            .insert(MOUNT_PREFIX_TAG.to_string(), "raw/2024/".to_string());
        assert_eq!(
            session_of(&spec).unwrap().mount.unwrap().prefix,
            "raw/2024/"
        );
    }
}
