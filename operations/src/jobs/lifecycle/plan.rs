//! Assembling the pinned facts one planning round decides on.
//!
//! Everything here is resolved before the pure planner runs: exact input
//! versions and their known holders, the advertised targets, and the membership
//! facts of the nodes that published them. Membership comes from the
//! authenticated realm config, never from an advertisement's own claims.

use std::collections::BTreeMap;

use aruna_core::compute::{ExecutionTargetId, NetworkAccess, StagingMode};
use aruna_core::scheduling::{
    ExecutionPlan, InputHolder, MAX_INPUT_HOLDERS, MAX_PLAN_CANDIDATES, PlanRequest, ResolvedInput,
    TargetCandidate, plan_execution,
};
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::{
    BlobVersion, BlobVersionState, InputMode, InputSource, LogicalJobSpec, NodeInfoDocument,
    PlacementPolicyRef, PlacementSubject, PolicyResolution, RealmConfigDocument, RealmNodeKind,
    VersionKey, VersionedObjectArn,
};
use aruna_core::types::NodeId;
use thiserror::Error;
use tracing::debug;
use ulid::Ulid;

use super::ids;
use crate::blob_holders::GetBlobHoldersOperation;
use crate::driver::{DriverContext, drive};
use crate::node_info::read_node_info_document;
use crate::placement_policy::{ResolvePolicyConfig, ResolvePolicyOperation};
use crate::s3::head_object::{HeadObjectInput, HeadObjectOperation};

/// Tag that pins the container network mode, shared with the executor path.
const NETWORK_TAG_KEY: &str = "aruna-engine.org/network";

#[derive(Debug, PartialEq, Error)]
pub enum PlanBuildError {
    #[error("input {key} could not be pinned: {reason}")]
    Input { key: String, reason: String },
    #[error("planning facts are unavailable: {0}")]
    Unavailable(String),
    #[error(transparent)]
    Plan(#[from] aruna_core::scheduling::PlanError),
    #[error(transparent)]
    Request(#[from] ids::RequestError),
}

/// Builds and runs one planning round for the sealed spec. Targets already
/// declined for this request are excluded before ranking.
pub async fn build_plan(
    context: &DriverContext,
    config: &RealmConfigDocument,
    spec: &LogicalJobSpec,
    excluded: &[ExecutionTargetId],
    now_ms: u64,
) -> Result<ExecutionPlan, PlanBuildError> {
    let local = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or_else(|| PlanBuildError::Unavailable("network handle unavailable".to_string()))?;
    let documents = advertisements(context, config).await;
    let inputs = resolve_inputs(context, config, spec, &documents, local).await?;
    let output_policies = inputs
        .iter()
        .flat_map(|input| input.policies.clone())
        .collect::<Vec<_>>();
    let policies = resolve_policies(context, spec, &inputs, &output_policies, now_ms, local).await;
    let request = PlanRequest {
        submission_id: spec.submission_id,
        request_digest: spec.request_digest,
        spec_digest: spec.spec_digest,
        admitted: true,
        resources: spec.resources,
        executor_constraint: spec.payload.executor_constraint.clone(),
        required_labels: ids::required_labels(&spec.payload)?,
        staging: staging_mode(spec),
        network: network_access(spec),
        inputs,
        output_policies,
        policies,
        candidates: candidates(config, &documents, excluded),
        now_ms,
    };
    Ok(plan_execution(&request, &config.compute)?)
}

/// Node-info documents of every sync-eligible realm member that has one.
async fn advertisements(
    context: &DriverContext,
    config: &RealmConfigDocument,
) -> BTreeMap<NodeId, NodeInfoDocument> {
    let mut documents = BTreeMap::new();
    let Ok(members) = config.sync_eligible_node_ids() else {
        return documents;
    };
    for node_id in members {
        if let Ok(Some(document)) = read_node_info_document(&context.storage_handle, node_id).await
        {
            documents.insert(node_id, document);
        }
    }
    documents
}

/// One candidate per advertised backend. `node_kind` and `active` come from the
/// realm config; a document may only describe a backend, never its own standing.
fn candidates(
    config: &RealmConfigDocument,
    documents: &BTreeMap<NodeId, NodeInfoDocument>,
    excluded: &[ExecutionTargetId],
) -> Vec<TargetCandidate> {
    let mut candidates = Vec::new();
    for (node_id, document) in documents {
        let Some(kind) = node_kind(config, *node_id) else {
            continue;
        };
        let entry = config.placement_entry(*node_id);
        for capability in &document.executors {
            let target = capability.target(*node_id);
            if excluded.contains(&target) {
                continue;
            }
            candidates.push(TargetCandidate {
                node_id: *node_id,
                node_kind: kind.clone(),
                active: !document.leaving,
                compute_draining: document.compute_draining
                    || entry.is_some_and(|entry| entry.draining),
                group_allowed: true,
                capability: capability.clone(),
                load_permille: document.utilization.load_permille,
            });
            if candidates.len() >= MAX_PLAN_CANDIDATES {
                return candidates;
            }
        }
    }
    candidates
}

fn node_kind(config: &RealmConfigDocument, node_id: NodeId) -> Option<RealmNodeKind> {
    config
        .nodes
        .iter()
        .find(|node| node.node_id == node_id.to_string())
        .map(|node| node.kind.clone())
}

/// Pins every declared input to one exact version plus the holders currently
/// known for its bytes.
async fn resolve_inputs(
    context: &DriverContext,
    config: &RealmConfigDocument,
    spec: &LogicalJobSpec,
    documents: &BTreeMap<NodeId, NodeInfoDocument>,
    local: NodeId,
) -> Result<Vec<ResolvedInput>, PlanBuildError> {
    let mut inputs = Vec::new();
    for input in &spec.payload.inputs {
        let InputSource::S3 {
            bucket,
            key,
            version_id,
        } = &input.source;
        let requested = version_id
            .as_deref()
            .map(Ulid::from_string)
            .transpose()
            .map_err(|_| PlanBuildError::Input {
                key: input.dest_key.clone(),
                reason: "version id is not a ulid".to_string(),
            })?;
        let head = drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: requested,
            }),
            context,
        )
        .await
        .map_err(|error| PlanBuildError::Unavailable(error.to_string()))?
        .transpose()
        .map_err(|error| PlanBuildError::Input {
            key: input.dest_key.clone(),
            reason: error.to_string(),
        })?
        .ok_or_else(|| PlanBuildError::Input {
            key: input.dest_key.clone(),
            reason: "object not found".to_string(),
        })?;
        let version = head
            .resolved_version_id
            .or(head.version_id)
            .ok_or_else(|| PlanBuildError::Input {
                key: input.dest_key.clone(),
                reason: "object has no version".to_string(),
            })?;
        let blake3 = match head
            .location
            .as_ref()
            .and_then(|location| location.hashes.get(HASH_BLAKE3))
            .and_then(|hash| <[u8; 32]>::try_from(hash.as_slice()).ok())
        {
            Some(hash) => hash,
            None => version_hash(context, bucket, key, version)
                .await
                .ok_or_else(|| PlanBuildError::Input {
                    key: input.dest_key.clone(),
                    reason: "version is not materialized".to_string(),
                })?,
        };
        // An unknown size only ranks a route worse; it never grants capacity.
        let bytes = head
            .location
            .as_ref()
            .map(|location| location.blob_size)
            .or_else(|| {
                head.source_metadata
                    .as_ref()
                    .map(|metadata| metadata.content_length)
            })
            .unwrap_or_default();
        let holders = input_holders(
            context,
            config,
            documents,
            blake3,
            local,
            head.location.is_some(),
        )
        .await;
        inputs.push(ResolvedInput {
            destination_key: input.dest_key.clone(),
            source: VersionedObjectArn {
                realm_id: spec.realm_id,
                node_id: local,
                bucket: bucket.clone(),
                key: key.clone(),
                version,
            },
            version_id: version,
            blake3,
            bytes,
            policies: head.source_policies.clone(),
            holders,
        });
    }
    Ok(inputs)
}

/// The blake3 of a version this node does not hold locally, read from the
/// replicated version record.
async fn version_hash(
    context: &DriverContext,
    bucket: &str,
    key: &str,
    version: Ulid,
) -> Option<[u8; 32]> {
    let version_key = VersionKey::new(bucket, key, version).to_bytes().ok()?;
    let event = context
        .storage_handle
        .send_storage_effect(aruna_core::effects::StorageEffect::Read {
            key_space: aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE.to_string(),
            key: version_key.into(),
            txn_id: None,
        })
        .await;
    let aruna_core::events::Event::Storage(aruna_core::events::StorageEvent::ReadResult {
        value: Some(bytes),
        ..
    }) = event
    else {
        return None;
    };
    match postcard::from_bytes::<BlobVersion>(&bytes).ok()?.state {
        BlobVersionState::Materialized { blob_hash, .. } => Some(blob_hash),
        _ => None,
    }
}

/// Known holders of one input's bytes, with the storage subject each sits on.
/// Discovery is locality evidence only: compliance is decided by the planner.
async fn input_holders(
    context: &DriverContext,
    config: &RealmConfigDocument,
    documents: &BTreeMap<NodeId, NodeInfoDocument>,
    blake3: [u8; 32],
    local: NodeId,
    held_locally: bool,
) -> Vec<InputHolder> {
    let mut nodes: Vec<NodeId> = match context.net_handle.as_ref() {
        Some(net) => drive(
            GetBlobHoldersOperation::new(blake3, *net.realm_id(), local),
            context,
        )
        .await
        .unwrap_or_default(),
        None => Vec::new(),
    };
    if held_locally {
        nodes.push(local);
    }
    nodes.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    nodes.dedup();
    nodes.truncate(MAX_INPUT_HOLDERS);
    nodes
        .into_iter()
        .map(|node_id| InputHolder {
            subject: holder_subject(config, documents, node_id),
            node_id,
        })
        .collect()
}

/// The subject a holder's copy sits on: its advertised execution site when it
/// published one, otherwise its placement entry in the realm config.
fn holder_subject(
    config: &RealmConfigDocument,
    documents: &BTreeMap<NodeId, NodeInfoDocument>,
    node_id: NodeId,
) -> PlacementSubject {
    if let Some(subject) = documents
        .get(&node_id)
        .and_then(|document| document.executors.first())
        .map(|capability| capability.subject.clone())
    {
        return PlacementSubject {
            executor_kind: None,
            ..subject
        };
    }
    let entry = config.placement_entry(node_id);
    PlacementSubject {
        node_id,
        generation: 0,
        location: entry
            .map(|entry| entry.effective_location().to_string())
            .unwrap_or_default(),
        labels: entry.map(|entry| entry.labels.clone()).unwrap_or_default(),
        executor_kind: None,
        local_to_controller: false,
    }
}

/// Every distinct policy ref of the request, resolved through the ordinary
/// verifying resolver. A ref that stays unresolved blocks a target instead of
/// allowing it.
async fn resolve_policies(
    context: &DriverContext,
    spec: &LogicalJobSpec,
    inputs: &[ResolvedInput],
    output_policies: &[PlacementPolicyRef],
    now_ms: u64,
    local: NodeId,
) -> BTreeMap<Ulid, PolicyResolution> {
    let mut refs: Vec<PlacementPolicyRef> = inputs
        .iter()
        .flat_map(|input| input.policies.clone())
        .chain(output_policies.iter().cloned())
        .collect();
    refs.sort_unstable();
    refs.dedup();
    let mut resolved = BTreeMap::new();
    for policy_ref in refs {
        let outcome = drive(
            ResolvePolicyOperation::new(ResolvePolicyConfig {
                realm_id: spec.realm_id,
                policy_ref,
                local_node_id: local,
                now_ms,
            }),
            context,
        )
        .await;
        let resolution = match outcome {
            Ok(resolved) => PolicyResolution::Known(resolved.policy),
            Err(error) => {
                debug!(error = %error, "Placement policy stayed unresolved for planning");
                PolicyResolution::Unresolved
            }
        };
        resolved.insert(policy_ref.policy_id, resolution);
    }
    resolved
}

/// Mounted inputs need an S3 mount at the target; everything else is staged as
/// files into the workspace.
fn staging_mode(spec: &LogicalJobSpec) -> StagingMode {
    match spec
        .payload
        .inputs
        .iter()
        .any(|input| input.mode == InputMode::Mount)
    {
        true => StagingMode::S3Mount,
        false => StagingMode::Files,
    }
}

/// Network mode pinned by the request tag, defaulting to no egress.
fn network_access(spec: &LogicalJobSpec) -> NetworkAccess {
    match spec.payload.tags.get(NETWORK_TAG_KEY).map(String::as_str) {
        Some("open") => NetworkAccess::Open,
        _ => NetworkAccess::Isolated,
    }
}
