//! Assembling the pinned facts one planning round decides on.
//!
//! Everything here is resolved before the pure planner runs: exact input
//! versions and their known holders, the advertised targets, and the membership
//! facts of the nodes that published them. Membership comes from the
//! authenticated realm config, never from an advertisement's own claims.

use std::collections::BTreeMap;

use aruna_core::compute::{ExecutionTargetId, NetworkAccess, StagingMode};
use aruna_core::scheduling::{
    ExecutionPlan, InputHolder, MAX_TARGET_SCAN, PlanRequest, ResolvedInput, TargetCandidate,
    plan_execution,
};
use aruna_core::structs::{
    AuthContext, BlobVersion, BlobVersionState, InputMode, InputSource, JobInputFact,
    LogicalJobSpec, NodeInfoDocument, Permission, PlacementPolicyRef, PlacementSubject,
    PolicyResolution, RealmConfigDocument, RealmNodeKind, VersionKey, VersionedObjectArn,
    WorkspaceMode, blob_group_permission_path, storage_subject,
};
use aruna_core::types::NodeId;
use thiserror::Error;
use tracing::{debug, warn};
use ulid::Ulid;

use super::ids;
use crate::driver::{DriverContext, drive};
use crate::node_info::read_node_info_document;
use crate::placement_policy::{ResolvePolicyConfig, ResolvePolicyOperation};
use crate::request_authorization::authorize;
use crate::request_policy::PolicyRequestExtras;

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
    let inputs = resolve_inputs(config, spec)?;
    let mut output_policies = inputs
        .iter()
        .flat_map(|input| input.policies.clone())
        .collect::<Vec<_>>();
    output_policies.extend(spec.output_policies.clone());
    output_policies.sort_unstable();
    output_policies.dedup();
    let policies = resolve_policies(context, spec, &inputs, &output_policies, now_ms, local).await;
    let scan = candidates(context, config, spec, &documents, excluded).await;
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
        candidates: scan.candidates,
        scan_incomplete: scan.incomplete,
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

/// The advertisements one round scanned, and whether the scan bound stopped it
/// with advertisements still unseen.
struct Scan {
    candidates: Vec<TargetCandidate>,
    incomplete: bool,
}

impl Scan {
    /// Takes one advertisement while the scan bound allows. The first one it
    /// refuses makes the round a continuation rather than a full answer.
    fn take(&mut self, candidate: TargetCandidate) -> bool {
        if self.candidates.len() >= MAX_TARGET_SCAN {
            self.incomplete = true;
            return false;
        }
        self.candidates.push(candidate);
        true
    }
}

/// One candidate per advertised backend. `node_kind` and `active` come from the
/// realm config; a document may only describe a backend, never its own standing.
/// Eligibility is decided by the planner over the whole scan, never here.
async fn candidates(
    context: &DriverContext,
    config: &RealmConfigDocument,
    spec: &LogicalJobSpec,
    documents: &BTreeMap<NodeId, NodeInfoDocument>,
    excluded: &[ExecutionTargetId],
) -> Scan {
    let mut scan = Scan {
        candidates: Vec::new(),
        incomplete: false,
    };
    for (node_id, document) in documents {
        if ids::workspace_of(&spec.payload).0 == WorkspaceMode::Existing
            && *node_id != spec.ingress_node_id
        {
            continue;
        }
        let Some(kind) = node_kind(config, *node_id) else {
            continue;
        };
        let entry = config.placement_entry(*node_id);
        let group_allowed = target_allowed(context, spec, *node_id).await;
        for capability in &document.executors {
            let target = capability.target(*node_id);
            if excluded.contains(&target) {
                continue;
            }
            let taken = scan.take(TargetCandidate {
                node_id: *node_id,
                node_kind: kind.clone(),
                active: !document.leaving,
                compute_draining: document.compute_draining
                    || entry.is_some_and(|entry| entry.draining),
                group_allowed,
                capability: capability.clone(),
                load_permille: document.utilization.load_permille,
            });
            if !taken {
                warn!(
                    scanned = MAX_TARGET_SCAN,
                    "Advertisement scan bound reached before every target was seen"
                );
                return scan;
            }
        }
    }
    scan
}

async fn target_allowed(context: &DriverContext, spec: &LogicalJobSpec, target: NodeId) -> bool {
    let auth = AuthContext {
        user_id: spec.created_by,
        realm_id: spec.realm_id,
        path_restrictions: None,
    };
    authorize(
        context,
        spec.realm_id,
        &auth,
        &blob_group_permission_path(spec.realm_id, spec.group_id, target),
        &Permission::WRITE,
        PolicyRequestExtras::rest(),
    )
    .await
    .is_ok()
}

fn node_kind(config: &RealmConfigDocument, node_id: NodeId) -> Option<RealmNodeKind> {
    config
        .nodes
        .iter()
        .find(|node| node.node_id == node_id.to_string())
        .map(|node| node.kind.clone())
}

/// Pins every declared input to one exact version at its sealed source.
fn resolve_inputs(
    config: &RealmConfigDocument,
    spec: &LogicalJobSpec,
) -> Result<Vec<ResolvedInput>, PlanBuildError> {
    if spec.input_facts.len() != spec.payload.inputs.len() {
        return Err(PlanBuildError::Unavailable(
            "sealed input facts are incomplete".to_string(),
        ));
    }
    let mut inputs = Vec::new();
    for input in &spec.payload.inputs {
        let InputSource::S3 {
            bucket,
            key,
            version_id,
        } = &input.source;
        let fact: &JobInputFact = spec
            .input_facts
            .iter()
            .find(|fact| fact.destination_key == input.dest_key)
            .ok_or_else(|| PlanBuildError::Input {
                key: input.dest_key.clone(),
                reason: "sealed input facts are unavailable".to_string(),
            })?;
        if fact.source_node_id != spec.ingress_node_id {
            return Err(PlanBuildError::Input {
                key: input.dest_key.clone(),
                reason: "sealed input endpoint changed".to_string(),
            });
        }
        if version_id
            .as_deref()
            .map(Ulid::from_string)
            .transpose()
            .map_err(|_| PlanBuildError::Input {
                key: input.dest_key.clone(),
                reason: "version id is not a ulid".to_string(),
            })?
            .is_some_and(|requested| requested != fact.version_id)
        {
            return Err(PlanBuildError::Input {
                key: input.dest_key.clone(),
                reason: "sealed input version changed".to_string(),
            });
        }
        let holders = input_holders(config, fact.source_node_id);
        inputs.push(ResolvedInput {
            destination_key: input.dest_key.clone(),
            source: VersionedObjectArn {
                realm_id: spec.realm_id,
                node_id: fact.source_node_id,
                bucket: bucket.clone(),
                key: key.clone(),
                version: fact.version_id,
            },
            version_id: fact.version_id,
            blake3: fact.blake3,
            bytes: fact.bytes,
            policies: fact.policies.clone(),
            holders,
        });
    }
    Ok(inputs)
}

/// The blake3 of a version this node does not hold locally, read from the
/// replicated version record.
pub async fn version_hash(
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

/// The source endpoint owns the sealed bucket/key/version. A same-hash blob on
/// another node is not evidence that the node owns that S3 object identity.
fn input_holders(config: &RealmConfigDocument, source_node: NodeId) -> Vec<InputHolder> {
    vec![InputHolder {
        subject: holder_subject(config, source_node),
        node_id: source_node,
    }]
}

/// A holder's storage subject comes from its realm placement entry, never an
/// advertised executor site.
fn holder_subject(config: &RealmConfigDocument, node_id: NodeId) -> PlacementSubject {
    let entry = config.placement_entry(node_id);
    if let Some(entry) = entry {
        return storage_subject(entry, 1);
    }
    PlacementSubject {
        node_id,
        generation: 0,
        location: String::new(),
        labels: BTreeMap::new(),
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
pub(crate) fn staging_mode(spec: &LogicalJobSpec) -> StagingMode {
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
pub(crate) fn network_access(spec: &LogicalJobSpec) -> NetworkAccess {
    match spec.payload.tags.get(NETWORK_TAG_KEY).map(String::as_str) {
        Some("open") => NetworkAccess::Open,
        _ => NetworkAccess::Isolated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::records::tests::fixture::node;
    use aruna_core::compute::ExecutorCapability;

    fn advertised() -> TargetCandidate {
        let subject = PlacementSubject {
            node_id: node(1),
            generation: 1,
            location: "eu".to_string(),
            labels: BTreeMap::new(),
            executor_kind: None,
            local_to_controller: true,
        };
        TargetCandidate {
            node_id: node(1),
            node_kind: RealmNodeKind::Server,
            active: true,
            compute_draining: false,
            group_allowed: true,
            capability: ExecutorCapability::new("docker".to_string(), subject)
                .expect("subject is valid"),
            load_permille: None,
        }
    }

    #[test]
    fn scan_bound_stops() {
        // Filling the scan is not a continuation; only an advertisement the
        // bound refused proves that more of them remain unseen.
        let advertisement = advertised();
        let mut scan = Scan {
            candidates: Vec::new(),
            incomplete: false,
        };
        for _ in 0..MAX_TARGET_SCAN {
            assert!(scan.take(advertisement.clone()));
        }

        assert!(!scan.incomplete);
        assert!(!scan.take(advertisement));
        assert!(scan.incomplete);
        assert_eq!(scan.candidates.len(), MAX_TARGET_SCAN);
    }
}
