//! Assembling the pinned values one planning round decides on.
//!
//! Everything here is resolved before the pure planner runs: exact input
//! versions and their known holders, the advertised targets, and the membership
//! details of the nodes that published them. Membership comes from the
//! authenticated realm config, never from an advertisement's own claims.

use std::collections::BTreeMap;

use aruna_core::compute::{ExecutionTargetId, ExecutorCapability, NetworkAccess, StagingMode};
use aruna_core::scheduling::{
    ExecutionPlan, InputHolder, MAX_TARGET_SCAN, PlanRequest, Planner, ResolvedInput,
    TargetCandidate,
};
use aruna_core::structs::{
    AuthContext, BlobVersion, BlobVersionState, CapturedInput, InputSource, LogicalJobSpec,
    NodeInfoDocument, Permission, PlacementPolicyRef, PlacementSubject, PolicyResolution,
    RealmConfigDocument, RealmNodeKind, VersionKey, VersionedObjectArn, WorkspaceMode,
    blob_group_permission_path, storage_subject,
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

/// A launch pins every input to one version and a pinned mount is delivered as
/// staged files, so every realm job stages files. An S3 mount serves the head
/// only and is reachable outside this lifecycle alone.
pub(crate) const REALM_STAGING: StagingMode = StagingMode::Files;

#[derive(Debug, PartialEq, Error)]
pub enum PlanBuildError {
    #[error("input {key} could not be pinned: {reason}")]
    Input { key: String, reason: String },
    #[error("planning inputs are unavailable: {0}")]
    Unavailable(String),
    #[error(transparent)]
    Plan(#[from] aruna_core::scheduling::PlanError),
    #[error(transparent)]
    Request(#[from] ids::RequestError),
}

/// Builds and runs one planning round for the stored spec. Targets already
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
    let (documents, unread) = advertisements(context, config).await;
    let inputs = resolve_inputs(config, spec)?;
    let mut output_policies = inputs
        .iter()
        .flat_map(|input| input.policies.clone())
        .collect::<Vec<_>>();
    output_policies.extend(spec.output_policies.clone());
    output_policies.sort_unstable();
    output_policies.dedup();
    let policies = resolve_policies(context, spec, &inputs, &output_policies, now_ms, local).await;
    let request = plan_request(spec, inputs, output_policies, policies, now_ms)?;
    let mut planner = Planner::new(&request, &config.compute)?;
    let scan = candidates(context, config, spec, &documents, excluded, &mut planner).await?;
    debug!(
        pages = scan.pages,
        scanned = scan.scanned,
        unread,
        "Screened the realm advertisements for one planning round"
    );
    Ok(planner.finish(unread))
}

/// The pinned values one round screens every advertisement against.
fn plan_request(
    spec: &LogicalJobSpec,
    inputs: Vec<ResolvedInput>,
    output_policies: Vec<PlacementPolicyRef>,
    policies: BTreeMap<Ulid, PolicyResolution>,
    now_ms: u64,
) -> Result<PlanRequest, PlanBuildError> {
    Ok(PlanRequest {
        submission_id: spec.submission_id,
        request_digest: spec.request_digest,
        spec_digest: spec.spec_digest,
        admitted: true,
        resources: spec.resources,
        executor_constraint: spec.payload.executor_constraint.clone(),
        required_labels: ids::required_labels(&spec.payload)?,
        staging: REALM_STAGING,
        network: network_access(spec),
        inputs,
        output_policies,
        policies,
        now_ms,
    })
}

/// Node-info documents of every sync-eligible realm member that has one, and
/// whether a read failed. An unread advertisement may hold the only legal
/// target, so the round is a continuation rather than a conclusive refusal.
async fn advertisements(
    context: &DriverContext,
    config: &RealmConfigDocument,
) -> (BTreeMap<NodeId, NodeInfoDocument>, bool) {
    let mut documents = BTreeMap::new();
    let Ok(members) = config.sync_eligible_node_ids() else {
        return (documents, true);
    };
    let mut unread = false;
    for node_id in members {
        match read_node_info_document(&context.storage_handle, node_id).await {
            Ok(Some(document)) => {
                documents.insert(node_id, document);
            }
            Ok(None) => {}
            Err(error) => {
                warn!(node = %node_id, error = %error, "Node info advertisement stayed unread");
                unread = true;
            }
        }
    }
    (documents, unread)
}

/// What one paged advertisement walk handed to the planner.
struct Scan {
    pages: u32,
    scanned: u64,
}

/// One candidate per advertised backend, screened in pages of at most
/// [`MAX_TARGET_SCAN`]. `node_kind` and `active` come from the realm config; a
/// document may only describe a backend, never its own standing. Eligibility is
/// decided by the planner over the whole scan, never here.
async fn candidates(
    context: &DriverContext,
    config: &RealmConfigDocument,
    spec: &LogicalJobSpec,
    documents: &BTreeMap<NodeId, NodeInfoDocument>,
    excluded: &[ExecutionTargetId],
    planner: &mut Planner<'_>,
) -> Result<Scan, PlanBuildError> {
    let mut scan = Scan {
        pages: 0,
        scanned: 0,
    };
    let mut page = Vec::new();
    // Members come out of the map in node id byte order and every document's
    // backends are walked by kind: the canonical order pages must continue in.
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
        let mut executors: Vec<&ExecutorCapability> = document.executors.iter().collect();
        executors.sort_unstable_by(|left, right| left.kind.as_str().cmp(right.kind.as_str()));
        for capability in executors {
            if excluded.contains(&capability.target(*node_id)) {
                continue;
            }
            page.push(TargetCandidate {
                node_id: *node_id,
                node_kind: kind.clone(),
                active: !document.leaving,
                compute_draining: document.compute_draining
                    || entry.is_some_and(|entry| entry.draining),
                group_allowed,
                capability: capability.clone(),
                load_permille: document.utilization.load_permille,
            });
            if page.len() == MAX_TARGET_SCAN {
                flush(planner, &mut page, &mut scan)?;
            }
        }
    }
    flush(planner, &mut page, &mut scan)?;
    Ok(scan)
}

/// Hands one buffered page to the planner. A page is only flushed with
/// advertisements in it, so a scan never ends on an empty page.
fn flush(
    planner: &mut Planner<'_>,
    page: &mut Vec<TargetCandidate>,
    scan: &mut Scan,
) -> Result<(), PlanBuildError> {
    if page.is_empty() {
        return Ok(());
    }
    planner.rank_page(page)?;
    scan.pages += 1;
    scan.scanned += page.len() as u64;
    page.clear();
    Ok(())
}

async fn target_allowed(context: &DriverContext, spec: &LogicalJobSpec, target: NodeId) -> bool {
    let auth = AuthContext {
        user_id: spec.created_by,
        realm_id: spec.realm_id,
        path_restrictions: None,
        session: None,
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

/// Pins every declared input to one exact version at its stored source.
fn resolve_inputs(
    config: &RealmConfigDocument,
    spec: &LogicalJobSpec,
) -> Result<Vec<ResolvedInput>, PlanBuildError> {
    if spec.captured_inputs.len() != spec.payload.inputs.len() {
        return Err(PlanBuildError::Unavailable(
            "captured inputs are incomplete".to_string(),
        ));
    }
    let mut inputs = Vec::new();
    for input in &spec.payload.inputs {
        let InputSource::S3 {
            bucket,
            key,
            version_id,
        } = &input.source;
        let captured: &CapturedInput = spec
            .captured_inputs
            .iter()
            .find(|captured| captured.destination_key == input.dest_key)
            .ok_or_else(|| PlanBuildError::Input {
                key: input.dest_key.clone(),
                reason: "captured inputs are unavailable".to_string(),
            })?;
        if captured.source_node_id != spec.ingress_node_id {
            return Err(PlanBuildError::Input {
                key: input.dest_key.clone(),
                reason: "captured input endpoint changed".to_string(),
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
            .is_some_and(|requested| requested != captured.version_id)
        {
            return Err(PlanBuildError::Input {
                key: input.dest_key.clone(),
                reason: "captured input version changed".to_string(),
            });
        }
        let holders = input_holders(config, captured.source_node_id);
        inputs.push(ResolvedInput {
            destination_key: input.dest_key.clone(),
            source: VersionedObjectArn {
                realm_id: spec.realm_id,
                node_id: captured.source_node_id,
                bucket: bucket.clone(),
                key: key.clone(),
                version: captured.version_id,
            },
            version_id: captured.version_id,
            blake3: captured.blake3,
            bytes: captured.bytes,
            policies: captured.policies.clone(),
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

/// The source endpoint owns the stored bucket/key/version. A same-hash blob on
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
    use crate::jobs::records::tests::fixture::{Family, REALM, context, node};
    use aruna_core::compute::{ExecutorCapability, MAX_ADVERTISED_EXECUTORS};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::NODE_INFO_KEYSPACE;
    use aruna_core::scheduling::plan_execution;
    use aruna_core::structs::{
        AdvertisementEpoch, InputMode, InputSelection, NodeUrls, NodeUtilization,
        node_info_storage_key,
    };

    /// A realm of `members` servers, each advertising eight backends, which is
    /// more advertisements than one planning page screens.
    fn realm(members: u8) -> (RealmConfigDocument, Vec<NodeInfoDocument>) {
        let mut config = RealmConfigDocument::new(REALM, Vec::new(), 5);
        let documents = (1..=members)
            .map(|seed| {
                config.ensure_node(node(seed), RealmNodeKind::Server);
                advertised(node(seed))
            })
            .collect();
        (config, documents)
    }

    fn advertised(node_id: NodeId) -> NodeInfoDocument {
        let executors = (0..MAX_ADVERTISED_EXECUTORS)
            .map(|index| {
                let subject = PlacementSubject {
                    node_id,
                    generation: 1,
                    location: "eu".to_string(),
                    labels: BTreeMap::new(),
                    executor_kind: None,
                    local_to_controller: true,
                };
                ExecutorCapability::new(format!("k{index}"), subject).expect("subject is valid")
            })
            .collect();
        NodeInfoDocument {
            node_id,
            executors,
            labels: BTreeMap::new(),
            urls: NodeUrls {
                api: None,
                s3: None,
            },
            utilization: NodeUtilization {
                storage_bytes_used: 0,
                documents_held: None,
                load_permille: Some(100),
                heartbeat_at_ms: 1,
            },
            updated_at_ms: 1,
            epoch: AdvertisementEpoch::default(),
            compute_draining: false,
            leaving: false,
            demand: Default::default(),
            reservation: Default::default(),
        }
    }

    /// The pinned values of one round, with the candidates left to the scan.
    fn round_request(spec: &LogicalJobSpec) -> PlanRequest {
        plan_request(spec, Vec::new(), Vec::new(), BTreeMap::new(), 2_000)
            .expect("request is well formed")
    }

    async fn publish(context: &DriverContext, document: &NodeInfoDocument) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: NODE_INFO_KEYSPACE.to_string(),
                key: node_info_storage_key(document.node_id).into(),
                value: document.to_bytes().expect("advertisement is valid").into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    /// One planning walk over the advertisements in the order they were
    /// published, with the cursor kept before the plan is stored.
    async fn walk(
        config: &RealmConfigDocument,
        documents: &[NodeInfoDocument],
    ) -> (Scan, Option<ExecutionTargetId>, ExecutionPlan) {
        let family = Family::new([4u8; 32]);
        let (_dir, context) = context(config, node(1)).await;
        for document in documents {
            publish(&context, document).await;
        }
        let spec = family.spec();
        let (read, _) = advertisements(&context, config).await;
        let request = round_request(&spec);
        let mut planner = Planner::new(&request, &config.compute).expect("request is well formed");
        let scan = candidates(&context, config, &spec, &read, &[], &mut planner)
            .await
            .expect("every page continues the scan");
        let cursor = planner.cursor().cloned();
        (scan, cursor, planner.finish(false))
    }

    #[tokio::test]
    async fn reversed_order_agrees() {
        // Publication order must never reach the plan: ascending nodes with
        // k0..k7 and descending nodes with k7..k0 walk into one identical round.
        let (config, ascending) = realm(129);
        let descending: Vec<_> = ascending
            .iter()
            .rev()
            .map(|document| {
                let mut reversed = document.clone();
                reversed.executors.reverse();
                reversed
            })
            .collect();

        let (first, first_cursor, first_plan) = walk(&config, &ascending).await;
        let (second, second_cursor, second_plan) = walk(&config, &descending).await;

        assert_eq!((first.pages, first.scanned), (second.pages, second.scanned));
        assert_eq!(first_cursor, second_cursor);
        assert_eq!(first_plan, second_plan);
    }

    #[tokio::test]
    async fn walks_every_page() {
        // 129 nodes advertising eight backends each overrun one page: the walk
        // must page the overrun into the same planner instead of stopping.
        let (config, documents) = realm(129);
        let family = Family::new([4u8; 32]);
        let (_dir, context) = context(&config, node(1)).await;
        for document in &documents {
            publish(&context, document).await;
        }
        let spec = family.spec();

        let (read, unread) = advertisements(&context, &config).await;
        assert!(!unread && read.len() == 129);
        let request = round_request(&spec);
        let mut planner = Planner::new(&request, &config.compute).expect("request is well formed");
        let scan = candidates(&context, &config, &spec, &read, &[], &mut planner)
            .await
            .expect("every page continues the scan");

        assert_eq!((scan.pages, scan.scanned), (2, 1_032));
        assert_eq!(planner.scanned(), 1_032);
        // The cursor proves the walk reached the last advertisement of the
        // highest node id, past the entries the old scan bound cut off.
        let last = read.values().last().expect("the realm advertises");
        assert_eq!(
            planner.cursor(),
            Some(&last.executors[MAX_ADVERTISED_EXECUTORS - 1].target(last.node_id))
        );
    }

    #[tokio::test]
    async fn excluded_targets_skipped() {
        // An excluded target leaves the scan without disturbing its order, so
        // the following pages still continue past the cursor.
        let (config, documents) = realm(129);
        let family = Family::new([4u8; 32]);
        let (_dir, context) = context(&config, node(1)).await;
        for document in &documents {
            publish(&context, document).await;
        }
        let spec = family.spec();
        let excluded = vec![documents[0].executors[0].target(documents[0].node_id)];

        let (read, _) = advertisements(&context, &config).await;
        let request = round_request(&spec);
        let mut planner = Planner::new(&request, &config.compute).expect("request is well formed");
        let scan = candidates(&context, &config, &spec, &read, &excluded, &mut planner)
            .await
            .expect("every page continues the scan");

        assert_eq!((scan.pages, scan.scanned), (2, 1_031));
    }

    /// One Docker-shaped site: file staging only, no S3 mount.
    fn docker(node_id: NodeId) -> ExecutorCapability {
        let subject = PlacementSubject {
            node_id,
            generation: 1,
            location: "eu".to_string(),
            labels: BTreeMap::new(),
            executor_kind: None,
            local_to_controller: true,
        };
        let mut capability =
            ExecutorCapability::new("docker".to_string(), subject).expect("subject is valid");
        capability.file_staging = true;
        capability
    }

    fn mounted(spec: &mut LogicalJobSpec) {
        spec.payload.inputs.push(InputSelection {
            source: InputSource::S3 {
                bucket: "inputs".to_string(),
                key: "reads.fastq".to_string(),
                version_id: None,
            },
            source_node_id: None,
            dest_key: "in/reads.fastq".to_string(),
            mode: InputMode::Mount,
            container_path: Some("/in/reads.fastq".to_string()),
            name: None,
            description: None,
        });
    }

    #[test]
    fn mounts_plan_files() {
        // A mounted input must not narrow a job to S3-mount sites: the launch
        // pins every input, and a pinned mount is delivered as staged files.
        let family = Family::new([4u8; 32]);
        let mut spec = family.spec();
        mounted(&mut spec);
        let request = round_request(&spec);
        assert_eq!(request.staging, StagingMode::Files);
        let candidate = TargetCandidate {
            node_id: node(1),
            node_kind: RealmNodeKind::Server,
            active: true,
            compute_draining: false,
            group_allowed: true,
            capability: docker(node(1)),
            load_permille: Some(100),
        };

        let plan = plan_execution(&request, &[candidate], &family.config.compute)
            .expect("request is well formed");

        assert_eq!(
            plan.selected.map(|selection| selection.target),
            Some(ExecutionTargetId {
                node_id: node(1),
                executor_kind: "docker".to_string(),
            })
        );
        assert!(plan.rejected.is_empty(), "{:?}", plan.rejected);
    }
}
