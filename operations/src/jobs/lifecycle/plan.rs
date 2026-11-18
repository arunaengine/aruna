//! Builds one planning round: pinned input versions and holders, plus the target candidates.
//! Publisher membership comes from the authenticated realm config, not from advertised claims.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::compute::{ExecutionTargetId, ExecutorCapability, NetworkAccess, StagingMode};
use aruna_core::id::NodeId;
use aruna_core::scheduling::{
    ExecutionPlan, InputHolder, MAX_INPUT_HOLDERS, MAX_TARGET_SCAN, PlanRequest, Planner,
    ResolvedInput, TargetCandidate,
};
use aruna_core::structs::execution::job::{
    CapturedInput, InputSource, LogicalJobSpec, WorkspaceMode,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::structs::placement::node_subject::storage_subject;
use aruna_core::structs::placement::policy::{
    PlacementPolicyRef, PlacementSubject, PolicyResolution,
};
use aruna_core::structs::placement::record::LabelMatch;
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_core::structs::storage::node_info::NodeInfoDocument;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use thiserror::Error;
use tracing::{debug, warn};
use ulid::Ulid;

use super::ids;
use crate::auth::check_permissions::CheckPermissionsConfig;
use crate::auth::enforcement::{policy_reads, read_policies};
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::blob::holders::{GetHoldersError, GetHoldersOperation};
use crate::placement::policy::{PolicySetError, PolicySetResolver, Resolutions, ResolveMode};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::heartbeat::{HeldHeartbeat, live_telemetry};
use aruna_core::node_state::{NodeInfoError, ReadInfoOperation};
use aruna_core::operation::Operation;
use aruna_core::types::Value;

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

impl From<IoError> for PlanBuildError {
    fn from(error: IoError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

/// What one planning round starts from.
#[derive(Debug, PartialEq)]
pub struct PlanRound {
    pub config: RealmConfigDocument,
    /// The stored spec with its paged inputs read in.
    pub spec: LogicalJobSpec,
    pub excluded: Vec<ExecutionTargetId>,
    pub now_ms: u64,
    pub local: NodeId,
    /// The fresh heartbeats whose telemetry overrides the advertised one.
    pub fresh: BTreeMap<NodeId, HeldHeartbeat>,
}

#[derive(Debug, PartialEq)]
enum PlanStep {
    Advertisement(NodeId),
    Holders(usize),
    Policies,
    PolicyRows,
    Grant(NodeId),
}

/// Builds and runs one planning round: the advertisement of every member, the pinned inputs
/// with their holders, the policies, and whether the submitter may run on each node. Targets
/// already declined for this request are excluded before ranking; an unread advertisement may
/// hold the only legal target, so the round is then a continuation, not a conclusive refusal.
#[derive(Debug, PartialEq)]
pub struct BuildPlanOperation {
    round: PlanRound,
    /// Members whose advertisement is still to read, the next one last.
    members: Vec<NodeId>,
    documents: BTreeMap<NodeId, NodeInfoDocument>,
    unread: bool,
    inputs: Vec<ResolvedInput>,
    policies: BTreeMap<Ulid, PolicyResolution>,
    /// Candidate nodes whose grant is still to read, the next one last.
    targets: Vec<NodeId>,
    /// Whether the request policies allow the submitter's write on each candidate node.
    policy_allows: BTreeMap<NodeId, bool>,
    allowed: BTreeMap<NodeId, bool>,
    step: PlanStep,
}

type PlanNext = Result<Next<ExecutionPlan>, PlanBuildError>;

impl BuildPlanOperation {
    pub fn new(round: PlanRound) -> Self {
        let (mut members, unread) = match round.config.sync_eligible_nodes() {
            Ok(members) => (members, false),
            Err(_) => (Vec::new(), true),
        };
        members.reverse();
        Self {
            round,
            members,
            documents: BTreeMap::new(),
            unread,
            inputs: Vec::new(),
            policies: BTreeMap::new(),
            targets: Vec::new(),
            policy_allows: BTreeMap::new(),
            allowed: BTreeMap::new(),
            step: PlanStep::Policies,
        }
    }

    fn advertisement(&mut self) -> PlanNext {
        if let Some(node) = self.members.pop() {
            self.step = PlanStep::Advertisement(node);
            return Ok(Next::Call(IoCall::child(ReadInfoOperation::new(vec![
                node,
            ]))));
        }
        live_telemetry(&self.round.fresh, &mut self.documents, self.round.now_ms);
        self.inputs = pinned_inputs(&self.round.config, &self.round.spec)?;
        self.holders(0)
    }

    /// Asks the DHT for the registered copies of input `index` onward; a reference has no
    /// stored copies to find by content, so its source stays its only holder.
    fn holders(&mut self, mut index: usize) -> PlanNext {
        while let Some(input) = self.inputs.get(index) {
            if let Some(blake3) = input.content.blake3() {
                self.step = PlanStep::Holders(index);
                let (realm_id, local) = (self.round.spec.realm_id, self.round.local);
                let lookup = GetHoldersOperation::new(blake3, realm_id, local);
                return Ok(Next::Call(IoCall::child(lookup)));
            }
            index += 1;
        }
        self.step = PlanStep::Policies;
        let refs = self.inputs.iter().flat_map(|input| input.policies.clone());
        let refs: Vec<_> = refs
            .chain(self.round.spec.output_policies.clone())
            .collect();
        let round = &self.round;
        let (realm_id, local, now_ms) = (round.spec.realm_id, round.local, round.now_ms);
        let resolve = PolicySetResolver::new(realm_id, local, now_ms, ResolveMode::Lenient, &refs);
        Ok(Next::Call(IoCall::child(resolve)))
    }

    /// The nodes a candidate may run on, then the request policy rows of the group.
    fn targets(&mut self) -> PlanNext {
        let spec = &self.round.spec;
        let existing = ids::workspace_of(&spec.payload).0 == WorkspaceMode::Existing;
        let mut targets: Vec<NodeId> = self
            .documents
            .keys()
            .filter(|node| !existing || **node == spec.ingress_node_id)
            .filter(|node| node_kind(&self.round.config, **node).is_some())
            .copied()
            .collect();
        targets.reverse();
        self.targets = targets;
        self.step = PlanStep::PolicyRows;
        let rows = policy_reads(spec.realm_id, Some(spec.group_id));
        Ok(Next::Call(IoCall::Read(rows)))
    }

    /// Evaluates the request policies for every candidate node; missing state fails closed.
    fn judge_policies(&mut self, values: Vec<Option<Value>>) {
        let spec = &self.round.spec;
        let evaluator = read_policies(spec.realm_id, Some(spec.group_id), values).ok();
        for node in &self.targets {
            let allows = evaluator.as_ref().is_some_and(|evaluator| {
                let check = write_check(spec, *node);
                let request = policy_request_with(
                    &check.path,
                    &Permission::WRITE,
                    Some(&check.auth_context),
                    PolicyRequestExtras::rest(),
                );
                evaluator.evaluate(&request).is_ok()
            });
            self.policy_allows.insert(*node, allows);
        }
    }

    /// Reads the submitter's grants on the next candidate node the policies allow.
    fn grant(&mut self) -> PlanNext {
        while let Some(node) = self.targets.pop() {
            let reads = match self.policy_allows.get(&node) {
                Some(true) => write_check(&self.round.spec, node).reads().ok(),
                _ => None,
            };
            match reads {
                Some(reads) => {
                    self.step = PlanStep::Grant(node);
                    return Ok(Next::Call(IoCall::Read(reads)));
                }
                None => {
                    self.allowed.insert(node, false);
                }
            }
        }
        self.finish()
    }

    fn finish(&mut self) -> PlanNext {
        let round = &self.round;
        let mut output_policies: Vec<PlacementPolicyRef> = self
            .inputs
            .iter()
            .flat_map(|input| input.policies.clone())
            .collect();
        output_policies.extend(round.spec.output_policies.clone());
        output_policies.sort_unstable();
        output_policies.dedup();
        let inputs = std::mem::take(&mut self.inputs);
        let policies = std::mem::take(&mut self.policies);
        let request = plan_request(&round.spec, inputs, output_policies, policies, round.now_ms)?;
        let mut planner = Planner::new(&request, &round.config.compute)?;
        let scan = candidates(round, &self.documents, &self.allowed, &mut planner)?;
        debug!(
            pages = scan.pages,
            scanned = scan.scanned,
            unread = self.unread,
            "Screened the realm advertisements for one planning round"
        );
        Ok(Next::Done(planner.finish(self.unread)))
    }
}

impl Operation for BuildPlanOperation {
    type Output = ExecutionPlan;
    type Error = PlanBuildError;

    fn first(&mut self) -> PlanNext {
        self.advertisement()
    }

    fn next(&mut self, result: IoResult) -> PlanNext {
        match self.step {
            PlanStep::Advertisement(node) => {
                let read = result.child::<BTreeMap<NodeId, NodeInfoDocument>, NodeInfoError>()?;
                match read.map(|mut documents| documents.remove(&node)) {
                    Ok(Some(document)) => {
                        self.documents.insert(node, document);
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(node = %node, error = %error, "Node info advertisement stayed unread");
                        self.unread = true;
                    }
                }
                self.advertisement()
            }
            PlanStep::Holders(index) => {
                let registered = result
                    .child::<Vec<NodeId>, GetHoldersError>()?
                    .unwrap_or_else(|error| {
                        debug!(error = %error, "Blob holder lookup found no registered copy");
                        Vec::new()
                    });
                let config = &self.round.config;
                if let Some(input) = self.inputs.get_mut(index) {
                    merge_holders(config, &mut input.holders, registered);
                }
                self.holders(index + 1)
            }
            PlanStep::Policies => {
                let resolved = result.child::<Resolutions, PolicySetError>()?;
                self.policies = resolved.unwrap_or_else(|error| {
                    debug!(error = %error, "Placement policies stayed unresolved for planning");
                    BTreeMap::new()
                });
                self.targets()
            }
            PlanStep::PolicyRows => {
                let values = row_values(result)?;
                self.judge_policies(values);
                self.grant()
            }
            PlanStep::Grant(node) => {
                let values = row_values(result)?;
                let check = write_check(&self.round.spec, node);
                self.allowed
                    .insert(node, check.allowed(values).unwrap_or(false));
                self.grant()
            }
        }
    }
}

fn row_values(result: IoResult) -> Result<Vec<Option<Value>>, PlanBuildError> {
    match result {
        IoResult::Values(values) => Ok(values),
        other => Err(IoError::Unexpected {
            expected: "IoResult::Values",
            got: other.label(),
        }
        .into()),
    }
}

/// The submitter's write check on the group path of `node`.
fn write_check(spec: &LogicalJobSpec, node: NodeId) -> CheckPermissionsConfig {
    CheckPermissionsConfig {
        auth_context: AuthContext {
            user_id: spec.created_by,
            realm_id: spec.realm_id,
            path_restrictions: None,
            session: None,
        },
        path: group_permission_path(spec.realm_id, spec.group_id, node),
        required_permission: Permission::WRITE,
    }
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
        session: ids::session_of(&spec.payload).is_some(),
        inputs,
        output_policies,
        policies,
        now_ms,
    })
}

/// The first required label no advertised executor carries; no round can place such a request.
pub(super) fn missing_label<'a>(
    documents: &BTreeMap<NodeId, NodeInfoDocument>,
    labels: &'a [LabelMatch],
) -> Option<&'a LabelMatch> {
    let executors = documents.values().flat_map(|document| &document.executors);
    labels.iter().find(|label| {
        !executors.clone().any(|capability| {
            let carried = capability.subject.labels.get(label.key.trim());
            carried.map(|value| value.trim()) == Some(label.value.trim())
        })
    })
}

/// What one paged advertisement walk handed to the planner.
struct Scan {
    pages: u32,
    scanned: u64,
}

/// One candidate per advertised backend, screened in pages of at most
/// [`MAX_TARGET_SCAN`]. A document may only describe a backend, never its own
/// standing; eligibility is decided by the planner over the whole scan.
fn candidates(
    round: &PlanRound,
    documents: &BTreeMap<NodeId, NodeInfoDocument>,
    allowed: &BTreeMap<NodeId, bool>,
    planner: &mut Planner<'_>,
) -> Result<Scan, PlanBuildError> {
    let (config, spec, excluded) = (&round.config, &round.spec, &round.excluded);
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
        let group_allowed = allowed.get(node_id).copied().unwrap_or(false);
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

fn node_kind(config: &RealmConfigDocument, node_id: NodeId) -> Option<RealmNodeKind> {
    config
        .nodes
        .iter()
        .find(|node| node.node_id == node_id.to_string())
        .map(|node| node.kind.clone())
}

/// Pins every declared input to one exact version at its stored source.
/// The source endpoint is each input's first holder; the registered copies follow later.
fn pinned_inputs(
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
        // Each input was resolved at its own node, or at ingress when it named none.
        if captured.source_node_id != input.source_node_id.unwrap_or(spec.ingress_node_id) {
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
        let holders = vec![InputHolder {
            subject: holder_subject(config, captured.source_node_id),
            node_id: captured.source_node_id,
        }];
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
            content: captured.content.clone(),
            bytes: captured.bytes,
            policies: captured.policies.clone(),
            holders,
        });
    }
    Ok(inputs)
}

/// The source endpoint owns the stored bucket/key/version, and every node the DHT lists for
/// the same content hash holds a registered copy, so running next to one saves the transfer; a
/// failed lookup leaves the source as the only route.
/// Adds the registered copies to the holder list, keeping the source endpoint
/// first, dropping repeats, and stopping at the bound the planner accepts.
fn merge_holders(
    config: &RealmConfigDocument,
    holders: &mut Vec<InputHolder>,
    registered: Vec<NodeId>,
) {
    for node_id in registered {
        if holders.len() >= MAX_INPUT_HOLDERS {
            return;
        }
        if holders.iter().all(|holder| holder.node_id != node_id) {
            holders.push(InputHolder {
                subject: holder_subject(config, node_id),
                node_id,
            });
        }
    }
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
    use crate::driver::DriverContext;
    use crate::jobs::lifecycle::ingress::advertisements;
    use crate::tests::records::{Family, REALM, context, node};
    use aruna_core::compute::{ExecutorCapability, MAX_ADVERTISED_EXECUTORS};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::NODE_INFO_KEYSPACE;
    use aruna_core::scheduling::plan_execution;
    use aruna_core::structs::execution::job::{InputMode, InputSelection};
    use aruna_core::structs::storage::node_info::{
        AdvertisementEpoch, NodeUrls, NodeUtilization, node_info_key,
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
                download_slots: None,
                storage_capacity_bytes: None,
            },
            updated_at_ms: 1,
            epoch: AdvertisementEpoch::default(),
            compute_draining: false,
            leaving: false,
            demand: Default::default(),
            reservation: Default::default(),
        }
    }

    /// A round over `config` for `spec` that excludes `excluded`.
    fn round(
        config: &RealmConfigDocument,
        spec: &LogicalJobSpec,
        excluded: &[ExecutionTargetId],
    ) -> PlanRound {
        PlanRound {
            config: config.clone(),
            spec: spec.clone(),
            excluded: excluded.to_vec(),
            now_ms: 2_000,
            local: node(1),
            fresh: BTreeMap::new(),
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
                key: node_info_key(document.node_id).into(),
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
        let scan = candidates(
            &round(config, &spec, &[]),
            &read,
            &BTreeMap::new(),
            &mut planner,
        )
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
        let scan = candidates(
            &round(&config, &spec, &[]),
            &read,
            &BTreeMap::new(),
            &mut planner,
        )
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
        let scan = candidates(
            &round(&config, &spec, &excluded),
            &read,
            &BTreeMap::new(),
            &mut planner,
        )
        .expect("every page continues the scan");

        assert_eq!((scan.pages, scan.scanned), (2, 1_031));
    }

    // A label no executor carries is named, so submit can refuse what no round would place.
    #[test]
    fn names_missing_label() {
        let mut document = advertised(node(1));
        (document.executors[0].subject.labels).insert("gpu".to_string(), "a100".to_string());
        let documents = BTreeMap::from([(node(1), document)]);
        let label = |key: &str, value: &str| LabelMatch {
            key: key.to_string(),
            value: value.to_string(),
        };

        let carried = [label("gpu", "a100")];
        assert_eq!(missing_label(&documents, &carried), None);
        let wanted = [label("gpu", "a100"), label("aruna-engine.org/node", "n2")];
        assert_eq!(missing_label(&documents, &wanted), Some(&wanted[1]));
        let other = [label("gpu", "h100")];
        assert_eq!(missing_label(&documents, &other), Some(&other[0]));
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
    fn flags_session_request() {
        // Only a backend that can open a session channel may take a session, so
        // the tag must reach the request every advertisement is screened by.
        let family = Family::new([4u8; 32]);
        let mut spec = family.spec();
        assert!(!round_request(&spec).session);

        spec.payload.tags.insert(
            aruna_core::compute::runtimes::SESSION_TAG.to_string(),
            aruna_core::compute::runtimes::SESSION_TAG_NOTEBOOK.to_string(),
        );
        assert!(round_request(&spec).session);
    }

    #[test]
    fn credits_dht_holder() {
        // A registered copy joins the source endpoint as a holder, repeats are
        // dropped, and the list stops at the bound the planner accepts.
        let (config, _) = realm(4);
        let source = node(1);
        let mut holders = vec![InputHolder {
            subject: holder_subject(&config, source),
            node_id: source,
        }];

        merge_holders(&config, &mut holders, vec![source, node(2), node(3)]);

        assert_eq!(
            holders
                .iter()
                .map(|holder| holder.node_id)
                .collect::<Vec<_>>(),
            vec![source, node(2), node(3)]
        );
        assert_eq!(holders[1].subject.node_id, node(2));
        merge_holders(
            &config,
            &mut holders,
            (10..=250u8).map(node).collect::<Vec<_>>(),
        );
        assert_eq!(holders.len(), MAX_INPUT_HOLDERS);
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

    /// Answers every call of one round: no member advertises, no copy is registered, no
    /// policy resolves and no policy row exists.
    fn quiet_round(operation: &mut BuildPlanOperation) -> (usize, ExecutionPlan) {
        use aruna_core::calls::ChildOutcome;
        let mut turn = operation.first();
        let mut unread = 0;
        loop {
            let call = match turn.expect("the round continues") {
                Next::Call(call) => call,
                Next::Done(plan) => return (unread, plan),
                other => panic!("unexpected turn {other:?}"),
            };
            let answer = match (&operation.step, call) {
                (PlanStep::Advertisement(_), IoCall::Child(_)) => {
                    unread += 1;
                    let error = NodeInfoError::Decode("unreadable".to_string());
                    IoResult::Child(ChildOutcome::new(Err::<
                        BTreeMap<NodeId, NodeInfoDocument>,
                        _,
                    >(error)))
                }
                (PlanStep::Holders(_), IoCall::Child(_)) => IoResult::Child(ChildOutcome::new(
                    Ok::<_, GetHoldersError>(Vec::<NodeId>::new()),
                )),
                (PlanStep::Policies, IoCall::Child(_)) => {
                    let resolved = Ok::<_, PolicySetError>(Resolutions::new());
                    IoResult::Child(ChildOutcome::new(resolved))
                }
                (_, IoCall::Read(reads)) => IoResult::Values(vec![None; reads.len()]),
                (step, call) => panic!("unexpected call {call:?} in {step:?}"),
            };
            turn = operation.next(answer);
        }
    }

    // A round reads every member's advertisement; one it cannot read leaves the round retryable.
    #[test]
    fn unread_round_retries() {
        let family = Family::new([4u8; 32]);
        let config = family.config.clone();
        let mut operation = BuildPlanOperation::new(round(&config, &family.spec(), &[]));
        let (unread, plan) = quiet_round(&mut operation);
        assert_eq!(unread, config.sync_eligible_nodes().expect("members").len());
        assert!(unread > 0 && plan.selected.is_none() && plan.retryable);
    }
}
