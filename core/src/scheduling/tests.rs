use super::*;
use crate::NodeId;
use crate::compute::{
    ExecutorAvailability, ExecutorCapability, NetworkAccess, ResourceEnvelope, StagingMode,
};
use crate::structs::{
    EffectiveResources, LabelMatch, LocationLink, PlacementPolicy, PlacementPolicyRef,
    PlacementSelector, PlacementSubject, PolicyResolution, RealmNodeKind, SubmissionId,
    VerifiedPolicy, VersionedObjectArn,
};
use std::collections::BTreeMap;
use ulid::Ulid;

pub(crate) const NOW_MS: u64 = 1_700_000_000_000;

pub(crate) fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

pub(crate) fn subject(node_id: NodeId, location: &str) -> PlacementSubject {
    PlacementSubject {
        node_id,
        generation: 3,
        location: location.to_string(),
        labels: BTreeMap::new(),
        executor_kind: None,
        local_to_controller: true,
    }
}

pub(crate) fn candidate(node_id: NodeId, kind: &str) -> TargetCandidate {
    candidate_at(node_id, kind, "eu")
}

pub(crate) fn candidate_at(node_id: NodeId, kind: &str, location: &str) -> TargetCandidate {
    let mut capability = ExecutorCapability::new(kind.to_string(), subject(node_id, location))
        .expect("subject is valid");
    capability.file_staging = true;
    TargetCandidate {
        node_id,
        node_kind: RealmNodeKind::Server,
        active: true,
        compute_draining: false,
        group_allowed: true,
        capability,
        load_permille: Some(100),
    }
}

/// Re-seals the advertised digest after a fixture changed the subject.
pub(crate) fn reseal(candidate: &mut TargetCandidate) {
    candidate.capability.subject_digest = candidate
        .capability
        .subject
        .digest()
        .expect("subject is valid");
}

pub(crate) fn holder(node_id: NodeId, location: &str) -> InputHolder {
    InputHolder {
        node_id,
        subject: subject(node_id, location),
    }
}

pub(crate) fn resolved_input(key: &str, seed: u8) -> ResolvedInput {
    let version = Ulid::from_bytes([seed; 16]);
    ResolvedInput {
        destination_key: key.to_string(),
        source: VersionedObjectArn {
            realm_id: crate::structs::RealmId::from_bytes([1u8; 32]),
            node_id: node(1),
            bucket: "bucket".to_string(),
            key: key.to_string(),
            version,
        },
        version_id: version,
        blake3: [seed; 32],
        bytes: 1_000,
        policies: Vec::new(),
        holders: Vec::new(),
    }
}

pub(crate) fn request(inputs: Vec<ResolvedInput>) -> PlanRequest {
    PlanRequest {
        submission_id: SubmissionId([7u8; 32]),
        request_digest: [8u8; 32],
        spec_digest: [9u8; 32],
        admitted: true,
        resources: EffectiveResources {
            cpu_cores: 2,
            ram_bytes: 1_024,
            disk_bytes: 2_048,
            max_walltime_ms: 60_000,
            preemptible: false,
        },
        executor_constraint: None,
        required_labels: Vec::new(),
        staging: StagingMode::Files,
        network: NetworkAccess::Isolated,
        inputs,
        output_policies: Vec::new(),
        policies: BTreeMap::new(),
        now_ms: NOW_MS,
    }
}

pub(crate) fn policy(seed: u8, selector: PlacementSelector) -> VerifiedPolicy {
    VerifiedPolicy::verify(
        PlacementPolicy::new(
            Ulid::from_bytes([seed; 16]),
            format!("policy-{seed}"),
            vec![selector],
        )
        .expect("policy is valid"),
    )
    .expect("policy is canonical")
}

pub(crate) fn selector() -> PlacementSelector {
    PlacementSelector {
        node_id: None,
        location: None,
        labels: Vec::new(),
        executor_kind: None,
    }
}

/// Registers `policy` as locally verified and returns its ref.
pub(crate) fn known(request: &mut PlanRequest, policy: &VerifiedPolicy) -> PlacementPolicyRef {
    request.policies.insert(
        policy.policy().policy_id,
        PolicyResolution::Known(policy.clone()),
    );
    policy.policy_ref()
}

pub(crate) fn config(links: Vec<LocationLink>) -> RealmComputeConfig {
    RealmComputeConfig {
        links,
        ..Default::default()
    }
}

pub(crate) fn link(from: &str, to: &str, bandwidth: u64) -> LocationLink {
    LocationLink {
        from: from.to_string(),
        to: to.to_string(),
        bandwidth_bytes_per_sec: bandwidth,
    }
}

/// `count` advertisements, in the canonical order the planner ranks them in.
fn scanned(count: u8) -> Vec<TargetCandidate> {
    let mut targets: Vec<_> = (1..=count)
        .map(|seed| candidate(node(seed), "docker"))
        .collect();
    targets.sort_by(|left, right| left.node_id.as_bytes().cmp(right.node_id.as_bytes()));
    targets
}

/// `nodes` nodes advertising eight backends each, the realistic shape that
/// overruns one page, in canonical scan order.
fn backends(nodes: u16) -> Vec<TargetCandidate> {
    let mut targets: Vec<_> = (0..nodes)
        .flat_map(|seed| {
            let node_id = node(u8::try_from(seed).expect("at most 256 nodes"));
            (0..8).map(move |kind| candidate(node_id, &format!("k{kind}")))
        })
        .collect();
    targets.sort_by(|left, right| {
        (left.node_id.as_bytes(), &left.capability.kind)
            .cmp(&(right.node_id.as_bytes(), &right.capability.kind))
    });
    targets
}

/// One page-by-page run over `candidates`, in the canonical order discovery
/// walks them in.
fn paged<'a>(
    request: &PlanRequest,
    candidates: &[TargetCandidate],
    compute: &'a RealmComputeConfig,
) -> Planner<'a> {
    let mut planner = Planner::new(request, compute).expect("request is well formed");
    for page in candidates.chunks(MAX_TARGET_SCAN) {
        planner.rank_page(page).expect("pages are ordered");
    }
    planner
}

fn plan(
    request: &PlanRequest,
    candidates: Vec<TargetCandidate>,
    compute: &RealmComputeConfig,
) -> ExecutionPlan {
    plan_execution(request, &candidates, compute).expect("request is well formed")
}

fn selected(
    request: &PlanRequest,
    candidates: Vec<TargetCandidate>,
    compute: &RealmComputeConfig,
) -> Selection {
    plan(request, candidates, compute)
        .selected
        .expect("a target is legal")
}

fn verdict(plan: &ExecutionPlan, node_id: NodeId) -> RejectionVerdict {
    plan.rejected
        .iter()
        .find(|rejection| rejection.target.node_id == node_id)
        .map(|rejection| rejection.verdict.clone())
        .expect("target was screened")
}

#[test]
fn policy_selects_site() {
    // Exact node, location, label, and executor-kind selectors each pin one of
    // two otherwise identical targets.
    let (first, second) = (node(2), node(3));
    let mut labelled = candidate(second, "docker");
    labelled
        .capability
        .subject
        .labels
        .insert("tier".to_string(), "gpu".to_string());
    reseal(&mut labelled);

    let cases = [
        PlacementSelector {
            node_id: Some(second),
            ..selector()
        },
        PlacementSelector {
            location: Some("us".to_string()),
            ..selector()
        },
        PlacementSelector {
            labels: vec![LabelMatch {
                key: "tier".to_string(),
                value: "gpu".to_string(),
            }],
            ..selector()
        },
        PlacementSelector {
            executor_kind: Some("apptainer".to_string()),
            ..selector()
        },
    ];
    let targets = [
        candidate(second, "docker"),
        candidate_at(second, "docker", "us"),
        labelled,
        candidate(second, "apptainer"),
    ];

    for (selector, target) in cases.into_iter().zip(targets) {
        let mut plan_request = request(Vec::new());
        let policy = policy(4, selector);
        plan_request.output_policies = vec![known(&mut plan_request, &policy)];
        let scan = vec![candidate(first, "docker"), target.clone()];

        let selection = selected(&plan_request, scan, &config(Vec::new()));
        assert_eq!(selection.target, target.capability.target(second));
    }
}

#[test]
fn local_copy_wins() {
    // A zero-cost local copy beats a remote holder on a fast link.
    let (local, remote) = (node(2), node(3));
    let mut input = resolved_input("in", 1);
    input.holders = vec![holder(local, "eu"), holder(node(4), "us")];
    let plan_request = request(vec![input]);

    let selection = selected(
        &plan_request,
        vec![
            candidate(local, "docker"),
            candidate_at(remote, "docker", "us"),
        ],
        &config(vec![link("us", "us", 1_000_000_000), link("eu", "eu", 1)]),
    );

    assert_eq!(selection.target.node_id, local);
    assert_eq!(selection.score.estimated_transfer_ms, 0);
    assert_eq!(selection.score.transfer_bytes, 0);
    assert_eq!(selection.inputs[0].source_node_id, None);
}

#[test]
fn cheapest_holder_wins() {
    // The cheapest directional link decides which legal holder is staged from.
    let target = node(2);
    let mut input = resolved_input("in", 1);
    input.holders = vec![holder(node(5), "us"), holder(node(6), "ap")];
    let plan_request = request(vec![input]);

    let selection = selected(
        &plan_request,
        vec![candidate(target, "docker")],
        &config(vec![link("us", "eu", 1_000), link("ap", "eu", 4_000)]),
    );

    assert_eq!(selection.inputs[0].source_node_id, Some(node(6)));
    assert_eq!(selection.score.estimated_transfer_ms, 250);
    assert_eq!(selection.score.transfer_bytes, 1_000);
    assert_eq!(selection.score.unknown_link_count, 0);
}

#[test]
fn skips_noncompliant_holder() {
    // A faster holder that the input policy does not allow is never scored,
    // and a candidate left without any legal source becomes unavailable.
    let target = node(2);
    let policy = policy(
        4,
        PlacementSelector {
            location: Some("eu".to_string()),
            ..selector()
        },
    );
    let mut plan_request = request(Vec::new());
    let policy_ref = known(&mut plan_request, &policy);
    let mut input = resolved_input("in", 1);
    input.policies = vec![policy_ref];
    input.holders = vec![holder(node(5), "us"), holder(node(6), "eu")];
    plan_request.inputs = vec![input.clone()];

    let selection = selected(
        &plan_request,
        vec![candidate(target, "docker")],
        &config(vec![link("us", "eu", 1_000_000), link("eu", "eu", 1_000)]),
    );
    assert_eq!(selection.inputs[0].source_node_id, Some(node(6)));
    assert_eq!(selection.score.estimated_transfer_ms, 1_000);

    let mut without = plan_request.clone();
    without.inputs[0].holders = vec![holder(node(5), "us")];
    let outcome = plan(
        &without,
        vec![candidate(target, "docker")],
        &config(Vec::new()),
    );
    assert!(outcome.selected.is_none() && outcome.retryable);
    assert_eq!(
        verdict(&outcome, target),
        RejectionVerdict::NoLegalSource {
            destination_key: "in".to_string()
        }
    );
}

#[test]
fn unknown_link_costs() {
    // A missing link stays size-sensitive and pessimistic instead of free.
    let target = node(2);
    let mut small = resolved_input("in", 1);
    small.holders = vec![holder(node(5), "us")];
    let mut large = small.clone();
    large.bytes = 10_000_000;
    let mut plan_request = request(vec![small]);
    let compute = config(Vec::new());
    let scan = vec![candidate(target, "docker")];

    let cheap = selected(&plan_request, scan.clone(), &compute);
    plan_request.inputs = vec![large];
    let expensive = selected(&plan_request, scan, &compute);

    assert_eq!(cheap.score.unknown_link_count, 1);
    assert!(expensive.score.estimated_transfer_ms > cheap.score.estimated_transfer_ms);
    assert_eq!(expensive.score.transfer_bytes, 10_000_000);
}

#[test]
fn saturates_transfer_cost() {
    // An enormous input must saturate instead of wrapping into a cheap plan.
    let target = node(2);
    let mut input = resolved_input("in", 1);
    input.bytes = u64::MAX;
    input.holders = vec![holder(node(5), "us")];
    let plan_request = request(vec![input]);

    let selection = selected(
        &plan_request,
        vec![candidate(target, "docker")],
        &config(vec![link("us", "eu", 1)]),
    );
    assert_eq!(selection.score.estimated_transfer_ms, u64::MAX);
}

#[test]
fn accepts_input_bound() {
    // The planner accepts the same 512-input limit as the public APIs.
    let inputs = (0..MAX_PLAN_INPUTS)
        .map(|index| resolved_input(&format!("input-{index}"), (index % 255) as u8 + 1))
        .collect();
    assert!(request(inputs).canonical().is_ok());
}

#[test]
fn envelope_rejects_target() {
    // Static ceilings hard-filter; free capacity never does.
    let target = node(2);
    let mut small = candidate(target, "docker");
    small.capability.limits = ResourceEnvelope {
        max_cpu_cores: Some(1),
        ..Default::default()
    };
    let plan_request = request(Vec::new());

    let outcome = plan(&plan_request, vec![small], &config(Vec::new()));
    assert!(outcome.selected.is_none() && !outcome.retryable);
    assert_eq!(verdict(&outcome, target), RejectionVerdict::Resources);
}

#[test]
fn stale_hints_only_rank() {
    // Exhausted and stale telemetry lower the rank of the only target but
    // never deny it, because exact admission belongs to the target.
    let target = node(2);
    let mut busy = candidate(target, "docker");
    busy.capability.limits = ResourceEnvelope {
        max_cpu_cores: Some(8),
        max_concurrent: Some(4),
        ..Default::default()
    };
    busy.capability.availability = Some(ExecutorAvailability {
        free_cpu_cores: Some(0),
        free_ram_bytes: None,
        free_disk_bytes: None,
        active_executions: 4,
        observed_at_ms: NOW_MS,
    });
    busy.load_permille = Some(990);
    let plan_request = request(Vec::new());

    let loaded = selected(&plan_request, vec![busy.clone()], &config(Vec::new()));
    assert_eq!(
        loaded.score.availability_pressure_permille,
        UNKNOWN_PERMILLE
    );
    assert_eq!(loaded.score.node_load_permille, 990);

    let mut idle = busy.clone();
    idle.capability.availability = Some(ExecutorAvailability {
        free_cpu_cores: Some(8),
        active_executions: 0,
        ..idle
            .capability
            .availability
            .expect("fixture reports availability")
    });
    assert_eq!(
        selected(&plan_request, vec![idle], &config(Vec::new()))
            .score
            .availability_pressure_permille,
        0
    );

    // An observation older than the configured bound counts as unknown.
    let mut old = busy;
    old.capability.availability = Some(ExecutorAvailability {
        free_cpu_cores: Some(8),
        active_executions: 0,
        observed_at_ms: NOW_MS - 600_000,
        free_ram_bytes: None,
        free_disk_bytes: None,
    });
    assert_eq!(
        selected(&plan_request, vec![old], &config(Vec::new()))
            .score
            .availability_pressure_permille,
        UNKNOWN_PERMILLE
    );
}

#[test]
fn drops_draining_targets() {
    // Administrative and policy draining both remove a target.
    let (administrative, transitioning, healthy) = (node(2), node(3), node(4));
    let mut drained = candidate(administrative, "docker");
    drained.compute_draining = true;
    let mut policy_drained = candidate(transitioning, "docker");
    policy_drained.capability.policy_draining = true;
    let plan_request = request(Vec::new());

    let outcome = plan(
        &plan_request,
        vec![drained, policy_drained, candidate(healthy, "docker")],
        &config(Vec::new()),
    );

    assert_eq!(
        outcome
            .selected
            .as_ref()
            .expect("healthy target remains")
            .target
            .node_id,
        healthy
    );
    assert_eq!(
        verdict(&outcome, administrative),
        RejectionVerdict::ComputeDraining
    );
    assert_eq!(
        verdict(&outcome, transitioning),
        RejectionVerdict::PolicyDraining
    );
}

#[test]
fn excludes_user_nodes() {
    // User nodes never take cross-node work, and a departed or unauthorized
    // controller is out regardless of what it advertises.
    let plan_request = request(Vec::new());
    let mut user = candidate(node(3), "docker");
    user.node_kind = RealmNodeKind::User;
    let mut inactive = candidate(node(4), "docker");
    inactive.active = false;
    let mut unauthorized = candidate(node(5), "docker");
    unauthorized.group_allowed = false;

    let outcome = plan(
        &plan_request,
        vec![user, inactive, unauthorized],
        &config(Vec::new()),
    );

    assert!(outcome.selected.is_none() && !outcome.retryable);
    assert_eq!(verdict(&outcome, node(3)), RejectionVerdict::NodeKind);
    assert_eq!(verdict(&outcome, node(4)), RejectionVerdict::Inactive);
    assert_eq!(verdict(&outcome, node(5)), RejectionVerdict::NotAuthorized);
}

#[test]
fn drops_drifted_subject() {
    // An advertisement whose digest does not match its subject is drift: the
    // sealed generation no longer describes the site policy was evaluated on.
    let target = node(2);
    let mut drifted = candidate(target, "docker");
    drifted.capability.subject.generation += 1;
    let mut foreign = candidate(node(3), "docker");
    foreign.capability.subject.node_id = node(9);
    reseal(&mut foreign);
    let plan_request = request(Vec::new());

    let outcome = plan(&plan_request, vec![drifted, foreign], &config(Vec::new()));

    assert!(outcome.selected.is_none());
    assert_eq!(verdict(&outcome, target), RejectionVerdict::SubjectDrift);
    assert_eq!(verdict(&outcome, node(3)), RejectionVerdict::SubjectDrift);
}

#[test]
fn filters_request_constraints() {
    // Executor kind, staging mode, and required labels are request-level hard
    // constraints, independent of any policy.
    let scan = vec![candidate(node(2), "docker")];
    let mut plan_request = request(Vec::new());
    plan_request.executor_constraint = Some("apptainer".to_string());
    assert_eq!(
        verdict(
            &plan(&plan_request, scan.clone(), &config(Vec::new())),
            node(2)
        ),
        RejectionVerdict::ExecutorKind
    );

    let mut staging = request(Vec::new());
    staging.staging = StagingMode::S3Mount;
    assert_eq!(
        verdict(&plan(&staging, scan.clone(), &config(Vec::new())), node(2)),
        RejectionVerdict::Staging
    );

    let mut labelled = request(Vec::new());
    labelled.required_labels = vec![LabelMatch {
        key: "tier".to_string(),
        value: "gpu".to_string(),
    }];
    assert_eq!(
        verdict(&plan(&labelled, scan, &config(Vec::new())), node(2)),
        RejectionVerdict::RequiredLabels
    );
}

#[test]
fn guards_open_network() {
    // Protected data may only run with open networking where the site enforces
    // network policy; unprotected data is unaffected.
    let target = node(2);
    let mut plan_request = request(Vec::new());
    let policy = policy(
        4,
        PlacementSelector {
            location: Some("eu".to_string()),
            ..selector()
        },
    );
    plan_request.output_policies = vec![known(&mut plan_request, &policy)];
    plan_request.network = NetworkAccess::Open;
    let scan = vec![candidate(target, "docker")];

    assert_eq!(
        verdict(
            &plan(&plan_request, scan.clone(), &config(Vec::new())),
            target
        ),
        RejectionVerdict::OpenNetwork
    );

    let mut enforcing = scan.clone();
    enforcing[0].capability.network_policy = true;
    assert!(
        plan(&plan_request, enforcing, &config(Vec::new()))
            .selected
            .is_some()
    );

    let mut unprotected = plan_request;
    unprotected.output_policies = Vec::new();
    assert!(
        plan(&unprotected, scan, &config(Vec::new()))
            .selected
            .is_some()
    );
}

#[test]
fn reports_policy_gaps() {
    // An unresolved policy blocks and stays retryable; a denial does not.
    let target = node(2);
    let policy = policy(
        4,
        PlacementSelector {
            location: Some("us".to_string()),
            ..selector()
        },
    );
    let mut missing = request(Vec::new());
    missing.output_policies = vec![policy.policy_ref()];
    let scan = vec![candidate(target, "docker")];

    let outcome = plan(&missing, scan.clone(), &config(Vec::new()));
    assert!(outcome.retryable);
    assert_eq!(
        verdict(&outcome, target),
        RejectionVerdict::Policy {
            verdict: PolicyVerdict::Required,
            policy_ids: vec![policy.policy().policy_id],
        }
    );

    let mut denied = missing.clone();
    known(&mut denied, &policy);
    let outcome = plan(&denied, scan, &config(Vec::new()));
    assert!(!outcome.retryable);
    assert_eq!(
        verdict(&outcome, target),
        RejectionVerdict::Policy {
            verdict: PolicyVerdict::Denied,
            policy_ids: vec![policy.policy().policy_id],
        }
    );
}

#[test]
fn ties_break_by_node() {
    // Equal scores fall back to node id bytes, then executor kind.
    let (low, high) = (node(2), node(3));
    let (low, high) = match low.as_bytes() < high.as_bytes() {
        true => (low, high),
        false => (high, low),
    };
    let plan_request = request(Vec::new());

    let outcome = plan(
        &plan_request,
        vec![
            candidate(high, "docker"),
            candidate(low, "docker"),
            candidate(low, "apptainer"),
        ],
        &config(Vec::new()),
    );
    let selection = outcome.selected.clone().expect("a target is legal");

    assert_eq!(selection.target.node_id, low);
    assert_eq!(selection.target.executor_kind, "apptainer");
    assert_eq!(outcome.alternatives.len(), 2);
    assert_eq!(outcome.alternatives[0].target.executor_kind, "docker");
    assert_eq!(outcome.alternatives[1].target.node_id, high);
}

#[test]
fn shuffled_facts_agree() {
    // Permuted inputs, holders, and candidates must seal the same digest.
    let mut first = resolved_input("a", 1);
    first.holders = vec![holder(node(5), "us"), holder(node(6), "ap")];
    let mut second = resolved_input("b", 2);
    second.holders = vec![holder(node(6), "ap"), holder(node(5), "us")];
    let compute = config(vec![link("us", "eu", 1_000), link("ap", "eu", 2_000)]);

    let forward = request(vec![first.clone(), second.clone()]);
    let mut reverse = request(vec![second, first]);
    reverse.inputs[0].holders.reverse();

    assert_eq!(
        selected(
            &forward,
            vec![candidate(node(2), "docker"), candidate(node(3), "docker")],
            &compute
        ),
        selected(
            &reverse,
            vec![candidate(node(3), "docker"), candidate(node(2), "docker")],
            &compute
        )
    );
}

#[test]
fn digest_seals_facts() {
    // Every sealed fact must move the digest: subject generation, input size,
    // chosen source, and the target itself.
    let target = node(2);
    let mut input = resolved_input("in", 1);
    input.holders = vec![holder(node(5), "us")];
    let plan_request = request(vec![input]);
    let scan = vec![candidate(target, "docker")];
    let compute = config(vec![link("us", "eu", 1_000)]);
    let base = selected(&plan_request, scan.clone(), &compute).plan_digest;

    let mut resized = plan_request.clone();
    resized.inputs[0].bytes += 1;
    assert_ne!(selected(&resized, scan.clone(), &compute).plan_digest, base);

    let mut regenerated = scan.clone();
    regenerated[0].capability.subject.generation += 1;
    reseal(&mut regenerated[0]);
    assert_ne!(
        selected(&plan_request, regenerated, &compute).plan_digest,
        base
    );

    let mut moved = plan_request.clone();
    moved.inputs[0].holders = vec![holder(node(6), "us")];
    assert_ne!(selected(&moved, scan, &compute).plan_digest, base);

    let elsewhere = vec![candidate(node(3), "docker")];
    assert_ne!(
        selected(&plan_request, elsewhere, &compute).plan_digest,
        base
    );
}

#[test]
fn rejects_invalid_config() {
    // An unusable link table must fail closed instead of ranking on it.
    let plan_request = request(Vec::new());
    assert!(
        plan_execution(
            &plan_request,
            &[candidate(node(2), "docker")],
            &config(vec![link("eu", "us", 0)])
        )
        .is_err()
    );
}

#[test]
fn beyond_bound_selected() {
    // A drained prefix as long as the ranking bound must not hide the one
    // legal target the scan found behind it.
    let mut targets = scanned(129);
    let legal = targets.last().expect("the scan is not empty").node_id;
    for target in targets.iter_mut().take(MAX_PLAN_CANDIDATES) {
        target.compute_draining = true;
    }
    let plan_request = request(Vec::new());

    let outcome = plan(&plan_request, targets, &config(Vec::new()));

    assert_eq!(
        outcome.selected.expect("a target is legal").target.node_id,
        legal
    );
    assert_eq!(outcome.rejected.len(), MAX_PLAN_REJECTIONS);
    assert_eq!(
        outcome.omitted,
        (MAX_PLAN_CANDIDATES - MAX_PLAN_REJECTIONS) as u32
    );
}

#[test]
fn best_beyond_bound() {
    // Ranking keeps the best of the whole scan, not the first bound worth of
    // it, so the cheapest target past that bound still wins.
    let mut targets = scanned(129);
    let last = targets.len() - 1;
    targets[last].load_permille = Some(1);
    let best = targets[last].node_id;
    let plan_request = request(Vec::new());

    let outcome = plan(&plan_request, targets, &config(Vec::new()));
    let selection = outcome.selected.expect("a target is legal");

    assert_eq!(selection.target.node_id, best);
    assert_eq!(selection.score.node_load_permille, 1);
    assert_eq!(outcome.alternatives.len(), MAX_PLAN_ALTERNATIVES);
}

#[test]
fn shuffled_scan_agrees() {
    // One scan in either order must seal the same selection, alternatives, and
    // digest, including which targets the ranking bound drops.
    let mut targets = scanned(130);
    for (index, target) in targets.iter_mut().enumerate() {
        target.load_permille = Some((index as u32 * 37) % 11);
    }
    let plan_request = request(Vec::new());
    let reverse: Vec<_> = targets.iter().rev().cloned().collect();
    let compute = config(Vec::new());

    let first = plan(&plan_request, targets, &compute);
    let second = plan(&plan_request, reverse, &compute);

    assert_eq!(first.selected, second.selected);
    assert_eq!(first.alternatives, second.alternatives);
    assert_eq!(first.rejected, second.rejected);
}

#[test]
fn incomplete_scan_retries() {
    // A round that could not read every advertisement may still be missing the
    // only legal target, so it stays retryable instead of reading conclusive.
    let mut drained = candidate(node(2), "docker");
    drained.compute_draining = true;
    let plan_request = request(Vec::new());
    let compute = config(Vec::new());

    let conclusive = paged(&plan_request, &[drained.clone()], &compute).finish(false);
    assert!(conclusive.selected.is_none() && !conclusive.retryable);

    let continued = paged(&plan_request, &[drained.clone()], &compute).finish(true);
    assert!(continued.selected.is_none() && continued.retryable);

    // A selection ends the round whatever the scan left unread.
    let scan = vec![drained, candidate(node(3), "docker")];
    let launched = paged(&plan_request, &scan, &compute).finish(true);
    assert!(launched.selected.is_some() && !launched.retryable);
}

#[test]
fn pages_past_bound() {
    // The page bound is separate from the scan it screens and from the ranked
    // set it feeds: a scan past either bound is ordinary and fully screened.
    let plan_request = request(Vec::new());
    let compute = config(Vec::new());
    let targets = backends(129);
    assert!(targets.len() > MAX_TARGET_SCAN);

    let planner = paged(&plan_request, &targets, &compute);
    assert_eq!(planner.scanned(), targets.len() as u64);
    assert!(
        plan_execution(&plan_request, &targets, &compute)
            .expect("a scan may exceed one page")
            .selected
            .is_some()
    );
}

#[test]
fn late_target_selected() {
    // The single eligible advertisement sits past the first page: page one
    // alone is never a conclusive refusal, and the second page selects it.
    let mut targets = backends(129);
    targets.truncate(MAX_TARGET_SCAN + 1);
    for target in targets.iter_mut().take(MAX_TARGET_SCAN) {
        target.compute_draining = true;
    }
    let legal = targets[MAX_TARGET_SCAN]
        .capability
        .target(targets[MAX_TARGET_SCAN].node_id);
    let plan_request = request(Vec::new());
    let compute = config(Vec::new());

    let unfinished = paged(&plan_request, &targets[..MAX_TARGET_SCAN], &compute).finish(true);
    assert!(unfinished.selected.is_none() && unfinished.retryable);

    let mut planner = Planner::new(&plan_request, &compute).expect("request is well formed");
    planner
        .rank_page(&targets[..MAX_TARGET_SCAN])
        .expect("the page is ordered");
    let boundary = &targets[MAX_TARGET_SCAN - 1];
    assert_eq!(
        planner.cursor(),
        Some(&boundary.capability.target(boundary.node_id))
    );

    planner
        .rank_page(&targets[MAX_TARGET_SCAN..])
        .expect("the page continues the scan");
    assert_eq!(planner.pages(), 2);
    assert_eq!(planner.scanned(), MAX_TARGET_SCAN as u64 + 1);
    let outcome = planner.finish(false);

    assert_eq!(outcome.selected.expect("a target is legal").target, legal);
    assert_eq!(outcome.rejected.len(), MAX_PLAN_REJECTIONS);
    assert_eq!(
        outcome.omitted,
        (MAX_TARGET_SCAN - MAX_PLAN_REJECTIONS) as u32
    );
}

#[test]
fn best_after_page() {
    // A cheaper target on the second page must win over the eligible one the
    // first page already found, which stays as an alternative.
    let mut targets = backends(129);
    targets.truncate(MAX_TARGET_SCAN + 1);
    for target in targets.iter_mut().take(MAX_TARGET_SCAN) {
        target.compute_draining = true;
    }
    targets[0].compute_draining = false;
    targets[0].load_permille = Some(500);
    targets[MAX_TARGET_SCAN].load_permille = Some(1);
    let plan_request = request(Vec::new());
    let compute = config(Vec::new());

    let outcome = paged(&plan_request, &targets, &compute).finish(false);
    let selection = outcome.selected.expect("a target is legal");

    assert_eq!(selection.score.node_load_permille, 1);
    assert_eq!(
        selection.target,
        targets[MAX_TARGET_SCAN]
            .capability
            .target(targets[MAX_TARGET_SCAN].node_id)
    );
    assert_eq!(outcome.alternatives.len(), 1);
    assert_eq!(
        outcome.alternatives[0].target,
        targets[0].capability.target(targets[0].node_id)
    );
}

#[test]
fn realm_backends_paged() {
    // 129 nodes advertising eight backends each overrun one page, and the only
    // eligible target of the realm sits in the overrun.
    let mut targets = backends(129);
    let last = targets.len() - 1;
    assert!(last >= MAX_TARGET_SCAN);
    for target in targets.iter_mut().take(last) {
        target.compute_draining = true;
    }
    let legal = targets[last].capability.target(targets[last].node_id);
    let plan_request = request(Vec::new());
    let compute = config(Vec::new());

    let planner = paged(&plan_request, &targets, &compute);
    assert_eq!(planner.pages(), 2);
    assert_eq!(planner.scanned(), 1_032);
    let outcome = planner.finish(false);

    assert_eq!(outcome.selected.expect("a target is legal").target, legal);
}

#[test]
fn shuffled_pages_agree() {
    // The same advertisements in any discovery order must page identically and
    // seal one plan, digest included.
    let mut targets = backends(129);
    for (index, target) in targets.iter_mut().enumerate() {
        target.load_permille = Some((index as u32 * 37) % 11);
    }
    let reverse: Vec<_> = targets.iter().rev().cloned().collect();
    let plan_request = request(Vec::new());
    let compute = config(Vec::new());

    let forward = plan(&plan_request, targets.clone(), &compute);
    let shuffled = plan(&plan_request, reverse, &compute);
    let walked = paged(&plan_request, &targets, &compute).finish(false);

    assert_eq!(forward, shuffled);
    assert_eq!(forward, walked);
    assert!(forward.selected.is_some());
}

#[test]
fn page_bounds_exact() {
    // Page boundaries land exactly on the bound: a full page is one page, one
    // entry more is two, and a scan of exactly two pages grows no empty third.
    let plan_request = request(Vec::new());
    let compute = config(Vec::new());
    let targets = backends(256);
    assert_eq!(targets.len(), 2 * MAX_TARGET_SCAN);

    let full = paged(&plan_request, &targets[..MAX_TARGET_SCAN], &compute);
    assert_eq!((full.pages(), full.scanned()), (1, MAX_TARGET_SCAN as u64));

    let over = paged(&plan_request, &targets[..MAX_TARGET_SCAN + 1], &compute);
    assert_eq!(
        (over.pages(), over.scanned()),
        (2, MAX_TARGET_SCAN as u64 + 1)
    );

    let mut exact = paged(&plan_request, &targets, &compute);
    assert_eq!(
        (exact.pages(), exact.scanned()),
        (2, 2 * MAX_TARGET_SCAN as u64)
    );
    exact.rank_page(&[]).expect("an empty page is allowed");
    assert_eq!(exact.pages(), 2);

    // Nothing may be screened twice, so the page just ranked is refused.
    assert!(matches!(
        exact.rank_page(&targets[MAX_TARGET_SCAN..]),
        Err(PlanError::PageOrder { .. })
    ));
}

#[test]
fn paged_scan_conclusive() {
    // Only a scan that reached its last page may answer "no eligible target".
    let mut targets = backends(129);
    for target in &mut targets {
        target.compute_draining = true;
    }
    let plan_request = request(Vec::new());
    let compute = config(Vec::new());

    let unfinished = paged(&plan_request, &targets[..MAX_TARGET_SCAN], &compute).finish(true);
    assert!(unfinished.selected.is_none() && unfinished.retryable);

    let outcome = paged(&plan_request, &targets, &compute).finish(false);

    assert!(outcome.selected.is_none() && !outcome.retryable);
    assert_eq!(
        outcome.omitted,
        (targets.len() - MAX_PLAN_REJECTIONS) as u32
    );
}

#[test]
fn retry_scans_pages() {
    // A repeated round rescans every page instead of stopping where the last
    // one did, so two rounds over unchanged advertisements plan identically.
    let mut targets = backends(129);
    let last = targets.len() - 1;
    targets[last].load_permille = Some(1);
    let plan_request = request(Vec::new());
    let compute = config(Vec::new());

    let first = paged(&plan_request, &targets, &compute);
    let second = paged(&plan_request, &targets, &compute);

    assert_eq!(first.scanned(), targets.len() as u64);
    assert_eq!(second.scanned(), targets.len() as u64);
    let (first, second) = (first.finish(false), second.finish(false));
    assert_eq!(first, second);
    assert_eq!(
        first.selected.expect("a target is legal").target,
        targets[last].capability.target(targets[last].node_id)
    );
}
