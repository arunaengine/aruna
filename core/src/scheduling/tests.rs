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
        candidates: Vec::new(),
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

fn plan(request: &PlanRequest, compute: &RealmComputeConfig) -> ExecutionPlan {
    plan_execution(request, compute).expect("request is well formed")
}

fn selected(request: &PlanRequest, compute: &RealmComputeConfig) -> Selection {
    plan(request, compute).selected.expect("a target is legal")
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
        plan_request.candidates = vec![candidate(first, "docker"), target.clone()];

        let selection = selected(&plan_request, &config(Vec::new()));
        assert_eq!(selection.target, target.capability.target(second));
    }
}

#[test]
fn local_copy_wins() {
    // A zero-cost local copy beats a remote holder on a fast link.
    let (local, remote) = (node(2), node(3));
    let mut input = resolved_input("in", 1);
    input.holders = vec![holder(local, "eu"), holder(node(4), "us")];
    let mut plan_request = request(vec![input]);
    plan_request.candidates = vec![
        candidate(local, "docker"),
        candidate_at(remote, "docker", "us"),
    ];

    let selection = selected(
        &plan_request,
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
    let mut plan_request = request(vec![input]);
    plan_request.candidates = vec![candidate(target, "docker")];

    let selection = selected(
        &plan_request,
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
    plan_request.candidates = vec![candidate(target, "docker")];

    let selection = selected(
        &plan_request,
        &config(vec![link("us", "eu", 1_000_000), link("eu", "eu", 1_000)]),
    );
    assert_eq!(selection.inputs[0].source_node_id, Some(node(6)));
    assert_eq!(selection.score.estimated_transfer_ms, 1_000);

    let mut without = plan_request.clone();
    without.inputs[0].holders = vec![holder(node(5), "us")];
    let outcome = plan(&without, &config(Vec::new()));
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
    plan_request.candidates = vec![candidate(target, "docker")];
    let compute = config(Vec::new());

    let cheap = selected(&plan_request, &compute);
    plan_request.inputs = vec![large];
    let expensive = selected(&plan_request, &compute);

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
    let mut plan_request = request(vec![input]);
    plan_request.candidates = vec![candidate(target, "docker")];

    let selection = selected(&plan_request, &config(vec![link("us", "eu", 1)]));
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
    let mut plan_request = request(Vec::new());
    plan_request.candidates = vec![small];

    let outcome = plan(&plan_request, &config(Vec::new()));
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
    let mut plan_request = request(Vec::new());
    plan_request.candidates = vec![busy.clone()];

    let loaded = selected(&plan_request, &config(Vec::new()));
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
    plan_request.candidates = vec![idle];
    assert_eq!(
        selected(&plan_request, &config(Vec::new()))
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
    plan_request.candidates = vec![old];
    assert_eq!(
        selected(&plan_request, &config(Vec::new()))
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
    let mut plan_request = request(Vec::new());
    plan_request.candidates = vec![drained, policy_drained, candidate(healthy, "docker")];

    let outcome = plan(&plan_request, &config(Vec::new()));

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
fn excludes_local_nodes() {
    // Local and User nodes never take cross-node work, and a departed or
    // unauthorized controller is out regardless of what it advertises.
    let mut plan_request = request(Vec::new());
    let mut local = candidate(node(2), "docker");
    local.node_kind = RealmNodeKind::Local;
    let mut user = candidate(node(3), "docker");
    user.node_kind = RealmNodeKind::User;
    let mut inactive = candidate(node(4), "docker");
    inactive.active = false;
    let mut unauthorized = candidate(node(5), "docker");
    unauthorized.group_allowed = false;
    plan_request.candidates = vec![local, user, inactive, unauthorized];

    let outcome = plan(&plan_request, &config(Vec::new()));

    assert!(outcome.selected.is_none() && !outcome.retryable);
    assert_eq!(verdict(&outcome, node(2)), RejectionVerdict::NodeKind);
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
    let mut plan_request = request(Vec::new());
    plan_request.candidates = vec![drifted, foreign];

    let outcome = plan(&plan_request, &config(Vec::new()));

    assert!(outcome.selected.is_none());
    assert_eq!(verdict(&outcome, target), RejectionVerdict::SubjectDrift);
    assert_eq!(verdict(&outcome, node(3)), RejectionVerdict::SubjectDrift);
}

#[test]
fn filters_request_constraints() {
    // Executor kind, staging mode, and required labels are request-level hard
    // constraints, independent of any policy.
    let mut plan_request = request(Vec::new());
    plan_request.executor_constraint = Some("apptainer".to_string());
    plan_request.candidates = vec![candidate(node(2), "docker")];
    assert_eq!(
        verdict(&plan(&plan_request, &config(Vec::new())), node(2)),
        RejectionVerdict::ExecutorKind
    );

    let mut staging = request(Vec::new());
    staging.staging = StagingMode::S3Mount;
    staging.candidates = vec![candidate(node(2), "docker")];
    assert_eq!(
        verdict(&plan(&staging, &config(Vec::new())), node(2)),
        RejectionVerdict::Staging
    );

    let mut labelled = request(Vec::new());
    labelled.required_labels = vec![LabelMatch {
        key: "tier".to_string(),
        value: "gpu".to_string(),
    }];
    labelled.candidates = vec![candidate(node(2), "docker")];
    assert_eq!(
        verdict(&plan(&labelled, &config(Vec::new())), node(2)),
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
    plan_request.candidates = vec![candidate(target, "docker")];

    assert_eq!(
        verdict(&plan(&plan_request, &config(Vec::new())), target),
        RejectionVerdict::OpenNetwork
    );

    let mut enforcing = plan_request.clone();
    enforcing.candidates[0].capability.network_policy = true;
    assert!(plan(&enforcing, &config(Vec::new())).selected.is_some());

    let mut unprotected = plan_request;
    unprotected.output_policies = Vec::new();
    assert!(plan(&unprotected, &config(Vec::new())).selected.is_some());
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
    missing.candidates = vec![candidate(target, "docker")];

    let outcome = plan(&missing, &config(Vec::new()));
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
    let outcome = plan(&denied, &config(Vec::new()));
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
    let mut plan_request = request(Vec::new());
    plan_request.candidates = vec![
        candidate(high, "docker"),
        candidate(low, "docker"),
        candidate(low, "apptainer"),
    ];

    let outcome = plan(&plan_request, &config(Vec::new()));
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

    let mut forward = request(vec![first.clone(), second.clone()]);
    forward.candidates = vec![candidate(node(2), "docker"), candidate(node(3), "docker")];
    let mut reverse = request(vec![second, first]);
    reverse.candidates = vec![candidate(node(3), "docker"), candidate(node(2), "docker")];
    reverse.inputs[0].holders.reverse();

    assert_eq!(selected(&forward, &compute), selected(&reverse, &compute));
}

#[test]
fn digest_seals_facts() {
    // Every sealed fact must move the digest: subject generation, input size,
    // chosen source, and the target itself.
    let target = node(2);
    let mut input = resolved_input("in", 1);
    input.holders = vec![holder(node(5), "us")];
    let mut plan_request = request(vec![input]);
    plan_request.candidates = vec![candidate(target, "docker")];
    let compute = config(vec![link("us", "eu", 1_000)]);
    let base = selected(&plan_request, &compute).plan_digest;

    let mut resized = plan_request.clone();
    resized.inputs[0].bytes += 1;
    assert_ne!(selected(&resized, &compute).plan_digest, base);

    let mut regenerated = plan_request.clone();
    regenerated.candidates[0].capability.subject.generation += 1;
    reseal(&mut regenerated.candidates[0]);
    assert_ne!(selected(&regenerated, &compute).plan_digest, base);

    let mut moved = plan_request.clone();
    moved.inputs[0].holders = vec![holder(node(6), "us")];
    assert_ne!(selected(&moved, &compute).plan_digest, base);

    let mut elsewhere = plan_request;
    elsewhere.candidates = vec![candidate(node(3), "docker")];
    assert_ne!(selected(&elsewhere, &compute).plan_digest, base);
}

#[test]
fn rejects_invalid_config() {
    // An unusable link table must fail closed instead of ranking on it.
    let mut plan_request = request(Vec::new());
    plan_request.candidates = vec![candidate(node(2), "docker")];
    assert!(plan_execution(&plan_request, &config(vec![link("eu", "us", 0)])).is_err());
}
