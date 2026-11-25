//! Tests of the run plan without I/O: a simulated graph and the planning rounds a launch would
//! run decide each step's node, mode, staged bytes, cache hits and task counts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::compute::ExecutionTargetId;
use aruna_core::scheduling::eligibility::RejectionVerdict;
use aruna_core::scheduling::inputs::TargetScore;
use aruna_core::scheduling::{PlannedInput, RejectedTarget, Selection};
use aruna_core::structs::execution::job::input::InputContent;
use aruna_core::structs::execution::workflow_run::WorkflowCacheRecord;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::user::id::UserId;

use super::*;

const MIB: u64 = 1 << 20;

fn node(seed: u8) -> NodeId {
    aruna_ops_identity::testing::node(seed)
}

fn pin(name: &str, seed: u8, holder: NodeId) -> PinnedInput {
    PinnedInput {
        name: name.into(),
        bucket: "data".into(),
        key: format!("{name}.bin"),
        version_id: Ulid::from_bytes([seed; 16]),
        source_node_id: Some(holder),
    }
}

fn operation(inputs: Vec<PinnedInput>) -> PlanRunOperation {
    let realm_id = RealmId([5; 32]);
    let auth = AuthContext::new(UserId::default(), realm_id);
    let node = IngressNode {
        local: node(1),
        realm_id,
        now_ms: 1,
        fresh: BTreeMap::new(),
    };
    let request = RunRequest {
        group_id: Ulid::from_bytes([2; 16]),
        workflow_id: Ulid::from_bytes([3; 16]),
        version: 1,
        values: json!({}),
        inputs,
        inputs_json: None,
        output: None,
        provenance_crate: false,
        idempotency_key: None,
        placement_site: None,
        secrets: BTreeMap::new(),
    };
    PlanRunOperation::new(&auth, node, request)
}

/// A round that chose `target`, with each input's size, holder it stages from and time.
fn chosen(target: NodeId, inputs: &[(u64, Option<NodeId>, u64)]) -> ExecutionPlan {
    let target = ExecutionTargetId {
        node_id: target,
        backend: "docker".into(),
        executor_kind: "docker".into(),
    };
    let inputs = (inputs.iter().enumerate())
        .map(|(index, (bytes, source, transfer_ms))| PlannedInput {
            destination_key: format!("input-{index}"),
            version_id: Ulid::from_bytes([index as u8 + 1; 16]),
            content: InputContent::Blake3([index as u8; 32]),
            bytes: *bytes,
            policies: Vec::new(),
            source_node_id: *source,
            transfer_ms: *transfer_ms,
            known_link: true,
        })
        .collect();
    ExecutionPlan {
        selected: Some(Selection {
            target,
            subject_digest: [0; 32],
            subject_generation: 1,
            score: TargetScore {
                estimated_transfer_ms: 0,
                transfer_bytes: 0,
                availability_pressure_permille: 0,
                node_load_permille: 0,
                compute_priority_inverse: 0,
                unknown_link_count: 0,
            },
            inputs,
            output_policies: Vec::new(),
            plan_digest: [0; 32],
        }),
        retryable: false,
        alternatives: Vec::new(),
        rejected: Vec::new(),
        omitted: 0,
    }
}

fn previewed(plan: ExecutionPlan) -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<_, PreviewError>(plan)))
}

fn task(id: u64, step: &str, reads: &[&PinnedInput]) -> Value {
    let local = node(1);
    let inputs: Vec<Value> = (reads.iter())
        .map(|pin| json!({"path": pin.key, "source": pin_address(local, pin), "directory": false}))
        .collect();
    json!({"task": {"id": id, "step": step, "image": "tool:1", "inputs": inputs,
        "resources": {"cpus": 2.0, "gpus": null}, "after": []}, "markers": []})
}

fn cache_hit() -> CacheHit {
    CacheHit {
        record: WorkflowCacheRecord {
            task_key: "key".into(),
            scope_namespace: "group".into(),
            exit_code: 0,
            outputs: Vec::new(),
            stdout_tail: Vec::new(),
            stderr_tail: Vec::new(),
            created_at_ms: 1,
            producer_group_id: Ulid::from_bytes([2; 16]),
            producer_run_id: Ulid::nil(),
            deterministic: true,
            has_secrets: false,
        },
        foreign: false,
    }
}

// Each task that reads only pinned inputs is planned like its launch: next to its data, with
// data staged to a GPU node, with fan-in data following the bulk input, or from the cache.
// Tasks that read other outputs, and those after a dynamic scatter, are placed at run time.
#[test]
fn places_every_step() {
    let (bulk_node, gpu_node) = (node(2), node(3));
    let reads = pin("reads", 1, gpu_node);
    let weights = pin("weights", 2, bulk_node);
    let mut operation = operation(vec![reads.clone(), weights.clone()]);
    let mut train = task(2, "train", &[&weights]);
    train["task"]["resources"]["gpus"] = json!(1);
    train["task"]["task_key_seed"] = json!("seed-train");
    let mut report = task(4, "report", &[]);
    report["markers"] = json!([{"reason": "scatter over the reports"}]);
    let graph = json!({
        "nodes": [task(1, "align", &[&reads]), train, task(3, "merge", &[&weights, &reads]),
            report, task(5, "split", &[])],
        "edges": [{"from": 1, "to": 4, "kind": {"file": {"path": "aligned.bam"}}},
            {"from": 4, "to": 5, "kind": "order"}],
    });

    let first = operation.simulated(&graph, &[]).unwrap();
    let Next::Call(IoCall::Child(_)) = first else {
        panic!("the first task is planned like its launch: {first:?}");
    };
    // The GPU task's preview asks for one GPU; the others ask for none.
    let gpus: Vec<_> = (operation.previews.iter())
        .map(|(_, spec)| spec.resources.gpus)
        .collect();
    assert_eq!(gpus, [Some(1), None]);
    operation
        .next(previewed(chosen(gpu_node, &[(MIB, None, 0)])))
        .unwrap();
    let mut to_gpu = chosen(gpu_node, &[(5_000, Some(bulk_node), 40)]);
    to_gpu.rejected.push(RejectedTarget {
        target: ExecutionTargetId {
            node_id: bulk_node,
            backend: "docker".into(),
            executor_kind: "docker".into(),
        },
        verdict: RejectionVerdict::Resources,
    });
    operation.next(previewed(to_gpu)).unwrap();
    let fan_in = chosen(
        bulk_node,
        &[(100 * MIB, None, 0), (MIB, Some(gpu_node), 12)],
    );
    let lookup = operation.next(previewed(fan_in)).unwrap();
    assert!(matches!(lookup, Next::Call(IoCall::Child(_))), "{lookup:?}");
    let hit = Ok::<_, CatalogError>(Some(cache_hit()));
    let Next::Done(plan) = operation
        .next(IoResult::Child(ChildOutcome::new(hit)))
        .unwrap()
    else {
        panic!("the plan ends after the cache lookups");
    };

    let step = |name: &str| {
        plan.tasks
            .iter()
            .find(|task| task.step == name)
            .unwrap()
            .clone()
    };
    let planned = |node_id, mode, bytes, transfer_ms, cached| {
        (Some(node_id), mode, bytes, transfer_ms, cached)
    };
    let shape = |step: PlannedStep| {
        (
            step.node_id,
            step.mode,
            step.bytes,
            step.transfer_ms,
            step.cached,
        )
    };
    assert_eq!(
        shape(step("align")),
        planned(gpu_node, COMPUTE_TO_DATA, Some(0), Some(0), false)
    );
    assert_eq!(
        shape(step("train")),
        planned(gpu_node, DATA_TO_COMPUTE, Some(5_000), Some(40), true)
    );
    assert_eq!(
        shape(step("merge")),
        planned(bulk_node, DATA_TO_COMPUTE, Some(MIB), Some(12), false)
    );
    // The report reads the alignment, so it is placed at run time near it.
    assert_eq!(
        shape(step("report")),
        planned(gpu_node, PLACED_AT_RUNTIME, None, None, false)
    );
    assert_eq!(step("report").task_count, Some(1));
    assert_eq!(step("split").task_count, None);
    assert!(!plan.complete);
    assert_eq!(
        plan.rejected,
        [(bulk_node, "not enough resources".to_owned())]
    );
    let order: Vec<_> = plan.tasks.iter().map(|task| task.step.as_str()).collect();
    assert_eq!(order, ["align", "train", "merge", "report", "split"]);
}

// A graph whose tasks all read pinned inputs and that has no markers is complete.
#[test]
fn complete_without_runtime() {
    let holder = node(2);
    let reads = pin("reads", 1, holder);
    let mut operation = operation(vec![reads.clone()]);
    let graph = json!({"nodes": [task(1, "align", &[&reads]), task(2, "align", &[&reads])],
        "edges": []});
    operation.simulated(&graph, &[]).unwrap();
    // Two tasks with the same inputs share one planning round.
    assert!(operation.previews.is_empty());
    let next = operation
        .next(previewed(chosen(holder, &[(10, None, 0)])))
        .unwrap();
    let Next::Done(plan) = next else {
        panic!("no cache lookup without a task key: {next:?}");
    };
    assert!(plan.complete);
    assert_eq!(plan.tasks.len(), 1);
    assert_eq!(plan.tasks[0].task_count, Some(2));
    assert_eq!(plan.tasks[0].node_id, Some(holder));
}

// A task no node can run, or whose inputs cannot be resolved, is placed at run time and the
// reason becomes a diagnostic; a simulation error ends the plan with its diagnostics only.
#[test]
fn reports_unplaceable_tasks() {
    let reads = pin("reads", 1, node(2));
    let mut planning = operation(vec![reads.clone()]);
    let graph = json!({"nodes": [task(1, "align", &[&reads])], "edges": []});
    planning.simulated(&graph, &[]).unwrap();
    let denied = Err::<ExecutionPlan, _>(PreviewError::Inputs(
        aruna_core::job_request::SubmitJobError::InputDenied("reads".into()),
    ));
    let Next::Done(plan) = planning
        .next(IoResult::Child(ChildOutcome::new(denied)))
        .unwrap()
    else {
        panic!("the plan ends");
    };
    assert_eq!(plan.tasks[0].mode, PLACED_AT_RUNTIME);
    assert!(!plan.complete);
    assert_eq!(plan.diagnostics[0]["severity"], "error");

    let mut failing = operation(Vec::new());
    let error = Diagnostic {
        severity: Severity::Error,
        message: "no such step".into(),
        pointer: Default::default(),
    };
    let Next::Done(plan) = failing
        .simulated(&json!({"nodes": [], "edges": []}), &[error])
        .unwrap()
    else {
        panic!("a simulation error ends the plan");
    };
    assert!(!plan.complete && plan.tasks.is_empty());
    assert_eq!(
        plan.diagnostics,
        [json!({"message": "no such step", "severity": "error"})]
    );
}

// Two tasks reading the same pinned input with different resource requirements get separate
// preview rounds so each receives its own placement instead of inheriting the first task's node.
#[test]
fn separates_distinct_resources() {
    let (data_node, gpu_node) = (node(2), node(3));
    let reads = pin("reads", 1, data_node);
    let mut planning = operation(vec![reads.clone()]);
    let mut train = task(2, "train", &[&reads]);
    train["task"]["resources"]["gpus"] = json!(1);
    let graph = json!({
        "nodes": [task(1, "align", &[&reads]), train],
        "edges": [],
    });

    planning.simulated(&graph, &[]).unwrap();
    assert_eq!(planning.previews.len(), 1, "gpu task queues a second round");
    assert_eq!(planning.previews[0].1.resources.gpus, Some(1));

    let next = planning
        .next(previewed(chosen(data_node, &[(MIB, None, 0)])))
        .unwrap();
    assert!(matches!(next, Next::Call(IoCall::Child(_))), "{next:?}");

    let Next::Done(plan) = planning
        .next(previewed(chosen(gpu_node, &[(MIB, Some(data_node), 20)])))
        .unwrap()
    else {
        panic!("plan completes after second preview");
    };
    assert!(plan.complete);
    assert_eq!(plan.tasks[0].step, "align");
    assert_eq!(plan.tasks[0].node_id, Some(data_node));
    assert_eq!(plan.tasks[0].mode, COMPUTE_TO_DATA);
    assert_eq!(plan.tasks[1].step, "train");
    assert_eq!(plan.tasks[1].node_id, Some(gpu_node));
    assert_eq!(plan.tasks[1].mode, DATA_TO_COMPUTE);
}
