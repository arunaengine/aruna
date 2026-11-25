//! Tests of run reports and the run crate on memory storage with a scripted coordinator: why,
//! compare with both reads, time, changes, the HTML report, the crate merge and its writer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::WORKFLOW_RUN_KEYSPACE;
use aruna_core::structs::execution::workflow_run::{
    RunFile, RunOutput, RunState, WorkflowRunRecord, run_key,
};
use aruna_core::structs::storage::data::identity::content_id;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_runner::driver::drive;
use aruna_workflow_contract::message::{CoordinatorMode, CoordinatorToHost, HostToCoordinator};
use aruna_workflow_contract::preflight::RunLogEntry;
use aruna_workflow_contract::{PreflightKind, PreflightResult};
use serde_json::{Value, json};
use ulid::Ulid;

use super::log::AppendLogOperation;
use super::provenance::{
    PROCESS_PROFILE, PROVENANCE_PROFILE, RunCrateOperation, WORKFLOW_PROFILE,
    WriteProvenanceOperation, action_id, crate_json, status,
};
use super::reports::{
    CompareRunsOperation, RunChangesOperation, RunReportOperation, RunTimeOperation,
    TaskWhyOperation,
};
use super::run::{ApplyEventOperation, FinishRunOperation};
use super::tests::{Runs, encrypt, runs, task};
use super::{ChangeRunOperation, RunChange, RunTasksOperation};
use crate::workflows::CatalogError;
use crate::workflows::tests::write;
use aruna_workflow_contract::message::TaskState;

fn answer(result: PreflightResult) -> CoordinatorToHost {
    CoordinatorToHost::PreflightResponse {
        request_id: 1,
        result: Ok(result),
    }
}

/// A run with task 1 finished, ended as succeeded; with `logged`, its run log holds its start.
async fn ended(runs: &Runs, values: Value, logged: bool) -> WorkflowRunRecord {
    let request = runs.request(None, values);
    let run = runs
        .start(&runs.fixture.alice, request)
        .await
        .unwrap()
        .record;
    ended_run(runs, run, logged).await
}

async fn ended_run(runs: &Runs, run: WorkflowRunRecord, logged: bool) -> WorkflowRunRecord {
    let context = &runs.fixture.context;
    let finished = task(TaskState::Finished, Some(0));
    drive(ApplyEventOperation::new(run.run_id, &finished), context)
        .await
        .unwrap();
    if logged {
        let begin = RunLogEntry::Begin {
            run_start_json: json!({"run": run.run_id.to_string()}),
        };
        drive(AppendLogOperation::new(run.run_id, begin), context)
            .await
            .unwrap();
    }
    let finish = FinishRunOperation::new(run.run_id, RunState::Succeeded, None);
    drive(finish, context).await.unwrap()
}

/// The requests sent to report sessions so far.
fn asked(runs: &Runs) -> Vec<PreflightKind> {
    (runs.host.sent().into_iter())
        .filter_map(|message| match message {
            HostToCoordinator::PreflightRequest { kind, .. } => Some(kind),
            _ => None,
        })
        .collect()
}

// Without a run log the record answers; with one, uwe's comparison through a report session
// that is opened in report mode and closed again.
#[tokio::test]
async fn explains_tasks() {
    let runs = runs().await;
    let context = &runs.fixture.context;
    let alice = runs.fixture.alice.clone();
    let plain = ended(&runs, json!({"message": "Hola"}), false).await;
    let why = drive(TaskWhyOperation::new(&alice, plain.run_id, 1), context)
        .await
        .unwrap();
    assert_eq!(
        (why["verdict"].as_str(), why["source"].as_str()),
        (Some("ran"), Some("record"))
    );
    assert!(runs.host.sessions().0.is_empty());

    let run = ended(&runs, json!({"message": "Hola"}), true).await;
    let earlier = json!({"run": "01K8W1Z0000000000000000000", "task": 1,
        "changes": ["the input `a.txt` changed: 6 bytes before, 10 now"]});
    runs.host.script(vec![answer(PreflightResult::ComputeWhy {
        why_json: Some(earlier),
    })]);
    let why = drive(TaskWhyOperation::new(&alice, run.run_id, 1), context)
        .await
        .unwrap();
    assert_eq!(why["verdict"], "reran", "{why}");
    assert_eq!(why["source"], "uwe");
    assert_eq!(
        why["changes"][0]["field"],
        "the input `a.txt` changed: 6 bytes before, 10 now"
    );
    let (opened, closed) = runs.host.sessions();
    assert_eq!(opened.len(), 1);
    assert_eq!(opened[0].1, CoordinatorMode::Report);
    assert!(
        closed.contains(&opened[0].0),
        "the report session is closed"
    );
    let asked = asked(&runs);
    let Some(PreflightKind::ComputeWhy {
        task_id, log_pages, ..
    }) = asked.first()
    else {
        panic!("a why request: {asked:?}");
    };
    assert_eq!((*task_id, log_pages.len()), (1, 1));

    let hidden = TaskWhyOperation::new(&runs.fixture.stranger, run.run_id, 1);
    assert_eq!(drive(hidden, context).await, Err(CatalogError::NotFound));
    let unknown = TaskWhyOperation::new(&alice, run.run_id, 99);
    assert_eq!(drive(unknown, context).await, Err(CatalogError::NotFound));
}

// A comparison needs READ on both runs; it diffs values and adds uwe's comparison of the logs.
#[tokio::test]
async fn compares_readable_runs() {
    let runs = runs().await;
    let context = &runs.fixture.context;
    let alice = runs.fixture.alice.clone();
    let first = ended(&runs, json!({"message": "Hola"}), true).await;
    let second = ended(&runs, json!({"message": "Ciao"}), true).await;
    runs.host
        .script(vec![answer(PreflightResult::ComputeCompare {
            comparison_json: Some(json!({"tasks": []})),
        })]);
    let compared = CompareRunsOperation::new(&alice, first.run_id, second.run_id);
    let compared = drive(compared, context).await.unwrap();
    assert_eq!(compared["same_version"], true);
    assert_eq!(
        compared["values"],
        json!([{"path": "/message", "first": "Hola", "second": "Ciao", "kind": "changed"}])
    );
    assert_eq!(compared["comparison"], json!({"tasks": []}));
    assert_eq!(compared["second"]["run_id"], second.run_id.to_string());

    let mut foreign = first.clone();
    foreign.run_id = Ulid::from_bytes([55; 16]);
    foreign.group_id = runs.fixture.group_b;
    foreign.created_by = runs.fixture.bob.user_id;
    let row = run_key(foreign.run_id).to_vec();
    write(
        context,
        WORKFLOW_RUN_KEYSPACE,
        row,
        foreign.to_bytes().unwrap(),
    )
    .await;
    for (left, right) in [
        (first.run_id, foreign.run_id),
        (foreign.run_id, first.run_id),
    ] {
        let hidden = CompareRunsOperation::new(&alice, left, right);
        assert_eq!(drive(hidden, context).await, Err(CatalogError::NotFound));
    }
    let missing = CompareRunsOperation::new(&alice, first.run_id, Ulid::from_bytes([56; 16]));
    assert_eq!(drive(missing, context).await, Err(CatalogError::NotFound));
    let bob = CompareRunsOperation::new(&runs.fixture.bob, foreign.run_id, first.run_id);
    assert_eq!(drive(bob, context).await, Err(CatalogError::NotFound));
}

// Time, changes and the HTML report answer from the records without a log and from uwe with one.
#[tokio::test]
async fn reports_time_changes() {
    let runs = runs().await;
    let context = &runs.fixture.context;
    let alice = runs.fixture.alice.clone();
    let plain = ended(&runs, json!({"message": "Hola"}), false).await;
    let time = drive(RunTimeOperation::new(&alice, plain.run_id), context)
        .await
        .unwrap();
    assert_eq!(time["source"], "record");
    assert_eq!(time["started"], plain.created_at_ms);
    assert_eq!(time["tasks"], 1);
    for field in [
        "ended", "ran", "waited", "busy", "before", "after", "reused",
    ] {
        assert!(time[field].is_u64(), "{field}: {time}");
    }
    assert!(time["critical_path"].is_array() && time["steps"].is_array());
    let changes = drive(RunChangesOperation::new(&alice, plain.run_id), context)
        .await
        .unwrap();
    assert_eq!(
        (changes["changed"].as_bool(), &changes["recorded"]),
        (Some(false), &Value::Null)
    );
    let html = drive(RunReportOperation::new(&alice, plain.run_id), context)
        .await
        .unwrap();
    assert!(html.contains("<html") && html.contains(&plain.run_id.to_string()));

    let run = ended(&runs, json!({"message": "Hola"}), true).await;
    let spent = json!({"run": run.run_id.to_string(), "started": 1, "ended": 5, "tasks": 1,
        "reused": 0, "ran": 3, "waited": 1, "busy": 3, "before": 1, "after": 0,
        "critical_path": [], "steps": []});
    let recorded = json!({"run": {"id": run.run_id.to_string()},
        "images": [{"named": "debian:stable-slim", "ran": "debian@sha256:00"}]});
    runs.host.script(vec![
        answer(PreflightResult::ComputeTime {
            time_json: Some(spent),
        }),
        answer(PreflightResult::ComputeChanges {
            changes_json: recorded.clone(),
        }),
        answer(PreflightResult::BuildReport {
            html: "<html>uwe</html>".into(),
        }),
    ]);
    let time = drive(RunTimeOperation::new(&alice, run.run_id), context)
        .await
        .unwrap();
    assert_eq!(
        (time["source"].as_str(), time["ended"].as_u64()),
        (Some("uwe"), Some(5))
    );
    let changes = drive(RunChangesOperation::new(&alice, run.run_id), context)
        .await
        .unwrap();
    assert_eq!(changes["recorded"], recorded);
    let html = drive(RunReportOperation::new(&alice, run.run_id), context)
        .await
        .unwrap();
    assert_eq!(html, "<html>uwe</html>");
    let hidden = RunReportOperation::new(&runs.fixture.stranger, run.run_id);
    assert_eq!(drive(hidden, context).await, Err(CatalogError::NotFound));
}

fn by_id<'a>(document: &'a Value, id: &str) -> &'a Value {
    (document["@graph"].as_array().unwrap().iter())
        .find(|entity| entity["@id"] == id)
        .unwrap_or_else(|| panic!("no entity {id} in {document}"))
}

fn ids(value: &Value) -> Vec<String> {
    match value {
        Value::Array(items) => items.iter().flat_map(ids).collect(),
        Value::Object(map) => map["@id"].as_str().map(str::to_owned).into_iter().collect(),
        _ => Vec::new(),
    }
}

// uwe's crate keeps its task actions; the run action takes the portal's id, the context and
// descriptor move to RO-Crate 1.2, and Aruna's facts join without duplicating uwe's entities.
#[test]
fn merges_uwe_crate() {
    let mut record = super::tests::sample_run();
    record.state = RunState::Succeeded;
    let run = record.run_id;
    let uwe = json!({
        "@context": "https://w3id.org/ro/crate/1.1/context",
        "@graph": [
            {"@id": "ro-crate-metadata.json", "@type": "CreativeWork", "about": {"@id": "./"},
                "conformsTo": [{"@id": "https://w3id.org/ro/crate/1.1"},
                    {"@id": "https://w3id.org/workflowhub/workflow-ro-crate/1.0"}]},
            {"@id": "./", "@type": "Dataset", "mainEntity": {"@id": "Snakefile"},
                "mentions": {"@id": format!("#run/{run}")},
                "conformsTo": [{"@id": PROCESS_PROFILE}, {"@id": WORKFLOW_PROFILE},
                    {"@id": PROVENANCE_PROFILE}]},
            {"@id": format!("#run/{run}"), "@type": "CreateAction", "instrument": {"@id": "Snakefile"}},
            {"@id": "#task/7", "@type": "CreateAction", "name": "echo-7", "result": {"@id": "#data/aa"}}
        ]
    });
    let tasks = vec![super::run::changed_task(
        run,
        &aruna_workflow_contract::message::TaskChange {
            task_id: 7,
            name: "echo-7-1".into(),
            step: "echo".into(),
            state: TaskState::Reused,
            attempt: Some(1),
            time_ms: 1,
            started_ms: None,
            ended_ms: None,
            cached: true,
            exit_code: Some(0),
            error: None,
        },
    )];
    let merged = crate_json(&record, &tasks, Some(uwe));
    assert_eq!(
        merged["@context"],
        json!([
            "https://w3id.org/ro/crate/1.2/context",
            "https://w3id.org/ro/terms/workflow-run/context"
        ])
    );
    let descriptor = by_id(&merged, "ro-crate-metadata.json");
    let specs = ids(&descriptor["conformsTo"]);
    assert_eq!(specs[0], "https://w3id.org/ro/crate/1.2");
    assert!(!specs.iter().any(|spec| spec.ends_with("/1.1")));
    let root = by_id(&merged, "./");
    assert_eq!(ids(&root["mentions"]), [action_id(run)]);
    let graph = merged["@graph"].as_array().unwrap();
    assert!(
        !graph
            .iter()
            .any(|entity| entity["@id"] == format!("#run/{run}"))
    );
    let action = by_id(&merged, &action_id(run));
    assert_eq!(action["url"], format!("/compute/workflow-runs/{run}"));
    assert_eq!(action["instrument"]["@id"], "Snakefile");
    let task = by_id(&merged, "#task/7");
    assert_eq!(task["result"]["@id"], "#data/aa");
    let tasks_seen = graph
        .iter()
        .filter(|entity| entity["@id"] == "#task/7")
        .count();
    assert_eq!(tasks_seen, 1);
    let reuse = by_id(&merged, "#task/7/aruna/cache-reuse");
    assert_eq!(reuse["value"], "reused");
    assert!(ids(&task["additionalProperty"]).contains(&"#task/7/aruna/cache-reuse".to_owned()));
}

// The writer stores the crate of an ended run once, records how its publication went, and the
// crate names the run's outputs by exact version and digest and takes the dataset's metadata.
#[tokio::test]
async fn writes_run_crate() {
    let runs = runs().await;
    let context = &runs.fixture.context;
    let alice = runs.fixture.alice.clone();
    let mut request = runs.request(None, json!({"message": "Hola"}));
    request.provenance_crate = true;
    request.output = Some(RunOutput {
        bucket: "results".into(),
        prefix: "hello/".into(),
        node_id: None,
        dataset_json: Some(
            json!({"name": "Greetings", "description": "Said hello",
                "license": "https://spdx.org/licenses/CC-BY-4.0", "public": false})
            .to_string(),
        ),
    });
    let run = runs.start(&alice, request).await.unwrap().record;
    let active = drive(RunCrateOperation::new(&alice, run.run_id), context).await;
    assert!(
        matches!(active, Err(CatalogError::Conflict(_))),
        "{active:?}"
    );
    let version = Ulid::from_bytes([21; 16]);
    let hash = [7u8; 32];
    let delivered = RunChange::Delivered {
        file: RunFile {
            name: "said.txt".into(),
            bucket: Some("results".into()),
            key: Some("hello/said.txt".into()),
            version_id: Some(version.to_string()),
            node_id: Some(runs.fixture.node),
            size: Some(5),
            digest: Some(hex::encode(hash)),
        },
        origin: None,
    };
    drive(
        ChangeRunOperation::new(run.run_id, vec![delivered]),
        context,
    )
    .await
    .unwrap();
    let run = ended_run(&runs, run, false).await;

    let write = WriteProvenanceOperation::new(run.run_id, runs.serving());
    let written = drive(write, context).await.unwrap();
    let outcome = written
        .provenance_status
        .clone()
        .expect("a recorded status");
    assert!(
        [status::WRITTEN, status::FAILED, status::DENIED].contains(&outcome.as_str()),
        "{outcome}"
    );
    assert_eq!(written.state, RunState::Succeeded);
    let again = WriteProvenanceOperation::new(run.run_id, runs.serving());
    assert_eq!(drive(again, context).await.unwrap(), written);

    let stored = drive(RunCrateOperation::new(&alice, run.run_id), context)
        .await
        .unwrap();
    let root = by_id(&stored, "./");
    let profiles = ids(&root["conformsTo"]);
    for profile in [PROCESS_PROFILE, WORKFLOW_PROFILE, PROVENANCE_PROFILE] {
        assert!(profiles.contains(&profile.to_owned()), "{profiles:?}");
    }
    assert_eq!(root["name"], "Greetings");
    assert_eq!(
        root["license"]["@id"],
        "https://spdx.org/licenses/CC-BY-4.0"
    );
    assert_eq!(ids(&root["mentions"]), [action_id(run.run_id)]);
    assert!(root["description"].as_str().is_some_and(|d| !d.is_empty()));
    let action = by_id(&stored, &action_id(run.run_id));
    assert_eq!(
        action["url"],
        format!("/compute/workflow-runs/{}", run.run_id)
    );
    assert_eq!(
        action["actionStatus"],
        "http://schema.org/CompletedActionStatus"
    );
    let arn = VersionedObjectArn::new(
        run.realm_id,
        runs.fixture.node,
        "results",
        "hello/said.txt",
        version,
    )
    .unwrap()
    .to_w3id();
    assert_eq!(ids(&root["hasPart"]), std::slice::from_ref(&arn));
    assert_eq!(ids(&action["result"]), std::slice::from_ref(&arn));
    let file = by_id(&stored, &arn);
    assert_eq!(file["identifier"], content_id(hash));
    assert_eq!(file["contentUrl"][0], "s3://results/hello/said.txt");
    let tasks = drive(RunTasksOperation::new(run.run_id), context)
        .await
        .unwrap();
    let task = by_id(&stored, &format!("#task/{}", tasks[0].task_id));
    assert_eq!(task["@type"], "CreateAction");
    let hidden = RunCrateOperation::new(&runs.fixture.stranger, run.run_id);
    assert_eq!(drive(hidden, context).await, Err(CatalogError::NotFound));
}

// The crate of a protected run stays beside the run, readable with it, and is never published
// as a document of its group.
#[tokio::test]
async fn keeps_protected_crate() {
    let runs = runs().await;
    let context = &runs.fixture.context;
    encrypt(&runs, "results").await;
    let mut request = runs.request(None, json!({"message": "Hola"}));
    request.provenance_crate = true;
    let run = runs
        .start(&runs.fixture.alice, request)
        .await
        .unwrap()
        .record;
    assert!(run.protected);
    let run = ended_run(&runs, run, false).await;
    let write = WriteProvenanceOperation::new(run.run_id, runs.serving());
    let written = drive(write, context).await.unwrap();
    assert_eq!(
        written.provenance_status.as_deref(),
        Some(status::PROTECTED)
    );
    assert_eq!(written.provenance_document_id, None);
    let stored = RunCrateOperation::new(&runs.fixture.alice, run.run_id);
    assert!(drive(stored, context).await.is_ok());
}

// A run that asked for no crate gets none written, but a reader still gets one built for it.
#[tokio::test]
async fn builds_missing_crate() {
    let runs = runs().await;
    let context = &runs.fixture.context;
    let run = ended(&runs, json!({"message": "Hola"}), false).await;
    let write = WriteProvenanceOperation::new(run.run_id, runs.serving());
    let untouched = drive(write, context).await.unwrap();
    assert_eq!(untouched.provenance_status, None);
    let built = drive(
        RunCrateOperation::new(&runs.fixture.alice, run.run_id),
        context,
    )
    .await
    .unwrap();
    assert_eq!(
        by_id(&built, "ro-crate-metadata.json")["about"]["@id"],
        "./"
    );
    let action = by_id(&built, &action_id(run.run_id));
    assert_eq!(action["identifier"], run.run_id.to_string());
}
