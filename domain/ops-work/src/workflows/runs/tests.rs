//! Tests of workflow runs on memory storage with a scripted coordinator: admission and its key,
//! a whole run with its log and events, child tasks, cancellation, deletion and recovery.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use aruna_compute::registry::ExecutorRegistry;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::compute::quota::{QuotaDenied, QuotaDimension, QuotaScope};
use aruna_core::job_request::SubmitJobError;
use aruna_core::keyspaces::{BUCKET_ENCRYPTION_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::execution::job::input::{
    InputMode, InputSelection, InputSource, OutputDestination, WORKFLOW_RUN_TAG, WORKFLOW_TASK_TAG,
    node_tag,
};
use aruna_core::structs::execution::job::local::WorkspaceMode;
use aruna_core::structs::execution::workflow_run::{
    CacheScope, RunOutput, RunState, WorkflowRunRecord, WorkflowTaskRecord,
};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::structs::storage::encryption::{BucketEncryption, EncryptionMode};
use aruna_core::structs::storage::routing::NodeRouting;
use aruna_core::workflow::{WorkflowAnswer, WorkflowEffect, WorkflowHost};
use aruna_runner::driver::drive;
use aruna_workflow_contract::host::{
    DirectInput, HostRpcRequest, HostRpcResult, StagedFile, StartTask, TaskFolders, TaskResources,
};
use aruna_workflow_contract::message::{
    CoordinatorToHost, HostToCoordinator, LiveWorkflowEvent, RunFinishedPayload, TaskChange,
    TaskState,
};
use aruna_workflow_contract::preflight::RunLogEntry;
use serde_json::{Value, json};
use ulid::Ulid;

use super::admit::{AdmittedRun, PinnedInput, RunRequest, StartRunOperation};
use super::cancel::{CancelRunOperation, DeleteRunOperation};
use super::data::{area_of, resolve};
use super::events::ReadEventsOperation;
use super::log::{AppendLogOperation, LoadLogOperation};
use super::reads::{ListTasksOperation, RunSnapshotOperation, TaskFilter, log_chunk};
use super::recover::{InterruptRunOperation, Interrupted};
use super::run::{
    AnswerCallOperation, ApplyEventOperation, CallNode, Chunk, FinishRunOperation, Inbound,
    PrepareRunOperation, Prepared, SessionOperation, Uploads, inbound,
};
use super::tasks::{POLL_DELAY, RunTaskOperation, child_key, task_key_of, task_spec};
use super::{ChangeRunOperation, RunChange};
use crate::jobs::lifecycle::admit::here::AcceptedSubmission;
use crate::jobs::lifecycle::family_reads::{FamilyProjectionOperation, FamilyView};
use crate::jobs::lifecycle::submit_external::IngressNode;
use crate::workflows::CatalogError;
use crate::workflows::tests::{Fixture, answer, fixture, new_workflow, upload, write};

const CWL: &str = "cwlVersion: v1.2\nclass: CommandLineTool\nbaseCommand: echo\n";

/// A coordinator that answers preflight requests like the catalog tests and plays a scripted
/// run: it hands out queued frames and keeps every message the node sent.
#[derive(Default)]
pub(super) struct RunHost {
    frames: Mutex<VecDeque<CoordinatorToHost>>,
    sent: Mutex<Vec<HostToCoordinator>>,
    opened: Mutex<Vec<(String, aruna_workflow_contract::CoordinatorMode)>>,
    closed: Mutex<Vec<String>>,
}

impl RunHost {
    pub(super) fn script(&self, frames: Vec<CoordinatorToHost>) {
        self.frames.lock().unwrap().extend(frames);
    }

    pub(super) fn sent(&self) -> Vec<HostToCoordinator> {
        self.sent.lock().unwrap().clone()
    }

    /// The sessions opened so far with their modes, and the sessions closed.
    pub(super) fn sessions(
        &self,
    ) -> (
        Vec<(String, aruna_workflow_contract::CoordinatorMode)>,
        Vec<String>,
    ) {
        let opened = self.opened.lock().unwrap().clone();
        (opened, self.closed.lock().unwrap().clone())
    }
}

#[async_trait::async_trait]
impl WorkflowHost for RunHost {
    async fn serve(&self, effect: WorkflowEffect) -> WorkflowAnswer {
        match effect {
            WorkflowEffect::Preflight(kind) => WorkflowAnswer::Preflight(answer(kind)),
            WorkflowEffect::Open { session, mode, .. } => {
                self.opened.lock().unwrap().push((session, mode));
                WorkflowAnswer::Opened(Ok(()))
            }
            WorkflowEffect::Send { message, .. } => {
                self.sent.lock().unwrap().push(message);
                WorkflowAnswer::Sent(Ok(()))
            }
            WorkflowEffect::Receive { .. } => {
                WorkflowAnswer::Received(Ok(self.frames.lock().unwrap().pop_front()))
            }
            WorkflowEffect::Close { session } => {
                self.closed.lock().unwrap().push(session);
                WorkflowAnswer::Closed(Ok(()))
            }
        }
    }
}

pub(super) struct Runs {
    pub(super) fixture: Fixture,
    pub(super) host: Arc<RunHost>,
    pub(super) workflow_id: Ulid,
}

pub(super) async fn runs() -> Runs {
    let mut fixture = fixture().await;
    let host = Arc::new(RunHost::default());
    let registry = ExecutorRegistry::new().with_workflow(host.clone());
    fixture.context.compute_handle = Some(Arc::new(registry));
    let (context, group, owner) = (&fixture.context, fixture.group_a, fixture.alice.user_id);
    aruna_runner::testing::seed_bucket(context, "results", group, owner, Vec::new()).await;
    // Run jobs are minted on this node, so it holds a job-control band like an onboarded node.
    let realm_id = fixture.alice.realm_id;
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.ensure_node(fixture.node, RealmNodeKind::Server);
    config.seed_default_placement();
    config.seed_job_control(fixture.node, 0);
    let actor = Actor {
        node_id: fixture.node,
        user_id: owner,
        realm_id,
    };
    let bytes = config.to_bytes(&actor).unwrap();
    write(
        context,
        REALM_CONFIG_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        bytes,
    )
    .await;
    let workflow = new_workflow(fixture.group_a, "hello", upload(CWL));
    let created = fixture.create(&fixture.alice, workflow).await.unwrap();
    Runs {
        fixture,
        host,
        workflow_id: created.workflow_id,
    }
}

impl Runs {
    pub(super) fn request(&self, key: Option<&str>, values: Value) -> RunRequest {
        RunRequest {
            group_id: self.fixture.group_a,
            workflow_id: self.workflow_id,
            version: 1,
            values,
            inputs: Vec::new(),
            inputs_json: None,
            output: Some(RunOutput {
                bucket: "results".into(),
                prefix: "hello/".into(),
                node_id: None,
                dataset_json: None,
            }),
            provenance_crate: false,
            idempotency_key: key.map(Into::into),
            placement_site: None,
            secrets: BTreeMap::new(),
        }
    }

    pub(super) fn serving(
        &self,
    ) -> (
        aruna_core::id::NodeId,
        aruna_core::structs::identity::realm::RealmId,
    ) {
        (self.fixture.node, self.fixture.alice.realm_id)
    }

    pub(super) async fn start(
        &self,
        auth: &aruna_core::structs::identity::auth::AuthContext,
        request: RunRequest,
    ) -> Result<AdmittedRun, CatalogError> {
        let start = StartRunOperation::new(auth, self.serving(), request);
        drive(start, &self.fixture.context).await
    }

    pub(super) async fn started(&self) -> WorkflowRunRecord {
        let request = self.request(None, json!({"message": "Hola"}));
        self.start(&self.fixture.alice, request)
            .await
            .unwrap()
            .record
    }

    pub(super) async fn prepare(&self, record: &WorkflowRunRecord) -> Prepared {
        let spec = aruna_core::structs::execution::workflow_run::WorkflowRunSpec {
            run_id: record.run_id,
            group_id: record.group_id,
            created_by: record.created_by,
            realm_id: record.realm_id,
        };
        let prepare = PrepareRunOperation::new(&spec, self.fixture.node, "blake3:claim".into());
        drive(prepare, &self.fixture.context).await.unwrap()
    }

    pub(super) fn call_node(&self) -> CallNode {
        let (node_id, realm_id) = self.serving();
        CallNode {
            node_id,
            realm_id,
            routing: NodeRouting::default(),
            ingress: IngressNode {
                local: node_id,
                realm_id,
                now_ms: 1,
                fresh: BTreeMap::new(),
            },
        }
    }

    /// Plays the scripted frames the way the node's session loop does, until the run ends or
    /// the script runs out.
    pub(super) async fn play(&self, record: &WorkflowRunRecord) -> Option<WorkflowRunRecord> {
        let context = &self.fixture.context;
        let run_id = record.run_id;
        loop {
            let frame = drive(SessionOperation::receive(run_id), context)
                .await
                .unwrap()?;
            match inbound(frame) {
                Inbound::Call { call_id, request } => {
                    let call = AnswerCallOperation::new(
                        record.clone(),
                        self.call_node(),
                        call_id,
                        request,
                    );
                    drive(call, context).await.unwrap();
                }
                Inbound::Event(event) => {
                    drive(ApplyEventOperation::new(run_id, &event), context)
                        .await
                        .unwrap();
                }
                Inbound::Finished(finished) => {
                    let finish = FinishRunOperation::reported(run_id, &finished);
                    return Some(drive(finish, context).await.unwrap());
                }
                other => panic!("an unexpected frame {other:?}"),
            }
        }
    }

    async fn events(&self, run_id: Ulid, after: u64) -> Vec<(u64, String)> {
        let read = ReadEventsOperation::new(&self.fixture.alice, run_id, after, 100);
        let batch = drive(read, &self.fixture.context).await.unwrap();
        (batch.events.into_iter())
            .map(|event| (event.seq, event.event_type))
            .collect()
    }
}

pub(super) fn call(call_id: u64, request: HostRpcRequest) -> CoordinatorToHost {
    CoordinatorToHost::RpcRequest { call_id, request }
}

pub(super) fn event(seq: u64, event: LiveWorkflowEvent) -> CoordinatorToHost {
    CoordinatorToHost::LiveEvent { seq, event }
}

pub(super) fn task(state: TaskState, exit_code: Option<i32>) -> LiveWorkflowEvent {
    LiveWorkflowEvent::TaskChanged(TaskChange {
        task_id: 1,
        name: "echo-1-1".into(),
        step: "echo".into(),
        state,
        attempt: Some(1),
        time_ms: 2,
        started_ms: Some(2),
        ended_ms: exit_code.map(|_| 3),
        cached: false,
        exit_code,
        error: None,
    })
}

pub(super) fn log_calls() -> Vec<CoordinatorToHost> {
    vec![
        call(
            1,
            HostRpcRequest::RunLogBegin {
                run_start_json: json!({"run": "begin"}),
            },
        ),
        call(
            2,
            HostRpcRequest::RunLogAppend {
                seq: 1,
                change_json: json!({"task": 1}),
                is_event: false,
            },
        ),
    ]
}

// Admission checks the group and the output, and one key starts one run.
#[tokio::test]
async fn admits_once() {
    let runs = runs().await;
    let values = json!({"message": "Hola"});
    let stranger = runs.fixture.stranger.clone();
    let hidden = runs
        .start(&stranger, runs.request(None, values.clone()))
        .await;
    assert_eq!(hidden, Err(CatalogError::NotFound));
    let mut bare = runs.request(None, values.clone());
    bare.output = None;
    let refused = runs.start(&runs.fixture.alice, bare).await;
    assert!(
        matches!(refused, Err(CatalogError::Invalid(_))),
        "{refused:?}"
    );
    let mut elsewhere = runs.request(None, values.clone());
    elsewhere.output.as_mut().unwrap().bucket = "absent".into();
    let missing = runs.start(&runs.fixture.alice, elsewhere).await;
    assert!(
        matches!(missing, Err(CatalogError::Invalid(_))),
        "{missing:?}"
    );

    let alice = runs.fixture.alice.clone();
    let first = runs
        .start(&alice, runs.request(Some("k1"), values.clone()))
        .await
        .unwrap();
    assert!(first.created);
    assert_eq!(first.record.state, RunState::Queued);
    assert!(first.record.parent_job_id.is_some());
    let again = runs
        .start(&alice, runs.request(Some("k1"), values))
        .await
        .unwrap();
    assert_eq!(
        (again.created, again.record.run_id),
        (false, first.record.run_id)
    );
    let other = runs.request(Some("k1"), json!({"message": "Ciao"}));
    let conflict = runs.start(&alice, other).await;
    assert!(
        matches!(conflict, Err(CatalogError::Conflict(_))),
        "{conflict:?}"
    );
    let events = runs.events(first.record.run_id, 0).await;
    assert_eq!(events, vec![(1, "run".to_owned())]);
}

/// Marks `bucket` as encrypting, the way enabling bucket encryption stores it.
pub(super) async fn encrypt(runs: &Runs, bucket: &str) {
    let settings = BucketEncryption {
        mode: EncryptionMode::VaultLocked,
        bucket_id: Some(Ulid::from_bytes([30; 16])),
        key_generation: 1,
        ..BucketEncryption::default()
    };
    let (context, key) = (&runs.fixture.context, bucket.as_bytes().to_vec());
    write(
        context,
        BUCKET_ENCRYPTION_KEYSPACE,
        key,
        settings.to_bytes().unwrap(),
    )
    .await;
}

// An encrypting input needs an encrypting output; such a run is protected, reuses only its own
// tasks and keeps its work files in its own area of the output bucket.
#[tokio::test]
async fn protects_encrypted_runs() {
    let runs = runs().await;
    let (context, group, alice) = (
        &runs.fixture.context,
        runs.fixture.group_a,
        &runs.fixture.alice,
    );
    aruna_runner::testing::seed_bucket(context, "secret", group, alice.user_id, Vec::new()).await;
    encrypt(&runs, "secret").await;
    let mut request = runs.request(None, json!({"message": "Hola"}));
    request.inputs.push(PinnedInput {
        name: "reads".into(),
        bucket: "secret".into(),
        key: "a.txt".into(),
        version_id: Ulid::from_bytes([31; 16]),
        source_node_id: None,
    });
    let refused = runs.start(alice, request.clone()).await;
    assert!(
        matches!(refused, Err(CatalogError::Invalid(_))),
        "{refused:?}"
    );
    let mut no_output = request.clone();
    no_output.output = None;
    let refused_run_area = runs.start(alice, no_output).await;
    assert!(
        matches!(refused_run_area, Err(CatalogError::Invalid(_))),
        "{refused_run_area:?}"
    );
    let open = runs.started().await;
    assert!(!open.protected);

    encrypt(&runs, "results").await;
    let run = runs.start(alice, request).await.unwrap().record;
    assert!(run.protected);
    assert_eq!(run.cache_scope, CacheScope::Run);
    let spec = task_spec(&run, &start_task(), "key").unwrap();
    let (bucket, area) = area_of(&run).unwrap();
    let work = OutputDestination::S3 {
        bucket,
        key: format!("{area}tasks/run-7-1/work"),
    };
    assert_eq!(spec.file_outputs[1].destination, work);
}

// A whole run: the session starts, the log is stored before each acknowledgement, live events
// change the records and the stream, and the reported end closes the run.
#[tokio::test]
async fn plays_whole_run() {
    let runs = runs().await;
    let record = runs.started().await;
    let Prepared::Ready(record) = runs.prepare(&record).await else {
        panic!("the run is prepared");
    };
    assert_eq!((record.state, record.attempts), (RunState::Running, 1));
    let mut frames = log_calls();
    frames.extend([
        event(
            1,
            LiveWorkflowEvent::RunStarted {
                run_id: record.run_id.to_string(),
                time_ms: 1,
            },
        ),
        event(2, task(TaskState::Requested, None)),
        event(3, task(TaskState::Started, None)),
        event(4, task(TaskState::Finished, Some(0))),
        call(
            3,
            HostRpcRequest::StorageStat {
                address: "aruna://results/hello/missing.txt".into(),
            },
        ),
        CoordinatorToHost::RunFinished(RunFinishedPayload {
            success: true,
            error: None,
            stopped: false,
            final_summary_json: json!({}),
        }),
    ]);
    runs.host.script(frames);
    let ended = runs.play(&record).await.expect("the run ends");
    assert_eq!(ended.state, RunState::Succeeded);
    assert!(ended.finished_at_ms.is_some());
    let step = &ended.steps[0];
    assert_eq!(
        (step.step.as_str(), step.succeeded, step.total()),
        ("echo", 1, 1)
    );

    let sent = runs.host.sent();
    let HostToCoordinator::RunStart(start) = &sent[0] else {
        panic!("the run starts first: {sent:?}");
    };
    assert_eq!(start.inputs, json!({"message": "Hola"}));
    assert!(start.prior_log_pages.is_empty() && start.resume_lineage.is_none());
    let answers: Vec<(u64, HostRpcResult)> = (sent.iter())
        .filter_map(|message| match message {
            HostToCoordinator::RpcResponse { call_id, result } => Some((*call_id, result.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        answers,
        vec![
            (1, HostRpcResult::RunLogAck { seq: 0 }),
            (2, HostRpcResult::RunLogAck { seq: 1 }),
            (3, HostRpcResult::StorageStat { entry: None }),
        ]
    );
    let pages = drive(LoadLogOperation::new(record.run_id), &runs.fixture.context)
        .await
        .unwrap();
    assert_eq!((pages.len(), pages[0].entries.len()), (1, 2));

    let events = runs.events(record.run_id, 0).await;
    let kinds: Vec<&str> = events.iter().map(|(_, kind)| kind.as_str()).collect();
    assert_eq!(
        kinds,
        [
            "run", "run", "state", "state", "task", "step", "task", "step", "task", "step", "state"
        ]
    );
    let tail = runs.events(record.run_id, 9).await;
    assert_eq!(
        tail,
        vec![(10, "step".to_owned()), (11, "state".to_owned())]
    );

    let context = &runs.fixture.context;
    let read = RunSnapshotOperation::new(&runs.fixture.alice, record.run_id);
    let snapshot = drive(read, context).await.unwrap();
    assert_eq!(snapshot.listed["echo"][0].state, RunState::Succeeded);
    let filter = TaskFilter {
        step: Some("echo".into()),
        state: Some(RunState::Succeeded),
    };
    let list = ListTasksOperation::new(&runs.fixture.alice, record.run_id, filter, (None, 10));
    let page = drive(list, context).await.unwrap();
    assert_eq!((page.total, page.items[0].task_id), (1, 1));
}

// A coordinator that broke off leaves the run for the next attempt, which replays the stored
// log; the last attempt fails the run.
#[tokio::test]
async fn recovers_from_log() {
    let runs = runs().await;
    let record = runs.started().await;
    let Prepared::Ready(record) = runs.prepare(&record).await else {
        panic!("the run is prepared");
    };
    runs.host.script(log_calls());
    assert_eq!(runs.play(&record).await, None);
    let context = &runs.fixture.context;
    let again = AppendLogOperation::new(
        record.run_id,
        RunLogEntry::Change {
            seq: 1,
            change_json: json!({"task": 1}),
        },
    );
    drive(again, context).await.unwrap();

    let interrupt = InterruptRunOperation::new(record.run_id, false, "channel closed");
    assert_eq!(drive(interrupt, context).await, Ok(Interrupted::Retry));
    let Prepared::Ready(resumed) = runs.prepare(&record).await else {
        panic!("the next attempt starts");
    };
    assert_eq!(resumed.attempts, 2);
    let sent = runs.host.sent();
    let Some(HostToCoordinator::RunStart(start)) = sent.last() else {
        panic!("the next attempt starts the run again: {sent:?}");
    };
    let Some(HostToCoordinator::RunReplayPages { pages }) = sent.get(sent.len().saturating_sub(2))
    else {
        panic!("replay pages precede RunStart: {sent:?}");
    };
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].entries.len(), 2);
    assert!(start.prior_log_pages.is_empty());
    assert_eq!(start.resume_lineage, Some(record.run_id.to_string()));

    let last = InterruptRunOperation::new(record.run_id, true, "channel closed");
    let ended = drive(last, context).await;
    assert_eq!(ended, Ok(Interrupted::Ended(RunState::Failed)));
}

// Cancel ends a run whose job never started, refuses an ended run, and delete waits for the end.
#[tokio::test]
async fn cancels_and_deletes() {
    let runs = runs().await;
    let context = &runs.fixture.context;
    let serving = runs.serving();
    let alice = runs.fixture.alice.clone();
    let queued = runs.started().await;
    let stranger = CancelRunOperation::new(&runs.fixture.stranger, queued.run_id, serving);
    assert_eq!(drive(stranger, context).await, Err(CatalogError::NotFound));
    let active = DeleteRunOperation::new(&alice, queued.run_id);
    assert!(matches!(
        drive(active, context).await,
        Err(CatalogError::Conflict(_))
    ));

    let cancel = CancelRunOperation::new(&alice, queued.run_id, serving);
    let cancelled = drive(cancel, context).await.unwrap();
    assert_eq!(cancelled.record.state, RunState::Cancelled);
    assert!(cancelled.running.is_empty());
    let again = CancelRunOperation::new(&alice, queued.run_id, serving);
    assert!(matches!(
        drive(again, context).await,
        Err(CatalogError::Conflict(_))
    ));

    let delete = DeleteRunOperation::new(&alice, queued.run_id);
    assert_eq!(drive(delete, context).await, Ok(()));
    let read = ReadEventsOperation::new(&alice, queued.run_id, 0, 10);
    assert_eq!(drive(read, context).await, Err(CatalogError::NotFound));
    let twice = DeleteRunOperation::new(&alice, queued.run_id);
    assert_eq!(drive(twice, context).await, Err(CatalogError::NotFound));
}

// Cancelling a running run stops its child jobs that have not ended, as its creator.
#[tokio::test]
async fn cancels_children() {
    let runs = runs().await;
    let context = &runs.fixture.context;
    let record = runs.started().await;
    let Prepared::Ready(record) = runs.prepare(&record).await else {
        panic!("the run is prepared");
    };
    let mut child = changed_task(&record, RunState::Running);
    child.child_job_id = Some(JobId::from_bytes([7; 16]));
    let change = ChangeRunOperation::new(record.run_id, vec![RunChange::Task(Box::new(child))]);
    drive(change, context).await.unwrap();
    let cancel = CancelRunOperation::new(&runs.fixture.alice, record.run_id, runs.serving());
    let cancelled = drive(cancel, context).await.unwrap();
    assert!(matches!(
        cancelled.record.state,
        RunState::Cancelling | RunState::Cancelled
    ));
}

fn changed_task(record: &WorkflowRunRecord, state: RunState) -> WorkflowTaskRecord {
    super::run::changed_task(
        record.run_id,
        &TaskChange {
            task_id: 7,
            name: "echo-7-1".into(),
            step: "echo".into(),
            state: match state {
                RunState::Running => TaskState::Started,
                _ => TaskState::Requested,
            },
            attempt: Some(1),
            time_ms: 1,
            started_ms: None,
            ended_ms: None,
            cached: false,
            exit_code: None,
            error: None,
        },
    )
}

pub(super) fn sample_run() -> WorkflowRunRecord {
    WorkflowRunRecord {
        run_id: Ulid::from_bytes([3; 16]),
        realm_id: aruna_core::structs::identity::realm::RealmId([5; 32]),
        group_id: Ulid::from_bytes([10; 16]),
        workflow_id: Ulid::from_bytes([4; 16]),
        workflow_name: "hello".into(),
        version: 1,
        lineage: Ulid::from_bytes([3; 16]),
        state: RunState::Running,
        node_id: aruna_ops_identity::testing::node(3),
        created_by: aruna_core::user::id::UserId::default(),
        created_at_ms: 1,
        finished_at_ms: None,
        error: None,
        values_json: "{}".into(),
        inputs_json: None,
        output: Some(RunOutput {
            bucket: "results".into(),
            prefix: "hello".into(),
            node_id: None,
            dataset_json: None,
        }),
        outputs: Vec::new(),
        provenance_crate: false,
        provenance_document_id: None,
        provenance_status: None,
        last_event_seq: 0,
        local_only: false,
        origin: None,
        parent_job_id: None,
        claim_token_digest: None,
        attempts: 1,
        steps: Vec::new(),
        resumes_run_id: None,
        repeats_run_id: None,
        cache_scope: Default::default(),
        placement_site: None,
        protected: false,
        started_at_ms: None,
        secrets: BTreeMap::new(),
        path_restrictions: None,
        captured_remote: Vec::new(),
        directory_pins: Vec::new(),
        directory_roots: Vec::new(),
        used_tools: Vec::new(),
    }
}

pub(super) fn start_task() -> StartTask {
    StartTask {
        task_key_seed: "{\"command\":\"echo hi\"}".into(),
        step_id: "echo".into(),
        task_id: Some(7),
        attempt: 1,
        name: "run-7-1".into(),
        spec_json: json!({}),
        command_wrapper_bytes: b"#!/bin/sh\necho hi\n".to_vec(),
        folders: TaskFolders {
            work: "/uwe/work".into(),
            control: "/uwe/control".into(),
            home: "/uwe/home".into(),
        },
        image: Some("debian:stable-slim".into()),
        resources: TaskResources {
            cpus: 2,
            ..TaskResources::default()
        },
        site: None,
        network_allowed: false,
        direct_inputs: vec![DirectInput {
            path: "in.txt".into(),
            address: format!("aruna://reads/a.txt?version={}", Ulid::from_bytes([8; 16])),
            digest: "sha256:00".into(),
            size: 3,
            executable: false,
            aruna_id: None,
            endpoints: Vec::new(),
        }],
        staged_files: vec![StagedFile {
            path: "script.py".into(),
            address: Some("aruna-staging://run-7-1/0".into()),
            directory: false,
            executable: true,
            size: 4,
        }],
        expected_outputs: Vec::new(),
        secret_placeholders: Vec::new(),
        step_settings: None,
    }
}

pub(super) fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

pub(super) fn ingress() -> IngressNode {
    let node = aruna_ops_identity::testing::node(3);
    IngressNode {
        local: node,
        realm_id: aruna_core::structs::identity::realm::RealmId([5; 32]),
        now_ms: 1,
        fresh: BTreeMap::new(),
    }
}

// The child spec carries the reserved tags, the wrapper as an inline file, the inputs by their
// exact versions and the attempt's folders as outputs in the run's area.
#[test]
fn builds_child_spec() {
    let run = sample_run();
    let request = start_task();
    let key = task_key_of(&request.task_key_seed);
    let spec = task_spec(&run, &request, &key).unwrap();
    assert_eq!(spec.tags[WORKFLOW_RUN_TAG], run.run_id.to_string());
    assert_eq!(spec.tags[WORKFLOW_TASK_TAG], key);
    assert_eq!(node_tag(&spec.tags), Some(WORKFLOW_RUN_TAG));
    assert_eq!(
        spec.entrypoint,
        Some(vec!["/bin/sh".into(), "/uwe/task.sh".into()])
    );
    assert!(spec.inputs[0].source.is_inline());
    let InputSource::S3 {
        bucket,
        key: object,
        version_id,
    } = &spec.inputs[1].source
    else {
        panic!("a direct input names its object");
    };
    assert_eq!((bucket.as_str(), object.as_str()), ("reads", "a.txt"));
    assert!(version_id.is_some());
    let (_, area) = area_of(&run).unwrap();
    assert_eq!(area, format!("hello/.uwe/runs/{}/", run.run_id));
    let InputSource::S3 { key: staged, .. } = &spec.inputs[2].source else {
        panic!("a staged file lands in the area");
    };
    assert_eq!(*staged, format!("{area}staging/run-7-1/0"));
    assert_eq!(spec.file_outputs.len(), 2);
    assert_eq!(spec.resources.cpu_cores, Some(2));
    assert_eq!(
        child_key(run.run_id, &key, 7, 1),
        child_key(run.run_id, &key, 7, 1)
    );
    assert_ne!(
        child_key(run.run_id, &key, 7, 1),
        child_key(run.run_id, &key, 7, 2)
    );
    assert!(resolve(&run, "ftp://x/y").is_err());
    assert!(resolve(&run, "aruna-staging://../x").is_err());
}

// Inline inputs are for plugin and workflow tasks only.
#[test]
fn inline_needs_tag() {
    let run = sample_run();
    let mut spec = task_spec(&run, &start_task(), "key").unwrap();
    let checked = crate::jobs::submit::execution::validate_execution(
        &mut spec.clone(),
        WorkspaceMode::None,
        None,
    );
    assert!(checked.is_ok(), "{checked:?}");
    spec.tags.clear();
    spec.inputs.push(InputSelection {
        source: InputSource::Inline { bytes: vec![1] },
        source_node_id: None,
        dest_key: "/x".into(),
        mode: InputMode::Snapshot,
        container_path: Some("/x".into()),
        name: None,
        description: None,
    });
    let refused =
        crate::jobs::submit::execution::validate_execution(&mut spec, WorkspaceMode::None, None);
    assert!(matches!(refused, Err(SubmitJobError::InvalidWorkspace(_))));
}

// An attempt whose child a former attempt of this node submitted adopts it and never submits.
#[test]
fn adopts_stored_child() {
    let run = sample_run();
    let mut task = RunTaskOperation::new(run.clone(), ingress(), Box::new(start_task()));
    task.stamp(5);
    assert!(matches!(task.first(), Ok(Next::Call(IoCall::Read(_)))));
    let job_id = JobId::from_bytes([9; 16]);
    let mut stored = changed_task(&run, RunState::Running);
    stored.child_job_id = Some(job_id);
    let value = stored.to_bytes().unwrap();
    let poll = task
        .next(IoResult::Values(vec![Some(value.into())]))
        .unwrap();
    let read = FamilyProjectionOperation::new(job_id, 5).for_reader(run.created_by);
    assert_eq!(poll, Next::Call(IoCall::child(read)));
    let unseen = task
        .next(child(Ok::<
            Option<FamilyView>,
            crate::jobs::route::JobRouteError,
        >(None)))
        .unwrap();
    assert_eq!(unseen, Next::Call(IoCall::Delay(POLL_DELAY)));
}

// A child refused for quota waits and is sent again; a plan conflict names the child to adopt.
#[test]
fn waits_for_quota() {
    let run = sample_run();
    let mut task = RunTaskOperation::new(run.clone(), ingress(), Box::new(start_task()));
    task.stamp(5);
    task.first().unwrap();
    let lookup = task.next(IoResult::Values(vec![None])).unwrap();
    assert!(matches!(lookup, Next::Call(IoCall::Child(_))));
    let miss = Ok::<Option<super::cache::CacheHit>, CatalogError>(None);
    let queued = task.next(child(miss)).unwrap();
    assert!(matches!(queued, Next::Call(IoCall::Child(_))));
    let check = task
        .next(child(Ok::<_, CatalogError>(run.clone())))
        .unwrap();
    assert!(matches!(check, Next::Call(IoCall::Read(_))));
    let submit = task.next(IoResult::Values(vec![None])).unwrap();
    assert!(matches!(submit, Next::Call(IoCall::Child(_))));
    let denied = QuotaDenied {
        scope: QuotaScope::Group,
        dimension: QuotaDimension::CpuCores,
        observed: 8,
        requested: 2,
        limit: 8,
    };
    let refused: Result<AcceptedSubmission, SubmitJobError> = Err(denied.into());
    let wait = task.next(child(refused)).unwrap();
    assert!(matches!(wait, Next::Call(IoCall::Delay(_))));
    let recheck = task.next(IoResult::Woke).unwrap();
    assert!(matches!(recheck, Next::Call(IoCall::Read(_))));
    let again = task.next(IoResult::Values(vec![None])).unwrap();
    assert!(matches!(again, Next::Call(IoCall::Child(_))));
    let existing = JobId::from_bytes([9; 16]);
    let conflict: Result<AcceptedSubmission, SubmitJobError> =
        Err(SubmitJobError::JobPlanConflict {
            existing_job_id: existing,
        });
    assert!(matches!(
        task.next(child(conflict)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let poll = task
        .next(child(Ok::<_, CatalogError>(run.clone())))
        .unwrap();
    let read = FamilyProjectionOperation::new(existing, 5).for_reader(run.created_by);
    assert_eq!(poll, Next::Call(IoCall::child(read)));
}

// Upload chunks collect until the last one, and a chunk of no open upload is refused.
#[test]
fn collects_uploads() {
    let run = sample_run();
    let target = resolve(&run, "aruna-staging://run-7-1/0").unwrap();
    let mut uploads = Uploads::default();
    uploads.open(4, target.clone());
    let id = super::run::upload_id(4);
    let first = uploads.chunk(&id, 0, b"ab".to_vec(), false);
    assert_eq!(
        first,
        Chunk::Answer(HostRpcResult::UploadAck {
            seq: 0,
            receipt: None
        })
    );
    let last = uploads.chunk(&id, 1, b"c".to_vec(), true);
    assert_eq!(
        last,
        Chunk::Store {
            target,
            bytes: b"abc".to_vec(),
            seq: 1
        }
    );
    assert!(matches!(
        uploads.chunk(&id, 2, Vec::new(), true),
        Chunk::Answer(HostRpcResult::Failed(_))
    ));
}

// A stale report of a task never replaces a newer one, and the tails read back by offset.
#[tokio::test]
async fn keeps_newest_attempt() {
    let runs = runs().await;
    let context = &runs.fixture.context;
    let record = runs.started().await;
    let mut done = changed_task(&record, RunState::Running);
    done.state = RunState::Succeeded;
    done.stdout_tail = b"hello world".to_vec();
    done.child_job_id = Some(JobId::from_bytes([7; 16]));
    let late = changed_task(&record, RunState::Running);
    let changes = vec![RunChange::Task(Box::new(done))];
    drive(ChangeRunOperation::new(record.run_id, changes), context)
        .await
        .unwrap();
    let changes = vec![RunChange::Task(Box::new(late))];
    let changed = drive(ChangeRunOperation::new(record.run_id, changes), context)
        .await
        .unwrap();
    assert_eq!(
        (changed.steps[0].succeeded, changed.steps[0].running),
        (1, 0)
    );
    let read = super::reads::GetTaskOperation::new(&runs.fixture.alice, record.run_id, 7);
    let stored = drive(read, context).await.unwrap();
    assert_eq!(stored.state, RunState::Succeeded);
    let chunk = log_chunk(
        &stored,
        aruna_workflow_contract::message::LogStream::Stdout,
        6,
    );
    assert_eq!(
        (chunk.text.as_str(), chunk.next_offset, chunk.complete),
        ("world", 11, true)
    );
}

// A path-restricted token starts a run only with the exact workflow marker, and never names an
// input of another node, which would be staged later as the unrestricted creator.
#[tokio::test]
async fn restricted_needs_marker() {
    use aruna_core::structs::identity::auth::{PathRestriction, Permission};
    use aruna_core::structs::storage::blob::bucket_permission_path;

    let runs = runs().await;
    let (realm_id, group) = (runs.fixture.alice.realm_id, runs.fixture.group_a);
    let results = bucket_permission_path(realm_id, group, runs.fixture.node, "results");
    let restricted = |patterns: Vec<String>| {
        let mut auth = runs.fixture.alice.clone();
        let restrictions = (patterns.into_iter())
            .map(|pattern| PathRestriction {
                pattern,
                permission: Permission::WRITE,
            })
            .collect();
        auth.path_restrictions = Some(restrictions);
        auth
    };
    let request = || runs.request(None, json!({"message": "Hola"}));
    let folder_only = restricted(vec![format!("{results}/**")]);
    let refused = runs.start(&folder_only, request()).await;
    assert_eq!(refused.unwrap_err(), CatalogError::NotFound);

    let marked = restricted(vec![
        crate::workflows::workflow_path(realm_id, group),
        format!("{results}/**"),
    ]);
    let admitted = runs.start(&marked, request()).await;
    assert!(admitted.is_ok(), "{admitted:?}");

    let mut remote = request();
    remote.inputs.push(super::admit::PinnedInput {
        name: "reads".into(),
        bucket: "results".into(),
        key: "reads.fastq".into(),
        version_id: Ulid::from_bytes([4; 16]),
        source_node_id: Some(aruna_ops_identity::testing::node(8)),
    });
    let elsewhere = runs.start(&marked, remote).await;
    assert_eq!(elsewhere.unwrap_err(), CatalogError::Forbidden);
}

// Secret bindings flow from admission to coordinator start and child task spec; unbound or
// reserved names and restricted tokens are refused.
#[tokio::test]
async fn binds_workflow_secrets() {
    use aruna_core::secrets::SecretName;
    use aruna_core::structs::execution::job::input::parse_workflow_secrets;
    use aruna_core::structs::execution::workflow_run::{
        WorkflowSecretBinding, WorkflowSecretScope,
    };
    use aruna_core::structs::identity::auth::{PathRestriction, Permission};
    use aruna_workflow_contract::host::SecretPlaceholder;

    let runs = runs().await;
    let binding = |scope, name: &str| WorkflowSecretBinding {
        scope,
        name: SecretName::try_from(name.to_string()).unwrap(),
        field: None,
    };
    let mut bad = runs.request(None, json!({}));
    bad.secrets.insert(
        "ARUNA_TOKEN".into(),
        binding(WorkflowSecretScope::User, "tok"),
    );
    assert!(matches!(
        runs.start(&runs.fixture.alice, bad).await,
        Err(CatalogError::Invalid(_))
    ));

    let mut req = runs.request(None, json!({}));
    req.secrets
        .insert("API_KEY".into(), binding(WorkflowSecretScope::User, "tok"));
    let mut restricted = runs.fixture.alice.clone();
    restricted.path_restrictions = Some(vec![PathRestriction {
        pattern: "/**".into(),
        permission: Permission::WRITE,
    }]);
    assert_eq!(
        runs.start(&restricted, req.clone()).await.unwrap_err(),
        CatalogError::Forbidden
    );

    let admitted = runs.start(&runs.fixture.alice, req).await.unwrap().record;
    assert!(matches!(runs.prepare(&admitted).await, Prepared::Ready(_)));
    let sent = runs.host.sent();
    let Some(HostToCoordinator::RunStart(start)) = sent.first() else {
        panic!("expected RunStart, got {sent:?}");
    };
    assert!(start.network_isolated);
    assert_eq!(start.secret_names, ["API_KEY"]);

    let mut task = start_task();
    task.secret_placeholders.push(SecretPlaceholder {
        name: "API_KEY".into(),
        placeholder: "__ARUNA_SECRET_x_API_KEY__".into(),
        env_name: Some("MY_KEY".into()),
    });
    let spec = task_spec(&admitted, &task, "k").unwrap();
    let parsed = parse_workflow_secrets(&spec.tags).unwrap();
    assert_eq!(parsed["MY_KEY"].name.as_str(), "tok");
    assert_eq!(parsed["MY_KEY"].scope, WorkflowSecretScope::User);

    let mut unbound = start_task();
    unbound.secret_placeholders.push(SecretPlaceholder {
        name: "OTHER".into(),
        placeholder: "__ARUNA_SECRET_x_OTHER__".into(),
        env_name: None,
    });
    assert!(task_spec(&admitted, &unbound, "k").is_err());
}

// Remote pins in `values` or `inputs` require a bearer token on new runs and refresh expired
// grants on repeat instead of reusing stale grants.
#[tokio::test]
async fn refreshes_remote_grants() {
    use aruna_core::id::NodeId;
    use aruna_core::metadata::AuthToken;
    use aruna_core::structs::execution::job::family::SubmissionId;
    use aruna_core::structs::execution::job::input::{
        CapturedInput, InputContent, InputMode, InputSelection, InputSource,
    };
    use aruna_core::structs::execution::workflow_run::PrecapturedInput;
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNode, RealmNodeKind};
    use aruna_core::structs::storage::read_grant::{GrantScope, ReadGrant};
    use aruna_core::structs::storage::replication::VersionedObjectArn;

    let runs = runs().await;
    let realm_id = runs.fixture.alice.realm_id;
    let group_id = runs.fixture.group_a;
    let peer: NodeId = aruna_ops_identity::testing::node(8);
    let mut cfg = RealmConfigDocument::new(realm_id, Vec::new(), 1);
    cfg.nodes.push(RealmNode {
        node_id: runs.fixture.node.to_string(),
        kind: RealmNodeKind::Server,
    });
    cfg.nodes.push(RealmNode {
        node_id: peer.to_string(),
        kind: RealmNodeKind::Server,
    });
    let ver = Ulid::from_bytes([6; 16]);
    let mut req = runs.request(
        None,
        json!({"file": format!("aruna://data/f.bin?version={ver}&node={peer}")}),
    );
    // New run with remote file-valued parameter and no bearer token is rejected at admission.
    let mut no_bearer = StartRunOperation::new(&runs.fixture.alice, runs.serving(), req.clone());
    no_bearer.stamp(10_000);
    assert!(matches!(
        no_bearer.queue_remote(&cfg, false),
        Err(CatalogError::Invalid(_))
    ));

    // With a bearer token, an expired grant is dropped and queued for refresh from the holder.
    let expired_grant = ReadGrant {
        realm_id,
        issuer: peer,
        submission_id: SubmissionId([1; 32]),
        user_id: runs.fixture.alice.user_id,
        restrictions: None,
        scope: GrantScope::Version(VersionedObjectArn {
            realm_id,
            node_id: peer,
            bucket: "data".into(),
            key: "f.bin".into(),
            version: ver,
        }),
        operations: vec!["s3.GetObject".into()],
        expires_at: 5,
    }
    .sign(&iroh::SecretKey::from_bytes(&[8; 32]))
    .unwrap();
    let selection = InputSelection {
        source: InputSource::S3 {
            bucket: "data".into(),
            key: "f.bin".into(),
            version_id: Some(ver.to_string()),
        },
        source_node_id: Some(peer),
        dest_key: "data/f.bin".into(),
        mode: InputMode::Snapshot,
        container_path: Some("/data/f.bin".into()),
        name: None,
        description: None,
    };
    let stale = PrecapturedInput {
        pin_node_id: peer,
        pin_bucket: "data".into(),
        pin_key: "f.bin".into(),
        pin_version_id: ver,
        resolved: selection,
        captured: CapturedInput {
            destination_key: "data/f.bin".into(),
            source_node_id: peer,
            version_id: ver,
            content: InputContent::Reference {
                etag: None,
                range_bound: false,
            },
            bytes: 10,
            policies: Vec::new(),
            source_group_id: group_id,
            public: false,
            link: None,
            source_pin: None,
            served_by: None,
            database: None,
            releases: Vec::new(),
            releases_complete: false,
            grant: Some(expired_grant),
        },
    };
    let bearer = Some(AuthToken::bearer("fresh-token").unwrap());
    let mut refresh = StartRunOperation::new(&runs.fixture.alice, runs.serving(), req.clone())
        .with_token(bearer)
        .with_captured(vec![stale]);
    refresh.stamp(10_000);
    refresh.queue_remote(&cfg, false).unwrap();
    assert!(refresh.captured.is_empty(), "expired grant must be dropped");
    assert_eq!(refresh.remote.get(&peer).map(Vec::len), Some(1));

    // Fresh grant is reused without re-queuing.
    let mut fresh_grant = ReadGrant {
        realm_id,
        issuer: peer,
        submission_id: SubmissionId([1; 32]),
        user_id: runs.fixture.alice.user_id,
        restrictions: None,
        scope: GrantScope::Version(VersionedObjectArn {
            realm_id,
            node_id: peer,
            bucket: "data".into(),
            key: "f.bin".into(),
            version: ver,
        }),
        operations: vec!["s3.GetObject".into()],
        expires_at: 999,
    }
    .sign(&iroh::SecretKey::from_bytes(&[8; 32]))
    .unwrap();
    let mut reused = PrecapturedInput {
        pin_node_id: peer,
        pin_bucket: "data".into(),
        pin_key: "f.bin".into(),
        pin_version_id: ver,
        resolved: InputSelection {
            source: InputSource::S3 {
                bucket: "data".into(),
                key: "f.bin".into(),
                version_id: Some(ver.to_string()),
            },
            source_node_id: Some(peer),
            dest_key: "data/f.bin".into(),
            mode: InputMode::Snapshot,
            container_path: Some("/data/f.bin".into()),
            name: None,
            description: None,
        },
        captured: CapturedInput {
            destination_key: "data/f.bin".into(),
            source_node_id: peer,
            version_id: ver,
            content: InputContent::Reference {
                etag: None,
                range_bound: false,
            },
            bytes: 10,
            policies: Vec::new(),
            source_group_id: group_id,
            public: false,
            link: None,
            source_pin: None,
            served_by: None,
            database: None,
            releases: Vec::new(),
            releases_complete: false,
            grant: Some(fresh_grant.clone()),
        },
    };
    fresh_grant.grant.expires_at = 999;
    reused.captured.grant = Some(fresh_grant);
    req.values = json!({"file": format!("s3://data/f.bin?version={ver}&node={peer}")});
    let mut keep = StartRunOperation::new(&runs.fixture.alice, runs.serving(), req)
        .with_captured(vec![reused]);
    keep.stamp(10_000);
    keep.queue_remote(&cfg, false).unwrap();
    assert_eq!(keep.captured.len(), 1);
    assert!(keep.remote.is_empty());
}

// Workflow addresses encode `?`, `%`, and `#` in object keys so pinned inputs and data calls
// round-trip special prefixes losslessly while still refusing encoded `..` traversal.
#[tokio::test]
async fn round_trips_addresses() {
    let runs = runs().await;
    let mut record = runs.started().await;
    let peer = aruna_ops_identity::testing::node(9);
    let ver = Ulid::from_bytes([33; 16]);
    let key = "team\\west?/a?b%25#1/file.txt";
    record.inputs_json = Some(
        json!({
            "local": {
                "bucket": "results",
                "key": key,
                "version_id": ver.to_string(),
            },
            "remote": {
                "bucket": "results",
                "key": key,
                "version_id": ver.to_string(),
                "source_node_id": peer.to_string(),
            }
        })
        .to_string(),
    );
    let values = super::run::run_inputs(&record).unwrap();
    let local_addr = values["local"].as_str().unwrap();
    let remote_addr = values["remote"].as_str().unwrap();
    let local_target = resolve(&record, local_addr).unwrap();
    assert_eq!(local_target.bucket, "results");
    assert_eq!(local_target.key, key);
    assert_eq!(local_target.version, Some(ver));
    assert_eq!(local_target.node, None);
    let remote_target = resolve(&record, remote_addr).unwrap();
    assert_eq!(remote_target.bucket, "results");
    assert_eq!(remote_target.key, key);
    assert_eq!(remote_target.version, Some(ver));
    assert_eq!(remote_target.node, Some(peer));
    assert!(resolve(&record, "aruna://results/%2E%2E/secret.txt").is_err());
    assert!(resolve(&record, "aruna://results/bad%2").is_err());

    // Admission rejects malformed `aruna://` and `s3://` values instead of dropping their checks.
    for bad in [
        "aruna://results/%2E%2E/secret.txt",
        "s3://results/bad%2",
        "aruna://results/ok.txt?unknown=1",
    ] {
        let req = runs.request(None, json!({ "input": bad }));
        let err = drive(
            StartRunOperation::new(&runs.fixture.alice, runs.serving(), req),
            &runs.fixture.context,
        )
        .await;
        assert!(
            matches!(err, Err(CatalogError::Invalid(_))),
            "{bad} => {err:?}"
        );
    }
}

/// Seeds a local materialized object version so `HeadObjectOperation` resolves its head version.
async fn seed_object(runs: &Runs, bucket: &str, key: &str, version_id: Ulid) {
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    };
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::storage::blob::{
        BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion,
        CurrentVersionPointer, VersionKey,
    };
    use aruna_core::structs::storage::format::{EncodingClass, StoredFormat};
    use std::collections::HashMap;
    use std::time::SystemTime;

    let mut hashes = HashMap::new();
    hashes.insert(HASH_BLAKE3.to_string(), vec![7; 32]);
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: bucket.to_string(),
        backend_path: key.to_string(),
        ulid: version_id,
        format: StoredFormat::default(),
        created_at: SystemTime::now(),
        created_by: Default::default(),
        staging: false,
        partial: false,
        blob_size: 4,
        hashes,
    };
    let head_key = BlobHeadKey::new(bucket, key).to_bytes().unwrap();
    write(
        &runs.fixture.context,
        BLOB_HEAD_KEYSPACE,
        head_key,
        CurrentVersionPointer::new(version_id).to_bytes().unwrap(),
    )
    .await;
    write(
        &runs.fixture.context,
        BLOB_VERSIONS_KEYSPACE,
        VersionKey::new(bucket, key, version_id).to_bytes().unwrap(),
        BlobVersion::materialized(
            [7; 32],
            BackendRef::node_default(),
            EncodingClass::Raw,
            location.created_at,
            location.created_by,
            None,
        )
        .to_bytes()
        .unwrap(),
    )
    .await;
    write(
        &runs.fixture.context,
        BLOB_LOCATIONS_KEYSPACE,
        BlobLocationKey::from_blake3(&[7; 32], EncodingClass::Raw, BackendRef::node_default())
            .unwrap()
            .to_bytes(),
        location.to_bytes().unwrap(),
    )
    .await;
}

// Cancelling a run on a replica holder refuses with Conflict without altering stored state.
#[tokio::test]
async fn rejects_replica_cancel() {
    let runs = runs().await;
    let record = runs.started().await;
    let other_node = aruna_ops_identity::testing::node(9);
    let err = drive(
        CancelRunOperation::new(
            &runs.fixture.alice,
            record.run_id,
            (other_node, runs.fixture.alice.realm_id),
        ),
        &runs.fixture.context,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, CatalogError::Conflict(_)), "{err:?}");
    let after = drive(
        RunSnapshotOperation::new(&runs.fixture.alice, record.run_id),
        &runs.fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(after.record.state, RunState::Queued);
}

// Admission pins local unversioned file values without changing idempotency digests, and
// cross-node resume attaches the original coordinator node to local file-valued parameters.
#[tokio::test]
async fn pins_value_versions() {
    use super::control::{Follow, FollowRunOperation};
    use aruna_core::keyspaces::WORKFLOW_RUN_KEYSPACE;

    let runs = runs().await;
    let version = Ulid::from_bytes([51; 16]);
    seed_object(&runs, "results", "inputs/a.txt", version).await;
    let req = runs.request(
        Some("pin-key"),
        json!({
            "one": "s3://results/inputs/a.txt",
            "many": ["aruna://results/inputs/a.txt"],
            "plain": "hello",
        }),
    );
    let first = runs.start(&runs.fixture.alice, req.clone()).await.unwrap();
    assert!(first.created);
    let values: Value = serde_json::from_str(&first.record.values_json).unwrap();
    assert_eq!(
        values["one"],
        format!("s3://results/inputs/a.txt?version={version}")
    );
    assert_eq!(
        values["many"][0],
        format!("aruna://results/inputs/a.txt?version={version}")
    );
    assert_eq!(values["plain"], "hello");

    // Retrying with the same unversioned request hits the idempotency key cleanly.
    let retry = runs.start(&runs.fixture.alice, req).await.unwrap();
    assert!(!retry.created);
    assert_eq!(retry.record.run_id, first.record.run_id);

    // Cross-node resume attaches `node=<origin>` to values held on the original coordinator node.
    let origin = aruna_ops_identity::testing::node(9);
    let peer = aruna_ops_identity::testing::node(10);
    let mut copy = first.record.clone();
    copy.run_id = Ulid::from_bytes([52; 16]);
    copy.lineage = copy.run_id;
    copy.node_id = origin;
    copy.state = RunState::Running;
    copy.values_json = json!({
        "local": format!("s3://results/inputs/a.txt?version={version}"),
        "already": format!("aruna://results/inputs/a.txt?version={version}&node={peer}"),
        "nested": [{"f": format!("aruna://results/inputs/a.txt?version={version}")}],
    })
    .to_string();
    write(
        &runs.fixture.context,
        WORKFLOW_RUN_KEYSPACE,
        super::run_key(copy.run_id).to_vec(),
        copy.to_bytes().unwrap(),
    )
    .await;
    let mut rewritten = json!({
        "local": format!("s3://results/inputs/a.txt?version={version}"),
        "already": format!("aruna://results/inputs/a.txt?version={version}&node={peer}"),
        "nested": [{"f": format!("aruna://results/inputs/a.txt?version={version}")}],
    });
    super::control::values_held_by(&mut rewritten, origin);
    assert_eq!(
        rewritten["local"],
        format!("s3://results/inputs/a.txt?version={version}&node={origin}")
    );
    assert_eq!(
        rewritten["already"],
        format!("aruna://results/inputs/a.txt?version={version}&node={peer}")
    );
    assert_eq!(
        rewritten["nested"][0]["f"],
        format!("aruna://results/inputs/a.txt?version={version}&node={origin}")
    );

    // End-to-end: resuming `copy` on a different node rewrites local value addresses to `origin`,
    // so admission enforces remote capture instead of silently reading locally.
    let err = drive(
        FollowRunOperation::new(
            &runs.fixture.alice,
            runs.serving(),
            copy.run_id,
            Follow::Resume,
        ),
        &runs.fixture.context,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, CatalogError::Invalid(_)), "{err:?}");
}

// Coordinator uploads targeting a remote node are rejected before UploadStarted or writing locally.
#[tokio::test]
async fn rejects_remote_upload() {
    use super::data::{WriteBody, WriteObjectOperation, upload_target};
    use super::run::OpenUploadOperation;

    let runs = runs().await;
    let record = runs.started().await;
    let remote = aruna_ops_identity::testing::node(9);
    let addr = format!("s3://results/hello/out.txt?node={remote}");
    assert!(upload_target(&record, runs.fixture.node, &addr).is_err());

    let opened = drive(
        OpenUploadOperation::new(record.clone(), runs.fixture.node, 7, addr.clone()),
        &runs.fixture.context,
    )
    .await
    .unwrap();
    assert!(opened.is_none());
    assert!(runs.host.sent().iter().any(|msg| matches!(
        msg,
        HostToCoordinator::RpcResponse {
            call_id: 7,
            result: HostRpcResult::Failed(_),
        }
    )));

    let target = resolve(&record, &addr).unwrap();
    let result = drive(
        WriteObjectOperation::new(
            (record, runs.fixture.node),
            NodeRouting::default(),
            target,
            WriteBody::Bytes(b"abc".to_vec()),
        ),
        &runs.fixture.context,
    )
    .await
    .unwrap();
    assert!(matches!(result, HostRpcResult::Failed(_)), "{result:?}");
}

// Explicitly staged empty directories are created by a launcher script while preserving wrapper $0.
#[tokio::test]
async fn stages_empty_directories() {
    let runs = runs().await;
    let record = runs.started().await;
    let mut task = start_task();
    task.staged_files.push(StagedFile {
        path: "nested/dir with 'quotes'".into(),
        address: None,
        directory: true,
        executable: false,
        size: 0,
    });
    let spec = task_spec(&record, &task, "k").unwrap();
    assert_eq!(
        spec.entrypoint.as_deref(),
        Some(&["/bin/sh".to_owned(), "/uwe/task.sh".to_owned()][..])
    );
    let inline = |path: &str| {
        spec.inputs.iter().find_map(|file| match &file.source {
            InputSource::Inline { bytes } if file.container_path.as_deref() == Some(path) => {
                Some(bytes.as_slice())
            }
            _ => None,
        })
    };
    let wrapper = inline("/uwe/wrapper.sh").expect("original wrapper stays at /uwe/wrapper.sh");
    assert_eq!(wrapper, task.command_wrapper_bytes.as_slice());
    let launcher = inline("/uwe/task.sh").expect("launcher script");
    let text = String::from_utf8(launcher.to_vec()).unwrap();
    assert!(
        text.contains("mkdir -p -- '/uwe/work/nested/dir with '\\''quotes'\\'''"),
        "{text}"
    );
    assert!(text.contains("exec /bin/sh '/uwe/wrapper.sh'"));

    let mut bad_dir = start_task();
    bad_dir.staged_files.push(StagedFile {
        path: "../escape".into(),
        address: None,
        directory: true,
        executable: false,
        size: 0,
    });
    assert!(task_spec(&record, &bad_dir, "k").is_err());

    let mut missing_addr = start_task();
    missing_addr.staged_files.push(StagedFile {
        path: "plain.txt".into(),
        address: None,
        directory: false,
        executable: false,
        size: 0,
    });
    assert!(task_spec(&record, &missing_addr, "k").is_err());
}

// Deleting a run lets the same idempotency key be reused for a fresh run instead of failing NotFound.
#[tokio::test]
async fn reuses_deleted_idempotency() {
    let runs = runs().await;
    let req = runs.request(Some("idem-del"), json!({"message": "first"}));
    let first = runs.start(&runs.fixture.alice, req.clone()).await.unwrap();
    drive(
        CancelRunOperation::new(&runs.fixture.alice, first.record.run_id, runs.serving()),
        &runs.fixture.context,
    )
    .await
    .unwrap();
    drive(
        DeleteRunOperation::new(&runs.fixture.alice, first.record.run_id),
        &runs.fixture.context,
    )
    .await
    .unwrap();

    // Retrying with the same key and even different values succeeds cleanly with a new run.
    let req2 = runs.request(Some("idem-del"), json!({"message": "second"}));
    let second = runs.start(&runs.fixture.alice, req2).await.unwrap();
    assert!(second.created);
    assert_ne!(second.record.run_id, first.record.run_id);
}

// Local directory-valued parameters capture versioned entries at admission so cross-node resume
// can list the remote folder and resolve exact versions for child reads and task staging.
#[tokio::test]
async fn resumes_directory_values() {
    use super::control::{Follow, FollowRunOperation};
    use super::data::DataCall;
    use super::remote::{RemoteReadOperation, resolve_precaptured};
    use aruna_core::keyspaces::WORKFLOW_RUN_KEYSPACE;

    let runs = runs().await;
    let ver_a = Ulid::from_bytes([61; 16]);
    let ver_b = Ulid::from_bytes([62; 16]);
    seed_object(&runs, "results", "folder/a.txt", ver_a).await;
    seed_object(&runs, "results", "folder/sub/b.txt", ver_b).await;

    let req = runs.request(None, json!({ "dir": "s3://results/folder/" }));
    let admitted = runs.start(&runs.fixture.alice, req).await.unwrap().record;
    assert_eq!(
        admitted.directory_pins.len(),
        2,
        "{:?}",
        admitted.directory_pins
    );
    assert!(
        admitted
            .directory_pins
            .iter()
            .any(|p| p.key == "folder/a.txt" && p.version_id == ver_a)
    );
    assert!(
        admitted
            .directory_pins
            .iter()
            .any(|p| p.key == "folder/sub/b.txt" && p.version_id == ver_b)
    );

    // Replicate as an active run on another realm server and resume it here.
    let origin = aruna_ops_identity::testing::node(9);
    let realm_id = runs.fixture.alice.realm_id;
    let mut cfg = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    cfg.ensure_node(runs.fixture.node, RealmNodeKind::Server);
    cfg.ensure_node(origin, RealmNodeKind::Server);
    cfg.seed_default_placement();
    cfg.seed_job_control(runs.fixture.node, 0);
    let actor = Actor {
        node_id: runs.fixture.node,
        user_id: runs.fixture.alice.user_id,
        realm_id,
    };
    write(
        &runs.fixture.context,
        REALM_CONFIG_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        cfg.to_bytes(&actor).unwrap(),
    )
    .await;
    let mut copy = admitted.clone();
    copy.run_id = Ulid::from_bytes([63; 16]);
    copy.lineage = copy.run_id;
    copy.node_id = origin;
    copy.state = RunState::Running;
    write(
        &runs.fixture.context,
        WORKFLOW_RUN_KEYSPACE,
        super::run_key(copy.run_id).to_vec(),
        copy.to_bytes().unwrap(),
    )
    .await;
    let resumed = drive(
        FollowRunOperation::new(
            &runs.fixture.alice,
            runs.serving(),
            copy.run_id,
            Follow::Resume,
        ),
        &runs.fixture.context,
    )
    .await
    .unwrap();
    let resumed_values: Value = serde_json::from_str(&resumed.values_json).unwrap();
    assert_eq!(
        resumed_values["dir"],
        format!("s3://{origin}@results/folder/")
    );
    assert_eq!(resumed.directory_pins.len(), 2);
    assert!(
        resumed
            .directory_pins
            .iter()
            .all(|p| p.source_node_id == Some(origin))
    );

    // RemoteReadOperation live-authorizes captured entries on the holder before serving stat/list.
    use aruna_core::replication_protocol::BaoReadRefusal;
    use aruna_ops_data::replication::bao_read::{BaoReadError, BaoReadOutput};
    let ok_meta = || {
        IoResult::Child(ChildOutcome::new(Ok::<BaoReadOutput, BaoReadError>(
            BaoReadOutput::Metadata {
                size: 4,
                blake3: [7; 32],
                etag: None,
                hashes: Default::default(),
            },
        )))
    };
    let denied_meta = || {
        IoResult::Child(ChildOutcome::new(Err::<BaoReadOutput, BaoReadError>(
            BaoReadError::Refused(BaoReadRefusal::ReadDenied),
        )))
    };
    let remote_dir = format!("s3://{origin}@results/folder/");
    let dir_target = resolve(&resumed, &remote_dir).unwrap();
    assert_eq!(dir_target.node, Some(origin));
    assert_eq!(dir_target.bucket, "results");
    assert_eq!(dir_target.key, "folder/");
    let mut stat_op = RemoteReadOperation::new(
        resumed.clone(),
        dir_target.clone(),
        origin,
        DataCall::Stat {
            address: remote_dir.clone(),
        },
    );
    assert!(matches!(
        stat_op.first().unwrap(),
        Next::Call(IoCall::Child(_))
    ));
    match stat_op.next(ok_meta()).unwrap() {
        Next::Done(HostRpcResult::StorageStat { entry: Some(entry) }) => {
            assert!(entry.directory);
        }
        other => panic!("expected directory stat, got {other:?}"),
    }
    let mut list_op = RemoteReadOperation::new(
        resumed.clone(),
        dir_target.clone(),
        origin,
        DataCall::List {
            address: remote_dir.clone(),
        },
    );
    assert!(matches!(
        list_op.first().unwrap(),
        Next::Call(IoCall::Child(_))
    ));
    assert!(matches!(
        list_op.next(ok_meta()).unwrap(),
        Next::Call(IoCall::Child(_))
    ));
    match list_op.next(ok_meta()).unwrap() {
        Next::Done(HostRpcResult::StorageList { entries }) => {
            assert_eq!(entries.len(), 2, "{entries:?}");
            assert!(entries.iter().any(|e| e.name == "a.txt" && !e.directory));
            assert!(entries.iter().any(|e| e.name == "sub" && e.directory));
        }
        other => panic!("expected directory listing, got {other:?}"),
    }

    // Revoked READ on a remote empty directory marker hides it from both Stat and List.
    let marker_ver = Ulid::from_bytes([66; 16]);
    let mut with_empty = resumed.clone();
    with_empty.directory_roots.push(
        aruna_core::structs::execution::workflow_run::DirectoryRoot {
            bucket: "results".into(),
            prefix: "empty_remote/".into(),
            version_id: Some(marker_ver),
            source_node_id: Some(origin),
        },
    );
    with_empty.directory_roots.push(
        aruna_core::structs::execution::workflow_run::DirectoryRoot {
            bucket: "results".into(),
            prefix: "unversioned_empty/".into(),
            version_id: None,
            source_node_id: Some(origin),
        },
    );
    let empty_addr = format!("s3://{origin}@results/empty_remote/");
    let empty_target = resolve(&with_empty, &empty_addr).unwrap();
    let mut denied_stat = RemoteReadOperation::new(
        with_empty.clone(),
        empty_target.clone(),
        origin,
        DataCall::Stat {
            address: empty_addr.clone(),
        },
    );
    assert!(matches!(
        denied_stat.first().unwrap(),
        Next::Call(IoCall::Child(_))
    ));
    assert_eq!(
        denied_stat.next(denied_meta()).unwrap(),
        Next::Done(HostRpcResult::StorageStat { entry: None })
    );
    let mut denied_list = RemoteReadOperation::new(
        with_empty.clone(),
        empty_target,
        origin,
        DataCall::List {
            address: empty_addr,
        },
    );
    assert!(matches!(
        denied_list.first().unwrap(),
        Next::Call(IoCall::Child(_))
    ));
    assert!(matches!(
        denied_list.next(denied_meta()).unwrap(),
        Next::Done(HostRpcResult::Failed(
            aruna_workflow_contract::ContractError::Denied { .. }
        ))
    ));
    // Unversioned remote empty roots are never exposed without holder authorization.
    let unv_addr = format!("s3://{origin}@results/unversioned_empty/");
    let unv_target = resolve(&with_empty, &unv_addr).unwrap();
    let mut unv_list = RemoteReadOperation::new(
        with_empty,
        unv_target,
        origin,
        DataCall::List { address: unv_addr },
    );
    assert!(matches!(
        unv_list.first().unwrap(),
        Next::Done(HostRpcResult::Failed(_))
    ));

    // Unversioned child reads and task DirectInputs resolve to the captured exact version.
    let child_addr = format!("s3://results/folder/a.txt?node={origin}");
    let child = resolve_precaptured(
        &resumed,
        resolve(&resumed, &child_addr).unwrap(),
        runs.fixture.node,
    );
    assert_eq!(child.version, Some(ver_a));
    assert_eq!(child.node, Some(origin));

    let mut task = start_task();
    task.direct_inputs.push(DirectInput {
        path: "a.txt".into(),
        address: child_addr,
        digest: "sha256:00".into(),
        size: 4,
        executable: false,
        aruna_id: None,
        endpoints: Vec::new(),
    });
    let spec = task_spec(&resumed, &task, "k").unwrap();
    assert!(spec.inputs.iter().any(|i| matches!(
        &i.source,
        InputSource::S3 { bucket, key, version_id }
            if bucket == "results" && key == "folder/a.txt" && version_id.as_deref() == Some(&ver_a.to_string())
    )));

    // A precaptured remote link keeps its original pin identity in `task_spec` for grant lookup.
    let link_ver = Ulid::from_bytes([64; 16]);
    let real_ver = Ulid::from_bytes([65; 16]);
    let mut linked = resumed.clone();
    let target_sel = InputSelection {
        source: InputSource::S3 {
            bucket: "results".into(),
            key: "folder/real.txt".into(),
            version_id: Some(real_ver.to_string()),
        },
        source_node_id: Some(origin),
        dest_key: "results/folder/alias.link".into(),
        mode: InputMode::Snapshot,
        container_path: None,
        name: None,
        description: None,
    };
    let mut cap = crate::tests::records::captured_input(&target_sel, origin);
    cap.version_id = real_ver;
    linked.captured_remote.push(
        aruna_core::structs::execution::workflow_run::PrecapturedInput {
            pin_node_id: origin,
            pin_bucket: "results".into(),
            pin_key: "folder/alias.link".into(),
            pin_version_id: link_ver,
            resolved: target_sel,
            captured: cap,
        },
    );
    let mut link_task = start_task();
    link_task.direct_inputs.push(DirectInput {
        path: "alias.link".into(),
        address: format!("aruna://results/folder/alias.link?version={link_ver}&node={origin}"),
        digest: "sha256:00".into(),
        size: 4,
        executable: false,
        aruna_id: None,
        endpoints: Vec::new(),
    });
    let link_spec = task_spec(&linked, &link_task, "k").unwrap();
    assert!(link_spec.inputs.iter().any(|i| matches!(
        &i.source,
        InputSource::S3 { bucket, key, version_id }
            if bucket == "results"
                && key == "folder/alias.link"
                && version_id.as_deref() == Some(&link_ver.to_string())
    )));

    // Two-chunk ReadDataOperation on an unversioned directory child link keeps `link_ver` across
    // chunk 1 and chunk 2 while reading bytes from `folder/real.txt@real_ver`.
    use aruna_core::events::Event;
    use aruna_core::jobs::JobEvent;
    use aruna_core::stream::BackendStream;
    use aruna_workflow_contract::DATA_CHUNK_BYTES;
    linked
        .directory_pins
        .push(aruna_core::structs::execution::workflow_run::DirectoryPin {
            bucket: "results".into(),
            key: "folder/alias.link".into(),
            version_id: link_ver,
            source_node_id: Some(origin),
        });
    let chunk1_addr = format!("s3://{origin}@results/folder/alias.link");
    let read1 = super::data::ReadDataOperation::new(
        linked.clone(),
        runs.fixture.node,
        DataCall::Download {
            address: chunk1_addr.clone(),
            offset: 0,
        },
    );
    let (req1, target1) = read1.prepare_read().unwrap();
    assert_eq!(req1.key, "folder/alias.link");
    assert_eq!(req1.version, Some(link_ver));
    assert_eq!(target1.key, "folder/real.txt");
    assert_eq!(target1.version, Some(real_ver));
    let mut rem1 = RemoteReadOperation::new(
        linked.clone(),
        req1,
        target1.remote(runs.fixture.node).unwrap(),
        DataCall::Download {
            address: chunk1_addr,
            offset: 0,
        },
    );
    let ok_auth = || {
        IoResult::Child(ChildOutcome::new(Ok::<BaoReadOutput, BaoReadError>(
            BaoReadOutput::Authorized,
        )))
    };
    let pin1 = rem1.first().unwrap();
    assert!(format!("{pin1:?}").contains("LinkAccess"));
    assert!(format!("{pin1:?}").contains("folder/alias.link"));
    assert!(matches!(
        rem1.next(ok_auth()).unwrap(),
        Next::Call(IoCall::Child(_))
    ));
    let total_size = (DATA_CHUNK_BYTES + 4) as u64;
    let meta_large = IoResult::Child(ChildOutcome::new(Ok::<BaoReadOutput, BaoReadError>(
        BaoReadOutput::Metadata {
            size: total_size,
            blake3: [7; 32],
            etag: None,
            hashes: Default::default(),
        },
    )));
    assert!(matches!(
        rem1.next(meta_large).unwrap(),
        Next::Call(IoCall::Child(_))
    ));
    let stream_ok = || {
        IoResult::Child(ChildOutcome::new(Ok::<BaoReadOutput, BaoReadError>(
            BaoReadOutput::Stream {
                blob: BackendStream::empty(),
                size: total_size,
                blake3: [7; 32],
                etag: None,
                hashes: Default::default(),
            },
        )))
    };
    assert!(matches!(
        rem1.next(stream_ok()).unwrap(),
        Next::Call(IoCall::Effect(_))
    ));
    let chunk1_ver = match rem1
        .next(IoResult::Event(Box::new(Event::Job(
            JobEvent::CollectedUpTo(Ok(Some(vec![1; DATA_CHUNK_BYTES]))),
        ))))
        .unwrap()
    {
        Next::Done(HostRpcResult::StorageChunk {
            eof: false,
            version: Some(ver),
            ..
        }) => ver,
        other => panic!("expected chunk 1 with link version, got {other:?}"),
    };
    assert_eq!(chunk1_ver, link_ver.to_string());

    // Chunk 2 pins the original link URI with chunk 1's reported version and resolves cleanly.
    let chunk2_addr = format!("s3://{origin}@results/folder/alias.link?version={chunk1_ver}");
    let read2 = super::data::ReadDataOperation::new(
        linked.clone(),
        runs.fixture.node,
        DataCall::Download {
            address: chunk2_addr.clone(),
            offset: DATA_CHUNK_BYTES as u64,
        },
    );
    let (req2, target2) = read2.prepare_read().unwrap();
    assert_eq!(req2.key, "folder/alias.link");
    assert_eq!(req2.version, Some(link_ver));
    assert_eq!(target2.key, "folder/real.txt");
    assert_eq!(target2.version, Some(real_ver));
    let mut rem2 = RemoteReadOperation::new(
        linked,
        req2,
        target2.remote(runs.fixture.node).unwrap(),
        DataCall::Download {
            address: chunk2_addr,
            offset: DATA_CHUNK_BYTES as u64,
        },
    );
    let pin2 = rem2.first().unwrap();
    assert!(format!("{pin2:?}").contains("LinkAccess"));
    assert!(matches!(
        rem2.next(ok_auth()).unwrap(),
        Next::Call(IoCall::Child(_))
    ));
    let meta_large2 = IoResult::Child(ChildOutcome::new(Ok::<BaoReadOutput, BaoReadError>(
        BaoReadOutput::Metadata {
            size: total_size,
            blake3: [7; 32],
            etag: None,
            hashes: Default::default(),
        },
    )));
    assert!(matches!(
        rem2.next(meta_large2).unwrap(),
        Next::Call(IoCall::Child(_))
    ));
    assert!(matches!(
        rem2.next(stream_ok()).unwrap(),
        Next::Call(IoCall::Effect(_))
    ));
    assert_eq!(
        rem2.next(IoResult::Event(Box::new(Event::Job(
            JobEvent::CollectedUpTo(Ok(Some(vec![2; 4]))),
        ))))
        .unwrap(),
        Next::Done(HostRpcResult::StorageChunk {
            bytes: vec![2; 4],
            eof: true,
            version: Some(link_ver.to_string()),
        })
    );

    // An uncaptured remote directory prefix is still refused.
    let other_target = resolve(&resumed, &format!("s3://results/other/?node={origin}")).unwrap();
    let mut refused = RemoteReadOperation::new(
        resumed,
        other_target,
        origin,
        DataCall::List {
            address: format!("s3://results/other/?node={origin}"),
        },
    );
    assert!(matches!(
        refused.first().unwrap(),
        Next::Done(HostRpcResult::Failed(_))
    ));
}

// Admission explicitly rejects directory inputs whose readable file count exceeds the cap.
#[tokio::test]
async fn rejects_oversized_dir() {
    let runs = runs().await;
    seed_object(&runs, "results", "many/a.txt", Ulid::from_bytes([71; 16])).await;
    seed_object(&runs, "results", "many/b.txt", Ulid::from_bytes([72; 16])).await;
    seed_object(&runs, "results", "many/c.txt", Ulid::from_bytes([73; 16])).await;

    let req = runs.request(None, json!({ "dir": "s3://results/many/" }));
    let over = drive(
        StartRunOperation::new(&runs.fixture.alice, runs.serving(), req.clone()).with_dir_limit(2),
        &runs.fixture.context,
    )
    .await;
    assert!(matches!(over, Err(CatalogError::Invalid(_))), "{over:?}");

    let exact = drive(
        StartRunOperation::new(&runs.fixture.alice, runs.serving(), req).with_dir_limit(3),
        &runs.fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(exact.record.directory_pins.len(), 3);
}

// Same-node resume retains local directory pins, serves captured membership on list, and pins
// child reads to the original versions even after objects are overwritten or added.
#[tokio::test]
async fn retains_local_pins() {
    use super::control::{Follow, FollowRunOperation};
    use super::data::{DataCall, ReadDataOperation};
    use aruna_core::keyspaces::WORKFLOW_RUN_KEYSPACE;

    let runs = runs().await;
    let ver_old = Ulid::from_bytes([81; 16]);
    let ver_new = Ulid::from_bytes([82; 16]);
    let ver_extra = Ulid::from_bytes([83; 16]);
    seed_object(&runs, "results", "snap/a.txt", ver_old).await;

    let req = runs.request(None, json!({ "dir": "s3://results/snap/" }));
    let mut started = runs.start(&runs.fixture.alice, req).await.unwrap().record;
    assert_eq!(started.directory_pins.len(), 1);
    assert_eq!(started.directory_pins[0].version_id, ver_old);

    // Replace `a.txt` with a newer head and upload a new sibling before resuming on this node.
    seed_object(&runs, "results", "snap/a.txt", ver_new).await;
    seed_object(&runs, "results", "snap/extra.txt", ver_extra).await;
    started.state = RunState::Failed;
    write(
        &runs.fixture.context,
        WORKFLOW_RUN_KEYSPACE,
        super::run_key(started.run_id).to_vec(),
        started.to_bytes().unwrap(),
    )
    .await;

    let resumed = drive(
        FollowRunOperation::new(
            &runs.fixture.alice,
            runs.serving(),
            started.run_id,
            Follow::Resume,
        ),
        &runs.fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(
        resumed.directory_pins.len(),
        1,
        "{:?}",
        resumed.directory_pins
    );
    assert_eq!(resumed.directory_pins[0].key, "snap/a.txt");
    assert_eq!(resumed.directory_pins[0].version_id, ver_old);
    assert_eq!(resumed.directory_pins[0].source_node_id, None);

    // Local listing returns only the captured membership, omitting `extra.txt`.
    let listed = drive(
        ReadDataOperation::new(
            resumed.clone(),
            runs.fixture.node,
            DataCall::List {
                address: "s3://results/snap/".into(),
            },
        ),
        &runs.fixture.context,
    )
    .await
    .unwrap();
    match listed {
        HostRpcResult::StorageList { entries } => {
            assert_eq!(entries.len(), 1, "{entries:?}");
            assert_eq!(entries[0].name, "a.txt");
        }
        other => panic!("expected captured local listing, got {other:?}"),
    }

    // Local stat and task_spec resolve the unversioned child to `ver_old`, not `ver_new`.
    let statted = drive(
        ReadDataOperation::new(
            resumed.clone(),
            runs.fixture.node,
            DataCall::Stat {
                address: "s3://results/snap/a.txt".into(),
            },
        ),
        &runs.fixture.context,
    )
    .await
    .unwrap();
    match statted {
        HostRpcResult::StorageStat { entry: Some(entry) } => {
            assert_eq!(entry.version.as_deref(), Some(ver_old.to_string().as_str()));
        }
        other => panic!("expected pinned stat, got {other:?}"),
    }
    let mut task = start_task();
    task.direct_inputs.push(DirectInput {
        path: "a.txt".into(),
        address: "s3://results/snap/a.txt".into(),
        digest: "sha256:00".into(),
        size: 4,
        executable: false,
        aruna_id: None,
        endpoints: Vec::new(),
    });
    let spec = task_spec(&resumed, &task, "k").unwrap();
    assert!(spec.inputs.iter().any(|i| matches!(
        &i.source,
        InputSource::S3 { bucket, key, version_id }
            if bucket == "results"
                && key == "snap/a.txt"
                && version_id.as_deref() == Some(&ver_old.to_string())
    )));

    // Empty admitted directory is retained in `directory_roots` and never picks up later files.
    let empty_req = runs.request(None, json!({ "empty": "s3://results/snap_empty/" }));
    let mut empty_run = runs
        .start(&runs.fixture.alice, empty_req)
        .await
        .unwrap()
        .record;
    assert!(empty_run.directory_pins.is_empty());
    assert!(
        empty_run
            .directory_roots
            .iter()
            .any(|r| r.bucket == "results" && r.prefix == "snap_empty/")
    );
    seed_object(
        &runs,
        "results",
        "snap_empty/late.txt",
        Ulid::from_bytes([84; 16]),
    )
    .await;
    empty_run.state = RunState::Succeeded;
    write(
        &runs.fixture.context,
        WORKFLOW_RUN_KEYSPACE,
        super::run_key(empty_run.run_id).to_vec(),
        empty_run.to_bytes().unwrap(),
    )
    .await;
    let repeated = drive(
        FollowRunOperation::new(
            &runs.fixture.alice,
            runs.serving(),
            empty_run.run_id,
            Follow::Repeat,
        ),
        &runs.fixture.context,
    )
    .await
    .unwrap();
    assert!(repeated.directory_pins.is_empty());
    let empty_list = drive(
        ReadDataOperation::new(
            repeated,
            runs.fixture.node,
            DataCall::List {
                address: "s3://results/snap_empty/".into(),
            },
        ),
        &runs.fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(
        empty_list,
        HostRpcResult::StorageList {
            entries: Vec::new()
        }
    );

    // Local captured list and stat run per-entry READ authorization and hide denied children.
    use aruna_core::structs::storage::blob::object_permission_path;
    let mut restricted_parent = resumed.clone();
    restricted_parent.path_restrictions = Some(vec![
        aruna_core::structs::identity::auth::PathRestriction {
            pattern: object_permission_path(
                runs.fixture.alice.realm_id,
                runs.fixture.group_a,
                runs.fixture.node,
                "results",
                "snap/",
            ),
            permission: aruna_core::structs::identity::auth::Permission::READ,
        },
        aruna_core::structs::identity::auth::PathRestriction {
            pattern: object_permission_path(
                runs.fixture.alice.realm_id,
                runs.fixture.group_a,
                runs.fixture.node,
                "results",
                "snap",
            ),
            permission: aruna_core::structs::identity::auth::Permission::READ,
        },
    ]);
    let hidden_list = drive(
        ReadDataOperation::new(
            restricted_parent.clone(),
            runs.fixture.node,
            DataCall::List {
                address: "s3://results/snap/".into(),
            },
        ),
        &runs.fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(
        hidden_list,
        HostRpcResult::StorageList {
            entries: Vec::new()
        }
    );
    let hidden_stat = drive(
        ReadDataOperation::new(
            restricted_parent,
            runs.fixture.node,
            DataCall::Stat {
                address: "s3://results/snap/".into(),
            },
        ),
        &runs.fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(hidden_stat, HostRpcResult::StorageStat { entry: None });

    // Same-name file and directory merge into a single directory entry in local captured list.
    seed_object(&runs, "results", "clash/sample", Ulid::from_bytes([85; 16])).await;
    seed_object(
        &runs,
        "results",
        "clash/sample/result.txt",
        Ulid::from_bytes([86; 16]),
    )
    .await;
    let clash_req = runs.request(None, json!({ "dir": "s3://results/clash/" }));
    let clash_run = runs
        .start(&runs.fixture.alice, clash_req)
        .await
        .unwrap()
        .record;
    let clash_list = drive(
        ReadDataOperation::new(
            clash_run,
            runs.fixture.node,
            DataCall::List {
                address: "s3://results/clash/".into(),
            },
        ),
        &runs.fixture.context,
    )
    .await
    .unwrap();
    match clash_list {
        HostRpcResult::StorageList { entries } => {
            assert_eq!(entries.len(), 1, "{entries:?}");
            assert_eq!(entries[0].name, "sample");
            assert!(entries[0].directory);
        }
        other => panic!("expected merged directory entry, got {other:?}"),
    }
}

// Delivering an empty output directory persists a folder marker visible to stat and list.
#[tokio::test]
async fn delivers_empty_dir() {
    use super::data::{
        DataCall, ReadDataOperation, WriteBody, WriteObjectOperation, delivery_target,
    };
    use aruna_blob::blob::BlobHandler;
    use aruna_core::egress::EgressPolicy;
    use aruna_core::structs::storage::blob::{Backend, BackendConfig};
    use aruna_p2p::NetHandle;
    use std::collections::HashMap;

    let mut runs = runs().await;
    let dir = tempfile::tempdir().unwrap();
    let blob_root = dir.path().join("blobs").to_string_lossy().into_owned();
    std::fs::create_dir_all(&blob_root).unwrap();
    let net = NetHandle::new(
        aruna_runner::testing::local_net(),
        runs.fixture.context.storage_handle.clone(),
    )
    .await
    .unwrap();
    let blob = BlobHandler::with_egress(
        BackendConfig {
            backend_type: Backend::FileSystem,
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100_000),
            multipart_bucket: Some("multipart".to_string()),
            root: blob_root,
            service_config: HashMap::new(),
            timeouts: Default::default(),
        },
        runs.fixture.context.storage_handle.clone(),
        net.clone(),
        EgressPolicy::loopback(),
    )
    .await
    .unwrap();
    runs.fixture.context.net_handle = Some(net);
    runs.fixture.context.blob_handle = Some(blob);

    let record = runs.started().await;
    let target = delivery_target(&record, "empty/").unwrap();
    assert_eq!(target.bucket, "results");
    assert_eq!(target.key, "hello/empty/");

    let delivered = drive(
        WriteObjectOperation::new(
            (record.clone(), runs.fixture.node),
            NodeRouting::default(),
            target,
            WriteBody::Bytes(Vec::new()),
        )
        .delivery("empty/"),
        &runs.fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(
        delivered,
        HostRpcResult::Delivered {
            location: "aruna://results/hello/empty/".into(),
        }
    );

    // Stat finds the empty directory with or without trailing slash.
    for addr in [
        "aruna://results/hello/empty/",
        "aruna://results/hello/empty",
    ] {
        let stat = drive(
            ReadDataOperation::new(
                record.clone(),
                runs.fixture.node,
                DataCall::Stat {
                    address: addr.into(),
                },
            ),
            &runs.fixture.context,
        )
        .await
        .unwrap();
        match stat {
            HostRpcResult::StorageStat { entry: Some(entry) } => {
                assert!(entry.directory, "{addr}: {entry:?}");
            }
            other => panic!("{addr}: expected directory stat, got {other:?}"),
        }
    }

    // Listing inside the empty directory returns empty, while parent listing shows `empty`.
    let inner = drive(
        ReadDataOperation::new(
            record.clone(),
            runs.fixture.node,
            DataCall::List {
                address: "aruna://results/hello/empty/".into(),
            },
        ),
        &runs.fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(
        inner,
        HostRpcResult::StorageList {
            entries: Vec::new()
        }
    );

    let parent = drive(
        ReadDataOperation::new(
            record,
            runs.fixture.node,
            DataCall::List {
                address: "aruna://results/hello/".into(),
            },
        ),
        &runs.fixture.context,
    )
    .await
    .unwrap();
    match parent {
        HostRpcResult::StorageList { entries } => {
            assert!(
                entries.iter().any(|e| e.name == "empty" && e.directory),
                "{entries:?}"
            );
        }
        other => panic!("expected parent listing with empty dir, got {other:?}"),
    }
}

// Local empty-file reads of a standalone precaptured link check the link pin and keep its version.
#[test]
fn keeps_empty_link() {
    use super::data::{DataCall, ReadDataOperation};
    use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
    use aruna_core::operation::Operation;
    use aruna_core::replication_protocol::BaoReadRefusal;
    use aruna_core::structs::execution::job::input::{InputMode, InputSelection, InputSource};
    use aruna_core::structs::execution::workflow_run::{
        CacheScope, PrecapturedInput, RunState, WorkflowRunRecord,
    };
    use aruna_core::structs::storage::blob::BucketInfo;
    use aruna_core::structs::storage::multipart::MultipartChecksumType;
    use aruna_ops_data::replication::bao_read::{BaoReadError, BaoReadOutput};
    use aruna_ops_data::s3::object::get::{GetObjectError, GetObjectResult};
    use aruna_ops_data::s3::object::head::{HeadObjectError, HeadObjectResult};
    use aruna_workflow_contract::ContractError;

    let local = crate::tests::records::node(1);
    let remote_node = crate::tests::records::node(2);
    let link_ver = Ulid::from_parts(8, 1);
    let target_ver = Ulid::from_parts(9, 1);
    let sel = InputSelection {
        source: InputSource::S3 {
            bucket: "results".into(),
            key: "folder/empty.dat".into(),
            version_id: Some(target_ver.to_string()),
        },
        source_node_id: Some(local),
        dest_key: "results/folder/empty.link".into(),
        mode: InputMode::Snapshot,
        container_path: None,
        name: None,
        description: None,
    };
    let mut cap = crate::tests::records::captured_input(&sel, local);
    cap.version_id = target_ver;
    let make_run = |pin_node| WorkflowRunRecord {
        run_id: Ulid::from_parts(1, 1),
        lineage: Ulid::from_parts(1, 1),
        group_id: aruna_core::types::GroupId::from_bytes([1; 16]),
        workflow_id: Ulid::from_parts(1, 2),
        workflow_name: "demo".into(),
        version: 1,
        realm_id: crate::tests::records::REALM,
        node_id: local,
        local_only: false,
        last_event_seq: 0,
        state: RunState::Running,
        created_by: crate::tests::records::user(),
        created_at_ms: 1,
        started_at_ms: None,
        finished_at_ms: None,
        error: None,
        values_json: "{}".into(),
        inputs_json: None,
        output: None,
        provenance_crate: false,
        provenance_status: None,
        provenance_document_id: None,
        outputs: Vec::new(),
        origin: None,
        parent_job_id: None,
        claim_token_digest: None,
        attempts: 1,
        steps: Vec::new(),
        resumes_run_id: None,
        repeats_run_id: None,
        cache_scope: CacheScope::Group,
        placement_site: None,
        protected: false,
        secrets: Default::default(),
        path_restrictions: None,
        captured_remote: vec![PrecapturedInput {
            pin_node_id: pin_node,
            pin_bucket: "results".into(),
            pin_key: "folder/empty.link".into(),
            pin_version_id: link_ver,
            resolved: sel.clone(),
            captured: cap.clone(),
        }],
        directory_pins: Vec::new(),
        directory_roots: Vec::new(),
        used_tools: Vec::new(),
    };
    let info = || BucketInfo {
        group_id: aruna_core::types::GroupId::from_bytes([1; 16]),
        created_at: std::time::UNIX_EPOCH,
        created_by: crate::tests::records::user(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Default::default(),
        distribution: None,
    };
    let head = HeadObjectResult {
        location: None,
        metadata: Default::default(),
        source_metadata: None,
        last_refresh: None,
        version_created_at: None,
        version_id: Some(link_ver),
        resolved_version_id: Some(link_ver),
        checksum_type: MultipartChecksumType::FullObject,
        composite_hashes: Default::default(),
        part_count: None,
        source_policies: Vec::new(),
        source_binding: None,
    };

    // Unversioned standalone local link checks original pin READ first, then keeps link_ver on EOF.
    let local_run = make_run(local);
    let mut read = ReadDataOperation::new(
        local_run.clone(),
        local,
        DataCall::Download {
            address: "s3://results/folder/empty.link".into(),
            offset: 0,
        },
    );
    let pin_access = read.first().unwrap();
    assert!(format!("{pin_access:?}").contains("folder/empty.link"));
    assert!(matches!(
        read.next(IoResult::Child(ChildOutcome::new(Ok::<_, CatalogError>(
            info()
        ))))
        .unwrap(),
        Next::Call(IoCall::Read(_))
    ));
    let pin_head = read.next(IoResult::Values(vec![None])).unwrap();
    assert!(format!("{pin_head:?}").contains("folder/empty.link"));
    let target_access = read
        .next(IoResult::Child(ChildOutcome::new(
            Ok::<_, HeadObjectError>(head),
        )))
        .unwrap();
    assert!(format!("{target_access:?}").contains("folder/empty.dat"));
    assert!(matches!(
        read.next(IoResult::Child(ChildOutcome::new(Ok::<_, CatalogError>(
            info()
        ))))
        .unwrap(),
        Next::Call(IoCall::Read(_))
    ));
    let get_call = read.next(IoResult::Values(vec![None])).unwrap();
    assert!(format!("{get_call:?}").contains("folder/empty.dat"));
    let eof = read
        .next(IoResult::Child(ChildOutcome::new(Err::<
            GetObjectResult,
            GetObjectError,
        >(
            GetObjectError::InvalidRange,
        ))))
        .unwrap();
    assert_eq!(
        eof,
        Next::Done(HostRpcResult::StorageChunk {
            bytes: Vec::new(),
            eof: true,
            version: Some(link_ver.to_string()),
        })
    );

    // Revoked READ on the local link fails immediately without checking the target.
    let mut denied = ReadDataOperation::new(
        local_run,
        local,
        DataCall::Download {
            address: "s3://results/folder/empty.link".into(),
            offset: 0,
        },
    );
    assert!(matches!(
        denied.first().unwrap(),
        Next::Call(IoCall::Child(_))
    ));
    assert!(matches!(
        denied
            .next(IoResult::Child(ChildOutcome::new(Err::<
                BucketInfo,
                CatalogError,
            >(
                CatalogError::Forbidden
            ))))
            .unwrap(),
        Next::Done(HostRpcResult::Failed(ContractError::Denied { .. }))
    ));

    // A remote link resolving to a local target checks LinkAccess at the remote holder first.
    let mut remote_link = ReadDataOperation::new(
        make_run(remote_node),
        local,
        DataCall::Download {
            address: format!("s3://{remote_node}@results/folder/empty.link"),
            offset: 0,
        },
    );
    let pin_call = remote_link.first().unwrap();
    assert!(format!("{pin_call:?}").contains("LinkAccess"));
    assert!(format!("{pin_call:?}").contains("folder/empty.link"));
    assert!(matches!(
        remote_link
            .next(IoResult::Child(ChildOutcome::new(Err::<
                BaoReadOutput,
                BaoReadError,
            >(
                BaoReadError::Refused(BaoReadRefusal::ReadDenied)
            ))))
            .unwrap(),
        Next::Done(HostRpcResult::Failed(ContractError::Denied { .. }))
    ));
}
