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
    assert_eq!(start.prior_log_pages.len(), 1);
    assert_eq!(start.prior_log_pages[0].entries.len(), 2);
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
    let submit = task
        .next(child(Ok::<_, CatalogError>(run.clone())))
        .unwrap();
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
    let again = task.next(IoResult::Woke).unwrap();
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
