//! Tests the channel, the warm preflight pool and run sessions against scripted coordinators
//! over in-memory pipes: framing, reuse, recycling, timeouts, crashes and refusals.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aruna_core::workflow::{WorkflowAnswer, WorkflowEffect, WorkflowError, WorkflowHost};
use aruna_workflow::{
    Attempt, Connection, CoordinatorTransport, WorkflowRuntime, WorkflowSettings,
};
use aruna_workflow_contract::preflight::WorkflowSource;
use aruna_workflow_contract::{
    ContractError, CoordinatorMode, CoordinatorToHost, HostRpcRequest, HostToCoordinator,
    PROTOCOL_VERSION, PreflightKind, PreflightResult, decode_frame_len, decode_message,
    encode_message,
};
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

/// What a scripted coordinator does with its preflight requests.
#[derive(Clone, Copy, Debug)]
enum Script {
    Answer,
    /// Answers the first request, then ends its channel.
    EndAfterOne,
    /// Never answers.
    Silent,
    Refuse,
    /// Calls the host first and answers with what the host said.
    CallHost,
}

struct Scripted {
    scripts: Mutex<Vec<Script>>,
    started: AtomicUsize,
}

impl Scripted {
    /// Coordinators that follow `scripts` in start order, then `Answer`.
    fn new(scripts: &[Script]) -> Arc<Self> {
        let mut scripts = scripts.to_vec();
        scripts.reverse();
        Arc::new(Self {
            scripts: Mutex::new(scripts),
            started: AtomicUsize::new(0),
        })
    }
}

async fn read_host(stream: &mut DuplexStream) -> Option<HostToCoordinator> {
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).await.ok()?;
    let mut payload = vec![0u8; decode_frame_len(prefix).ok()?];
    stream.read_exact(&mut payload).await.ok()?;
    decode_message(&payload).ok()
}

async fn write_host(stream: &mut DuplexStream, message: &CoordinatorToHost) {
    let frame = encode_message(message).expect("encodes");
    stream.write_all(&frame).await.expect("writes");
}

fn tools(count: usize) -> PreflightResult {
    PreflightResult::ExtractTools {
        tools: vec![serde_json::json!({"id": "t"}); count],
    }
}

async fn coordinator(mut stream: DuplexStream, script: Script) {
    let Some(HostToCoordinator::Hello { .. }) = read_host(&mut stream).await else {
        return;
    };
    let ack = CoordinatorToHost::HelloAck {
        protocol_version: PROTOCOL_VERSION,
        uwe_revision: "test".into(),
        capabilities: Vec::new(),
    };
    write_host(&mut stream, &ack).await;
    while let Some(message) = read_host(&mut stream).await {
        let request_id = match message {
            HostToCoordinator::PreflightRequest { request_id, .. } => request_id,
            HostToCoordinator::RunStart(_) => {
                let event = CoordinatorToHost::LiveEvent {
                    seq: 1,
                    event: aruna_workflow_contract::LiveWorkflowEvent::Printed {
                        text: "started".into(),
                    },
                };
                write_host(&mut stream, &event).await;
                continue;
            }
            HostToCoordinator::Shutdown => return,
            _ => continue,
        };
        let result = match script {
            Script::Silent => continue,
            Script::Refuse => Err(ContractError::workflow("no such workflow")),
            Script::CallHost => {
                let call = CoordinatorToHost::RpcRequest {
                    call_id: 9,
                    request: HostRpcRequest::StopOrphans { run_id: "r".into() },
                };
                write_host(&mut stream, &call).await;
                match read_host(&mut stream).await {
                    Some(HostToCoordinator::RpcResponse { result, .. }) => {
                        Err(ContractError::invalid(format!("{result:?}")))
                    }
                    _ => return,
                }
            }
            Script::Answer | Script::EndAfterOne => Ok(tools(1)),
        };
        let answer = CoordinatorToHost::PreflightResponse { request_id, result };
        write_host(&mut stream, &answer).await;
        if matches!(script, Script::EndAfterOne) {
            return;
        }
    }
}

#[async_trait]
impl CoordinatorTransport for Scripted {
    async fn start(&self) -> Result<Attempt, WorkflowError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        let script = self.scripts.lock().unwrap().pop().unwrap_or(Script::Answer);
        let (host, remote) = tokio::io::duplex(64 * 1024);
        let task = tokio::spawn(coordinator(remote, script));
        let (reader, writer) = tokio::io::split(host);
        Ok(Attempt {
            reader: Box::pin(reader),
            writer: Box::pin(writer),
            guard: Box::new(AbortOnDrop(task)),
        })
    }
}

/// Ends the scripted coordinator with its channel, as a killed process would.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn settings(recycle_after: u64) -> WorkflowSettings {
    WorkflowSettings {
        preflight_workers: 2,
        request_timeout: Duration::from_secs(5),
        recycle_after,
        ..WorkflowSettings::default()
    }
}

fn request() -> PreflightKind {
    PreflightKind::ExtractTools {
        source: WorkflowSource::default(),
        language: None,
        entrypoint: "main.nf".into(),
    }
}

#[tokio::test]
async fn frames_round_trip() {
    let transport = Scripted::new(&[]);
    let mut connection = Connection::new(transport.start().await.unwrap());
    connection
        .greet(CoordinatorMode::Preflight, "frames", "")
        .await
        .unwrap();
    let answer = connection.preflight(4, request()).await.unwrap();
    assert_eq!(answer, Ok(tools(1)));
    connection.send(&HostToCoordinator::Shutdown).await.unwrap();
    assert_eq!(connection.receive().await.unwrap(), None);
}

#[tokio::test]
async fn rejects_truncated_frames() {
    let (host, mut remote) = tokio::io::duplex(1024);
    let (reader, writer) = tokio::io::split(host);
    let mut connection = Connection::new(Attempt {
        reader: Box::pin(reader),
        writer: Box::pin(writer),
        guard: Box::new(()),
    });
    remote.write_all(&[0, 0, 0, 9, b'{']).await.unwrap();
    drop(remote);
    let read = connection.receive().await;
    assert!(matches!(read, Err(WorkflowError::Crashed(_))), "{read:?}");
}

#[tokio::test]
async fn recycles_warm_workers() {
    let transport = Scripted::new(&[]);
    let runtime = WorkflowRuntime::new(transport.clone(), &settings(2));
    for _ in 0..5 {
        assert_eq!(runtime.pool().request(request()).await, Ok(tools(1)));
    }
    // Two requests per coordinator: five requests need three.
    assert_eq!(transport.started.load(Ordering::SeqCst), 3);
    assert_eq!(runtime.pool().started(), 3);
}

#[tokio::test(start_paused = true)]
async fn replaces_silent_workers() {
    let transport = Scripted::new(&[Script::Silent]);
    let runtime = WorkflowRuntime::new(transport.clone(), &settings(10));
    let late = runtime.pool().request(request()).await;
    assert_eq!(late, Err(WorkflowError::Timeout { seconds: 5 }));
    assert_eq!(runtime.pool().request(request()).await, Ok(tools(1)));
    assert_eq!(transport.started.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn restarts_ended_workers() {
    let transport = Scripted::new(&[Script::EndAfterOne]);
    let runtime = WorkflowRuntime::new(transport.clone(), &settings(10));
    assert_eq!(runtime.pool().request(request()).await, Ok(tools(1)));
    // The warm coordinator is gone; the request goes to a new one.
    assert_eq!(runtime.pool().request(request()).await, Ok(tools(1)));
    assert_eq!(transport.started.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn keeps_refusing_workers() {
    let transport = Scripted::new(&[Script::Refuse]);
    let runtime = WorkflowRuntime::new(transport.clone(), &settings(10));
    let refused = runtime.pool().request(request()).await;
    let expected = WorkflowError::Refused(ContractError::workflow("no such workflow"));
    assert_eq!(refused, Err(expected.clone()));
    assert_eq!(runtime.pool().request(request()).await, Err(expected));
    assert_eq!(transport.started.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn denies_host_calls() {
    let transport = Scripted::new(&[Script::CallHost]);
    let runtime = WorkflowRuntime::new(transport, &settings(10));
    let answer = runtime.serve(WorkflowEffect::Preflight(request())).await;
    let WorkflowAnswer::Preflight(Err(WorkflowError::Refused(ContractError::Invalid { message }))) =
        answer
    else {
        panic!("{answer:?}");
    };
    assert!(message.contains("Denied"), "{message}");
}

#[tokio::test]
async fn drives_run_sessions() {
    let transport = Scripted::new(&[]);
    let runtime = WorkflowRuntime::new(transport, &settings(10));
    let open = WorkflowEffect::Open {
        session: "run-1".into(),
        mode: CoordinatorMode::Run,
        run_id: "run-1".into(),
        claim_token_digest: "digest".into(),
    };
    assert_eq!(runtime.serve(open).await, WorkflowAnswer::Opened(Ok(())));
    let start = serde_json::from_value(serde_json::json!({
        "type": "run_start", "source": {"files": []}, "entrypoint": "main.nf", "language": null,
        "inputs": null, "params": {}, "pinned_images": {}, "cache_namespace": null,
        "resume_lineage": null, "secret_names": [], "network_isolated": true,
        "max_pending": null, "prior_log_pages": [], "step_settings": {}
    }))
    .expect("a run start");
    let send = WorkflowEffect::Send {
        session: "run-1".into(),
        message: start,
    };
    assert_eq!(runtime.serve(send).await, WorkflowAnswer::Sent(Ok(())));
    let receive = || WorkflowEffect::Receive {
        session: "run-1".into(),
    };
    let WorkflowAnswer::Received(Ok(Some(CoordinatorToHost::LiveEvent { seq: 1, .. }))) =
        runtime.serve(receive()).await
    else {
        panic!("no live event");
    };
    let close = WorkflowEffect::Close {
        session: "run-1".into(),
    };
    assert_eq!(runtime.serve(close).await, WorkflowAnswer::Closed(Ok(())));
    let gone = runtime.serve(receive()).await;
    let unknown = WorkflowError::UnknownSession("run-1".into());
    assert_eq!(gone, WorkflowAnswer::Received(Err(unknown)));
}
