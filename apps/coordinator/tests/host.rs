//! A scripted Aruna host over in-memory pipes: greets the coordinator, sends requests, answers
//! its calls from memory and records the run log, the tasks and the live events.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use aruna_coordinator::{ServeError, ServeOptions, serve_streams};
use aruna_workflow_contract::host::{
    ListEntry, StartTask, StorageEntry, TaskFinished, TaskOutput, UploadReceipt,
};
use aruna_workflow_contract::message::RunFinishedPayload;
use aruna_workflow_contract::preflight::{RunLogEntry, RunLogPage};
use aruna_workflow_contract::{
    ContractError, CoordinatorMode, CoordinatorToHost, DATA_CHUNK_BYTES, HostRpcRequest,
    HostRpcResult, HostToCoordinator, LiveWorkflowEvent, PROTOCOL_VERSION, PreflightKind,
    PreflightResult, decode_frame_len, decode_message, encode_message,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, ReadHalf, WriteHalf};
use tokio::task::JoinHandle;

/// How long a test waits for one frame before it fails instead of hanging.
const PATIENCE: Duration = Duration::from_secs(600);

pub struct FakeHost {
    reader: ReadHalf<DuplexStream>,
    writer: WriteHalf<DuplexStream>,
    pub served: Option<JoinHandle<Result<(), ServeError>>>,
    next_request: u64,
}

/// What a host learns from the greeting.
pub struct Greeting {
    pub protocol_version: u32,
    pub capabilities: Vec<String>,
}

impl FakeHost {
    /// Starts a coordinator session over pipes; the greeting is the test's to send.
    pub fn connect(options: ServeOptions) -> FakeHost {
        let (host, coordinator) = tokio::io::duplex(1 << 20);
        let (read, write) = tokio::io::split(coordinator);
        let served = tokio::spawn(serve_streams(read, write, options));
        let (reader, writer) = tokio::io::split(host);
        FakeHost {
            reader,
            writer,
            served: Some(served),
            next_request: 1,
        }
    }

    /// Connects and greets with this protocol's version.
    pub async fn greeted(mode: CoordinatorMode, run_id: &str) -> (FakeHost, Greeting) {
        let mut host = FakeHost::connect(ServeOptions::default());
        let greeting = host.greet(PROTOCOL_VERSION, mode, run_id).await;
        (host, greeting)
    }

    pub async fn greet(&mut self, version: u32, mode: CoordinatorMode, run_id: &str) -> Greeting {
        self.send(&HostToCoordinator::Hello {
            protocol_version: version,
            run_id: run_id.into(),
            claim_token_digest: "blake3:claim".into(),
            mode,
        })
        .await;
        match self.receive().await {
            Some(CoordinatorToHost::HelloAck {
                protocol_version,
                capabilities,
                uwe_revision,
            }) => {
                assert_eq!(uwe_revision, aruna_coordinator::UWE_REVISION);
                Greeting {
                    protocol_version,
                    capabilities,
                }
            }
            other => panic!("expected a greeting, got {other:?}"),
        }
    }

    pub async fn send(&mut self, message: &HostToCoordinator) {
        let frame = encode_message(message).expect("encodes");
        self.writer.write_all(&frame).await.expect("writes");
        self.writer.flush().await.expect("flushes");
    }

    /// The next frame of the coordinator; `None` once it closed the channel.
    pub async fn receive(&mut self) -> Option<CoordinatorToHost> {
        let read = async {
            let mut prefix = [0u8; 4];
            match self.reader.read_exact(&mut prefix).await {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return None,
                Err(error) => panic!("the channel failed: {error}"),
            }
            let size = decode_frame_len(prefix).expect("a bounded frame");
            let mut payload = vec![0u8; size];
            self.reader
                .read_exact(&mut payload)
                .await
                .expect("a whole frame");
            Some(decode_message(&payload).expect("a coordinator message"))
        };
        tokio::time::timeout(PATIENCE, read)
            .await
            .expect("the coordinator answers in time")
    }

    /// Sends a preflight request and waits for its answer.
    pub async fn preflight(
        &mut self,
        kind: PreflightKind,
    ) -> Result<PreflightResult, ContractError> {
        let request_id = self.next_request;
        self.next_request += 1;
        self.send(&HostToCoordinator::PreflightRequest { request_id, kind })
            .await;
        match self.receive().await {
            Some(CoordinatorToHost::PreflightResponse {
                request_id: answered,
                result,
            }) if answered == request_id => result,
            Some(other) => panic!("unexpected frame {other:?}"),
            None => panic!("the coordinator closed the channel"),
        }
    }

    /// Ends the host's side of the channel without a word.
    pub async fn close_writer(&mut self) {
        self.writer.shutdown().await.expect("closes");
    }

    /// Shuts the session down and waits until it ended.
    pub async fn shutdown(mut self) -> Result<(), ServeError> {
        self.send(&HostToCoordinator::Shutdown).await;
        while self.receive().await.is_some() {}
        self.finished().await
    }

    pub async fn finished(&mut self) -> Result<(), ServeError> {
        let served = self.served.take().expect("a session");
        tokio::time::timeout(PATIENCE, served)
            .await
            .expect("the session ends in time")
            .expect("the session task")
    }
}

/// How the fake node answers `StartTask`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskMode {
    /// Every expected output exists with made-up content, and the task ends with 0.
    Succeed,
    /// The task never ends by itself.
    Hold,
}

/// The fake node: its storage, uploads, run log and tasks.
pub struct MockNode {
    pub mode: TaskMode,
    pub files: HashMap<String, Vec<u8>>,
    uploads: HashMap<String, (String, Vec<u8>)>,
    pub log: Vec<RunLogEntry>,
    pub tasks: Vec<StartTask>,
    pub cancelled: Vec<String>,
    pub stopped_runs: Vec<String>,
    pub pinned: Vec<String>,
    pub delivered: Vec<(String, String)>,
    pub downloads: Vec<String>,
    pub listed: Vec<String>,
    pub fail_delivery: Option<String>,
    pub omit_sha256: bool,
    pub events: Vec<LiveWorkflowEvent>,
    pub event_seqs: Vec<u64>,
}

impl MockNode {
    pub fn new(mode: TaskMode) -> MockNode {
        MockNode {
            mode,
            files: HashMap::new(),
            uploads: HashMap::new(),
            log: Vec::new(),
            tasks: Vec::new(),
            cancelled: Vec::new(),
            stopped_runs: Vec::new(),
            pinned: Vec::new(),
            delivered: Vec::new(),
            downloads: Vec::new(),
            listed: Vec::new(),
            fail_delivery: None,
            omit_sha256: false,
            events: Vec::new(),
            event_seqs: Vec::new(),
        }
    }

    /// The recorded run log as pages of at most `size` entries.
    pub fn pages(&self, size: usize) -> Vec<RunLogPage> {
        (self.log.chunks(size))
            .map(|entries| RunLogPage {
                entries: entries.to_vec(),
            })
            .collect()
    }

    /// The answer to a call; `None` leaves it unanswered, like a running task.
    pub fn answer(&mut self, request: HostRpcRequest) -> Option<HostRpcResult> {
        Some(match request {
            HostRpcRequest::PinImage { image, .. } => {
                self.pinned.push(image.clone());
                let name = image.split(':').next().unwrap_or(&image).to_owned();
                HostRpcResult::PinImage {
                    pinned: Some(format!("{name}@sha256:{}", "ab".repeat(32))),
                }
            }
            HostRpcRequest::StartTask(task) => {
                let task = *task;
                self.tasks.push(task.clone());
                if self.mode == TaskMode::Hold {
                    return None;
                }
                HostRpcResult::TaskFinished(Box::new(self.finish(&task)))
            }
            HostRpcRequest::CancelTask { name } => {
                self.cancelled.push(name.clone());
                HostRpcResult::TaskCancelled { name }
            }
            HostRpcRequest::StopOrphans { run_id } => {
                self.stopped_runs.push(run_id);
                HostRpcResult::StopOrphans {
                    stopped: self.cancelled.clone(),
                }
            }
            HostRpcRequest::StorageStat { address } => {
                if address
                    .split_once('?')
                    .is_some_and(|(_, q)| q.contains('/'))
                {
                    return Some(HostRpcResult::Failed(ContractError::invalid(format!(
                        "malformed query in {address}"
                    ))));
                }
                let Some(decoded) = decode_addr(&address) else {
                    return Some(HostRpcResult::Failed(ContractError::invalid(format!(
                        "malformed escape in {address}"
                    ))));
                };
                match self.files.get(&decoded) {
                    Some(bytes) => HostRpcResult::StorageStat {
                        entry: Some(StorageEntry {
                            directory: false,
                            size: bytes.len() as u64,
                            modified_ms: Some(1_700_000_000_000),
                            version: Some("v1".into()),
                            sha256: (!self.omit_sha256)
                                .then(|| task_model::Digest::sha256(bytes).hex().to_owned()),
                            aruna_id: Some(decoded.rsplit('/').next().unwrap_or_default().into()),
                            endpoints: vec!["node-a".into()],
                        }),
                    },
                    None => {
                        let (base, query) = split_query(&decoded);
                        let prefix = base.trim_end_matches('/').to_owned() + "/";
                        let is_dir = self.files.keys().any(|key| {
                            let (kbase, kquery) = split_query(key);
                            kquery == query && kbase.starts_with(&prefix)
                        });
                        let entry = is_dir.then(|| StorageEntry {
                            directory: true,
                            size: 0,
                            modified_ms: None,
                            version: None,
                            sha256: None,
                            aruna_id: None,
                            endpoints: Vec::new(),
                        });
                        HostRpcResult::StorageStat { entry }
                    }
                }
            }
            HostRpcRequest::StorageList { address } => {
                if address
                    .split_once('?')
                    .is_some_and(|(_, q)| q.contains('/'))
                {
                    return Some(HostRpcResult::Failed(ContractError::invalid(format!(
                        "malformed query in {address}"
                    ))));
                }
                self.listed.push(address.clone());
                let Some(decoded) = decode_addr(&address) else {
                    return Some(HostRpcResult::Failed(ContractError::invalid(format!(
                        "malformed escape in {address}"
                    ))));
                };
                let (base, query) = split_query(&decoded);
                let prefix = base.trim_end_matches('/').to_owned() + "/";
                let mut entries: BTreeMap<String, bool> = BTreeMap::new();
                for key in self.files.keys() {
                    let (kbase, kquery) = split_query(key);
                    if kquery != query {
                        continue;
                    }
                    let Some(path) = kbase.strip_prefix(&prefix) else {
                        continue;
                    };
                    match path.split_once('/') {
                        Some((folder, _)) => entries.insert(folder.into(), true),
                        None => entries.insert(path.into(), false),
                    };
                }
                HostRpcResult::StorageList {
                    entries: (entries.into_iter())
                        .map(|(name, directory)| ListEntry { name, directory })
                        .collect(),
                }
            }
            HostRpcRequest::StorageDownload { address, offset } => {
                if address
                    .split_once('?')
                    .is_some_and(|(_, q)| q.contains('/'))
                {
                    return Some(HostRpcResult::Failed(ContractError::invalid(format!(
                        "malformed query in {address}"
                    ))));
                }
                self.downloads.push(address.clone());
                let Some(decoded) = decode_addr(&address) else {
                    return Some(HostRpcResult::Failed(ContractError::invalid(format!(
                        "malformed escape in {address}"
                    ))));
                };
                let Some(bytes) = self.files.get(&decoded) else {
                    let message = format!("no file {address}");
                    return Some(HostRpcResult::Failed(ContractError::NotFound { message }));
                };
                let start = (offset as usize).min(bytes.len());
                let end = (start + DATA_CHUNK_BYTES).min(bytes.len());
                HostRpcResult::StorageChunk {
                    bytes: bytes[start..end].to_vec(),
                    eof: end == bytes.len(),
                    version: Some("v1".into()),
                }
            }
            HostRpcRequest::StorageUploadStart { address, .. } => {
                let upload_id = format!("upload-{}", self.uploads.len() + 1);
                self.uploads
                    .insert(upload_id.clone(), (address, Vec::new()));
                HostRpcResult::UploadStarted { upload_id }
            }
            HostRpcRequest::StorageUploadChunk {
                upload_id,
                seq,
                bytes,
                eof,
            } => {
                let upload = self.uploads.get_mut(&upload_id).expect("an open upload");
                upload.1.extend_from_slice(&bytes);
                let receipt = eof.then(|| {
                    let (address, bytes) = self.uploads.remove(&upload_id).expect("open");
                    let size = bytes.len() as u64;
                    self.files.insert(address.clone(), bytes);
                    UploadReceipt {
                        address,
                        size,
                        version: Some("v1".into()),
                        sha256: None,
                    }
                });
                HostRpcResult::UploadAck { seq, receipt }
            }
            HostRpcRequest::TaskDirectoryPage { .. } => {
                HostRpcResult::Failed(ContractError::NotFound {
                    message: "no paged remote directory in mock host".into(),
                })
            }
            HostRpcRequest::DeliverOutput {
                source_ref,
                target_path,
            } => {
                if let Some(reason) = &self.fail_delivery {
                    return Some(HostRpcResult::Failed(ContractError::Denied {
                        message: reason.clone(),
                    }));
                }
                self.delivered.push((source_ref, target_path.clone()));
                HostRpcResult::Delivered {
                    location: format!("aruna://delivered/{target_path}"),
                }
            }
            HostRpcRequest::RunLogBegin { run_start_json } => {
                self.log.push(RunLogEntry::Begin { run_start_json });
                HostRpcResult::RunLogAck { seq: 0 }
            }
            HostRpcRequest::RunLogAppend {
                seq, change_json, ..
            } => {
                self.log.push(RunLogEntry::Change { seq, change_json });
                HostRpcResult::RunLogAck { seq }
            }
        })
    }

    /// Ends a task with 0 and one made-up file per expected output.
    fn finish(&mut self, task: &StartTask) -> TaskFinished {
        let mut outputs = Vec::new();
        for (index, expected) in task.expected_outputs.iter().enumerate() {
            if expected.pattern == "cwl.output.json" {
                continue;
            }
            let address = format!("aruna://object/{}-{index}", task.name);
            let content = format!("output {} of {}\n", expected.pattern, task.step_id);
            self.files
                .insert(address.clone(), content.clone().into_bytes());
            outputs.push(TaskOutput {
                path: expected.pattern.clone(),
                directory: false,
                address,
                digest: None,
                size: content.len() as u64,
                files: Vec::new(),
                files_token: None,
            });
        }
        TaskFinished {
            exit_code: Some(0),
            outputs,
            stdout_tail: format!("ran {}\n", task.name).into_bytes(),
            started: Some(1_700_000_000.5),
            ended: Some(1_700_000_002.0),
            ..TaskFinished::default()
        }
    }
}

fn split_query(address: &str) -> (&str, &str) {
    match address.split_once('?') {
        Some((path, query))
            if !query.is_empty()
                && query.split('&').all(|pair| {
                    matches!(pair.split_once('='), Some(("version" | "node", v)) if !v.is_empty())
                }) =>
        {
            (path, query)
        }
        _ => (address, ""),
    }
}

fn decode_addr(address: &str) -> Option<String> {
    let (path, query) = split_query(address);
    let decoded = aruna_workflow_contract::host::decode_key(path)?;
    Some(match query.is_empty() {
        true => decoded,
        false => format!("{decoded}?{query}"),
    })
}

/// Drives a run session to its end: answers every call of the coordinator, records the
/// events, and sends `Cancel` once `cancel_after` tasks started.
pub async fn drive_run(
    host: &mut FakeHost,
    node: &mut MockNode,
    cancel_after: Option<usize>,
) -> RunFinishedPayload {
    let mut cancel_sent = false;
    loop {
        match host.receive().await {
            Some(CoordinatorToHost::RpcRequest { call_id, request }) => {
                if let Some(result) = node.answer(request) {
                    let message = HostToCoordinator::RpcResponse { call_id, result };
                    host.send(&message).await;
                }
                let due = cancel_after.is_some_and(|count| node.tasks.len() >= count);
                if due && !cancel_sent {
                    cancel_sent = true;
                    let reason = "the owner cancelled the run".to_owned();
                    host.send(&HostToCoordinator::Cancel { reason }).await;
                }
            }
            Some(CoordinatorToHost::LiveEvent { seq, event }) => {
                node.event_seqs.push(seq);
                node.events.push(event);
            }
            Some(CoordinatorToHost::RunFinished(finished)) => return finished,
            Some(other) => panic!("unexpected frame {other:?}"),
            None => panic!("the coordinator closed the channel before the run ended"),
        }
    }
}
