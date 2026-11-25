//! Serde round trips of every message variant, through whole frames as the channel carries them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::io::Cursor;

use aruna_workflow_contract::host::{
    DirectInput, ExpectedOutput, ListEntry, SecretPlaceholder, StagedFile, StartTask, StorageEntry,
    TaskFinished, TaskFolders, TaskOutput, TaskResources, TaskUsage, UploadReceipt,
};
use aruna_workflow_contract::message::{
    LogStream, ProgressCounts, RunFinishedPayload, RunStartPayload, TaskChange, TaskState,
};
use aruna_workflow_contract::preflight::{
    ComposedLowering, Diagnostic, DiagnosticPointer, EdgePointer, GraphEdge, GraphNode, NodeKind,
    RunLogEntry, RunLogPage, Severity, SourceFile, SuggestDirection, WorkflowGraph,
    WorkflowLanguage, WorkflowSource,
};
use aruna_workflow_contract::{
    ArunaStepSettings, ContractError, CoordinatorMode, CoordinatorToHost, HostRpcRequest,
    HostRpcResult, HostToCoordinator, LiveWorkflowEvent, PROTOCOL_VERSION, PreflightKind,
    PreflightResult, read_frame, write_frame,
};
use serde_json::{Map, json};

fn source() -> WorkflowSource {
    WorkflowSource {
        files: vec![SourceFile {
            path: "main.nf".into(),
            content: b"workflow { }\n\x00\xff".to_vec(),
            executable: true,
        }],
    }
}

fn pages() -> Vec<RunLogPage> {
    vec![RunLogPage {
        entries: vec![
            RunLogEntry::Begin {
                run_start_json: json!({"id": "run-1"}),
            },
            RunLogEntry::Change {
                seq: 1,
                change_json: json!({"change": "started", "task": 1, "attempt": 1}),
            },
        ],
    }]
}

fn settings() -> ArunaStepSettings {
    ArunaStepSettings::from_value(json!({
        "resources": {"cpus": 2, "memory_gib": 4},
        "execution": {"network": false},
    }))
    .expect("reads")
}

/// Writes each message as a frame and reads it back unchanged.
fn assert_frames<T>(messages: &[T])
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let mut buffer = Vec::new();
    for message in messages {
        write_frame(&mut buffer, message).expect("writes");
    }
    let mut reader = Cursor::new(buffer);
    for message in messages {
        let read: Option<T> = read_frame(&mut reader).expect("reads");
        assert_eq!(read.as_ref(), Some(message));
    }
    let end: Option<T> = read_frame(&mut reader).expect("a clean end");
    assert!(end.is_none());
}

fn preflight_kinds() -> Vec<PreflightKind> {
    let entrypoint = "main.nf".to_owned();
    let mut params = Map::new();
    params.insert("greeting".into(), json!("Hola"));
    vec![
        PreflightKind::Check {
            source: source(),
            language: Some(WorkflowLanguage::Nextflow),
            entrypoint: entrypoint.clone(),
        },
        PreflightKind::CheckOrder {
            source: source(),
            language: None,
            entrypoint: entrypoint.clone(),
        },
        PreflightKind::Inspect {
            source: source(),
            language: Some(WorkflowLanguage::Galaxy),
            entrypoint: entrypoint.clone(),
        },
        PreflightKind::ValidateValues {
            source: source(),
            language: Some(WorkflowLanguage::Cwl),
            entrypoint: entrypoint.clone(),
            values: json!({"message": "hi"}),
        },
        PreflightKind::LowerComposed {
            uwe_yaml: "uwe_version: \"1\"\n".into(),
            catalog_tools: vec![json!({"id": "upper"})],
            tool_files: source(),
        },
        PreflightKind::ExtractTools {
            source: source(),
            language: Some(WorkflowLanguage::Snakemake),
            entrypoint: entrypoint.clone(),
        },
        PreflightKind::SuggestTools {
            catalog_tools: vec![json!({"id": "sort"})],
            direction: SuggestDirection::Producers,
            kind: json!("file"),
            cardinality: json!("list"),
            formats: vec!["format_2330".into()],
            conditions: vec![json!("indexed")],
            limit: 5,
        },
        PreflightKind::DryRun {
            source: source(),
            language: Some(WorkflowLanguage::Wdl),
            entrypoint: entrypoint.clone(),
            inputs: json!({}),
            params,
        },
        PreflightKind::ComputeWhy {
            run_id: "run-1".into(),
            task_id: 3,
            log_pages: pages(),
        },
        PreflightKind::ComputeCompare {
            first_run_id: "run-1".into(),
            second_run_id: "run-2".into(),
            first_log_pages: pages(),
            second_log_pages: Vec::new(),
        },
        PreflightKind::ComputeTime {
            run_id: "run-1".into(),
            log_pages: pages(),
        },
        PreflightKind::ComputeChanges {
            run_id: "run-1".into(),
            log_pages: pages(),
        },
        PreflightKind::BuildRunCrate {
            run_id: "run-1".into(),
            log_pages: pages(),
            base_uri: "https://w3id.org/aruna/data/".into(),
            source: source(),
            entrypoint,
        },
        PreflightKind::BuildReport {
            run_id: "run-1".into(),
            log_pages: pages(),
        },
    ]
}

fn preflight_results() -> Vec<PreflightResult> {
    let graph = WorkflowGraph {
        nodes: vec![GraphNode {
            id: "upper".into(),
            label: "upper".into(),
            kind: NodeKind::Step,
            tool: Some("upper".into()),
            version: Some("1.0".into()),
        }],
        edges: vec![GraphEdge {
            from: "input:text".into(),
            to: "upper".into(),
            from_port: Some("text".into()),
            to_port: Some("text".into()),
        }],
    };
    let diagnostic = Diagnostic {
        severity: Severity::Error,
        message: "does not fit".into(),
        pointer: DiagnosticPointer {
            line: Some(12),
            node: Some("sort".into()),
            edge: Some(EdgePointer {
                from: "upper/upper".into(),
                to: "sort/text".into(),
            }),
            field: Some("aruna.resources.cpus".into()),
        },
    };
    vec![
        PreflightResult::Check {
            runnable: true,
            report_json: json!({"parts": []}),
            images: vec!["debian:stable-slim".into()],
        },
        PreflightResult::CheckOrder {
            order_dependent: false,
            differences: Vec::new(),
        },
        PreflightResult::Inspect {
            form_json: json!({"schema": {}}),
            graph: graph.clone(),
        },
        PreflightResult::ValidateValues {
            valid: false,
            response_json: json!({"diagnostics": []}),
        },
        PreflightResult::LowerComposed(ComposedLowering {
            diagnostics: vec![diagnostic.clone()],
            graph,
            wir_json: Some(json!({"wir_version": "0.1"})),
            step_settings: BTreeMap::from([("sort".into(), settings())]),
            check_json: Some(json!({"fit": "fits"})),
        }),
        PreflightResult::ExtractTools {
            tools: vec![json!({"id": "upper"})],
        },
        PreflightResult::SuggestTools {
            suggestions: vec![json!({"tool": "sort"})],
        },
        PreflightResult::DryRun {
            graph_json: json!({"tasks": []}),
            tasks: vec![json!({"id": 1})],
            diagnostics: vec![diagnostic],
        },
        PreflightResult::ComputeWhy { why_json: None },
        PreflightResult::ComputeCompare {
            comparison_json: Some(json!({})),
        },
        PreflightResult::ComputeTime {
            time_json: Some(json!({"total": 1})),
        },
        PreflightResult::ComputeChanges {
            changes_json: json!({"sources": []}),
        },
        PreflightResult::BuildRunCrate {
            metadata_json: json!({"@graph": []}),
            files: vec!["main.nf".into()],
        },
        PreflightResult::BuildReport {
            html: "<html></html>".into(),
        },
    ]
}

fn start_task() -> StartTask {
    StartTask {
        task_key_seed: "{\"format\":1}".into(),
        step_id: "sayHello".into(),
        task_id: Some(2),
        attempt: 1,
        name: "run-1-sayHello-2-1".into(),
        spec_json: json!({"format": 1}),
        command_wrapper_bytes: b"#!/bin/sh\nexit 0\n".to_vec(),
        folders: TaskFolders {
            work: "/uwe/work".into(),
            control: "/uwe/control".into(),
            home: "/uwe/home".into(),
        },
        image: Some("debian@sha256:00".into()),
        resources: TaskResources {
            cpus: 2,
            memory_bytes: Some(1 << 30),
            disk_bytes: None,
            time_seconds: Some(60),
            gpus: 0,
        },
        site: Some("giessen".into()),
        network_allowed: false,
        direct_inputs: vec![DirectInput {
            path: "reads.fq".into(),
            address: "aruna://object/abc".into(),
            digest: "sha256:00".into(),
            size: 10,
            executable: false,
            aruna_id: Some("abc".into()),
            endpoints: vec!["node-a".into()],
        }],
        staged_files: vec![StagedFile {
            path: "script.sh".into(),
            address: Some("aruna-upload://run-1/1".into()),
            directory: false,
            executable: true,
            size: 12,
        }],
        expected_outputs: vec![ExpectedOutput {
            pattern: "*.txt".into(),
        }],
        secret_placeholders: vec![SecretPlaceholder {
            name: "TOKEN".into(),
            placeholder: "__ARUNA_SECRET_x_TOKEN__".into(),
            env_name: Some("API_TOKEN".into()),
        }],
        step_settings: Some(settings()),
    }
}

fn rpc_requests() -> Vec<HostRpcRequest> {
    vec![
        HostRpcRequest::PinImage {
            image: "debian:12".into(),
            site: None,
        },
        HostRpcRequest::StartTask(Box::new(start_task())),
        HostRpcRequest::CancelTask {
            name: "run-1-sayHello-2-1".into(),
        },
        HostRpcRequest::StopOrphans {
            run_id: "run-1".into(),
        },
        HostRpcRequest::StorageStat {
            address: "aruna://object/abc".into(),
        },
        HostRpcRequest::StorageList {
            address: "s3://bucket/prefix/".into(),
        },
        HostRpcRequest::StorageDownload {
            address: "aruna://object/abc".into(),
            offset: 262_144,
        },
        HostRpcRequest::StorageUploadStart {
            address: "s3://bucket/out.txt".into(),
            size: 3,
            digest: None,
        },
        HostRpcRequest::StorageUploadChunk {
            upload_id: "u-1".into(),
            seq: 0,
            bytes: vec![0, 1, 2, 255],
            eof: true,
        },
        HostRpcRequest::DeliverOutput {
            source_ref: "aruna://object/abc".into(),
            target_path: "s3://bucket/results/out.txt".into(),
        },
        HostRpcRequest::RunLogBegin {
            run_start_json: json!({"id": "run-1"}),
        },
        HostRpcRequest::RunLogAppend {
            seq: 4,
            change_json: json!({"change": "ended"}),
            is_event: true,
        },
    ]
}

fn rpc_results() -> Vec<HostRpcResult> {
    vec![
        HostRpcResult::PinImage {
            pinned: Some("debian@sha256:00".into()),
        },
        HostRpcResult::TaskCancelled {
            name: "run-1-sayHello-2-1".into(),
        },
        HostRpcResult::TaskFinished(Box::new(TaskFinished {
            exit_code: Some(0),
            error: None,
            retryable: false,
            cancelled: false,
            outputs: vec![TaskOutput {
                path: "out.txt".into(),
                directory: false,
                address: "aruna://object/out".into(),
                digest: Some("sha256:00".into()),
                size: 3,
            }],
            stdout_tail: b"hello\n".to_vec(),
            stderr_tail: Vec::new(),
            started: Some(1.5),
            ended: Some(2.5),
            usage: Some(TaskUsage {
                cpu_usec: Some(10),
                ..TaskUsage::default()
            }),
        })),
        HostRpcResult::StopOrphans {
            stopped: vec!["job-1".into()],
        },
        HostRpcResult::StorageStat {
            entry: Some(StorageEntry {
                directory: false,
                size: 3,
                modified_ms: Some(1),
                version: Some("v1".into()),
                sha256: None,
                aruna_id: Some("abc".into()),
                endpoints: Vec::new(),
            }),
        },
        HostRpcResult::StorageList {
            entries: vec![ListEntry {
                name: "a.txt".into(),
                directory: false,
            }],
        },
        HostRpcResult::StorageChunk {
            bytes: vec![1, 2, 3],
            eof: true,
            version: None,
        },
        HostRpcResult::UploadStarted {
            upload_id: "u-1".into(),
        },
        HostRpcResult::UploadAck {
            seq: 0,
            receipt: Some(UploadReceipt {
                address: "s3://bucket/out.txt".into(),
                size: 3,
                version: Some("v2".into()),
                sha256: None,
            }),
        },
        HostRpcResult::Delivered {
            location: "s3://bucket/results/out.txt".into(),
        },
        HostRpcResult::RunLogAck { seq: 4 },
        HostRpcResult::Failed(ContractError::Denied {
            message: "no read permission".into(),
        }),
    ]
}

#[test]
fn host_messages_travel() {
    let mut messages = vec![
        HostToCoordinator::Hello {
            protocol_version: PROTOCOL_VERSION,
            run_id: "run-1".into(),
            claim_token_digest: "blake3:00".into(),
            mode: CoordinatorMode::Run,
        },
        HostToCoordinator::RunStart(RunStartPayload {
            source: source(),
            entrypoint: "main.nf".into(),
            language: Some(WorkflowLanguage::Nextflow),
            inputs: json!({"reads": "aruna://object/abc"}),
            params: Map::new(),
            pinned_images: BTreeMap::from([("debian:12".into(), "debian@sha256:00".into())]),
            cache_namespace: Some("group:1".into()),
            resume_lineage: Some("run-0".into()),
            secret_names: vec!["TOKEN".into()],
            network_isolated: true,
            max_pending: Some(8),
            prior_log_pages: pages(),
            step_settings: BTreeMap::from([("upper".into(), settings())]),
        }),
        HostToCoordinator::Cancel {
            reason: "the owner cancelled".into(),
        },
        HostToCoordinator::Shutdown,
    ];
    for (request_id, kind) in (0..).zip(preflight_kinds()) {
        messages.push(HostToCoordinator::PreflightRequest { request_id, kind });
    }
    for (call_id, result) in (0..).zip(rpc_results()) {
        messages.push(HostToCoordinator::RpcResponse { call_id, result });
    }
    assert_frames(&messages);
}

#[test]
fn coordinator_messages_travel() {
    let mut messages = vec![
        CoordinatorToHost::HelloAck {
            protocol_version: PROTOCOL_VERSION,
            uwe_revision: "a278b311".into(),
            capabilities: vec!["preflight".into(), "run".into()],
        },
        CoordinatorToHost::PreflightResponse {
            request_id: 9,
            result: Err(ContractError::Version {
                expected: 1,
                found: 2,
            }),
        },
        CoordinatorToHost::RunFinished(RunFinishedPayload {
            success: false,
            error: Some("a task failed".into()),
            stopped: false,
            final_summary_json: json!({"tasks": 3}),
        }),
    ];
    for (request_id, result) in (0..).zip(preflight_results()) {
        let result = Ok(result);
        messages.push(CoordinatorToHost::PreflightResponse { request_id, result });
    }
    for (call_id, request) in (0..).zip(rpc_requests()) {
        messages.push(CoordinatorToHost::RpcRequest { call_id, request });
    }
    let events = [
        LiveWorkflowEvent::RunStarted {
            run_id: "run-1".into(),
            time_ms: 1,
        },
        LiveWorkflowEvent::TaskChanged(TaskChange {
            task_id: 1,
            name: "sayHello (1)".into(),
            step: "sayHello".into(),
            state: TaskState::Finished,
            attempt: Some(1),
            time_ms: 2,
            started_ms: Some(1),
            ended_ms: Some(2),
            cached: false,
            exit_code: Some(0),
            error: None,
        }),
        LiveWorkflowEvent::TaskLog {
            task_id: 1,
            attempt: 1,
            stream: LogStream::Stderr,
            chunk: "line\n".into(),
            time_ms: 3,
        },
        LiveWorkflowEvent::TaskUsage {
            task_id: 1,
            attempt: 1,
            cpu_percent: Some(50.0),
            memory_bytes: Some(1024),
            time_ms: 3,
        },
        LiveWorkflowEvent::TaskMarked {
            task_id: 1,
            kind: "content".into(),
            reason: "the outputs decide the next tasks".into(),
        },
        LiveWorkflowEvent::Progress(ProgressCounts {
            requested: 5,
            running: 1,
            finished: 2,
            cached: 1,
            failed: 1,
            stopped: 1,
        }),
        LiveWorkflowEvent::Printed {
            text: "Hello world!".into(),
        },
        LiveWorkflowEvent::RunEnded {
            run_id: "run-1".into(),
            time_ms: 4,
            error: None,
        },
    ];
    for (seq, event) in (0..).zip(events) {
        messages.push(CoordinatorToHost::LiveEvent { seq, event });
    }
    assert_frames(&messages);
}

#[test]
fn tags_message_names() {
    let hello = serde_json::to_value(HostToCoordinator::Hello {
        protocol_version: 1,
        run_id: "r".into(),
        claim_token_digest: "d".into(),
        mode: CoordinatorMode::Preflight,
    })
    .expect("writes");
    assert_eq!(hello["type"], "hello");
    assert_eq!(hello["mode"], "preflight");
    let request = serde_json::to_value(&rpc_requests()[6]).expect("writes");
    assert_eq!(request["call"], "storage_download");
    let chunk = serde_json::to_value(&rpc_requests()[8]).expect("writes");
    assert_eq!(chunk["bytes"], "AAEC/w==");
    for kind in preflight_kinds() {
        let value = serde_json::to_value(&kind).expect("writes");
        assert_eq!(value["op"], kind.name());
    }
}

#[test]
fn source_rejects_escapes() {
    assert!(source().validate().is_ok());
    for path in ["", "/etc/passwd", "../x", "a/../b", "a//b", "a\\b", "./a"] {
        let mut escaping = source();
        escaping.files[0].path = path.into();
        assert!(escaping.validate().is_err(), "{path:?}");
    }
    let mut twice = source();
    twice.files.push(twice.files[0].clone());
    assert!(twice.validate().is_err());
}
