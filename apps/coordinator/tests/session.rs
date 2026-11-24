//! Drives the coordinator over pipes as its node does: the greeting, preflight requests, whole
//! runs with mocked tasks and cancellation, and reports over the recorded run logs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod host;
mod lifecycle;
mod preflight;
mod reports;
mod workflows;

use aruna_coordinator::{CAPABILITIES, ServeError, ServeOptions};
use aruna_workflow_contract::{ContractError, CoordinatorMode, PROTOCOL_VERSION};

use crate::host::FakeHost;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn greets_then_ends() {
    let (host, greeting) = FakeHost::greeted(CoordinatorMode::Preflight, "run-greet").await;
    assert_eq!(greeting.protocol_version, PROTOCOL_VERSION);
    assert_eq!(greeting.capabilities, CAPABILITIES.map(str::to_owned));
    host.shutdown().await.expect("a clean end");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_other_versions() {
    let mut host = FakeHost::connect(ServeOptions::default());
    let greeting = host
        .greet(PROTOCOL_VERSION + 1, CoordinatorMode::Run, "run-version")
        .await;
    assert_eq!(greeting.protocol_version, PROTOCOL_VERSION);
    assert!(
        host.receive().await.is_none(),
        "the coordinator closes the channel"
    );
    match host.finished().await {
        Err(ServeError::Version(ContractError::Version { expected, found })) => {
            assert_eq!((expected, found), (PROTOCOL_VERSION, PROTOCOL_VERSION + 1));
        }
        other => panic!("expected a version refusal, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requires_greeting_first() {
    let mut host = FakeHost::connect(ServeOptions::default());
    host.send(&aruna_workflow_contract::HostToCoordinator::Shutdown)
        .await;
    assert!(host.receive().await.is_none());
    assert!(matches!(
        host.finished().await,
        Err(ServeError::UnexpectedFirst)
    ));
    let mut silent = FakeHost::connect(ServeOptions::default());
    silent.close_writer().await;
    assert!(matches!(
        silent.finished().await,
        Err(ServeError::NoGreeting)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn modes_refuse_requests() {
    let (mut host, _) = FakeHost::greeted(CoordinatorMode::Report, "run-modes").await;
    let kind = aruna_workflow_contract::PreflightKind::LowerComposed {
        uwe_yaml: String::new(),
        catalog_tools: Vec::new(),
        tool_files: Default::default(),
    };
    assert!(matches!(
        host.preflight(kind).await,
        Err(ContractError::Invalid { .. })
    ));
    host.shutdown().await.expect("a clean end");
    let (mut host, _) = FakeHost::greeted(CoordinatorMode::Preflight, "run-modes").await;
    let kind = aruna_workflow_contract::PreflightKind::BuildReport {
        run_id: "run-modes".into(),
        log_pages: Vec::new(),
    };
    assert!(matches!(
        host.preflight(kind).await,
        Err(ContractError::Invalid { .. })
    ));
    host.shutdown().await.expect("a clean end");
}
