//! Drives the real `aruna-coordinator` binary as a child process: a check, a composed lowering and
//! the recycling of warm coordinators. `ARUNA_WORKFLOW_COORDINATOR_BIN` names the binary.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use aruna_workflow::{LocalBinary, WorkflowRuntime, WorkflowSettings};
use aruna_workflow_contract::preflight::{SourceFile, WorkflowLanguage, WorkflowSource};
use aruna_workflow_contract::{PreflightKind, PreflightResult};

const CWL: &str = "cwlVersion: v1.2
class: CommandLineTool
baseCommand: echo
hints:
  DockerRequirement:
    dockerPull: debian:stable-slim
inputs:
  message:
    type: string
    default: Hello
    inputBinding:
      position: 1
outputs:
  said:
    type: stdout
stdout: said.txt
";

fn binary() -> PathBuf {
    std::env::var_os("ARUNA_WORKFLOW_COORDINATOR_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../apps/coordinator/target/debug/aruna-coordinator")
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs the aruna-coordinator binary"]
async fn checks_with_binary() {
    let settings = WorkflowSettings {
        binary: Some(binary()),
        preflight_workers: 1,
        request_timeout: Duration::from_secs(120),
        recycle_after: 2,
        ..WorkflowSettings::default()
    };
    let transport = Arc::new(LocalBinary::new(binary()));
    let runtime = WorkflowRuntime::new(transport, &settings);
    let check = PreflightKind::Check {
        source: WorkflowSource {
            files: vec![SourceFile {
                path: "hello.cwl".into(),
                content: CWL.as_bytes().to_vec(),
                executable: false,
            }],
        },
        language: Some(WorkflowLanguage::Cwl),
        entrypoint: "hello.cwl".into(),
    };
    for _ in 0..3 {
        let result = runtime.pool().request(check.clone()).await;
        let Ok(PreflightResult::Check {
            runnable, images, ..
        }) = result
        else {
            panic!("{result:?}");
        };
        assert!(runnable);
        assert_eq!(images, ["debian:stable-slim"]);
    }
    assert_eq!(runtime.pool().started(), 2);
    let lowered = runtime
        .pool()
        .request(PreflightKind::LowerComposed {
            uwe_yaml: "uwe_version: \"1\"\nsteps:\n  count:\n    tool: count\n".into(),
            catalog_tools: Vec::new(),
            tool_files: WorkflowSource::default(),
        })
        .await;
    let Ok(PreflightResult::LowerComposed(lowering)) = lowered else {
        panic!("{lowered:?}");
    };
    assert!(lowering.wir_json.is_none());
    assert!(!lowering.diagnostics.is_empty());
}
