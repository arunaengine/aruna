//! Tests script request building, runtime checks, and job error mapping in the compute tools.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

#[test]
fn builds_script_request() {
    let plan = build_script(
        RunScriptInput {
            group_id: "01J00000000000000000000000".to_string(),
            name: None,
            description: None,
            bucket: "scripts".to_string(),
            runtime: "deno".to_string(),
            script: "console.log('ok');\n".to_string(),
            dependencies: Some(vec!["chalk@5".to_string()]),
            inputs: None,
            outputs: None,
            cpu_cores: Some(2),
            ram_bytes: Some(1_000_000_000),
            max_walltime_ms: Some(60_000),
            target: None,
            tags: None,
        },
        "01JTEST0000000000000000000",
    )
    .unwrap();

    assert_eq!(
        plan.script_key,
        ".aruna/scripts/01JTEST0000000000000000000/script.ts"
    );
    assert_eq!(
        plan.request.command,
        [
            "deno",
            "run",
            "-A",
            "--config=/work/deno.json",
            "/work/script.ts"
        ]
    );
    assert_eq!(
        plan.request.tags.get(NETWORK_TAG).map(String::as_str),
        Some("open")
    );
}

fn error_body(result: CallToolResult) -> serde_json::Value {
    assert_eq!(result.is_error, Some(true));
    result.structured_content.expect("structured error body")
}

fn build_err(input: RunScriptInput) -> CallToolResult {
    match build_script(input, "01JTEST0000000000000000000") {
        Ok(_) => panic!("expected build_script to refuse the input"),
        Err(result) => result,
    }
}

fn script_input(runtime: &str, deps: Option<Vec<String>>) -> RunScriptInput {
    RunScriptInput {
        group_id: "01J00000000000000000000000".to_string(),
        name: None,
        description: None,
        bucket: "scripts".to_string(),
        runtime: runtime.to_string(),
        script: "print('hi')\n".to_string(),
        dependencies: deps,
        inputs: None,
        outputs: None,
        cpu_cores: None,
        ram_bytes: None,
        max_walltime_ms: None,
        target: None,
        tags: None,
    }
}

#[test]
fn python_inline_deps() {
    let plan = build_script(
        script_input("python-uv", Some(vec!["httpx>=0.27".to_string()])),
        "01JTEST0000000000000000000",
    )
    .unwrap();
    assert!(plan.script_text.starts_with("# /// script"));
    assert!(plan.script_text.contains("requires-python"));
    assert!(plan.dependency.is_none());
    assert_eq!(
        plan.request.tags.get(NETWORK_TAG).map(String::as_str),
        Some("open")
    );
}

#[test]
fn bash_refuses_dependencies() {
    let text = error_body(build_err(script_input(
        "bash",
        Some(vec!["jq".to_string()]),
    )));
    assert!(
        text["error"]
            .as_str()
            .unwrap_or_default()
            .contains("resolves no dependencies")
    );
}

#[test]
fn unknown_runtime_refused() {
    let text = error_body(build_err(script_input("ruby", None)));
    assert!(
        text["error"]
            .as_str()
            .unwrap_or_default()
            .contains("list_runtimes")
    );
}

#[test]
fn rejects_bad_group() {
    let mut input = script_input("bash", None);
    input.group_id = "not-a-ulid".to_string();
    let text = error_body(build_err(input));
    assert_eq!(text["code"], "Bad request");
}

#[test]
fn npm_strips_versions() {
    assert_eq!(npm_package_name("chalk@5"), "chalk");
    assert_eq!(npm_package_name("plain"), "plain");
    assert_eq!(npm_package_name("@scope/pkg@1.2.3"), "@scope/pkg");
    assert_eq!(npm_package_name("@scope/pkg"), "@scope/pkg");
}

#[test]
fn parse_job_reasons() {
    let text = error_body(parse_job("bad").unwrap_err());
    assert!(
        text["error"]
            .as_str()
            .unwrap_or_default()
            .contains("job ULID")
    );
    let id = aruna_core::structs::execution::job::JobId::from_bytes([7u8; 16]).to_string();
    assert!(parse_job(&id).is_ok());
}

#[test]
fn job_state_names() {
    assert!(parse_job_state("running").is_ok());
    assert!(parse_job_state("bogus").is_err());
}

#[test]
fn job_submit_errors() {
    assert!(
        error_body(job_error(crate::error::ServerError::NotFound))["error"]
            .as_str()
            .unwrap_or_default()
            .contains("list_jobs")
    );
    assert!(
        error_body(submit_error(crate::error::ServerError::BadRequest))["error"]
            .as_str()
            .unwrap_or_default()
            .contains("group_id")
    );
    assert!(
        error_body(submit_error(crate::error::ServerError::Forbidden))["error"]
            .as_str()
            .unwrap_or_default()
            .contains("write permission")
    );
}

#[test]
fn runtime_output_ids() {
    let ids = QUICK_RUNTIMES
        .iter()
        .map(RuntimeOutput::from)
        .map(|runtime| runtime.id)
        .collect::<Vec<_>>();
    assert!(ids.iter().any(|id| id == "bash"));
    assert!(ids.iter().any(|id| id == "deno"));
    assert!(ids.iter().any(|id| id == "python-uv"));
}
