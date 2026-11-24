//! Preflight requests against real workflows of every language, composed ones with their Aruna
//! step settings included: checks, forms, value checks, dry runs, tool cards and lowering.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_workflow_contract::preflight::{
    NodeKind, Severity, SuggestDirection, WorkflowLanguage, WorkflowSource,
};
use aruna_workflow_contract::{ContractError, CoordinatorMode, PreflightKind, PreflightResult};
use serde_json::{Value, json};

use crate::host::FakeHost;
use crate::workflows::{COMPOSED, CWL, FIXTURES, Fixture, REVERSE_TOOL, SHOUT_TOOL, source};

fn check(fixture: &Fixture) -> PreflightKind {
    PreflightKind::Check {
        source: fixture.source(),
        language: fixture.language,
        entrypoint: fixture.entrypoint.into(),
    }
}

fn dry_run(fixture: &Fixture, inputs: Value) -> PreflightKind {
    PreflightKind::DryRun {
        source: fixture.source(),
        language: fixture.language,
        entrypoint: fixture.entrypoint.into(),
        inputs,
        params: Default::default(),
    }
}

/// The language uwe reports; wir documents and composed workflows run as wir.
fn reported(fixture: &Fixture) -> &'static str {
    match fixture.name {
        "composed" => "wir",
        name => name,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checks_every_language() {
    let (mut host, _) = FakeHost::greeted(CoordinatorMode::Preflight, "pre-check").await;
    for fixture in &FIXTURES {
        let result = host.preflight(check(fixture)).await;
        let Ok(PreflightResult::Check {
            runnable,
            report_json,
            images,
        }) = result
        else {
            panic!("{}: {result:?}", fixture.name);
        };
        assert!(runnable, "{}: {report_json}", fixture.name);
        assert_eq!(
            report_json["language"],
            reported(fixture),
            "{}",
            fixture.name
        );
        assert_eq!(images, ["debian:stable-slim"], "{}", fixture.name);
    }
    host.shutdown().await.expect("a clean end");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inspects_every_language() {
    let (mut host, _) = FakeHost::greeted(CoordinatorMode::Preflight, "pre-inspect").await;
    for fixture in &FIXTURES {
        let result = host
            .preflight(PreflightKind::Inspect {
                source: fixture.source(),
                language: fixture.language,
                entrypoint: fixture.entrypoint.into(),
            })
            .await;
        let Ok(PreflightResult::Inspect { form_json, graph }) = result else {
            panic!("{}: {result:?}", fixture.name);
        };
        assert_eq!(form_json["language"], reported(fixture), "{}", fixture.name);
        assert_eq!(form_json["schema"]["type"], "object", "{}", fixture.name);
        // A Galaxy tool needs its input before it simulates; its form says so.
        if fixture.name == "galaxy" {
            assert_eq!(form_json["schema"]["required"], json!(["input"]));
            continue;
        }
        assert!(!graph.nodes.is_empty(), "{}", fixture.name);
    }
    let composed = &FIXTURES[7];
    let result = host
        .preflight(PreflightKind::Inspect {
            source: composed.source(),
            language: None,
            entrypoint: composed.entrypoint.into(),
        })
        .await;
    let Ok(PreflightResult::Inspect { graph, .. }) = result else {
        panic!("{result:?}");
    };
    let nodes: Vec<(&str, NodeKind)> = (graph.nodes.iter())
        .map(|node| (node.id.as_str(), node.kind))
        .collect();
    let expected = [
        ("input:text", NodeKind::Input),
        ("first", NodeKind::Step),
        ("second", NodeKind::Step),
        ("output:result", NodeKind::Output),
    ];
    assert_eq!(nodes, expected);
    let edges: Vec<(&str, &str)> = (graph.edges.iter())
        .map(|edge| (edge.from.as_str(), edge.to.as_str()))
        .collect();
    assert_eq!(
        edges,
        [
            ("input:text", "first"),
            ("first", "second"),
            ("second", "output:result")
        ]
    );
    host.shutdown().await.expect("a clean end");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validates_input_values() {
    let (mut host, _) = FakeHost::greeted(CoordinatorMode::Preflight, "pre-values").await;
    let validate = |values: Value| PreflightKind::ValidateValues {
        source: source(&[("hello.cwl", CWL)]),
        language: Some(WorkflowLanguage::Cwl),
        entrypoint: "hello.cwl".into(),
        values,
    };
    let accepted = host.preflight(validate(json!({"message": "Hola"}))).await;
    let Ok(PreflightResult::ValidateValues {
        valid,
        response_json,
    }) = accepted
    else {
        panic!("{accepted:?}");
    };
    assert!(valid, "{response_json}");
    let refused = host.preflight(validate(json!({"message": 3}))).await;
    let Ok(PreflightResult::ValidateValues {
        valid,
        response_json,
    }) = refused
    else {
        panic!("{refused:?}");
    };
    assert!(!valid);
    let diagnostics = response_json["diagnostics"]
        .as_array()
        .expect("diagnostics");
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic["input"] == "message")
    );
    host.shutdown().await.expect("a clean end");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simulates_every_language() {
    let (mut host, _) = FakeHost::greeted(CoordinatorMode::Preflight, "pre-dry").await;
    for fixture in &FIXTURES {
        let result = host.preflight(dry_run(fixture, Value::Null)).await;
        if fixture.name == "galaxy" {
            assert!(
                matches!(result, Err(ContractError::Workflow { .. })),
                "{result:?}"
            );
            continue;
        }
        let Ok(PreflightResult::DryRun {
            graph_json, tasks, ..
        }) = result
        else {
            panic!("{}: {result:?}", fixture.name);
        };
        assert!(graph_json["simulation"].is_string(), "{}", fixture.name);
        assert!(!tasks.is_empty(), "{}", fixture.name);
    }
    let steps = |tasks: &[Value]| -> Vec<String> {
        (tasks.iter())
            .map(|task| task["step"].as_str().unwrap_or_default().to_owned())
            .collect()
    };
    let result = host.preflight(dry_run(&FIXTURES[1], Value::Null)).await;
    let Ok(PreflightResult::DryRun { tasks, .. }) = result else {
        panic!("{result:?}");
    };
    assert_eq!(steps(&tasks)[..3], ["greet", "greet", "collect"]);
    let values = json!({"text": "aruna://object/text"});
    let result = host.preflight(dry_run(&FIXTURES[7], values)).await;
    let Ok(PreflightResult::DryRun { tasks, .. }) = result else {
        panic!("{result:?}");
    };
    assert_eq!(steps(&tasks), ["reverse", "shout"]);
    host.shutdown().await.expect("a clean end");
}

async fn tool_cards(host: &mut FakeHost, files: &[(&str, &str)], entry: &str) -> Vec<Value> {
    let result = host
        .preflight(PreflightKind::ExtractTools {
            source: source(files),
            language: None,
            entrypoint: entry.into(),
        })
        .await;
    match result {
        Ok(PreflightResult::ExtractTools { tools }) => tools,
        other => panic!("{entry}: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extracts_tool_cards() {
    let (mut host, _) = FakeHost::greeted(CoordinatorMode::Preflight, "pre-tools").await;
    for fixture in FIXTURES.iter().filter(|fixture| fixture.name != "wir") {
        let entry = match fixture.name {
            "composed" => "tools/reverse.yaml",
            _ => fixture.entrypoint,
        };
        let tools = tool_cards(&mut host, fixture.files, entry).await;
        assert!(!tools.is_empty(), "{}", fixture.name);
        for tool in &tools {
            assert_eq!(tool["source"]["address"], entry, "{}: {tool}", fixture.name);
        }
    }
    let result = host
        .preflight(PreflightKind::ExtractTools {
            source: FIXTURES[6].source(),
            language: None,
            entrypoint: FIXTURES[6].entrypoint.into(),
        })
        .await;
    assert!(
        matches!(result, Err(ContractError::Workflow { .. })),
        "{result:?}"
    );
    let escaping = PreflightKind::ExtractTools {
        source: source(&[("../main.nf", "")]),
        language: None,
        entrypoint: "../main.nf".into(),
    };
    assert!(matches!(
        host.preflight(escaping).await,
        Err(ContractError::Invalid { .. })
    ));
    host.shutdown().await.expect("a clean end");
}

const TOOL_FILES: [(&str, &str); 2] = [
    ("tools/reverse.yaml", REVERSE_TOOL),
    ("tools/shout.yaml", SHOUT_TOOL),
];

async fn catalog(host: &mut FakeHost) -> Vec<Value> {
    let mut tools = tool_cards(host, &TOOL_FILES, "tools/reverse.yaml").await;
    tools.extend(tool_cards(host, &TOOL_FILES, "tools/shout.yaml").await);
    tools
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lowers_composed_workflows() {
    let (mut host, _) = FakeHost::greeted(CoordinatorMode::Preflight, "pre-lower").await;
    let catalog_tools = catalog(&mut host).await;
    let lower = |uwe_yaml: String| PreflightKind::LowerComposed {
        uwe_yaml,
        catalog_tools: catalog_tools.clone(),
        tool_files: source(&TOOL_FILES),
    };
    let result = host.preflight(lower(COMPOSED.into())).await;
    let Ok(PreflightResult::LowerComposed(lowering)) = result else {
        panic!("{result:?}");
    };
    let errors: Vec<_> = (lowering.diagnostics.iter())
        .filter(|diagnostic| diagnostic.severity == Severity::Error)
        .collect();
    assert!(errors.is_empty(), "{errors:?}");
    let wir = lowering.wir_json.as_ref().expect("a wir document");
    assert!(wir["wir_version"].is_string(), "{wir}");
    assert_eq!(lowering.step_settings.len(), 2);
    assert_eq!(
        lowering.step_settings["first"].resources.memory_gib,
        Some(1.5)
    );
    let site = lowering.step_settings["second"].placement.site.as_deref();
    assert_eq!(site, Some("giessen"));
    assert_eq!(lowering.graph.nodes.len(), 4);
    assert!(lowering.check_json.is_some());

    let broken = COMPOSED
        .replace("cpus: 2", "cpus: 0")
        .replace("site: giessen", "site: giessen\n      queue: fast");
    let result = host.preflight(lower(broken)).await;
    let Ok(PreflightResult::LowerComposed(lowering)) = result else {
        panic!("{result:?}");
    };
    assert!(lowering.wir_json.is_none());
    let pointers: Vec<_> = (lowering.diagnostics.iter())
        .filter(|diagnostic| diagnostic.severity == Severity::Error)
        .map(|diagnostic| {
            let pointer = &diagnostic.pointer;
            (
                pointer.node.as_deref(),
                pointer.field.as_deref(),
                pointer.line,
            )
        })
        .collect();
    assert_eq!(
        pointers,
        [
            (Some("first"), Some("aruna.resources.cpus"), Some(16)),
            (Some("second"), Some("aruna"), Some(26)),
        ]
    );

    let misfit = COMPOSED.replace("text: first/reversed", "text: first/missing");
    let result = host.preflight(lower(misfit)).await;
    let Ok(PreflightResult::LowerComposed(lowering)) = result else {
        panic!("{result:?}");
    };
    assert!(lowering.wir_json.is_none());
    let edge = (lowering.diagnostics.iter())
        .find(|diagnostic| diagnostic.severity == Severity::Error)
        .expect("a refused edge or step");
    assert_eq!(edge.pointer.node.as_deref(), Some("second"), "{edge:?}");
    host.shutdown().await.expect("a clean end");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn suggests_catalog_tools() {
    let (mut host, _) = FakeHost::greeted(CoordinatorMode::Preflight, "pre-suggest").await;
    let catalog_tools = catalog(&mut host).await;
    let suggest = |direction: SuggestDirection, limit: u32| PreflightKind::SuggestTools {
        catalog_tools: catalog_tools.clone(),
        direction,
        kind: json!("file"),
        cardinality: json!("one"),
        formats: Vec::new(),
        conditions: Vec::new(),
        limit,
    };
    for (direction, ports) in [
        (SuggestDirection::Followers, ["text", "text"]),
        (SuggestDirection::Producers, ["reversed", "shouted"]),
    ] {
        let result = host.preflight(suggest(direction, 10)).await;
        let Ok(PreflightResult::SuggestTools { suggestions }) = result else {
            panic!("{result:?}");
        };
        let mut found: Vec<&str> = (suggestions.iter())
            .filter_map(|suggestion| suggestion["port"].as_str())
            .collect();
        found.sort_unstable();
        assert_eq!(found, ports, "{direction:?}");
        let result = host.preflight(suggest(direction, 1)).await;
        let Ok(PreflightResult::SuggestTools { suggestions }) = result else {
            panic!("{result:?}");
        };
        assert_eq!(suggestions.len(), 1);
    }
    let mut wrong = suggest(SuggestDirection::Followers, 1);
    if let PreflightKind::SuggestTools { kind, .. } = &mut wrong {
        *kind = json!("folder");
    }
    assert!(matches!(
        host.preflight(wrong).await,
        Err(ContractError::Invalid { .. })
    ));
    host.shutdown().await.expect("a clean end");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_order_checks() {
    let (mut host, _) = FakeHost::greeted(CoordinatorMode::Preflight, "pre-order").await;
    let result = host
        .preflight(PreflightKind::CheckOrder {
            source: FIXTURES[0].source(),
            language: FIXTURES[0].language,
            entrypoint: FIXTURES[0].entrypoint.into(),
        })
        .await;
    assert!(
        matches!(result, Err(ContractError::Unsupported { .. })),
        "{result:?}"
    );
    let missing = PreflightKind::Check {
        source: WorkflowSource::default(),
        language: None,
        entrypoint: "main.nf".into(),
    };
    assert!(matches!(
        host.preflight(missing).await,
        Err(ContractError::Invalid { .. })
    ));
    host.shutdown().await.expect("a clean end");
}
