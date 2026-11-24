//! Tests the workflow and tool routes through their router: bodies and statuses as the portal
//! reads them, with a scripted coordinator, without one, and with the real coordinator binary.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_compute::registry::ExecutorRegistry;
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::user::id::UserId;
use aruna_core::workflow::{WorkflowAnswer, WorkflowEffect, WorkflowError, WorkflowHost};
use aruna_workflow_contract::preflight::{
    ComposedLowering, GraphEdge, GraphNode, NodeKind, WorkflowGraph,
};
use aruna_workflow_contract::{ContractError, PreflightKind, PreflightResult};
use axum::Extension;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::{Value, json};
use tower::ServiceExt;
use ulid::Ulid;

use crate::auth::ValidatedBearer;
use crate::server::state::ServerState;
use crate::tests::routes::{seed_group_docs, seed_realm_auth, seed_realm_config, test_state};

const CWL: &str = "cwlVersion: v1.2\nclass: CommandLineTool\nbaseCommand: echo\n";

struct Scripted;

fn scripted(kind: PreflightKind) -> Result<PreflightResult, WorkflowError> {
    Ok(match kind {
        PreflightKind::Check { .. } => PreflightResult::Check {
            runnable: true,
            report_json: json!({"language": "cwl", "parts": [], "problems": [
                {"kind": "warning", "message": "message has no default"}
            ]}),
            images: Vec::new(),
        },
        PreflightKind::Inspect { .. } => PreflightResult::Inspect {
            form_json: json!({"schema": {"type": "object", "required": ["message"]}}),
            graph: WorkflowGraph {
                nodes: vec![GraphNode {
                    id: "echo".into(),
                    label: "echo".into(),
                    kind: NodeKind::Step,
                    tool: None,
                    version: None,
                }],
                edges: vec![GraphEdge {
                    from: "input:message".into(),
                    to: "echo".into(),
                    from_port: Some("message".into()),
                    to_port: None,
                }],
            },
        },
        PreflightKind::ValidateValues { values, .. } => PreflightResult::ValidateValues {
            valid: values["message"].is_string(),
            response_json: json!({"diagnostics": []}),
        },
        PreflightKind::LowerComposed { .. } => PreflightResult::LowerComposed(ComposedLowering {
            wir_json: Some(json!({"wir_version": "1"})),
            ..ComposedLowering::default()
        }),
        PreflightKind::ExtractTools { entrypoint, .. } => PreflightResult::ExtractTools {
            tools: vec![json!({"id": "echo", "version": "1", "title": "Echo",
                "source": {"kind": "cwl", "address": entrypoint, "revision": "r"},
                "image": {"name": "debian:stable-slim"},
                "inputs": [{"name": "message", "kind": {"value": "string"}}],
                "outputs": [{"name": "said", "kind": "file"}]})],
        },
        PreflightKind::SuggestTools { catalog_tools, .. } => PreflightResult::SuggestTools {
            suggestions: (catalog_tools.iter())
                .map(|tool| {
                    json!({"tool": tool["id"], "version": tool["version"], "port": "said",
                    "check": {"fit": "unknown", "scatter": true}})
                })
                .collect(),
        },
        other => {
            return Err(WorkflowError::Refused(ContractError::unsupported(
                other.name(),
            )));
        }
    })
}

#[async_trait::async_trait]
impl WorkflowHost for Scripted {
    async fn serve(&self, effect: WorkflowEffect) -> WorkflowAnswer {
        match effect {
            WorkflowEffect::Preflight(kind) => WorkflowAnswer::Preflight(scripted(kind)),
            other => other.unavailable(),
        }
    }
}

struct Node {
    _dir: tempfile::TempDir,
    state: Arc<ServerState>,
    owner: AuthContext,
    stranger: AuthContext,
    group_id: Ulid,
}

async fn node(host: Option<Arc<dyn WorkflowHost>>) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let storage = aruna_storage::storage::memory::MemoryStorage::open();
    let mut context = aruna_runner::testing::test_context(storage);
    if let Some(host) = host {
        context.compute_handle = Some(Arc::new(ExecutorRegistry::new().with_workflow(host)));
    }
    let context = Arc::new(context);
    let realm_id = RealmId([1; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[6; 32]).public();
    let user = |seed: u8| {
        AuthContext::new(
            UserId::local(Ulid::from_bytes([seed; 16]), realm_id),
            realm_id,
        )
    };
    let (owner, stranger) = (user(1), user(2));
    let actor = Actor {
        node_id,
        user_id: owner.user_id,
        realm_id,
    };
    let group_id = Ulid::from_bytes([30; 16]);
    seed_realm_auth(&context, realm_id, &actor).await;
    seed_realm_config(&context, realm_id, &actor).await;
    seed_group_docs(&context, realm_id, &actor, group_id, "flows", owner.user_id).await;
    let capabilities = NodeCapabilities::user_node(realm_id).unwrap();
    let state = Arc::new(test_state(context, realm_id, node_id, capabilities).await);
    Node {
        _dir: dir,
        state,
        owner,
        stranger,
        group_id,
    }
}

impl Node {
    async fn call(
        &self,
        auth: &AuthContext,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let (router, _) = super::router().split_for_parts();
        let body = body.map_or_else(Body::empty, |body| Body::from(body.to_string()));
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .unwrap();
        let router = router
            .layer(Extension(Some(auth.clone())))
            .layer(Extension(Option::<ValidatedBearer>::None))
            .with_state(self.state.clone());
        let response = router.oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, value)
    }

    async fn create(&self, body: Value) -> (StatusCode, Value) {
        self.call(&self.owner, "POST", "/compute/workflows", Some(body))
            .await
    }
}

fn upload(group_id: Ulid) -> Value {
    json!({"group_id": group_id.to_string(), "name": "hello", "upload": {
        "entrypoint": "hello.cwl", "files": [{"path": "hello.cwl", "content": CWL}]}})
}

#[tokio::test]
async fn serves_registry() {
    let node = node(Some(Arc::new(Scripted))).await;
    let (status, created) = node.create(upload(node.group_id)).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created["version"], "1");
    assert!(created["digest"].as_str().unwrap().starts_with("sha256:"));
    let id = created["workflow_id"].as_str().unwrap().to_owned();

    let (status, page) = node
        .call(&node.owner, "GET", "/compute/workflows", None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["total"], 1);
    assert_eq!(page["next_cursor"], Value::Null);
    let summary = &page["items"][0];
    assert_eq!(
        (summary["language"].as_str(), summary["visibility"].as_str()),
        (Some("cwl"), Some("group"))
    );
    assert_eq!(
        (
            summary["latest_version"].as_str(),
            summary["can_write"].as_bool()
        ),
        (Some("1"), Some(true))
    );

    let path = format!("/compute/workflows/{id}");
    let (status, _) = node.call(&node.stranger, "GET", &path, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let shared = json!({"visibility": "public", "description": null});
    let (status, patched) = node.call(&node.owner, "PATCH", &path, Some(shared)).await;
    assert_eq!(
        (status, &patched["description"]),
        (StatusCode::OK, &Value::Null)
    );
    let (status, read) = node.call(&node.stranger, "GET", &path, None).await;
    assert_eq!(
        (status, read["can_write"].as_bool()),
        (StatusCode::OK, Some(false))
    );

    let version = format!("{path}/versions/1");
    let (status, versions) = node
        .call(&node.owner, "GET", &format!("{path}/versions"), None)
        .await;
    assert_eq!(
        (status, versions["items"][0]["entrypoint"].as_str()),
        (StatusCode::OK, Some("hello.cwl"))
    );
    let (_, check) = node
        .call(&node.owner, "GET", &format!("{version}/check"), None)
        .await;
    assert_eq!(
        check,
        json!({"ok": true, "diagnostics": [
            {"severity": "warning", "message": "message has no default"}
        ]})
    );
    let (_, schema) = node
        .call(&node.owner, "GET", &format!("{version}/schema"), None)
        .await;
    assert_eq!(schema["required"], json!(["message"]));
    let (_, graph) = node
        .call(&node.owner, "GET", &format!("{version}/graph"), None)
        .await;
    assert_eq!(
        graph["edges"][0],
        json!({"from": {"node": "input:message", "port": "message"},
        "to": {"node": "echo", "port": ""}})
    );
    assert_eq!(graph["complete"], false);
    let (_, tools) = node
        .call(&node.owner, "GET", &format!("{version}/tools"), None)
        .await;
    assert_eq!(tools["tools"][0]["language"], "cwl");
    let values = json!({"values": {"message": 3}});
    let (_, checked) = node
        .call(
            &node.owner,
            "POST",
            &format!("{version}/values/validate"),
            Some(values),
        )
        .await;
    assert_eq!(checked, json!({"ok": false, "diagnostics": []}));
    let (status, _) = node
        .call(&node.owner, "GET", &format!("{path}/versions/x"), None)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let next = json!({"uwe_yaml": "uwe_version: \"1\"\n", "message": "composed"});
    let (status, _) = node
        .call(&node.owner, "POST", &format!("{path}/versions"), Some(next))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = node.call(&node.stranger, "DELETE", &path, None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = node.call(&node.owner, "DELETE", &path, None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = node.call(&node.owner, "DELETE", &path, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn refuses_bad_imports() {
    let node = node(Some(Arc::new(Scripted))).await;
    let mut foreign = upload(node.group_id);
    let (status, _) = node
        .call(
            &node.stranger,
            "POST",
            "/compute/workflows",
            Some(foreign.clone()),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    foreign["upload"]["files"][0]["encoding"] = json!("base64");
    foreign["upload"]["files"][0]["content"] = json!("not base64!");
    let (status, _) = node.create(foreign).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let git = json!({"group_id": node.group_id.to_string(), "name": "git",
        "git": {"url": "https://example.org/flows.git", "revision": "main"}});
    let (status, refused) = node.create(git).await;
    assert_eq!(
        (status, refused["code"].as_str()),
        (StatusCode::BAD_REQUEST, Some("not_supported"))
    );

    let offline = self::node(None).await;
    let (status, _) = offline.create(upload(offline.group_id)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn serves_tool_catalog() {
    let node = node(Some(Arc::new(Scripted))).await;
    let group = node.group_id.to_string();
    let extract = json!({"group_id": group, "save": true, "source": {
        "files": [{"path": "hello.cwl", "content": CWL}], "entrypoint": "hello.cwl", "language": "cwl"}});
    let (status, extracted) = node
        .call(&node.owner, "POST", "/compute/tools/extract", Some(extract))
        .await;
    assert_eq!(status, StatusCode::OK, "{extracted}");
    assert_eq!(extracted["tools"][0]["group_id"], json!(group));
    assert_eq!(extracted["diagnostics"], json!([]));
    let definition =
        json!({"id": "shout", "version": "2", "title": "Shout", "inputs": [], "outputs": []});
    let tool = json!({"group_id": group, "definition": definition});
    let (status, created) = node
        .call(&node.owner, "POST", "/compute/tools", Some(tool.clone()))
        .await;
    assert_eq!(
        (status, created["tool_id"].as_str()),
        (StatusCode::CREATED, Some("shout"))
    );
    let (status, _) = node
        .call(&node.owner, "POST", "/compute/tools", Some(tool))
        .await;
    assert_eq!(status, StatusCode::CONFLICT);

    let (_, first) = node
        .call(&node.owner, "GET", "/compute/tools?limit=1", None)
        .await;
    assert_eq!(first["total"], 2);
    let cursor = first["next_cursor"].as_str().unwrap();
    let (_, rest) = node
        .call(
            &node.owner,
            "GET",
            &format!("/compute/tools?limit=1&cursor={cursor}"),
            None,
        )
        .await;
    assert_eq!(
        (rest["items"][0]["tool_id"].as_str(), &rest["next_cursor"]),
        (Some("shout"), &Value::Null)
    );
    let (_, cwl) = node
        .call(&node.owner, "GET", "/compute/tools?language=cwl", None)
        .await;
    assert_eq!(cwl["total"], 1);

    let (status, detail) = node
        .call(&node.owner, "GET", "/compute/tools/echo/versions/1", None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        detail["definition"]["source"]["address"],
        "echo/1/hello.cwl"
    );
    assert_eq!(detail["image"], "debian:stable-slim");
    let (status, _) = node
        .call(
            &node.stranger,
            "GET",
            "/compute/tools/echo/versions/1",
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let suggest = json!({"group_id": group, "direction": "previous", "kind": "file"});
    let (status, found) = node
        .call(&node.owner, "POST", "/compute/tools/suggest", Some(suggest))
        .await;
    assert_eq!(status, StatusCode::OK);
    let first = &found["suggestions"][0];
    assert_eq!(
        (
            first["port"].as_str(),
            first["fit"].as_str(),
            first["scatter"].as_bool()
        ),
        (Some("said"), Some("unknown"), Some(true))
    );
    let draft = json!({"language": "uwe", "group_id": group, "uwe_yaml": "uwe_version: \"1\"\n"});
    let (status, checked) = node
        .call(
            &node.owner,
            "POST",
            "/compute/workflows/validate",
            Some(draft),
        )
        .await;
    assert_eq!(
        (status, &checked["wir"]),
        (StatusCode::OK, &json!({"wir_version": "1"}))
    );
    assert_eq!(checked["graph"]["complete"], true);

    let (status, _) = node
        .call(
            &node.owner,
            "DELETE",
            "/compute/tools/shout/versions/2",
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = node
        .call(
            &node.owner,
            "DELETE",
            "/compute/tools/shout/versions/2",
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// An Argo template that reverses the lines of a text, as a tool to extract.
const REVERSE_TOOL: &str = "apiVersion: argoproj.io/v1alpha1
kind: WorkflowTemplate
metadata:
  name: reverse
spec:
  entrypoint: reverse
  templates:
    - name: reverse
      inputs:
        artifacts:
          - name: text
            path: /tmp/in/text.txt
      container:
        image: debian:stable-slim
        command: [sh, -c]
        args: [\"tac /tmp/in/text.txt > /tmp/reversed.txt\"]
      outputs:
        artifacts:
          - name: reversed
            path: /tmp/reversed.txt
            archive:
              none: {}
";

fn coordinator_binary() -> std::path::PathBuf {
    std::env::var_os("ARUNA_WORKFLOW_COORDINATOR_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../apps/coordinator/target/debug/aruna-coordinator")
        })
}

/// Import, check, schema, graph, validate, extract and suggest against the real coordinator.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs the aruna-coordinator binary"]
async fn drives_real_coordinator() {
    let settings = aruna_workflow::WorkflowSettings {
        binary: Some(coordinator_binary()),
        ..aruna_workflow::WorkflowSettings::default()
    };
    let runtime = aruna_workflow::WorkflowRuntime::configured(&settings, None).unwrap();
    let node = node(Some(Arc::new(runtime))).await;
    let group = node.group_id.to_string();

    let extract = json!({"group_id": group, "save": true, "source": {"language": "argo",
        "entrypoint": "tools/reverse.yaml", "files": [{"path": "tools/reverse.yaml", "content": REVERSE_TOOL}]}});
    let (status, extracted) = node
        .call(&node.owner, "POST", "/compute/tools/extract", Some(extract))
        .await;
    assert_eq!(status, StatusCode::OK, "{extracted}");
    let tool = &extracted["tools"][0];
    assert_eq!(tool["tool_id"], "reverse", "{extracted}");
    let version = tool["version"].as_str().unwrap().to_owned();

    let composed = format!(
        "uwe_version: \"1\"\nname: once\ninputs:\n  text:\n    type: file\nsteps:\n  first:\n    tool: reverse\n    version: \"{version}\"\n    in:\n      text: text\noutputs:\n  result:\n    from: first/reversed\n"
    );
    let draft = json!({"language": "uwe", "group_id": group, "uwe_yaml": composed});
    let (status, checked) = node
        .call(
            &node.owner,
            "POST",
            "/compute/workflows/validate",
            Some(draft),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{checked}");
    assert_eq!(checked["check"]["ok"], true, "{checked}");
    assert!(checked["wir"].is_object(), "{checked}");

    let create = json!({"group_id": group, "name": "once", "uwe_yaml": composed});
    let (status, created) = node.create(create).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = created["workflow_id"].as_str().unwrap();
    let version_path = format!("/compute/workflows/{id}/versions/1");
    let (_, check) = node
        .call(&node.owner, "GET", &format!("{version_path}/check"), None)
        .await;
    assert_eq!(check["ok"], true, "{check}");
    let (status, schema) = node
        .call(&node.owner, "GET", &format!("{version_path}/schema"), None)
        .await;
    assert_eq!(
        (status, &schema["type"]),
        (StatusCode::OK, &json!("object")),
        "{schema}"
    );
    let (_, graph) = node
        .call(&node.owner, "GET", &format!("{version_path}/graph"), None)
        .await;
    let ids: Vec<&str> = graph["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|node| node["id"].as_str())
        .collect();
    assert_eq!(ids, ["input:text", "first", "output:result"], "{graph}");
    let (_, read) = node.call(&node.owner, "GET", &version_path, None).await;
    assert_eq!(read["uwe_yaml"], json!(composed));

    let cwl = "cwlVersion: v1.2\nclass: CommandLineTool\nbaseCommand: echo\nhints:\n  DockerRequirement:\n    dockerPull: debian:stable-slim\ninputs:\n  message:\n    type: string\n    inputBinding:\n      position: 1\noutputs:\n  said:\n    type: stdout\nstdout: said.txt\n";
    let upload = json!({"group_id": group, "name": "hello", "upload": {"entrypoint": "hello.cwl",
        "files": [{"path": "hello.cwl", "content": cwl}]}});
    let (status, hello) = node.create(upload).await;
    assert_eq!(status, StatusCode::CREATED, "{hello}");
    let hello_path = format!(
        "/compute/workflows/{}/versions/1",
        hello["workflow_id"].as_str().unwrap()
    );
    let (_, summary) = node
        .call(
            &node.owner,
            "GET",
            &format!(
                "/compute/workflows/{}",
                hello["workflow_id"].as_str().unwrap()
            ),
            None,
        )
        .await;
    assert_eq!(summary["language"], "cwl");
    let values = json!({"values": {"message": 3}});
    let (_, refused) = node
        .call(
            &node.owner,
            "POST",
            &format!("{hello_path}/values/validate"),
            Some(values),
        )
        .await;
    assert_eq!(refused["ok"], false, "{refused}");
    let values = json!({"values": {"message": "Hola"}});
    let (_, accepted) = node
        .call(
            &node.owner,
            "POST",
            &format!("{hello_path}/values/validate"),
            Some(values),
        )
        .await;
    assert_eq!(accepted["ok"], true, "{accepted}");
    let (_, tools) = node
        .call(&node.owner, "GET", &format!("{hello_path}/tools"), None)
        .await;
    assert!(!tools["tools"].as_array().unwrap().is_empty(), "{tools}");

    let suggest =
        json!({"group_id": group, "direction": "next", "kind": "file", "cardinality": "one"});
    let (status, found) = node
        .call(&node.owner, "POST", "/compute/tools/suggest", Some(suggest))
        .await;
    assert_eq!(status, StatusCode::OK, "{found}");
    assert_eq!(found["suggestions"][0]["port"], "text", "{found}");
}
