//! Tests the workflow registry and tool catalog on memory storage with a scripted coordinator:
//! every source kind, group access and visibility, paging, stored results and the tools.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::{Arc, Mutex};

use aruna_compute::registry::ExecutorRegistry;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::structs::execution::workflow::{
    StoredWorkflowLanguage, ToolCuration, WorkflowVisibility,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_core::user::id::UserId;
use aruna_core::workflow::{WorkflowAnswer, WorkflowEffect, WorkflowError, WorkflowHost};
use aruna_runner::driver::{DriverContext, drive};
use aruna_workflow_contract::preflight::{
    ComposedLowering, Diagnostic, DiagnosticPointer, GraphEdge, GraphNode, NodeKind, Severity,
    SourceFile, SuggestDirection, WorkflowGraph,
};
use aruna_workflow_contract::{ContractError, PreflightKind, PreflightResult};
use serde_json::{Value, json};
use ulid::Ulid;

use super::CatalogError;
use super::import::{
    AddVersionOperation, CreateWorkflowOperation, GitInput, NewWorkflow, ObjectsInput, SourceInput,
};
use super::inspect::{
    Draft, Facet, FacetValue, InspectOperation, ValidateDraftOperation, ValidateValuesOperation,
};
use super::reads::{
    DeleteWorkflowOperation, GetWorkflowOperation, ListWorkflowsOperation, PatchWorkflowOperation,
    WorkflowFilter, WorkflowPatch,
};
use super::tools::{
    CreateToolOperation, DeleteToolOperation, ExtractSource, ExtractToolsOperation,
    GetToolOperation, ListToolsOperation, PortQuery, SuggestToolsOperation, ToolFilter,
};
use super::versions::{GetVersionOperation, ListVersionsOperation};

const CWL: &str = "cwlVersion: v1.2\nclass: CommandLineTool\nbaseCommand: echo\n";
const COMPOSED: &str = "uwe_version: \"1\"\nsteps:\n  first:\n    tool: reverse\n";

/// A coordinator that answers from the request alone and counts what it was asked.
#[derive(Default)]
struct Scripted {
    asked: Mutex<Vec<&'static str>>,
}

impl Scripted {
    fn count(&self, name: &str) -> usize {
        self.asked
            .lock()
            .unwrap()
            .iter()
            .filter(|asked| **asked == name)
            .count()
    }
}

fn reverse_tool(address: &str) -> Value {
    json!({
        "id": "reverse", "version": "1", "title": "Reverse",
        "source": {"kind": "argo", "address": address, "revision": "r1"},
        "inputs": [{"name": "text", "kind": "file", "formats": ["format_2330"]}],
        "outputs": [{"name": "reversed", "kind": "file", "format": {"terms": ["format_2330"]}}]
    })
}

pub(super) fn answer(kind: PreflightKind) -> Result<PreflightResult, WorkflowError> {
    let broken = |source: &aruna_workflow_contract::preflight::WorkflowSource| {
        source.files.iter().any(|file| file.content == b"broken")
    };
    Ok(match kind {
        PreflightKind::Check {
            source, language, ..
        } => {
            if broken(&source) {
                return Err(WorkflowError::Refused(ContractError::workflow(
                    "cannot parse",
                )));
            }
            let language = language.map_or("cwl".into(), |language| {
                serde_json::to_value(language)
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_owned()
            });
            PreflightResult::Check {
                runnable: true,
                report_json: json!({"language": language, "parts": [
                    {"name": "echo", "support": "compiled", "images": ["debian:stable-slim"]}
                ], "notes": ["remote files were not read"]}),
                images: vec!["debian:stable-slim".into()],
            }
        }
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
                edges: Vec::new(),
            },
        },
        PreflightKind::ValidateValues { values, .. } => {
            let valid = values["message"].is_string();
            let diagnostics: Vec<Value> = match valid {
                true => Vec::new(),
                false => vec![
                    json!({"message": "message is required", "instance_path": "", "input": "message", "keyword": "required"}),
                ],
            };
            PreflightResult::ValidateValues {
                valid,
                response_json: json!({"accepted": valid, "diagnostics": diagnostics}),
            }
        }
        PreflightKind::LowerComposed {
            uwe_yaml,
            catalog_tools,
            tool_files,
        } => {
            let known = catalog_tools.iter().any(|tool| tool["id"] == "reverse")
                && tool_files.file("reverse/1/tools/reverse.yaml").is_some();
            let mut lowering = ComposedLowering {
                graph: WorkflowGraph {
                    nodes: vec![GraphNode {
                        id: "first".into(),
                        label: "first".into(),
                        kind: NodeKind::Step,
                        tool: Some("reverse".into()),
                        version: Some("1".into()),
                    }],
                    edges: vec![GraphEdge {
                        from: "input:text".into(),
                        to: "first".into(),
                        from_port: Some("text".into()),
                        to_port: Some("text".into()),
                    }],
                },
                ..ComposedLowering::default()
            };
            match known && uwe_yaml.contains("tool: reverse") {
                true => lowering.wir_json = Some(json!({"wir_version": "1"})),
                false => lowering.diagnostics.push(Diagnostic {
                    severity: Severity::Error,
                    message: "step first: no tool reverse".into(),
                    pointer: DiagnosticPointer {
                        node: Some("first".into()),
                        ..DiagnosticPointer::default()
                    },
                }),
            }
            PreflightResult::LowerComposed(lowering)
        }
        PreflightKind::ExtractTools { entrypoint, .. } => PreflightResult::ExtractTools {
            tools: vec![reverse_tool(&entrypoint)],
        },
        PreflightKind::SuggestTools {
            catalog_tools,
            direction,
            ..
        } => {
            let port = match direction {
                SuggestDirection::Followers => "text",
                SuggestDirection::Producers => "reversed",
            };
            let suggestions = (catalog_tools.iter())
                .map(|tool| {
                    json!({"tool": tool["id"], "version": tool["version"], "port": port,
                    "check": {"fit": "fits", "scatter": false, "format": "exact", "findings": []}})
                })
                .collect();
            PreflightResult::SuggestTools { suggestions }
        }
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
        let WorkflowEffect::Preflight(kind) = effect else {
            return WorkflowAnswer::Opened(Err(WorkflowError::Unavailable));
        };
        self.asked.lock().unwrap().push(kind.name());
        WorkflowAnswer::Preflight(answer(kind))
    }
}

pub(super) struct Fixture {
    pub(super) context: DriverContext,
    host: Arc<Scripted>,
    pub(super) node: NodeId,
    pub(super) alice: AuthContext,
    pub(super) bob: AuthContext,
    pub(super) stranger: AuthContext,
    pub(super) group_a: Ulid,
    group_b: Ulid,
}

pub(super) async fn write(context: &DriverContext, space: &str, key: Vec<u8>, value: Vec<u8>) {
    let effect = StorageEffect::Write {
        key_space: space.into(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    };
    let event = context.storage_handle.send_storage_effect(effect).await;
    assert!(
        matches!(event, Event::Storage(StorageEvent::WriteResult { .. })),
        "{event:?}"
    );
}

pub(super) async fn fixture() -> Fixture {
    let mut context =
        aruna_runner::testing::test_context(aruna_storage::storage::memory::MemoryStorage::open());
    let host = Arc::new(Scripted::default());
    let registry = ExecutorRegistry::new().with_workflow(host.clone());
    context.compute_handle = Some(Arc::new(registry));
    let realm_id = RealmId([5u8; 32]);
    let node = aruna_ops_identity::testing::node(3);
    let user = |seed: u8| {
        AuthContext::new(
            UserId::new(Ulid::from_bytes([seed; 16]), realm_id),
            realm_id,
        )
    };
    let (alice, bob, stranger) = (user(1), user(2), user(9));
    let (group_a, group_b) = (Ulid::from_bytes([10; 16]), Ulid::from_bytes([11; 16]));
    let actor = Actor {
        node_id: node,
        user_id: alice.user_id,
        realm_id,
    };
    let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
    write(
        &context,
        AUTH_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        realm_auth.to_bytes(&actor).unwrap(),
    )
    .await;
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.ensure_node(node, RealmNodeKind::Server);
    write(
        &context,
        REALM_CONFIG_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        config.to_bytes(&actor).unwrap(),
    )
    .await;
    for (group_id, owner) in [(group_a, &alice), (group_b, &bob)] {
        let roles =
            GroupAuthorizationDocument::default_group_doc(owner.user_id, realm_id, group_id);
        let group = Group {
            display_name: "workflows".into(),
            group_id,
            realm_id,
            roles: roles.roles.keys().copied().collect(),
            owner: owner.user_id,
        };
        write(
            &context,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            roles.to_bytes(&actor).unwrap(),
        )
        .await;
        write(
            &context,
            GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group.to_bytes(&actor).unwrap(),
        )
        .await;
    }
    Fixture {
        context,
        host,
        node,
        alice,
        bob,
        stranger,
        group_a,
        group_b,
    }
}

pub(super) fn upload(content: &str) -> SourceInput {
    SourceInput::Upload {
        files: vec![SourceFile {
            path: "hello.cwl".into(),
            content: content.as_bytes().to_vec(),
            executable: false,
        }],
        entrypoint: "hello.cwl".into(),
    }
}

pub(super) fn new_workflow(group_id: Ulid, name: &str, source: SourceInput) -> NewWorkflow {
    NewWorkflow {
        group_id,
        name: name.into(),
        description: Some("Says hello".into()),
        language: None,
        visibility: WorkflowVisibility::Group,
        source,
    }
}

impl Fixture {
    pub(super) async fn create(
        &self,
        auth: &AuthContext,
        input: NewWorkflow,
    ) -> Result<super::import::Created, CatalogError> {
        drive(
            CreateWorkflowOperation::new(auth, self.node, input),
            &self.context,
        )
        .await
    }

    async fn list(&self, auth: &AuthContext, filter: WorkflowFilter) -> Vec<String> {
        let page = drive(
            ListWorkflowsOperation::new(auth, filter, None, 50),
            &self.context,
        )
        .await
        .unwrap();
        page.items
            .into_iter()
            .map(|item| item.record.name)
            .collect()
    }

    async fn tool(
        &self,
        auth: &AuthContext,
        group_id: Ulid,
    ) -> Result<super::tools::ToolView, CatalogError> {
        let tools = drive(
            ExtractToolsOperation::new(
                auth,
                group_id,
                ExtractSource::Files {
                    files: vec![SourceFile {
                        path: "tools/reverse.yaml".into(),
                        content: b"kind: WorkflowTemplate".to_vec(),
                        executable: false,
                    }],
                    entrypoint: "tools/reverse.yaml".into(),
                    language: StoredWorkflowLanguage::Argo,
                },
                true,
            ),
            &self.context,
        )
        .await?;
        Ok(tools.tools.into_iter().next().unwrap())
    }
}

#[tokio::test]
async fn imports_uploads() {
    let fixture = fixture().await;
    let created = fixture
        .create(
            &fixture.alice,
            new_workflow(fixture.group_a, "hello", upload(CWL)),
        )
        .await
        .unwrap();
    assert_eq!(created.version, 1);
    assert!(created.digest.starts_with("sha256:"));
    let read = drive(
        GetWorkflowOperation::new(&fixture.alice, created.workflow_id),
        &fixture.context,
    )
    .await
    .unwrap();
    assert!(read.can_write);
    assert_eq!(read.record.language, StoredWorkflowLanguage::Cwl);
    let version = drive(
        GetVersionOperation::new(&fixture.alice, created.workflow_id, 1),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(version.version.entrypoint, "hello.cwl");
    assert_eq!(version.version.files, ["hello.cwl"]);
    assert!(version.version.inspection.check.is_some());
    assert!(version.version.inspection.schema.is_some());
    // The node has no backend, so the image stays unpinned and the check says so.
    assert!(version.version.images.is_empty());
    let check = drive(
        InspectOperation::new(&fixture.alice, created.workflow_id, 1, Facet::Check),
        &fixture.context,
    )
    .await
    .unwrap();
    let FacetValue::Check(report) = check else {
        panic!("{check:?}")
    };
    assert!(report.ok);
    assert!(
        report
            .diagnostics
            .iter()
            .any(|found| found.message.contains("unpinned"))
    );

    let broken = fixture
        .create(
            &fixture.alice,
            new_workflow(fixture.group_a, "broken", upload("broken")),
        )
        .await;
    assert_eq!(broken, Err(CatalogError::Invalid("cannot parse".into())));
    let escaping = SourceInput::Upload {
        files: vec![SourceFile {
            path: "../x".into(),
            content: Vec::new(),
            executable: false,
        }],
        entrypoint: "../x".into(),
    };
    let refused = fixture
        .create(&fixture.alice, new_workflow(fixture.group_a, "x", escaping))
        .await;
    assert!(
        matches!(refused, Err(CatalogError::Invalid(_))),
        "{refused:?}"
    );
}

#[tokio::test]
async fn guards_group_access() {
    let fixture = fixture().await;
    let foreign = fixture
        .create(
            &fixture.bob,
            new_workflow(fixture.group_a, "hello", upload(CWL)),
        )
        .await;
    assert_eq!(foreign, Err(CatalogError::Forbidden));
    let created = fixture
        .create(
            &fixture.alice,
            new_workflow(fixture.group_a, "hello", upload(CWL)),
        )
        .await
        .unwrap();
    fixture
        .create(
            &fixture.bob,
            new_workflow(fixture.group_b, "other", upload(CWL)),
        )
        .await
        .unwrap();
    let id = created.workflow_id;
    let hidden = drive(
        GetWorkflowOperation::new(&fixture.bob, id),
        &fixture.context,
    )
    .await;
    assert_eq!(hidden, Err(CatalogError::NotFound));
    assert_eq!(
        fixture.list(&fixture.bob, WorkflowFilter::default()).await,
        ["other"]
    );
    assert!(
        fixture
            .list(&fixture.stranger, WorkflowFilter::default())
            .await
            .is_empty()
    );

    let public = WorkflowPatch {
        visibility: Some(WorkflowVisibility::Public),
        name: Some("hello public".into()),
        ..WorkflowPatch::default()
    };
    let patched = drive(
        PatchWorkflowOperation::new(&fixture.alice, id, public),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(patched.record.name, "hello public");
    let shared = drive(
        GetWorkflowOperation::new(&fixture.bob, id),
        &fixture.context,
    )
    .await
    .unwrap();
    assert!(!shared.can_write);
    let mut listed = fixture.list(&fixture.bob, WorkflowFilter::default()).await;
    listed.sort();
    assert_eq!(listed, ["hello public", "other"]);
    assert_eq!(
        fixture
            .list(&fixture.stranger, WorkflowFilter::default())
            .await,
        ["hello public"]
    );
    let group_only = WorkflowFilter {
        group_id: Some(fixture.group_b),
        ..WorkflowFilter::default()
    };
    assert_eq!(fixture.list(&fixture.bob, group_only).await, ["other"]);

    let renamed = WorkflowPatch {
        name: Some("taken".into()),
        ..WorkflowPatch::default()
    };
    let refused = drive(
        PatchWorkflowOperation::new(&fixture.bob, id, renamed),
        &fixture.context,
    )
    .await;
    assert_eq!(refused, Err(CatalogError::Forbidden));
    let removed = drive(
        DeleteWorkflowOperation::new(&fixture.bob, id),
        &fixture.context,
    )
    .await;
    assert_eq!(removed, Err(CatalogError::Forbidden));
    let added = drive(
        AddVersionOperation::new((&fixture.bob, fixture.node), id, upload(CWL), None),
        &fixture.context,
    )
    .await;
    assert_eq!(added, Err(CatalogError::Forbidden));

    drive(
        DeleteWorkflowOperation::new(&fixture.alice, id),
        &fixture.context,
    )
    .await
    .unwrap();
    let gone = drive(
        GetVersionOperation::new(&fixture.alice, id, 1),
        &fixture.context,
    )
    .await;
    assert_eq!(gone, Err(CatalogError::NotFound));
    assert_eq!(
        drive(
            DeleteWorkflowOperation::new(&fixture.alice, id),
            &fixture.context
        )
        .await,
        Err(CatalogError::NotFound)
    );
}

#[tokio::test]
async fn pages_versions() {
    let fixture = fixture().await;
    let created = fixture
        .create(
            &fixture.alice,
            new_workflow(fixture.group_a, "hello", upload(CWL)),
        )
        .await
        .unwrap();
    let id = created.workflow_id;
    for message in ["second", "third"] {
        let added = AddVersionOperation::new(
            (&fixture.alice, fixture.node),
            id,
            upload(CWL),
            Some(message.into()),
        );
        drive(added, &fixture.context).await.unwrap();
    }
    let wir = AddVersionOperation::new(
        (&fixture.alice, fixture.node),
        id,
        SourceInput::Wir(json!({"wir_version": "1"})),
        None,
    );
    assert!(matches!(
        drive(wir, &fixture.context).await,
        Err(CatalogError::Invalid(_))
    ));
    let first = drive(
        ListVersionsOperation::new(&fixture.alice, id, None, 2),
        &fixture.context,
    )
    .await
    .unwrap();
    let numbers: Vec<u32> = first.items.iter().map(|version| version.version).collect();
    assert_eq!((numbers, first.next, first.total), (vec![1, 2], Some(2), 3));
    let rest = drive(
        ListVersionsOperation::new(&fixture.alice, id, first.next, 2),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(rest.items[0].message.as_deref(), Some("third"));
    assert_eq!(rest.next, None);
    let read = drive(
        GetWorkflowOperation::new(&fixture.alice, id),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(read.record.version_count, 3);

    let wir_workflow = new_workflow(
        fixture.group_a,
        "lowered",
        SourceInput::Wir(json!({"wir_version": "1"})),
    );
    let wir = fixture.create(&fixture.alice, wir_workflow).await.unwrap();
    let wir_version = drive(
        GetVersionOperation::new(&fixture.alice, wir.workflow_id, 1),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(wir_version.version.entrypoint, "workflow.wir.json");
    assert_eq!(wir_version.workflow.language, StoredWorkflowLanguage::Wir);
    let only_wir = WorkflowFilter {
        language: Some(StoredWorkflowLanguage::Wir),
        ..WorkflowFilter::default()
    };
    assert_eq!(fixture.list(&fixture.alice, only_wir).await, ["lowered"]);
    let words = WorkflowFilter {
        text: Some("SAYS".into()),
        ..WorkflowFilter::default()
    };
    assert_eq!(fixture.list(&fixture.alice, words).await.len(), 2);
    let page = drive(
        ListWorkflowsOperation::new(&fixture.alice, WorkflowFilter::default(), None, 1),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!((page.items.len(), page.total), (1, 2));
    let next = drive(
        ListWorkflowsOperation::new(&fixture.alice, WorkflowFilter::default(), page.next, 1),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!((next.items.len(), next.next), (1, None));
}

#[tokio::test]
async fn lowers_composed_workflows() {
    let fixture = fixture().await;
    let missing = fixture
        .create(
            &fixture.alice,
            new_workflow(
                fixture.group_a,
                "draft",
                SourceInput::UweYaml(COMPOSED.into()),
            ),
        )
        .await
        .unwrap();
    let check = drive(
        InspectOperation::new(&fixture.alice, missing.workflow_id, 1, Facet::Check),
        &fixture.context,
    )
    .await
    .unwrap();
    assert!(matches!(check, FacetValue::Check(report) if !report.ok));

    let tool = fixture.tool(&fixture.alice, fixture.group_a).await.unwrap();
    assert_eq!(
        tool.definition["source"]["address"],
        "reverse/1/tools/reverse.yaml"
    );
    let composed = fixture
        .create(
            &fixture.alice,
            new_workflow(
                fixture.group_a,
                "twice",
                SourceInput::UweYaml(COMPOSED.into()),
            ),
        )
        .await
        .unwrap();
    let version = drive(
        GetVersionOperation::new(&fixture.alice, composed.workflow_id, 1),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(version.workflow.language, StoredWorkflowLanguage::Uwe);
    assert_eq!(version.version.uwe_yaml.as_deref(), Some(COMPOSED));
    assert_eq!(
        version.version.files,
        ["workflow.uwe.yaml", "workflow.wir.json"]
    );
    let graph = drive(
        InspectOperation::new(&fixture.alice, composed.workflow_id, 1, Facet::Graph),
        &fixture.context,
    )
    .await
    .unwrap();
    let FacetValue::Graph(graph) = graph else {
        panic!("{graph:?}")
    };
    assert!(graph.complete);
    assert_eq!(graph.graph.nodes[0].tool.as_deref(), Some("reverse"));
    let upload = AddVersionOperation::new(
        (&fixture.alice, fixture.node),
        composed.workflow_id,
        upload(CWL),
        None,
    );
    assert!(matches!(
        drive(upload, &fixture.context).await,
        Err(CatalogError::Invalid(_))
    ));
}

#[tokio::test]
async fn stores_inspection_once() {
    let fixture = fixture().await;
    let created = fixture
        .create(
            &fixture.alice,
            new_workflow(fixture.group_a, "hello", upload(CWL)),
        )
        .await
        .unwrap();
    let id = created.workflow_id;
    let inspected = fixture.host.count("inspect");
    let schema = drive(
        InspectOperation::new(&fixture.alice, id, 1, Facet::Schema),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(
        schema,
        FacetValue::Schema(json!({"type": "object", "required": ["message"]}))
    );
    assert_eq!(fixture.host.count("inspect"), inspected);
    for _ in 0..2 {
        let tools = drive(
            InspectOperation::new(&fixture.alice, id, 1, Facet::Tools),
            &fixture.context,
        )
        .await
        .unwrap();
        assert!(matches!(tools, FacetValue::Tools(found) if found.len() == 1));
    }
    assert_eq!(fixture.host.count("extract_tools"), 1);
    let hidden = drive(
        InspectOperation::new(&fixture.bob, id, 1, Facet::Tools),
        &fixture.context,
    )
    .await;
    assert_eq!(hidden, Err(CatalogError::NotFound));

    let values = |values: Value| ValidateValuesOperation::new(&fixture.alice, id, 1, values);
    let ok = drive(values(json!({"message": "hi"})), &fixture.context)
        .await
        .unwrap();
    assert!(ok.ok && ok.diagnostics.is_empty());
    let refused = drive(values(json!({})), &fixture.context).await.unwrap();
    assert!(!refused.ok);
    assert_eq!(refused.diagnostics[0]["input"], "message");
}

#[tokio::test]
async fn validates_drafts() {
    let fixture = fixture().await;
    fixture.tool(&fixture.alice, fixture.group_a).await.unwrap();
    let composed = Draft {
        language: StoredWorkflowLanguage::Uwe,
        group_id: Some(fixture.group_a),
        uwe_yaml: Some(COMPOSED.into()),
        files: Vec::new(),
        entrypoint: None,
    };
    let report = drive(
        ValidateDraftOperation::new(&fixture.alice, composed.clone()),
        &fixture.context,
    )
    .await
    .unwrap();
    assert!(report.check.ok);
    assert_eq!(report.wir, Some(json!({"wir_version": "1"})));
    let foreign = drive(
        ValidateDraftOperation::new(&fixture.bob, composed),
        &fixture.context,
    )
    .await;
    assert_eq!(foreign, Err(CatalogError::Forbidden));
    let files = Draft {
        language: StoredWorkflowLanguage::Cwl,
        group_id: None,
        uwe_yaml: None,
        files: vec![SourceFile {
            path: "hello.cwl".into(),
            content: b"broken".to_vec(),
            executable: false,
        }],
        entrypoint: Some("hello.cwl".into()),
    };
    let report = drive(
        ValidateDraftOperation::new(&fixture.alice, files),
        &fixture.context,
    )
    .await
    .unwrap();
    assert!(!report.check.ok);
    assert_eq!(report.check.diagnostics[0].message, "cannot parse");
    assert!(
        fixture
            .list(&fixture.alice, WorkflowFilter::default())
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn manages_catalog_tools() {
    let fixture = fixture().await;
    let created = drive(
        CreateToolOperation::new(&fixture.alice, fixture.group_a, reverse_tool("main.nf")),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(created.record.curation, ToolCuration::Unchecked);
    let again = drive(
        CreateToolOperation::new(&fixture.alice, fixture.group_a, reverse_tool("main.nf")),
        &fixture.context,
    )
    .await;
    assert!(matches!(again, Err(CatalogError::Conflict(_))));
    let foreign = drive(
        CreateToolOperation::new(&fixture.bob, fixture.group_a, reverse_tool("main.nf")),
        &fixture.context,
    )
    .await;
    assert_eq!(foreign, Err(CatalogError::Forbidden));
    let unnamed = drive(
        CreateToolOperation::new(&fixture.alice, fixture.group_a, json!({"id": "x"})),
        &fixture.context,
    )
    .await;
    assert!(matches!(unnamed, Err(CatalogError::Invalid(_))));
    // Another group may hold its own copy of the same tool version.
    drive(
        CreateToolOperation::new(&fixture.bob, fixture.group_b, reverse_tool("main.nf")),
        &fixture.context,
    )
    .await
    .unwrap();

    let list =
        |auth: &AuthContext, filter: ToolFilter| ListToolsOperation::new(auth, filter, None, 10);
    let all = drive(
        list(&fixture.alice, ToolFilter::default()),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(all.total, 1);
    let argo = ToolFilter {
        language: Some("argo".into()),
        format: Some("format_2330".into()),
        ..ToolFilter::default()
    };
    assert_eq!(
        drive(list(&fixture.alice, argo), &fixture.context)
            .await
            .unwrap()
            .total,
        1
    );
    let curated = ToolFilter {
        curation: Some(ToolCuration::Curated),
        ..ToolFilter::default()
    };
    assert_eq!(
        drive(list(&fixture.alice, curated), &fixture.context)
            .await
            .unwrap()
            .total,
        0
    );
    let read = drive(
        GetToolOperation::new(&fixture.stranger, "reverse", "1"),
        &fixture.context,
    )
    .await;
    assert_eq!(read, Err(CatalogError::NotFound));
    let read = drive(
        GetToolOperation::new(&fixture.bob, "reverse", "1"),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(read.record.group_id, fixture.group_b);

    let suggest = |direction| PortQuery {
        direction,
        kind: json!("file"),
        cardinality: json!("one"),
        formats: Vec::new(),
        conditions: Vec::new(),
        limit: 5,
    };
    let next = drive(
        SuggestToolsOperation::new(
            &fixture.alice,
            Some(fixture.group_a),
            suggest(SuggestDirection::Followers),
        ),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(
        (
            next[0].tool.record.tool_id.as_str(),
            next[0].port.as_str(),
            next[0].fit.as_str()
        ),
        ("reverse", "text", "fits")
    );
    let previous = drive(
        SuggestToolsOperation::new(&fixture.alice, None, suggest(SuggestDirection::Producers)),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(previous[0].port, "reversed");
    let foreign = drive(
        SuggestToolsOperation::new(
            &fixture.bob,
            Some(fixture.group_a),
            suggest(SuggestDirection::Followers),
        ),
        &fixture.context,
    )
    .await;
    assert_eq!(foreign, Err(CatalogError::Forbidden));

    drive(
        DeleteToolOperation::new(&fixture.alice, "reverse", "1"),
        &fixture.context,
    )
    .await
    .unwrap();
    let gone = drive(
        DeleteToolOperation::new(&fixture.alice, "reverse", "1"),
        &fixture.context,
    )
    .await;
    assert_eq!(gone, Err(CatalogError::NotFound));
    assert!(
        drive(
            GetToolOperation::new(&fixture.bob, "reverse", "1"),
            &fixture.context
        )
        .await
        .is_ok()
    );
}

#[tokio::test]
async fn extracts_tools() {
    let fixture = fixture().await;
    let created = fixture
        .create(
            &fixture.alice,
            new_workflow(fixture.group_a, "hello", upload(CWL)),
        )
        .await
        .unwrap();
    let source = ExtractSource::Workflow {
        workflow_id: created.workflow_id,
        version: 1,
    };
    let shown = drive(
        ExtractToolsOperation::new(&fixture.alice, fixture.group_a, source.clone(), false),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(shown.tools.len(), 1);
    assert_eq!(shown.tools[0].record.files[0].path, "reverse/1/hello.cwl");
    let listed = drive(
        ListToolsOperation::new(&fixture.alice, ToolFilter::default(), None, 10),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(listed.total, 0);
    let foreign = drive(
        ExtractToolsOperation::new(&fixture.bob, fixture.group_b, source.clone(), true),
        &fixture.context,
    )
    .await;
    assert_eq!(foreign, Err(CatalogError::NotFound));
    let viewer = drive(
        ExtractToolsOperation::new(&fixture.bob, fixture.group_a, source.clone(), false),
        &fixture.context,
    )
    .await;
    assert_eq!(viewer, Err(CatalogError::Forbidden));
    drive(
        ExtractToolsOperation::new(&fixture.alice, fixture.group_a, source, true),
        &fixture.context,
    )
    .await
    .unwrap();
    let listed = drive(
        ListToolsOperation::new(&fixture.alice, ToolFilter::default(), None, 10),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(listed.total, 1);
    let git = ExtractSource::Git(GitInput {
        url: "https://example.org/flows.git".into(),
        revision: Some("main".into()),
        path: None,
        entrypoint: Some("main.nf".into()),
    });
    let unsupported = drive(
        ExtractToolsOperation::new(&fixture.alice, fixture.group_a, git, false),
        &fixture.context,
    )
    .await;
    assert!(matches!(unsupported, Err(CatalogError::Unsupported(_))));
}

#[tokio::test]
async fn refuses_foreign_sources() {
    let fixture = fixture().await;
    let git = |url: &str| {
        SourceInput::Git(GitInput {
            url: url.into(),
            revision: Some("v1.0".into()),
            path: None,
            entrypoint: Some("main.nf".into()),
        })
    };
    let insecure = fixture
        .create(
            &fixture.alice,
            new_workflow(fixture.group_a, "git", git("http://example.org/x.git")),
        )
        .await;
    assert!(
        matches!(insecure, Err(CatalogError::Invalid(_))),
        "{insecure:?}"
    );
    let unsupported = fixture
        .create(
            &fixture.alice,
            new_workflow(fixture.group_a, "git", git("https://example.org/x.git")),
        )
        .await;
    assert!(
        matches!(unsupported, Err(CatalogError::Unsupported(_))),
        "{unsupported:?}"
    );
    let objects = |node_id| {
        SourceInput::Objects(ObjectsInput {
            bucket: "flows".into(),
            prefix: Some("hello/".into()),
            keys: Vec::new(),
            node_id,
            entrypoint: Some("main.nf".into()),
        })
    };
    let elsewhere = objects(Some(aruna_ops_identity::testing::node(8)));
    let refused = fixture
        .create(
            &fixture.alice,
            new_workflow(fixture.group_a, "objects", elsewhere),
        )
        .await;
    assert!(
        matches!(refused, Err(CatalogError::Invalid(_))),
        "{refused:?}"
    );
    let missing = fixture
        .create(
            &fixture.alice,
            new_workflow(fixture.group_a, "objects", objects(None)),
        )
        .await;
    assert_eq!(missing, Err(CatalogError::NotFound));
}
