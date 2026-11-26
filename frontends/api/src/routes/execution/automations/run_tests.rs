//! HTTP automation run control, kept-event replay, applet visitor runs and MCP safety tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::applet::record::{Access, Applet, Embedding, Folder, Grants, Lifecycle as AppLife};
use aruna_core::automation::keys::kept_key;
use aruna_core::automation::record::KeptEvent;
use aruna_core::keyspaces::{AUTOMATIONS_KEYSPACE, WORKFLOW_RUN_KEYSPACE};
use aruna_core::plugin_runs::SourceEvent;
use aruna_core::structs::execution::workflow_run::{
    AutomationOrigin, CacheScope, RunOutput, RunState, WorkflowRunRecord, run_key,
};
use aruna_ops_work::applets::intake::Visit;
use aruna_ops_work::applets::runs::VisitorRunsOperation;
use aruna_ops_work::automations::runner::StepFiringsOperation;
use aruna_runner::driver::drive;
use axum::http::StatusCode;
use rmcp::handler::server::tool::Extension as McpExt;
use rmcp::handler::server::wrapper::Parameters;
use serde_json::json;
use ulid::Ulid;

use super::route_tests::Harness;
use crate::mcp::{JsonPayload, McpServer, automations as mcp_auto, caller};
use crate::tests::routes::write_doc;

#[tokio::test]
async fn runs_and_visitors() {
    let h = Harness::new().await;
    h.set_features(true, true).await;
    let body = json!({
        "group_id": h.group.to_string(),
        "name": "run-test",
        "spec": h.base_spec()
    });
    let (_, created) = h
        .call(&h.owner, "POST", "/compute/automations", Some(body))
        .await;
    let id = created["automation_id"].as_str().unwrap().to_string();
    h.call(
        &h.owner,
        "POST",
        &format!("/compute/automations/{id}/request-approval"),
        Some(json!({})),
    )
    .await;
    h.call(
        &h.owner,
        "POST",
        &format!("/compute/automations/{id}/approve"),
        Some(json!({})),
    )
    .await;

    // Manual start -> step -> stage approval (admin only) -> list/get/cancel -> replay kept event.
    let (status, started) = h
        .call(
            &h.owner,
            "POST",
            &format!("/compute/automations/{id}/start"),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{started}");
    let fid = started["firing_id"].as_str().unwrap().to_string();
    drive(
        StepFiringsOperation::new((h.node, h.realm)),
        &h.state.get_ctx(),
    )
    .await
    .unwrap();
    let (status, runs) = h
        .call(
            &h.owner,
            "GET",
            &format!("/compute/automation-runs?group_id={}", h.group),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(runs["items"].as_array().unwrap().len(), 1);
    let (status, _) = h
        .call(
            &h.member,
            "POST",
            &format!("/compute/automation-runs/{fid}/stages/gate/approve"),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, approved) = h
        .call(
            &h.owner,
            "POST",
            &format!("/compute/automation-runs/{fid}/stages/gate/approve"),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(approved["stages"][0]["state"], "succeeded");
    let (status, cancelled) = h
        .call(
            &h.owner,
            "POST",
            &format!("/compute/automation-runs/{fid}/cancel"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cancelled["state"], "cancelled");

    // Seed one kept event and replay it over HTTP.
    let auto_ulid = Ulid::from_string(&id).unwrap();
    let ev_id = Ulid::from_bytes([88; 16]);
    let kept = KeptEvent {
        event_id: ev_id,
        automation_id: auto_ulid,
        event: SourceEvent::data_uploaded(
            (h.realm, h.group, h.node),
            ("raw", "in/x.csv"),
            Ulid::from_bytes([89; 16]),
            Some(h.owner.user_id),
            5,
        ),
        reason: "paused".into(),
        kept_at_ms: 5,
    };
    write_doc(
        &h.state.get_ctx(),
        AUTOMATIONS_KEYSPACE,
        kept_key(auto_ulid, ev_id),
        kept.to_bytes().unwrap().into(),
    )
    .await;
    let (status, evs) = h
        .call(
            &h.owner,
            "GET",
            &format!("/compute/automations/{id}/events"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(evs.as_array().unwrap().len(), 1);
    let (status, started_evs) = h
        .call(
            &h.owner,
            "POST",
            &format!("/compute/automations/{id}/events/start"),
            Some(json!({"event_ids": [ev_id.to_string()]})),
        )
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(started_evs.as_array().unwrap().len(), 1);

    // Applet without StartGrant still lets a visitor see their automation-started run only.
    let applet_id = Ulid::from_bytes([77; 16]);
    let applet = Applet {
        applet_id,
        group_id: h.group,
        name: "intake".into(),
        slug: "intake".into(),
        host: "intake.apps.local".into(),
        repository: None,
        host_node: h.node,
        lifecycle: AppLife::Live,
        access: Access::Public,
        live: Some(1),
        versions: Vec::new(),
        drafts: Vec::new(),
        grants: Grants {
            submissions: Some(Folder {
                bucket: "uploads".into(),
                prefix: "intake/".into(),
            }),
            ..Grants::default()
        },
        embedding: Embedding::default(),
        guests: None,
        domains: Vec::new(),
        pending: None,
        account: None,
        role_id: None,
        visitor_salt: [1; 32],
        created_by: h.owner.user_id,
        created_at_ms: 1,
        revision: 1,
    };
    let vis_run = Ulid::from_bytes([78; 16]);
    let rec = WorkflowRunRecord {
        run_id: vis_run,
        realm_id: h.realm,
        group_id: h.group,
        workflow_id: Ulid::from_bytes([79; 16]),
        workflow_name: "wf".into(),
        version: 1,
        lineage: vis_run,
        state: RunState::Succeeded,
        node_id: h.node,
        created_by: h.owner.user_id,
        created_at_ms: 10,
        finished_at_ms: Some(15),
        error: None,
        values_json: "{}".into(),
        inputs_json: None,
        output: Some(RunOutput {
            bucket: "uploads".into(),
            prefix: "intake/vis-1/runs".into(),
            node_id: Some(h.node),
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
        cache_scope: CacheScope::Run,
        placement_site: None,
        protected: false,
        started_at_ms: None,
        secrets: BTreeMap::new(),
        path_restrictions: None,
        captured_remote: Vec::new(),
        directory_pins: Vec::new(),
        directory_roots: Vec::new(),
        used_tools: Vec::new(),
        limits: None,
        automation: Some(AutomationOrigin {
            automation_id: auto_ulid,
            firing_id: Ulid::from_string(&fid).unwrap(),
            stage: "run".into(),
            applet_id: Some(applet_id),
            depth: 0,
        }),
    };
    write_doc(
        &h.state.get_ctx(),
        WORKFLOW_RUN_KEYSPACE,
        run_key(vis_run),
        rec.to_bytes().unwrap().into(),
    )
    .await;
    let vis_a = Visit {
        pseudonym: "vis-1".into(),
        guest: true,
    };
    let vis_b = Visit {
        pseudonym: "vis-2".into(),
        guest: true,
    };
    let seen = drive(
        VisitorRunsOperation::new(&applet, vis_a, 10),
        &h.state.get_ctx(),
    )
    .await
    .unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].run_id, vis_run);
    let other = drive(
        VisitorRunsOperation::new(&applet, vis_b, 10),
        &h.state.get_ctx(),
    )
    .await
    .unwrap();
    assert!(other.is_empty());
}

#[tokio::test]
async fn mcp_tools_safe() {
    let tools = mcp_auto::toolset().list_all();
    let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
    assert_eq!(names.len(), 6, "{names:?}");
    for name in &names {
        assert!(!name.contains("approve"), "agent must not approve: {name}");
        assert!(!name.contains("reject"), "agent must not reject: {name}");
    }
    let h = Harness::new().await;
    h.set_features(true, true).await;
    let mcp = McpServer::new(h.state.clone());
    let created = mcp
        .automation_create(
            McpExt(caller(&h.owner)),
            Parameters(mcp_auto::CreateInput {
                group_id: h.group.to_string(),
                name: "mcp-auto".into(),
                description: "via mcp".into(),
                spec: JsonPayload(h.base_spec()),
            }),
        )
        .await
        .unwrap()
        .0
        .0;
    let id = created["automation_id"].as_str().unwrap().to_string();
    let got = mcp
        .automation_get(
            McpExt(caller(&h.owner)),
            Parameters(mcp_auto::AutoIdInput {
                automation_id: id.clone(),
            }),
        )
        .await
        .unwrap()
        .0
        .0;
    assert_eq!(got["name"], "mcp-auto");
    let checked = mcp
        .automation_check(
            McpExt(caller(&h.owner)),
            Parameters(mcp_auto::CheckInput {
                automation_id: id.clone(),
                sample_values: None,
            }),
        )
        .await
        .unwrap()
        .0
        .0;
    assert_eq!(checked["ok"], true);
    let requested = mcp
        .automation_request_approval(
            McpExt(caller(&h.owner)),
            Parameters(mcp_auto::RevisionInput {
                automation_id: id,
                expected_revision: Some(1),
            }),
        )
        .await
        .unwrap()
        .0
        .0;
    assert_eq!(requested["lifecycle"], "waiting_for_approval");
}
