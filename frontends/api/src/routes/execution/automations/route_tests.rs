//! HTTP route tests for automation feature switches, CRUD, diff, check, graph and lifecycle.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use aruna_compute::registry::ExecutorRegistry;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities, Permission, Role};
use aruna_core::structs::identity::features::{Feature, FeatureMode};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_core::user::id::UserId;
use aruna_core::workflow::{WorkflowAnswer, WorkflowEffect, WorkflowHost};
use aruna_workflow_contract::preflight::{GraphNode, NodeKind, WorkflowGraph};
use aruna_workflow_contract::{ContractError, PreflightKind, PreflightResult};
use axum::Extension;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::{Value, json};
use tower::ServiceExt;
use ulid::Ulid;

use crate::auth::ValidatedBearer;
use crate::server::state::ServerState;
use crate::tests::routes::{seed_realm_auth, test_state, write_doc};

struct IdleHost;

#[async_trait::async_trait]
impl WorkflowHost for IdleHost {
    async fn serve(&self, effect: WorkflowEffect) -> WorkflowAnswer {
        match effect {
            WorkflowEffect::Preflight(PreflightKind::Check { .. }) => {
                WorkflowAnswer::Preflight(Ok(PreflightResult::Check {
                    runnable: true,
                    report_json: json!({"language": "wir", "parts": [], "problems": []}),
                    images: Vec::new(),
                }))
            }
            WorkflowEffect::Preflight(PreflightKind::Inspect { .. }) => {
                WorkflowAnswer::Preflight(Ok(PreflightResult::Inspect {
                    form_json: json!({"schema": {"type": "object"}}),
                    graph: WorkflowGraph {
                        nodes: vec![GraphNode {
                            id: "step".into(),
                            label: "step".into(),
                            kind: NodeKind::Step,
                            tool: None,
                            version: None,
                        }],
                        edges: Vec::new(),
                    },
                }))
            }
            WorkflowEffect::Preflight(other) => {
                WorkflowAnswer::Preflight(Err(aruna_core::workflow::WorkflowError::Refused(
                    ContractError::unsupported(other.name()),
                )))
            }
            other => other.unavailable(),
        }
    }
}

pub(super) struct Harness {
    pub(super) state: Arc<ServerState>,
    pub(super) realm: RealmId,
    pub(super) node: aruna_core::id::NodeId,
    pub(super) group: Ulid,
    pub(super) owner: AuthContext,
    pub(super) member: AuthContext,
    pub(super) stranger: AuthContext,
}

impl Harness {
    pub(super) async fn new() -> Self {
        let storage = aruna_storage::storage::memory::MemoryStorage::open();
        let mut raw = aruna_runner::testing::test_context(storage);
        raw.compute_handle = Some(Arc::new(
            ExecutorRegistry::new().with_workflow(Arc::new(IdleHost)),
        ));
        let ctx = Arc::new(raw);
        let realm = RealmId([1; 32]);
        let node = iroh::SecretKey::from_bytes(&[8; 32]).public();
        let group = Ulid::from_bytes([31; 16]);
        let user = |s: u8| AuthContext::new(UserId::local(Ulid::from_bytes([s; 16]), realm), realm);
        let (owner, member, stranger) = (user(1), user(2), user(3));
        let act = Actor {
            node_id: node,
            user_id: owner.user_id,
            realm_id: realm,
        };
        seed_realm_auth(&ctx, realm, &act).await;
        let mut gauth = GroupAuthorizationDocument::default_group_doc(owner.user_id, realm, group);
        let rid = Ulid::from_bytes([45; 16]);
        let path = format!("{}**", group_permission_path(realm, group, node));
        gauth.roles.insert(
            rid,
            Role {
                role_id: rid,
                name: "writer".into(),
                permissions: HashMap::from([(path, Permission::WRITE)]),
                assigned_users: HashSet::from([member.user_id]),
            },
        );
        let grp = Group {
            display_name: "lab".into(),
            group_id: group,
            realm_id: realm,
            roles: gauth.roles.keys().copied().collect(),
            owner: owner.user_id,
        };
        write_doc(
            &ctx,
            AUTH_KEYSPACE,
            group.to_bytes().into(),
            gauth.to_bytes(&act).unwrap().into(),
        )
        .await;
        write_doc(
            &ctx,
            GROUP_KEYSPACE,
            group.to_bytes().into(),
            grp.to_bytes(&act).unwrap().into(),
        )
        .await;
        let caps = NodeCapabilities::user_node(realm).unwrap();
        let hosting = aruna_core::applet::settings::HostingSettings {
            applet_domain: "apps.local".into(),
            edge_key_sha256: [1; 32],
            custom_domains: false,
            guests: false,
            embedding: false,
            max_origins: 4,
            portal_origin: None,
            protected_hosts: Vec::new(),
        };
        let state = Arc::new(test_state(ctx, realm, node, caps).await.with_applets(
            aruna_core::applet::settings::AppletSettings {
                hosting: Some(hosting),
                building: None,
            },
        ));
        let h = Self {
            state,
            realm,
            node,
            group,
            owner,
            member,
            stranger,
        };
        h.set_features(false, false).await;
        h
    }

    pub(super) async fn set_features(&self, autos: bool, applets: bool) {
        let act = Actor {
            node_id: self.node,
            user_id: self.owner.user_id,
            realm_id: self.realm,
        };
        let mut cfg = RealmConfigDocument::default_for_realm(self.realm, Vec::new());
        let mode = |on| FeatureMode::Scoped {
            default: on,
            groups: BTreeMap::new(),
        };
        cfg.features.insert(Feature::Automations, mode(autos));
        cfg.features.insert(Feature::Applets, mode(applets));
        cfg.features.insert(Feature::Workflows, mode(true));
        write_doc(
            &self.state.get_ctx(),
            REALM_CONFIG_KEYSPACE,
            (*self.realm.as_bytes()).into(),
            cfg.to_bytes(&act).unwrap().into(),
        )
        .await;
    }

    pub(super) async fn call(
        &self,
        auth: &AuthContext,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let (router, _) = super::router()
            .merge(crate::routes::applets::router())
            .merge(crate::routes::execution::workflow::router())
            .split_for_parts();
        let body = body.map_or_else(Body::empty, |b| Body::from(b.to_string()));
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .unwrap();
        let router = router
            .layer(Extension(Some(auth.clone())))
            .layer(Extension(Option::<ValidatedBearer>::None))
            .with_state(self.state.clone());
        let resp = router.oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let val = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, val)
    }

    pub(super) fn base_spec(&self) -> Value {
        let base = format!("/{}/g/{}/data/{}/raw/**", self.realm, self.group, self.node);
        json!({
            "triggers": [
                {"kind": "manual"},
                {"kind": "data_uploaded", "bucket": "raw", "prefix": "in/"}
            ],
            "stages": [
                {"name": "gate", "needs": [], "kind": "approval", "message": "ok?"},
                {"name": "deliver", "needs": ["gate"], "kind": "copy", "from_stage": "gate", "dest_bucket": "raw", "dest_template": "in/{date}/{filename}"},
                {"name": "alert", "needs": ["deliver"], "kind": "notify", "audience": "group_admins", "message": "done"}
            ],
            "scope": {
                "read": [base],
                "write": [base],
                "audiences": ["group_admins"]
            }
        })
    }
}

#[tokio::test]
async fn routes_and_switches() {
    let h = Harness::new().await;
    let create_body = json!({
        "group_id": h.group.to_string(),
        "name": "ingest",
        "description": "test",
        "spec": h.base_spec()
    });
    // Feature::Automations defaults off and refuses creation with 403.
    let (status, _) = h
        .call(
            &h.owner,
            "POST",
            "/compute/automations",
            Some(create_body.clone()),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Enable automations only: applet_submission still refused while applets is off.
    h.set_features(true, false).await;
    let app_spec = json!({
        "group_id": h.group.to_string(),
        "name": "app-auto",
        "spec": {
            "triggers": [{"kind": "applet_submission", "applet_id": Ulid::from_bytes([5; 16]).to_string()}],
            "stages": [{"name": "a", "kind": "notify", "audience": "group_admins", "message": "hi"}],
            "scope": {"audiences": ["group_admins"]}
        }
    });
    let (status, _) = h
        .call(&h.owner, "POST", "/compute/automations", Some(app_spec))
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, created) = h
        .call(&h.owner, "POST", "/compute/automations", Some(create_body))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = created["automation_id"].as_str().unwrap().to_string();
    assert_eq!(created["lifecycle"], "draft");
    let (status, page) = h
        .call(
            &h.owner,
            "GET",
            &format!("/compute/automations?group_id={}", h.group),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    let (status, _) = h
        .call(
            &h.stranger,
            "GET",
            &format!("/compute/automations/{id}"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Graph, diff (loop warning on raw write overlap), and dry-run check.
    let (status, graph) = h
        .call(
            &h.owner,
            "GET",
            &format!("/compute/automations/{id}/graph"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(graph["complete"], true);
    let (status, diff) = h
        .call(
            &h.owner,
            "GET",
            &format!("/compute/automations/{id}/diff"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(!diff["loop_warnings"].as_array().unwrap().is_empty());
    let (status, check) = h
        .call(
            &h.owner,
            "POST",
            &format!("/compute/automations/{id}/check"),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(check["ok"], true);

    // Lifecycle transitions: request approval, reject non-admin, approve as admin, pause/resume.
    let (status, wait) = h
        .call(
            &h.member,
            "POST",
            &format!("/compute/automations/{id}/request-approval"),
            Some(json!({"expected_revision": 1})),
        )
        .await;
    assert_eq!(
        (status, wait["lifecycle"].as_str()),
        (StatusCode::OK, Some("waiting_for_approval"))
    );
    let (status, _) = h
        .call(
            &h.member,
            "POST",
            &format!("/compute/automations/{id}/approve"),
            Some(json!({"expected_revision": 2})),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, active) = h
        .call(
            &h.owner,
            "POST",
            &format!("/compute/automations/{id}/approve"),
            Some(json!({"expected_revision": 2})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{active}");
    assert_eq!(active["lifecycle"], "active");
    assert!(active["account"].is_string());

    let (status, _) = h
        .call(
            &h.owner,
            "PATCH",
            &format!("/compute/automations/{id}"),
            Some(json!({"expected_revision": 1, "name": "stale"})),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, paused) = h
        .call(
            &h.owner,
            "POST",
            &format!("/compute/automations/{id}/pause"),
            Some(json!({})),
        )
        .await;
    assert_eq!(
        (status, paused["lifecycle"].as_str()),
        (StatusCode::OK, Some("paused"))
    );
    let (status, resumed) = h
        .call(
            &h.owner,
            "POST",
            &format!("/compute/automations/{id}/resume"),
            Some(json!({})),
        )
        .await;
    assert_eq!(
        (status, resumed["lifecycle"].as_str()),
        (StatusCode::OK, Some("active"))
    );
}

#[tokio::test]
async fn triggers_and_save() {
    use aruna_core::applet::record::{Access, Applet, Embedding, Grants, Lifecycle, applet_key};
    use aruna_core::keyspaces::APPLETS_KEYSPACE;

    let h = Harness::new().await;
    h.set_features(true, true).await;

    // Missing zone on weekdays, time_of_day or schedule is rejected with 400.
    for bad_spec in [
        json!({
            "triggers": [{"kind": "manual"}],
            "conditions": {"mode": "all", "items": [{"kind": "weekdays", "days": ["mon"]}]},
            "stages": [{"name": "a", "kind": "notify", "audience": "group_admins", "message": "hi"}],
            "scope": {"audiences": ["group_admins"]}
        }),
        json!({
            "triggers": [{"kind": "manual"}],
            "conditions": {"mode": "all", "items": [{"kind": "time_of_day", "from": "08:00", "to": "18:00"}]},
            "stages": [{"name": "a", "kind": "notify", "audience": "group_admins", "message": "hi"}],
            "scope": {"audiences": ["group_admins"]}
        }),
        json!({
            "triggers": [{"kind": "schedule", "every": "day", "at": "06:00"}],
            "stages": [{"name": "a", "kind": "notify", "audience": "group_admins", "message": "hi"}],
            "scope": {"audiences": ["group_admins"]}
        }),
    ] {
        let (status, err) = h
            .call(
                &h.owner,
                "POST",
                "/compute/automations",
                Some(json!({"group_id": h.group.to_string(), "name": "bad", "spec": bad_spec})),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
    }

    // Explicit limits remain null when omitted while effective defaults are populated.
    let now_hour = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        / 3600)
        % 24;
    let from = format!("{:02}:00", (now_hour + 12) % 24);
    let to = format!("{:02}:00", (now_hour + 13) % 24);
    let cond_spec = json!({
        "triggers": [{"kind": "manual"}],
        "conditions": {
            "mode": "all",
            "items": [{"kind": "time_of_day", "from": from, "to": to, "zone": "UTC"}]
        },
        "stages": [{"name": "a", "kind": "notify", "audience": "group_admins", "message": "hi"}],
        "scope": {"audiences": ["group_admins"], "limits": {"per_hour": 10}}
    });
    let (status, created) = h
        .call(
            &h.owner,
            "POST",
            "/compute/automations",
            Some(json!({"group_id": h.group.to_string(), "name": "guarded", "spec": cond_spec})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let auto_id = created["automation_id"].as_str().unwrap().to_string();
    assert_eq!(created["spec"]["scope"]["limits"]["per_hour"], 10);
    assert!(created["spec"]["scope"]["limits"]["per_day"].is_null());
    assert_eq!(
        created["spec"]["scope"]["limits"]["effective"]["per_day"],
        200
    );

    // PATCH round-trip with returned spec keeps unset limits null.
    let (status, patched) = h
        .call(
            &h.owner,
            "PATCH",
            &format!("/compute/automations/{auto_id}"),
            Some(json!({"expected_revision": 1, "spec": created["spec"]})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert!(patched["spec"]["scope"]["limits"]["per_day"].is_null());

    // Approve and manually start -> 409 conditions_not_met with persisted skipped firing.
    let (status, _) = h
        .call(
            &h.owner,
            "POST",
            &format!("/compute/automations/{auto_id}/approve"),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let (status, refused) = h
        .call(
            &h.owner,
            "POST",
            &format!("/compute/automations/{auto_id}/start"),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["error"], "conditions_not_met");
    assert_eq!(refused["code"], "conditions_not_met");
    assert_eq!(refused["firing"]["state"], "skipped");
    assert_eq!(refused["firing"]["trigger_event"]["kind"], "manual");
    assert_eq!(
        refused["firing"]["conditions_checked"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let (status, skipped_runs) = h
        .call(
            &h.owner,
            "GET",
            &format!(
                "/compute/automation-runs?group_id={}&state=skipped",
                h.group
            ),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{skipped_runs}");
    assert_eq!(skipped_runs["items"].as_array().unwrap().len(), 1);

    // Unified single-transaction save creates linked workflow + automation and supports filter.
    let save_body = json!({
        "group_id": h.group.to_string(),
        "workflow": {
            "name": "studio-flow",
            "language": "wir",
            "source": {"wir": {"wir_version": "1"}}
        },
        "automation": {
            "name": "studio-auto",
            "spec": {
                "triggers": [{"kind": "manual"}],
                "stages": [{
                    "name": "run",
                    "kind": "run",
                    "workflow_id": Ulid::nil().to_string(),
                    "version": 0,
                    "values_json": "{}"
                }],
                "scope": {"workflows": [{"workflow_id": Ulid::nil().to_string(), "version": 0}]}
            }
        }
    });
    let (status, saved) = h
        .call(
            &h.owner,
            "POST",
            "/compute/automations/save",
            Some(save_body),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{saved}");
    let workflow_id = saved["workflow"]["workflow_id"]
        .as_str()
        .unwrap()
        .to_string();
    let paired_auto = saved["automation"]["automation_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(saved["workflow"]["automation_id"], paired_auto);
    assert_eq!(saved["automation"]["workflow_id"], workflow_id);
    assert_eq!(saved["version"]["version"], "1");

    let (status, by_workflow) = h
        .call(
            &h.owner,
            "GET",
            &format!(
                "/compute/automations?group_id={}&workflow_id={workflow_id}",
                h.group
            ),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{by_workflow}");
    assert_eq!(by_workflow["items"].as_array().unwrap().len(), 1);
    assert_eq!(by_workflow["items"][0]["automation_id"], paired_auto);

    // PUT /applets/{applet_id}/automations links same-group automations through the domain op.
    let applet_id = Ulid::from_bytes([66; 16]);
    let applet = Applet {
        applet_id,
        group_id: h.group,
        name: "Form".into(),
        slug: "form".into(),
        host: "form.apps.local".into(),
        repository: None,
        host_node: h.node,
        lifecycle: Lifecycle::Live,
        access: Access::Private,
        live: Some(1),
        versions: Vec::new(),
        drafts: Vec::new(),
        grants: Grants::default(),
        embedding: Embedding::default(),
        guests: None,
        domains: Vec::new(),
        pending: None,
        account: None,
        role_id: None,
        visitor_salt: [2; 32],
        created_by: h.owner.user_id,
        created_at_ms: 1,
        revision: 1,
        automations: Vec::new(),
    };
    write_doc(
        &h.state.get_ctx(),
        APPLETS_KEYSPACE,
        applet_key(applet_id),
        applet.to_bytes().unwrap().into(),
    )
    .await;
    let (status, linked) = h
        .call(
            &h.owner,
            "PUT",
            &format!("/applets/{applet_id}/automations"),
            Some(json!({"automation_ids": [paired_auto], "expected_revision": 1})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{linked}");
    assert_eq!(linked["automation_ids"], json!([paired_auto]));

    let (status, _) = h
        .call(
            &h.owner,
            "PUT",
            &format!("/applets/{applet_id}/automations"),
            Some(json!({"automation_ids": [Ulid::from_bytes([99; 16]).to_string()], "expected_revision": 2})),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
