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
        effect.unavailable()
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
        let state = Arc::new(test_state(ctx, realm, node, caps).await);
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
        let (router, _) = super::router().split_for_parts();
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
