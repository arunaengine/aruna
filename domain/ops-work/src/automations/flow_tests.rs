//! Tests automation CRUD, revision checks, admin approval, widening, matching and replay.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap, HashSet};

use aruna_core::automation::record::{
    Automation, Lifecycle, Spec, Stage, StageKind, Trigger, When,
};
use aruna_core::automation::scope::{Audience, Scope};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::plugin_runs::{SourceEvent, journal_entries};
use aruna_core::structs::execution::workflow_run::{
    AutomationOrigin, CacheScope, RunState, WorkflowRunRecord,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission, Role};
use aruna_core::structs::identity::features::{Feature, FeatureMode};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId,
};
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_core::types::GroupId;
use aruna_core::user::id::UserId;
use aruna_runner::driver::{DriverContext, drive};
use aruna_storage::storage::memory::MemoryStorage;
use byteview::ByteView;
use ulid::Ulid;

use super::change::{AutoChange, CreateAutoOperation, NewAuto};
use super::lifecycle::ChangeAutoOperation;
use super::reads::ListAutosOperation;
use super::{AutoAccessOperation, AutoError, Need, automation_auth};

pub(super) struct Env {
    pub(super) ctx: DriverContext,
    pub(super) realm: RealmId,
    pub(super) node: NodeId,
    pub(super) group: GroupId,
    pub(super) owner: AuthContext,
    pub(super) member: AuthContext,
    pub(super) stranger: AuthContext,
}

impl Env {
    pub(super) async fn new() -> Self {
        let ctx = aruna_runner::testing::test_context(MemoryStorage::open());
        let realm = RealmId([9; 32]);
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let group = Ulid::from_bytes([11; 16]);
        let user = |s: u8| AuthContext::new(UserId::local(Ulid::from_bytes([s; 16]), realm), realm);
        let env = Self {
            ctx,
            realm,
            node,
            group,
            owner: user(1),
            member: user(2),
            stranger: user(3),
        };
        env.seed_auth().await;
        env.set_features(true, true).await;
        env
    }

    pub(super) fn actor(&self) -> Actor {
        Actor {
            node_id: self.node,
            user_id: self.owner.user_id,
            realm_id: self.realm,
        }
    }

    pub(super) async fn write_row(
        &self,
        space: &str,
        key: impl Into<ByteView>,
        value: impl Into<ByteView>,
    ) {
        let res = self
            .ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            res,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    pub(super) async fn read_row(&self, space: &str, key: &[u8]) -> Option<ByteView> {
        let res = self
            .ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: space.to_string(),
                key: ByteView::from(key.to_vec()),
                txn_id: None,
            })
            .await;
        match res {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            other => panic!("unexpected read: {other:?}"),
        }
    }

    pub(super) async fn scan_rows(&self, space: &str) -> Vec<(ByteView, ByteView)> {
        let res = self
            .ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: space.to_string(),
                prefix: None,
                start: None,
                limit: 100,
                txn_id: None,
            })
            .await;
        match res {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values,
            other => panic!("unexpected iter: {other:?}"),
        }
    }

    async fn seed_auth(&self) {
        let act = self.actor();
        let rdoc = RealmAuthorizationDocument::default_realm_doc(self.realm)
            .to_bytes(&act)
            .unwrap();
        self.write_row(AUTH_KEYSPACE, self.realm.as_bytes().to_vec(), rdoc)
            .await;
        let mut gauth = GroupAuthorizationDocument::default_group_doc(
            self.owner.user_id,
            self.realm,
            self.group,
        );
        let rid = Ulid::from_bytes([44; 16]);
        let path = format!(
            "{}**",
            group_permission_path(self.realm, self.group, self.node)
        );
        gauth.roles.insert(
            rid,
            Role {
                role_id: rid,
                name: "writer".into(),
                permissions: HashMap::from([(path, Permission::WRITE)]),
                assigned_users: HashSet::from([self.member.user_id]),
            },
        );
        let grp = Group {
            display_name: "lab".into(),
            group_id: self.group,
            realm_id: self.realm,
            roles: gauth.roles.keys().copied().collect(),
            owner: self.owner.user_id,
        };
        self.write_row(
            AUTH_KEYSPACE,
            self.group.to_bytes().to_vec(),
            gauth.to_bytes(&act).unwrap(),
        )
        .await;
        self.write_row(
            GROUP_KEYSPACE,
            self.group.to_bytes().to_vec(),
            grp.to_bytes(&act).unwrap(),
        )
        .await;
    }

    pub(super) async fn set_features(&self, autos: bool, applets: bool) {
        let mut cfg = RealmConfigDocument::default_for_realm(self.realm, Vec::new());
        let mode = |on| FeatureMode::Scoped {
            default: on,
            groups: BTreeMap::new(),
        };
        cfg.features.insert(Feature::Automations, mode(autos));
        cfg.features.insert(Feature::Applets, mode(applets));
        cfg.features.insert(Feature::Workflows, mode(true));
        self.write_row(
            REALM_CONFIG_KEYSPACE,
            self.realm.as_bytes().to_vec(),
            cfg.to_bytes(&self.actor()).unwrap(),
        )
        .await;
    }

    pub(super) async fn journal(&self, event: &SourceEvent) {
        for (ks, k, v) in journal_entries(event).unwrap() {
            self.write_row(&ks, k, v).await;
        }
    }

    pub(super) fn base_spec(&self) -> Spec {
        Spec {
            triggers: vec![Trigger::Manual { form_json: None }],
            conditions: Default::default(),
            stages: vec![Stage {
                name: "alert".into(),
                needs: None,
                when: When::Passed,
                kind: StageKind::Notify {
                    audience: Audience::GroupAdmins,
                    message: "done".into(),
                },
            }],
            scope: Scope {
                audiences: vec![Audience::GroupAdmins],
                ..Scope::default()
            },
        }
    }

    pub(super) fn sample_run(
        &self,
        run_id: Ulid,
        wf_id: Ulid,
        state: RunState,
        origin: Option<AutomationOrigin>,
    ) -> WorkflowRunRecord {
        WorkflowRunRecord {
            run_id,
            realm_id: self.realm,
            group_id: self.group,
            workflow_id: wf_id,
            workflow_name: "wf".into(),
            version: 1,
            lineage: run_id,
            state,
            node_id: self.node,
            created_by: self.owner.user_id,
            created_at_ms: 10,
            finished_at_ms: None,
            error: None,
            values_json: "{}".into(),
            inputs_json: None,
            output: None,
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
            automation: origin,
            applet: None,
        }
    }

    pub(super) async fn create_active(&self, spec: Spec) -> Automation {
        let created = drive(
            CreateAutoOperation::new(
                &self.owner,
                self.node,
                NewAuto {
                    group_id: self.group,
                    name: "ingest".into(),
                    description: "test".into(),
                    spec,
                    workflow_id: None,
                },
            ),
            &self.ctx,
        )
        .await
        .unwrap();
        let req = ChangeAutoOperation::new(
            &self.owner,
            (created.automation_id, Some(1)),
            AutoChange::RequestApproval,
            self.node,
        );
        drive(req, &self.ctx).await.unwrap();
        let approve = ChangeAutoOperation::new(
            &self.owner,
            (created.automation_id, Some(2)),
            AutoChange::Approve,
            self.node,
        );
        drive(approve, &self.ctx).await.unwrap()
    }
}

#[tokio::test]
async fn crud_and_widening() {
    let env = Env::new().await;
    let created = drive(
        CreateAutoOperation::new(
            &env.owner,
            env.node,
            NewAuto {
                group_id: env.group,
                name: "qc".into(),
                description: "checks".into(),
                spec: env.base_spec(),
                workflow_id: None,
            },
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!((created.lifecycle, created.revision), (Lifecycle::Draft, 1));
    let list = drive(
        ListAutosOperation::new(&env.owner, (env.group, env.node), None, None, 10),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(list.items.len(), 1);
    let denied = drive(
        AutoAccessOperation::new(&env.stranger, created.automation_id, env.node, Need::Read),
        &env.ctx,
    )
    .await;
    assert_eq!(denied, Err(AutoError::NotFound));

    // Request approval, refuse non-admin writer, then approve as human admin.
    let id = created.automation_id;
    let wait = drive(
        ChangeAutoOperation::new(
            &env.member,
            (id, Some(1)),
            AutoChange::RequestApproval,
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(wait.lifecycle, Lifecycle::WaitingForApproval);
    assert_eq!(
        drive(
            ChangeAutoOperation::new(&env.member, (id, Some(2)), AutoChange::Approve, env.node),
            &env.ctx,
        )
        .await,
        Err(AutoError::Forbidden)
    );
    let active = drive(
        ChangeAutoOperation::new(&env.owner, (id, Some(2)), AutoChange::Approve, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(active.lifecycle, Lifecycle::Active);
    assert!(active.account.is_some() && active.role_id.is_some());

    // Stale revision conflicts; narrowing applies live; widening waits in pending.
    let stale = AutoChange::Update {
        name: Some("x".into()),
        description: None,
        spec: None,
        workflow_id: None,
    };
    assert!(matches!(
        drive(
            ChangeAutoOperation::new(&env.owner, (id, Some(1)), stale, env.node),
            &env.ctx,
        )
        .await,
        Err(AutoError::Conflict(_))
    ));
    let mut narrower = active.spec.clone();
    narrower.scope.limits.per_hour = Some(5);
    let narrowed = drive(
        ChangeAutoOperation::new(
            &env.owner,
            (id, Some(active.revision)),
            AutoChange::Update {
                name: None,
                description: None,
                spec: Some(narrower),
                workflow_id: None,
            },
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(narrowed.pending.is_none());
    assert_eq!(narrowed.spec.scope.limits.per_hour, Some(5));
    let mut wider = narrowed.spec.clone();
    wider.scope.limits.per_hour = Some(500);
    let staged = drive(
        ChangeAutoOperation::new(
            &env.owner,
            (id, Some(narrowed.revision)),
            AutoChange::Update {
                name: None,
                description: None,
                spec: Some(wider),
                workflow_id: None,
            },
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(staged.pending.is_some());
    let rej = drive(
        ChangeAutoOperation::new(
            &env.owner,
            (id, Some(staged.revision)),
            AutoChange::Reject,
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(rej.pending.is_none());
    let del = drive(
        ChangeAutoOperation::new(&env.owner, (id, None), AutoChange::Delete, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(del.lifecycle, Lifecycle::Removed);
    assert!(del.role_id.is_none());
    assert!(automation_auth(&del, env.realm).is_none());
}

#[tokio::test]
async fn paused_widening_staged() {
    use aruna_core::automation::record::PauseReason;
    let env = Env::new().await;
    let active = env.create_active(env.base_spec()).await;
    let id = active.automation_id;
    let paused = drive(
        ChangeAutoOperation::new(
            &env.owner,
            (id, Some(active.revision)),
            AutoChange::Pause(PauseReason::Owner),
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(paused.lifecycle, Lifecycle::Paused);
    let orig_per_hour = paused.spec.scope.limits.hour_limit();

    // Widening edit while paused keeps approved spec intact and stages in pending.
    let mut wider = paused.spec.clone();
    wider.scope.limits.per_hour = Some(orig_per_hour + 100);
    let edited = drive(
        ChangeAutoOperation::new(
            &env.owner,
            (id, Some(paused.revision)),
            AutoChange::Update {
                name: None,
                description: None,
                spec: Some(wider.clone()),
                workflow_id: None,
            },
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(edited.spec.scope.limits.hour_limit(), orig_per_hour);
    assert_eq!(
        edited
            .pending
            .as_ref()
            .map(|p| p.spec.scope.limits.hour_limit()),
        Some(orig_per_hour + 100)
    );

    // Resuming without approval activates only the previously approved spec.
    let resumed = drive(
        ChangeAutoOperation::new(
            &env.owner,
            (id, Some(edited.revision)),
            AutoChange::Resume,
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(resumed.lifecycle, Lifecycle::Active);
    assert_eq!(resumed.spec.scope.limits.hour_limit(), orig_per_hour);
    assert!(resumed.pending.is_some());

    // Pause -> RequestApproval -> Reject returns to Paused with approved spec intact.
    let paused2 = drive(
        ChangeAutoOperation::new(
            &env.owner,
            (id, Some(resumed.revision)),
            AutoChange::Pause(PauseReason::Owner),
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    let waiting = drive(
        ChangeAutoOperation::new(
            &env.member,
            (id, Some(paused2.revision)),
            AutoChange::RequestApproval,
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(waiting.lifecycle, Lifecycle::WaitingForApproval);
    assert_eq!(waiting.spec.scope.limits.hour_limit(), orig_per_hour);
    let rejected = drive(
        ChangeAutoOperation::new(
            &env.owner,
            (id, Some(waiting.revision)),
            AutoChange::Reject,
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(rejected.lifecycle, Lifecycle::Paused);
    assert!(rejected.pending.is_none());
    assert_eq!(rejected.spec.scope.limits.hour_limit(), orig_per_hour);
}

#[tokio::test]
async fn delete_blocked_active() {
    use super::control::CancelFiringOperation;
    use super::start::StartManualOperation;

    let env = Env::new().await;
    let mut spec = env.base_spec();
    spec.scope.limits.max_active_firings = Some(1);
    let active = env.create_active(spec).await;
    let id = active.automation_id;

    let first = drive(
        StartManualOperation::new(&env.owner, id, env.node, "{}".into()),
        &env.ctx,
    )
    .await
    .unwrap();
    let second = drive(
        StartManualOperation::new(&env.owner, id, env.node, "{}".into()),
        &env.ctx,
    )
    .await;
    assert!(matches!(second, Err(AutoError::Limited(_))), "{second:?}");

    // Deletion is refused while an active firing exists.
    let blocked = drive(
        ChangeAutoOperation::new(&env.owner, (id, None), AutoChange::Delete, env.node),
        &env.ctx,
    )
    .await;
    assert!(
        matches!(blocked, Err(AutoError::Conflict(_))),
        "{blocked:?}"
    );

    // Cancelling the firing frees the active row so deletion succeeds.
    drive(
        CancelFiringOperation::new(&env.owner, first.firing_id, (env.node, env.realm)),
        &env.ctx,
    )
    .await
    .unwrap();
    let deleted = drive(
        ChangeAutoOperation::new(&env.owner, (id, None), AutoChange::Delete, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(deleted.lifecycle, Lifecycle::Removed);
}
