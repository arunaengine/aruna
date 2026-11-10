//! Tests prepared installs: activation, resumption after each step, refusals and later changes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::groups::create_group::{CreateGroupConfig, CreateGroupOperation};
use crate::plugins::account::{read_auth, read_user};
use crate::plugins::lifecycle::{
    pause_install, remove_install, resume_install, set_grants, update_install,
};
use crate::plugins::publish::{PublishEntryConfig, PublishEntryOperation};
use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use crate::users::admin_audit::{AuditPageInput, AuditPageOperation};
use crate::users::service_account::create::{CreateServiceConfig, CreateServiceOperation};
use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::structs::identity::admin_audit::AuditIndex;
use aruna_core::structs::identity::auth::Role;
use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
use aruna_core::structs::identity::user::User;
use aruna_tasks::TaskHandle;
use serde_json::json;
use tempfile::{TempDir, tempdir};

struct Fixture {
    _dir: TempDir,
    ctx: DriverContext,
    actor: Actor,
    group_id: GroupId,
}

impl Fixture {
    async fn new() -> Self {
        let dir = tempdir().unwrap();
        let ctx = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        };
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            user_id: UserId::local(Ulid::from_parts(1, 1), realm_id),
            realm_id,
        };
        let realm = CreateRealmOperation::new(CreateRealmConfig {
            actor: actor.clone(),
            realm_description: "Test realm".to_string(),
            oidc_providers: Vec::new(),
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        });
        drive(realm, &ctx).await.unwrap();
        // The actor publishes test versions, which needs the realm admin role.
        let mut realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
        for role in realm_auth.roles.values_mut() {
            role.assigned_users.insert(actor.user_id);
        }
        let write = StorageEffect::Write {
            key_space: AUTH_KEYSPACE.to_string(),
            key: (*realm_id.as_bytes()).into(),
            value: realm_auth.to_bytes(&actor).unwrap().into(),
            txn_id: None,
        };
        ctx.storage_handle.send_storage_effect(write).await;
        let group = CreateGroupOperation::new(CreateGroupConfig {
            actor: actor.clone(),
            display_name: "Lab A".to_string(),
            owner_cap: None,
        });
        let (group, _) = drive(group, &ctx).await.unwrap();
        let fixture = Self {
            _dir: dir,
            ctx,
            actor,
            group_id: group.group_id,
        };
        fixture.publish("0.1.0", "ab", json!({})).await;
        fixture
    }

    fn auth(&self) -> AuthContext {
        AuthContext {
            user_id: self.actor.user_id,
            realm_id: self.actor.realm_id,
            path_restrictions: None,
            session: None,
        }
    }

    /// Publishes a version whose manifest asks for metadata access and one connection.
    async fn publish(&self, version: &str, byte: &str, extra: Value) -> String {
        let mut manifest = json!({
            "name": "announcer",
            "version": version,
            "settings": { "catalog_url": { "title": "Catalog", "type": "url" } },
            "connections": { "catalog": { "url": "{catalog_url}/api/" } },
            "permissions": { "required": ["metadata.read"], "optional": ["metadata.write"] }
        });
        if let (Some(base), Some(extra)) = (manifest.as_object_mut(), extra.as_object()) {
            base.extend(extra.clone());
        }
        let digest = format!("sha256:{}", byte.repeat(32));
        let plugin = PublishedPlugin {
            name: "announcer".into(),
            version: version.into(),
            digest: digest.clone(),
            size: 10,
            manifest: manifest.to_string(),
            published_by: self.actor.user_id,
            published_at_ms: version.len() as u64,
        };
        let write = PublishEntryOperation::new(PublishEntryConfig {
            actor: self.actor.clone(),
            auth_context: self.auth(),
            plugin,
        });
        drive(write, &self.ctx).await.unwrap();
        digest
    }

    fn request(&self, install_id: Option<Ulid>) -> InstallRequest {
        let settings = json!({ "catalog_url": "https://catalog.example.org" });
        InstallRequest {
            plugin: "announcer".into(),
            group_id: self.group_id,
            version: None,
            settings: settings.as_object().cloned().unwrap_or_default(),
            grants: BTreeMap::new(),
            install_id,
        }
    }

    async fn install(&self, request: &InstallRequest) -> Result<PluginInstall, InstallError> {
        install_plugin(&self.ctx, &self.actor, &self.auth(), request).await
    }

    async fn installs(&self) -> Vec<PluginInstall> {
        read_config(&self.ctx, self.actor.realm_id)
            .await
            .unwrap()
            .installs
    }

    async fn account(&self, install_id: Ulid) -> Option<User> {
        let account = UserId::local(install_id, self.actor.realm_id);
        read_user(&self.ctx, account).await.unwrap()
    }

    async fn role(&self, install_id: Ulid) -> Option<Role> {
        let auth_doc = read_auth(&self.ctx, self.group_id).await.unwrap().unwrap();
        auth_doc.roles.get(&install_id).cloned()
    }
}

#[tokio::test]
async fn installs_and_resumes() {
    let fixture = Fixture::new().await;
    let (auth, first) = (fixture.auth(), fixture.request(None));
    let preview = prepare_install(&fixture.ctx, &auth, fixture.actor.realm_id, &first).await;
    let id = preview.unwrap().scope.install_id;
    assert!(
        fixture.installs().await.is_empty(),
        "a preview writes nothing"
    );

    let request = fixture.request(Some(id));
    let installed = fixture.install(&request).await.unwrap();
    assert_eq!(
        (installed.state, installed.revision),
        (InstallState::Active, 2)
    );
    assert_eq!(installed.prerequisites.len(), 2);
    assert_eq!(fixture.installs().await, vec![installed.clone()]);
    let account = fixture.account(id).await.unwrap();
    assert_eq!(account.service_group(), Some(fixture.group_id));
    assert_eq!(account.install(), Some(id));
    assert!(!account.is_deactivated());
    assert!(account.name.starts_with("plugin announcer (Lab A)"));
    let role = fixture.role(id).await.unwrap();
    assert_eq!(role.name, "plugin-write");
    assert!(role.assigned_users.contains(&installed.account));

    assert_eq!(
        fixture.install(&request).await.unwrap(),
        installed,
        "a retry answers the same"
    );
    let mut other = request.clone();
    other.grants.insert("metadata.write".into(), Vec::new());
    assert!(matches!(
        fixture.install(&other).await,
        Err(InstallError::Changed)
    ));
}

#[tokio::test]
async fn resumes_after_steps() {
    for stop in 1..=4 {
        let fixture = Fixture::new().await;
        let (ctx, actor, auth) = (&fixture.ctx, &fixture.actor, fixture.auth());
        let request = fixture.request(Some(Ulid::from_parts(9, stop)));
        let prepared = prepare_install(ctx, &auth, actor.realm_id, &request)
            .await
            .unwrap();
        let record = new_record(actor, &prepared);
        let (scope, account) = (prepared.scope, record.account);
        write_record(ctx, actor, &auth, None, record, None)
            .await
            .unwrap();
        if stop >= 2 {
            ensure_account(ctx, actor, &auth, &scope, account, "x".into())
                .await
                .unwrap();
        }
        if stop >= 3 {
            let paths = role_paths(&prepared.grants);
            ensure_role(ctx, actor, &auth, &scope, account, &paths)
                .await
                .unwrap();
        }
        if stop == 4 {
            teardown(ctx, actor, &auth, &scope).await.unwrap();
            assert!(
                fixture
                    .account(scope.install_id)
                    .await
                    .unwrap()
                    .is_deactivated()
            );
            assert!(fixture.role(scope.install_id).await.is_none());
        }
        assert_eq!(fixture.installs().await[0].state, InstallState::Preparing);
        let installed = fixture.install(&request).await.unwrap();
        assert_eq!(
            installed.state,
            InstallState::Active,
            "stop after step {stop}"
        );
        assert!(
            !fixture
                .account(scope.install_id)
                .await
                .unwrap()
                .is_deactivated()
        );
        assert!(fixture.role(scope.install_id).await.is_some());
    }
}

#[tokio::test]
async fn refuses_requests() {
    let fixture = Fixture::new().await;
    let mut outsider = fixture.auth();
    outsider.user_id = UserId::local(Ulid::from_parts(4, 4), fixture.actor.realm_id);
    let realm_id = fixture.actor.realm_id;
    let refused = prepare_install(&fixture.ctx, &outsider, realm_id, &fixture.request(None)).await;
    assert!(
        matches!(refused, Err(InstallError::Unauthorized)),
        "{refused:?}"
    );

    let mut unknown = fixture.request(None);
    unknown.plugin = "other".into();
    assert!(matches!(
        fixture.install(&unknown).await,
        Err(InstallError::NotPublished)
    ));
    let mut outside = fixture.request(None);
    let other_group = format!("/{realm_id}/g/{}/meta/**", Ulid::from_parts(5, 5));
    outside
        .grants
        .insert("metadata.read".into(), vec![other_group]);
    assert!(matches!(
        fixture.install(&outside).await,
        Err(InstallError::Grant(_))
    ));
    let mut settings = fixture.request(None);
    settings.settings.clear();
    assert!(matches!(
        fixture.install(&settings).await,
        Err(InstallError::Setting(_))
    ));

    let secret = fixture
        .publish(
            "0.1.1",
            "cd",
            json!({ "secrets": { "key": { "title": "Key" } } }),
        )
        .await;
    let mut secrets = fixture.request(None);
    secrets.version = Some(secret);
    assert!(matches!(
        fixture.install(&secrets).await,
        Err(InstallError::Grant(_))
    ));
    assert!(fixture.installs().await.is_empty());
}

#[tokio::test]
async fn taken_account_aborts() {
    let fixture = Fixture::new().await;
    let id = Ulid::from_parts(9, 9);
    let taken = CreateServiceOperation::new(CreateServiceConfig {
        actor: fixture.actor.clone(),
        auth_context: fixture.auth(),
        group_id: fixture.group_id,
        name: "someone".into(),
        user_id: UserId::local(id, fixture.actor.realm_id),
        install: None,
    });
    drive(taken, &fixture.ctx).await.unwrap();
    let failed = fixture.install(&fixture.request(Some(id))).await;
    assert!(
        matches!(
            failed,
            Err(InstallError::Step {
                step: "account",
                ..
            })
        ),
        "{failed:?}"
    );
    let record = &fixture.installs().await[0];
    assert_eq!(
        (record.state, record.failed.as_deref()),
        (InstallState::Preparing, Some("account"))
    );
    assert!(
        !fixture.account(id).await.unwrap().is_deactivated(),
        "a foreign account stays"
    );
}

#[tokio::test]
async fn later_changes() {
    let fixture = Fixture::new().await;
    let (ctx, actor, auth) = (&fixture.ctx, &fixture.actor, fixture.auth());
    let installed = fixture.install(&fixture.request(None)).await.unwrap();
    let id = installed.install_id;
    let meta = format!("/{}/g/{}/meta", actor.realm_id, fixture.group_id);
    let choices = BTreeMap::from([(
        "metadata.write".to_string(),
        vec![format!("{meta}/runs/**")],
    )]);
    let narrowed = set_grants(ctx, actor, &auth, id, &choices).await.unwrap();
    assert_eq!(
        narrowed.grants["metadata.write"],
        vec![format!("{meta}/runs/**")]
    );
    let role = fixture.role(id).await.unwrap();
    assert_eq!(role.permissions[&format!("{meta}/runs/**")], Access::WRITE);
    assert_eq!(role.permissions[&format!("{meta}/**")], Access::READ);

    let paused = pause_install(ctx, actor, &auth, id).await.unwrap();
    assert_eq!(paused.state, InstallState::Paused);
    assert!(matches!(
        pause_install(ctx, actor, &auth, id).await,
        Err(InstallError::State(_))
    ));
    assert_eq!(
        resume_install(ctx, actor, &auth, id).await.unwrap().state,
        InstallState::Active
    );

    let needs = json!({ "permissions": { "required": ["metadata.read", "objects.read"], "optional": ["metadata.write"] } });
    let digest = fixture.publish("0.2.0", "ef", needs).await;
    let (updated, missing) = update_install(ctx, actor, &auth, id, &digest, None)
        .await
        .unwrap();
    assert_eq!(
        (updated.state, missing),
        (InstallState::Blocked, vec!["objects.read".to_string()])
    );
    assert_eq!(
        updated.grants["metadata.write"],
        vec![format!("{meta}/runs/**")]
    );
    let approved = set_grants(ctx, actor, &auth, id, &BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(approved.state, InstallState::Active);

    remove_install(ctx, actor, &auth, id).await.unwrap();
    assert!(fixture.installs().await.is_empty());
    assert!(fixture.account(id).await.unwrap().is_deactivated());
    assert!(fixture.role(id).await.is_none());
    let page = AuditPageOperation::new(AuditPageInput {
        index: AuditIndex::Group(fixture.group_id),
        start_after: None,
        limit: 50,
    });
    let actions: Vec<_> = (drive(page, ctx).await.unwrap().records.into_iter())
        .filter(|record| record.plugin.is_some())
        .map(|record| record.action)
        .collect();
    let expected = [
        AdminAuditAction::PluginInstalled,
        AdminAuditAction::PluginGrantsChanged,
        AdminAuditAction::PluginPaused,
        AdminAuditAction::PluginResumed,
        AdminAuditAction::PluginUpdated,
        AdminAuditAction::PluginGrantsChanged,
        AdminAuditAction::PluginRemoved,
    ];
    assert_eq!(actions.len(), expected.len(), "{actions:?}");
    assert!(expected.iter().all(|action| actions.contains(action)));
}
