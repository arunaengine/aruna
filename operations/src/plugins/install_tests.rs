//! Tests prepared installs: activation, resuming after each step, refusals, held accounts,
//! foreign roles, grant checks against roles and policies, and overlapping retries.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::groups::add_role::{AddRoleConfig, AddRoleOperation};
use crate::groups::create_group::{CreateGroupConfig, CreateGroupOperation};
use crate::plugins::account::read_user;
use crate::plugins::publish::{PublishEntryConfig, PublishEntryOperation};
use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::request_policy::{PolicyKind, RequestPolicy};
use aruna_core::structs::identity::auth::{Permission as Access, Role};
use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
use aruna_core::structs::identity::user::User;
use aruna_tasks::TaskHandle;
use serde_json::json;
use std::collections::{BTreeSet, HashMap, HashSet};
use tempfile::{TempDir, tempdir};

pub(crate) struct Fixture {
    _dir: TempDir,
    pub ctx: DriverContext,
    pub actor: Actor,
    pub group_id: GroupId,
}

impl Fixture {
    pub(crate) async fn new() -> Self {
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
        // The actor publishes test versions, which needs the realm admin role.
        fixture.realm_roles(&[]).await;
        fixture.publish("0.1.0", "ab", json!({})).await;
        fixture
    }

    pub(crate) fn auth(&self) -> AuthContext {
        AuthContext {
            user_id: self.actor.user_id,
            realm_id: self.actor.realm_id,
            path_restrictions: None,
            session: None,
        }
    }

    /// Gives the actor and `others` every realm role.
    async fn realm_roles(&self, others: &[UserId]) {
        let realm_id = self.actor.realm_id;
        let mut realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
        for role in realm_auth.roles.values_mut() {
            role.assigned_users.insert(self.actor.user_id);
            role.assigned_users.extend(others.iter().copied());
        }
        let value = realm_auth.to_bytes(&self.actor).unwrap();
        self.write_row(AUTH_KEYSPACE, realm_id.as_bytes().to_vec(), value)
            .await;
    }

    async fn write_row(&self, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        let write = StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        self.ctx.storage_handle.send_storage_effect(write).await;
    }

    /// Publishes a version whose manifest asks for metadata access and one connection.
    pub(crate) async fn publish(&self, version: &str, byte: &str, extra: Value) -> String {
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
        let published_at_ms = u64::from(u8::from_str_radix(byte, 16).unwrap_or_default());
        let plugin = PublishedPlugin {
            name: "announcer".into(),
            version: version.into(),
            digest: digest.clone(),
            size: 10,
            manifest: manifest.to_string(),
            published_by: self.actor.user_id,
            published_at_ms,
        };
        let write = PublishEntryOperation::new(PublishEntryConfig {
            actor: self.actor.clone(),
            auth_context: self.auth(),
            plugin,
            copies: BTreeSet::from([self.actor.node_id]),
        });
        drive(write, &self.ctx).await.unwrap();
        digest
    }

    pub(crate) fn request(&self, install_id: Option<Ulid>) -> InstallRequest {
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

    pub(crate) async fn install(
        &self,
        request: &InstallRequest,
    ) -> Result<PluginInstall, InstallError> {
        install_plugin(&self.ctx, &self.actor, &self.auth(), request).await
    }

    pub(crate) async fn installs(&self) -> Vec<PluginInstall> {
        read_config(&self.ctx, self.actor.realm_id)
            .await
            .unwrap()
            .installs
    }

    pub(crate) async fn account(&self, install: &PluginInstall) -> Option<User> {
        read_user(&self.ctx, install.account).await.unwrap()
    }

    pub(crate) async fn roles(&self) -> HashMap<Ulid, Role> {
        read_auth(&self.ctx, self.group_id)
            .await
            .unwrap()
            .unwrap()
            .roles
    }

    pub(crate) async fn role(&self, install: &PluginInstall) -> Option<Role> {
        self.roles().await.get(&install.role_id).cloned()
    }

    /// Replaces the realm's request policies with one deny policy per expression.
    pub(crate) async fn deny(&self, expressions: &[&str]) {
        let mut config = read_config(&self.ctx, self.actor.realm_id).await.unwrap();
        config.request_policies = (expressions.iter().enumerate())
            .map(|(n, expression)| RequestPolicy {
                policy_id: Ulid::from_parts(7, n as u128),
                name: format!("deny {n}"),
                kind: PolicyKind::Deny,
                when: None,
                expression: expression.to_string(),
                enabled: true,
            })
            .collect();
        let target = DocumentTarget::RealmConfig {
            realm_id: self.actor.realm_id,
        };
        let value = config.to_bytes(&self.actor).unwrap();
        let key = target.storage_key().to_vec();
        self.write_row(target.storage_keyspace(), key, value).await;
    }

    /// The first preparation step alone, as a crash right after it leaves the install.
    async fn prepared_only(&self, install_id: Ulid) -> PluginInstall {
        let auth = self.auth();
        let request = self.request(Some(install_id));
        let prepared = prepare_install(&self.ctx, &auth, self.actor.realm_id, &request).await;
        let record = new_record(&self.actor, &prepared.unwrap());
        write_record(&self.ctx, &self.actor, &auth, None, record, None)
            .await
            .unwrap()
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
    assert_ne!(
        installed.account.user_ulid, id,
        "the account id is not the caller's"
    );
    assert_ne!(installed.role_id, id, "the role id is not the caller's");
    assert_eq!(fixture.installs().await, vec![installed.clone()]);
    let account = fixture.account(&installed).await.unwrap();
    assert_eq!(account.service_group(), Some(fixture.group_id));
    assert_eq!(account.install(), Some(id));
    assert!(!account.is_deactivated());
    assert!(account.name.starts_with("plugin announcer (Lab A)"));
    let role = fixture.role(&installed).await.unwrap();
    assert_eq!(role.name, "plugin-write");
    assert_eq!(role.assigned_users, HashSet::from([installed.account]));

    assert_eq!(
        fixture.install(&request).await.unwrap(),
        installed,
        "a retry answers the same"
    );
    // A newer version published meanwhile does not change what a retry without version means.
    fixture.publish("0.1.1", "cd", json!({})).await;
    assert_eq!(fixture.install(&request).await.unwrap(), installed);
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
        let id = Ulid::from_parts(9, stop);
        let record = fixture.prepared_only(id).await;
        let (scope, identity) = (scope_of(actor.realm_id, &record), Identity::of(&record));
        if stop >= 2 {
            ensure_account(ctx, actor, &auth, &scope, identity, "x".into())
                .await
                .unwrap();
        }
        if stop >= 3 {
            let paths = role_paths(&parse_grants(&record.grants));
            ensure_role(ctx, actor, &auth, &scope, identity, &paths)
                .await
                .unwrap();
        }
        if stop == 4 {
            teardown(ctx, actor, &auth, &scope, identity).await.unwrap();
            assert!(fixture.account(&record).await.unwrap().is_deactivated());
            assert!(fixture.role(&record).await.is_none());
        }
        assert_eq!(fixture.installs().await[0].state, InstallState::Preparing);
        let installed = fixture.install(&fixture.request(Some(id))).await.unwrap();
        assert_eq!(
            installed.state,
            InstallState::Active,
            "stop after step {stop}"
        );
        assert!(!fixture.account(&installed).await.unwrap().is_deactivated());
        assert!(fixture.role(&installed).await.is_some());
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
async fn refuses_held_accounts() {
    // An account id that already holds a role is never adopted: neither a realm role nor
    // another role of the group.
    let fixture = Fixture::new().await;
    let record = fixture.prepared_only(Ulid::from_parts(9, 1)).await;
    fixture.realm_roles(&[record.account]).await;
    let held = fixture
        .install(&fixture.request(Some(record.install_id)))
        .await;
    assert!(
        matches!(
            held,
            Err(InstallError::Step {
                step: "account",
                ..
            })
        ),
        "{held:?}"
    );
    assert!(
        fixture.account(&record).await.is_none(),
        "no account is created"
    );
    assert_eq!(
        fixture.installs().await[0].failed.as_deref(),
        Some("account")
    );

    let fixture = Fixture::new().await;
    let record = fixture.prepared_only(Ulid::from_parts(9, 2)).await;
    let extra = AddRoleOperation::new(AddRoleConfig {
        auth_context: fixture.auth(),
        actor: fixture.actor.clone(),
        realm_id: fixture.actor.realm_id,
        group_id: fixture.group_id,
        role: Role {
            role_id: Ulid::from_parts(8, 8),
            name: "extra".into(),
            permissions: HashMap::new(),
            assigned_users: HashSet::from([record.account]),
        },
    });
    drive(extra, &fixture.ctx).await.unwrap();
    let held = fixture
        .install(&fixture.request(Some(record.install_id)))
        .await;
    assert!(
        matches!(
            held,
            Err(InstallError::Step {
                step: "account",
                ..
            })
        ),
        "{held:?}"
    );
}

#[tokio::test]
async fn keeps_foreign_roles() {
    // An install id equal to an existing role's id leaves that role alone.
    let fixture = Fixture::new().await;
    let roles = fixture.roles().await;
    let (admin_id, admin) = (roles.iter())
        .find(|(_, role)| role.name == "admin")
        .unwrap();
    let installed = fixture
        .install(&fixture.request(Some(*admin_id)))
        .await
        .unwrap();
    assert_ne!(installed.role_id, *admin_id);
    assert_eq!(fixture.roles().await.get(admin_id), Some(admin));
}

#[tokio::test]
async fn checks_policies() {
    let fixture = Fixture::new().await;
    let meta = format!("/{}/g/{}/meta", fixture.actor.realm_id, fixture.group_id);
    // A policy that reads the path: a subtree cannot be proven, exact paths can.
    fixture
        .deny(&[&format!("path.startsWith('{meta}/private')")])
        .await;
    let refused = fixture.install(&fixture.request(None)).await;
    assert!(
        matches!(refused, Err(InstallError::NotHeld(_))),
        "{refused:?}"
    );
    let mut exact = fixture.request(None);
    exact
        .grants
        .insert("metadata.read".into(), vec![format!("{meta}/runs")]);
    exact.grants.insert("metadata.write".into(), Vec::new());
    assert!(fixture.install(&exact).await.is_ok());

    // A policy on one action refuses that action only, with its own access and operation.
    let fixture = Fixture::new().await;
    fixture
        .deny(&["operation == 'plugin.metadata.write' && permission == 'write'"])
        .await;
    let refused = fixture.install(&fixture.request(None)).await;
    assert!(
        matches!(refused, Err(InstallError::NotHeld(_))),
        "{refused:?}"
    );
    let mut read_only = fixture.request(None);
    read_only.grants.insert("metadata.write".into(), Vec::new());
    assert!(fixture.install(&read_only).await.is_ok());
}

#[tokio::test]
async fn serializes_retries() {
    let fixture = Fixture::new().await;
    let request = fixture.request(Some(Ulid::from_parts(9, 3)));
    let (first, second) = tokio::join!(fixture.install(&request), fixture.install(&request));
    let (first, second) = (first.unwrap(), second.unwrap());
    assert_eq!(first, second);
    assert_eq!(first.state, InstallState::Active);
    assert!(!fixture.account(&first).await.unwrap().is_deactivated());
    assert!(fixture.role(&first).await.is_some());
}

#[test]
fn accesses_per_action() {
    use crate::plugins::grants::accesses;
    use aruna_plugins::permission::Permission;
    assert_eq!(
        accesses(Permission::ObjectsCopy),
        [Access::READ, Access::WRITE]
    );
    assert_eq!(accesses(Permission::MetadataRead), [Access::READ]);
}
