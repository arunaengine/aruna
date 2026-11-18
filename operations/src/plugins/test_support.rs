//! Builds the realms, installs, secrets and guests that plugin tests here and in the node use.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use crate::driver::drive;
use crate::groups::create_group::CreateGroupConfig;
use crate::groups::create_group::CreateGroupOperation;
use crate::jobs::executor::JobContext;
use crate::jobs::executor::ProgressReporter;
use crate::plugins::account::ReadGroupAuth;
use crate::plugins::calls::install_plugin;
use crate::plugins::calls::open;
use crate::plugins::calls::prepare_install;
use crate::plugins::calls::read_config;
use crate::plugins::calls::release;
use crate::plugins::calls::store_local;
use crate::plugins::calls::write_record;
use crate::plugins::check::RunScope;
use crate::plugins::install::InstallError;
use crate::plugins::install::InstallRequest;
use crate::plugins::install::new_record;
use crate::plugins::publish::PublishEntryConfig;
use crate::plugins::publish::PublishEntryOperation;
use crate::plugins::rows::StoreError;
use crate::plugins::rows::decode;
use crate::plugins::rows::read_rows;
use crate::plugins::rows::row;
use crate::plugins::rows::write_rows;
use crate::plugins::rules::EditRules;
use crate::plugins::rules::RuleError;
use crate::plugins::rules::RuleGuard;
use crate::plugins::rules::consumer;
use crate::plugins::submit::CopyRequest;
use crate::plugins::submit::digest;
use crate::plugins::transfer_steps::account;
use crate::plugins::work::Identity;
use crate::plugins::work::WorkKind;
use crate::plugins::work::WorkRow;
use crate::plugins::work::index_key;
use crate::plugins::work::reserve;
use crate::plugins::work::row_of;
use crate::realm::create_realm::CreateRealmConfig;
use crate::realm::create_realm::CreateRealmOperation;
use crate::secrets::SecretChange;
use crate::secrets::owner::OwnerChange;
use crate::secrets::owner::OwnerSecretOperation;
use aruna_core::UserId;
use aruna_core::admin_documents::AdminDocumentTarget;
use aruna_core::compute::Secret;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::keyspaces::GROUP_KEYSPACE;
use aruna_core::keyspaces::PLUGIN_WORK_KEYSPACE;
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_installs::SecretBinding;
use aruna_core::plugin_runs::PluginRun;
use aruna_core::plugin_runs::RunState;
use aruna_core::plugin_runs::RunTrigger;
use aruna_core::plugin_runs::SourceEvent;
use aruna_core::plugin_runs::SourceKind;
use aruna_core::plugins::PublishedPlugin;
use aruna_core::realm_vault::RealmKeys;
use aruna_core::request_policy::PolicyKind;
use aruna_core::request_policy::RequestPolicy;
use aruna_core::secret_registers::SecretOwner;
use aruna_core::secrets::Delivery;
use aruna_core::secrets::ResponseExposure;
use aruna_core::secrets::SecretConsumer;
use aruna_core::secrets::SecretFields;
use aruna_core::secrets::SecretKind;
use aruna_core::secrets::SecretName;
use aruna_core::secrets::SecretSettings;
use aruna_core::secrets::UseRule;
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::execution::job::JobProgress;
use aruna_core::structs::execution::job::PluginWorkAction;
use aruna_core::structs::execution::job::PluginWorkSpec;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::auth::Role;
use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::user::User;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::GroupId;
use aruna_plugins::artifact::ArtifactDigest;
use aruna_plugins::artifact::attach_manifest;
use aruna_plugins::artifact::read_manifest;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use ed25519_dalek::SigningKey;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

/// The signing key of the realm of [`Fixture::keyed`].
pub const REALM_KEY: [u8; 32] = [9u8; 32];

pub struct Fixture {
    _dir: TempDir,
    pub ctx: DriverContext,
    pub actor: Actor,
    pub group_id: GroupId,
}

/// A driver context on the storage in `dir`, passed through `wrap`.
pub fn node_context(
    dir: &Path,
    wrap: impl FnOnce(StorageHandle) -> StorageHandle,
) -> DriverContext {
    let storage = aruna_storage::FjallStorage::open(dir.to_str().unwrap()).unwrap();
    DriverContext {
        storage_handle: wrap(storage),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
        outbound_handle: None,
    }
}

pub fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        match entry.file_type().unwrap().is_dir() {
            true => copy_dir(&entry.path(), &target),
            false => drop(std::fs::copy(entry.path(), target).unwrap()),
        }
    }
}

impl Fixture {
    pub async fn new() -> Self {
        Self::on(|storage| storage).await
    }

    /// A fixture whose storage is passed through `wrap` first.
    pub async fn on(wrap: impl FnOnce(StorageHandle) -> StorageHandle) -> Self {
        Self::in_realm(RealmId::from_bytes([1u8; 32]), wrap).await
    }

    /// A fixture whose realm id is the public half of `REALM_KEY`, so its realm keys exist.
    pub async fn keyed() -> Self {
        let key = ed25519_dalek::SigningKey::from_bytes(&REALM_KEY);
        let realm_id = RealmId::from_bytes(key.verifying_key().to_bytes());
        Self::in_realm(realm_id, |storage| storage).await
    }

    pub async fn in_realm(
        realm_id: RealmId,
        wrap: impl FnOnce(StorageHandle) -> StorageHandle,
    ) -> Self {
        let dir = tempdir().unwrap();
        let ctx = node_context(dir.path(), wrap);
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

    /// Two management nodes of one realm that start from this fixture's state.
    pub async fn split(self) -> (Fixture, Fixture) {
        let Fixture {
            _dir,
            ctx,
            actor,
            group_id,
        } = self;
        ctx.storage_handle.close().await;
        let copy = tempdir().unwrap();
        copy_dir(_dir.path(), copy.path());
        let node = |dir: TempDir| Fixture {
            ctx: node_context(dir.path(), |storage| storage),
            _dir: dir,
            actor: actor.clone(),
            group_id,
        };
        (node(_dir), node(copy))
    }

    /// Hands this node's realm config to `other`, as replication would.
    pub async fn deliver_config(&self, other: &Fixture) {
        let realm_id = self.actor.realm_id;
        let document = DocumentTarget::RealmConfig { realm_id };
        let state = reducer_state_key(&AdminDocumentTarget::RealmConfig { realm_id });
        let rows = [
            (
                document.storage_keyspace().to_string(),
                document.storage_key().to_vec(),
            ),
            (DOCUMENT_STATE_KEYSPACE.to_string(), state.to_vec()),
        ];
        self.deliver(other, &rows).await;
    }

    /// Hands this node's group and its roles to `other`, as replication would.
    pub async fn deliver_group(&self, other: &Fixture) {
        let group_id = self.group_id;
        let state = reducer_state_key(&AdminDocumentTarget::Group { group_id });
        let rows = [
            (AUTH_KEYSPACE.to_string(), group_id.to_bytes().to_vec()),
            (GROUP_KEYSPACE.to_string(), group_id.to_bytes().to_vec()),
            (DOCUMENT_STATE_KEYSPACE.to_string(), state.to_vec()),
        ];
        self.deliver(other, &rows).await;
    }

    pub async fn deliver(&self, other: &Fixture, rows: &[(String, Vec<u8>)]) {
        for (key_space, key) in rows {
            let txn = crate::plugins::store::Txn::direct(&self.ctx);
            let value = txn.read(key_space, key).await.unwrap().unwrap();
            other.write_row(key_space, key.clone(), value).await;
        }
    }

    pub fn auth(&self) -> AuthContext {
        AuthContext {
            user_id: self.actor.user_id,
            realm_id: self.actor.realm_id,
            path_restrictions: None,
            session: None,
        }
    }

    /// Gives the actor and `others` every realm role.
    pub async fn realm_roles(&self, others: &[UserId]) {
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

    pub async fn write_row(&self, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        let write = StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        self.ctx.storage_handle.send_storage_effect(write).await;
    }

    /// Publishes a version whose manifest asks for metadata access and one connection.
    pub async fn publish(&self, version: &str, byte: &str, extra: Value) -> String {
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

    pub fn request(&self, install_id: Option<Ulid>) -> InstallRequest {
        let settings = json!({ "catalog_url": "https://catalog.example.org" });
        InstallRequest {
            plugin: "announcer".into(),
            group_id: self.group_id,
            version: None,
            settings: settings.as_object().cloned().unwrap_or_default(),
            grants: BTreeMap::new(),
            install_id,
            secrets: BTreeMap::new(),
        }
    }

    pub async fn install(&self, request: &InstallRequest) -> Result<PluginInstall, InstallError> {
        install_plugin(&self.ctx, &self.actor, &self.auth(), request).await
    }

    pub async fn installs(&self) -> Vec<PluginInstall> {
        read_config(&self.ctx, self.actor.realm_id)
            .await
            .unwrap()
            .installs
    }

    pub async fn account(&self, install: &PluginInstall) -> Option<User> {
        let row = (USER_KEYSPACE, install.account.to_bytes().to_vec());
        let rows = drive(read_rows(vec![row]), &self.ctx).await.unwrap();
        let row = rows.into_iter().next().flatten();
        row.map(|bytes| User::from_bytes(&bytes).unwrap())
    }

    pub async fn roles(&self) -> HashMap<Ulid, Role> {
        drive(ReadGroupAuth(self.group_id), &self.ctx)
            .await
            .unwrap()
            .unwrap()
            .roles
    }

    pub async fn role(&self, install: &PluginInstall) -> Option<Role> {
        self.roles().await.get(&install.role_id).cloned()
    }

    /// Replaces the realm's request policies with one deny policy per expression.
    pub async fn deny(&self, expressions: &[&str]) {
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
    pub async fn prepared_only(&self, install_id: Ulid) -> PluginInstall {
        let auth = self.auth();
        let request = self.request(Some(install_id));
        let prepared = prepare_install(&self.ctx, &auth, self.actor.realm_id, &request).await;
        let record = new_record(&self.actor, &prepared.unwrap(), self.actor.node_id);
        write_record(&self.ctx, &self.actor, &auth, None, record, None)
            .await
            .unwrap()
    }
}

pub const TOKEN: &str = "canary-release-4f1e";

pub const ORIGIN: &str = "https://api.example.org";

/// Gives the fixture's storage the realm keys of `REALM_KEY`, as a management node holds them.
pub fn hold_keys(ctx: &DriverContext) {
    let key = SigningKey::from_bytes(&REALM_KEY);
    ctx.storage_handle.open_realm(RealmKeys::derive(&key));
}

/// Writes the realm config as it is after `edit`.
pub async fn edit_config(fixture: &Fixture, edit: impl FnOnce(&mut RealmConfigDocument)) {
    let mut config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    edit(&mut config);
    let target = DocumentTarget::RealmConfig {
        realm_id: fixture.actor.realm_id,
    };
    let value = config.to_bytes(&fixture.actor).unwrap();
    let key = target.storage_key().to_vec();
    fixture
        .write_row(target.storage_keyspace(), key, value)
        .await;
}

/// An install moved to a version whose `catalog` connection injects the bearer slot `api_key`,
/// with that slot bound to the group secret `api-key`.
pub async fn bound(fixture: &Fixture) -> PluginInstall {
    let slots = json!({
        "secrets": { "api_key": { "title": "API key", "kind": "bearer" } },
        "connections": { "catalog": { "url": "{catalog_url}/api/", "auth": "api_key" } }
    });
    let installed = fixture.install(&fixture.request(None)).await.unwrap();
    let digest = fixture.publish("0.2.0", "cd", slots).await;
    let binding = SecretBinding::parse("group:api-key").unwrap();
    edit_config(fixture, |config| {
        let install = (config.installs.iter_mut())
            .find(|install| install.install_id == installed.install_id)
            .unwrap();
        (install.digest, install.version) = (digest, "0.2.0".to_string());
        install.secrets = BTreeMap::from([("api_key".to_string(), binding)]);
    })
    .await;
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    config.installs.into_iter().next().unwrap()
}

/// The `inject` rule of `install` toward the fixture origins with `response`.
pub fn rule_for(install: &PluginInstall, response: ResponseExposure) -> UseRule {
    UseRule {
        consumer: SecretConsumer::Plugin {
            plugin_id: install.plugin.clone(),
            install_id: install.install_id,
        },
        delivery: Delivery::Inject,
        origins: vec![ORIGIN.to_string()],
        paths: Vec::new(),
        methods: Vec::new(),
        response,
        groups: Vec::new(),
    }
}

/// The group secret `api-key` with one `inject` rule for `install` toward `ORIGIN`.
pub async fn secret(fixture: &Fixture, install: &PluginInstall) {
    secret_with(fixture, install, ResponseExposure::Status).await;
}

/// The group secret `api-key` whose one rule for `install` exposes `response`.
pub async fn secret_with(fixture: &Fixture, install: &PluginInstall, response: ResponseExposure) {
    let rule = rule_for(install, response);
    let change = SecretChange::Create {
        name: SecretName::try_from("api-key".to_string()).unwrap(),
        kind: SecretKind::parse("bearer").unwrap(),
        description: String::new(),
        settings: SecretSettings::new(),
        uses: vec![rule],
        fields: SecretFields::new([("token".to_string(), Secret::new(TOKEN))].into()),
    };
    let input = OwnerChange {
        realm_id: fixture.actor.realm_id,
        node_id: fixture.actor.node_id,
        owner: SecretOwner::Group(install.group_id),
        actor: fixture.actor.user_id,
        change,
        readable: false,
        now_ms: 1,
        write_ids: [1, 2, 3].map(|part| Ulid::from_parts(77, part)),
    };
    drive(OwnerSecretOperation::new(input), &fixture.ctx)
        .await
        .unwrap();
}

/// A queued run of `install_id`; ids grow with `seq`.
pub fn queued(install_id: Ulid, seq: u64) -> PluginRun {
    PluginRun {
        run_id: Ulid::from_parts(seq, 1),
        install_id,
        group_id: Ulid::from_parts(1, 9),
        plugin: "announcer".into(),
        version: "0.1.0".into(),
        revision: 1,
        digest: format!("sha256:{}", "ab".repeat(32)),
        handler: "on_manual".into(),
        settings: "{}".into(),
        trigger: RunTrigger::Timer { occurrence_ms: seq },
        state: RunState::Queued,
        attempts: 0,
        due_ms: seq,
        created_ms: seq,
        finished_ms: None,
        message: None,
        output: None,
        log: Vec::new(),
    }
}

/// Sets the rules of one consumer on a group secret, now.
pub async fn edit_rules(
    ctx: &DriverContext,
    actor: &Actor,
    target: (GroupId, &SecretName),
    rules: (&SecretConsumer, Vec<UseRule>),
    guard: RuleGuard,
) -> Result<bool, RuleError> {
    let now_ms = unix_timestamp_millis();
    drive(EditRules::new(actor, target, rules, guard, now_ms), ctx).await
}

pub fn scope(fixture: &Fixture, install: &PluginInstall) -> RunScope {
    let mut run = queued(install.install_id, 1);
    (run.digest, run.group_id) = (install.digest.clone(), install.group_id);
    RunScope {
        realm_id: fixture.actor.realm_id,
        local: fixture.actor.node_id,
        run,
    }
}

pub fn path(fixture: &Fixture, install: &PluginInstall, rest: &str) -> String {
    let (realm, node) = (fixture.actor.realm_id, fixture.actor.node_id);
    format!("/{realm}/g/{}/data/{node}/{rest}", install.group_id)
}

pub fn request(fixture: &Fixture, install: &PluginInstall, target: &str) -> CopyRequest {
    CopyRequest {
        source: path(fixture, install, "in/a.txt"),
        source_version: None,
        target: path(fixture, install, target),
        ordinal: 0,
    }
}

pub async fn ready(fixture: &Fixture) -> PluginInstall {
    let install = copier(fixture).await;
    let account = install.account;
    for bucket in ["in", "out"] {
        seed_bucket(&fixture.ctx, bucket, install.group_id, account, Vec::new()).await;
    }
    install
}

/// Changes the row of work `ids` as a test needs; `None` when it is gone.
pub async fn edit_work(
    ctx: &DriverContext,
    (install_id, work_id): (Ulid, Ulid),
    change: impl FnOnce(&mut WorkRow),
) -> Result<Option<WorkRow>, StoreError> {
    let index = vec![(PLUGIN_WORK_KEYSPACE, index_key(install_id, work_id))];
    let [key] = <[_; 1]>::try_from(drive(read_rows(index), ctx).await?).unwrap();
    let Some(key) = key else {
        return Ok(None);
    };
    let read = vec![(PLUGIN_WORK_KEYSPACE, key.clone())];
    let [value] = <[_; 1]>::try_from(drive(read_rows(read), ctx).await?).unwrap();
    let Some(value) = value else {
        return Ok(None);
    };
    let mut row: WorkRow = decode(&value)?;
    change(&mut row);
    drive(write_rows(vec![row_of(key, &row)?]), ctx).await?;
    Ok(Some(row))
}

pub const FIXTURE: &[u8] = include_bytes!("../../../plugins/tests/fixture.wasm");

pub fn leb(mut value: usize, out: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        match value {
            0 => return out.push(byte),
            _ => out.push(byte | 0x80),
        }
    }
}

/// The fixture without its secret and connection, which installs refuse.
pub fn runnable() -> Vec<u8> {
    artifact(false, false)
}

/// The fixture whose `api` connection sends without a secret.
pub fn connected() -> Vec<u8> {
    artifact(true, false)
}

/// The fixture whose `api` connection injects its bearer slot `api`.
pub fn secured() -> Vec<u8> {
    artifact(true, true)
}

/// The runnable fixture, whose sync triggers `echo` handles; it answers the event.
pub fn echoed_sync() -> Vec<u8> {
    let (component, mut manifest) = bare();
    (manifest.connections, manifest.secrets) = (BTreeMap::new(), BTreeMap::new());
    attach_manifest(component, &manifest).unwrap()
}

/// The runnable fixture with only its manual and `job_finished` triggers, so no timer or event
/// of the test's own writes starts a run.
pub fn job_guest() -> Vec<u8> {
    use aruna_plugins::trigger::TriggerKind;
    let (component, mut manifest) = bare();
    (manifest.connections, manifest.secrets) = (BTreeMap::new(), BTreeMap::new());
    let kept = [TriggerKind::Manual, TriggerKind::JobFinished];
    manifest.triggers.retain(|kind, _| kept.contains(kind));
    attach_manifest(component, &manifest).unwrap()
}

/// The fixture's component without its manifest section, and that manifest.
pub fn bare() -> (&'static [u8], aruna_plugins::manifest::Manifest) {
    let manifest = read_manifest(FIXTURE).unwrap();
    let json = serde_json::to_vec(&manifest).unwrap();
    let mut content = Vec::new();
    leb("aruna-plugin".len(), &mut content);
    content.extend_from_slice(b"aruna-plugin");
    content.extend_from_slice(&json);
    let mut section = vec![0u8];
    leb(content.len(), &mut section);
    section.extend_from_slice(&content);
    (FIXTURE.strip_suffix(section.as_slice()).unwrap(), manifest)
}

pub fn artifact(connection: bool, secret: bool) -> Vec<u8> {
    let (component, manifest) = bare();
    let mut narrow = manifest;
    match secret {
        true => narrow.version = "0.2.1".to_string(),
        false => narrow.secrets.clear(),
    }
    match (connection, secret) {
        (true, true) => {}
        (true, false) => (narrow.connections.values_mut()).for_each(|c| c.auth = None),
        (false, _) => narrow.connections.clear(),
    }
    attach_manifest(component, &narrow).unwrap()
}

/// An install of the runnable fixture whose artifact this node keeps.
pub async fn installed(fixture: &Fixture) -> PluginInstall {
    install_artifact(fixture, runnable()).await
}

/// Publishes the fixture artifact `bytes` and keeps it on this node; answers its digest.
pub async fn publish_artifact(fixture: &Fixture, bytes: &[u8]) -> String {
    let manifest = read_manifest(bytes).unwrap();
    let digest = ArtifactDigest::of(bytes).to_string();
    let plugin = PublishedPlugin {
        name: manifest.name.clone(),
        version: manifest.version.clone(),
        digest: digest.clone(),
        size: bytes.len() as u64,
        manifest: serde_json::to_string(&manifest).unwrap(),
        published_by: fixture.actor.user_id,
        published_at_ms: 1,
    };
    let publish = PublishEntryOperation::new(PublishEntryConfig {
        actor: fixture.actor.clone(),
        auth_context: fixture.auth(),
        plugin,
        copies: BTreeSet::from([fixture.actor.node_id]),
    });
    drive(publish, &fixture.ctx).await.unwrap();
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    store_local(&fixture.ctx, &config, &digest, bytes, 1)
        .await
        .unwrap();
    digest
}

/// An install of the fixture artifact `bytes`, kept on this node.
pub async fn install_artifact(fixture: &Fixture, bytes: Vec<u8>) -> PluginInstall {
    let manifest = read_manifest(&bytes).unwrap();
    let digest = publish_artifact(fixture, &bytes).await;
    let settings = serde_json::json!({ "api_url": "https://api.example.org" });
    let request = InstallRequest {
        plugin: manifest.name,
        group_id: fixture.group_id,
        version: Some(digest),
        settings: settings.as_object().cloned().unwrap_or_default(),
        grants: BTreeMap::new(),
        install_id: None,
        secrets: BTreeMap::new(),
    };
    fixture.install(&request).await.unwrap()
}

/// Grant choices that scope both `read` and `write` to `patterns`.
pub fn scoped(patterns: Vec<String>) -> BTreeMap<String, Vec<String>> {
    BTreeMap::from([
        ("read".into(), patterns.clone()),
        ("write".into(), patterns),
    ])
}

/// A created dataset `resource` under the install's group metadata, and that metadata's base.
pub fn created(install: &PluginInstall, resource: &str) -> (SourceEvent, String) {
    let whole = &install.grants["read"][0];
    let base = format!("{}/meta", whole.strip_suffix("/**").unwrap());
    let event = SourceEvent {
        kind: SourceKind::DatasetCreated,
        group_id: install.group_id,
        path: format!("{base}/{resource}"),
        resource: resource.into(),
        revision: "r1".into(),
        source: format!("dataset_created/{resource}"),
        anchor_ms: 1,
        journaled_ms: 1,
        actor: None,
    };
    (event, base)
}

/// A completed sync job of the relationship `relationship` under `prefix` of the install's data.
pub fn synced(install: &PluginInstall, prefix: &str, relationship: Ulid) -> SourceEvent {
    let whole = &install.grants["read"][0];
    let path = format!("{}/data/{prefix}", whole.strip_suffix("/**").unwrap());
    let creator = install.account;
    let kind = SourceKind::SyncCompleted;
    let node = iroh::SecretKey::from_bytes(&[9; 32]).public();
    let ids = (node, Ulid::from_parts(relationship.timestamp_ms(), 7));
    SourceEvent::sync_job(
        kind,
        (install.group_id, path),
        (relationship, creator),
        ids,
        1,
    )
}

pub fn job_context(ctx: &DriverContext, fixture: &Fixture) -> JobContext {
    JobContext {
        driver: Arc::new(ctx.clone()),
        job_id: JobId::from_bytes([7u8; 16]),
        owner_node_id: fixture.actor.node_id,
        claim_token: Ulid::generate(),
        final_attempt: false,
        cancel: CancellationToken::new(),
        shutdown: CancellationToken::new(),
        progress: ProgressReporter::from_progress(&JobProgress::new("bytes")),
        range_turns: Default::default(),
        handlers: Default::default(),
    }
}

/// An active install of a version that requests `objects.copy`, granted the group's data.
pub async fn copier(fixture: &Fixture) -> PluginInstall {
    let copy = json!({ "permissions": { "required": ["objects.copy"] } });
    let digest = fixture.publish("0.2.0", "cd", copy).await;
    let mut request = fixture.request(None);
    request.version = Some(digest);
    let data = format!("/{}/g/{}/data/**", fixture.actor.realm_id, fixture.group_id);
    request.grants = std::collections::BTreeMap::from([
        ("read".into(), vec![data.clone()]),
        ("write".into(), vec![data]),
    ]);
    fixture.install(&request).await.unwrap()
}

/// A recorded copy of `install` between two object paths of its group on this node.
pub async fn recorded(
    fixture: &Fixture,
    install: &PluginInstall,
    (source, target): (&str, &str),
) -> PluginWorkSpec {
    let mut row = WorkRow::reserved(install, WorkKind::Copy, Ulid::generate(), 1);
    row.version_id = Some(Ulid::generate());
    let identity = Identity::Call {
        run_id: row.run_id,
        ordinal: 0,
    };
    let (row, _) = drive(
        reserve((install, &identity), row, None).unwrap(),
        &fixture.ctx,
    )
    .await
    .unwrap();
    let data = format!("/{}/g/{}/data", fixture.actor.realm_id, install.group_id);
    let node = fixture.actor.node_id;
    PluginWorkSpec {
        realm_id: fixture.actor.realm_id,
        install_id: install.install_id,
        work_id: row.work_id,
        group_id: install.group_id,
        action: PluginWorkAction::Copy {
            source: format!("{data}/{node}/{source}"),
            source_version: None,
            target: format!("{data}/{node}/{target}"),
            version_id: row.version_id.unwrap(),
        },
    }
}

/// The install's objects `in/a.txt` (5 bytes) and the empty bucket `out` on a blob backend, and
/// a recorded copy between them.
pub async fn ready_copy(
    fixture: &Fixture,
) -> (
    tempfile::TempDir,
    DriverContext,
    PluginInstall,
    PluginWorkSpec,
) {
    use crate::driver::drive;
    use crate::s3::object::put::PutObjectOperation;
    use crate::testing_objects::{put_config, seed_bucket, with_blob};
    let install = copier(fixture).await;
    let root = tempfile::tempdir().unwrap();
    let blob = with_blob(&fixture.ctx, root.path()).await;
    for bucket in ["in", "out"] {
        seed_bucket(&blob, bucket, install.group_id, install.account, Vec::new()).await;
    }
    let (realm, node) = (fixture.actor.realm_id, fixture.actor.node_id);
    let source = put_config(realm, install.group_id, node, "in", "a.txt", b"bytes");
    drive(PutObjectOperation::new(source), &blob).await.unwrap();
    let spec = recorded(fixture, &install, ("in/a.txt", "out/a.txt")).await;
    (root, blob, install, spec)
}

/// The keyspaces with a row that holds `canary` in plain bytes.
pub async fn leaks(fixture: &Fixture, canary: &str) -> Vec<&'static str> {
    use aruna_core::keyspaces::KEYSPACE_CATALOG;
    let txn = crate::plugins::store::Txn::direct(&fixture.ctx);
    let held = |bytes: &Vec<u8>| {
        bytes
            .windows(canary.len())
            .any(|part| part == canary.as_bytes())
    };
    let mut found = Vec::new();
    for space in KEYSPACE_CATALOG {
        let Ok(rows) = txn.scan(space, &[]).await else {
            continue;
        };
        if rows.iter().any(|(key, value)| held(key) || held(value)) {
            found.push(*space);
        }
    }
    found
}
