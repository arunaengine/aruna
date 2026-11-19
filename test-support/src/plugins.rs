//! A plugin test node with a realm, a group and a published version, and the plugin helpers
//! of the operations and node tests whose signatures hold only core, driver or std types.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[path = "plugin_artifacts.rs"]
pub mod artifacts;
#[path = "plugin_flows.rs"]
pub mod flows;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::admin_documents::AdminDocumentTarget;
use aruna_core::compute::Secret;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::jobs::ProgressReporter;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, DOCUMENT_STATE_KEYSPACE, GROUP_KEYSPACE, USER_KEYSPACE,
};
use aruna_core::plugin_installs::{PluginInstall, SecretBinding};
use aruna_core::plugin_runs::{PluginRun, RunState, RunTrigger, SourceEvent, SourceKind};
use aruna_core::plugins::PublishedPlugin;
use aruna_core::realm_vault::RealmKeys;
use aruna_core::request_policy::{PolicyKind, RequestPolicy};
use aruna_core::secret_registers::SecretOwner;
use aruna_core::secrets::{
    Delivery, ResponseExposure, SecretConsumer, SecretFields, SecretKind, SecretName,
    SecretSettings, UseRule,
};
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::execution::job::{JobId, JobProgress, PluginWorkAction, PluginWorkSpec};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId,
};
use aruna_core::structs::identity::user::User;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::GroupId;
use aruna_drivers::driver::{DriverContext, drive};
use aruna_drivers::jobs::context::JobContext;
use aruna_drivers::testing::{seed_bucket, with_blob};
use aruna_operations::groups::create_group::{CreateGroupConfig, CreateGroupOperation};
use aruna_operations::plugins::install::{
    InstallCaller, InstallPrepared, InstallRequest, PrepareInstall,
};
use aruna_operations::plugins::publish::{PublishEntryConfig, PublishEntryOperation};
use aruna_operations::plugins::rows::read_rows;
use aruna_operations::plugins::work::{Identity, WorkKind, WorkRow, reserve};
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_operations::s3::object::put::PutObjectOperation;
use aruna_operations::secrets::SecretChange;
use aruna_operations::secrets::owner::{OwnerChange, OwnerSecretOperation};
use aruna_operations::testing_objects::put_config;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use ed25519_dalek::SigningKey;
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use self::flows::matcher::config;
use self::rows::Txn;

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
            let txn = Txn::direct(&self.ctx);
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

    /// Installs the published `announcer` at `version`, or its newest, with `grants`.
    pub async fn install_with(
        &self,
        version: Option<String>,
        grants: BTreeMap<String, Vec<String>>,
    ) -> PluginInstall {
        let settings = json!({ "catalog_url": "https://catalog.example.org" });
        let request = InstallRequest {
            plugin: "announcer".into(),
            group_id: self.group_id,
            version,
            settings: settings.as_object().cloned().unwrap_or_default(),
            grants,
            install_id: None,
            secrets: BTreeMap::new(),
        };
        install(self, &request).await
    }

    pub async fn installs(&self) -> Vec<PluginInstall> {
        config(self).await.installs
    }

    pub async fn account(&self, install: &PluginInstall) -> Option<User> {
        let row = (USER_KEYSPACE, install.account.to_bytes().to_vec());
        let rows = drive(read_rows(vec![row]), &self.ctx).await.unwrap();
        let row = rows.into_iter().next().flatten();
        row.map(|bytes| User::from_bytes(&bytes).unwrap())
    }

    /// Replaces the realm's request policies with one deny policy per expression.
    pub async fn deny(&self, expressions: &[&str]) {
        edit_config(self, |config| {
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
        })
        .await;
    }
}

/// Installs `request` as the fixture's actor, through the steps the API runs.
async fn install(fixture: &Fixture, request: &InstallRequest) -> PluginInstall {
    let auth = fixture.auth();
    let local = fixture.ctx.net_handle.as_ref().map(|net| net.node_id());
    let prepare = PrepareInstall::new(&auth, (fixture.actor.realm_id, local), request);
    let prepared = drive(prepare, &fixture.ctx).await.unwrap();
    let caller = InstallCaller::new(&fixture.actor, &auth, unix_timestamp_millis());
    drive(InstallPrepared::new(caller, prepared), &fixture.ctx)
        .await
        .unwrap()
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
    let mut config = config(fixture).await;
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
    let installed = fixture.install_with(None, BTreeMap::new()).await;
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
    config(fixture).await.installs.into_iter().next().unwrap()
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
    change_secret(fixture, install, change, (1, 77)).await;
}

/// Replaces the rules of the group secret `api-key` of `install`, as an admin edit does.
pub async fn replace_rules(fixture: &Fixture, install: &PluginInstall, uses: Vec<UseRule>) {
    let change = SecretChange::Replace {
        name: SecretName::try_from("api-key".to_string()).unwrap(),
        description: String::new(),
        settings: SecretSettings::new(),
        uses,
        expected: None,
    };
    let now_ms = unix_timestamp_millis();
    change_secret(fixture, install, change, (now_ms, now_ms)).await;
}

/// Applies `change` to a group secret of `install` at `now_ms`, with write ids from `seed`.
async fn change_secret(
    fixture: &Fixture,
    install: &PluginInstall,
    change: SecretChange,
    (now_ms, seed): (u64, u64),
) {
    let input = OwnerChange {
        realm_id: fixture.actor.realm_id,
        node_id: fixture.actor.node_id,
        owner: SecretOwner::Group(install.group_id),
        actor: fixture.actor.user_id,
        change,
        readable: false,
        now_ms,
        write_ids: [1, 2, 3].map(|part| Ulid::from_parts(seed, part)),
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

/// The queued run of `install` that the fixture's node runs.
pub fn install_run(install: &PluginInstall) -> PluginRun {
    let mut run = queued(install.install_id, 1);
    (run.digest, run.group_id) = (install.digest.clone(), install.group_id);
    run
}

pub fn path(fixture: &Fixture, install: &PluginInstall, rest: &str) -> String {
    let (realm, node) = (fixture.actor.realm_id, fixture.actor.node_id);
    format!("/{realm}/g/{}/data/{node}/{rest}", install.group_id)
}

pub async fn ready(fixture: &Fixture) -> PluginInstall {
    let install = copier(fixture).await;
    let account = install.account;
    for bucket in ["in", "out"] {
        seed_bucket(&fixture.ctx, bucket, install.group_id, account, Vec::new()).await;
    }
    install
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
    let data = format!("/{}/g/{}/data/**", fixture.actor.realm_id, fixture.group_id);
    let grants = BTreeMap::from([
        ("read".into(), vec![data.clone()]),
        ("write".into(), vec![data]),
    ]);
    fixture.install_with(Some(digest), grants).await
}

/// A recorded copy of `install` between two object paths of its group on this node.
pub async fn recorded(
    fixture: &Fixture,
    install: &PluginInstall,
    (source, target): (&str, &str),
) -> PluginWorkSpec {
    let mut row = WorkRow::reserved(
        install,
        WorkKind::Copy,
        (Ulid::generate(), Ulid::generate()),
        1,
    );
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
    let txn = Txn::direct(&fixture.ctx);
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

/// An async access to plugin rows for tests that seed or inspect them.
pub mod rows {
    use aruna_core::effects::{IterStart, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::types::TxnId;
    use aruna_drivers::driver::DriverContext;
    use aruna_operations::plugins::rows::Rows;

    /// Plugin row reads and writes; `direct` runs each on its own.
    pub struct Txn<'a> {
        ctx: &'a DriverContext,
        id: Option<TxnId>,
    }

    impl<'a> Txn<'a> {
        /// Reads and writes outside a transaction, each on its own.
        pub fn direct(ctx: &'a DriverContext) -> Self {
            Self { ctx, id: None }
        }

        pub async fn read(&self, space: &str, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
            let read = StorageEffect::Read {
                key_space: space.to_string(),
                key: key.to_vec().into(),
                txn_id: self.id,
            };
            match self.ctx.storage_handle.send_storage_effect(read).await {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    Ok(value.map(|value| value.to_vec()))
                }
                other => Err(failed(other)),
            }
        }

        /// At most `limit` rows below `prefix` after the key `after`, and the key to go on after.
        pub async fn page(
            &self,
            space: &str,
            prefix: &[u8],
            after: Option<Vec<u8>>,
            limit: usize,
        ) -> Result<(Rows, Option<Vec<u8>>), String> {
            let iter = StorageEffect::Iter {
                key_space: space.to_string(),
                prefix: Some(prefix.to_vec().into()),
                start: after.map(|key| IterStart::After(key.into())),
                limit,
                txn_id: self.id,
            };
            match self.ctx.storage_handle.send_storage_effect(iter).await {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => Ok((
                    (values.into_iter())
                        .map(|(key, value)| (key.to_vec(), value.to_vec()))
                        .collect(),
                    next_start_after.map(|key| key.to_vec()),
                )),
                other => Err(failed(other)),
            }
        }

        /// Every row below `prefix`, read page by page.
        pub async fn scan(&self, space: &str, prefix: &[u8]) -> Result<Rows, String> {
            let (mut rows, mut after) = (Vec::new(), None);
            loop {
                let (page, next) = self.page(space, prefix, after, 1_000).await?;
                rows.extend(page);
                match next {
                    Some(key) => after = Some(key),
                    None => return Ok(rows),
                }
            }
        }

        pub async fn write(&self, space: &str, rows: Rows) -> Result<(), String> {
            if rows.is_empty() {
                return Ok(());
            }
            let writes = (rows.into_iter())
                .map(|(key, value)| (space.to_string(), key.into(), value.into()))
                .collect();
            let write = StorageEffect::BatchWrite {
                writes,
                txn_id: self.id,
            };
            match self.ctx.storage_handle.send_storage_effect(write).await {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
                other => Err(failed(other)),
            }
        }

        pub async fn delete(&self, space: &str, keys: Vec<Vec<u8>>) -> Result<(), String> {
            if keys.is_empty() {
                return Ok(());
            }
            let deletes = (keys.into_iter())
                .map(|key| (space.to_string(), key.into()))
                .collect();
            let delete = StorageEffect::BatchDelete {
                deletes,
                txn_id: self.id,
            };
            match self.ctx.storage_handle.send_storage_effect(delete).await {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
                other => Err(failed(other)),
            }
        }
    }

    fn failed(event: Event) -> String {
        match event {
            Event::Storage(StorageEvent::Error { error }) => error.to_string(),
            other => format!("unexpected {}", other.label()),
        }
    }
}
