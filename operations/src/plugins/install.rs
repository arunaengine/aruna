//! Installs a published plugin into a group: the record `preparing` first, then account and role,
//! activation last. Each step checks current state, so a retry with the same id resumes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock};

use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::plugin_installs::{
    BoundScope, InstallGuard, InstallState, PluginInstall, Prerequisite, SecretBinding,
};
use aruna_core::plugins::PublishedPlugin;
use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::time::{unix_timestamp_millis, unix_timestamp_secs};
use aruna_core::types::GroupId;
use aruna_core::{NodeId, UserId};
use aruna_plugins::artifact::ArtifactDigest;
use aruna_plugins::manifest::Manifest;
use aruna_plugins::permission::{Grant, Permission};
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::sync::{Mutex, OwnedMutexGuard};
use tracing::{info, warn};
use ulid::Ulid;

use super::account::{
    Identity, account_name, check_ready, ensure_account, ensure_role, guard_of, read_auth,
    read_row, read_user, teardown,
};
use super::bound::{BindError, BindPlace, bind_slots, vault_kind};
use super::grants::covers;
use super::grants::{
    GrantError, Grants, InstallScope, accesses, carry_grants, check_supported, narrow_grants,
    parse_grants, patterns_for, role_paths, stored_grants, writes,
};
use super::receiver::pick_receiver;
use super::records::{InstallEntryConfig, InstallEntryOperation, RecordError};
use super::rules::{ensure_rules, retire_rules};
use super::settings::{SettingError, check_settings};
use super::sweep::{ORPHAN_GRACE_MS, plugin_accounts};
use crate::auth::enforcement::enforce_policies;
use crate::auth::request_authorization::permission_rules;
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::document_repository::GetConfigOperation;
use crate::driver::{DriverContext, drive};
use crate::realm::mutate_placement::is_management;
use crate::users::account_status::{AccountStatusConfig, AccountStatusOperation, REALM_ADMIN_ROLE};
use crate::users::service_account::check::{ServiceCheckConfig, ServiceCheckOperation};

#[derive(Debug, Error)]
pub enum InstallError {
    #[error("caller may not administer this group's plugins")]
    Unauthorized,
    #[error("no published version of this plugin matches")]
    NotPublished,
    #[error("no install with this id")]
    NotFound,
    #[error("the published manifest cannot be read: {0}")]
    Manifest(String),
    #[error(transparent)]
    Grant(#[from] GrantError),
    #[error("settings: {0}")]
    Setting(#[from] SettingError),
    #[error("the caller does not hold {0} and cannot grant it")]
    NotHeld(String),
    #[error("an install with this id exists for another request")]
    Changed,
    #[error("the install is {0}")]
    State(&'static str),
    #[error(transparent)]
    Record(#[from] RecordError),
    #[error("the install stopped at step {step}: {error}")]
    Step { step: &'static str, error: String },
    #[error("{0}")]
    Unavailable(String),
    #[error("{0}")]
    Binding(String),
}

/// What a group admin asks to install.
#[derive(Clone, Debug, PartialEq)]
pub struct InstallRequest {
    pub plugin: String,
    pub group_id: GroupId,
    /// The published digest to pin; `None` takes the newest published version.
    pub version: Option<String>,
    pub settings: Map<String, Value>,
    /// Narrowed grants by permission; a permission left out gets all of the group.
    pub grants: BTreeMap<String, Vec<String>>,
    /// The id a preview answered; installing with it again resumes that install.
    pub install_id: Option<Ulid>,
    /// `scope:name` of the secret per slot; a slot left out uses the same name in the group.
    pub secrets: BTreeMap<String, String>,
}

/// A checked request: what the summary shows and the install then writes.
#[derive(Clone, Debug, PartialEq)]
pub struct Prepared {
    pub scope: InstallScope,
    pub plugin: PublishedPlugin,
    pub manifest: Manifest,
    pub settings: String,
    pub grants: Grants,
    /// The secret each slot uses, checked where it lives.
    pub secrets: BTreeMap<String, SecretBinding>,
    /// What the admin should know, such as rules only a realm admin can add.
    pub warnings: Vec<String>,
    /// Digest of the checked request; a retry with the same id must match it.
    pub request: String,
}

/// Checks a request as a group admin and writes nothing; a preview answers with this. Without
/// a version, a retry of an existing install keeps its digest and a new install takes the newest.
pub async fn prepare_install(
    ctx: &DriverContext,
    auth: &AuthContext,
    realm_id: RealmId,
    request: &InstallRequest,
) -> Result<Prepared, InstallError> {
    check_admin(ctx, auth, request.group_id).await?;
    let config = read_config(ctx, realm_id).await?;
    let existing = (request.install_id).and_then(|id| {
        config
            .installs
            .iter()
            .find(|install| install.install_id == id)
    });
    let version = (request.version.as_deref()).or(existing.map(|install| install.digest.as_str()));
    let plugin = pick_version(&config, &request.plugin, version)?;
    let manifest = read_manifest(&plugin)?;
    check_supported(&manifest)?;
    let settings = check_settings(&manifest, &request.settings)?;
    let scope = InstallScope {
        realm_id,
        group_id: request.group_id,
        install_id: request.install_id.unwrap_or_else(Ulid::generate),
    };
    let grants = narrow_grants(&manifest, &scope, &request.grants)?;
    check_holder(ctx, auth, &scope, &grants).await?;
    // Node secrets are checked on the receiver, so it is chosen first.
    let receiver = (existing.map(|install| install.receiver))
        .or_else(|| pick_receiver(&config, realm_id, request.group_id));
    let place = BindPlace {
        group_id: request.group_id,
        local: ctx.net_handle.as_ref().map(|net| net.node_id()),
        receiver,
    };
    let bound = bind_slots(ctx, &config, place, &manifest, &request.secrets).await;
    let (secrets, warnings) = bound.map_err(|error| match error {
        BindError::Invalid(why) => InstallError::Binding(why),
        BindError::Unavailable(why) => InstallError::Unavailable(why),
    })?;
    let request = request_digest(&plugin.digest, &scope, &settings, (&grants, &secrets));
    Ok(Prepared {
        scope,
        plugin,
        manifest,
        settings,
        grants,
        secrets,
        warnings,
        request,
    })
}

/// Installs, or resumes the install with the request's id. Answers once it is settled; an
/// existing settled install is checked again before it is answered.
pub async fn install_plugin(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    request: &InstallRequest,
) -> Result<PluginInstall, InstallError> {
    let prepared = prepare_install(ctx, auth, actor.realm_id, request).await?;
    let id = prepared.scope.install_id;
    let _lock = lock_install(id).await;
    let config = read_config(ctx, actor.realm_id).await?;
    let record = match config
        .installs
        .iter()
        .find(|install| install.install_id == id)
    {
        Some(existing) if existing.request != prepared.request => {
            return Err(InstallError::Changed);
        }
        Some(existing) if existing.state == InstallState::Removing => {
            return Err(InstallError::State("being removed"));
        }
        Some(existing) if existing.state != InstallState::Preparing => {
            let paths = role_paths(&parse_grants(&existing.grants));
            let identity = Identity::of(existing);
            check_ready(ctx, &prepared.scope, identity, &paths)
                .await
                .map_err(|error| InstallError::Step {
                    step: "check",
                    error,
                })?;
            return Ok(existing.clone());
        }
        Some(existing) => existing.clone(),
        None => {
            let group_id = prepared.scope.group_id;
            let receiver = pick_receiver(&config, actor.realm_id, group_id).ok_or_else(|| {
                InstallError::Unavailable(
                    "no eligible server or management node holds the group's metadata".into(),
                )
            })?;
            let record = new_record(actor, &prepared, receiver);
            write_record(ctx, actor, auth, None, record, None).await?
        }
    };
    let audit = Some(AdminAuditAction::PluginInstalled);
    let settled = settle(ctx, actor, auth, record, audit).await?;
    info!(%id, plugin = %settled.plugin, version = %settled.version, "Installed a plugin");
    Ok(settled)
}

/// The record of a new install. The account and role ids are fresh, never taken from the
/// caller, so no existing user or role can be adopted or replaced.
fn new_record(actor: &Actor, prepared: &Prepared, receiver: NodeId) -> PluginInstall {
    let scope = &prepared.scope;
    PluginInstall {
        install_id: scope.install_id,
        group_id: scope.group_id,
        plugin: prepared.plugin.name.clone(),
        version: prepared.plugin.version.clone(),
        digest: prepared.plugin.digest.clone(),
        state: InstallState::Preparing,
        paused: false,
        settings: prepared.settings.clone(),
        grants: stored_grants(&prepared.grants),
        account: UserId::local(Ulid::generate(), scope.realm_id),
        role_id: Ulid::generate(),
        receiver,
        granted_by: actor.user_id,
        request: prepared.request.clone(),
        revision: 1,
        active_since_ms: None,
        failed: None,
        prerequisites: Vec::new(),
        activated_by: None,
        widened: None,
        secrets: prepared.secrets.clone(),
    }
}

/// Makes a `preparing` record's account and role match it, then writes its settled state:
/// blocked while something required is not granted, else paused or active. A failure undoes
/// the account and role, but only while the record is still the revision this call wrote.
pub(crate) async fn settle(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    record: PluginInstall,
    audit: Option<AdminAuditAction>,
) -> Result<PluginInstall, InstallError> {
    let config = read_config(ctx, actor.realm_id).await?;
    let manifest = read_manifest(&pick_version(
        &config,
        &record.plugin,
        Some(&record.digest),
    )?)?;
    let scope = scope_of(actor.realm_id, &record);
    let grants = parse_grants(&record.grants);
    let prerequisites = match make_ready(ctx, actor, auth, &scope, &record).await {
        Ok(prerequisites) => prerequisites,
        Err((step, error)) => {
            abandon(ctx, actor, auth, &record, step).await;
            return Err(InstallError::Step { step, error });
        }
    };
    if let Err(error) = ensure_rules(ctx, actor, &record, &manifest).await {
        abandon(ctx, actor, auth, &record, "rules").await;
        return Err(InstallError::Step {
            step: "rules",
            error,
        });
    }
    let mut next = record.clone();
    let (_, missing) = carry_grants(&grants, &manifest, &scope);
    next.state = match (missing.is_empty(), record.paused) {
        (false, _) => InstallState::Blocked,
        (true, true) => InstallState::Paused,
        (true, false) => InstallState::Active,
    };
    if next.state == InstallState::Active && next.active_since_ms.is_none() {
        next.active_since_ms = Some(unix_timestamp_millis());
        next.activated_by = Some(actor.node_id);
    }
    next.failed = None;
    next.prerequisites = prerequisites;
    next.revision += 1;
    let audit = audit.map(|action| AdminAuditRecord::for_install(action, actor.user_id, &next));
    write_record(ctx, actor, auth, Some(record.revision), next, audit).await
}

/// Undoes a failed preparation and records the failed step, unless another change of the
/// install moved on meanwhile; the undo writes themselves refuse a record that moved on.
async fn abandon(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    record: &PluginInstall,
    step: &'static str,
) {
    let id = record.install_id;
    let current = read_config(ctx, actor.realm_id)
        .await
        .ok()
        .and_then(|config| {
            config
                .installs
                .into_iter()
                .find(|install| install.install_id == id)
        });
    if current.as_ref() != Some(record) {
        warn!(%id, step, "A plugin install changed meanwhile; leaving it as it is");
        return;
    }
    let scope = scope_of(actor.realm_id, record);
    let undo = teardown(
        ctx,
        actor,
        auth,
        &scope,
        Identity::of(record),
        guard_of(record),
    );
    if let Err(error) = undo.await {
        warn!(%id, %error, "Could not undo a failed plugin install step");
    }
    // Only an install that never became active loses its rules here.
    retire_rules(ctx, actor, record).await;
    let mut failed = record.clone();
    failed.failed = Some(step.to_string());
    failed.revision += 1;
    if let Err(error) = write_record(ctx, actor, auth, Some(record.revision), failed, None).await {
        warn!(%id, %error, "Could not record the failed plugin install step");
    }
}

/// Steps 2 to 5 for `record`: the account, its role, then a check of both against current
/// state, each write refused once the record moved on. Secret use rules join as step 4 later.
async fn make_ready(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    scope: &InstallScope,
    record: &PluginInstall,
) -> Result<Vec<Prerequisite>, (&'static str, String)> {
    let (identity, guard) = (Identity::of(record), guard_of(record));
    let name = account_name(ctx, &record.plugin, scope.group_id).await;
    ensure_account(ctx, actor, auth, scope, identity, name, guard)
        .await
        .map_err(|error| ("account", error))?;
    let paths = role_paths(&parse_grants(&record.grants));
    ensure_role(ctx, actor, auth, scope, identity, &paths, guard)
        .await
        .map_err(|error| ("role", error))?;
    check_ready(ctx, scope, identity, &paths)
        .await
        .map_err(|error| ("check", error))
}

/// Repairs the account and role of every settled install from its current record on a
/// management node, as the approving admin, then sweeps accounts no record names any more.
pub(crate) async fn reconcile_installs(ctx: &DriverContext) -> Result<usize, InstallError> {
    let Some(net) = ctx.net_handle.as_ref() else {
        return Ok(0);
    };
    let (node_id, realm_id) = (net.node_id(), *net.realm_id());
    let config = read_config(ctx, realm_id).await?;
    if !is_management(&config, node_id) {
        return Ok(0);
    }
    let repaired = repair_settled(ctx, node_id, realm_id, &config.installs).await;
    let swept = sweep_orphans(ctx, node_id, &config, unix_timestamp_millis()).await;
    Ok(repaired
        + swept.unwrap_or_else(|error| {
            warn!(%error, "Could not sweep unnamed plugin accounts");
            0
        }))
}

/// Repairs each settled install; one that fails is logged and the others are still repaired.
pub(crate) async fn repair_settled(
    ctx: &DriverContext,
    node_id: aruna_core::NodeId,
    realm_id: RealmId,
    installs: &[PluginInstall],
) -> usize {
    let mut repaired = 0;
    for install in installs {
        if matches!(
            install.state,
            InstallState::Preparing | InstallState::Removing
        ) {
            continue;
        }
        match repair_install(ctx, node_id, realm_id, install).await {
            Ok(changed) => repaired += usize::from(changed),
            Err(error) => {
                warn!(install = %install.install_id, %error, "Could not repair a plugin install");
            }
        }
    }
    repaired
}

/// Makes one settled install's account and role match its record again; `true` if it changed.
pub(crate) async fn repair_install(
    ctx: &DriverContext,
    node_id: aruna_core::NodeId,
    realm_id: RealmId,
    install: &PluginInstall,
) -> Result<bool, InstallError> {
    let (scope, identity) = (scope_of(realm_id, install), Identity::of(install));
    let paths = role_paths(&parse_grants(&install.grants));
    if check_ready(ctx, &scope, identity, &paths).await.is_ok() {
        return Ok(false);
    }
    let step = |step| move |error| InstallError::Step { step, error };
    // An administrator's deactivation stands; repair restores only what the install stopped.
    let user = read_user(ctx, identity.account)
        .await
        .map_err(step("account"))?;
    if user.is_some_and(|user| user.is_deactivated() && !user.stopped_by_install()) {
        return Ok(false);
    }
    let actor = Actor {
        node_id,
        user_id: install.granted_by,
        realm_id,
    };
    let auth = AuthContext {
        user_id: install.granted_by,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let guard = guard_of(install);
    let name = account_name(ctx, &install.plugin, scope.group_id).await;
    ensure_account(ctx, &actor, &auth, &scope, identity, name, guard)
        .await
        .map_err(step("account"))?;
    ensure_role(ctx, &actor, &auth, &scope, identity, &paths, guard)
        .await
        .map_err(step("role"))?;
    info!(install = %install.install_id, "Repaired a plugin install's account and role");
    Ok(true)
}

pub(crate) fn scope_of(realm_id: RealmId, install: &PluginInstall) -> InstallScope {
    InstallScope {
        realm_id,
        group_id: install.group_id,
        install_id: install.install_id,
    }
}

/// Holds the lock of one install on this node, so its changes never overlap here.
pub(crate) async fn lock_install(install_id: Ulid) -> OwnedMutexGuard<()> {
    static LOCKS: LazyLock<std::sync::Mutex<HashMap<Ulid, Arc<Mutex<()>>>>> =
        LazyLock::new(Default::default);
    let lock = {
        let mut locks = LOCKS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        locks.entry(install_id).or_default().clone()
    };
    lock.lock_owned().await
}

/// Refuses a caller who is not an admin of the group.
pub(crate) async fn check_admin(
    ctx: &DriverContext,
    auth: &AuthContext,
    group_id: GroupId,
) -> Result<(), InstallError> {
    let check = ServiceCheckOperation::new(ServiceCheckConfig {
        auth_context: auth.clone(),
        group_id,
        target: None,
    });
    drive(check, ctx)
        .await
        .map(drop)
        .map_err(|_| InstallError::Unauthorized)
}

/// Refuses grants the caller does not hold now: each action they allow, with its own access and
/// operation `plugin.<permission>`, against roles, denies below a subtree, restrictions and
/// policies. Any policy reading the path refuses every subtree, even one guarded off for plugins.
pub(crate) async fn check_holder(
    ctx: &DriverContext,
    auth: &AuthContext,
    scope: &InstallScope,
    grants: &Grants,
) -> Result<(), InstallError> {
    let root = format!("/{}/g/{}", scope.realm_id, scope.group_id);
    let rules = permission_rules(ctx, auth, &root)
        .await
        .map_err(|error| InstallError::Unavailable(error.to_string()))?;
    let config = read_config(ctx, scope.realm_id).await?;
    let group = read_auth(ctx, scope.group_id)
        .await
        .map_err(InstallError::Unavailable)?;
    let group_policies = group.iter().flat_map(|auth_doc| auth_doc.policies.iter());
    let path_policies =
        (config.request_policies.iter().chain(group_policies)).any(|policy| policy.reads_path());
    for permission in Permission::ALL {
        for pattern in &patterns_for(grants, permission) {
            let subtree = pattern.strip_suffix("**");
            if subtree.is_some() && path_policies {
                let why = "a request policy reads the path, so grant exact paths";
                return Err(InstallError::NotHeld(format!(
                    "{permission} on {pattern} ({why})"
                )));
            }
            for access in accesses(permission) {
                let what = || format!("{permission} ({access}) on {pattern}");
                let held = match subtree {
                    Some(base) => rules.holds_below(base, access),
                    None => rules.allows(pattern, access),
                };
                if !held {
                    return Err(InstallError::NotHeld(what()));
                }
                let extras = PolicyRequestExtras::operation(format!("plugin.{permission}"));
                let request = policy_request_with(pattern, access, Some(auth), extras);
                if let Err(error) = enforce_policies(ctx, scope.realm_id, &request).await {
                    return Err(InstallError::NotHeld(format!("{} ({error})", what())));
                }
            }
        }
    }
    Ok(())
}

/// The published version with `digest`, or the newest one of `name`.
pub fn pick_version(
    config: &RealmConfigDocument,
    name: &str,
    digest: Option<&str>,
) -> Result<PublishedPlugin, InstallError> {
    let mut versions = config.plugins.iter().filter(|plugin| plugin.name == name);
    let found = match digest {
        Some(digest) => versions.find(|plugin| plugin.digest == digest),
        None => versions.max_by_key(|plugin| (plugin.published_at_ms, plugin.digest.clone())),
    };
    found.cloned().ok_or(InstallError::NotPublished)
}

pub fn read_manifest(plugin: &PublishedPlugin) -> Result<Manifest, InstallError> {
    serde_json::from_str(&plugin.manifest)
        .map_err(|error| InstallError::Manifest(error.to_string()))
}

fn request_digest(
    digest: &str,
    scope: &InstallScope,
    settings: &str,
    (grants, secrets): (&Grants, &BTreeMap<String, SecretBinding>),
) -> String {
    let group = scope.group_id.to_string();
    let mut request = serde_json::json!([digest, group, settings, stored_grants(grants)]);
    // Bindings join the digest only when there are any, so earlier installs keep theirs.
    if let (false, Some(parts)) = (secrets.is_empty(), request.as_array_mut()) {
        let bound: BTreeMap<&String, String> = (secrets.iter())
            .map(|(slot, binding)| (slot, binding.to_string()))
            .collect();
        parts.push(serde_json::json!(bound));
    }
    ArtifactDigest::of(request.to_string().as_bytes()).to_string()
}

pub async fn read_config(
    ctx: &DriverContext,
    realm_id: RealmId,
) -> Result<RealmConfigDocument, InstallError> {
    drive(GetConfigOperation::new(realm_id), ctx)
        .await
        .map_err(|error| InstallError::Unavailable(format!("realm config: {error:?}")))
}

/// Writes the record as the caller, expecting the revision it read.
pub(crate) async fn write_record(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    expected: Option<u64>,
    install: PluginInstall,
    audit: Option<AdminAuditRecord>,
) -> Result<PluginInstall, InstallError> {
    let write = InstallEntryConfig {
        actor: actor.clone(),
        auth_context: auth.clone(),
        install_id: install.install_id,
        group_id: install.group_id,
        expected,
        install: Some(install.clone()),
        audit,
    };
    drive(InstallEntryOperation::new(write), ctx).await?;
    Ok(install)
}

/// One grant line: `read`, or `write` when the plugin writes, and its granted patterns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionLine {
    pub permission: String,
    pub required: bool,
    pub granted: Vec<String>,
}

/// One declared service, its methods and whether the install may call it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionLine {
    pub name: String,
    pub url: String,
    pub methods: Vec<String>,
    pub required: bool,
    pub granted: bool,
    /// The plugin downloads, and the node adds a secret to requests of this service.
    pub stored: bool,
}

/// One secret slot: its kind, the secret it uses and whether the code reads the value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretLine {
    pub slot: String,
    pub kind: String,
    pub secret: String,
    pub value: bool,
}

/// What an admin approves: the version, the permissions and connections, and the triggers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    pub id: String,
    pub plugin: String,
    pub title: Option<String>,
    pub version: String,
    pub digest: String,
    pub group: String,
    pub permissions: Vec<PermissionLine>,
    pub connections: Vec<ConnectionLine>,
    pub triggers: Vec<String>,
    pub secrets: Vec<SecretLine>,
    pub warnings: Vec<String>,
}

/// The summary of a checked install request, before anything is written.
pub fn prepared_summary(prepared: &Prepared) -> Summary {
    let grants = stored_grants(&prepared.grants);
    let granted = (&grants, &prepared.secrets);
    summary_of(
        &prepared.manifest,
        &prepared.plugin.digest,
        &prepared.scope,
        granted,
    )
}

/// The summary of a stored install, from the record alone while the pinned manifest is missing.
pub fn installed_summary(config: &RealmConfigDocument, install: &PluginInstall) -> Summary {
    let manifest = pick_version(config, &install.plugin, Some(&install.digest))
        .ok()
        .and_then(|plugin| read_manifest(&plugin).ok());
    let scope = InstallScope {
        realm_id: config.realm_id,
        group_id: install.group_id,
        install_id: install.install_id,
    };
    match manifest {
        Some(manifest) => {
            let granted = (&install.grants, &install.secrets);
            summary_of(&manifest, &install.digest, &scope, granted)
        }
        None => bare_summary(install),
    }
}

/// The summary of `grants` against the manifest's requests.
fn summary_of(
    manifest: &Manifest,
    digest: &str,
    scope: &InstallScope,
    (grants, bound): (
        &BTreeMap<String, Vec<String>>,
        &BTreeMap<String, SecretBinding>,
    ),
) -> Summary {
    let granted = parse_grants(grants);
    let requested = [Grant::Read]
        .into_iter()
        .chain(writes(manifest).then_some(Grant::Write));
    let permissions = requested
        .map(|grant| PermissionLine {
            permission: grant.to_string(),
            required: true,
            granted: (granted.get(&grant).into_iter().flatten())
                .cloned()
                .collect(),
        })
        .collect();
    let sends = granted.get(&Grant::Services);
    let values = granted.get(&Grant::Secrets);
    let readable = |slot: &str| {
        let path = scope.secret(slot);
        values.is_some_and(|patterns| patterns.iter().any(|pattern| covers(pattern, &path)))
    };
    let downloads = (manifest.requested()).contains(&Permission::TransfersDownload);
    let connections = (manifest.connections.iter())
        .map(|(name, connection)| ConnectionLine {
            name: name.clone(),
            url: connection.url.clone(),
            methods: (connection.methods.iter())
                .map(|method| method.as_str().to_string())
                .collect(),
            required: !connection.optional,
            granted: sends.is_some_and(|paths| paths.contains(&scope.connection(name))),
            stored: downloads && connection.auth.is_some(),
        })
        .collect();
    Summary {
        id: scope.install_id.to_string(),
        plugin: manifest.name.clone(),
        title: manifest.title.clone(),
        version: manifest.version.clone(),
        digest: digest.to_string(),
        group: scope.group_id.to_string(),
        permissions,
        connections,
        triggers: manifest
            .triggers
            .keys()
            .map(|kind| kind.as_str().to_string())
            .collect(),
        secrets: (bound.iter())
            .map(|(slot, binding)| SecretLine {
                slot: slot.clone(),
                kind: (manifest.secrets.get(slot))
                    .and_then(|declared| vault_kind(declared.kind))
                    .map_or_else(String::new, |kind| kind.as_str().to_string()),
                secret: binding.to_string(),
                value: readable(slot),
            })
            .collect(),
        warnings: (bound.iter())
            .filter_map(|(slot, binding)| {
                let shared = matches!(binding.scope, BoundScope::Realm | BoundScope::Node);
                shared
                    .then(|| format!("a realm admin adds the use rule of `{binding}` for `{slot}`"))
            })
            .collect(),
    }
}

/// A summary from the record alone.
fn bare_summary(install: &PluginInstall) -> Summary {
    let permissions = (install.grants.iter())
        .map(|(permission, granted)| PermissionLine {
            permission: permission.clone(),
            required: false,
            granted: granted.clone(),
        })
        .collect();
    Summary {
        id: install.install_id.to_string(),
        plugin: install.plugin.clone(),
        title: None,
        version: install.version.clone(),
        digest: install.digest.clone(),
        group: install.group_id.to_string(),
        permissions,
        connections: Vec::new(),
        triggers: Vec::new(),
        secrets: Vec::new(),
        warnings: Vec::new(),
    }
}

/// When this node first saw each unnamed account; a restart starts the grace again.
static SEEN: LazyLock<std::sync::Mutex<HashMap<UserId, u64>>> = LazyLock::new(Default::default);

/// Deactivates the active plugin accounts the canonical records have not named for the grace
/// period, as a realm administrator. Returns how many it deactivated.
pub(crate) async fn sweep_orphans(
    ctx: &DriverContext,
    node_id: NodeId,
    config: &RealmConfigDocument,
    now_ms: u64,
) -> Result<usize, String> {
    let accounts = drive(plugin_accounts(), ctx)
        .await
        .map_err(|error| error.0)?;
    let mut due = Vec::new();
    {
        let mut seen = SEEN.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        for (install_id, user) in accounts {
            let named = (config.installs.iter())
                .any(|install| install.install_id == install_id && install.account == user.user_id);
            if named {
                seen.remove(&user.user_id);
                continue;
            }
            let first = *seen.entry(user.user_id).or_insert(now_ms);
            if now_ms.saturating_sub(first) >= ORPHAN_GRACE_MS {
                due.push((install_id, user.user_id));
            }
        }
    }
    if due.is_empty() {
        return Ok(0);
    }
    let Some(admin) = realm_admin(ctx, config).await? else {
        warn!("No realm administrator to deactivate unnamed plugin accounts with");
        return Ok(0);
    };
    let realm_id = config.realm_id;
    let mut swept = 0;
    for (install_id, account) in due {
        let status = AccountStatusOperation::new(AccountStatusConfig {
            actor: Actor {
                node_id,
                user_id: admin,
                realm_id,
            },
            auth_context: AuthContext {
                user_id: admin,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            target: account,
            active: false,
            now: unix_timestamp_secs(),
        })
        .guarded(InstallGuard::Unnamed {
            install_id,
            account,
        })
        .by_install();
        match drive(status, ctx).await {
            Ok(()) => {
                info!(%account, %install_id, "Deactivated a plugin account its install no longer names");
                swept += 1;
            }
            Err(error) => warn!(%account, %error, "Could not deactivate an unnamed plugin account"),
        }
    }
    Ok(swept)
}

/// The realm administrator with the smallest id, so every node acts as the same one.
async fn realm_admin(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
) -> Result<Option<UserId>, String> {
    let key = config.realm_id.as_bytes().to_vec();
    let realm = read_row(ctx, AUTH_KEYSPACE, key).await?;
    let realm = (realm.map(|bytes| RealmAuthorizationDocument::from_bytes(&bytes)))
        .transpose()
        .map_err(|error| error.to_string())?;
    let admins = (realm.iter().flat_map(|realm| realm.roles.values()))
        .filter(|role| role.name == REALM_ADMIN_ROLE)
        .flat_map(|role| role.assigned_users.iter().copied());
    Ok(admins.min())
}

#[cfg(test)]
#[path = "install_tests.rs"]
pub(crate) mod tests;
