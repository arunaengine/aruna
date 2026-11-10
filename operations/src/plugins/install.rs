//! Installs a published plugin into a group. The record is written `preparing` first, then the
//! account and its role are made right, and activation is the last write. Every step checks the
//! current state, so a retry or a restart with the same install id resumes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::UserId;
use aruna_core::plugin_installs::{InstallState, PluginInstall, Prerequisite};
use aruna_core::plugins::PublishedPlugin;
use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission as Access};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::GroupId;
use aruna_plugins::artifact::ArtifactDigest;
use aruna_plugins::manifest::Manifest;
use serde_json::{Map, Value};
use std::collections::HashMap;
use thiserror::Error;
use tracing::{info, warn};
use ulid::Ulid;

use super::account::{account_name, check_ready, ensure_account, ensure_role, teardown};
use super::grants::{
    GrantError, Grants, InstallScope, check_supported, narrow_grants, role_paths, stored_grants,
};
use super::records::{InstallEntryConfig, InstallEntryOperation, RecordError};
use super::settings::{SettingError, check_settings};
use crate::auth::permission_rules::permission_rules;
use crate::auth::request_policy::{PolicyRequestExtras, enforce_policies, policy_request_with};
use crate::driver::{DriverContext, drive};
use crate::realm::get_config::GetConfigOperation;
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
    #[error(transparent)]
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
}

/// A checked request: what the summary shows and the install then writes.
#[derive(Clone, Debug, PartialEq)]
pub struct Prepared {
    pub scope: InstallScope,
    pub plugin: PublishedPlugin,
    pub manifest: Manifest,
    pub settings: String,
    pub grants: Grants,
    /// Digest of the checked request; a retry with the same id must match it.
    pub request: String,
}

/// Checks a request as a group admin and writes nothing; a preview answers with this.
pub async fn prepare_install(
    ctx: &DriverContext,
    auth: &AuthContext,
    realm_id: RealmId,
    request: &InstallRequest,
) -> Result<Prepared, InstallError> {
    check_admin(ctx, auth, request.group_id).await?;
    let config = read_config(ctx, realm_id).await?;
    let plugin = pick_version(&config, &request.plugin, request.version.as_deref())?;
    let manifest = read_manifest(&plugin)?;
    check_supported(&manifest)?;
    let settings = check_settings(&manifest, &request.settings)?;
    let scope = InstallScope {
        realm_id,
        group_id: request.group_id,
        install_id: request.install_id.unwrap_or_else(Ulid::generate),
    };
    let grants = narrow_grants(&manifest, &scope, &request.grants)?;
    check_holder(ctx, auth, &scope, &role_paths(&grants)).await?;
    let request = request_digest(&plugin.digest, &scope, &settings, &grants);
    Ok(Prepared {
        scope,
        plugin,
        manifest,
        settings,
        grants,
        request,
    })
}

/// Installs, or resumes the install with the request's id. Answers once it is active.
pub async fn install_plugin(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    request: &InstallRequest,
) -> Result<PluginInstall, InstallError> {
    let prepared = prepare_install(ctx, auth, actor.realm_id, request).await?;
    let config = read_config(ctx, actor.realm_id).await?;
    let id = prepared.scope.install_id;
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
        Some(existing) if existing.state != InstallState::Preparing => return Ok(existing.clone()),
        Some(existing) => existing.clone(),
        None => write_record(ctx, actor, auth, None, new_record(actor, &prepared), None).await?,
    };
    let prerequisites = match make_ready(ctx, actor, auth, &prepared, record.account).await {
        Ok(prerequisites) => prerequisites,
        Err((step, error)) => {
            if let Err(error) = teardown(ctx, actor, auth, &prepared.scope).await {
                warn!(%id, %error, "Could not undo a failed plugin install");
            }
            let mut failed = record.clone();
            failed.failed = Some(step.to_string());
            failed.revision += 1;
            if let Err(error) =
                write_record(ctx, actor, auth, Some(record.revision), failed, None).await
            {
                warn!(%id, %error, "Could not record the failed plugin install step");
            }
            return Err(InstallError::Step { step, error });
        }
    };
    let mut active = record.clone();
    active.state = InstallState::Active;
    active.active_since_ms = Some(unix_timestamp_millis());
    active.failed = None;
    active.prerequisites = prerequisites;
    active.granted_by = actor.user_id;
    active.revision += 1;
    let audit =
        AdminAuditRecord::for_install(AdminAuditAction::PluginInstalled, actor.user_id, &active);
    let active = write_record(ctx, actor, auth, Some(record.revision), active, Some(audit)).await?;
    info!(%id, plugin = %active.plugin, version = %active.version, "Installed a plugin");
    Ok(active)
}

fn new_record(actor: &Actor, prepared: &Prepared) -> PluginInstall {
    let scope = &prepared.scope;
    PluginInstall {
        install_id: scope.install_id,
        group_id: scope.group_id,
        plugin: prepared.plugin.name.clone(),
        version: prepared.plugin.version.clone(),
        digest: prepared.plugin.digest.clone(),
        state: InstallState::Preparing,
        settings: prepared.settings.clone(),
        grants: stored_grants(&prepared.grants),
        account: UserId::local(scope.install_id, scope.realm_id),
        role_id: scope.install_id,
        granted_by: actor.user_id,
        request: prepared.request.clone(),
        revision: 1,
        active_since_ms: None,
        failed: None,
        prerequisites: Vec::new(),
    }
}

/// Steps 2 to 5: the account, its role, then a check of both against current state. The
/// secret use rules join as step 4 once plugins with secrets can be installed.
async fn make_ready(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    prepared: &Prepared,
    account: UserId,
) -> Result<Vec<Prerequisite>, (&'static str, String)> {
    let scope = &prepared.scope;
    let name = account_name(ctx, &prepared.plugin.name, scope.group_id).await;
    ensure_account(ctx, actor, auth, scope, account, name)
        .await
        .map_err(|error| ("account", error))?;
    let paths = role_paths(&prepared.grants);
    ensure_role(ctx, actor, auth, scope, account, &paths)
        .await
        .map_err(|error| ("role", error))?;
    check_ready(ctx, scope, account, &paths)
        .await
        .map_err(|error| ("check", error))
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

/// Refuses grants the caller does not hold now: every path with its access, including denies
/// below a granted subtree, the session's own restrictions and the request policies.
pub(crate) async fn check_holder(
    ctx: &DriverContext,
    auth: &AuthContext,
    scope: &InstallScope,
    paths: &HashMap<String, Access>,
) -> Result<(), InstallError> {
    let root = format!("/{}/g/{}", scope.realm_id, scope.group_id);
    let rules = permission_rules(ctx, auth, &root)
        .await
        .map_err(|error| InstallError::Unavailable(error.to_string()))?;
    for (pattern, access) in paths {
        let held = match pattern.strip_suffix("**") {
            Some(base) => rules.holds_below(base, access),
            None => rules.allows(pattern, access),
        };
        let what = || format!("{access} on {pattern}");
        if !held {
            return Err(InstallError::NotHeld(what()));
        }
        let request = policy_request_with(pattern, access, Some(auth), PolicyRequestExtras::rest());
        if let Err(error) = enforce_policies(ctx, scope.realm_id, &request).await {
            return Err(InstallError::NotHeld(format!("{} ({error})", what())));
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

fn request_digest(digest: &str, scope: &InstallScope, settings: &str, grants: &Grants) -> String {
    let group = scope.group_id.to_string();
    let request = serde_json::json!([digest, group, settings, stored_grants(grants)]);
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
    let write = InstallEntryOperation::new(InstallEntryConfig {
        actor: actor.clone(),
        auth_context: auth.clone(),
        install_id: install.install_id,
        group_id: install.group_id,
        expected,
        install: Some(install.clone()),
        audit,
    });
    drive(write, ctx).await?;
    Ok(install)
}
