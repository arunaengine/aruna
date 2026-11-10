//! Changes an install after it is active: its grants, its version, pause and resume, removal.
//! Each change is a group admin's, is audited, and expects the record revision it read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::plugins::PublishedPlugin;
use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::time::unix_timestamp_millis;
use aruna_plugins::manifest::Manifest;
use serde_json::{Map, Value};
use tracing::info;
use ulid::Ulid;

use super::account::{check_ready, ensure_role, teardown};
use super::grants::{
    Grants, InstallScope, carry_grants, check_supported, narrow_grants, parse_grants, role_paths,
    stored_grants,
};
use super::install::{
    InstallError, check_admin, check_holder, pick_version, read_config, read_manifest, write_record,
};
use super::records::{InstallEntryConfig, InstallEntryOperation};
use super::settings::check_settings;
use crate::driver::{DriverContext, drive};

/// Replaces the grants: a permission left out gets all of the group, an empty list removes an
/// optional one. A blocked install becomes active, since the admin approved what it needs.
pub async fn set_grants(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    install_id: Ulid,
    choices: &BTreeMap<String, Vec<String>>,
) -> Result<PluginInstall, InstallError> {
    let (config, current) = load(ctx, actor, auth, install_id).await?;
    settled(&current)?;
    let manifest = read_manifest(&pinned(&config, &current)?)?;
    let scope = scope_of(actor, &current);
    let grants = narrow_grants(&manifest, &scope, choices)?;
    check_holder(ctx, auth, &scope, &role_paths(&grants)).await?;
    let mut next = regrant(ctx, actor, auth, &scope, &current, &grants).await?;
    next.granted_by = actor.user_id;
    if next.state == InstallState::Blocked {
        next.state = InstallState::Active;
        next.active_since_ms = Some(unix_timestamp_millis());
    }
    commit(
        ctx,
        actor,
        auth,
        &current,
        next,
        AdminAuditAction::PluginGrantsChanged,
    )
    .await
}

/// Moves the install to another published version of the same plugin. It keeps the grants the
/// new version still requests and grants nothing new; it is blocked while something it
/// requires is not granted. Returns the record and what still needs approval.
pub async fn update_install(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    install_id: Ulid,
    digest: &str,
    settings: Option<&Map<String, Value>>,
) -> Result<(PluginInstall, Vec<String>), InstallError> {
    let (config, current) = load(ctx, actor, auth, install_id).await?;
    settled(&current)?;
    let plugin = pick_version(&config, &current.plugin, Some(digest))?;
    let manifest: Manifest = read_manifest(&plugin)?;
    check_supported(&manifest)?;
    let stored: Map<String, Value> = serde_json::from_str(&current.settings).unwrap_or_default();
    let settings = check_settings(&manifest, settings.unwrap_or(&stored))?;
    let scope = scope_of(actor, &current);
    let (kept, missing) = carry_grants(&parse_grants(&current.grants), &manifest, &scope);
    let mut next = regrant(ctx, actor, auth, &scope, &current, &kept).await?;
    next.version = plugin.version;
    next.digest = plugin.digest;
    next.settings = settings;
    next.state = match (missing.is_empty(), current.state) {
        (false, _) => InstallState::Blocked,
        (true, InstallState::Blocked) => InstallState::Active,
        (true, state) => state,
    };
    let record = commit(
        ctx,
        actor,
        auth,
        &current,
        next,
        AdminAuditAction::PluginUpdated,
    )
    .await?;
    Ok((record, missing))
}

/// Stops runs and keeps everything else; a paused install passes no per call check.
pub async fn pause_install(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    install_id: Ulid,
) -> Result<PluginInstall, InstallError> {
    let (_, current) = load(ctx, actor, auth, install_id).await?;
    if current.state != InstallState::Active {
        return Err(InstallError::State(current.state.as_str()));
    }
    let mut next = current.clone();
    next.state = InstallState::Paused;
    next.revision += 1;
    commit(
        ctx,
        actor,
        auth,
        &current,
        next,
        AdminAuditAction::PluginPaused,
    )
    .await
}

/// Makes a paused install active again; events before now are not replayed.
pub async fn resume_install(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    install_id: Ulid,
) -> Result<PluginInstall, InstallError> {
    let (_, current) = load(ctx, actor, auth, install_id).await?;
    if current.state != InstallState::Paused {
        return Err(InstallError::State(current.state.as_str()));
    }
    let mut next = current.clone();
    next.state = InstallState::Active;
    next.active_since_ms = Some(unix_timestamp_millis());
    next.revision += 1;
    commit(
        ctx,
        actor,
        auth,
        &current,
        next,
        AdminAuditAction::PluginResumed,
    )
    .await
}

/// Marks the install `removing`, deactivates its account, removes its role, then deletes the
/// record. A retry after a failure resumes from `removing`. The account id stays in audit rows.
pub async fn remove_install(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    install_id: Ulid,
) -> Result<PluginInstall, InstallError> {
    let (_, current) = load(ctx, actor, auth, install_id).await?;
    let removing = match current.state {
        InstallState::Removing => current,
        _ => {
            let mut next = current.clone();
            next.state = InstallState::Removing;
            next.revision += 1;
            write_record(ctx, actor, auth, Some(current.revision), next, None).await?
        }
    };
    let scope = scope_of(actor, &removing);
    teardown(ctx, actor, auth, &scope)
        .await
        .map_err(|error| InstallError::Step {
            step: "remove",
            error,
        })?;
    let audit =
        AdminAuditRecord::for_install(AdminAuditAction::PluginRemoved, actor.user_id, &removing);
    let delete = InstallEntryOperation::new(InstallEntryConfig {
        actor: actor.clone(),
        auth_context: auth.clone(),
        install_id,
        group_id: removing.group_id,
        expected: Some(removing.revision),
        install: None,
        audit: Some(audit),
    });
    drive(delete, ctx).await?;
    info!(%install_id, plugin = %removing.plugin, "Removed a plugin install");
    Ok(removing)
}

/// The install as the realm config holds it now, after the caller proved group admin.
async fn load(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    install_id: Ulid,
) -> Result<(RealmConfigDocument, PluginInstall), InstallError> {
    let config = read_config(ctx, actor.realm_id).await?;
    let install = (config.installs.iter())
        .find(|install| install.install_id == install_id)
        .cloned()
        .ok_or(InstallError::NotFound)?;
    check_admin(ctx, auth, install.group_id).await?;
    Ok((config, install))
}

/// Refuses changes to an install that is not prepared yet or being removed.
fn settled(install: &PluginInstall) -> Result<(), InstallError> {
    match install.state {
        InstallState::Preparing | InstallState::Removing => {
            Err(InstallError::State(install.state.as_str()))
        }
        InstallState::Active | InstallState::Paused | InstallState::Blocked => Ok(()),
    }
}

fn pinned(
    config: &RealmConfigDocument,
    install: &PluginInstall,
) -> Result<PublishedPlugin, InstallError> {
    pick_version(config, &install.plugin, Some(&install.digest))
}

fn scope_of(actor: &Actor, install: &PluginInstall) -> InstallScope {
    InstallScope {
        realm_id: actor.realm_id,
        group_id: install.group_id,
        install_id: install.install_id,
    }
}

/// Sets the role to `grants` and returns the next record with them and the new prerequisites.
async fn regrant(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    scope: &InstallScope,
    current: &PluginInstall,
    grants: &Grants,
) -> Result<PluginInstall, InstallError> {
    let step = |step| move |error| InstallError::Step { step, error };
    let paths = role_paths(grants);
    ensure_role(ctx, actor, auth, scope, current.account, &paths)
        .await
        .map_err(step("role"))?;
    let prerequisites = check_ready(ctx, scope, current.account, &paths)
        .await
        .map_err(step("check"))?;
    let mut next = current.clone();
    next.grants = stored_grants(grants);
    next.prerequisites = prerequisites;
    next.revision += 1;
    Ok(next)
}

async fn commit(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    current: &PluginInstall,
    next: PluginInstall,
    action: AdminAuditAction,
) -> Result<PluginInstall, InstallError> {
    let audit = AdminAuditRecord::for_install(action, actor.user_id, &next);
    write_record(ctx, actor, auth, Some(current.revision), next, Some(audit)).await
}
