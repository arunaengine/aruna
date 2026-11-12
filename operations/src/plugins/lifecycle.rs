//! Changes an active install: grants, version, pause, resume, removal. A role change first writes
//! an audited `preparing` record, so the role is never wider than an audited record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::plugin_installs::{InstallState, PluginInstall, SecretBinding, Widening};
use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::time::unix_timestamp_millis;
use aruna_plugins::manifest::Manifest;
use aruna_plugins::trigger::TriggerKind;
use serde_json::{Map, Value};
use tracing::info;
use ulid::Ulid;

use super::account::{Identity, guard_of, teardown};
use super::bound::{BindError, BindPlace, bind_slots};
use super::check::covers;
use super::grants::{
    Grants, carry_grants, check_supported, narrow_grants, parse_grants, stored_grants,
};
use super::install::{
    InstallError, check_admin, check_holder, lock_install, pick_version, read_config,
    read_manifest, scope_of, settle, write_record,
};
use super::records::{InstallEntryConfig, InstallEntryOperation};
use super::rules::retire_rules;
use super::settings::check_settings;
use crate::driver::{DriverContext, drive};

/// Replaces the grants: a permission left out gets all of the group, an empty list removes an
/// optional one. The caller must hold every grant. It also retries a failed earlier change.
pub async fn set_grants(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    install_id: Ulid,
    choices: &BTreeMap<String, Vec<String>>,
) -> Result<PluginInstall, InstallError> {
    let _lock = lock_install(install_id).await;
    let (config, current) = load(ctx, actor, auth, install_id).await?;
    changeable(&current)?;
    let plugin = pick_version(&config, &current.plugin, Some(&current.digest))?;
    let manifest = read_manifest(&plugin)?;
    let scope = scope_of(actor.realm_id, &current);
    let grants = narrow_grants(&manifest, &scope, choices)?;
    check_holder(ctx, auth, &scope, &grants).await?;
    let mut next = preparing(&current, &grants);
    next.granted_by = actor.user_id;
    widen(&mut next, &current, (&manifest, &manifest), actor);
    let action = AdminAuditAction::PluginGrantsChanged;
    let next = commit(ctx, actor, auth, &current, next, action).await?;
    settle(ctx, actor, auth, next, None).await
}

/// Moves the install to another published version of the same plugin. It keeps the grants the
/// new version still requests, which the caller must hold now, and grants nothing new; it is
/// blocked while something it requires is not granted. Returns the record and what is missing.
pub async fn update_install(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    install_id: Ulid,
    digest: &str,
    settings: Option<&Map<String, Value>>,
) -> Result<(PluginInstall, Vec<String>), InstallError> {
    let _lock = lock_install(install_id).await;
    let (config, current) = load(ctx, actor, auth, install_id).await?;
    changeable(&current)?;
    let plugin = pick_version(&config, &current.plugin, Some(digest))?;
    let manifest = read_manifest(&plugin)?;
    check_supported(&manifest)?;
    let stored: Map<String, Value> = serde_json::from_str(&current.settings).unwrap_or_default();
    let settings = check_settings(&manifest, settings.unwrap_or(&stored))?;
    let scope = scope_of(actor.realm_id, &current);
    let (kept, missing) = carry_grants(&parse_grants(&current.grants), &manifest, &scope);
    check_holder(ctx, auth, &scope, &kept).await?;
    let secrets = rebind(ctx, (&config, &current), &manifest).await?;
    let mut next = preparing(&current, &kept);
    next.secrets = secrets;
    next.version = plugin.version;
    next.digest = plugin.digest;
    next.settings = settings;
    let before = read_manifest(&pick_version(
        &config,
        &current.plugin,
        Some(&current.digest),
    )?)?;
    widen(&mut next, &current, (&before, &manifest), actor);
    let action = AdminAuditAction::PluginUpdated;
    let next = commit(ctx, actor, auth, &current, next, action).await?;
    Ok((settle(ctx, actor, auth, next, None).await?, missing))
}

/// Stops runs and keeps everything else. A blocked install stays blocked, and paused.
pub async fn pause_install(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    install_id: Ulid,
) -> Result<PluginInstall, InstallError> {
    let _lock = lock_install(install_id).await;
    let (_, current) = load(ctx, actor, auth, install_id).await?;
    let state = match current.state {
        InstallState::Active => InstallState::Paused,
        InstallState::Blocked if !current.paused => InstallState::Blocked,
        state => return Err(InstallError::State(state.as_str())),
    };
    let mut next = current.clone();
    next.paused = true;
    next.state = state;
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

/// Lifts a pause. The install becomes active only while everything it requires is granted,
/// else it stays blocked; events from the pause are not replayed.
pub async fn resume_install(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    install_id: Ulid,
) -> Result<PluginInstall, InstallError> {
    let _lock = lock_install(install_id).await;
    let (config, current) = load(ctx, actor, auth, install_id).await?;
    let paused = matches!(current.state, InstallState::Paused | InstallState::Blocked);
    if !current.paused || !paused {
        return Err(InstallError::State(current.state.as_str()));
    }
    let plugin = pick_version(&config, &current.plugin, Some(&current.digest))?;
    let manifest = read_manifest(&plugin)?;
    let scope = scope_of(actor.realm_id, &current);
    let (_, missing) = carry_grants(&parse_grants(&current.grants), &manifest, &scope);
    let mut next = current.clone();
    next.paused = false;
    next.state = match missing.is_empty() {
        true => InstallState::Active,
        false => InstallState::Blocked,
    };
    if next.state == InstallState::Active {
        next.active_since_ms = Some(unix_timestamp_millis());
        next.activated_by = Some(actor.node_id);
    }
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
    let _lock = lock_install(install_id).await;
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
    let scope = scope_of(actor.realm_id, &removing);
    teardown(
        ctx,
        actor,
        auth,
        &scope,
        Identity::of(&removing),
        guard_of(&removing),
    )
    .await
    .map_err(|error| InstallError::Step {
        step: "remove",
        error,
    })?;
    retire_rules(ctx, actor, &removing).await;
    let action = AdminAuditAction::PluginRemoved;
    let audit = AdminAuditRecord::for_install(action, actor.user_id, &removing);
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

/// Refuses changes while the install is removed or not installed yet. A `preparing` install
/// that was active before failed an earlier change, which a new change retries.
fn changeable(install: &PluginInstall) -> Result<(), InstallError> {
    match install.state {
        InstallState::Removing => Err(InstallError::State(install.state.as_str())),
        InstallState::Preparing if install.active_since_ms.is_none() => {
            Err(InstallError::State(install.state.as_str()))
        }
        _ => Ok(()),
    }
}

/// The next record of a role change: inactive, with the new grants, until it is settled.
/// The bindings of a new version's slots: a kept slot's binding checked again where it lives, an
/// added slot bound to the group secret of its name, and a dropped slot's binding gone.
async fn rebind(
    ctx: &DriverContext,
    (config, current): (&RealmConfigDocument, &PluginInstall),
    manifest: &Manifest,
) -> Result<BTreeMap<String, SecretBinding>, InstallError> {
    let kept: BTreeMap<String, String> = (current.secrets.iter())
        .filter(|(slot, _)| manifest.secrets.contains_key(*slot))
        .map(|(slot, binding)| (slot.clone(), binding.to_string()))
        .collect();
    let place = BindPlace {
        group_id: current.group_id,
        local: ctx.net_handle.as_ref().map(|net| net.node_id()),
        receiver: Some(current.receiver),
    };
    let bound = bind_slots(ctx, config, place, manifest, &kept).await;
    bound
        .map(|(secrets, _)| secrets)
        .map_err(|error| match error {
            BindError::Invalid(why) => InstallError::Binding(why),
            BindError::Unavailable(why) => InstallError::Unavailable(why),
        })
}

fn preparing(current: &PluginInstall, grants: &Grants) -> PluginInstall {
    let mut next = current.clone();
    next.state = InstallState::Preparing;
    next.grants = stored_grants(grants);
    next.failed = None;
    next.revision += 1;
    next
}

/// The event coverage of an install: per event trigger kind, the grant patterns its permission
/// needs. A kind without a needed permission covers everything of the group.
pub(crate) fn coverage(
    manifest: &Manifest,
    grants: &Grants,
) -> BTreeMap<TriggerKind, BTreeSet<String>> {
    (manifest.triggers.keys())
        .filter(|kind| kind.needs().is_some())
        .map(|kind| {
            let needed = kind.needs().and_then(|permission| grants.get(&permission));
            (*kind, needed.cloned().unwrap_or_default())
        })
        .collect()
}

/// Records a widening when the next record matches events the current one did not: a new kind,
/// or a pattern the current grants of that kind did not hold.
fn widen(
    next: &mut PluginInstall,
    current: &PluginInstall,
    manifests: (&Manifest, &Manifest),
    actor: &Actor,
) {
    let before = coverage(manifests.0, &parse_grants(&current.grants));
    let after = coverage(manifests.1, &parse_grants(&next.grants));
    let grown = (after.iter()).any(|(kind, patterns)| {
        let held = before.get(kind);
        (patterns.iter()).any(|pattern| held.is_none_or(|held| !held_covers(held, pattern)))
    });
    if grown && current.state == InstallState::Active {
        next.widened = Some(Widening {
            at_ms: unix_timestamp_millis(),
            by: actor.node_id,
            extends: current.revision,
        });
    }
}

/// Whether a held pattern reaches every path of `pattern`: a subtree only by a subtree.
pub(crate) fn held_covers(held: &BTreeSet<String>, pattern: &str) -> bool {
    let (base, subtree) = match pattern.strip_suffix("/**") {
        Some(base) => (base, true),
        None => (pattern, false),
    };
    (held.iter()).any(|q| (!subtree || q.ends_with("/**")) && covers(q, base))
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

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
