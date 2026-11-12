//! Edits the use rules one plugin install holds on a group secret, inside the install's own
//! steps: ensure them for the current revision, or remove them once the install is gone.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::plugin_installs::{BoundScope, InstallState, PluginInstall};
use aruna_core::secret_registers::{SecretBody, SecretOwner};
use aruna_core::secrets::{Delivery, ResponseExposure, SecretConsumer, SecretName, UseRule};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::GroupId;
use aruna_plugins::manifest::Manifest;
use aruna_plugins::permission::Permission;
use aruna_plugins::trigger::TriggerKind;
use tracing::warn;
use ulid::Ulid;

use super::check::covers;
use super::grants::{InstallScope, parse_grants};
use super::install::read_config;
use crate::driver::{DriverContext, drive};
use crate::secrets::SecretChange;
use crate::secrets::owner::{EntryReader, OwnerChange, OwnerSecretError, OwnerSecretOperation};

/// Which install state a rule edit may still write for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuleGuard {
    /// The local install record is at this revision and not removing.
    Revision { install_id: Ulid, revision: u64 },
    /// The install is removing, failed before activation, or gone.
    Retired { install_id: Ulid },
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum RuleError {
    #[error("the group secret does not exist here")]
    Missing,
    #[error(transparent)]
    Secret(#[from] OwnerSecretError),
    #[error("the install record cannot be read: {0}")]
    Unreadable(String),
}

/// The install's consumer identity in a use rule.
pub fn consumer(install: &PluginInstall) -> SecretConsumer {
    SecretConsumer::Plugin {
        plugin_id: install.plugin.clone(),
        install_id: install.install_id,
    }
}

/// Sets this install's rules on the group secret `name` to `wanted`, keeping every other
/// consumer's rules; returns whether it wrote. A guard the local record no longer meets writes
/// nothing, and so does a list that is already in place.
pub async fn edit_rules(
    ctx: &DriverContext,
    actor: &Actor,
    (group_id, name): (GroupId, &SecretName),
    (consumer, wanted): (&SecretConsumer, Vec<UseRule>),
    guard: RuleGuard,
) -> Result<bool, RuleError> {
    if !guard_holds(ctx, actor, guard).await? {
        return Ok(false);
    }
    let owner = SecretOwner::Group(group_id);
    let mut reader = EntryReader::new(actor.realm_id, &owner, name);
    let mut effect = reader.start(None);
    loop {
        let aruna_core::effects::Effect::Storage(storage) = effect else {
            return Err(RuleError::Unreadable(
                "the reader asked for another effect".into(),
            ));
        };
        let event = ctx.storage_handle.send_storage_effect(storage).await;
        match reader.step(event, None)? {
            Some(next) => effect = next,
            None => break,
        }
    }
    let entry = reader.entry().filter(|entry| entry.complete());
    let entry = entry.ok_or(RuleError::Missing)?;
    let Some(SecretBody::Info(info)) = entry.info.as_ref().map(|info| &info.body) else {
        return Err(RuleError::Missing);
    };
    let mut uses: Vec<UseRule> = (info.uses.iter())
        .filter(|rule| rule.consumer != *consumer)
        .cloned()
        .collect();
    uses.extend(wanted);
    if uses == info.uses {
        return Ok(false);
    }
    let change = SecretChange::Replace {
        name: name.clone(),
        description: info.description.clone(),
        settings: info.settings.clone(),
        uses,
        expected: Some(entry.revision()),
    };
    let input = OwnerChange {
        realm_id: actor.realm_id,
        node_id: actor.node_id,
        owner,
        actor: actor.user_id,
        change,
        readable: false,
        now_ms: unix_timestamp_millis(),
        write_ids: [(); 3].map(|_| Ulid::generate()),
        txn_id: None,
    };
    drive(OwnerSecretOperation::new(input), ctx).await?;
    Ok(true)
}

async fn guard_holds(
    ctx: &DriverContext,
    actor: &Actor,
    guard: RuleGuard,
) -> Result<bool, RuleError> {
    let config = read_config(ctx, actor.realm_id).await;
    let config = config.map_err(|error| RuleError::Unreadable(error.to_string()))?;
    let find = |install_id| config.installs.iter().find(|i| i.install_id == install_id);
    Ok(match guard {
        RuleGuard::Revision {
            install_id,
            revision,
        } => find(install_id).is_some_and(|install| {
            install.revision == revision && install.state != InstallState::Removing
        }),
        RuleGuard::Retired { install_id } => find(install_id).is_none_or(|install| {
            matches!(
                install.state,
                InstallState::Removing | InstallState::Preparing
            )
        }),
    })
}

/// The rules the install needs on each group secret it binds, from its manifest, grants and
/// settings: `inject` for granted connections, `verify` for the webhook, `value` for a granted
/// `secrets.value`.
pub fn wanted_rules(
    install: &PluginInstall,
    manifest: &Manifest,
) -> Result<BTreeMap<SecretName, Vec<UseRule>>, String> {
    let settings: serde_json::Value =
        serde_json::from_str(&install.settings).map_err(|error| error.to_string())?;
    let scope = InstallScope {
        realm_id: install.account.realm_id,
        group_id: install.group_id,
        install_id: install.install_id,
    };
    let granted = parse_grants(&install.grants);
    let sends = granted.get(&Permission::ConnectionsSend);
    let group_bound = |slot: &String| {
        (install.secrets.get(slot)).filter(|binding| binding.scope == BoundScope::Group)
    };
    let mut wanted: BTreeMap<SecretName, Vec<UseRule>> = BTreeMap::new();
    let rule = |delivery| UseRule {
        consumer: consumer(install),
        delivery,
        origins: Vec::new(),
        paths: Vec::new(),
        methods: Vec::new(),
        response: ResponseExposure::Status,
        groups: Vec::new(),
    };
    for (name, connection) in &manifest.connections {
        let granted = sends.is_some_and(|paths| paths.contains(&scope.connection(name)));
        let Some(binding) = connection.auth.as_ref().and_then(group_bound) else {
            continue;
        };
        if !granted {
            continue;
        }
        let base = connection
            .base(&settings)
            .map_err(|why| format!("`{name}`: {why}"))?;
        let prefix = base.path().trim_end_matches('/');
        let fields = connection.response.fields();
        let response = match (connection.response.full(), fields.is_empty()) {
            (true, _) => ResponseExposure::Full,
            (false, true) => ResponseExposure::Status,
            (false, false) => ResponseExposure::Fields {
                pointers: fields.to_vec(),
                headers: Vec::new(),
            },
        };
        wanted
            .entry(binding.name.clone())
            .or_default()
            .push(UseRule {
                origins: vec![base.origin().ascii_serialization()],
                paths: (!prefix.is_empty())
                    .then(|| prefix.to_string())
                    .into_iter()
                    .collect(),
                methods: connection
                    .methods
                    .iter()
                    .map(|m| m.as_str().to_string())
                    .collect(),
                response,
                ..rule(Delivery::Inject)
            });
    }
    let hook = manifest.triggers.get(&TriggerKind::Webhook);
    if let Some(binding) = hook
        .and_then(|hook| hook.verify.as_ref())
        .and_then(group_bound)
    {
        wanted
            .entry(binding.name.clone())
            .or_default()
            .push(rule(Delivery::Verify));
    }
    let values = granted.get(&Permission::SecretsValue);
    for slot in manifest.secrets.keys() {
        let path = scope.secret(slot);
        let shown = values.is_some_and(|patterns| patterns.iter().any(|p| covers(p, &path)));
        if let Some(binding) = group_bound(slot).filter(|_| shown) {
            let rules = wanted.entry(binding.name.clone()).or_default();
            rules.push(rule(Delivery::Value));
        }
    }
    wanted.values_mut().for_each(widen_shared);
    Ok(wanted)
}

/// A use check takes the first matching rule, so every rule gets the exposure of each rule a
/// request could match with it, and exact duplicates go. The broker still narrows each answer to
/// its own connection's fields.
fn widen_shared(rules: &mut Vec<UseRule>) {
    let joined: Vec<ResponseExposure> = (rules.iter())
        .map(|rule| {
            (rules.iter())
                .filter(|other| overlaps(rule, other))
                .fold(rule.response.clone(), |joined, other| {
                    join(joined, &other.response)
                })
        })
        .collect();
    let mut kept: Vec<UseRule> = Vec::new();
    for (mut rule, response) in rules.drain(..).zip(joined) {
        rule.response = response;
        if !kept.contains(&rule) {
            kept.push(rule);
        }
    }
    *rules = kept;
}

/// Whether one request could match both inject rules.
fn overlaps(a: &UseRule, b: &UseRule) -> bool {
    let nested = |p: &String, q: &String| {
        let within = |path: &str, prefix: &str| {
            let rest = path.strip_prefix(prefix);
            rest.is_some_and(|rest| {
                rest.is_empty() || prefix.ends_with('/') || rest.starts_with('/')
            })
        };
        within(p, q) || within(q, p)
    };
    a.delivery == b.delivery
        && a.origins.iter().any(|origin| b.origins.contains(origin))
        && (a.methods.is_empty()
            || b.methods.is_empty()
            || a.methods.iter().any(|method| b.methods.contains(method)))
        && (a.paths.is_empty()
            || b.paths.is_empty()
            || a.paths.iter().any(|p| b.paths.iter().any(|q| nested(p, q))))
}

/// The exposure that shows what either shows.
fn join(joined: ResponseExposure, other: &ResponseExposure) -> ResponseExposure {
    use ResponseExposure::{Fields, Full, Status};
    match (joined, other) {
        (Full, _) | (_, Full) => Full,
        (Status, other) => other.clone(),
        (fields, Status) => fields,
        (
            Fields { pointers, headers },
            Fields {
                pointers: more,
                headers: added,
            },
        ) => {
            // Sorted, so rules with the same selectors and fields compare equal.
            let union = |mut list: Vec<String>, more: &[String]| {
                list.extend_from_slice(more);
                list.sort_unstable();
                list.dedup();
                list
            };
            Fields {
                pointers: union(pointers, more),
                headers: union(headers, added),
            }
        }
    }
}

/// Ensures the install's rules on every group secret it binds, for its current revision.
pub async fn ensure_rules(
    ctx: &DriverContext,
    actor: &Actor,
    install: &PluginInstall,
    manifest: &Manifest,
) -> Result<(), String> {
    let mut wanted = wanted_rules(install, manifest)?;
    let guard = RuleGuard::Revision {
        install_id: install.install_id,
        revision: install.revision,
    };
    // Slots may share a secret; each secret is edited once with all of its rules.
    let names: BTreeSet<&SecretName> = (install.secrets.values())
        .filter(|binding| binding.scope == BoundScope::Group)
        .map(|binding| &binding.name)
        .collect();
    for name in names {
        let rules = wanted.remove(name).unwrap_or_default();
        let target = (install.group_id, name);
        let edited = edit_rules(ctx, actor, target, (&consumer(install), rules), guard).await;
        edited.map_err(|error| format!("`group:{name}`: {error}"))?;
    }
    Ok(())
}

/// Removes the rules of a removing, abandoned or removed install from its group secrets.
pub async fn retire_rules(ctx: &DriverContext, actor: &Actor, install: &PluginInstall) {
    let guard = RuleGuard::Retired {
        install_id: install.install_id,
    };
    let ours = consumer(install);
    for binding in install.secrets.values() {
        if binding.scope != BoundScope::Group {
            continue;
        }
        let target = (install.group_id, &binding.name);
        let edited = edit_rules(ctx, actor, target, (&ours, Vec::new()), guard);
        match edited.await {
            Ok(_) | Err(RuleError::Missing) => {}
            Err(error) => warn!(%error, %binding, "Could not remove a plugin install's use rules"),
        }
    }
}

#[cfg(test)]
#[path = "rules_tests.rs"]
mod tests;
