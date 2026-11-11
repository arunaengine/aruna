//! Edits the use rules one plugin install holds on a group secret, inside the install's own
//! steps: ensure them for the current revision, or remove them once the install is gone.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::secret_registers::{SecretBody, SecretOwner};
use aruna_core::secrets::{SecretConsumer, SecretName, UseRule};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::GroupId;
use ulid::Ulid;

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

#[cfg(test)]
#[path = "rules_tests.rs"]
mod tests;
