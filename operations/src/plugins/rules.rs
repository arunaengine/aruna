//! Edits the use rules one plugin install holds on a group secret, inside the install's own
//! steps: ensure them for the current revision, or remove them once the install is gone.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::{BoundScope, InstallState, PluginInstall, SecretBinding};
use aruna_core::secret_registers::{SecretBody, SecretEntry, SecretOwner};
use aruna_core::secrets::{Delivery, ResponseExposure, SecretConsumer, SecretName, UseRule};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::types::GroupId;
use aruna_plugins::manifest::Manifest;
use aruna_plugins::permission::Grant;
use aruna_plugins::trigger::TriggerKind;
use tracing::warn;
use ulid::Ulid;

use super::entry::{EntryError, ReadEntry};
use super::grants::covers;
use super::grants::{InstallScope, parse_grants};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::secrets::SecretChange;
use crate::secrets::owner::{OwnerChange, OwnerSecretError, OwnerSecretOperation};

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
    #[error(transparent)]
    Read(#[from] EntryError),
    #[error("the install record cannot be read: {0}")]
    Unreadable(String),
}

impl From<IoError> for RuleError {
    fn from(error: IoError) -> Self {
        Self::Unreadable(error.to_string())
    }
}

/// The install's consumer identity in a use rule.
pub fn consumer(install: &PluginInstall) -> SecretConsumer {
    SecretConsumer::Plugin {
        plugin_id: install.plugin.clone(),
        install_id: install.install_id,
    }
}

/// When a rule edit happens and the ids of the writes it may make.
pub(crate) type RuleStamp = (u64, [Ulid; 3]);

#[derive(Debug, PartialEq)]
enum EditPhase {
    Guard,
    Entry,
    Writing,
}

/// Sets this install's rules on the group secret `name` to `wanted`, keeping every other
/// consumer's rules; answers whether it wrote. A guard the local record no longer meets writes
/// nothing, and so does a list that is already in place.
#[derive(Debug, PartialEq)]
pub(crate) struct EditRules {
    actor: Actor,
    group_id: GroupId,
    name: SecretName,
    consumer: SecretConsumer,
    wanted: Vec<UseRule>,
    guard: RuleGuard,
    stamp: RuleStamp,
    phase: EditPhase,
}

impl EditRules {
    pub(crate) fn new(
        actor: &Actor,
        (group_id, name): (GroupId, &SecretName),
        (consumer, wanted): (&SecretConsumer, Vec<UseRule>),
        guard: RuleGuard,
        stamp: RuleStamp,
    ) -> Self {
        Self {
            actor: actor.clone(),
            group_id,
            name: name.clone(),
            consumer: consumer.clone(),
            wanted,
            guard,
            stamp,
            phase: EditPhase::Guard,
        }
    }

    fn guard_holds(&self, config: &RealmConfigDocument) -> bool {
        let find = |install_id| config.installs.iter().find(|i| i.install_id == install_id);
        match self.guard {
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
        }
    }

    fn owner(&self) -> SecretOwner {
        SecretOwner::Group(self.group_id)
    }

    fn change(&mut self, entry: SecretEntry) -> Result<Next<bool>, RuleError> {
        let Some(SecretBody::Info(info)) = entry.info.as_ref().map(|info| &info.body) else {
            return Err(RuleError::Missing);
        };
        let mut uses: Vec<UseRule> = (info.uses.iter())
            .filter(|rule| rule.consumer != self.consumer)
            .cloned()
            .collect();
        uses.append(&mut self.wanted);
        if uses == info.uses {
            return Ok(Next::Done(false));
        }
        let change = SecretChange::Replace {
            name: self.name.clone(),
            description: info.description.clone(),
            settings: info.settings.clone(),
            uses,
            expected: Some(entry.revision()),
        };
        let (now_ms, write_ids) = self.stamp;
        let input = OwnerChange {
            realm_id: self.actor.realm_id,
            node_id: self.actor.node_id,
            owner: self.owner(),
            actor: self.actor.user_id,
            change,
            readable: false,
            now_ms,
            write_ids,
        };
        self.phase = EditPhase::Writing;
        Ok(Next::Call(IoCall::child(OwnerSecretOperation::new(input))))
    }
}

impl Operation for EditRules {
    type Output = bool;
    type Error = RuleError;

    fn first(&mut self) -> Result<Next<bool>, RuleError> {
        let read = GetConfigOperation::new(self.actor.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, RuleError> {
        match self.phase {
            EditPhase::Guard => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config
                    .map_err(|error| RuleError::Unreadable(format!("realm config: {error:?}")))?;
                if !self.guard_holds(&config) {
                    return Ok(Next::Done(false));
                }
                self.phase = EditPhase::Entry;
                let read = ReadEntry::new(self.actor.realm_id, &self.owner(), &self.name);
                Ok(Next::Call(IoCall::child(read)))
            }
            EditPhase::Entry => {
                let entry = result.child::<Option<SecretEntry>, EntryError>()??;
                self.change(entry.ok_or(RuleError::Missing)?)
            }
            EditPhase::Writing => {
                result.child::<Option<SecretEntry>, OwnerSecretError>()??;
                Ok(Next::Done(true))
            }
        }
    }
}

/// Why the rules of an install could not be put in place.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct RulesFailure(pub String);

impl From<IoError> for RulesFailure {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

/// Ensures the install's rules on every group secret it binds, for its current revision. Slots
/// may share a secret; each secret is edited once with all of its rules.
#[derive(Debug, PartialEq)]
pub(crate) struct EnsureRules {
    actor: Actor,
    group_id: GroupId,
    consumer: SecretConsumer,
    guard: RuleGuard,
    edits: VecDeque<(SecretName, Vec<UseRule>)>,
    stamps: VecDeque<RuleStamp>,
    current: Option<SecretName>,
}

impl EnsureRules {
    pub(crate) fn new(
        actor: &Actor,
        install: &PluginInstall,
        manifest: &Manifest,
        stamps: Vec<RuleStamp>,
    ) -> Result<Self, String> {
        let mut wanted = wanted_rules(install, manifest)?;
        let names: BTreeSet<&SecretName> = (install.secrets.values())
            .filter(|binding| binding.scope == BoundScope::Group)
            .map(|binding| &binding.name)
            .collect();
        let edits = (names.into_iter())
            .map(|name| (name.clone(), wanted.remove(name).unwrap_or_default()))
            .collect();
        Ok(Self {
            actor: actor.clone(),
            group_id: install.group_id,
            consumer: consumer(install),
            guard: RuleGuard::Revision {
                install_id: install.install_id,
                revision: install.revision,
            },
            edits,
            stamps: stamps.into(),
            current: None,
        })
    }

    fn advance(&mut self) -> Result<Next<()>, RulesFailure> {
        let Some((name, rules)) = self.edits.pop_front() else {
            return Ok(Next::Done(()));
        };
        let stamp = self.stamps.pop_front().ok_or(IoError::Unfinished)?;
        let target = (self.group_id, &name);
        let rules = (&self.consumer, rules);
        let edit = EditRules::new(&self.actor, target, rules, self.guard, stamp);
        self.current = Some(name);
        Ok(Next::Call(IoCall::child(edit)))
    }
}

impl Operation for EnsureRules {
    type Output = ();
    type Error = RulesFailure;

    fn first(&mut self) -> Result<Next<()>, RulesFailure> {
        self.advance()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, RulesFailure> {
        let name = self.current.take().ok_or(IoError::Unfinished)?;
        let edited = result.child::<bool, RuleError>()?;
        edited.map_err(|error| RulesFailure(format!("`group:{name}`: {error}")))?;
        self.advance()
    }
}

/// Removes the rules of a removing, abandoned or removed install from its group secrets. A
/// secret that cannot be edited is logged and the others are still edited.
#[derive(Debug, PartialEq)]
pub(crate) struct RetireRules {
    actor: Actor,
    group_id: GroupId,
    consumer: SecretConsumer,
    guard: RuleGuard,
    bindings: VecDeque<SecretBinding>,
    stamps: VecDeque<RuleStamp>,
    current: Option<SecretBinding>,
}

impl RetireRules {
    pub(crate) fn new(actor: &Actor, install: &PluginInstall, stamps: Vec<RuleStamp>) -> Self {
        let bindings = (install.secrets.values())
            .filter(|binding| binding.scope == BoundScope::Group)
            .cloned()
            .collect();
        Self {
            actor: actor.clone(),
            group_id: install.group_id,
            consumer: consumer(install),
            guard: RuleGuard::Retired {
                install_id: install.install_id,
            },
            bindings,
            stamps: stamps.into(),
            current: None,
        }
    }

    fn advance(&mut self) -> Result<Next<()>, RulesFailure> {
        let Some(binding) = self.bindings.pop_front() else {
            return Ok(Next::Done(()));
        };
        let stamp = self.stamps.pop_front().ok_or(IoError::Unfinished)?;
        let target = (self.group_id, &binding.name);
        let rules = (&self.consumer, Vec::new());
        let edit = EditRules::new(&self.actor, target, rules, self.guard, stamp);
        self.current = Some(binding);
        Ok(Next::Call(IoCall::child(edit)))
    }
}

impl Operation for RetireRules {
    type Output = ();
    type Error = RulesFailure;

    fn first(&mut self) -> Result<Next<()>, RulesFailure> {
        self.advance()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, RulesFailure> {
        let binding = self.current.take().ok_or(IoError::Unfinished)?;
        match result.child::<bool, RuleError>()? {
            Ok(_) | Err(RuleError::Missing) => {}
            Err(error) => warn!(%error, %binding, "Could not remove a plugin install's use rules"),
        }
        self.advance()
    }
}

/// The rules the install needs on each group secret it binds, from its manifest, grants and
/// settings: `inject` for granted connections, `verify` for the webhook, `value` for a granted
/// `secrets.value`.
pub(crate) fn wanted_rules(
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
    let sends = granted.get(&Grant::Services);
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
                response: ResponseExposure::Full,
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
    let values = granted.get(&Grant::Secrets);
    for slot in manifest.secrets.keys() {
        let path = scope.secret(slot);
        let shown = values.is_some_and(|patterns| patterns.iter().any(|p| covers(p, &path)));
        if let Some(binding) = group_bound(slot).filter(|_| shown) {
            let rules = wanted.entry(binding.name.clone()).or_default();
            rules.push(rule(Delivery::Value));
        }
    }
    wanted.values_mut().for_each(distinct);
    Ok(wanted)
}

/// Drops exact duplicates, such as two connections with the same address and methods.
fn distinct(rules: &mut Vec<UseRule>) {
    let mut kept: Vec<UseRule> = Vec::new();
    for rule in rules.drain(..) {
        if !kept.contains(&rule) {
            kept.push(rule);
        }
    }
    *rules = kept;
}

#[cfg(test)]
#[path = "rules_tests.rs"]
pub(crate) mod tests;
