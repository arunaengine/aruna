//! Decides one use of a secret a plugin install binds to a slot, in the serving node's own view:
//! selects the current entry, checks the use, writes the audit row, then opens the value.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::SecretBytes;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::NODE_SECRETS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::{BoundScope, InstallState, PluginInstall, SecretBinding};
use aruna_core::plugins::{Refusal, ReleaseError, ReleaseRequest, Released};
use aruna_core::realm_vault::{RealmSealed, RealmSecretRef, RealmVaultRecord};
use aruna_core::request_policy::PolicyRequest;
use aruna_core::secret_registers::{SecretBody, SecretEntry, SecretOwner};
use aruna_core::secrets::{
    Delivery, ResponseExposure, SecretConsumer, SecretKind, SecretName, SecretRecord, SecretScope,
    SecretSettings, UseRequest, UseRule, check_use,
};
use aruna_core::structs::identity::admin_audit::{AdminAuditRecord, SecretAudit, SecretUse};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use tracing::warn;
use ulid::Ulid;
use url::Url;

use super::account::{owned, read_user_call, read_user_result};
use super::entry::{EntryError, ReadEntry};
use super::grants::InstallScope;
use super::install::{pick_version, read_manifest};
use super::rows::{Row, StoreError, read_call, values, write_rows};
use crate::auth::enforcement::{policy_reads, read_policies};
use crate::secrets::node::{record_key, value_entry};
use crate::secrets::owner::sealed_value;

/// A failed release, as the operations behind it report it.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error(transparent)]
pub struct ReleaseFailure(#[from] pub ReleaseError);

impl From<IoError> for ReleaseFailure {
    fn from(error: IoError) -> Self {
        unavailable(error.to_string())
    }
}

impl From<StoreError> for ReleaseFailure {
    fn from(error: StoreError) -> Self {
        unavailable(error.to_string())
    }
}

fn unavailable(why: String) -> ReleaseFailure {
    ReleaseFailure(ReleaseError::Unavailable(why))
}

/// The current entry of a bound secret in the serving node's view.
#[derive(Clone, Debug, PartialEq)]
pub struct Selected {
    pub kind: SecretKind,
    pub settings: SecretSettings,
    pub rules: Vec<UseRule>,
    pub incarnation: Option<Ulid>,
    pub value_id: Ulid,
    pub sealed: Sealed,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Sealed {
    Owner(Box<SecretEntry>),
    Realm(RealmVaultRecord),
    /// The node vault entry of a node secret, by its value id.
    Node(Ulid),
}

/// The kind the install's manifest declares for `slot`.
pub(crate) fn slot_kind(
    config: &RealmConfigDocument,
    install: &PluginInstall,
    slot: &str,
) -> Result<SecretKind, ReleaseError> {
    let unavailable = |error: String| ReleaseError::Unavailable(error);
    let plugin = pick_version(config, &install.plugin, Some(&install.digest));
    let manifest = read_manifest(&plugin.map_err(|error| unavailable(error.to_string()))?);
    let manifest = manifest.map_err(|error| unavailable(error.to_string()))?;
    let declared = manifest.secrets.get(slot).map(|slot| slot.kind);
    let name = serde_json::to_value(declared).ok();
    let name = name
        .as_ref()
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    SecretKind::parse(name).map_err(|_| ReleaseError::Refused(Refusal::Entry))
}

/// The current entry of a bound secret, or `None` when it is absent or incomplete.
#[derive(Debug, PartialEq)]
pub(crate) struct SelectSecret {
    config: Arc<RealmConfigDocument>,
    group_id: Ulid,
    binding: SecretBinding,
}

impl SelectSecret {
    pub(crate) fn new(
        config: Arc<RealmConfigDocument>,
        install: &PluginInstall,
        binding: &SecretBinding,
    ) -> Self {
        Self {
            config,
            group_id: install.group_id,
            binding: binding.clone(),
        }
    }

    fn realm_secret(&self) -> Option<Selected> {
        let name = &self.binding.name;
        let record = (self.config.secrets.iter()).find(|record| record.name == *name)?;
        let entry = RealmSecretRef::named(record.value_id).entry;
        let sealed = self.config.vault.iter().find(|held| held.entry == entry)?;
        Some(Selected {
            kind: record.kind.clone(),
            settings: record.settings.clone(),
            rules: record.uses.clone(),
            incarnation: None,
            value_id: record.value_id,
            sealed: Sealed::Realm(sealed.clone()),
        })
    }
}

fn node_secret(row: Option<Vec<u8>>) -> Option<Selected> {
    let record = row.and_then(|bytes| postcard::from_bytes::<SecretRecord>(&bytes).ok())?;
    Some(Selected {
        kind: record.kind,
        settings: record.settings,
        rules: record.uses,
        incarnation: None,
        value_id: record.value_id,
        sealed: Sealed::Node(record.value_id),
    })
}

fn group_secret(entry: SecretEntry) -> Option<Selected> {
    let (info, value, kind) = (entry.info.as_ref()?, entry.value.as_ref()?, entry.kind()?);
    let SecretBody::Info(info) = &info.body else {
        return None;
    };
    Some(Selected {
        kind: kind.clone(),
        settings: info.settings.clone(),
        rules: info.uses.clone(),
        incarnation: Some(entry.lifecycle.stamp.write_id),
        value_id: value.stamp.write_id,
        sealed: Sealed::Owner(Box::new(entry.clone())),
    })
}

impl Operation for SelectSecret {
    type Output = Option<Selected>;
    type Error = ReleaseFailure;

    fn first(&mut self) -> Result<Next<Self::Output>, ReleaseFailure> {
        let name = &self.binding.name;
        Ok(match self.binding.scope {
            BoundScope::Node => {
                let row = (NODE_SECRETS_KEYSPACE, record_key(name));
                Next::Call(read_call(vec![row]))
            }
            BoundScope::Realm => Next::Done(self.realm_secret()),
            BoundScope::Group => {
                let owner = SecretOwner::Group(self.group_id);
                let read = ReadEntry::new(self.config.realm_id, &owner, name);
                Next::Call(IoCall::child(read))
            }
        })
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ReleaseFailure> {
        Ok(Next::Done(match self.binding.scope {
            BoundScope::Node => {
                let [row] = values::<1>(result)?;
                node_secret(row)
            }
            _ => {
                let entry = result.child::<Option<SecretEntry>, EntryError>()?;
                let entry = entry.map_err(|error| unavailable(error.to_string()))?;
                entry.and_then(group_secret)
            }
        }))
    }
}

/// Opens a selected secret: a node secret from this node's vault, others with the realm keys the
/// storage backend holds on a management node.
#[derive(Debug, PartialEq)]
pub(crate) struct OpenSealed {
    realm_id: RealmId,
    sealed: Sealed,
}

impl OpenSealed {
    pub(crate) fn new(realm_id: RealmId, sealed: Sealed) -> Self {
        Self { realm_id, sealed }
    }
}

impl Operation for OpenSealed {
    type Output = SecretBytes;
    type Error = ReleaseFailure;

    fn first(&mut self) -> Result<Next<SecretBytes>, ReleaseFailure> {
        let realm_id = self.realm_id;
        let effect = match &self.sealed {
            Sealed::Owner(entry) => {
                let sealed = sealed_value(realm_id, entry);
                let sealed = sealed.map_err(|error| unavailable(error.to_string()))?;
                StorageEffect::RealmOpen(Box::new(sealed))
            }
            Sealed::Realm(record) => StorageEffect::RealmOpen(Box::new(RealmSealed::Record {
                realm_id,
                record: record.clone(),
            })),
            Sealed::Node(value_id) => StorageEffect::VaultRead {
                entry: value_entry(*value_id),
                txn_id: None,
            },
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(
            effect,
        )))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SecretBytes>, ReleaseFailure> {
        let event = result.event()?;
        match (&self.sealed, event) {
            (
                Sealed::Node(_),
                Event::Storage(StorageEvent::VaultResult {
                    secret: Some(secret),
                    ..
                }),
            ) => Ok(Next::Done(secret)),
            (Sealed::Node(_), other) => Err(unavailable(format!(
                "the node vault answered {}",
                other.label()
            ))),
            (_, Event::Storage(StorageEvent::RealmOpened { result })) => {
                let secret = result.map_err(|error| unavailable(error.to_string()))?;
                Ok(Next::Done(secret))
            }
            (_, other) => Err(unavailable(format!(
                "the realm keys answered {}",
                other.label()
            ))),
        }
    }
}

pub(crate) fn delivery_name(delivery: Delivery) -> &'static str {
    match delivery {
        Delivery::Inject => "inject",
        Delivery::Verify => "verify",
        Delivery::Value => "value",
    }
}

/// The permission path a `secret.use` policy decides on.
pub(crate) fn secret_path(
    realm_id: RealmId,
    scope: SecretScope,
    name: &SecretName,
    node: NodeId,
) -> String {
    match scope {
        SecretScope::Group(group_id) => format!("/{realm_id}/g/{group_id}/secrets/{name}"),
        SecretScope::Realm => format!("/{realm_id}/secrets/{name}"),
        SecretScope::Node => format!("/{realm_id}/n/{node}/secrets/{name}"),
        SecretScope::User(user) => format!("/{realm_id}/u/{}/secrets/{name}", user.user_ulid),
    }
}

fn parse_target(request: &ReleaseRequest) -> Result<Option<(String, Url)>, ReleaseError> {
    let Some((method, url)) = &request.target else {
        return Ok(None);
    };
    let url = Url::parse(url).map_err(|_| ReleaseError::Refused(Refusal::Rule))?;
    Ok(Some((method.clone(), url)))
}

/// A use the rules admitted, on its way through the remaining checks.
#[derive(Debug, PartialEq)]
struct Admitted {
    selected: Selected,
    exposure: ResponseExposure,
    origin: Option<String>,
}

#[derive(Debug, PartialEq)]
enum ReleasePhase {
    Selecting,
    ReadingAccount(Box<Admitted>),
    ReadingPolicies(Box<Admitted>),
    Auditing(Box<Admitted>),
    Opening(Box<Admitted>),
    Refusing(Refusal),
}

/// The release a node serves to `peer`, decided from the config and its own storage. Every use
/// and refusal at a known binding leaves an audit row; without a row of a use nothing is released.
#[derive(Debug, PartialEq)]
pub(crate) struct ReleaseSecret {
    config: Arc<RealmConfigDocument>,
    local: NodeId,
    peer: NodeId,
    request: ReleaseRequest,
    phase: ReleasePhase,
}

impl ReleaseSecret {
    pub(crate) fn new(
        config: Arc<RealmConfigDocument>,
        (local, peer): (NodeId, NodeId),
        request: ReleaseRequest,
    ) -> Self {
        Self {
            config,
            local,
            peer,
            request,
            phase: ReleasePhase::Selecting,
        }
    }

    fn install(&self) -> Result<&PluginInstall, ReleaseFailure> {
        let id = self.request.install_id;
        let install = (self.config.installs.iter()).find(|install| install.install_id == id);
        Ok(install.ok_or(ReleaseError::Refused(Refusal::Binding))?)
    }

    fn binding(&self) -> Result<Option<&SecretBinding>, ReleaseFailure> {
        Ok(self.install()?.secrets.get(&self.request.slot))
    }

    fn audit_rows(
        &self,
        refusal: Option<Refusal>,
        selected: Option<&Selected>,
    ) -> Result<Option<Vec<Row>>, ReleaseFailure> {
        let install = self.install()?;
        let Some(binding) = self.binding()? else {
            return Ok(None);
        };
        let secret = SecretAudit {
            scope: binding.scope(install.group_id),
            name: binding.name.clone(),
            incarnation: selected.and_then(|selected| selected.incarnation),
            write_id: selected.map(|selected| selected.value_id),
        };
        let used = SecretUse {
            delivery: self.request.delivery,
            run_id: self.request.run_id,
            refused: refusal.map(|refusal| refusal.as_str().to_string()),
        };
        let record = AdminAuditRecord::for_use(install, secret, used);
        let rows = record.entries();
        Ok(Some(rows.map_err(|error| unavailable(error.to_string()))?))
    }

    /// Records the refusal, and answers it whether or not the row could be written.
    fn refuse(
        &mut self,
        refusal: Refusal,
        selected: Option<&Selected>,
    ) -> Result<Next<Released>, ReleaseFailure> {
        match self.audit_rows(Some(refusal), selected) {
            Ok(Some(rows)) => {
                self.phase = ReleasePhase::Refusing(refusal);
                Ok(Next::Call(IoCall::child(write_rows(rows))))
            }
            Ok(None) => Err(ReleaseError::Refused(refusal).into()),
            Err(error) => {
                warn!(%error, "Could not record a refused secret use");
                Err(ReleaseError::Refused(refusal).into())
            }
        }
    }

    /// The rule check of the selected entry, then the account check.
    fn selected(&mut self, selected: Option<Selected>) -> Result<Next<Released>, ReleaseFailure> {
        let install = self.install()?;
        let slot = &self.request.slot;
        let selected = match selected {
            Some(selected) if slot_kind(&self.config, install, slot)? == selected.kind => selected,
            _ => return self.refuse(Refusal::Entry, None),
        };
        let binding = self.binding()?.ok_or(IoError::Unfinished)?;
        let scope = binding.scope(install.group_id);
        let target = parse_target(&self.request)?;
        let consumer = SecretConsumer::Plugin {
            plugin_id: install.plugin.clone(),
            install_id: install.install_id,
        };
        let used = UseRequest {
            consumer: &consumer,
            delivery: self.request.delivery,
            group: Some(install.group_id),
            target: target.as_ref().map(|(method, url)| (method.as_str(), url)),
        };
        let Ok(rule) = check_use(scope, &selected.rules, &used) else {
            return self.refuse(Refusal::Rule, Some(&selected));
        };
        let exposure = rule.response.clone();
        // Step 5: the install is active and its account is the active account marked with it.
        if install.state != InstallState::Active {
            return self.refuse(Refusal::Authority, Some(&selected));
        }
        let account = install.account;
        let origin = target.map(|(_, url)| url.origin().ascii_serialization());
        self.phase = ReleasePhase::ReadingAccount(Box::new(Admitted {
            selected,
            exposure,
            origin,
        }));
        Ok(Next::Call(read_user_call(account)))
    }

    fn account_read(
        &mut self,
        admitted: Box<Admitted>,
        result: IoResult,
    ) -> Result<Next<Released>, ReleaseFailure> {
        let user = read_user_result(result).map_err(|error| unavailable(error.0))?;
        let install = self.install()?;
        let scope = InstallScope {
            realm_id: install.account.realm_id,
            group_id: install.group_id,
            install_id: install.install_id,
        };
        let active = user.is_some_and(|user| {
            user.user_id == install.account && owned(&user, &scope) && !user.is_deactivated()
        });
        if !active {
            return self.refuse(Refusal::Authority, Some(&admitted.selected));
        }
        let reads = policy_reads(self.config.realm_id, Some(install.group_id));
        self.phase = ReleasePhase::ReadingPolicies(admitted);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn policy_request(&self, admitted: &Admitted) -> Result<PolicyRequest, ReleaseFailure> {
        let install = self.install()?;
        let binding = self.binding()?.ok_or(IoError::Unfinished)?;
        let scope = binding.scope(install.group_id);
        let delivery = delivery_name(self.request.delivery).to_string();
        let params = BTreeMap::from([
            ("consumer".to_string(), install.install_id.to_string()),
            ("delivery".to_string(), delivery),
            (
                "origin".to_string(),
                admitted.origin.clone().unwrap_or_default(),
            ),
            (
                "kind".to_string(),
                admitted.selected.kind.as_str().to_string(),
            ),
        ]);
        Ok(PolicyRequest {
            path: secret_path(self.config.realm_id, scope, &binding.name, install.receiver),
            permission: "read".to_string(),
            user: install.account.to_string(),
            operation: "secret.use".to_string(),
            params,
            headers: BTreeMap::new(),
            body: None,
            session: None,
        })
    }

    fn policies_read(
        &mut self,
        admitted: Box<Admitted>,
        result: IoResult,
    ) -> Result<Next<Released>, ReleaseFailure> {
        let group = Some(self.install()?.group_id);
        let policies = read_policies(self.config.realm_id, group, result.values()?);
        let policies = policies.map_err(|error| unavailable(error.to_string()))?;
        let request = self.policy_request(&admitted)?;
        if policies.evaluate(&request).is_err() {
            return self.refuse(Refusal::Policy, Some(&admitted.selected));
        }
        // The row is durable before any value is opened; without it nothing is released.
        let rows = self.audit_rows(None, Some(&admitted.selected))?;
        let rows = rows.ok_or(IoError::Unfinished)?;
        self.phase = ReleasePhase::Auditing(admitted);
        Ok(Next::Call(IoCall::child(write_rows(rows))))
    }

    fn released(admitted: Admitted, fields: Option<SecretBytes>) -> Next<Released> {
        let selected = admitted.selected;
        Next::Done(Released {
            kind: selected.kind,
            settings: selected.settings,
            exposure: admitted.exposure,
            value_id: selected.value_id,
            fields,
        })
    }
}

impl Operation for ReleaseSecret {
    type Output = Released;
    type Error = ReleaseFailure;

    fn first(&mut self) -> Result<Next<Released>, ReleaseFailure> {
        let install = self.install()?;
        if install.receiver != self.peer {
            return self.refuse(Refusal::Peer, None);
        }
        // Node secrets never leave their node; the receiver releases them for itself.
        let here = self.peer == self.local;
        let binding = self.binding()?;
        let Some(binding) = binding.filter(|binding| here || binding.scope != BoundScope::Node)
        else {
            return self.refuse(Refusal::Binding, None);
        };
        let select = SelectSecret::new(self.config.clone(), install, binding);
        Ok(Next::Call(IoCall::child(select)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Released>, ReleaseFailure> {
        match std::mem::replace(&mut self.phase, ReleasePhase::Selecting) {
            ReleasePhase::Selecting => {
                let selected = result.child::<Option<Selected>, ReleaseFailure>()??;
                self.selected(selected)
            }
            ReleasePhase::ReadingAccount(admitted) => self.account_read(admitted, result),
            ReleasePhase::ReadingPolicies(admitted) => self.policies_read(admitted, result),
            ReleasePhase::Auditing(admitted) => {
                let written = result.child::<(), StoreError>()?;
                written.map_err(|error| unavailable(error.to_string()))?;
                if self.request.held == Some(admitted.selected.value_id) {
                    return Ok(Self::released(*admitted, None));
                }
                let sealed = admitted.selected.sealed.clone();
                self.phase = ReleasePhase::Opening(admitted);
                let open = OpenSealed::new(self.config.realm_id, sealed);
                Ok(Next::Call(IoCall::child(open)))
            }
            ReleasePhase::Opening(admitted) => {
                let fields = result.child::<SecretBytes, ReleaseFailure>()??;
                Ok(Self::released(*admitted, Some(fields)))
            }
            ReleasePhase::Refusing(refusal) => {
                if let Err(error) = result.child::<(), StoreError>()? {
                    warn!(%error, "Could not record a refused secret use");
                }
                Err(ReleaseError::Refused(refusal).into())
            }
        }
    }
}

#[cfg(test)]
#[path = "secret_use_tests.rs"]
mod tests;
