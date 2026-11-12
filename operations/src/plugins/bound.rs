//! Selects the secret a plugin install binds to a slot in this node's own view, and opens it
//! with the realm keys a management node holds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::compute::SecretBytes;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::NODE_SECRETS_KEYSPACE;
use aruna_core::plugin_installs::{BoundScope, PluginInstall, SecretBinding};
use aruna_core::realm_vault::{RealmSealKey, RealmSecretRef, RealmVaultKey, RealmVaultRecord};
use aruna_core::secret_registers::{SecretBody, SecretEntry, SecretOwner};
use aruna_core::secrets::{SecretKind, SecretName, SecretRecord, SecretSettings, UseRule};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_plugins::trigger::TriggerKind;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::install::{pick_version, read_manifest};
use super::store::Txn;
use crate::driver::DriverContext;
use crate::realm::mutate_placement::node_kind;
use crate::secrets::node::{record_key, value_entry};
use crate::secrets::owner::{EntryReader, open_value};

/// The step of the use check that refused a release.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Refusal {
    Peer,
    Binding,
    Entry,
    Rule,
    Authority,
    Policy,
}

impl Refusal {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Peer => "peer",
            Self::Binding => "binding",
            Self::Entry => "entry",
            Self::Rule => "rule",
            Self::Authority => "authority",
            Self::Policy => "policy",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum ReleaseError {
    #[error("the use of the secret was refused at the {} check", .0.as_str())]
    Refused(Refusal),
    #[error("the secret cannot be released now: {0}")]
    Unavailable(String),
}

/// The realm keys a management node opens group and realm secrets with.
#[derive(Clone, Default)]
pub struct ReleaseKeys {
    pub seal: Option<Arc<RealmSealKey>>,
    pub vault: Option<Arc<RealmVaultKey>>,
}

/// The current entry of a bound secret in the serving node's view.
pub struct Selected {
    pub kind: SecretKind,
    pub settings: SecretSettings,
    pub rules: Vec<UseRule>,
    pub incarnation: Option<Ulid>,
    pub value_id: Ulid,
    pub sealed: Sealed,
}

pub enum Sealed {
    Owner(Box<SecretEntry>),
    Realm(RealmVaultRecord),
    /// The node vault entry of a node secret, by its value id.
    Node(Ulid),
}

/// The kind the install's manifest declares for `slot`.
pub fn slot_kind(
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

/// The current entry of a group or realm secret, or `None` when it is absent or incomplete.
pub async fn select(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    install: &PluginInstall,
    binding: &SecretBinding,
) -> Result<Option<Selected>, ReleaseError> {
    if binding.scope == BoundScope::Node {
        let key = record_key(&binding.name);
        let row = Txn::direct(ctx).read(NODE_SECRETS_KEYSPACE, &key).await;
        let row = row.map_err(|error| ReleaseError::Unavailable(error.to_string()))?;
        let record = row.and_then(|bytes| postcard::from_bytes::<SecretRecord>(&bytes).ok());
        return Ok(record.map(|record| Selected {
            kind: record.kind,
            settings: record.settings,
            rules: record.uses,
            incarnation: None,
            value_id: record.value_id,
            sealed: Sealed::Node(record.value_id),
        }));
    }
    if binding.scope == BoundScope::Realm {
        let Some(record) = config
            .secrets
            .iter()
            .find(|record| record.name == binding.name)
        else {
            return Ok(None);
        };
        let entry = RealmSecretRef::named(record.value_id).entry;
        let sealed = config.vault.iter().find(|held| held.entry == entry);
        return Ok(sealed.map(|sealed| Selected {
            kind: record.kind.clone(),
            settings: record.settings.clone(),
            rules: record.uses.clone(),
            incarnation: None,
            value_id: record.value_id,
            sealed: Sealed::Realm(sealed.clone()),
        }));
    }
    let owner = SecretOwner::Group(install.group_id);
    let Some(entry) = read_entry(ctx, config.realm_id, &owner, &binding.name).await? else {
        return Ok(None);
    };
    let (Some(info), Some(value), Some(kind)) = (&entry.info, &entry.value, entry.kind()) else {
        return Ok(None);
    };
    let SecretBody::Info(info) = &info.body else {
        return Ok(None);
    };
    Ok(Some(Selected {
        kind: kind.clone(),
        settings: info.settings.clone(),
        rules: info.uses.clone(),
        incarnation: Some(entry.lifecycle.stamp.write_id),
        value_id: value.stamp.write_id,
        sealed: Sealed::Owner(Box::new(entry.clone())),
    }))
}

/// Drives the register reader of one name on the node's own storage.
pub(crate) async fn read_entry(
    ctx: &DriverContext,
    realm_id: RealmId,
    owner: &SecretOwner,
    name: &SecretName,
) -> Result<Option<SecretEntry>, ReleaseError> {
    let unavailable = |error: String| ReleaseError::Unavailable(error);
    let mut reader = EntryReader::new(realm_id, owner, name);
    let mut effect = reader.start(None);
    loop {
        let aruna_core::effects::Effect::Storage(storage) = effect else {
            return Err(unavailable(
                "the reader asked for a non-storage effect".into(),
            ));
        };
        let event = ctx.storage_handle.send_storage_effect(storage).await;
        match reader.step(event, None) {
            Ok(Some(next)) => effect = next,
            Ok(None) => return Ok(reader.entry().filter(SecretEntry::complete)),
            Err(error) => return Err(unavailable(error.to_string())),
        }
    }
}

pub async fn open(
    ctx: &DriverContext,
    keys: &ReleaseKeys,
    realm_id: RealmId,
    sealed: &Sealed,
) -> Result<SecretBytes, ReleaseError> {
    let missing = || ReleaseError::Unavailable("this node holds no realm key".into());
    let failed = |error: String| ReleaseError::Unavailable(error);
    match sealed {
        Sealed::Owner(entry) => {
            let key = keys.seal.as_ref().ok_or_else(missing)?;
            open_value(key, &realm_id, entry).map_err(|error| failed(error.to_string()))
        }
        Sealed::Realm(record) => {
            let key = keys.vault.as_ref().ok_or_else(missing)?;
            key.open(&realm_id, record)
                .map_err(|error| failed(error.to_string()))
        }
        Sealed::Node(value_id) => {
            let entry = value_entry(*value_id);
            let read = StorageEffect::VaultRead {
                entry,
                txn_id: None,
            };
            match ctx.storage_handle.send_storage_effect(read).await {
                Event::Storage(StorageEvent::VaultResult {
                    secret: Some(secret),
                    ..
                }) => Ok(secret),
                other => Err(failed(format!("the node vault answered {other:?}"))),
            }
        }
    }
}

/// The vault kind a manifest slot kind names.
pub fn vault_kind(kind: aruna_plugins::manifest::SecretKind) -> Option<SecretKind> {
    let name = serde_json::to_value(kind).ok()?;
    SecretKind::parse(name.as_str()?).ok()
}

/// Who installs into which group, on which management node, for which receiver.
#[derive(Clone, Copy, Debug)]
pub struct BindPlace {
    pub group_id: aruna_core::types::GroupId,
    pub local: Option<NodeId>,
    pub receiver: Option<NodeId>,
}

/// The bindings of every slot the manifest declares, from `requested` (slot to `scope:name`)
/// or the same name in the group, each checked where it lives; and the warnings for slots whose
/// use rule a realm admin must add.
pub async fn bind_slots(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    place: BindPlace,
    manifest: &aruna_plugins::manifest::Manifest,
    requested: &BTreeMap<String, String>,
) -> Result<(BTreeMap<String, SecretBinding>, Vec<String>), BindError> {
    if let Some(slot) = requested
        .keys()
        .find(|slot| !manifest.secrets.contains_key(*slot))
    {
        return Err(BindError::Invalid(format!(
            "the manifest has no secret slot `{slot}`"
        )));
    }
    let (mut bindings, mut warnings) = (BTreeMap::new(), Vec::new());
    for (slot, declared) in &manifest.secrets {
        let text = requested
            .get(slot)
            .cloned()
            .unwrap_or_else(|| format!("group:{slot}"));
        let invalid = || BindError::Invalid(format!("`{text}` is not a secret of slot `{slot}`"));
        let binding = SecretBinding::parse(&text).ok_or_else(invalid)?;
        let kind = vault_kind(declared.kind).ok_or_else(invalid)?;
        let shown = kind.as_str();
        let missing = || BindError::Invalid(format!("no {shown} secret `{binding}` for `{slot}`"));
        // A node secret's settings live on the receiver, which checks them on every delivery.
        let settings = match binding.scope {
            BoundScope::Group => {
                let owner = SecretOwner::Group(place.group_id);
                let entry = read_entry(ctx, config.realm_id, &owner, &binding.name).await;
                let entry = entry.map_err(|error| BindError::Unavailable(error.to_string()))?;
                let entry = entry.filter(|entry| entry.kind() == Some(&kind));
                match entry.as_ref().and_then(|entry| entry.info.as_ref()) {
                    Some(info) => match &info.body {
                        SecretBody::Info(info) => Some(info.settings.clone()),
                        _ => None,
                    },
                    None => return Err(missing()),
                }
            }
            BoundScope::Realm => {
                let record = config
                    .secrets
                    .iter()
                    .find(|record| record.name == binding.name);
                let record = record.filter(|record| record.kind == kind);
                let record = record.ok_or_else(missing)?;
                warnings.push(format!(
                    "a realm admin adds the use rule of `{binding}` for `{slot}`"
                ));
                Some(record.settings.clone())
            }
            BoundScope::Node => {
                let (Some(local), Some(receiver)) = (place.local, place.receiver) else {
                    return Err(BindError::Unavailable(
                        "the receiver is not known yet".into(),
                    ));
                };
                match check_node_binding(ctx, (local, receiver), &binding.name, &kind).await {
                    Ok(_) => {}
                    Err(NodeCheckError::Unavailable) => {
                        return Err(BindError::Unavailable("the receiver did not answer".into()));
                    }
                    Err(_) => return Err(missing()),
                }
                warnings.push(format!(
                    "a realm admin adds the use rule of `{binding}` for `{slot}`"
                ));
                None
            }
        };
        if settings.is_some_and(|settings| delivery_clash(manifest, slot, &settings)) {
            return Err(BindError::Invalid(format!(
                "the webhook's delivery id is the header `{binding}` checks"
            )));
        }
        bindings.insert(slot.clone(), binding);
    }
    Ok((bindings, warnings))
}

/// Whether the webhook that `slot` verifies reads its delivery id from the header the secret
/// checks, so the id would carry the credential. Install and receiver both refuse it.
pub fn delivery_clash(
    manifest: &aruna_plugins::manifest::Manifest,
    slot: &str,
    settings: &SecretSettings,
) -> bool {
    let hook = manifest.triggers.get(&TriggerKind::Webhook);
    let hook = hook.filter(|hook| hook.verify.as_deref() == Some(slot));
    let id = hook.and_then(|hook| hook.delivery_id.as_deref());
    (id.zip(settings.get("header"))).is_some_and(|(id, header)| id.eq_ignore_ascii_case(header))
}

/// Why slots could not be bound.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BindError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Unavailable(String),
}

/// Why a node binding does not fit its slot on the install's receiver.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum NodeCheckError {
    #[error("the receiver has no node secret of this name")]
    Missing,
    #[error("the node secret has another kind than the slot")]
    OtherKind,
    #[error("only a server node of the realm may ask")]
    Forbidden,
    #[error("the receiver did not answer")]
    Unavailable,
}

/// Whether this node holds the node secret `name` with `kind`; answers the header its `header`
/// setting names, in lowercase. Never the value.
pub async fn node_secret_fits(
    ctx: &DriverContext,
    name: &SecretName,
    kind: &SecretKind,
) -> Result<Option<String>, NodeCheckError> {
    let row = Txn::direct(ctx)
        .read(NODE_SECRETS_KEYSPACE, &record_key(name))
        .await;
    let row = row.map_err(|_| NodeCheckError::Unavailable)?;
    let record = row.and_then(|bytes| postcard::from_bytes::<SecretRecord>(&bytes).ok());
    match record {
        None => Err(NodeCheckError::Missing),
        Some(record) if record.kind != *kind => Err(NodeCheckError::OtherKind),
        Some(record) => Ok(record
            .settings
            .get("header")
            .map(|h| h.to_ascii_lowercase())),
    }
}

/// Checks a node binding where it lives: on `receiver`, here or over the metadata transport;
/// answers the secret's checked header as [`node_secret_fits`] does.
pub async fn check_node_binding(
    ctx: &DriverContext,
    (local, receiver): (NodeId, NodeId),
    name: &SecretName,
    kind: &SecretKind,
) -> Result<Option<String>, NodeCheckError> {
    if receiver == local {
        return node_secret_fits(ctx, name, kind).await;
    }
    let metadata = ctx.metadata_handle.as_ref();
    let metadata = metadata.ok_or(NodeCheckError::Unavailable)?;
    let asked = metadata.request_node_check(receiver, name.clone(), kind.clone());
    asked.await
}

/// The receiver side of a node binding check, for a management node that installs or a server
/// node that relays a webhook.
pub(crate) async fn serve_node_check(
    ctx: &DriverContext,
    peer: NodeId,
    name: SecretName,
    kind: SecretKind,
) -> Result<Option<String>, NodeCheckError> {
    let local = super::artifacts::local_config(ctx).await;
    let (config, _) = local.map_err(|_| NodeCheckError::Unavailable)?;
    node_check_for(ctx, (&config, peer), &name, &kind).await
}

async fn node_check_for(
    ctx: &DriverContext,
    (config, peer): (&RealmConfigDocument, NodeId),
    name: &SecretName,
    kind: &SecretKind,
) -> Result<Option<String>, NodeCheckError> {
    let server =
        node_kind(config, peer).is_some_and(|kind| !matches!(kind, RealmNodeKind::User { .. }));
    if !server {
        return Err(NodeCheckError::Forbidden);
    }
    node_secret_fits(ctx, name, kind).await
}

#[cfg(test)]
#[path = "bound_tests.rs"]
pub(crate) mod tests;
