//! Selects the secret a plugin install binds to a slot in this node's own view, and opens it
//! with the realm keys a management node holds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::compute::SecretBytes;
use aruna_core::plugin_installs::{BoundScope, PluginInstall, SecretBinding};
use aruna_core::realm_vault::{RealmSealKey, RealmSecretRef, RealmVaultKey, RealmVaultRecord};
use aruna_core::secret_registers::{SecretBody, SecretEntry, SecretOwner};
use aruna_core::secrets::{SecretKind, SecretName, SecretSettings, UseRule};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::install::{pick_version, read_manifest};
use crate::driver::DriverContext;
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
async fn read_entry(
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

pub fn open(
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
    }
}

#[cfg(test)]
#[path = "bound_tests.rs"]
pub(crate) mod tests;
