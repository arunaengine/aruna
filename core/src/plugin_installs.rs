//! Describes a plugin installed into a group: its pinned version, settings, grants and state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::admin_documents::{AdminDocumentClock, AdminDocumentTarget};
use crate::plugins::{valid_digest, valid_name, valid_version};
use crate::secrets::{SecretName, SecretScope};
use crate::types::{GroupId, RoleId};
use crate::{NodeId, UserId};

/// Bytes of the setting values, as JSON.
pub const MAX_SETTINGS: usize = 64 * 1024;
/// Bytes of a whole install record, as JSON.
pub const MAX_RECORD: usize = 256 * 1024;
/// Secret slots one install binds at most, as many as a manifest declares.
pub const MAX_SLOTS: usize = 32;

/// Only `active` installs dispatch runs, pass the per call check or match a vault use rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstallState {
    Preparing,
    Active,
    Paused,
    /// An update waits until an admin approves what the new version newly requires.
    Blocked,
    Removing,
}

impl InstallState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Preparing => "preparing",
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Blocked => "blocked",
            Self::Removing => "removing",
        }
    }
}

/// Makes a role or account write of an install conditional on this node's record of the
/// install, read in the write's own transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InstallGuard {
    /// Admits while the record still has `revision`.
    Revision { install_id: Ulid, revision: u64 },
    /// Admits while the record is absent or conflicting, or names an account other than
    /// `account`: the account is no longer the install's.
    Unnamed { install_id: Ulid, account: UserId },
}

impl InstallGuard {
    pub fn admits(
        &self,
        config: Option<&crate::structs::identity::realm::RealmConfigDocument>,
    ) -> bool {
        let (Self::Revision { install_id, .. } | Self::Unnamed { install_id, .. }) = self;
        let install = config.and_then(|config| {
            (config.installs.iter()).find(|install| install.install_id == *install_id)
        });
        match self {
            Self::Revision { revision, .. } => {
                install.is_some_and(|install| install.revision == *revision)
            }
            Self::Unnamed { account, .. } => {
                install.is_none_or(|install| install.account != *account)
            }
        }
    }
}

/// A write another document must contain before a node decides about this install: the
/// target's clock right after the write.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Prerequisite {
    pub target: AdminDocumentTarget,
    pub clock: AdminDocumentClock,
}

/// One plugin installed into one group. The plugin acts as `account`, a service account of
/// the group marked with this install, whose role `role_id` is computed from `grants`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginInstall {
    pub install_id: Ulid,
    pub group_id: GroupId,
    pub plugin: String,
    pub version: String,
    /// The published artifact this install pins.
    pub digest: String,
    pub state: InstallState,
    /// An admin paused it; it stays paused through updates and grant changes until resumed.
    pub paused: bool,
    /// The checked setting values as a JSON object.
    pub settings: String,
    /// Granted resource patterns by permission name, each within what the manifest requests.
    pub grants: BTreeMap<String, Vec<String>>,
    pub account: UserId,
    pub role_id: RoleId,
    /// The node that admits, queues and runs this install's runs, chosen once and never changed.
    pub receiver: NodeId,
    /// The group admin who approved the current grants.
    pub granted_by: UserId,
    /// `sha256:<hex>` of the install request, so a retry with another body is refused.
    pub request: String,
    /// Raised by every write of this record.
    pub revision: u64,
    /// When the activating node made the install active, in its own clock.
    pub active_since_ms: Option<u64>,
    /// The preparation step that failed last, until a retry finishes.
    pub failed: Option<String>,
    /// The account and role writes the current revision depends on.
    pub prerequisites: Vec<Prerequisite>,
    /// The node whose clock `active_since_ms` uses.
    #[serde(default)]
    pub activated_by: Option<NodeId>,
    /// The last revision change that grew the events the install matches.
    #[serde(default)]
    pub widened: Option<Widening>,
    /// The secret each slot of the manifest uses, by slot name.
    #[serde(default)]
    pub secrets: BTreeMap<String, SecretBinding>,
}

/// Which vault a slot's secret lives in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BoundScope {
    /// The install's group.
    Group,
    Realm,
    /// The node vault of the install's receiver; node secrets never leave their node.
    Node,
}

/// The secret one slot of an install uses. The record is the only binding a release reads.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretBinding {
    pub scope: BoundScope,
    pub name: SecretName,
}

impl SecretBinding {
    /// `group:<name>`, `realm:<name>` or `node:<name>`; a bare name is a group secret.
    pub fn parse(text: &str) -> Option<Self> {
        let (scope, name) = match text.split_once(':') {
            Some(("group", name)) => (BoundScope::Group, name),
            Some(("realm", name)) => (BoundScope::Realm, name),
            Some(("node", name)) => (BoundScope::Node, name),
            Some(_) => return None,
            None => (BoundScope::Group, text),
        };
        let name = SecretName::try_from(name.to_string()).ok()?;
        Some(Self { scope, name })
    }

    /// The vault scope of the bound secret for an install of `group_id`.
    pub fn scope(&self, group_id: GroupId) -> SecretScope {
        match self.scope {
            BoundScope::Group => SecretScope::Group(group_id),
            BoundScope::Realm => SecretScope::Realm,
            BoundScope::Node => SecretScope::Node,
        }
    }
}

impl std::fmt::Display for SecretBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let scope = match self.scope {
            BoundScope::Group => "group",
            BoundScope::Realm => "realm",
            BoundScope::Node => "node",
        };
        write!(f, "{scope}:{}", self.name)
    }
}

/// A slot name as a manifest declares it: 1 to 64 of `a-z`, `0-9`, `_` and `-`.
fn valid_slot(slot: &str) -> bool {
    let allowed =
        |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_-".contains(&byte);
    !slot.is_empty() && slot.len() <= 64 && slot.bytes().all(allowed)
}

/// When and where an install's event coverage (trigger kinds, or the grant patterns their
/// permissions need) last grew, and the active revision whose coverage it extends.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Widening {
    pub at_ms: u64,
    pub by: NodeId,
    pub extends: u64,
}

impl PluginInstall {
    /// Whether the record is well formed. The writing management node checked its meaning.
    pub fn valid(&self) -> bool {
        let realm_id = self.account.realm_id;
        let group_root = format!("/{realm_id}/g/{}/", self.group_id);
        let grants = (self.grants.iter()).all(|(permission, patterns)| {
            valid_permission(permission)
                && (patterns.iter()).all(|pattern| pattern.starts_with(&group_root))
        });
        let settings = self.settings.len() <= MAX_SETTINGS
            && serde_json::from_str::<serde_json::Value>(&self.settings)
                .is_ok_and(|value| value.is_object());
        let active = self.state != InstallState::Active || self.active_since_ms.is_some();
        let paused = self.state != InstallState::Paused || self.paused;
        let size = serde_json::to_vec(self).is_ok_and(|json| json.len() <= MAX_RECORD);
        let slots = self.secrets.len() <= MAX_SLOTS && self.secrets.keys().all(|s| valid_slot(s));
        !self.install_id.is_nil()
            && valid_name(&self.plugin)
            && valid_version(&self.version)
            && valid_digest(&self.digest)
            && valid_digest(&self.request)
            && self.revision > 0
            && grants
            && settings
            && active
            && paused
            && size
            && slots
    }
}

/// A permission name such as `metadata.read`: two lowercase words joined by a dot.
fn valid_permission(name: &str) -> bool {
    let word = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_lowercase());
    name.len() <= 64
        && name
            .split_once('.')
            .is_some_and(|(a, b)| word(a) && word(b))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::structs::identity::realm::RealmId;

    pub(crate) fn install() -> PluginInstall {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let install_id = Ulid::from_parts(1, 7);
        let group_id = Ulid::from_parts(1, 9);
        let pattern = format!("/{realm_id}/g/{group_id}/meta/**");
        let node = iroh::SecretKey::from_bytes(&[3u8; 32]).public();
        PluginInstall {
            install_id,
            group_id,
            plugin: "dataset-announcer".into(),
            version: "0.1.0".into(),
            digest: format!("sha256:{}", "ab".repeat(32)),
            state: InstallState::Active,
            paused: false,
            settings: r#"{"catalog_url":"https://catalog.example.org"}"#.into(),
            grants: BTreeMap::from([("metadata.read".to_string(), vec![pattern])]),
            account: UserId::local(install_id, realm_id),
            role_id: install_id,
            receiver: node,
            granted_by: UserId::local(Ulid::from_parts(1, 3), realm_id),
            request: format!("sha256:{}", "cd".repeat(32)),
            revision: 1,
            active_since_ms: Some(5),
            failed: None,
            prerequisites: vec![Prerequisite {
                target: AdminDocumentTarget::Group { group_id },
                clock: AdminDocumentClock::default().with_observed(node, 4),
            }],
            activated_by: Some(node),
            widened: Some(Widening {
                at_ms: 9,
                by: node,
                extends: 1,
            }),
            secrets: BTreeMap::from([(
                "catalog_key".to_string(),
                SecretBinding::parse("group:catalog-key").unwrap(),
            )]),
        }
    }

    #[test]
    fn record_shape() {
        let record = install();
        assert!(record.valid());
        let json = serde_json::to_string(&record).unwrap();
        assert_eq!(
            serde_json::from_str::<PluginInstall>(&json).unwrap(),
            record
        );
        let cases: [fn(&mut PluginInstall); 10] = [
            |i| {
                let binding = SecretBinding::parse("key").unwrap();
                drop(i.secrets.insert("Key Slot".into(), binding))
            },
            |i| i.state = InstallState::Paused,
            |i| i.install_id = Ulid::nil(),
            |i| i.digest = "sha256:ab".into(),
            |i| i.settings = "[]".into(),
            |i| i.revision = 0,
            |i| i.active_since_ms = None,
            |i| drop(i.grants.insert("Metadata.Read".into(), Vec::new())),
            |i| {
                drop(
                    i.grants
                        .insert("objects.read".into(), vec!["/x/g/y/data/**".into()]),
                )
            },
            |i| i.settings = format!("{{\"a\":\"{}\"}}", "x".repeat(MAX_SETTINGS)),
        ];
        for edit in cases {
            let mut invalid = install();
            edit(&mut invalid);
            assert!(!invalid.valid(), "{invalid:?}");
        }
    }

    #[test]
    fn binding_text() {
        for text in ["group:catalog-key", "realm:shared", "node:local"] {
            let binding = SecretBinding::parse(text).unwrap();
            assert_eq!(binding.to_string(), text);
        }
        let bare = SecretBinding::parse("catalog-key").unwrap();
        assert_eq!(bare.scope, BoundScope::Group);
        assert_eq!(SecretBinding::parse("user:mine"), None);
        assert_eq!(SecretBinding::parse("group:Bad Name"), None);
        let group_id = Ulid::from_parts(1, 9);
        assert_eq!(bare.scope(group_id), SecretScope::Group(group_id));
    }

    #[test]
    fn guard_variants() {
        use crate::structs::identity::realm::RealmConfigDocument;
        let record = install();
        let mut config = RealmConfigDocument::new(record.account.realm_id, Vec::new(), 1);
        config.installs.push(record.clone());
        let install_id = record.install_id;
        let revision = InstallGuard::Revision {
            install_id,
            revision: 1,
        };
        let named = InstallGuard::Unnamed {
            install_id,
            account: record.account,
        };
        let other = InstallGuard::Unnamed {
            install_id,
            account: record.granted_by,
        };
        assert!(revision.admits(Some(&config)) && !named.admits(Some(&config)));
        assert!(other.admits(Some(&config)));
        config.installs.clear();
        assert!(!revision.admits(Some(&config)) && named.admits(Some(&config)));
        assert!(named.admits(None));
    }

    #[test]
    fn reducer_paths() {
        use crate::admin_documents::AdminDocumentOperation;
        use crate::reducer::{AdminDocumentError, AdminDocumentState};
        use crate::structs::identity::auth::Actor;

        let record = install();
        let realm_id = record.account.realm_id;
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            user_id: record.granted_by,
            realm_id,
        };
        let mut state = AdminDocumentState::new(AdminDocumentTarget::RealmConfig { realm_id });
        let set =
            |install_id, install: Option<PluginInstall>| AdminDocumentOperation::ConfigInstallSet {
                install_id,
                install: install.map(Box::new),
            };
        let other = set(Ulid::from_parts(2, 2), Some(record.clone()));
        let refused = state.apply_operation(&actor, other);
        assert_eq!(refused.err(), Some(AdminDocumentError::InvalidInstall));
        let written = set(record.install_id, Some(record.clone()));
        state.apply_operation(&actor, written).unwrap();
        assert_eq!(state.materialized_installs(), vec![record.clone()]);
        state
            .apply_operation(&actor, set(record.install_id, None))
            .unwrap();
        assert!(state.materialized_installs().is_empty());
    }
}
