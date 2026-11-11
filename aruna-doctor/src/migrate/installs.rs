//! Rewrites realm configs whose plugin installs predate `activated_by` and `widened` (alpha.184)
//! or `secrets` (alpha.188): the install list is the config's last field, so it is split off.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::UserId;
use aruna_core::plugin_installs::{InstallState, PluginInstall, Prerequisite, Widening};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::types::{GroupId, RoleId};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use ulid::Ulid;

/// An install as alpha.184 stored it.
#[derive(Deserialize)]
struct StoredInstall {
    install_id: Ulid,
    group_id: GroupId,
    plugin: String,
    version: String,
    digest: String,
    state: InstallState,
    paused: bool,
    settings: String,
    grants: BTreeMap<String, Vec<String>>,
    account: UserId,
    role_id: RoleId,
    receiver: aruna_core::NodeId,
    granted_by: UserId,
    request: String,
    revision: u64,
    active_since_ms: Option<u64>,
    failed: Option<String>,
    prerequisites: Vec<Prerequisite>,
}

/// An install as alpha.188 stored it: the alpha.184 fields, then the coverage fields.
#[derive(Deserialize)]
struct CoveredInstall(
    Ulid,
    GroupId,
    String,
    String,
    String,
    InstallState,
    bool,
    String,
    BTreeMap<String, Vec<String>>,
    UserId,
    RoleId,
    aruna_core::NodeId,
    UserId,
    String,
    u64,
    Option<u64>,
    Option<String>,
    Vec<Prerequisite>,
    Option<aruna_core::NodeId>,
    Option<Widening>,
);

impl From<CoveredInstall> for PluginInstall {
    fn from(old: CoveredInstall) -> Self {
        let stored = StoredInstall {
            install_id: old.0,
            group_id: old.1,
            plugin: old.2,
            version: old.3,
            digest: old.4,
            state: old.5,
            paused: old.6,
            settings: old.7,
            grants: old.8,
            account: old.9,
            role_id: old.10,
            receiver: old.11,
            granted_by: old.12,
            request: old.13,
            revision: old.14,
            active_since_ms: old.15,
            failed: old.16,
            prerequisites: old.17,
        };
        Self {
            activated_by: old.18,
            widened: old.19,
            ..stored.into()
        }
    }
}

impl From<StoredInstall> for PluginInstall {
    fn from(old: StoredInstall) -> Self {
        Self {
            install_id: old.install_id,
            group_id: old.group_id,
            plugin: old.plugin,
            version: old.version,
            digest: old.digest,
            state: old.state,
            paused: old.paused,
            settings: old.settings,
            grants: old.grants,
            account: old.account,
            role_id: old.role_id,
            receiver: old.receiver,
            granted_by: old.granted_by,
            request: old.request,
            revision: old.revision,
            active_since_ms: old.active_since_ms,
            failed: old.failed,
            prerequisites: old.prerequisites,
            activated_by: None,
            widened: None,
            secrets: BTreeMap::new(),
        }
    }
}

/// The config re-encoded with its stored installs in the current shape, or `None` when no split
/// of the row reads as the fields before the install list and a list of stored installs.
pub(super) fn upgraded(value: &[u8]) -> Option<Vec<u8>> {
    upgraded_from::<CoveredInstall>(value).or_else(|| upgraded_from::<StoredInstall>(value))
}

fn upgraded_from<T: DeserializeOwned + Into<PluginInstall>>(value: &[u8]) -> Option<Vec<u8>> {
    // An empty list is one zero byte, so a config with no installs already decodes.
    for split in (0..value.len()).rev() {
        let Ok((stored, rest)) = postcard::take_from_bytes::<Vec<T>>(&value[split..]) else {
            continue;
        };
        if !rest.is_empty() || stored.is_empty() {
            continue;
        }
        let head = [&value[..split], &[0u8][..]].concat();
        let Ok(mut config) = RealmConfigDocument::from_bytes(&head) else {
            continue;
        };
        config.installs = stored.into_iter().map(Into::into).collect();
        return postcard::to_allocvec(&config).ok();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;
    use serde::Serialize;

    /// The alpha.184 shape, to write rows as that release did.
    #[derive(Serialize)]
    struct OldInstall {
        install_id: Ulid,
        group_id: GroupId,
        plugin: String,
        version: String,
        digest: String,
        state: InstallState,
        paused: bool,
        settings: String,
        grants: BTreeMap<String, Vec<String>>,
        account: UserId,
        role_id: RoleId,
        receiver: aruna_core::NodeId,
        granted_by: UserId,
        request: String,
        revision: u64,
        active_since_ms: Option<u64>,
        failed: Option<String>,
        prerequisites: Vec<Prerequisite>,
    }

    fn old_install(seed: u8, realm_id: RealmId) -> OldInstall {
        let id = Ulid::from_parts(u64::from(seed), 1);
        OldInstall {
            install_id: id,
            group_id: Ulid::from_parts(9, 9),
            plugin: "announcer".into(),
            version: "0.1.0".into(),
            digest: format!("sha256:{}", "ab".repeat(32)),
            state: InstallState::Active,
            paused: false,
            settings: "{}".into(),
            grants: BTreeMap::from([("metadata.read".into(), vec!["/r/g/x/meta/**".into()])]),
            account: UserId::local(id, realm_id),
            role_id: id,
            receiver: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
            granted_by: UserId::local(Ulid::from_parts(3, 3), realm_id),
            request: format!("sha256:{}", "cd".repeat(32)),
            revision: 2,
            active_since_ms: Some(1_000),
            failed: None,
            prerequisites: Vec::new(),
        }
    }

    #[test]
    fn keeps_stored_installs() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let empty = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        let current = postcard::to_allocvec(&empty).unwrap();
        // The old row: the config without its empty install list, then two old installs.
        let olds = vec![old_install(1, realm_id), old_install(2, realm_id)];
        let old = [
            &current[..current.len() - 1],
            &postcard::to_allocvec(&olds).unwrap(),
        ]
        .concat();
        assert!(RealmConfigDocument::from_bytes(&old).is_err());
        let migrated = RealmConfigDocument::from_bytes(&upgraded(&old).unwrap()).unwrap();
        assert_eq!(migrated.installs.len(), 2);
        let first = &migrated.installs[0];
        assert_eq!((first.install_id, first.revision), (olds[0].install_id, 2));
        assert_eq!(first.receiver, olds[0].receiver);
        assert_eq!(
            (first.active_since_ms, first.activated_by),
            (Some(1_000), None)
        );
        assert_eq!(first.widened, None);
        assert_eq!(migrated.realm_id, realm_id);
        assert_eq!(upgraded(&current), None);
    }

    #[test]
    fn keeps_covered_installs() {
        // An alpha.188 install: the alpha.184 fields, then `activated_by` and `widened`.
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let empty = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        let current = postcard::to_allocvec(&empty).unwrap();
        let node = iroh::SecretKey::from_bytes(&[5u8; 32]).public();
        let widened = Widening {
            at_ms: 9,
            by: node,
            extends: 1,
        };
        let olds = vec![(old_install(1, realm_id), Some(node), Some(widened))];
        let old = [
            &current[..current.len() - 1],
            &postcard::to_allocvec(&olds).unwrap(),
        ]
        .concat();
        assert!(RealmConfigDocument::from_bytes(&old).is_err());
        let migrated = RealmConfigDocument::from_bytes(&upgraded(&old).unwrap()).unwrap();
        let first = &migrated.installs[0];
        assert_eq!(
            (first.install_id, first.revision),
            (olds[0].0.install_id, 2)
        );
        assert_eq!(
            (first.activated_by, first.widened),
            (Some(node), Some(widened))
        );
        assert!(first.secrets.is_empty());
        assert_eq!(first.prerequisites, olds[0].0.prerequisites);
    }
}
