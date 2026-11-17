//! Builds what an admin approves for a plugin install: grants, connections, triggers and secrets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::plugin_installs::{BoundScope, PluginInstall, SecretBinding};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_plugins::manifest::Manifest;
use aruna_plugins::permission::{Grant, Permission};

use super::bound::vault_kind;
use super::check::covers;
use super::grants::{InstallScope, parse_grants, stored_grants, writes};
use super::install::{Prepared, pick_version, read_manifest};

/// One grant line: `read`, or `write` when the plugin writes, and its granted patterns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionLine {
    pub permission: String,
    pub required: bool,
    pub granted: Vec<String>,
}

/// One declared service, its methods and whether the install may call it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionLine {
    pub name: String,
    pub url: String,
    pub methods: Vec<String>,
    pub required: bool,
    pub granted: bool,
    /// The plugin downloads, and the node adds a secret to requests of this service.
    pub stored: bool,
}

/// One secret slot: its kind, the secret it uses and whether the code reads the value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretLine {
    pub slot: String,
    pub kind: String,
    pub secret: String,
    pub value: bool,
}

/// What an admin approves: the version, the permissions and connections, and the triggers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    pub id: String,
    pub plugin: String,
    pub title: Option<String>,
    pub version: String,
    pub digest: String,
    pub group: String,
    pub permissions: Vec<PermissionLine>,
    pub connections: Vec<ConnectionLine>,
    pub triggers: Vec<String>,
    pub secrets: Vec<SecretLine>,
    pub warnings: Vec<String>,
}

/// The summary of a checked install request, before anything is written.
pub fn prepared(prepared: &Prepared) -> Summary {
    let grants = stored_grants(&prepared.grants);
    let granted = (&grants, &prepared.secrets);
    build(
        &prepared.manifest,
        &prepared.plugin.digest,
        &prepared.scope,
        granted,
    )
}

/// The summary of a stored install, from the record alone while the pinned manifest is missing.
pub fn installed(config: &RealmConfigDocument, install: &PluginInstall) -> Summary {
    let manifest = pick_version(config, &install.plugin, Some(&install.digest))
        .ok()
        .and_then(|plugin| read_manifest(&plugin).ok());
    let scope = InstallScope {
        realm_id: config.realm_id,
        group_id: install.group_id,
        install_id: install.install_id,
    };
    match manifest {
        Some(manifest) => {
            let granted = (&install.grants, &install.secrets);
            build(&manifest, &install.digest, &scope, granted)
        }
        None => bare(install),
    }
}

/// The summary of `grants` against the manifest's requests.
fn build(
    manifest: &Manifest,
    digest: &str,
    scope: &InstallScope,
    (grants, bound): (
        &BTreeMap<String, Vec<String>>,
        &BTreeMap<String, SecretBinding>,
    ),
) -> Summary {
    let granted = parse_grants(grants);
    let requested = [Grant::Read]
        .into_iter()
        .chain(writes(manifest).then_some(Grant::Write));
    let permissions = requested
        .map(|grant| PermissionLine {
            permission: grant.to_string(),
            required: true,
            granted: (granted.get(&grant).into_iter().flatten())
                .cloned()
                .collect(),
        })
        .collect();
    let sends = granted.get(&Grant::Services);
    let values = granted.get(&Grant::Secrets);
    let readable = |slot: &str| {
        let path = scope.secret(slot);
        values.is_some_and(|patterns| patterns.iter().any(|pattern| covers(pattern, &path)))
    };
    let downloads = (manifest.requested()).contains(&Permission::TransfersDownload);
    let connections = (manifest.connections.iter())
        .map(|(name, connection)| ConnectionLine {
            name: name.clone(),
            url: connection.url.clone(),
            methods: (connection.methods.iter())
                .map(|method| method.as_str().to_string())
                .collect(),
            required: !connection.optional,
            granted: sends.is_some_and(|paths| paths.contains(&scope.connection(name))),
            stored: downloads && connection.auth.is_some(),
        })
        .collect();
    Summary {
        id: scope.install_id.to_string(),
        plugin: manifest.name.clone(),
        title: manifest.title.clone(),
        version: manifest.version.clone(),
        digest: digest.to_string(),
        group: scope.group_id.to_string(),
        permissions,
        connections,
        triggers: manifest
            .triggers
            .keys()
            .map(|kind| kind.as_str().to_string())
            .collect(),
        secrets: (bound.iter())
            .map(|(slot, binding)| SecretLine {
                slot: slot.clone(),
                kind: (manifest.secrets.get(slot))
                    .and_then(|declared| vault_kind(declared.kind))
                    .map_or_else(String::new, |kind| kind.as_str().to_string()),
                secret: binding.to_string(),
                value: readable(slot),
            })
            .collect(),
        warnings: (bound.iter())
            .filter_map(|(slot, binding)| {
                let shared = matches!(binding.scope, BoundScope::Realm | BoundScope::Node);
                shared
                    .then(|| format!("a realm admin adds the use rule of `{binding}` for `{slot}`"))
            })
            .collect(),
    }
}

/// A summary from the record alone.
fn bare(install: &PluginInstall) -> Summary {
    let permissions = (install.grants.iter())
        .map(|(permission, granted)| PermissionLine {
            permission: permission.clone(),
            required: false,
            granted: granted.clone(),
        })
        .collect();
    Summary {
        id: install.install_id.to_string(),
        plugin: install.plugin.clone(),
        title: None,
        version: install.version.clone(),
        digest: install.digest.clone(),
        group: install.group_id.to_string(),
        permissions,
        connections: Vec::new(),
        triggers: Vec::new(),
        secrets: Vec::new(),
        warnings: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use ulid::Ulid;

    fn scope() -> InstallScope {
        InstallScope {
            realm_id: aruna_core::structs::identity::realm::RealmId::from_bytes([1u8; 32]),
            group_id: Ulid::from_parts(1, 1),
            install_id: Ulid::from_parts(2, 2),
        }
    }

    #[test]
    fn summary_warnings() {
        let warned = |triggers: Value, binding: Option<&str>| {
            let manifest =
                serde_json::json!({"name": "p", "version": "0.1.0", "triggers": triggers});
            let manifest: Manifest = serde_json::from_value(manifest).unwrap();
            let bound = (binding.into_iter())
                .map(|text| ("key".to_string(), SecretBinding::parse(text).unwrap()))
                .collect();
            build(&manifest, "sha256:ab", &scope(), (&BTreeMap::new(), &bound)).warnings
        };
        // Sync triggers are delivered now and warn about nothing; a shared secret still warns.
        let sync = serde_json::json!({"sync_completed": {}, "sync_failed": {}});
        assert!(warned(sync.clone(), None).is_empty());
        assert!(warned(sync.clone(), Some("group:key")).is_empty());
        assert_eq!(
            warned(sync, Some("realm:key")),
            vec!["a realm admin adds the use rule of `realm:key` for `key`".to_string()]
        );
    }

    #[test]
    fn summary_lists_methods() {
        let manifest = serde_json::json!({"name": "p", "version": "0.1.0", "connections": {
            "catalog": {"url": "https://c.example.org/", "methods": ["GET", "POST"]}}});
        let manifest: Manifest = serde_json::from_value(manifest).unwrap();
        let none = (&BTreeMap::new(), &BTreeMap::new());
        let lines = build(&manifest, "sha256:ab", &scope(), none).connections;
        assert_eq!(lines[0].methods, ["GET", "POST"]);
    }

    #[test]
    fn summary_stored_answers() {
        // Only a download through a connection with a secret stores answers the rule screens.
        let stored = |required: Value| {
            let manifest = serde_json::json!({"name": "p", "version": "0.1.0",
                "permissions": {"required": required},
                "connections": {
                    "files": {"url": "https://f.example.org/", "auth": "token"},
                    "public": {"url": "https://p.example.org/"}}});
            let manifest: Manifest = serde_json::from_value(manifest).unwrap();
            let none = (&BTreeMap::new(), &BTreeMap::new());
            let lines = build(&manifest, "sha256:ab", &scope(), none).connections;
            lines.iter().map(|line| line.stored).collect::<Vec<_>>()
        };
        let download = serde_json::json!(["transfers.download"]);
        assert_eq!(stored(download), [true, false]);
        assert_eq!(stored(serde_json::json!([])), [false, false]);
    }
}
