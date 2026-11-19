//! The fixture plugin component in its test variants, and installs of it kept on a node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugins::PublishedPlugin;
use aruna_drivers::driver::drive;
use aruna_operations::plugins::copy_rows::StoreCopy;
use aruna_operations::plugins::install::InstallRequest;
use aruna_operations::plugins::publish::{PublishEntryConfig, PublishEntryOperation};
use aruna_plugin_types::artifact::{ArtifactDigest, attach_manifest, read_manifest};
use aruna_plugin_types::manifest::Manifest;

use super::flows::matcher::config;
use super::{Fixture, install};

pub const FIXTURE: &[u8] = include_bytes!("../../plugins/tests/fixture.wasm");

pub fn leb(mut value: usize, out: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        match value {
            0 => return out.push(byte),
            _ => out.push(byte | 0x80),
        }
    }
}

/// The fixture without its secret and connection, which installs refuse.
pub fn runnable() -> Vec<u8> {
    artifact(false, false)
}

/// The fixture whose `api` connection sends without a secret.
pub fn connected() -> Vec<u8> {
    artifact(true, false)
}

/// The fixture whose `api` connection injects its bearer slot `api`.
pub fn secured() -> Vec<u8> {
    artifact(true, true)
}

/// The runnable fixture, whose sync triggers `echo` handles; it answers the event.
pub fn echoed_sync() -> Vec<u8> {
    let (component, mut manifest) = bare();
    (manifest.connections, manifest.secrets) = (BTreeMap::new(), BTreeMap::new());
    attach_manifest(component, &manifest).unwrap()
}

/// The runnable fixture with only its manual and `job_finished` triggers, so no timer or event
/// of the test's own writes starts a run.
pub fn job_guest() -> Vec<u8> {
    use aruna_plugin_types::trigger::TriggerKind;
    let (component, mut manifest) = bare();
    (manifest.connections, manifest.secrets) = (BTreeMap::new(), BTreeMap::new());
    let kept = [TriggerKind::Manual, TriggerKind::JobFinished];
    manifest.triggers.retain(|kind, _| kept.contains(kind));
    attach_manifest(component, &manifest).unwrap()
}

/// The fixture's component without its manifest section, and that manifest.
pub fn bare() -> (&'static [u8], Manifest) {
    let manifest = read_manifest(FIXTURE).unwrap();
    let json = serde_json::to_vec(&manifest).unwrap();
    let mut content = Vec::new();
    leb("aruna-plugin".len(), &mut content);
    content.extend_from_slice(b"aruna-plugin");
    content.extend_from_slice(&json);
    let mut section = vec![0u8];
    leb(content.len(), &mut section);
    section.extend_from_slice(&content);
    (FIXTURE.strip_suffix(section.as_slice()).unwrap(), manifest)
}

pub fn artifact(connection: bool, secret: bool) -> Vec<u8> {
    let (component, manifest) = bare();
    let mut narrow = manifest;
    match secret {
        true => narrow.version = "0.2.1".to_string(),
        false => narrow.secrets.clear(),
    }
    match (connection, secret) {
        (true, true) => {}
        (true, false) => (narrow.connections.values_mut()).for_each(|c| c.auth = None),
        (false, _) => narrow.connections.clear(),
    }
    attach_manifest(component, &narrow).unwrap()
}

/// An install of the runnable fixture whose artifact this node keeps.
pub async fn installed(fixture: &Fixture) -> PluginInstall {
    install_artifact(fixture, runnable()).await
}

/// Publishes the fixture artifact `bytes` and keeps it on this node; answers its digest.
pub async fn publish_artifact(fixture: &Fixture, bytes: &[u8]) -> String {
    let manifest = read_manifest(bytes).unwrap();
    let digest = ArtifactDigest::of(bytes).to_string();
    let plugin = PublishedPlugin {
        name: manifest.name.clone(),
        version: manifest.version.clone(),
        digest: digest.clone(),
        size: bytes.len() as u64,
        manifest: serde_json::to_string(&manifest).unwrap(),
        published_by: fixture.actor.user_id,
        published_at_ms: 1,
    };
    let publish = PublishEntryOperation::new(PublishEntryConfig {
        actor: fixture.actor.clone(),
        auth_context: fixture.auth(),
        plugin,
        copies: BTreeSet::from([fixture.actor.node_id]),
    });
    drive(publish, &fixture.ctx).await.unwrap();
    let config = config(fixture).await;
    let bytes = bytes::Bytes::copy_from_slice(bytes);
    drive(StoreCopy::new(&config, &digest, bytes, 1), &fixture.ctx)
        .await
        .unwrap();
    digest
}

/// An install of the fixture artifact `bytes`, kept on this node.
pub async fn install_artifact(fixture: &Fixture, bytes: Vec<u8>) -> PluginInstall {
    let manifest = read_manifest(&bytes).unwrap();
    let digest = publish_artifact(fixture, &bytes).await;
    let settings = serde_json::json!({ "api_url": "https://api.example.org" });
    let request = InstallRequest {
        plugin: manifest.name,
        group_id: fixture.group_id,
        version: Some(digest),
        settings: settings.as_object().cloned().unwrap_or_default(),
        grants: BTreeMap::new(),
        install_id: None,
        secrets: BTreeMap::new(),
    };
    install(fixture, &request).await
}
