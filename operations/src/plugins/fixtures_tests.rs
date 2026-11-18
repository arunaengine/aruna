//! Builds the records and answers the plugin operation tests feed their operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::events::{Event, MetadataAnswer, NetEvent, PeerRequestError};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugins::PublishedPlugin;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use serde_json::{Value, json};
use ulid::Ulid;

use super::install::InstallCaller;
use super::rows::tests::child_result;
use super::work::tests::sample_install;
use crate::document_repository::GetConfigError;
use crate::metadata::protocol::{MetadataTransportMessage, encode_message};

pub(crate) fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

pub(crate) fn realm() -> RealmId {
    RealmId::from_bytes([1u8; 32])
}

/// The install the tests change; node 3 receives it.
pub(crate) fn install() -> PluginInstall {
    sample_install(Ulid::from_parts(1, 1))
}

pub(crate) fn caller() -> InstallCaller {
    let actor = Actor {
        node_id: node(3),
        user_id: install().granted_by,
        realm_id: realm(),
    };
    let auth = AuthContext::from(&actor);
    InstallCaller::new(&actor, &auth, 5_000)
}

/// The version of the install's plugin, its manifest extended by `extra`.
pub(crate) fn plugin(extra: Value) -> PublishedPlugin {
    let install = install();
    let mut manifest = json!({
        "name": install.plugin,
        "version": install.version,
        "permissions": { "required": ["metadata.read"] }
    });
    if let (Some(base), Some(extra)) = (manifest.as_object_mut(), extra.as_object()) {
        base.extend(extra.clone());
    }
    PublishedPlugin {
        name: install.plugin,
        version: install.version,
        digest: install.digest,
        size: 10,
        manifest: manifest.to_string(),
        published_by: UserId::nil(realm()),
        published_at_ms: 1,
    }
}

/// A realm where node 3 manages, the plugin is published and `install` is recorded.
pub(crate) fn config(install: &PluginInstall, extra: Value) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
    config.ensure_node(node(3), RealmNodeKind::Management);
    config.plugins.push(plugin(extra));
    config.installs.push(install.clone());
    config
}

/// Nodes 1 and 2 manage, node 3 serves and node 4 is a device.
pub(crate) fn realm_nodes() -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
    config.ensure_node(node(1), RealmNodeKind::Management);
    config.ensure_node(node(2), RealmNodeKind::Management);
    config.ensure_node(node(3), RealmNodeKind::Server);
    let owner = UserId::nil(realm());
    config.ensure_node(node(4), RealmNodeKind::User { owner });
    config
}

pub(crate) fn config_result(config: RealmConfigDocument) -> IoResult {
    child_result(Ok::<RealmConfigDocument, GetConfigError>(config))
}

pub(crate) fn is_child<T, E>(next: &Result<Next<T>, E>) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Child(_))))
}

pub(crate) fn is_effect<T, E>(next: &Result<Next<T>, E>) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Effect(_))))
}

fn answer_of(peer: NodeId, result: Result<Vec<u8>, PeerRequestError>) -> IoResult {
    IoResult::Event(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
        MetadataAnswer {
            peer,
            elapsed: Duration::ZERO,
            result,
        },
    )))))
}

/// `peer` answering with `message`.
pub(crate) fn peer_says(peer: NodeId, message: &MetadataTransportMessage) -> IoResult {
    answer_of(peer, Ok(encode_message(message).unwrap()))
}

/// `peer` not answering in time.
pub(crate) fn peer_silent(peer: NodeId) -> IoResult {
    answer_of(peer, Err(PeerRequestError::TimedOut))
}
