//! Tests selecting a bound secret: a group secret is found, opened and matched to the manifest's
//! slot kind; an absent or realm secret without its record selects nothing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::compute::Secret;
use aruna_core::document::DocumentTarget;
use aruna_core::secrets::{Delivery, ResponseExposure, SecretConsumer, SecretFields};
use ed25519_dalek::SigningKey;
use serde_json::json;

use super::*;
use crate::driver::drive;
use crate::plugins::install::tests::{Fixture, REALM_KEY};
use crate::plugins::install::{pick_version, read_config, read_manifest};
use crate::secrets::SecretChange;
use crate::secrets::owner::{OwnerChange, OwnerSecretOperation};

pub(crate) const TOKEN: &str = "canary-release-4f1e";
pub(crate) const ORIGIN: &str = "https://api.example.org";

pub(crate) fn keys() -> ReleaseKeys {
    let key = SigningKey::from_bytes(&REALM_KEY);
    ReleaseKeys {
        seal: Some(Arc::new(RealmSealKey::derive(&key))),
        vault: Some(Arc::new(RealmVaultKey::derive(&key))),
    }
}

/// Writes the realm config as it is after `edit`.
pub(crate) async fn edit_config(fixture: &Fixture, edit: impl FnOnce(&mut RealmConfigDocument)) {
    let mut config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    edit(&mut config);
    let target = DocumentTarget::RealmConfig {
        realm_id: fixture.actor.realm_id,
    };
    let value = config.to_bytes(&fixture.actor).unwrap();
    let key = target.storage_key().to_vec();
    fixture
        .write_row(target.storage_keyspace(), key, value)
        .await;
}

/// An install moved to a version whose `catalog` connection injects the bearer slot `api_key`,
/// with that slot bound to the group secret `api-key`.
pub(crate) async fn bound(fixture: &Fixture) -> PluginInstall {
    let slots = json!({
        "secrets": { "api_key": { "title": "API key", "kind": "bearer" } },
        "connections": { "catalog": { "url": "{catalog_url}/api/", "auth": "api_key" } }
    });
    let installed = fixture.install(&fixture.request(None)).await.unwrap();
    let digest = fixture.publish("0.2.0", "cd", slots).await;
    let binding = SecretBinding::parse("group:api-key").unwrap();
    edit_config(fixture, |config| {
        let install = (config.installs.iter_mut())
            .find(|install| install.install_id == installed.install_id)
            .unwrap();
        (install.digest, install.version) = (digest, "0.2.0".to_string());
        install.secrets = BTreeMap::from([("api_key".to_string(), binding)]);
    })
    .await;
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    config.installs.into_iter().next().unwrap()
}

/// The `inject` rule of `install` toward the fixture origins with `response`.
pub(crate) fn rule_for(install: &PluginInstall, response: ResponseExposure) -> UseRule {
    UseRule {
        consumer: SecretConsumer::Plugin {
            plugin_id: install.plugin.clone(),
            install_id: install.install_id,
        },
        delivery: Delivery::Inject,
        origins: vec![ORIGIN.to_string()],
        paths: Vec::new(),
        methods: Vec::new(),
        response,
        groups: Vec::new(),
    }
}

/// The group secret `api-key` with one `inject` rule for `install` toward `ORIGIN`.
pub(crate) async fn secret(fixture: &Fixture, install: &PluginInstall) {
    secret_with(fixture, install, ResponseExposure::Status).await;
}

/// The group secret `api-key` whose one rule for `install` exposes `response`.
pub(crate) async fn secret_with(
    fixture: &Fixture,
    install: &PluginInstall,
    response: ResponseExposure,
) {
    let rule = rule_for(install, response);
    let change = SecretChange::Create {
        name: SecretName::try_from("api-key".to_string()).unwrap(),
        kind: SecretKind::parse("bearer").unwrap(),
        description: String::new(),
        settings: SecretSettings::new(),
        uses: vec![rule],
        fields: SecretFields::new([("token".to_string(), Secret::new(TOKEN))].into()),
    };
    let input = OwnerChange {
        realm_id: fixture.actor.realm_id,
        node_id: fixture.actor.node_id,
        owner: SecretOwner::Group(install.group_id),
        actor: fixture.actor.user_id,
        change,
        readable: false,
        now_ms: 1,
        write_ids: [1, 2, 3].map(|part| Ulid::from_parts(77, part)),
        txn_id: None,
    };
    drive(OwnerSecretOperation::new(input), &fixture.ctx)
        .await
        .unwrap();
}

#[tokio::test]
async fn selects_group_secret() {
    let fixture = Fixture::keyed().await;
    let install = bound(&fixture).await;
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    let binding = install.secrets["api_key"].clone();
    assert!(
        select(&fixture.ctx, &config, &install, &binding)
            .await
            .unwrap()
            .is_none()
    );
    secret(&fixture, &install).await;
    let selected = select(&fixture.ctx, &config, &install, &binding)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        selected.kind,
        slot_kind(&config, &install, "api_key").unwrap()
    );
    assert_eq!(
        (selected.rules.len(), selected.incarnation.is_some()),
        (1, true)
    );
    let opened = open(&fixture.ctx, &keys(), config.realm_id, &selected.sealed).await;
    let fields = SecretFields::decode(&opened.unwrap());
    assert_eq!(fields.unwrap().get("token"), Some(TOKEN));
    let none = ReleaseKeys::default();
    let missing = open(&fixture.ctx, &none, config.realm_id, &selected.sealed).await;
    assert!(matches!(missing, Err(ReleaseError::Unavailable(_))));
    let realm = SecretBinding::parse("realm:api-key").unwrap();
    assert!(
        select(&fixture.ctx, &config, &install, &realm)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        slot_kind(&config, &install, "none"),
        Err(ReleaseError::Refused(Refusal::Entry))
    );
}

#[tokio::test]
async fn node_binding_checks() {
    use crate::secrets::node::{NodeChange, NodeSecretOperation};
    let fixture = Fixture::keyed().await;
    fixture
        .ctx
        .storage_handle
        .open_vault(aruna_core::node_vault::NodeVaultKey::random());
    let name = SecretName::try_from("api-key".to_string()).unwrap();
    let bearer = SecretKind::parse("bearer").unwrap();
    let local = fixture.actor.node_id;
    let mine = (local, local);
    let missing = check_node_binding(&fixture.ctx, mine, &name, &bearer).await;
    assert_eq!(missing, Err(NodeCheckError::Missing));
    let change = SecretChange::Create {
        name: name.clone(),
        kind: bearer.clone(),
        description: String::new(),
        settings: SecretSettings::new(),
        uses: Vec::new(),
        fields: SecretFields::new([("token".to_string(), Secret::new(TOKEN))].into()),
    };
    let input = NodeChange {
        realm_id: fixture.actor.realm_id,
        node_id: local,
        actor: fixture.actor.user_id,
        change,
        now_ms: 1,
        write_id: Ulid::from_parts(66, 2),
    };
    drive(NodeSecretOperation::new(input), &fixture.ctx)
        .await
        .unwrap();
    assert_eq!(
        check_node_binding(&fixture.ctx, mine, &name, &bearer).await,
        Ok(())
    );
    let basic = SecretKind::parse("basic").unwrap();
    let other = check_node_binding(&fixture.ctx, mine, &name, &basic).await;
    assert_eq!(other, Err(NodeCheckError::OtherKind));
    // Another receiver is asked over the transport, which this fixture does not run.
    let receiver = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
    let remote = check_node_binding(&fixture.ctx, (local, receiver), &name, &bearer).await;
    assert_eq!(remote, Err(NodeCheckError::Unavailable));
    // Only a management node may ask a receiver.
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    let served = node_check_for(&fixture.ctx, (&config, receiver), &name, &bearer).await;
    assert_eq!(served, Err(NodeCheckError::Forbidden));
    let asked = node_check_for(&fixture.ctx, (&config, local), &name, &bearer).await;
    assert_eq!(asked, Ok(()));
}

#[tokio::test]
async fn slots_bind() {
    let fixture = Fixture::keyed().await;
    let install = bound(&fixture).await;
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    let plugin = pick_version(&config, &install.plugin, Some(&install.digest)).unwrap();
    let manifest = read_manifest(&plugin).unwrap();
    let place = BindPlace {
        group_id: install.group_id,
        local: Some(fixture.actor.node_id),
        receiver: Some(install.receiver),
    };
    let bind = |pairs: &[(&str, &str)]| {
        let requested: BTreeMap<String, String> = (pairs.iter())
            .map(|(slot, secret)| (slot.to_string(), secret.to_string()))
            .collect();
        let (ctx, config, manifest) = (&fixture.ctx, &config, &manifest);
        async move { bind_slots(ctx, config, place, manifest, &requested).await }
    };
    // By default the slot uses the group secret of its name, which does not exist.
    assert!(matches!(bind(&[]).await, Err(BindError::Invalid(_))));
    secret(&fixture, &install).await;
    let (bindings, warnings) = bind(&[("api_key", "group:api-key")]).await.unwrap();
    assert_eq!(bindings["api_key"].to_string(), "group:api-key");
    assert!(warnings.is_empty());
    for wrong in [
        ("other", "group:api-key"),
        ("api_key", "user:mine"),
        ("api_key", "realm:api-key"),
    ] {
        assert!(
            matches!(bind(&[wrong]).await, Err(BindError::Invalid(_))),
            "{wrong:?}"
        );
    }
    // A node secret of the slot's kind binds, with a warning about its rule.
    fixture
        .ctx
        .storage_handle
        .open_vault(aruna_core::node_vault::NodeVaultKey::random());
    let input = crate::secrets::node::NodeChange {
        realm_id: fixture.actor.realm_id,
        node_id: fixture.actor.node_id,
        actor: fixture.actor.user_id,
        change: SecretChange::Create {
            name: SecretName::try_from("local-key".to_string()).unwrap(),
            kind: SecretKind::parse("basic").unwrap(),
            description: String::new(),
            settings: SecretSettings::new(),
            uses: Vec::new(),
            fields: SecretFields::new(
                [("username", "u"), ("password", "p")]
                    .map(|(name, value)| (name.to_string(), Secret::new(value)))
                    .into(),
            ),
        },
        now_ms: 1,
        write_id: Ulid::from_parts(66, 3),
    };
    drive(
        crate::secrets::node::NodeSecretOperation::new(input),
        &fixture.ctx,
    )
    .await
    .unwrap();
    let kind = bind(&[("api_key", "node:local-key")]).await;
    assert!(
        matches!(kind, Err(BindError::Invalid(_))),
        "a basic secret for a bearer slot"
    );
}

/// Writes the group secret `hook-key` of kind `header`, checking the header `checked`.
async fn hook_secret(fixture: &Fixture, install: &PluginInstall, checked: &str, round: u64) {
    let name = SecretName::try_from("hook-key".to_string()).unwrap();
    let settings = SecretSettings::from([("header".to_string(), checked.to_string())]);
    let change = match round {
        0 => SecretChange::Create {
            name,
            kind: SecretKind::parse("header").unwrap(),
            description: String::new(),
            settings,
            uses: Vec::new(),
            fields: SecretFields::new([("value".to_string(), Secret::new(TOKEN))].into()),
        },
        _ => SecretChange::Replace {
            name,
            description: String::new(),
            settings,
            uses: Vec::new(),
            expected: None,
        },
    };
    let input = OwnerChange {
        realm_id: fixture.actor.realm_id,
        node_id: fixture.actor.node_id,
        owner: SecretOwner::Group(install.group_id),
        actor: fixture.actor.user_id,
        change,
        readable: false,
        now_ms: 1 + round,
        write_ids: [1, 2, 3].map(|part| Ulid::from_parts(88 + round, part)),
        txn_id: None,
    };
    drive(OwnerSecretOperation::new(input), &fixture.ctx)
        .await
        .unwrap();
}

#[tokio::test]
async fn delivery_clashes() {
    let fixture = Fixture::keyed().await;
    let install = bound(&fixture).await;
    let hook = json!({
        "secrets": { "hook_key": { "title": "Hook key", "kind": "header" } },
        "triggers": { "webhook": { "verify": "hook_key", "delivery_id": "x-delivery" } }
    });
    let digest = fixture.publish("0.3.0", "ef", hook).await;
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    let plugin = pick_version(&config, &install.plugin, Some(&digest)).unwrap();
    let manifest = read_manifest(&plugin).unwrap();
    let place = BindPlace {
        group_id: install.group_id,
        local: None,
        receiver: None,
    };
    let requested = BTreeMap::from([("hook_key".to_string(), "group:hook-key".to_string())]);
    let bind = || bind_slots(&fixture.ctx, &config, place, &manifest, &requested);
    hook_secret(&fixture, &install, "x-signature", 0).await;
    assert!(bind().await.is_ok());
    // An edit that makes the checked header the delivery id fails closed at install and receipt.
    hook_secret(&fixture, &install, "X-Delivery", 1).await;
    assert!(matches!(bind().await, Err(BindError::Invalid(_))));
    let binding = SecretBinding::parse("group:hook-key").unwrap();
    let selected = select(&fixture.ctx, &config, &install, &binding).await;
    let selected = selected.unwrap().unwrap();
    assert!(delivery_clash(&manifest, "hook_key", &selected.settings));
    assert!(!delivery_clash(&manifest, "api_key", &selected.settings));
}
