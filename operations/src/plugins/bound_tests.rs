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
use crate::plugins::install::read_config;
use crate::plugins::install::tests::{Fixture, REALM_KEY};
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

/// The group secret `api-key` with one `inject` rule for `install` toward `ORIGIN`.
pub(crate) async fn secret(fixture: &Fixture, install: &PluginInstall) {
    let rule = UseRule {
        consumer: SecretConsumer::Plugin {
            plugin_id: install.plugin.clone(),
            install_id: install.install_id,
        },
        delivery: Delivery::Inject,
        origins: vec![ORIGIN.to_string()],
        paths: Vec::new(),
        methods: Vec::new(),
        response: ResponseExposure::Status,
        groups: Vec::new(),
    };
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
    let fields = SecretFields::decode(&open(&keys(), config.realm_id, &selected.sealed).unwrap());
    assert_eq!(fields.unwrap().get("token"), Some(TOKEN));
    let missing = open(&ReleaseKeys::default(), config.realm_id, &selected.sealed);
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
