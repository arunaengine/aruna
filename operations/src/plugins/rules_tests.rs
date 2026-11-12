//! Tests rule edits: ensuring is idempotent, keeps other consumers' rules, writes nothing for a
//! stale revision, removal only runs for a retired install, and an install gets every rule.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::plugin_installs::PluginInstall;
use aruna_core::secrets::{Delivery, ResponseExposure};

use super::*;
use crate::plugins::bound::select;
use crate::plugins::bound::tests::{ORIGIN, bound, edit_config, secret};
use crate::plugins::install::tests::Fixture;

fn rule(consumer: &SecretConsumer, response: ResponseExposure) -> UseRule {
    UseRule {
        consumer: consumer.clone(),
        delivery: Delivery::Inject,
        origins: vec![ORIGIN.to_string()],
        paths: vec!["/api".to_string()],
        methods: vec!["POST".to_string()],
        response,
        groups: Vec::new(),
    }
}

async fn rules(fixture: &Fixture, install: &PluginInstall) -> Vec<UseRule> {
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    let binding = &install.secrets["api_key"];
    let selected = select(&fixture.ctx, &config, install, binding)
        .await
        .unwrap();
    selected.unwrap().rules
}

#[tokio::test]
async fn ensure_and_retire() {
    let fixture = Fixture::keyed().await;
    let install = bound(&fixture).await;
    secret(&fixture, &install).await;
    let name = SecretName::try_from("api-key".to_string()).unwrap();
    let target = (install.group_id, &name);
    let ours = consumer(&install);
    let other = SecretConsumer::Plugin {
        plugin_id: "other".to_string(),
        install_id: Ulid::from_parts(8, 8),
    };
    let current = RuleGuard::Revision {
        install_id: install.install_id,
        revision: install.revision,
    };
    let edit = |consumer: &SecretConsumer, wanted: Vec<UseRule>, guard| {
        let consumer = consumer.clone();
        let (ctx, actor) = (&fixture.ctx, &fixture.actor);
        async move { edit_rules(ctx, actor, target, (&consumer, wanted), guard).await }
    };
    let theirs = rule(&other, ResponseExposure::Status);
    assert_eq!(edit(&other, vec![theirs.clone()], current).await, Ok(true));
    let wanted = vec![rule(&ours, ResponseExposure::Full)];
    assert_eq!(edit(&ours, wanted.clone(), current).await, Ok(true));
    assert_eq!(edit(&ours, wanted.clone(), current).await, Ok(false));
    assert_eq!(
        rules(&fixture, &install).await,
        [theirs.clone(), wanted[0].clone()]
    );
    // A revision the record has moved past writes nothing.
    let stale = RuleGuard::Revision {
        install_id: install.install_id,
        revision: install.revision + 1,
    };
    assert_eq!(edit(&ours, Vec::new(), stale).await, Ok(false));
    let retired = RuleGuard::Retired {
        install_id: install.install_id,
    };
    assert_eq!(edit(&ours, Vec::new(), retired).await, Ok(false));
    edit_config(&fixture, |config| {
        config.installs[0].state = aruna_core::plugin_installs::InstallState::Removing;
    })
    .await;
    assert_eq!(edit(&ours, Vec::new(), retired).await, Ok(true));
    assert_eq!(rules(&fixture, &install).await, [theirs]);
    let missing = SecretName::try_from("absent".to_string()).unwrap();
    let absent = edit_rules(
        &fixture.ctx,
        &fixture.actor,
        (install.group_id, &missing),
        (&ours, Vec::new()),
        retired,
    );
    assert_eq!(absent.await, Err(RuleError::Missing));
}

#[tokio::test]
async fn install_rules() {
    let fixture = Fixture::keyed().await;
    let install = bound(&fixture).await;
    secret(&fixture, &install).await;
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    let plugin =
        crate::plugins::install::pick_version(&config, &install.plugin, Some(&install.digest));
    let manifest = crate::plugins::install::read_manifest(&plugin.unwrap()).unwrap();
    let wanted = wanted_rules(&install, &manifest).unwrap();
    let name = SecretName::try_from("api-key".to_string()).unwrap();
    let inject = &wanted[&name][0];
    assert_eq!(
        (inject.delivery, inject.response.clone()),
        (Delivery::Inject, ResponseExposure::Status)
    );
    assert_eq!(inject.origins, ["https://catalog.example.org"]);
    assert_eq!(
        (inject.paths.clone(), inject.methods.clone()),
        (vec!["/api".to_string()], vec!["GET".to_string()])
    );
    // Ensuring replaces the fixture's broader rule with the computed one.
    ensure_rules(&fixture.ctx, &fixture.actor, &install, &manifest)
        .await
        .unwrap();
    assert_eq!(
        rules(&fixture, &install).await,
        std::slice::from_ref(inject)
    );
    // A setting change moves the origin.
    edit_config(&fixture, |config| {
        config.installs[0].settings = r#"{"catalog_url":"https://moved.example.org"}"#.into();
    })
    .await;
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    let moved = config.installs[0].clone();
    ensure_rules(&fixture.ctx, &fixture.actor, &moved, &manifest)
        .await
        .unwrap();
    let held = rules(&fixture, &moved).await;
    assert_eq!(held[0].origins, ["https://moved.example.org"]);
    // Removal takes this install's rules away, and only once it is removing.
    retire_rules(&fixture.ctx, &fixture.actor, &moved).await;
    assert_eq!(rules(&fixture, &moved).await.len(), 1);
    edit_config(&fixture, |config| {
        config.installs[0].state = aruna_core::plugin_installs::InstallState::Removing
    })
    .await;
    retire_rules(&fixture.ctx, &fixture.actor, &moved).await;
    assert!(rules(&fixture, &moved).await.is_empty());
}

/// The group secret `name` of `kind` with `settings` and one field, without use rules.
pub(crate) async fn plain_secret(
    fixture: &Fixture,
    (name, kind): (&str, &str),
    settings: &[(&str, &str)],
) {
    use crate::driver::drive;
    use crate::secrets::SecretChange;
    use crate::secrets::owner::{OwnerChange, OwnerSecretOperation};
    use aruna_core::compute::Secret;
    use aruna_core::secret_registers::SecretOwner;
    use aruna_core::secrets::{SecretFields, SecretKind};
    let field = match kind {
        "bearer" => "token",
        _ => "value",
    };
    let change = SecretChange::Create {
        name: SecretName::try_from(name.to_string()).unwrap(),
        kind: SecretKind::parse(kind).unwrap(),
        description: String::new(),
        settings: (settings.iter())
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect(),
        uses: Vec::new(),
        fields: SecretFields::new([(field.to_string(), Secret::new("canary-plain-9d2c"))].into()),
    };
    let input = OwnerChange {
        realm_id: fixture.actor.realm_id,
        node_id: fixture.actor.node_id,
        owner: SecretOwner::Group(fixture.group_id),
        actor: fixture.actor.user_id,
        change,
        readable: false,
        now_ms: 1,
        write_ids: [1, 2, 3].map(|_| Ulid::generate()),
        txn_id: None,
    };
    drive(OwnerSecretOperation::new(input), &fixture.ctx)
        .await
        .unwrap();
}

#[tokio::test]
async fn installs_with_secrets() {
    use crate::plugins::grants::GrantError;
    use crate::plugins::install::InstallError;
    let fixture = Fixture::keyed().await;
    plain_secret(&fixture, ("api-key", "bearer"), &[]).await;
    plain_secret(
        &fixture,
        ("hook-key", "header"),
        &[("header", "x-signature")],
    )
    .await;
    let features = serde_json::json!({
        "secrets": {
            "api_key": { "title": "API key", "kind": "bearer" },
            "hook_key": { "title": "Hook key", "kind": "header" }
        },
        "connections": { "catalog": { "url": "{catalog_url}/api/", "auth": "api_key" } },
        "triggers": { "webhook": {
            "verify": "hook_key", "delivery_id": "x-delivery", "handler": "on_webhook"
        } },
        "permissions": { "required": ["metadata.read"], "optional": ["secrets.value"] }
    });
    let digest = fixture.publish("0.4.0", "aa", features).await;
    let mut request = fixture.request(None);
    request.version = Some(digest);
    request.secrets = [("api_key", "group:api-key"), ("hook_key", "group:hook-key")]
        .map(|(slot, secret)| (slot.to_string(), secret.to_string()))
        .into();
    let install = fixture.install(&request).await.unwrap();
    assert_eq!(
        install.state,
        aruna_core::plugin_installs::InstallState::Active
    );
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    let delivered = async |slot: &str| {
        let binding = &install.secrets[slot];
        let selected = select(&fixture.ctx, &config, &install, binding).await;
        let rules = selected.unwrap().unwrap().rules;
        rules
            .into_iter()
            .map(|rule| rule.delivery)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        delivered("api_key").await,
        [Delivery::Inject, Delivery::Value]
    );
    assert_eq!(
        delivered("hook_key").await,
        [Delivery::Verify, Delivery::Value]
    );
    // Container work stays refused.
    let jobs = serde_json::json!({ "permissions": { "required": ["jobs.run"] } });
    let digest = fixture.publish("0.5.0", "bb", jobs).await;
    let mut request = fixture.request(None);
    request.version = Some(digest);
    let refused = fixture.install(&request).await;
    assert!(
        matches!(
            refused,
            Err(InstallError::Grant(GrantError::Unsupported(_)))
        ),
        "{refused:?}"
    );
}

/// The bound install moved to a version whose `catalog` and `mirror` connections inject the
/// slots `api_key` and `other_key`, both bound to the group secret `api-key`.
async fn shared_install(fixture: &Fixture) -> (PluginInstall, aruna_plugins::manifest::Manifest) {
    use aruna_core::plugin_installs::SecretBinding;
    let install = bound(fixture).await;
    secret(fixture, &install).await;
    let slots = serde_json::json!({
        "secrets": {
            "api_key": { "title": "API key", "kind": "bearer" },
            "other_key": { "title": "Mirror key", "kind": "bearer" }
        },
        "connections": {
            "catalog": { "url": "{catalog_url}/api/", "auth": "api_key" },
            "mirror": { "url": "{catalog_url}/mirror/", "auth": "other_key" }
        }
    });
    let digest = fixture.publish("0.3.0", "ee", slots).await;
    let scope = super::super::grants::InstallScope {
        realm_id: install.account.realm_id,
        group_id: install.group_id,
        install_id: install.install_id,
    };
    edit_config(fixture, |config| {
        let held = &mut config.installs[0];
        (held.digest, held.version) = (digest.clone(), "0.3.0".to_string());
        let binding = SecretBinding::parse("group:api-key").unwrap();
        held.secrets = [("api_key", &binding), ("other_key", &binding)]
            .map(|(slot, binding)| (slot.to_string(), binding.clone()))
            .into();
        let paths = ["catalog", "mirror"].map(|name| scope.connection(name));
        held.grants
            .insert("connections.send".to_string(), paths.to_vec());
    })
    .await;
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    let plugin = crate::plugins::install::pick_version(&config, &install.plugin, Some(&digest));
    let manifest = crate::plugins::install::read_manifest(&plugin.unwrap()).unwrap();
    (config.installs[0].clone(), manifest)
}

#[tokio::test]
async fn shared_secret_slots() {
    let fixture = Fixture::keyed().await;
    let (install, manifest) = shared_install(&fixture).await;
    ensure_rules(&fixture.ctx, &fixture.actor, &install, &manifest)
        .await
        .unwrap();
    // Both slots use the one secret, so it keeps the rules of both connections.
    let paths: Vec<Vec<String>> = (rules(&fixture, &install).await.into_iter())
        .map(|rule| rule.paths)
        .collect();
    assert_eq!(
        paths,
        [vec!["/api".to_string()], vec!["/mirror".to_string()]]
    );
}

#[tokio::test]
async fn overlapping_rules_join() {
    let fixture = Fixture::keyed().await;
    let (mut install, _) = shared_install(&fixture).await;
    let fields = |pointers: &[&str]| ResponseExposure::Fields {
        pointers: pointers.iter().map(|pointer| pointer.to_string()).collect(),
        headers: Vec::new(),
    };
    let manifest: aruna_plugins::manifest::Manifest = serde_json::from_value(serde_json::json!({
        "name": "announcer", "version": "0.4.0",
        "settings": { "catalog_url": { "title": "Catalog", "type": "url" } },
        "secrets": { "api_key": { "title": "API key", "kind": "bearer" } },
        "connections": {
            "ids": { "url": "{catalog_url}/api/", "auth": "api_key", "response": ["/id"] },
            "names": { "url": "{catalog_url}/api/", "auth": "api_key", "response": ["/name"] },
            "deep": { "url": "{catalog_url}/api/v2/", "auth": "api_key", "response": ["/z"] },
            "whole": { "url": "{catalog_url}/files/", "auth": "api_key", "response": "full" },
            "brief": { "url": "{catalog_url}/files/", "auth": "api_key", "response": ["/id"] }
        }
    }))
    .unwrap();
    let scope = super::super::grants::InstallScope {
        realm_id: install.account.realm_id,
        group_id: install.group_id,
        install_id: install.install_id,
    };
    let names = manifest
        .connections
        .keys()
        .map(|name| scope.connection(name));
    install
        .grants
        .insert("connections.send".to_string(), names.collect());
    let name = SecretName::try_from("api-key".to_string()).unwrap();
    let wanted = wanted_rules(&install, &manifest).unwrap();
    let shown: Vec<(Vec<String>, ResponseExposure)> = (wanted[&name].iter())
        .map(|rule| (rule.paths.clone(), rule.response.clone()))
        .collect();
    // Identical selectors join into one rule; a nested prefix shows what the outer one shows,
    // and the reverse, so the first matching rule never hides a connection's fields.
    let joined = fields(&["/id", "/name", "/z"]);
    assert_eq!(
        shown,
        [
            (vec!["/files".to_string()], ResponseExposure::Full),
            (vec!["/api/v2".to_string()], joined.clone()),
            (vec!["/api".to_string()], joined),
        ]
    );
}
