//! Tests webhook admission: verified requests queue runs without credential headers, a delivery
//! id starts one run, and a failed check, a paused install or a header clash starts nothing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::plugin_installs::{PluginInstall, SecretBinding};
use aruna_core::plugin_runs::PluginRun;
use serde_json::json;

use super::*;
use crate::plugins::bound::tests::{TOKEN, bound, edit_config, hook_secret, keys};
use crate::plugins::install::tests::Fixture;
use crate::plugins::outbound::tests::leaks;
use crate::plugins::runs::list_runs;

/// An install moved to a version whose webhook the `header` slot `hook_key` verifies, bound to
/// the group secret `hook-key` that checks `x-signature`.
async fn hooked(fixture: &Fixture) -> PluginInstall {
    let install = bound(fixture).await;
    let hook = json!({
        "secrets": { "hook_key": { "title": "Hook key", "kind": "header" } },
        "triggers": { "webhook": {
            "verify": "hook_key", "delivery_id": "x-delivery", "handler": "on_webhook"
        } }
    });
    let digest = fixture.publish("0.3.0", "ef", hook).await;
    let binding = SecretBinding::parse("group:hook-key").unwrap();
    edit_config(fixture, |config| {
        let held = &mut config.installs[0];
        (held.digest, held.version) = (digest, "0.3.0".to_string());
        held.secrets = BTreeMap::from([("hook_key".to_string(), binding)]);
    })
    .await;
    hook_secret(fixture, &install, "x-signature", 0).await;
    let config = read_config(&fixture.ctx, fixture.actor.realm_id).await;
    config.unwrap().installs.into_iter().next().unwrap()
}

fn request(signature: &str, delivery: Option<&str>) -> HookRequest {
    let headers = [
        ("x-signature", signature),
        ("cookie", "canary-cookie"),
        ("authorization", "Bearer canary-auth"),
        ("x-aruna-relay-hop", "1"),
        ("x-custom", "kept"),
    ];
    let headers = (headers.into_iter())
        .chain(delivery.map(|id| ("x-delivery", id)))
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
    HookRequest {
        delivery_id: None,
        method: "POST".to_string(),
        query: "a=1".to_string(),
        headers,
        body: b"{\"event\":\"push\"}".to_vec(),
    }
}

async fn runs(fixture: &Fixture, install: &PluginInstall) -> Vec<PluginRun> {
    let listed = list_runs(&fixture.ctx, install.install_id, None, 100).await;
    listed.unwrap().0
}

#[tokio::test]
async fn admits_verified() {
    let fixture = Fixture::keyed().await;
    let install = hooked(&fixture).await;
    let keys = keys();
    let ids = (fixture.actor.realm_id, install.install_id);
    let admit = |request| admit_hook(&fixture.ctx, (install.receiver, &keys), ids, request, 10);
    // Without a delivery id every request is a run; one id starts one run.
    assert_eq!(admit(request(TOKEN, None)).await, Ok(true));
    assert_eq!(admit(request(TOKEN, None)).await, Ok(true));
    assert_eq!(admit(request(TOKEN, Some("d-1"))).await, Ok(true));
    assert_eq!(admit(request(TOKEN, Some("d-1"))).await, Ok(false));
    // A failed check starts nothing and records no delivery id.
    let wrong = admit(request("canary-wrong", Some("d-2"))).await;
    assert_eq!(wrong, Err(HookError::Unverified));
    assert_eq!(runs(&fixture, &install).await.len(), 3);
    assert_eq!(admit(request(TOKEN, Some("d-2"))).await, Ok(true));
    let held = runs(&fixture, &install).await;
    let hooks: Vec<&HookRequest> = (held.iter())
        .filter_map(|run| match &run.trigger {
            RunTrigger::Webhook(hook) => Some(hook),
            _ => None,
        })
        .collect();
    assert_eq!(hooks.len(), 4);
    let first = hooks
        .iter()
        .find(|hook| hook.delivery_id.as_deref() == Some("d-1"));
    let names: Vec<&str> = (first.unwrap().headers.iter())
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(names, ["x-custom", "x-delivery"]);
    let stored = format!("{held:?}");
    for canary in [TOKEN, "canary-cookie", "canary-auth", "canary-wrong"] {
        assert!(!stored.contains(canary), "{canary}");
        assert_eq!(
            leaks(&fixture, canary).await,
            Vec::<&str>::new(),
            "{canary}"
        );
    }
}

#[tokio::test]
async fn refusals_start_nothing() {
    let fixture = Fixture::keyed().await;
    let install = hooked(&fixture).await;
    let keys = keys();
    let place = (install.receiver, &keys);
    let realm_id = fixture.actor.realm_id;
    let admit = |install_id| {
        let ctx = &fixture.ctx;
        admit_hook(
            ctx,
            place,
            (realm_id, install_id),
            request(TOKEN, Some("d")),
            10,
        )
    };
    assert_eq!(
        admit(Ulid::from_parts(1, 1)).await,
        Err(HookError::NotFound)
    );
    // An edit that makes the checked header the delivery id fails closed.
    hook_secret(&fixture, &install, "x-delivery", 1).await;
    let clash = admit(install.install_id).await;
    assert!(matches!(clash, Err(HookError::Unavailable(_))), "{clash:?}");
    hook_secret(&fixture, &install, "x-signature", 2).await;
    edit_config(&fixture, |config| {
        config.installs[0].state = InstallState::Paused
    })
    .await;
    let paused = admit(install.install_id).await;
    assert_eq!(paused, Err(HookError::Paused("paused")));
    assert!(runs(&fixture, &install).await.is_empty());
    // A relay keeps `authorization` only for a verify secret that checks it.
    let config = read_config(&fixture.ctx, realm_id).await.unwrap();
    let place = (fixture.actor.node_id, install.install_id);
    assert_eq!(
        checks_authorization(&fixture.ctx, &config, place).await,
        Some(false)
    );
    hook_secret(&fixture, &install, "Authorization", 3).await;
    assert_eq!(
        checks_authorization(&fixture.ctx, &config, place).await,
        Some(true)
    );
}

/// Writes the node secret `hook-key` of kind `header` on this node, checking `checked`.
async fn node_secret(fixture: &Fixture, checked: &str, round: u64) {
    use crate::driver::drive;
    use crate::secrets::SecretChange;
    use crate::secrets::node::{NodeChange, NodeSecretOperation};
    use aruna_core::compute::Secret;
    use aruna_core::secrets::{SecretFields, SecretKind, SecretName, SecretSettings};
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
    let input = NodeChange {
        realm_id: fixture.actor.realm_id,
        node_id: fixture.actor.node_id,
        actor: fixture.actor.user_id,
        change,
        now_ms: 1 + round,
        write_id: Ulid::from_parts(70 + round, 1),
    };
    drive(NodeSecretOperation::new(input), &fixture.ctx)
        .await
        .unwrap();
}

#[tokio::test]
async fn node_checks_relay() {
    let fixture = Fixture::keyed().await;
    let install = hooked(&fixture).await;
    edit_config(&fixture, |config| {
        let binding = SecretBinding::parse("node:hook-key").unwrap();
        config.installs[0].secrets = BTreeMap::from([("hook_key".to_string(), binding)]);
    })
    .await;
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    let relay = |local| checks_authorization(&fixture.ctx, &config, (local, install.install_id));
    let here = fixture.actor.node_id;
    // The receiver holds no such node secret yet, so the relay cannot tell and forwards nothing.
    assert_eq!(relay(here).await, None);
    fixture
        .ctx
        .storage_handle
        .open_vault(aruna_core::node_vault::NodeVaultKey::random());
    node_secret(&fixture, "x-signature", 0).await;
    assert_eq!(relay(here).await, Some(false));
    node_secret(&fixture, "Authorization", 1).await;
    assert_eq!(relay(here).await, Some(true));
    // A relaying node that cannot reach the receiver cannot tell either.
    let elsewhere = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
    assert_eq!(relay(elsewhere).await, None);
}
