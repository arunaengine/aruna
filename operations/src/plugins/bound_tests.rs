//! Tests selecting a bound secret: a group secret is found, opened and matched to the manifest's
//! slot kind; an absent or realm secret without its record selects nothing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::compute::Secret;
use aruna_core::secrets::Delivery;
use aruna_core::secrets::ResponseExposure;
use aruna_core::secrets::SecretFields;
use serde_json::json;

use super::*;
use crate::driver::drive;
use crate::plugins::calls::*;
use crate::plugins::install::tests::Fixture;
use crate::plugins::install::{pick_version, read_manifest};
use crate::plugins::secret_use::slot_kind;
#[allow(unused_imports)]
pub(crate) use crate::plugins::test_support::{
    ORIGIN, TOKEN, bound, edit_config, hold_keys, rule_for, secret, secret_with,
};
use crate::secrets::SecretChange;
use crate::secrets::owner::{OwnerChange, OwnerSecretOperation};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugins::{Refusal, ReleaseError};
use aruna_core::secrets::UseRule;
use ulid::Ulid;

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
    let missing = open(&fixture.ctx, config.realm_id, &selected.sealed).await;
    assert!(matches!(missing, Err(ReleaseError::Unavailable(_))));
    hold_keys(&fixture.ctx);
    let opened = open(&fixture.ctx, config.realm_id, &selected.sealed).await;
    let fields = SecretFields::decode(&opened.unwrap());
    assert_eq!(fields.unwrap().get("token"), Some(TOKEN));
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
        Ok(None)
    );
    let basic = SecretKind::parse("basic").unwrap();
    let other = check_node_binding(&fixture.ctx, mine, &name, &basic).await;
    assert_eq!(other, Err(NodeCheckError::OtherKind));
    // Another receiver is asked over the transport, which this fixture does not run.
    let receiver = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
    let remote = check_node_binding(&fixture.ctx, (local, receiver), &name, &bearer).await;
    assert_eq!(remote, Err(NodeCheckError::Unavailable));
    // Only a management node may ask a receiver.
    let place = (fixture.actor.realm_id, local);
    let check = |peer| ServeNodeCheck::new(place, peer, name.clone(), bearer.clone());
    let served = drive(check(receiver), &fixture.ctx).await;
    assert_eq!(served, Err(NodeCheckFailure(NodeCheckError::Forbidden)));
    assert_eq!(drive(check(local), &fixture.ctx).await, Ok(None));
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

/// Writes the group secret `hook-key` of kind `header`, checking the header `checked`, with
/// the `verify` rule of `install`.
pub(crate) async fn hook_secret(
    fixture: &Fixture,
    install: &PluginInstall,
    checked: &str,
    round: u64,
) {
    let name = SecretName::try_from("hook-key".to_string()).unwrap();
    let settings = SecretSettings::from([("header".to_string(), checked.to_string())]);
    let uses = vec![UseRule {
        delivery: Delivery::Verify,
        origins: Vec::new(),
        paths: Vec::new(),
        methods: Vec::new(),
        ..rule_for(install, ResponseExposure::Status)
    }];
    let change = match round {
        0 => SecretChange::Create {
            name,
            kind: SecretKind::parse("header").unwrap(),
            description: String::new(),
            settings,
            uses,
            fields: SecretFields::new([("value".to_string(), Secret::new(TOKEN))].into()),
        },
        _ => SecretChange::Replace {
            name,
            description: String::new(),
            settings,
            uses,
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

mod operations {
    use aruna_core::plugins::NodeCheckError;

    use super::*;
    use crate::document_repository::GetConfigError;
    use crate::metadata::protocol::MetadataTransportMessage;
    use crate::plugins::entry::EntryError;
    use crate::plugins::fixtures::{
        self, config_result, is_child, is_effect, node, peer_says, peer_silent,
    };
    use crate::plugins::rows::tests::{child_result, values_result};

    fn key() -> (SecretName, SecretKind) {
        let name = SecretName::try_from("key".to_string()).unwrap();
        (name, SecretKind::parse("bearer").unwrap())
    }

    fn checked(result: Result<Option<String>, NodeCheckFailure>) -> IoResult {
        child_result(result)
    }

    fn slots(requested: &[(&str, &str)], receiver: Option<NodeId>) -> BindSlots {
        let extra = json!({
            "secrets": { "hook_key": { "title": "Hook key", "kind": "bearer" } }
        });
        let plugin = fixtures::plugin(extra);
        let manifest = read_manifest(&plugin).unwrap();
        let install = fixtures::install();
        let place = BindPlace {
            group_id: install.group_id,
            local: Some(node(3)),
            receiver,
        };
        let requested = (requested.iter())
            .map(|(slot, text)| (slot.to_string(), text.to_string()))
            .collect();
        let config = Arc::new(fixtures::config(&install, json!({})));
        BindSlots::new(config, place, &manifest, &requested)
    }

    #[test]
    fn slots_refuse_unknown() {
        let mut bind = slots(&[("other", "group:key")], None);
        assert!(matches!(bind.first(), Err(BindError::Invalid(_))));
        let mut bind = slots(&[("hook_key", "node:key")], None);
        assert!(matches!(bind.first(), Err(BindError::Unavailable(_))));
    }

    #[test]
    fn slots_read_group() {
        let mut bind = slots(&[], None);
        assert!(is_child(&bind.first()));
        let none = child_result(Ok::<Option<SecretEntry>, EntryError>(None));
        assert!(matches!(bind.next(none), Err(BindError::Invalid(_))));
    }

    #[test]
    fn slots_check_node() {
        let requested = [("hook_key", "node:key")];
        let mut bind = slots(&requested, Some(node(1)));
        assert!(is_child(&bind.first()));
        let Ok(Next::Done((bound, warnings))) = bind.next(checked(Ok(None))) else {
            panic!("a checked node secret binds the slot");
        };
        assert!(bound.contains_key("hook_key"));
        assert_eq!(warnings.len(), 1);
        let mut silent = slots(&requested, Some(node(1)));
        silent.first().unwrap();
        let failed = NodeCheckFailure(NodeCheckError::Unavailable);
        let answer = silent.next(checked(Err(failed)));
        assert!(matches!(answer, Err(BindError::Unavailable(_))));
        let mut missing = slots(&requested, Some(node(1)));
        missing.first().unwrap();
        let failed = NodeCheckFailure(NodeCheckError::Missing);
        assert!(matches!(
            missing.next(checked(Err(failed))),
            Err(BindError::Invalid(_))
        ));
    }

    #[test]
    fn node_secret_missing() {
        let (name, kind) = key();
        let mut fits = NodeSecretFits::new(&name, &kind);
        assert!(matches!(fits.first(), Ok(Next::Call(IoCall::Read(_)))));
        let missing = NodeCheckFailure(NodeCheckError::Missing);
        assert_eq!(fits.next(values_result(vec![None])), Err(missing));
    }

    #[test]
    fn binding_asks_receiver() {
        let (name, kind) = key();
        let mut here = CheckNodeBinding::new((node(3), node(3)), &name, &kind);
        assert!(is_child(&here.first()));
        let header = Some("x-key".to_string());
        let answer = here.next(checked(Ok(header.clone())));
        assert_eq!(answer, Ok(Next::Done(header.clone())));
        let mut there = CheckNodeBinding::new((node(3), node(1)), &name, &kind);
        assert!(is_effect(&there.first()));
        let result = Ok(header.clone());
        let said = peer_says(
            node(1),
            &MetadataTransportMessage::NodeSecretChecked { result },
        );
        assert_eq!(there.next(said), Ok(Next::Done(header)));
        let silent = NodeCheckFailure(NodeCheckError::Unavailable);
        assert_eq!(there.next(peer_silent(node(1))), Err(silent));
    }

    #[test]
    fn serve_refuses_devices() {
        let (name, kind) = key();
        let place = (fixtures::realm(), node(1));
        let mut device = ServeNodeCheck::new(place, node(4), name.clone(), kind.clone());
        assert!(is_child(&device.first()));
        let refused = NodeCheckFailure(NodeCheckError::Forbidden);
        let answer = device.next(config_result(fixtures::realm_nodes()));
        assert_eq!(answer, Err(refused));
        let mut server = ServeNodeCheck::new(place, node(3), name, kind);
        server.first().unwrap();
        assert!(is_child(
            &server.next(config_result(fixtures::realm_nodes()))
        ));
        assert_eq!(server.next(checked(Ok(None))), Ok(Next::Done(None)));
        let mut lost = ServeNodeCheck::new(place, node(3), key().0, key().1);
        lost.first().unwrap();
        let failed = child_result(Err::<RealmConfigDocument, _>(GetConfigError::NotFinished));
        let unavailable = NodeCheckFailure(NodeCheckError::Unavailable);
        assert_eq!(lost.next(failed), Err(unavailable));
    }
}
