//! Tests the release path: a bound group secret reaches the install's receiver once every check
//! passes, each failed check refuses at its step with an audit row, and a held value stays home.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::secrets::SecretFields;
use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AuditIndex, audit_prefix};

use super::*;
use crate::plugins::bound::tests::{ORIGIN, TOKEN, bound, edit_config, keys, secret};
use crate::plugins::install::read_config;
use crate::plugins::install::tests::Fixture;

fn request(install: &PluginInstall, url: &str) -> ReleaseRequest {
    ReleaseRequest {
        install_id: install.install_id,
        slot: "api_key".to_string(),
        delivery: Delivery::Inject,
        target: Some(("POST".to_string(), url.to_string())),
        held: None,
        run_id: Some(Ulid::from_parts(5, 5)),
    }
}

async fn released(
    fixture: &Fixture,
    peer: NodeId,
    request: &ReleaseRequest,
) -> Result<Released, ReleaseError> {
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    release(&fixture.ctx, &keys(), (&config, peer), request).await
}

/// The use audit actions this node recorded, oldest first.
async fn audited(fixture: &Fixture) -> Vec<(AdminAuditAction, Option<String>)> {
    let txn = Txn::direct(&fixture.ctx);
    let rows = txn
        .scan(ADMIN_AUDIT_KEYSPACE, &audit_prefix(AuditIndex::Realm))
        .await
        .unwrap();
    (rows.iter())
        .filter_map(|(_, value)| AdminAuditRecord::from_bytes(value).ok())
        .filter_map(|record| Some((record.action, record.secret_use?.refused)))
        .collect()
}

#[tokio::test]
async fn releases_bound_secret() {
    let fixture = Fixture::keyed().await;
    let install = bound(&fixture).await;
    secret(&fixture, &install).await;
    let receiver = install.receiver;
    let mut asked = request(&install, &format!("{ORIGIN}/api/records"));
    let answer = released(&fixture, receiver, &asked).await.unwrap();
    let fields = SecretFields::decode(answer.fields.as_ref().unwrap()).unwrap();
    assert_eq!(fields.get("token"), Some(TOKEN));
    assert_eq!(
        (answer.kind.as_str(), &answer.exposure),
        ("bearer", &ResponseExposure::Status)
    );
    // A receiver that holds this value gets the check and the row, not the bytes again.
    asked.held = Some(answer.value_id);
    let again = released(&fixture, receiver, &asked).await.unwrap();
    assert_eq!((again.fields, again.value_id), (None, answer.value_id));
    let rows = audited(&fixture).await;
    let release = (AdminAuditAction::SecretReleased, None);
    assert_eq!(rows, [release.clone(), release]);
    assert!(!format!("{answer:?}").contains(TOKEN));
}

#[tokio::test]
async fn refusals_per_step() {
    let fixture = Fixture::keyed().await;
    let install = bound(&fixture).await;
    secret(&fixture, &install).await;
    let receiver = install.receiver;
    let good = request(&install, &format!("{ORIGIN}/api/records"));
    let refused = |refusal| Err(ReleaseError::Refused(refusal));
    let other = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
    assert_eq!(
        released(&fixture, other, &good).await,
        refused(Refusal::Peer)
    );
    let mut unbound = good.clone();
    unbound.slot = "other".to_string();
    assert_eq!(
        released(&fixture, receiver, &unbound).await,
        refused(Refusal::Binding)
    );
    let elsewhere = request(&install, "https://elsewhere.example.org/api/");
    assert_eq!(
        released(&fixture, receiver, &elsewhere).await,
        refused(Refusal::Rule)
    );
    fixture.deny(&["operation == 'secret.use'"]).await;
    assert_eq!(
        released(&fixture, receiver, &good).await,
        refused(Refusal::Policy)
    );
    fixture.deny(&[]).await;
    edit_config(&fixture, |config| {
        config.installs[0].state = InstallState::Paused
    })
    .await;
    assert_eq!(
        released(&fixture, receiver, &good).await,
        refused(Refusal::Authority)
    );
    edit_config(&fixture, |config| {
        config.installs[0].state = InstallState::Active;
        config.installs[0].account = UserId::local(Ulid::from_parts(9, 9), config.realm_id);
    })
    .await;
    assert_eq!(
        released(&fixture, receiver, &good).await,
        refused(Refusal::Authority)
    );
    let steps: Vec<Option<String>> = audited(&fixture)
        .await
        .into_iter()
        .map(|row| row.1)
        .collect();
    let expected = ["peer", "rule", "policy", "authority", "authority"];
    assert_eq!(steps, expected.map(|step| Some(step.to_string())));
}
