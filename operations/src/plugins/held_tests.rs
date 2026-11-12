//! Tests the run side of releases: held fields are reused only while the value is unchanged,
//! a refusal reaches the run, and a management receiver releases group secrets itself.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::cell::RefCell;

use aruna_core::compute::Secret;
use aruna_core::secrets::{Delivery, ResponseExposure, SecretKind, SecretSettings};

use super::*;
use crate::plugins::bound::tests::{ORIGIN, TOKEN, bound, keys, secret};
use crate::plugins::install::read_config;
use crate::plugins::install::tests::Fixture;
use crate::plugins::release::Refusal;

fn request(slot: &str) -> ReleaseRequest {
    ReleaseRequest {
        install_id: Ulid::from_parts(1, 1),
        slot: slot.to_string(),
        delivery: Delivery::Inject,
        target: None,
        held: None,
        run_id: None,
    }
}

fn answer(value_id: u128, token: Option<&str>) -> Released {
    let fields = token.map(|token| {
        let fields = SecretFields::new([("token".to_string(), Secret::new(token))].into());
        fields.encode().unwrap()
    });
    Released {
        kind: SecretKind::parse("bearer").unwrap(),
        settings: SecretSettings::new(),
        exposure: ResponseExposure::Status,
        value_id: Ulid::from_parts(1, value_id),
        fields,
    }
}

#[tokio::test]
async fn reuse_while_unchanged() {
    let mut held = HeldSecrets::default();
    let asked = RefCell::new(Vec::new());
    let ask = |reply: Result<Released, ReleaseError>| {
        let asked = &asked;
        move |request: ReleaseRequest| {
            asked.borrow_mut().push(request.held);
            std::future::ready(reply)
        }
    };
    let (_, first) = held
        .obtain(request("a"), ask(Ok(answer(1, Some("one")))))
        .await
        .unwrap();
    assert_eq!(first.get("token"), Some("one"));
    // The check passed again for the same value: the held fields are used.
    let (_, again) = held
        .obtain(request("a"), ask(Ok(answer(1, None))))
        .await
        .unwrap();
    assert_eq!(again.get("token"), Some("one"));
    // A rotated value arrives with its fields and replaces the held ones.
    let (_, rotated) = held
        .obtain(request("a"), ask(Ok(answer(2, Some("two")))))
        .await
        .unwrap();
    assert_eq!(rotated.get("token"), Some("two"));
    let unknown = held.obtain(request("a"), ask(Ok(answer(3, None)))).await;
    assert!(matches!(unknown, Err(ReleaseError::Unavailable(_))));
    let refused = Err(ReleaseError::Refused(Refusal::Rule));
    let denied = held.obtain(request("a"), ask(refused.clone())).await;
    assert_eq!(denied.map(|_| ()), refused.map(|_: Released| ()));
    let ids =
        [None, Some(1), Some(1), Some(2), Some(2)].map(|id| id.map(|id| Ulid::from_parts(1, id)));
    assert_eq!(asked.into_inner(), ids);
}

#[tokio::test]
async fn management_receiver() {
    let fixture = Fixture::keyed().await;
    let install = bound(&fixture).await;
    secret(&fixture, &install).await;
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    let first = ReleaseRequest {
        install_id: install.install_id,
        target: Some(("POST".to_string(), format!("{ORIGIN}/api/records"))),
        ..request("api_key")
    };
    let mut held = HeldSecrets::default();
    let realm_keys = keys();
    let local = (fixture.actor.node_id, &realm_keys);
    let ask = |asked| ask_release(&fixture.ctx, &config, local, asked);
    let (_, fields) = held.obtain(first, ask).await.unwrap();
    assert_eq!(fields.get("token"), Some(TOKEN));
    // Without the realm keys the node has nobody else to ask in this realm.
    let none = ReleaseKeys::default();
    let asked = ReleaseRequest {
        install_id: install.install_id,
        target: Some(("POST".to_string(), format!("{ORIGIN}/api/records"))),
        ..request("api_key")
    };
    let alone = ask_release(&fixture.ctx, &config, (fixture.actor.node_id, &none), asked).await;
    assert!(matches!(alone, Err(ReleaseError::Unavailable(_))));
}
