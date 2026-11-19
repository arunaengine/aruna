//! Tests the run side of releases: held fields are reused only while the value is unchanged,
//! a refusal reaches the run, and a management receiver releases group secrets itself.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::Secret;
use aruna_core::secrets::{Delivery, ResponseExposure, SecretKind, SecretSettings};

use super::*;
use crate::plugins::bound::tests::{ORIGIN, TOKEN, bound, hold_keys, secret};
use crate::plugins::calls::*;
use crate::plugins::install::tests::Fixture;
use aruna_core::plugins::Refusal;

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
    let local = fixture.actor.node_id;
    // Without the realm keys the node has nobody else to ask in this realm.
    let asked = ReleaseRequest {
        install_id: install.install_id,
        target: Some(("POST".to_string(), format!("{ORIGIN}/api/records"))),
        ..request("api_key")
    };
    let alone = ask_release(&fixture.ctx, &config, local, asked).await;
    assert!(matches!(alone, Err(ReleaseError::Unavailable(_))));
    hold_keys(&fixture.ctx);
    let released = ask_release(&fixture.ctx, &config, local, first).await;
    let (_, fields) = fresh_fields(released.unwrap()).unwrap();
    assert_eq!(fields.get("token"), Some(TOKEN));
}

mod operations {
    use serde_json::json;

    use super::*;
    use crate::plugins::fixtures::{
        self, config_result, is_child, is_effect, node, peer_says, peer_silent,
    };
    use crate::plugins::rows::tests::child_result;
    use crate::plugins::secret_use::ServeRelease;
    use crate::plugins::work::tests::proposed;

    fn released_by(result: Result<Released, ReleaseError>) -> MetadataTransportMessage {
        MetadataTransportMessage::SecretReleased { result }
    }

    fn unavailable() -> ReleaseError {
        ReleaseError::Unavailable("no".into())
    }

    fn plugin_answer(event: PluginEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Plugin(event)))
    }

    fn obtain(run_id: Ulid) -> ObtainSecret {
        let place = (Arc::new(fixtures::realm_nodes()), node(3));
        let holder = Ulid::from_parts(8, 1);
        let mut obtain = ObtainSecret::new(place, (run_id, holder), request("a"));
        let slot = "a".to_string();
        let held = PluginEffect::HeldValue {
            run_id,
            holder,
            slot,
        };
        let held = Effect::Plugin(held);
        assert_eq!(
            obtain.first(),
            Ok(Next::Call(IoCall::Effect(Box::new(held))))
        );
        obtain
    }

    #[test]
    fn obtain_keeps_fields() {
        let run_id = Ulid::from_parts(9, 1);
        let mut obtain = obtain(run_id);
        let value_id = Ulid::from_parts(1, 1);
        let held = plugin_answer(PluginEvent::Held(Some(value_id)));
        assert!(
            is_child(&obtain.next(held)),
            "the release names the held value"
        );
        // The check passed again for the held value, so its fields come from the run.
        let released = child_result(Ok::<_, ReleaseFailure>(answer(1, None)));
        let Ok(Next::Call(IoCall::Effect(hold))) = obtain.next(released) else {
            panic!("the release is kept for the run");
        };
        let slot = "a".to_string();
        let fields = None;
        let expected = PluginEffect::HoldValue {
            run_id,
            holder: Ulid::from_parts(8, 1),
            slot,
            value_id,
            fields,
        };
        assert_eq!(*hold, Effect::Plugin(expected));
        let kept = plugin_answer(PluginEvent::Fields(answer(1, Some("one")).fields));
        let Ok(Next::Done((_, fields))) = obtain.next(kept) else {
            panic!("the held fields answer the use");
        };
        assert_eq!(fields.get("token"), Some("one"));
    }

    #[test]
    fn obtain_refuses_lost() {
        let mut lost = obtain(Ulid::from_parts(9, 2));
        lost.next(plugin_answer(PluginEvent::Held(None))).unwrap();
        let released = child_result(Ok::<_, ReleaseFailure>(answer(3, None)));
        lost.next(released).unwrap();
        let none = lost.next(plugin_answer(PluginEvent::Fields(None)));
        assert_eq!(none.unwrap_err().0.kind, ErrorKind::Unavailable);
        let mut refused = obtain(Ulid::from_parts(9, 3));
        refused
            .next(plugin_answer(PluginEvent::Held(None)))
            .unwrap();
        let rule: ReleaseFailure = ReleaseError::Refused(Refusal::Rule).into();
        let denied = refused.next(child_result(Err::<Released, _>(rule)));
        assert_eq!(denied.unwrap_err().0.kind, ErrorKind::Denied);
    }

    #[test]
    fn fresh_needs_fields() {
        let (_, fields) = fresh_fields(answer(1, Some("one"))).unwrap();
        assert_eq!(fields.get("token"), Some("one"));
        let lost = fresh_fields(answer(1, None));
        assert!(matches!(lost, Err(ReleaseError::Unavailable(_))));
    }

    fn ask(local: u8) -> AskRelease {
        AskRelease::new(Arc::new(fixtures::realm_nodes()), node(local), request("a"))
    }

    #[test]
    fn ask_tries_managers() {
        let mut release = ask(3);
        assert!(is_effect(&release.first()));
        let busy = peer_says(node(1), &released_by(Err(unavailable())));
        assert!(is_effect(&release.next(busy)));
        let done = peer_says(node(2), &released_by(Ok(answer(1, Some("one")))));
        assert!(matches!(release.next(done), Ok(Next::Done(_))));
    }

    #[test]
    fn ask_reports_silence() {
        let mut release = ask(3);
        release.first().unwrap();
        assert!(is_effect(&release.next(peer_silent(node(1)))));
        let answer = release.next(peer_silent(node(2)));
        assert!(matches!(
            answer,
            Err(ReleaseFailure(ReleaseError::Unavailable(_)))
        ));
    }

    #[test]
    fn manager_releases_first() {
        let mut release = ask(1);
        assert!(is_child(&release.first()));
        let here = child_result(Err::<Released, _>(ReleaseFailure(unavailable())));
        assert!(is_effect(&release.next(here)));
        let mut release = ask(1);
        release.first().unwrap();
        let here = child_result(Ok::<_, ReleaseFailure>(answer(1, Some("one"))));
        assert!(matches!(release.next(here), Ok(Next::Done(_))));
    }

    #[test]
    fn request_needs_context() {
        let install = fixtures::install();
        let mut row = proposed(&install, 1);
        row.context = None;
        let config = Arc::new(fixtures::config(&install, json!({})));
        let mut prepare = PrepareRequest::new(config, node(3), &install, &row);
        let Err(CallError(error)) = prepare.first() else {
            panic!("a row without a request is refused");
        };
        assert_eq!(error.kind, ErrorKind::Invalid);
    }

    #[test]
    fn serve_runs_release() {
        let place = (fixtures::realm(), node(1));
        let mut serve = ServeRelease::new(place, node(3), request("a"));
        assert!(is_child(&serve.first()));
        assert!(is_child(
            &serve.next(config_result(fixtures::realm_nodes()))
        ));
        let done = child_result(Ok::<_, ReleaseFailure>(answer(1, None)));
        assert!(matches!(serve.next(done), Ok(Next::Done(_))));
    }
}
