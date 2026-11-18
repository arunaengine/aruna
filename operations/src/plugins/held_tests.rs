//! Tests the run side of releases: held fields are reused only while the value is unchanged,
//! a refusal reaches the run, and a management receiver releases group secrets itself.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::cell::RefCell;

use aruna_core::compute::Secret;
use aruna_core::secrets::{Delivery, ResponseExposure, SecretKind, SecretSettings};

use super::*;
use crate::plugins::bound::tests::{ORIGIN, TOKEN, bound, hold_keys, secret};
use crate::plugins::calls::*;
use crate::plugins::install::tests::Fixture;
use aruna_core::plugins::Refusal;

impl HeldSecrets {
    /// One checked use of a slot, as a run makes it.
    async fn obtain<F: Future<Output = Result<Released, ReleaseError>>>(
        &mut self,
        mut request: ReleaseRequest,
        ask: impl FnOnce(ReleaseRequest) -> F,
    ) -> Result<(Released, SecretFields), ReleaseError> {
        self.prepare(&mut request);
        let slot = request.slot.clone();
        self.absorb(&slot, ask(request).await?)
    }
}

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
    let mut held = HeldSecrets::default();
    let ask = |asked| ask_release(&fixture.ctx, &config, local, asked);
    let (_, fields) = held.obtain(first, ask).await.unwrap();
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
