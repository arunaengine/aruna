//! Tests bringing requests and statements to a set's publisher.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::events::{MetadataAnswer, PeerRequestError};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::distribution::Statement;
use aruna_core::types::Value;
use std::time::Duration;

use super::*;
use crate::distribution::test_record::{member, realm, record, secret};
use crate::metadata::protocol::encode_message;

fn node(seed: u8) -> NodeId {
    secret(seed).public()
}

fn ids() -> NodeRealm {
    Some((node(1), realm()))
}

fn revision() -> DistributionRevision {
    record(true).0.revision()
}

fn statement() -> Statement {
    let (snapshot, _) = record(true);
    let signer = snapshot.member(member(1)).unwrap();
    let range = &snapshot.config.ranges[0];
    Statement::split(
        realm(),
        snapshot.set_id(),
        signer,
        snapshot.number,
        range,
        "f",
    )
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn answered(message: MetadataTransportMessage) -> IoResult {
    answer(Ok(encode_message(&message).unwrap()))
}

fn answer(result: Result<Vec<u8>, PeerRequestError>) -> IoResult {
    let answer = MetadataAnswer {
        peer: node(2),
        elapsed: Duration::ZERO,
        result,
    };
    IoResult::Event(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
        answer,
    )))))
}

fn accepted(result: Revision) -> IoResult {
    answered(MetadataTransportMessage::DistributionAccepted { result })
}

fn auth() -> AuthContext {
    AuthContext {
        user_id: aruna_core::UserId::nil(realm()),
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    }
}

fn publish() -> PublishLocalOperation {
    let ids_of = (Ulid::from_bytes([1; 16]), Ulid::from_bytes([2; 16]));
    PublishLocalOperation::new(ids(), ids_of, DistributionRequest::Activate, None)
}

fn page(rows: usize) -> IoResult {
    let rows = (0..rows)
        .map(|_| (Key::from(vec![1]), Value::from(vec![1])))
        .collect();
    child(Ok::<_, IoError>(IoResult::Page { rows, next: None }))
}

// A committed publish wakes the drain only while a set waits for its bootstrap.
#[test]
fn wakes_waiting_bootstrap() {
    let mut waiting = publish();
    assert!(matches!(waiting.first(), Ok(Next::Call(IoCall::Child(_)))));
    let done = child(Ok::<_, PublishError>(revision()));
    assert!(matches!(
        waiting.next(done),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let woken = waiting.next(page(1));
    assert_eq!(
        woken,
        Ok(Next::Then {
            output: revision(),
            calls: vec![IoCall::from(schedule_blob_drain())],
        })
    );
    let mut idle = publish();
    idle.first().unwrap();
    idle.next(child(Ok::<_, PublishError>(revision()))).unwrap();
    assert_eq!(idle.next(page(0)), Ok(Next::Done(revision())));
}

// A failed look for waiting sets never undoes the committed change.
#[test]
fn failed_look_keeps() {
    let mut publish = publish();
    publish.first().unwrap();
    publish
        .next(child(Ok::<_, PublishError>(revision())))
        .unwrap();
    let failed = child(Err::<IoResult, _>(IoError::Unfinished));
    assert_eq!(publish.next(failed), Ok(Next::Done(revision())));
}

// A publisher error becomes the refusal callers act on, and no transport refuses at once.
#[test]
fn maps_publish_failures() {
    let mut refused = publish();
    refused.first().unwrap();
    let error = child(Err::<DistributionRevision, _>(PublishError::Unauthorized));
    assert_eq!(refused.next(error), Err(PublishRefusal::Forbidden));
    let ids_of = (Ulid::nil(), Ulid::nil());
    let request = DistributionRequest::Activate;
    let mut bare = PublishLocalOperation::new(None, ids_of, request, None);
    assert_eq!(bare.first(), Err(no_transport()));
}

// A statement goes to the publisher's node, and one it already embedded counts as done.
#[test]
fn submits_to_publisher() {
    let signed = statement().sign(&secret(1));
    let mut remote = SubmitOperation::new(ids(), node(2), signed.clone());
    assert!(matches!(remote.first(), Ok(Next::Call(IoCall::Effect(_)))));
    let replayed = accepted(Err(PublishRefusal::Replayed));
    assert_eq!(remote.next(replayed), Ok(Next::Done(())));
    let mut local = SubmitOperation::new(ids(), node(1), signed.clone());
    assert!(matches!(local.first(), Ok(Next::Call(IoCall::Child(_)))));
    let refused = child(Err::<DistributionRevision, _>(PublishRefusal::Forbidden));
    assert_eq!(local.next(refused), Err(PublishRefusal::Forbidden));
    let mut lost = SubmitOperation::new(ids(), node(2), signed);
    lost.first().unwrap();
    let silent = answer(Err(PeerRequestError::TimedOut));
    assert!(matches!(
        lost.next(silent),
        Err(PublishRefusal::Unavailable(_))
    ));
}

// A forwarded admin request needs the caller's bearer token.
#[test]
fn admin_needs_bearer() {
    let mut send = SendAdminOperation::new(
        (ids(), node(2)),
        Ulid::nil(),
        (auth(), None),
        DistributionRequest::Activate,
    );
    assert_eq!(send.first(), Err(PublishRefusal::Unauthorized));
    let token = AuthToken::bearer("token").unwrap();
    let mut sent = SendAdminOperation::new(
        (ids(), node(2)),
        Ulid::nil(),
        (auth(), Some(token)),
        DistributionRequest::Activate,
    );
    assert!(matches!(sent.first(), Ok(Next::Call(IoCall::Effect(_)))));
    assert_eq!(
        sent.next(accepted(Ok(revision()))),
        Ok(Next::Done(revision()))
    );
}

// Signing failures and refusals are reported, so each caller logs them its own way.
#[test]
fn states_signed_claims() {
    let claim = StatementClaim {
        signer: node(1),
        statement: statement(),
    };
    let mut state = StateOperation::new(ids(), node(2), claim.clone());
    let sign = Effect::Net(NetEffect::StatementSign(Box::new(claim.clone())));
    assert_eq!(state.first(), Ok(Next::Call(IoCall::from(sign))));
    let event = |signed| IoResult::Event(Box::new(Event::Net(NetEvent::StatementSign(signed))));
    let unsigned = event(StatementSignEvent::Unavailable("no key".to_string()));
    let ended = Next::Done(Stated::Unsigned("no key".to_string()));
    assert_eq!(state.next(unsigned), Ok(ended));

    let mut taken = StateOperation::new(ids(), node(2), claim);
    taken.first().unwrap();
    let signed = Box::new(statement().sign(&secret(1)));
    let submit = taken.next(event(StatementSignEvent::Signed(signed)));
    assert!(matches!(submit, Ok(Next::Call(IoCall::Child(_)))));
    let refusal = PublishRefusal::Forbidden;
    let refused = child(Err::<(), _>(refusal.clone()));
    assert_eq!(
        taken.next(refused),
        Ok(Next::Done(Stated::Refused(refusal)))
    );
}
