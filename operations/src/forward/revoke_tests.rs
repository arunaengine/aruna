//! Tests applying a forwarded token revocation as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::calls::{called, child, node, runs_child};
use aruna_core::UserId;
use aruna_core::structs::identity::realm::RealmNodeKind;
use ulid::Ulid;

const CALLER_TOKEN: &str = "caller-token";

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn user(seed: u8) -> UserId {
    UserId::local(Ulid::from_bytes([seed; 16]), realm())
}

fn caller() -> AuthContext {
    AuthContext {
        user_id: user(4),
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    }
}

fn restricted() -> AuthContext {
    AuthContext {
        path_restrictions: Some(Vec::new()),
        ..caller()
    }
}

fn claims(sub: UserId, iss: RealmId, exp: u64) -> TokenClaims {
    TokenClaims {
        sub: sub.to_string(),
        iss: iss.to_base64(),
        iat: 900,
        exp,
        jti: "token-id".to_string(),
        sid: None,
        session_kind: None,
        restrictions: None,
        issuer_pubkey: None,
        delegation_signature: None,
        name: None,
        via: None,
        auth_time: None,
    }
}

fn message(token: &str) -> MetadataTransportMessage {
    MetadataTransportMessage::ForwardTokenRevocation {
        auth_token: AuthToken::bearer(CALLER_TOKEN).unwrap(),
        token: Secret::new(token),
    }
}

fn config() -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 2);
    config.ensure_node(node(1), RealmNodeKind::Server);
    config
}

fn operation(message: MetadataTransportMessage) -> ApplyRevokeOperation {
    let local = Some((node(1), realm()));
    let mut operation = ApplyRevokeOperation::new(local, node(2), message);
    operation.stamp(1_000_000);
    assert!(runs_child(operation.first()));
    operation
}

fn config_read() -> IoResult {
    child(Ok::<_, GetConfigError>(config()))
}

/// Runs up to the verification of the named token, for this caller.
fn verifying(auth: AuthContext, token: &str) -> ApplyRevokeOperation {
    let mut operation = operation(message(token));
    assert!(runs_child(operation.next(config_read())));
    assert_eq!(operation.stage, ApplyStage::Caller);
    let IoCall::Effect(effect) = called(operation.next(child(Ok::<_, ForwardAuthError>(auth))))
    else {
        panic!("expected the token check");
    };
    assert!(matches!(
        *effect,
        Effect::Net(NetEffect::RevocationClaims(_))
    ));
    operation
}

fn verified(claims: TokenClaims) -> IoResult {
    let event = NetEvent::RevocationClaims(Ok(Box::new(claims)));
    IoResult::Event(Box::new(Event::Net(event)))
}

fn own_claims() -> TokenClaims {
    claims(user(4), realm(), 2_000)
}

fn other_claims() -> TokenClaims {
    claims(user(5), realm(), 2_000)
}

fn denied_with(error: WriteAuthError) -> Result<Next<MetadataTransportMessage>, IoError> {
    Ok(Next::Done(MetadataTransportMessage::ForwardedWriteDenied {
        error,
    }))
}

fn is_rejected(turn: Result<Next<MetadataTransportMessage>, IoError>) -> bool {
    matches!(turn, Ok(Next::Done(MetadataTransportMessage::Reject(_))))
}

fn revoking() -> ApplyRevokeOperation {
    let mut operation = verifying(caller(), "target");
    assert!(runs_child(operation.next(verified(own_claims()))));
    assert_eq!(operation.stage, ApplyStage::Revoke);
    operation
}

#[test]
fn missing_config_unavailable() {
    let missing = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
    assert_eq!(
        operation(message("target")).next(child(missing)),
        Ok(Next::Done(
            MetadataTransportMessage::ForwardedWriteUnavailable
        ))
    );
}

#[test]
fn other_message_rejected() {
    let mut operation = operation(MetadataTransportMessage::WriteNotFound);
    assert!(is_rejected(operation.next(config_read())));
}

#[test]
fn internal_token_denied() {
    let message = MetadataTransportMessage::ForwardTokenRevocation {
        auth_token: AuthToken::Internal(caller()),
        token: Secret::new("target"),
    };
    assert_eq!(
        operation(message).next(config_read()),
        denied_with(WriteAuthError::Unauthorized)
    );
}

#[test]
fn invalid_target_rejected() {
    let event = NetEvent::RevocationClaims(Err("bad signature".to_string()));
    let answer = IoResult::Event(Box::new(Event::Net(event)));
    assert!(is_rejected(verifying(caller(), "target").next(answer)));
}

#[test]
fn distant_expiry_rejected() {
    let far = claims(user(4), realm(), u64::MAX);
    assert!(is_rejected(
        verifying(caller(), "target").next(verified(far))
    ));
}

#[test]
fn unreadable_subject_rejected() {
    let mut broken = own_claims();
    broken.sub = "not-a-user".to_string();
    assert!(is_rejected(
        verifying(caller(), "target").next(verified(broken))
    ));
}

#[test]
fn foreign_subject_forbidden() {
    let other = RealmId::from_bytes([9; 32]);
    let foreign = claims(UserId::nil(other), other, 2_000);
    assert_eq!(
        verifying(caller(), "target").next(verified(foreign)),
        denied_with(WriteAuthError::Forbidden)
    );
}

#[test]
fn own_token_revoked() {
    assert_eq!(revoking().stage, ApplyStage::Revoke);
}

#[test]
fn restricted_self_revokes() {
    let mut operation = verifying(restricted(), CALLER_TOKEN);
    assert!(runs_child(operation.next(verified(own_claims()))));
    assert_eq!(operation.stage, ApplyStage::Revoke);
}

#[test]
fn restricted_sibling_checked() {
    let mut operation = verifying(restricted(), "target");
    assert!(runs_child(operation.next(verified(own_claims()))));
    assert_eq!(operation.stage, ApplyStage::Write);
}

fn admin_checked() -> ApplyRevokeOperation {
    let mut operation = verifying(caller(), "target");
    assert!(runs_child(operation.next(verified(other_claims()))));
    assert_eq!(operation.stage, ApplyStage::Write);
    operation
}

#[test]
fn admin_may_revoke() {
    let mut operation = admin_checked();
    let allowed = child(Ok::<(), AuthorizeError>(()));
    assert!(runs_child(operation.next(allowed)));
    assert_eq!(operation.stage, ApplyStage::Revoke);
}

#[test]
fn stranger_is_forbidden() {
    let denied = child(Err::<(), _>(AuthorizeError::PermissionDenied));
    assert_eq!(
        admin_checked().next(denied),
        denied_with(WriteAuthError::Forbidden)
    );
}

#[test]
fn revocation_confirmed() {
    let stored = child(Ok::<_, RevokeTokenError>(config()));
    assert_eq!(
        revoking().next(stored),
        Ok(Next::Done(MetadataTransportMessage::ForwardedTokenRevoked))
    );
}

#[test]
fn full_list_reported() {
    let full = child(Err::<RealmConfigDocument, _>(
        RevokeTokenError::CapacityReached,
    ));
    assert_eq!(
        revoking().next(full),
        Ok(Next::Done(
            MetadataTransportMessage::TokenRevocationCapacity
        ))
    );
}

#[test]
fn failed_revocation_rejected() {
    let failed = child(Err::<RealmConfigDocument, _>(
        RevokeTokenError::ConfigMissing,
    ));
    assert!(is_rejected(revoking().next(failed)));
}
