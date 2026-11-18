//! Tests vault routing and the checks of forwarded vault requests as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::users::vault_write::VaultChange;
use aruna_core::events::NetEvent;
use aruna_core::structs::identity::realm::RealmNodeKind;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn user() -> UserId {
    UserId::local(Ulid::from_bytes([2; 16]), realm())
}

fn bearer() -> Option<AuthToken> {
    Some(AuthToken::bearer("caller-token").unwrap())
}

fn auth() -> AuthContext {
    AuthContext {
        user_id: user(),
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    }
}

fn append_config() -> AppendVaultConfig {
    AppendVaultConfig {
        node_id: node(1),
        realm_id: realm(),
        user_id: user(),
        record_id: Ulid::from_bytes([3; 16]),
        change: VaultChange::Delete,
        now_ms: 5,
    }
}

fn answer<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn checked(result: Result<AuthContext, WritePeerRefusal>) -> IoResult {
    let event = NetEvent::WritePeerChecked(result.map(Box::new));
    IoResult::Event(Box::new(Event::Net(event)))
}

fn config_answer(config: RealmConfigDocument) -> IoResult {
    answer(Ok::<_, GetConfigError>(config))
}

fn server_config() -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 2);
    config.ensure_node(node(2), RealmNodeKind::Server);
    config
}

fn forwarding() -> AppendRoutedOperation {
    let mut operation = AppendRoutedOperation::new(append_config(), bearer());
    operation.first().unwrap();
    let missed = AppendVaultError::NotHolder {
        holders: vec![node(4)],
    };
    let turn = operation.next(answer(Err::<VaultAppended, _>(missed)));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    operation
}

fn holder_says(message: MetadataTransportMessage) -> IoResult {
    answer(Ok::<_, HolderWriteError>(message))
}

#[test]
fn local_append_finishes() {
    let mut operation = AppendRoutedOperation::new(append_config(), None);
    operation.first().unwrap();
    let appended = VaultAppended::Heads(Vec::new());
    assert_eq!(
        operation.next(answer(Ok::<_, AppendVaultError>(appended.clone()))),
        Ok(Next::Done(appended))
    );
}

#[test]
fn holder_answer_returned() {
    let appended = VaultAppended::Heads(Vec::new());
    let message = MetadataTransportMessage::ForwardedVaultChange {
        result: Ok(appended.clone()),
    };
    assert_eq!(
        forwarding().next(holder_says(message)),
        Ok(Next::Done(appended))
    );
}

#[test]
fn holder_refusal_kept() {
    let message = MetadataTransportMessage::ForwardedVaultChange {
        result: Err(VaultRefusal::TooManyKeys),
    };
    assert_eq!(
        forwarding().next(holder_says(message)),
        Err(VaultRouteError::Refused(VaultRefusal::TooManyKeys))
    );
}

#[test]
fn odd_answer_undeliverable() {
    let turn = forwarding().next(holder_says(MetadataTransportMessage::WriteNotFound));
    assert!(matches!(
        turn,
        Err(VaultRouteError::Forward(HolderWriteError::Undeliverable(_)))
    ));
}

fn caller() -> VaultCallerOperation {
    let mut operation = VaultCallerOperation::new(node(2), realm(), bearer());
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    operation
}

#[test]
fn caller_needs_bearer() {
    let mut operation = VaultCallerOperation::new(node(2), realm(), None);
    assert_eq!(operation.first(), Err(MetadataReadError::Unauthorized));
}

#[test]
fn caller_names_user() {
    let mut operation = caller();
    operation.next(checked(Ok(auth()))).unwrap();
    assert_eq!(
        operation.next(config_answer(server_config())),
        Ok(Next::Done(user()))
    );
}

#[test]
fn restricted_caller_forbidden() {
    let mut operation = caller();
    let restricted = AuthContext {
        path_restrictions: Some(Vec::new()),
        ..auth()
    };
    operation.next(checked(Ok(restricted))).unwrap();
    assert_eq!(
        operation.next(config_answer(server_config())),
        Err(MetadataReadError::Forbidden)
    );
}

#[test]
fn refused_peer_stops() {
    assert_eq!(
        caller().next(checked(Err(WritePeerRefusal::Unauthorized))),
        Err(MetadataReadError::Unauthorized)
    );
}

fn change_message() -> MetadataTransportMessage {
    MetadataTransportMessage::ForwardVaultChange {
        auth_token: bearer(),
        record_id: Ulid::from_bytes([3; 16]),
        change: Box::new(VaultChange::Delete),
        created_at_ms: 5,
    }
}

fn applying() -> ApplyVaultOperation {
    let local = Some((node(1), realm()));
    let mut operation = ApplyVaultOperation::new(local, node(2), change_message());
    operation.first().unwrap();
    operation
}

#[test]
fn apply_reports_denial() {
    let denied = answer(Err::<UserId, _>(MetadataReadError::Forbidden));
    assert_eq!(
        applying().next(denied),
        Ok(Next::Done(MetadataTransportMessage::ForwardedWriteDenied {
            error: WriteAuthError::Forbidden,
        }))
    );
}

#[test]
fn apply_maps_refusal() {
    let mut operation = applying();
    let turn = operation.next(answer(Ok::<_, MetadataReadError>(user())));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let full = answer(Err::<VaultAppended, _>(AppendVaultError::TooManyKeys));
    assert_eq!(
        operation.next(full),
        Ok(Next::Done(MetadataTransportMessage::ForwardedVaultChange {
            result: Err(VaultRefusal::TooManyKeys),
        }))
    );
}

#[test]
fn apply_rejects_other() {
    let mut operation =
        ApplyVaultOperation::new(None, node(2), MetadataTransportMessage::WriteNotFound);
    assert!(matches!(
        operation.first(),
        Ok(Next::Done(MetadataTransportMessage::Reject(_)))
    ));
}

fn serving(query: VaultQuery) -> ServeVaultOperation {
    let local = Some((node(1), realm()));
    let mut operation = ServeVaultOperation::new(local, node(2), user(), query);
    operation.first().unwrap();
    operation
}

#[test]
fn keys_need_infrastructure() {
    let config = RealmConfigDocument::new(realm(), Vec::new(), 2);
    assert_eq!(
        serving(VaultQuery::Keys).next(config_answer(config)),
        Err(MetadataReadError::Forbidden)
    );
}

#[test]
fn foreign_heads_forbidden() {
    let query = VaultQuery::Heads {
        auth_token: bearer(),
    };
    let mut operation = serving(query);
    let turn = operation.next(config_answer(server_config()));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let other = UserId::local(Ulid::from_bytes([9; 16]), realm());
    assert_eq!(
        operation.next(answer(Ok::<_, MetadataReadError>(other))),
        Err(MetadataReadError::Forbidden)
    );
}

#[test]
fn serve_needs_network() {
    let mut operation = ServeVaultOperation::new(None, node(2), user(), VaultQuery::Keys);
    assert_eq!(operation.first(), Err(MetadataReadError::Unavailable));
}
