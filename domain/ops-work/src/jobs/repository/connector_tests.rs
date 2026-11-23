//! Checks the calls and results of the connector reads and of opening a connector.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::repository::push::creator_auth;
use crate::jobs::repository::test_support::{child, credential_key, link, scope};
use aruna_core::compute::Secret;
use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::structs::execution::harvest::RepositoryConnector;
use std::collections::HashMap;
use std::time::SystemTime;

fn view(kind: RepositoryConnectorKind, has_secret_config: bool) -> ConnectorView {
    ConnectorView {
        connector: RepositoryConnector {
            connector_id: link().connector_id,
            group_id: link().group_id,
            name: "repository".into(),
            kind,
            endpoint: link().endpoint,
            public_config: HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: link().created_by,
        },
        has_secret_config,
    }
}

fn request(credential: Option<RepositoryCredential>) -> ConnectRequest {
    ConnectRequest {
        scope: scope(),
        auth: creator_auth(&scope(), &link()),
        group_id: link().group_id,
        connector_id: link().connector_id,
        permission: Permission::READ,
        limit: 1024,
        credential,
        stop: None,
    }
}

fn past_access(operation: &mut ConnectOperation) {
    operation.first().unwrap();
    operation.next(child::<(), AuthorizeError>(Ok(()))).unwrap();
}

fn invenio() -> IoResult {
    child::<_, TransferError>(Ok(view(RepositoryConnectorKind::Invenio, false)))
}

#[test]
fn view_maps_failures() {
    let mut operation = ConnectorViewOperation::new(link().group_id, link().connector_id);
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::child(GetRepositoryOperation::new(
            link().group_id,
            link().connector_id
        ))))
    );
    let missing = child::<ConnectorView, ReadConnectorError>(Err(ReadConnectorError::NotFound));
    assert_eq!(
        operation.next(missing),
        Err(TransferError::Permanent(
            "repository connector unavailable".into()
        ))
    );
    let mut operation = ConnectorViewOperation::new(link().group_id, link().connector_id);
    operation.first().unwrap();
    let broken = child::<ConnectorView, ReadConnectorError>(Err(ReadConnectorError::Unexpected));
    assert!(matches!(
        operation.next(broken),
        Err(TransferError::Retryable(_))
    ));
}

#[test]
fn denied_access_final() {
    let mut operation = ConnectOperation::new(request(None));
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let denied = child::<(), AuthorizeError>(Err(AuthorizeError::PermissionDenied));
    assert_eq!(
        operation.next(denied),
        Err(TransferError::Permanent("permission denied".into()))
    );
}

#[test]
fn unavailable_access_retries() {
    let mut operation = ConnectOperation::new(request(None));
    operation.first().unwrap();
    let failed = child::<(), AuthorizeError>(Err(AuthorizeError::CheckFailed("later".into())));
    assert_eq!(
        operation.next(failed),
        Err(TransferError::Retryable(
            "authorization check failed: later".into()
        ))
    );
}

#[test]
fn other_kind_refused() {
    let mut operation = ConnectOperation::new(request(None));
    past_access(&mut operation);
    let answer = child::<_, TransferError>(Ok(view(RepositoryConnectorKind::OaiPmh, false)));
    assert!(matches!(
        operation.next(answer),
        Err(TransferError::Permanent(_))
    ));
}

#[test]
fn public_connector_opens() {
    let mut operation = ConnectOperation::new(request(None));
    past_access(&mut operation);
    let Ok(Next::Done(session)) = operation.next(invenio()) else {
        panic!("the connector opens without a token");
    };
    assert_eq!(session.access().token(), None);
}

#[test]
fn credential_gives_token() {
    let link = link();
    let credential = RepositoryCredential::seal_link(
        (&credential_key(), [3; 24]),
        link.created_by,
        link.group_id,
        link.connector_id,
        None,
        link.endpoint,
        "secret-token",
    )
    .unwrap();
    let mut operation = ConnectOperation::new(request(Some(credential)));
    past_access(&mut operation);
    let Ok(Next::Done(session)) = open_with(&mut operation, Some(credential_key())) else {
        panic!("the connector opens with the credential");
    };
    assert_eq!(session.access().token(), Some("secret-token"));
}

/// Answers the connector's credential open as the net adapter does on a node holding `key`.
fn open_with(
    operation: &mut ConnectOperation,
    key: Option<CredentialEncryptionKey>,
) -> Result<Next<Session>, TransferError> {
    let Ok(Next::Call(IoCall::Effect(effect))) = operation.next(invenio()) else {
        panic!("the connector asks the net adapter to open the credential");
    };
    let Effect::Net(NetEffect::OpenCredential(open)) = *effect else {
        panic!("expected the credential open effect");
    };
    let opened = key.map(|key| open.open(&key).map_err(|error| error.to_string()));
    let opened = opened.map(|token| token.map(Secret::new));
    let event = Event::Net(NetEvent::CredentialOpened(opened));
    operation.next(IoResult::Event(Box::new(event)))
}

// A node without a credential key retries later; another node's key never opens the token.
#[test]
fn credential_needs_key() {
    let sealed = RepositoryCredential::seal_link(
        (&credential_key(), [3; 24]),
        link().created_by,
        link().group_id,
        link().connector_id,
        None,
        link().endpoint,
        "token",
    )
    .unwrap();
    let mut operation = ConnectOperation::new(request(Some(sealed.clone())));
    past_access(&mut operation);
    assert!(matches!(
        open_with(&mut operation, None),
        Err(TransferError::Retryable(_))
    ));
    let mut operation = ConnectOperation::new(request(Some(sealed)));
    past_access(&mut operation);
    let other = CredentialEncryptionKey::derive(&[2; 32]);
    assert!(matches!(
        open_with(&mut operation, Some(other)),
        Err(TransferError::Permanent(_))
    ));
}

#[test]
fn stored_secret_read() {
    let mut operation = ConnectOperation::new(request(None));
    past_access(&mut operation);
    let answer = child::<_, TransferError>(Ok(view(RepositoryConnectorKind::Invenio, true)));
    assert_eq!(
        operation.next(answer),
        Ok(Next::Call(
            read_secret_effect(link().connector_id, None).into()
        ))
    );
}

#[test]
fn missing_blob_retries() {
    let mut connect = request(None);
    connect.scope.has_blob = false;
    let mut operation = ConnectOperation::new(connect);
    past_access(&mut operation);
    assert_eq!(
        operation.next(invenio()),
        Err(TransferError::Retryable("blob handle unavailable".into()))
    );
}
