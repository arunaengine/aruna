//! Checks the calls and results of the connector reads and of opening a connector.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::repository::push::creator_auth;
use crate::jobs::repository::test_support::{child, credential_key, link, local_scope, scope};
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
    operation.next(child::<(), AccessError>(Ok(()))).unwrap();
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
fn denied_access_is_final() {
    let mut operation = ConnectOperation::new(request(None));
    assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Child(_)))));
    let denied = child::<(), AccessError>(Err(AccessError::Denied("no".into())));
    assert_eq!(
        operation.next(denied),
        Err(TransferError::Permanent("no".into()))
    );
}

#[test]
fn unavailable_access_retries() {
    let mut operation = ConnectOperation::new(request(None));
    operation.first().unwrap();
    let failed = child::<(), AccessError>(Err(AccessError::Unavailable("later".into())));
    assert_eq!(
        operation.next(failed),
        Err(TransferError::Retryable("later".into()))
    );
}

#[test]
fn other_kind_is_refused() {
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
        &credential_key(),
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
    let Ok(Next::Done(session)) = operation.next(invenio()) else {
        panic!("the connector opens with the credential");
    };
    assert_eq!(session.access().token(), Some("secret-token"));
}

#[test]
fn credential_needs_key() {
    let mut connect = request(Some(
        RepositoryCredential::seal_link(
            &credential_key(),
            link().created_by,
            link().group_id,
            link().connector_id,
            None,
            link().endpoint,
            "token",
        )
        .unwrap(),
    ));
    connect.scope = local_scope();
    let mut operation = ConnectOperation::new(connect);
    past_access(&mut operation);
    assert!(matches!(
        operation.next(invenio()),
        Err(TransferError::Retryable(_))
    ));
}

#[test]
fn stored_secret_is_read() {
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
