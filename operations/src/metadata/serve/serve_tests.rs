//! Call tests of the serve operation: each request runs its child and answers its reply.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::bucket::get::GetBucketError;
use crate::tests::calls::{caller_of, child, node, runs_child};
use aruna_core::structs::identity::auth::AuthContext;

fn local() -> ServeNode {
    ServeNode {
        local: Some((node(1), RealmId([1u8; 32]))),
        graph: true,
        indexed: false,
    }
}

fn no_realm() -> ServeNode {
    ServeNode {
        local: None,
        ..local()
    }
}

fn reply(turn: ServeTurn) -> MetadataTransportMessage {
    match turn {
        Ok(Next::Done((reply, None))) => reply,
        other => panic!("expected a reply, got {other:?}"),
    }
}

#[test]
fn realm_ask_unavailable() {
    // A node without a realm answers a realm request without asking anything.
    let request = MetadataTransportMessage::PromotionCheck;
    let mut serve = ServeRequestOperation::new(node(2), no_realm(), request);
    assert_eq!(
        reply(serve.first()),
        MetadataTransportMessage::PromotionReady {
            result: Err(MetadataReadError::Unavailable)
        }
    );
}

#[test]
fn promotion_check_answered() {
    let mut serve =
        ServeRequestOperation::new(node(2), local(), MetadataTransportMessage::PromotionCheck);
    assert!(runs_child(serve.first()));
    let answer: Result<PromotionAnswer, MetadataReadError> = Ok(Ok(true));
    assert_eq!(
        reply(serve.next(child(answer))),
        MetadataTransportMessage::PromotionReady { result: Ok(true) }
    );
}

#[test]
fn staging_spec_refused() {
    let request = MetadataTransportMessage::StagingSpec {
        transfer_id: ulid::Ulid::from_bytes([3u8; 16]),
        job_id: aruna_core::structs::execution::job::JobId::from_bytes([4u8; 16]),
    };
    let mut serve = ServeRequestOperation::new(node(2), local(), request);
    assert!(runs_child(serve.first()));
    let answer: Result<HandedImport, MetadataReadError> = Err(MetadataReadError::NotFound);
    assert_eq!(
        reply(serve.next(child(answer))),
        MetadataTransportMessage::StagingSpecResult {
            result: Err(MetadataReadError::NotFound)
        }
    );
}

#[test]
fn other_request_unserved() {
    let request = MetadataTransportMessage::SyncMirrorCreated;
    let mut serve = ServeRequestOperation::new(node(2), local(), request.clone());
    assert_eq!(serve.first(), Err(ServeError::Unserved(Box::new(request))));
}

fn owner_read(realm: bool) -> CallerReadOperation {
    let request = MetadataTransportMessage::BucketOwner {
        auth_token: None,
        bucket: "b".to_string(),
    };
    let serving = if realm { local() } else { no_realm() };
    CallerReadOperation::new(node(2), serving, request, ReadReply::Owner)
}

#[test]
fn graph_needs_store() {
    // Without the graph store a graph read answers before the caller is checked.
    let request = MetadataTransportMessage::QueryGraphs {
        auth_token: None,
        graph_iris: None,
        sparql: "ASK {}".to_string(),
    };
    let serving = ServeNode {
        graph: false,
        ..local()
    };
    let mut read = CallerReadOperation::new(node(2), serving, request, ReadReply::Query);
    assert_eq!(
        read.first(),
        Ok(Next::Done(MetadataTransportMessage::QueryResults {
            result: Err(MetadataReadError::Unavailable)
        }))
    );
}

#[test]
fn read_needs_realm() {
    let mut read = owner_read(false);
    assert_eq!(
        read.first(),
        Ok(Next::Done(MetadataTransportMessage::BucketOwnerResult {
            result: Err(MetadataReadError::Unavailable)
        }))
    );
}

#[test]
fn refused_caller_answered() {
    let mut read = owner_read(true);
    assert!(runs_child(read.first()));
    let refused: Result<Option<AuthContext>, MetadataReadError> = Err(MetadataReadError::Forbidden);
    assert_eq!(
        read.next(child(refused)),
        Ok(Next::Done(MetadataTransportMessage::BucketOwnerResult {
            result: Err(MetadataReadError::Forbidden)
        }))
    );
}

#[test]
fn owner_needs_caller() {
    let mut read = owner_read(true);
    read.first().unwrap();
    let anonymous: Result<Option<AuthContext>, MetadataReadError> = Ok(None);
    assert_eq!(
        read.next(child(anonymous)),
        Ok(Next::Done(MetadataTransportMessage::BucketOwnerResult {
            result: Err(MetadataReadError::Unauthorized)
        }))
    );
}

#[test]
fn owner_missing_bucket() {
    let mut read = owner_read(true);
    read.first().unwrap();
    let caller: Result<Option<AuthContext>, MetadataReadError> =
        Ok(Some(caller_of(RealmId([1u8; 32]))));
    assert!(runs_child(read.next(child(caller))));
    let missing: Result<aruna_core::structs::storage::blob::BucketInfo, GetBucketError> =
        Err(GetBucketError::NotFound);
    assert_eq!(
        read.next(child(missing)),
        Ok(Next::Done(MetadataTransportMessage::BucketOwnerResult {
            result: Err(MetadataReadError::NotFound)
        }))
    );
}
