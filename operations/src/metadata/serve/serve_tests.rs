//! Call tests of the serve operation: each request runs its child and answers its reply.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::document_repository::GetConfigError;
use crate::metadata::api::MetadataApiError;
use crate::s3::bucket::get::GetBucketError;
use crate::tests::calls::{caller_of, child, node, runs_child};
use aruna_core::metadata::protocol::MetadataPathCandidate;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmConfigDocument;

fn local() -> ServeNode {
    ServeNode {
        local: Some((node(1), RealmId([1u8; 32]))),
        graph: true,
        indexed: false,
        metadata_bytes: 1024,
        here: None,
        routing: None,
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
fn other_request_rejected() {
    let request = MetadataTransportMessage::SyncMirrorCreated;
    let mut serve = ServeRequestOperation::new(node(2), local(), request.clone());
    let unexpected = MetadataTransportMessage::Reject("unexpected metadata control message".into());
    assert_eq!(reply(serve.first()), unexpected);
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

fn config(seed: u8) -> Result<RealmConfigDocument, GetConfigError> {
    Ok(RealmConfigDocument::new(RealmId([seed; 32]), Vec::new(), 2))
}

fn digest(seed: u8) -> [u8; 32] {
    config(seed).unwrap().digest().unwrap()
}

#[test]
fn export_needs_digest() {
    // A Profile export under another realm config than the sender's answers unavailable.
    let ask = ExportAsk::Profile {
        profile_id: ulid::Ulid::from_bytes([5u8; 16]),
        expected_revision: ulid::Ulid::from_bytes([6u8; 16]),
    };
    let mut export = ExportServeOperation::new((node(2), local()), digest(1), ask);
    assert!(runs_child(export.first()));
    assert_eq!(
        export.next(child(config(2))),
        Ok(Next::Done((
            MetadataTransportMessage::ForwardedExport {
                result: Err(MetadataReadError::Unavailable)
            },
            None
        )))
    );
}

#[test]
fn query_needs_holder() {
    // A document query for a document this node does not hold is never checked further.
    let ask = PathAsk::Query {
        document_id: ulid::Ulid::from_bytes([5u8; 16]),
        sparql: "ASK {}".to_string(),
    };
    let mut query = PathReadOperation::new((node(2), local()), None, digest(1), ask);
    assert!(runs_child(query.first()));
    assert_eq!(
        query.next(child(config(1))),
        Ok(Next::Done(MetadataTransportMessage::DocumentQueryResults {
            result: Err(MetadataReadError::Unavailable)
        }))
    );
}

#[test]
fn lookup_rechecks_config() {
    // A path lookup only stands while the config still matches after the read.
    let ask = PathAsk::Lookup {
        group_id: ulid::Ulid::from_bytes([5u8; 16]),
        path: "a/b".to_string(),
    };
    let mut lookup = PathReadOperation::new((node(2), local()), None, digest(1), ask);
    assert!(runs_child(lookup.first()));
    let caller: Result<Option<AuthContext>, MetadataReadError> = Ok(None);
    assert!(runs_child(lookup.next(child(caller))));
    assert!(runs_child(lookup.next(child(config(1)))));
    let found: Result<Vec<MetadataPathCandidate>, MetadataApiError> = Ok(Vec::new());
    assert!(runs_child(lookup.next(child(found))));
    assert_eq!(
        lookup.next(child(config(2))),
        Ok(Next::Done(MetadataTransportMessage::ForwardedPathLookup {
            result: Err(MetadataReadError::Unavailable)
        }))
    );
}

fn relationship(source_bucket: &str) -> aruna_core::structs::SyncRelationship {
    use aruna_core::structs::storage::replication::ArunaArn;
    use aruna_core::structs::{SyncMode, SyncState, SyncStatusSnapshot};
    let realm_id = RealmId([1u8; 32]);
    aruna_core::structs::SyncRelationship {
        id: ulid::Ulid::from_bytes([7u8; 16]),
        source: ArunaArn::s3_bucket(realm_id, node(1), source_bucket).unwrap(),
        target: ArunaArn::s3_bucket(realm_id, node(2), "target").unwrap(),
        mode: SyncMode::Continuous,
        reference_handling: Default::default(),
        reference_serving: false,
        replicate_deletes: true,
        created_by: aruna_core::UserId::local(ulid::Ulid::from_bytes([8u8; 16]), realm_id),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        state: SyncState::Enabled,
        status: SyncStatusSnapshot::default(),
    }
}

#[test]
fn workspace_delete_allowed() {
    // Only a delete may name a workspace bucket, and only its creator may ask.
    use super::mirror::valid_sync_request;
    let stored = relationship("ws-temporary");
    let buckets = ("ws-temporary", "target");
    let realm_id = RealmId([1u8; 32]);
    assert!(valid_sync_request(
        &stored,
        buckets,
        realm_id,
        stored.created_by,
        true
    ));
    assert!(!valid_sync_request(
        &stored,
        buckets,
        realm_id,
        stored.created_by,
        false
    ));
    let other = aruna_core::UserId::local(ulid::Ulid::from_bytes([9u8; 16]), realm_id);
    assert!(!valid_sync_request(&stored, buckets, realm_id, other, true));
}

#[test]
fn rollback_check_scope() {
    use super::mirror::RollbackRequest;
    let stored = relationship("source");
    let request = RollbackRequest {
        peer: node(1),
        local_node: node(2),
        vouched: true,
    };
    assert!(request.allows(&stored, &stored));
    let other_peer = RollbackRequest {
        peer: node(3),
        ..request
    };
    assert!(!other_peer.allows(&stored, &stored));
    let outgoing = RollbackRequest {
        peer: node(2),
        local_node: node(1),
        ..request
    };
    assert!(!outgoing.allows(&stored, &stored));
    let bearer = RollbackRequest {
        vouched: false,
        ..request
    };
    assert!(!bearer.allows(&stored, &stored));
    let mut changed = stored.clone();
    changed.created_at += std::time::Duration::from_millis(1);
    assert!(!request.allows(&stored, &changed));
}

#[test]
fn sync_bucket_owned() {
    // A mirror's new bucket belongs to the source group and the mirror's creator.
    let stored = relationship("source");
    let group = ulid::Ulid::from_bytes([4u8; 16]);
    let bucket = super::mirror::sync_bucket(group, &stored);
    assert_eq!(bucket.group_id, group);
    assert_eq!(bucket.created_by, stored.created_by);
}

#[test]
fn mirror_needs_token() {
    use super::mirror::{MirrorRequest, MirrorServeOperation};
    let request = MirrorRequest {
        token: None,
        relationship: relationship("source"),
        source_group: None,
        extras: None,
        delete: true,
        rollback: false,
    };
    let mut mirror = MirrorServeOperation::new(node(1), local().local, request);
    let denied = MetadataTransportMessage::Reject("access_denied".to_string());
    assert_eq!(mirror.first(), Ok(Next::Done(denied)));
}

#[test]
fn read_only_refused() {
    // A read-only bucket refuses a mirror like a denied write, before any bucket is made.
    use super::mirror::{MirrorRequest, MirrorServeOperation};
    use crate::staging::bucket_guard::BucketGuardError;
    let realm_id = RealmId([1u8; 32]);
    let caller = caller_of(realm_id);
    let mut stored = relationship("source");
    stored.created_by = caller.user_id;
    let request = MirrorRequest {
        token: Some(aruna_core::metadata::AuthToken::Internal(caller.clone())),
        relationship: stored,
        source_group: Some(ulid::Ulid::from_bytes([4u8; 16])),
        extras: None,
        delete: false,
        rollback: false,
    };
    let mut mirror = MirrorServeOperation::new(node(1), Some((node(2), realm_id)), request);
    assert!(runs_child(mirror.first()));
    let accepted: Result<AuthContext, aruna_core::events::PeerAuthRefusal> = Ok(caller);
    assert!(runs_child(mirror.next(child(accepted))));
    let guarded: Result<(), BucketGuardError> = Err(BucketGuardError::ReadOnly("target".into()));
    let denied = MetadataTransportMessage::Reject("access_denied".to_string());
    assert_eq!(mirror.next(child(guarded)), Ok(Next::Done(denied)));
}

#[test]
fn refused_forward_prefixed() {
    use super::forwards::ForwardReply;
    let error = IoError::Unexpected {
        expected: "a reply",
        got: "none",
    };
    let failed: Result<MetadataTransportMessage, IoError> = Err(error.clone());
    let reply = ForwardReply::Refused("group create failed: ").answer(child(failed));
    let expected = format!("group create failed: {error}");
    assert_eq!(reply, Ok(MetadataTransportMessage::Reject(expected)));
}

#[test]
fn late_audit_unavailable() {
    // An audit page the deadline cut off answers unavailable.
    use super::forwards::ForwardReply;
    use aruna_core::events::{GroupEvent, GroupResult};
    let results = vec![GroupResult::Unfinished];
    let late = IoResult::Group(GroupEvent::Finished { results });
    assert_eq!(
        ForwardReply::Audit.answer(late),
        Ok(MetadataTransportMessage::ForwardedAuditPage {
            result: Err(MetadataReadError::Unavailable)
        })
    );
}

#[test]
fn audit_needs_caller() {
    use super::forwards::PeerAuditOperation;
    let request = aruna_core::audit::AuditPageRequest {
        auth_token: None,
        config_digest: [0u8; 32],
        realm_id: RealmId([1u8; 32]),
        group_id: ulid::Ulid::from_bytes([2u8; 16]),
        document_id: None,
        start_after: None,
        limit: 10,
    };
    let mut audit = PeerAuditOperation::new(node(2), (node(1), RealmId([1u8; 32])), request);
    assert!(runs_child(audit.first()));
    let anonymous: Result<Option<AuthContext>, MetadataReadError> = Ok(None);
    assert_eq!(
        audit.next(child(anonymous)),
        Err(MetadataReadError::Unauthorized)
    );
}

#[test]
fn held_needs_holder() {
    // A record read for a document this node does not hold answers unavailable.
    use super::held::{HeldAsk, HeldReadOperation};
    let document_id = ulid::Ulid::from_bytes([5u8; 16]);
    let read = (None, digest(1));
    let mut held = HeldReadOperation::new((node(2), local()), read, document_id, HeldAsk::Record);
    assert!(runs_child(held.first()));
    assert!(runs_child(held.next(child(config(1)))));
    let caller: Result<Option<AuthContext>, MetadataReadError> = Ok(None);
    assert_eq!(
        held.next(child(caller)),
        Ok(Next::Done(MetadataTransportMessage::ForwardedRead {
            result: Err(MetadataReadError::Unavailable)
        }))
    );
}

#[test]
fn write_needs_digest() {
    use super::writes::WriteServeOperation;
    let request = MetadataTransportMessage::ForwardDeleteDocument {
        auth_token: None,
        config_digest: digest(1),
        document_id: ulid::Ulid::from_bytes([5u8; 16]),
    };
    let mut write = WriteServeOperation::new((node(2), local()), digest(1), request);
    assert!(runs_child(write.first()));
    assert_eq!(
        write.next(child(config(2))),
        Ok(Next::Done(
            MetadataTransportMessage::ForwardedWriteUnavailable
        ))
    );
}

#[test]
fn create_needs_path() {
    // A create whose path is only slashes is refused before any write check.
    use super::writes::WriteServeOperation;
    use crate::forward::authorize::ForwardAuthError;
    use aruna_core::metadata::api::CreateDocumentPayload;
    let request = MetadataTransportMessage::ForwardCreateDocument {
        auth_token: None,
        config_digest: digest(1),
        group_id: ulid::Ulid::from_bytes([3u8; 16]),
        document_id: ulid::Ulid::from_bytes([5u8; 16]),
        document_path: "/".to_string(),
        public: false,
        payload: CreateDocumentPayload::RoCrate {
            jsonld: "{}".to_string(),
        },
        commit_message: None,
    };
    let mut write = WriteServeOperation::new((node(2), local()), digest(1), request);
    write.first().unwrap();
    assert!(runs_child(write.next(child(config(1)))));
    let caller: Result<AuthContext, ForwardAuthError> = Ok(caller_of(RealmId([1u8; 32])));
    let refused = "forwarded metadata create has an empty document path".to_string();
    assert_eq!(
        write.next(child(caller)),
        Ok(Next::Done(MetadataTransportMessage::Reject(refused)))
    );
}
