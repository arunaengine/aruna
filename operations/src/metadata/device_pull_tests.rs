//! Drives the pull and listing operations through their caller, receipt and write answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use super::*;
use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::replication::VersionedObjectArn;

fn realm() -> RealmId {
    RealmId::from_bytes([4; 32])
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn auth() -> AuthContext {
    AuthContext {
        user_id: UserId::nil(realm()),
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    }
}

fn request(deleted: bool) -> SyncPull {
    SyncPull {
        source: VersionedObjectArn::new(
            realm(),
            node(9),
            "folder-x".to_string(),
            "note.txt".to_string(),
            Ulid::from_bytes([2; 16]),
        )
        .unwrap(),
        blake3: None,
        size: 4,
        target_bucket: "lab".to_string(),
        target_key: "note.txt".to_string(),
        deleted,
    }
}

fn bucket() -> BucketInfo {
    BucketInfo {
        group_id: Ulid::from_parts(3, 3),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: UserId::nil(realm()),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn token() -> AuthToken {
    AuthToken::internal(auth())
}

#[test]
fn foreign_source_refused() {
    let mut caller = PullCallerOperation::new((node(8), realm()), token(), request(false));
    assert!(matches!(caller.first(), Ok(Next::Call(IoCall::Child(_)))));
    let answered = caller.next(child(Ok::<_, ForwardAuthError>(auth())));
    assert!(matches!(answered, Err(SyncRefusal::Invalid(_))));
}

#[test]
fn refused_caller_unauthorized() {
    let mut caller = PullCallerOperation::new((node(9), realm()), token(), request(false));
    caller.first().unwrap();
    let denied = caller.next(child(Err::<AuthContext, _>(ForwardAuthError::Forbidden)));
    assert_eq!(denied, Err(SyncRefusal::Unauthorized));
}

#[test]
fn receipt_names_version() {
    let mut receipt = ReceiptOperation::new(&request(false));
    assert!(matches!(receipt.first(), Ok(Next::Call(IoCall::Read(_)))));
    let version = Ulid::from_parts(7, 7);
    let value = IoResult::Values(vec![Some(Key::from(version.to_bytes().to_vec()))]);
    assert_eq!(receipt.next(value), Ok(Next::Done(Some(version))));
}

#[test]
fn replay_admits_nothing() {
    let mut admit = AdmitPullOperation::new(node(1), auth(), request(false));
    admit.first().unwrap();
    let version = Ulid::from_parts(7, 7);
    let replayed = admit.next(child(Ok::<_, SyncRefusal>(Some(version))));
    assert_eq!(
        replayed,
        Ok(Next::Done(Admission::Replayed(replay_ack(version))))
    );
}

#[test]
fn missing_bucket_not_found() {
    let mut admit = AdmitPullOperation::new(node(1), auth(), request(false));
    admit.first().unwrap();
    admit
        .next(child(Ok::<Option<Ulid>, SyncRefusal>(None)))
        .unwrap();
    let missing = admit.next(child(Err::<BucketInfo, _>(GetBucketError::NotFound)));
    assert_eq!(missing, Err(SyncRefusal::NotFound));
}

#[test]
fn ordinary_bucket_checks_write() {
    let mut admit = AdmitPullOperation::new(node(1), auth(), request(false));
    admit.first().unwrap();
    admit
        .next(child(Ok::<Option<Ulid>, SyncRefusal>(None)))
        .unwrap();
    admit
        .next(child(Ok::<_, GetBucketError>(bucket())))
        .unwrap();
    assert!(matches!(
        admit.next(child(Ok::<(), BucketGuardError>(()))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let denied = admit.next(child(Err::<(), _>(AuthorizeError::PermissionDenied)));
    assert_eq!(denied, Err(SyncRefusal::Forbidden));
}

fn commit(deleted: bool) -> CommitPullOperation {
    let node = (node(1), NodeRouting::default());
    let bytes_done = Arc::new(AtomicU64::new(0));
    CommitPullOperation::new(node, (auth(), bucket()), request(deleted), bytes_done)
}

#[test]
fn delete_queues_marker() {
    let mut pull = commit(true);
    pull.first().unwrap();
    assert!(matches!(
        pull.next(child(Ok::<Option<Ulid>, SyncRefusal>(None))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let marker = DeleteObjectResult {
        version_id: Ulid::from_parts(8, 8),
        delete_marker: true,
    };
    let Ok(Next::Then { output, calls }) = pull.next(child(Ok::<_, DeleteObjectError>(marker)))
    else {
        panic!("expected the marker and its live queue entry");
    };
    assert_eq!(output.version_id, Ulid::from_parts(8, 8));
    assert_eq!(calls.len(), 1);
}

#[test]
fn failed_write_replays() {
    let mut pull = commit(false);
    pull.first().unwrap();
    pull.next(child(Ok::<Option<Ulid>, SyncRefusal>(None)))
        .unwrap();
    let failed = pull.next(child(Err::<BaoReadOutput, _>(BaoReadError::Refused(
        BaoReadRefusal::HashMismatch,
    ))));
    assert!(matches!(failed, Ok(Next::Call(IoCall::Child(_)))));
    let committed = Ulid::from_parts(9, 9);
    let answered = pull.next(child(Ok::<_, SyncRefusal>(Some(committed))));
    assert_eq!(answered, Ok(Next::Done(replay_ack(committed))));
}

#[test]
fn failed_write_refuses() {
    let mut pull = commit(false);
    pull.first().unwrap();
    pull.next(child(Ok::<Option<Ulid>, SyncRefusal>(None)))
        .unwrap();
    pull.next(child(Err::<BaoReadOutput, _>(BaoReadError::Refused(
        BaoReadRefusal::HashMismatch,
    ))))
    .unwrap();
    let refused = pull.next(child(Ok::<Option<Ulid>, SyncRefusal>(None)));
    assert!(matches!(refused, Err(SyncRefusal::Invalid(_))));
}

#[test]
fn listing_needs_caller() {
    let listing = SetListing {
        bucket: "lab".to_string(),
        prefix: String::new(),
        cursor: None,
        limit: SyncPageLimit::default(),
    };
    let mut list = ListHeadsOperation::new(node(9), (node(1), realm()), token(), listing);
    list.first().unwrap();
    let denied = list.next(child(Err::<AuthContext, _>(ForwardAuthError::Unauthorized)));
    assert_eq!(denied, Err(SyncRefusal::Unauthorized));
}
