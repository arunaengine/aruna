//! Tests the release pin refusal of permanent deletions: which answers refuse, which purge
//! scopes a release reaches, and that a refusal names no release.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::storage::bucket_metadata::ReleaseRef;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::origin::OriginVersion;
use aruna_core::structs::storage::release::ReleaseMembers;
use std::time::SystemTime;

use super::*;

fn query(key: &str) -> MembershipQuery {
    MembershipQuery {
        key: key.to_string(),
        version_id: Ulid::from_bytes([1; 16]),
    }
}

fn release(label: &str) -> ReleaseRef {
    ReleaseRef {
        crate_version: OriginVersion::Global(GlobalId::Version {
            realm_id: RealmId::from_bytes([1; 32]),
            version_id: Ulid::from_bytes([2; 16]),
        }),
        label: label.to_string(),
        upstream: Vec::new(),
    }
}

fn output(answers: Vec<Vec<ReleaseRef>>, complete: bool) -> MembersOutput {
    MembersOutput {
        answers: (answers.into_iter())
            .map(|releases| ReleaseMembers { releases, complete })
            .collect(),
        releases: Vec::new(),
        complete,
        refused: Vec::new(),
    }
}

#[test]
fn pins_follow_queries() {
    let queries = [query("db/a"), query("db/b")];
    let answers = vec![vec![release("6.0"), release("6.1")], Vec::new()];
    let pins = pinned(&queries, &output(answers, true)).unwrap();
    assert_eq!(pins, [true, false]);
}

#[test]
fn unchecked_answers_refuse() {
    // A release that could not be read may pin the version, so nothing is deleted.
    let queries = [query("db/a")];
    let incomplete = pinned(&queries, &output(vec![Vec::new()], false));
    assert!(matches!(incomplete, Err(PinnedError::Unchecked(_))));
    let short = pinned(&queries, &output(Vec::new(), true));
    assert!(matches!(short, Err(PinnedError::Unchecked(_))));
}

#[test]
fn scopes_reach_releases() {
    let file = StoragePurgeScope::File {
        bucket: "b".to_string(),
        key: "db/a.faa".to_string(),
    };
    let folder = StoragePurgeScope::Prefix {
        bucket: "b".to_string(),
        prefix: "db/".to_string(),
    };
    let bucket = StoragePurgeScope::Bucket {
        bucket: "b".to_string(),
    };
    assert_eq!(reach(&file, "db/"), Some("db/a.faa"));
    assert_eq!(reach(&file, ""), Some("db/a.faa"));
    assert_eq!(reach(&file, "other/"), None);
    // A folder inside a release's prefix, or one holding it, reaches it.
    assert_eq!(reach(&folder, "db/v6/"), Some("db/"));
    assert_eq!(reach(&folder, ""), Some("db/"));
    assert_eq!(reach(&folder, "dbx/"), None);
    // The whole bucket holds every release.
    assert_eq!(reach(&bucket, ""), Some(""));
    assert_eq!(reach(&bucket, "db/v6/"), Some(""));
}

#[test]
fn refusal_names_none() {
    // A release the caller cannot read also refuses, so no release label is shown.
    let error = PinnedError::Version {
        key: "db/a".to_string(),
        version_id: Ulid::from_bytes([1; 16]),
    };
    let message = error.to_string();
    assert!(
        message.contains("db/a") && message.contains("a release"),
        "{message}"
    );
    assert!(PinnedError::Scope.to_string().contains("a release"));
}

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn info() -> BucketInfo {
    BucketInfo {
        group_id: Ulid::from_bytes([3; 16]),
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

fn auth() -> AuthContext {
    AuthContext {
        user_id: UserId::nil(realm()),
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    }
}

fn scope(info: &BucketInfo) -> BucketScope<'_> {
    BucketScope {
        realm_id: realm(),
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        bucket: "b",
        info,
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn is_child<T: std::fmt::Debug>(turn: &Result<Next<T>, PinnedError>) -> bool {
    matches!(turn, Ok(Next::Call(IoCall::Child(_))))
}

#[test]
fn no_queries_no_reads() {
    let info = info();
    let mut operation = VersionPinsOperation::new(scope(&info), &auth(), Vec::new());
    assert_eq!(operation.first(), Ok(Next::Done((Vec::new(), Vec::new()))));
}

#[test]
fn pins_keep_marker() {
    // The marker read before the releases becomes the guard of the deletes.
    let info = info();
    let queries = vec![query("db/a")];
    let mut operation = VersionPinsOperation::new(scope(&info), &auth(), queries);
    assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
    let marker = Value::from(vec![7]);
    let turn = operation.next(IoResult::Values(vec![Some(marker.clone())]));
    assert!(is_child(&turn));
    let answers = vec![vec![release("6.0")]];
    let turn = operation.next(child(Ok::<_, PinnedError>(output(answers, true))));
    let Ok(Next::Done((pins, guards))) = turn else {
        panic!("the check ends: {turn:?}");
    };
    assert_eq!(pins, [true]);
    assert_eq!(guards[0].0, BUCKET_RELEASE_KEYSPACE);
    assert_eq!(guards[0].2, Some(marker));
}

#[test]
fn pinned_version_refused() {
    let info = info();
    let version_id = Ulid::from_bytes([1; 16]);
    let mut operation = RefusePinnedOperation::new(scope(&info), &auth(), ("db/a", version_id));
    assert!(is_child(&operation.first()));
    let pins = Ok::<PinsOutput, PinnedError>((vec![true], Vec::new()));
    assert_eq!(
        operation.next(child(pins)),
        Err(PinnedError::Version {
            key: "db/a".to_string(),
            version_id,
        })
    );
}

#[test]
fn free_version_guarded() {
    let info = info();
    let version_id = Ulid::from_bytes([1; 16]);
    let mut operation = RefusePinnedOperation::new(scope(&info), &auth(), ("db/a", version_id));
    operation.first().unwrap();
    let guards = vec![(
        BUCKET_RELEASE_KEYSPACE.to_string(),
        Key::from(vec![1]),
        None,
    )];
    let pins = Ok::<PinsOutput, PinnedError>((vec![false], guards.clone()));
    assert_eq!(operation.next(child(pins)), Ok(Next::Done(guards)));
}

fn valid(prefix: &str) -> ValidRelease {
    ValidRelease {
        release: release("6.0"),
        key: "release.json".to_string(),
        version_id: Ulid::from_bytes([5; 16]),
        prefix: prefix.to_string(),
    }
}

fn listed(releases: Vec<ValidRelease>, complete: bool) -> IoResult {
    let mut members = output(Vec::new(), complete);
    members.releases = releases;
    child(Ok::<_, PinnedError>(members))
}

fn purge() -> RefuseScopeOperation {
    let info = info();
    let file = StoragePurgeScope::File {
        bucket: "b".to_string(),
        key: "db/a".to_string(),
    };
    let mut operation = RefuseScopeOperation::new(scope(&info), &auth(), &file);
    assert!(is_child(&operation.first()));
    operation
}

fn view(keys: &[&str]) -> IoResult {
    let files = (keys.iter())
        .map(|key| crate::s3::bucket::release::view::PinnedFile {
            key: key.to_string(),
            version_id: Ulid::from_bytes([6; 16]),
            size: 1,
            id: String::new(),
            content_id: None,
        })
        .collect();
    child(Ok::<_, ViewError>(ReleaseView {
        label: "6.0".to_string(),
        prefix: "db/".to_string(),
        files,
        folders: Vec::new(),
        next: None,
    }))
}

#[test]
fn incomplete_list_refuses() {
    let turn = purge().next(listed(Vec::new(), false));
    assert!(matches!(turn, Err(PinnedError::Unchecked(_))));
}

#[test]
fn distant_release_passes() {
    let turn = purge().next(listed(vec![valid("other/")], true));
    assert_eq!(turn, Ok(Next::Done(())));
}

#[test]
fn pinned_scope_refused() {
    let mut operation = purge();
    assert!(is_child(&operation.next(listed(vec![valid("db/")], true))));
    assert_eq!(operation.next(view(&["db/a"])), Err(PinnedError::Scope));
}

#[test]
fn empty_view_continues() {
    // A release that lists no file of the scope, or is gone, pins nothing here.
    let mut operation = purge();
    let releases = vec![valid("db/"), valid("")];
    assert!(is_child(&operation.next(listed(releases, true))));
    assert!(is_child(&operation.next(view(&[]))));
    let gone = Err::<ReleaseView, _>(ViewError::Missing);
    assert_eq!(operation.next(child(gone)), Ok(Next::Done(())));
}

#[test]
fn unreadable_view_refuses() {
    let mut operation = purge();
    operation.next(listed(vec![valid("db/")], true)).unwrap();
    let failed = Err::<ReleaseView, _>(ViewError::Malformed);
    let turn = operation.next(child(failed));
    assert!(matches!(turn, Err(PinnedError::Unchecked(_))));
}
