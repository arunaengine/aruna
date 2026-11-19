//! Drives preflight target resolution through bucket, permission, version and key answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use super::*;
use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::storage::format::Compression;
use ulid::Ulid;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[2; 32]).public()
}

fn auth() -> AuthContext {
    AuthContext {
        user_id: UserId::nil(realm()),
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    }
}

fn info() -> BucketInfo {
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

fn bucket_target(operation: MetadataStorageOperation) -> ReferenceTarget {
    ReferenceTarget::BucketPrefix {
        bucket: "genes".to_string(),
        prefix: Some("dir/".to_string()),
        operation,
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

#[test]
fn content_targets_dedup() {
    let hash = [7u8; 32];
    let w3id = format!("{ARUNA_DATA_PREFIX}{}", hex::encode(hash));
    let target = ReferenceTarget::ContentW3ids {
        content_w3ids: vec![w3id.clone(), w3id],
        remove_resolvable_locations: true,
    };
    let mut resolve = ResolveTargetsOperation::new((realm(), node()), auth(), target, None);
    let Ok(Next::Done(resolved)) = resolve.first() else {
        panic!("expected the targets at once");
    };
    assert!(resolved.complete);
    assert_eq!(resolved.targets.len(), 1);
    assert!(resolved.targets[0].remove_resolvable_locations);
}

#[test]
fn empty_bucket_refused() {
    let target = ReferenceTarget::BucketPrefix {
        bucket: " ".to_string(),
        prefix: None,
        operation: MetadataStorageOperation::LatestVersionTombstone,
    };
    let mut resolve = ResolveTargetsOperation::new((realm(), node()), auth(), target, None);
    assert!(matches!(resolve.first(), Err(MetadataApiError::BadRequest)));
}

#[test]
fn bucket_not_found() {
    let target = bucket_target(MetadataStorageOperation::LatestVersionTombstone);
    let mut resolve = ResolveTargetsOperation::new((realm(), node()), auth(), target, None);
    resolve.first().unwrap();
    let missing = resolve.next(child(Err::<BucketInfo, _>(GetBucketError::NotFound)));
    assert!(matches!(missing, Err(MetadataApiError::NotFound)));
}

#[test]
fn denied_scope_refused() {
    let target = bucket_target(MetadataStorageOperation::AllVersionsPurge);
    let mut resolve = ResolveTargetsOperation::new((realm(), node()), auth(), target, None);
    resolve.first().unwrap();
    assert!(matches!(
        resolve.next(child(Ok::<_, GetBucketError>(info()))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let denied = resolve.next(child(Err::<(), _>(MetadataApiError::Forbidden)));
    assert!(matches!(denied, Err(MetadataApiError::Forbidden)));
}

#[test]
fn keys_checked_once() {
    let target = bucket_target(MetadataStorageOperation::LatestVersionTombstone);
    let mut resolve = ResolveTargetsOperation::new((realm(), node()), auth(), target, None);
    resolve.first().unwrap();
    resolve
        .next(child(Ok::<_, GetBucketError>(info())))
        .unwrap();
    assert!(matches!(
        resolve.next(child(Ok::<(), MetadataApiError>(()))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let user = UserId::nil(realm());
    let version = |id: u64| {
        let key = VersionKey::new("genes", "dir/a", Ulid::from_parts(id, 0));
        (key, BlobVersion::deleted(SystemTime::UNIX_EPOCH, user))
    };
    let versions: Versions = vec![version(1), version(2)];
    let Ok(Next::Call(IoCall::Group(group))) =
        resolve.next(child(Ok::<_, MetadataApiError>((versions, true))))
    else {
        panic!("expected the key permission checks");
    };
    assert_eq!(group.members.len(), 1);
}

#[test]
fn heads_read_once() {
    let mut versions = ResolveVersionsOperation::new(
        "genes",
        None,
        MetadataStorageOperation::LatestVersionTombstone,
    );
    assert!(matches!(versions.first(), Ok(Next::Call(IoCall::Child(_)))));
    let heads: HeadVersions = (Vec::new(), true);
    let done = versions.next(child(Ok::<_, HeadVersionsError>(heads)));
    assert!(matches!(done, Ok(Next::Done((found, true))) if found.is_empty()));
}

#[test]
fn purge_scans_lengths() {
    let scans = purge_scans("genes", Some("dir/")).unwrap();
    let short = scans
        .iter()
        .filter(|scan| matches!(scan, Scan::Short(_)))
        .count();
    assert_eq!(short, MAX_KEY_LEN - "dir/".len());
    assert_eq!(scans.len() - short, 0x80);
    assert_eq!(purge_scans("genes", None).unwrap().len(), 1);
}
