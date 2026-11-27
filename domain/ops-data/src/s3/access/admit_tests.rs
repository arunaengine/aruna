//! Tests credential usability, the live local credential rule, owner admission and the
//! permission path of a request.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::calls::ChildOutcome;
use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::errors::StorageError;
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::share::SHARE_CREDENTIAL_PREFIX;
use std::time::Duration;
use ulid::Ulid;

const ISSUED: u64 = 1_000_000;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn owner() -> UserId {
    UserId::local(Ulid::from_bytes([2; 16]), realm())
}

fn group(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

fn at(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
}

/// A sealed credential of `owner()` in group 9, minted at `ISSUED` and valid for an hour.
fn local_access() -> UserAccess {
    let mut access = UserAccess {
        access_key: Ulid::from_parts(ISSUED * 1000, 7).to_string(),
        user_identity: owner(),
        group_id: group(9),
        secret: EncryptedS3Secret::empty(),
        expiry: at(ISSUED + 3600),
        path_restrictions: None,
        issued_by: *node(1).as_bytes(),
        revoked_at: None,
    };
    let key = CredentialEncryptionKey::derive(&[7; 32]);
    access.encrypt_secret(&key, "secret", [3; 24]).unwrap();
    access
}

#[test]
fn usable_checks_credential() {
    let access = local_access();
    assert_eq!(check_credential_usable(&access, at(ISSUED)), Ok(()));
    let expired = check_credential_usable(&access, access.expiry);
    assert_eq!(expired, Err(AdmissionFailure::Expired));
    // A revoked credential reads as revoked even once it has also expired.
    let revoked = UserAccess {
        revoked_at: Some(at(ISSUED)),
        ..access.clone()
    };
    for now in [at(ISSUED), access.expiry] {
        let refused = check_credential_usable(&revoked, now);
        assert_eq!(refused, Err(AdmissionFailure::Revoked));
    }
}

#[test]
fn local_access_only() {
    let access = local_access();
    assert!(is_local_access(&access));

    // A temporary session, even one with a sealed secret, is no stored credential.
    let session = UserAccess {
        access_key: S3Session::build_access_key(&Ulid::from_parts(ISSUED * 1000, 8).to_string())
            .unwrap(),
        ..access.clone()
    };
    assert!(!is_local_access(&session));
    // A share link's delegated id, a forwarded grant without a secret and the anonymous
    // principal are refused too.
    let share = UserAccess {
        access_key: format!("{SHARE_CREDENTIAL_PREFIX}{}", Ulid::from_bytes([4; 16])),
        ..access.clone()
    };
    let forwarded = UserAccess {
        secret: EncryptedS3Secret::empty(),
        ..access.clone()
    };
    let anonymous = UserAccess {
        access_key: String::new(),
        user_identity: UserId::nil(realm()),
        ..access.clone()
    };
    let everyone = UserAccess {
        user_identity: UserId::nil(realm()),
        ..access
    };
    for refused in [share, forwarded, anonymous, everyone] {
        assert!(!is_local_access(&refused), "{refused:?}");
    }
}

fn admit(revoke: bool) -> AdmitOwnerOperation {
    AdmitOwnerOperation::new(AdmitOwnerConfig {
        realm_id: realm(),
        access_key: local_access().access_key,
        user_id: owner(),
        issued_by: *node(1).as_bytes(),
        revoke,
    })
}

fn realm_config(nodes: &[NodeId]) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
    for node_id in nodes {
        config.ensure_node(*node_id, RealmNodeKind::Server);
    }
    config
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn is_child(next: &AdmitTurn) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Child(_))))
}

/// Answers the issuer and cutoff reads of an admission whose issuer is a realm node.
fn past_cutoff(operation: &mut AdmitOwnerOperation, cutoff: Option<u64>) -> AdmitTurn {
    assert!(is_child(&operation.first()));
    let config: Result<_, GetConfigError> = Ok(realm_config(&[node(3), node(1)]));
    assert!(is_child(&operation.next(child(config))));
    operation.next(child(Ok::<_, ArunaBearerError>(cutoff)))
}

#[test]
fn admits_live_owner() {
    let mut operation = admit(true);
    // A cutoff at the second of issue does not refuse the credential.
    let next = past_cutoff(&mut operation, Some(ISSUED));
    assert!(is_child(&next));
    let active = child(Ok::<_, ArunaBearerError>(false));
    assert_eq!(operation.next(active), Ok(Next::Done(())));
}

#[test]
fn foreign_issuer_refused() {
    let mut operation = admit(true);
    operation.first().unwrap();
    let config: Result<_, GetConfigError> = Ok(realm_config(&[node(3)]));
    let refused = operation.next(child(config));
    assert_eq!(refused, Err(AdmitOwnerError::IssuerUnknown));

    let mut operation = admit(true);
    let missing: Result<RealmConfigDocument, _> = Err(GetConfigError::DocumentNotFound);
    let refused = operation.next(child(missing));
    assert_eq!(refused, Err(AdmitOwnerError::ConfigUnavailable));
}

#[test]
fn cutoff_revokes_credential() {
    let mut operation = admit(true);
    let next = past_cutoff(&mut operation, Some(ISSUED + 1));
    assert!(
        is_child(&next),
        "a refused stored credential is deleted first"
    );
    let deleted: Result<_, RevokeUserError> = Ok(local_access());
    assert_eq!(
        operation.next(child(deleted)),
        Err(AdmitOwnerError::Revoked)
    );

    // A session is refused without a deletion.
    let mut operation = admit(false);
    let refused = past_cutoff(&mut operation, Some(ISSUED + 1));
    assert_eq!(refused, Err(AdmitOwnerError::Revoked));
}

#[test]
fn deactivated_owner_refused() {
    let mut operation = admit(true);
    assert!(is_child(&past_cutoff(&mut operation, None)));
    let deactivated = child(Ok::<_, ArunaBearerError>(true));
    assert!(is_child(&operation.next(deactivated)));
    // A failed deletion keeps the denial.
    let failed: Result<UserAccess, _> =
        Err(RevokeUserError::StorageError(StorageError::CommitFailed));
    assert_eq!(
        operation.next(child(failed)),
        Err(AdmitOwnerError::Deactivated)
    );
}

#[test]
fn unavailable_state_refused() {
    // Unreadable revocation or account state refuses without deleting the credential.
    let mut operation = admit(true);
    operation.first().unwrap();
    let config: Result<_, GetConfigError> = Ok(realm_config(&[node(1)]));
    operation.next(child(config)).unwrap();
    let unreadable: Result<Option<u64>, _> = Err(ArunaBearerError::RevocationUnavailable);
    let refused = operation.next(child(unreadable));
    assert_eq!(refused, Err(AdmitOwnerError::RevocationUnavailable));

    let mut operation = admit(true);
    assert!(is_child(&past_cutoff(&mut operation, None)));
    let unreadable: Result<bool, _> = Err(ArunaBearerError::RevocationUnavailable);
    let refused = operation.next(child(unreadable));
    assert_eq!(refused, Err(AdmitOwnerError::AccountUnavailable));

    // A result that does not answer the current stage fails instead of being ignored.
    let mut operation = admit(true);
    operation.first().unwrap();
    let wrong = operation.next(child(Ok::<_, ArunaBearerError>(false)));
    assert!(matches!(
        wrong,
        Err(AdmitOwnerError::Io(IoError::Unexpected { .. }))
    ));
}

fn bucket_info(group_id: Ulid) -> BucketInfo {
    BucketInfo {
        group_id,
        created_at: at(ISSUED),
        created_by: owner(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    }
}

fn request<'a>(
    access: &'a UserAccess,
    bucket_info: Option<&'a BucketInfo>,
    key: Option<&'a str>,
    operation: &'a str,
) -> PathRequest<'a> {
    PathRequest {
        realm_id: realm(),
        node_id: node(1),
        access,
        bucket: Some("study"),
        bucket_info,
        key,
        operation,
    }
}

#[test]
fn builds_own_paths() {
    let access = local_access();
    let own = bucket_info(group(9));
    let group_path = group_permission_path(realm(), group(9), node(1));

    let listing = PathRequest {
        bucket: None,
        ..request(&access, None, None, "ListBuckets")
    };
    let built = build_authorization_path(listing).unwrap();
    assert_eq!(built.path, group_path);
    assert_eq!(built.context.user_id, owner());
    assert!(!built.swapped);

    let object = request(&access, Some(&own), Some("a/b.txt"), "PutObject");
    let built = build_authorization_path(object).unwrap();
    assert_eq!(built.path, format!("{group_path}/study/a/b.txt"));
    assert_eq!(built.access, access);

    let bucket = build_authorization_path(request(&access, Some(&own), None, "HeadBucket"));
    assert_eq!(bucket.unwrap().path, format!("{group_path}/study"));

    // Only CreateBucket addresses a bucket that does not exist yet.
    let created = build_authorization_path(request(&access, None, None, "CreateBucket"));
    assert_eq!(created.unwrap().path, format!("{group_path}/study"));
    for (key, operation) in [(None, "HeadBucket"), (Some("key"), "CreateBucket")] {
        let missing = build_authorization_path(request(&access, None, key, operation));
        assert_eq!(missing, Err(PathError::NoSuchBucket));
    }
}

#[test]
fn other_group_public() {
    let mut access = local_access();
    access.path_restrictions = Some(Vec::new());
    let other = bucket_info(group(5));
    let read = request(&access, Some(&other), Some("a.txt"), "GetObject");
    let built = build_authorization_path(read).unwrap();
    let other_path = group_permission_path(realm(), group(5), node(1));
    assert_eq!(built.path, format!("{other_path}/study/a.txt"));
    assert_eq!(built.context, AuthContext::anonymous(realm()));
    assert!(built.swapped);
    assert_eq!(built.access.user_identity, UserId::nil(realm()));
    assert_eq!(built.access.group_id, group(5));
    assert_eq!(built.access.path_restrictions, None);

    for (key, operation) in [
        (Some("a.txt"), "PutObject"),
        (Some("a.txt"), "GetObjectTagging"),
        (None, "ListObjectVersions"),
        (None, "GetObject"),
    ] {
        let refused = build_authorization_path(request(&access, Some(&other), key, operation));
        assert_eq!(refused, Err(PathError::OtherGroup), "{operation}");
    }
}

#[test]
fn public_reads_listed() {
    for operation in ["GetObject", "HeadObject"] {
        assert!(public_read_allowed(operation, true));
        assert!(!public_read_allowed(operation, false));
    }
    for operation in [
        "HeadBucket",
        "GetBucketLocation",
        "ListObjects",
        "ListObjectsV2",
    ] {
        assert!(public_read_allowed(operation, false));
        assert!(!public_read_allowed(operation, true));
    }
    for operation in [
        "ListObjectVersions",
        "ListMultipartUploads",
        "GetObjectTagging",
    ] {
        assert!(!public_read_allowed(operation, false));
        assert!(!public_read_allowed(operation, true));
    }
}
