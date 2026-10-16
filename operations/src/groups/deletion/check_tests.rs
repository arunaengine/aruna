//! Checks deletion blockers without network peers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_DELETE_KEYSPACE};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::group_delete::{GroupDeletionError, MembershipFence};
use aruna_core::structs::storage::blob::bucket_permission_path;
use ulid::Ulid;

use super::check::{check_node, scan};
use super::tests::{Fixture, node, read, write};
use crate::driver::drive;
use crate::s3::bucket::create::CreateBucketOperation;

/// Creates the buckets and denies the caller READ on `hidden` at `seed`.
pub(super) async fn buckets(fixture: &Fixture, seed: u8, names: &[&str]) {
    for name in names {
        drive(
            CreateBucketOperation::new(name.to_string(), fixture.bucket()),
            &fixture.context,
        )
        .await
        .unwrap();
    }
    let group_id = fixture.plan.group_id;
    let key = group_id.to_bytes().to_vec();
    let bytes = read(&fixture.context, AUTH_KEYSPACE, key.clone())
        .await
        .unwrap();
    let mut document = GroupAuthorizationDocument::from_bytes(&bytes).unwrap();
    let path = bucket_permission_path(fixture.auth.realm_id, group_id, node(seed), "hidden");
    for role in document.roles.values_mut() {
        role.permissions.insert(path.clone(), Permission::DENY);
    }
    let bytes = document.to_bytes(&fixture.actor).unwrap();
    write(&fixture.context, AUTH_KEYSPACE, key, bytes).await;
}

#[tokio::test]
async fn check_changes_nothing() {
    let fixture = Fixture::new(&[1]).await;
    let group_id = fixture.plan.group_id;
    assert_eq!(
        scan(&fixture.context, &fixture.auth, group_id, node(1)).await,
        Ok(())
    );
    buckets(&fixture, 1, &["shown", "hidden"]).await;
    assert_eq!(
        scan(&fixture.context, &fixture.auth, group_id, node(1)).await,
        Err(GroupDeletionError::NotEmpty {
            kinds: vec!["S3 buckets".into()],
            buckets: vec!["shown".into()],
            more: false,
        })
    );
    let fence = MembershipFence::key(fixture.plan.realm_id).to_vec();
    for key in [group_id.to_bytes().to_vec(), fence] {
        assert!(
            read(&fixture.context, GROUP_DELETE_KEYSPACE, key)
                .await
                .is_none()
        );
    }
}

#[tokio::test]
async fn check_refuses_strangers() {
    let fixture = Fixture::new(&[1]).await;
    let group_id = fixture.plan.group_id;
    let mut auth = fixture.auth.clone();
    auth.user_id = UserId::local(Ulid::from_bytes([8; 16]), auth.realm_id);
    let context = Arc::new(fixture.context);
    assert_eq!(
        check_node(&context, &auth, group_id, node(1)).await,
        Err(GroupDeletionError::Unauthorized)
    );
    assert_eq!(
        check_node(&context, &fixture.auth, group_id, node(1)).await,
        Ok(())
    );
}
