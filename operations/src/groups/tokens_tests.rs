//! Tests who may list and revoke group tokens and that revocations stay inside the group.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::list::{GroupTokensConfig, ListTokensOperation};
use super::revoke::{RevokeTarget, RevokeTokensConfig, RevokeTokensOperation};
use super::{GroupTokensError, TokenKind, TokenStatus};
use crate::driver::{DriverContext, drive};
use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use crate::session::index::encode_index;
use crate::users::revoke_all::{RevokeAllConfig, RevokeAllError, RevokeAllOperation};
use aruna_core::UserId;
use aruna_core::auth::bearer_token_hash;
use aruna_core::credential_encryption::EncryptedS3Secret;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, USER_ACCESS_KEYSPACE, USER_KEYSPACE, USER_OWNER_KEYSPACE,
    USER_SESSION_KEYSPACE,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, SessionKind};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId};
use aruna_core::structs::identity::user::User;
use aruna_core::structs::identity::user::session::UserSession;
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::types::GroupId;
use aruna_core::user::validation::SERVICE_GROUP_ATTRIBUTE;
use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use ulid::Ulid;

struct Fixture {
    _dir: TempDir,
    context: DriverContext,
    node_id: NodeId,
    realm_admin: UserId,
    admin_a: UserId,
    member: UserId,
    account: UserId,
    group_a: GroupId,
    group_b: GroupId,
    now: u64,
}

async fn put(context: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    let effect = StorageEffect::Write {
        key_space: key_space.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    };
    let event = context.storage_handle.send_storage_effect(effect).await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn exists(context: &DriverContext, key_space: &str, key: &str) -> bool {
    let effect = StorageEffect::Read {
        key_space: key_space.to_string(),
        key: key.as_bytes().into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
        other => panic!("unexpected read result: {other:?}"),
    }
}

async fn seed_group(
    context: &DriverContext,
    actor: &Actor,
    admin: UserId,
    users: &[UserId],
) -> GroupId {
    let group_id = Ulid::generate();
    let mut doc = GroupAuthorizationDocument::default_group_doc(admin, actor.realm_id, group_id);
    let role = doc
        .roles
        .values_mut()
        .find(|role| role.name == "user")
        .unwrap();
    role.assigned_users.extend(users.iter().copied());
    let group = Group {
        display_name: "Lab".to_string(),
        group_id,
        realm_id: actor.realm_id,
        roles: doc.roles.keys().copied().collect(),
        owner: admin,
    };
    let key = group_id.to_bytes().to_vec();
    put(
        context,
        GROUP_KEYSPACE,
        key.clone(),
        group.to_bytes(actor).unwrap(),
    )
    .await;
    put(context, AUTH_KEYSPACE, key, doc.to_bytes(actor).unwrap()).await;
    group_id
}

async fn seed_session(context: &DriverContext, owner: UserId, now: u64) -> String {
    let sid = Ulid::generate().to_string();
    let session = UserSession {
        sid: sid.clone(),
        user_id: owner,
        kind: SessionKind::Api,
        label: None,
        created_at: now - 10,
        expires_at: now + 3_600,
        token_hash: bearer_token_hash(&sid),
        revoked: false,
        via: None,
    };
    let index = encode_index(&BTreeSet::from([sid.clone()])).unwrap();
    put(
        context,
        USER_SESSION_KEYSPACE,
        sid.as_bytes().to_vec(),
        session.to_bytes().unwrap(),
    )
    .await;
    put(
        context,
        USER_OWNER_KEYSPACE,
        crate::session::index::owner_key(owner).to_vec(),
        index.to_vec(),
    )
    .await;
    sid
}

async fn seed_credential(context: &DriverContext, owner: UserId, group_id: GroupId) -> String {
    let access_key = Ulid::generate().to_string();
    let access = UserAccess {
        access_key: access_key.clone(),
        user_identity: owner,
        group_id,
        secret: EncryptedS3Secret::empty(),
        expiry: SystemTime::now() + Duration::from_secs(3_600),
        path_restrictions: None,
        issued_by: [0u8; 32],
        revoked_at: None,
    };
    put(
        context,
        USER_ACCESS_KEYSPACE,
        access_key.as_bytes().to_vec(),
        access.to_bytes().unwrap(),
    )
    .await;
    access_key
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let storage = aruna_storage::storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(aruna_tasks::TaskHandle::new()),
        compute_handle: None,
    };
    let realm_id = RealmId::from_bytes([7u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[5u8; 32]).public();
    let user = |n: u64| UserId::local(Ulid::from_parts(n, 1), realm_id);
    let (realm_admin, admin_a, admin_b, member, account) =
        (user(1), user(2), user(3), user(4), user(5));
    let actor = Actor {
        node_id,
        user_id: realm_admin,
        realm_id,
    };
    let realm = CreateRealmConfig {
        actor: actor.clone(),
        realm_description: "tokens".to_string(),
        oidc_providers: Vec::new(),
        node_location: None,
        node_weight: None,
        node_labels: Default::default(),
    };
    drive(CreateRealmOperation::new(realm), &context)
        .await
        .unwrap();
    let mut realm_doc = RealmAuthorizationDocument::default_realm_doc(realm_id);
    for role in realm_doc.roles.values_mut() {
        role.assigned_users.insert(realm_admin);
    }
    put(
        &context,
        AUTH_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        realm_doc.to_bytes(&actor).unwrap(),
    )
    .await;
    let group_a = seed_group(&context, &actor, admin_a, &[member, account]).await;
    let group_b = seed_group(&context, &actor, admin_b, &[member]).await;
    let service = User {
        user_id: account,
        name: "robot".to_string(),
        subject_ids: Vec::new(),
        alias_user_ids: Default::default(),
        attributes: HashMap::from([(SERVICE_GROUP_ATTRIBUTE.to_string(), group_a.to_string())]),
    };
    put(
        &context,
        USER_KEYSPACE,
        account.to_bytes(),
        service.to_bytes(&actor).unwrap(),
    )
    .await;
    Fixture {
        _dir: dir,
        context,
        node_id,
        realm_admin,
        admin_a,
        member,
        account,
        group_a,
        group_b,
        now: aruna_core::time::unix_timestamp_secs(),
    }
}

fn auth(user_id: UserId) -> AuthContext {
    AuthContext {
        user_id,
        realm_id: user_id.realm_id,
        path_restrictions: None,
        session: None,
    }
}

impl Fixture {
    async fn list(
        &self,
        caller: UserId,
        group_id: GroupId,
    ) -> Result<Vec<super::GroupToken>, GroupTokensError> {
        let config = GroupTokensConfig {
            auth_context: auth(caller),
            group_id,
            now: self.now,
        };
        drive(ListTokensOperation::new(config), &self.context).await
    }

    async fn revoke(
        &self,
        caller: UserId,
        group_id: GroupId,
        target: RevokeTarget,
    ) -> Result<super::revoke::Revocation, GroupTokensError> {
        let config = RevokeTokensConfig {
            actor: Actor {
                node_id: self.node_id,
                user_id: caller,
                realm_id: caller.realm_id,
            },
            auth_context: auth(caller),
            group_id,
            target,
            now: self.now,
        };
        drive(RevokeTokensOperation::new(config), &self.context).await
    }
}

#[tokio::test]
async fn member_refused() {
    let fixture = fixture().await;
    let listed = fixture.list(fixture.member, fixture.group_a).await;
    assert_eq!(listed, Err(GroupTokensError::Refused));
    let target = RevokeTarget::Member(fixture.account);
    let revoked = fixture
        .revoke(fixture.member, fixture.group_a, target)
        .await;
    assert_eq!(revoked, Err(GroupTokensError::Refused));
}

#[tokio::test]
async fn admin_lists_own() {
    let fixture = fixture().await;
    let own = seed_credential(&fixture.context, fixture.member, fixture.group_a).await;
    let other = seed_credential(&fixture.context, fixture.member, fixture.group_b).await;
    let session = seed_session(&fixture.context, fixture.account, fixture.now).await;
    seed_session(&fixture.context, fixture.member, fixture.now).await;

    let tokens = fixture
        .list(fixture.admin_a, fixture.group_a)
        .await
        .unwrap();
    let mut ids: Vec<_> = tokens
        .iter()
        .map(|token| (token.kind, token.id.clone()))
        .collect();
    ids.sort_by(|a, b| a.1.cmp(&b.1));
    let mut expected = vec![
        (TokenKind::S3Credential, own),
        (TokenKind::Session, session),
    ];
    expected.sort_by(|a, b| a.1.cmp(&b.1));
    assert_eq!(ids, expected);
    assert!(
        tokens
            .iter()
            .all(|token| token.status == TokenStatus::Active)
    );
    assert!(!tokens.iter().any(|token| token.id == other));
    // The admin of group A administers nothing in group B.
    let foreign = fixture.list(fixture.admin_a, fixture.group_b).await;
    assert_eq!(foreign, Err(GroupTokensError::Refused));
}

#[tokio::test]
async fn realm_admin_allowed() {
    let fixture = fixture().await;
    let credential = seed_credential(&fixture.context, fixture.member, fixture.group_b).await;
    let tokens = fixture
        .list(fixture.realm_admin, fixture.group_b)
        .await
        .unwrap();
    assert_eq!(tokens.len(), 1);
    let target = RevokeTarget::Token {
        kind: TokenKind::S3Credential,
        id: credential.clone(),
    };
    let revoked = fixture
        .revoke(fixture.realm_admin, fixture.group_b, target)
        .await
        .unwrap();
    assert_eq!((revoked.revoked, revoked.credentials), (1, 1));
    assert!(!exists(&fixture.context, USER_ACCESS_KEYSPACE, &credential).await);
}

#[tokio::test]
async fn member_scoped_revocation() {
    let fixture = fixture().await;
    let own = seed_credential(&fixture.context, fixture.member, fixture.group_a).await;
    let other = seed_credential(&fixture.context, fixture.member, fixture.group_b).await;
    let personal = seed_session(&fixture.context, fixture.member, fixture.now).await;

    let target = RevokeTarget::Member(fixture.member);
    let revoked = fixture
        .revoke(fixture.admin_a, fixture.group_a, target)
        .await
        .unwrap();
    assert_eq!((revoked.revoked, revoked.all_tokens), (1, false));
    assert!(!exists(&fixture.context, USER_ACCESS_KEYSPACE, &own).await);
    // Access to group B and the member's own sessions stay untouched.
    assert!(exists(&fixture.context, USER_ACCESS_KEYSPACE, &other).await);
    assert!(exists(&fixture.context, USER_SESSION_KEYSPACE, &personal).await);
    let cutoff = crate::auth::bearer_token::realm_user_cutoff(
        &fixture.context.storage_handle,
        fixture.member.realm_id,
        &fixture.member,
    )
    .await
    .unwrap();
    assert_eq!(cutoff, None);
}

#[tokio::test]
async fn foreign_token_missing() {
    let fixture = fixture().await;
    let other = seed_credential(&fixture.context, fixture.member, fixture.group_b).await;
    let target = RevokeTarget::Token {
        kind: TokenKind::S3Credential,
        id: other.clone(),
    };
    let revoked = fixture
        .revoke(fixture.admin_a, fixture.group_a, target)
        .await;
    assert_eq!(revoked, Err(GroupTokensError::NotFound));
    assert!(exists(&fixture.context, USER_ACCESS_KEYSPACE, &other).await);
}

#[tokio::test]
async fn service_account_cutoff() {
    let fixture = fixture().await;
    let session = seed_session(&fixture.context, fixture.account, fixture.now).await;
    let target = RevokeTarget::Member(fixture.account);
    let revoked = fixture
        .revoke(fixture.admin_a, fixture.group_a, target)
        .await
        .unwrap();
    assert!(revoked.all_tokens);
    assert_eq!(revoked.revoked, 1);
    let tokens = fixture
        .list(fixture.admin_a, fixture.group_a)
        .await
        .unwrap();
    let listed = tokens.iter().find(|token| token.id == session).unwrap();
    assert_eq!(listed.status, TokenStatus::Revoked);
}

#[tokio::test]
async fn session_revoked_once() {
    let fixture = fixture().await;
    let session = seed_session(&fixture.context, fixture.account, fixture.now).await;
    let target = RevokeTarget::Token {
        kind: TokenKind::Session,
        id: session.clone(),
    };
    let first = fixture
        .revoke(fixture.admin_a, fixture.group_a, target.clone())
        .await
        .unwrap();
    assert_eq!(first.revoked, 1);
    // Repeating it finds the session already revoked and changes nothing.
    let again = fixture
        .revoke(fixture.admin_a, fixture.group_a, target)
        .await
        .unwrap();
    assert_eq!(again.revoked, 0);
    let tokens = fixture
        .list(fixture.admin_a, fixture.group_a)
        .await
        .unwrap();
    assert_eq!(tokens[0].status, TokenStatus::Revoked);
}

#[tokio::test]
async fn revoke_all_gated() {
    let fixture = fixture().await;
    let config = |caller: UserId| RevokeAllConfig {
        actor: Actor {
            node_id: fixture.node_id,
            user_id: caller,
            realm_id: caller.realm_id,
        },
        auth_context: auth(caller),
        target: fixture.member,
        now: fixture.now,
    };
    let refused = drive(
        RevokeAllOperation::new(config(fixture.admin_a)),
        &fixture.context,
    )
    .await;
    assert_eq!(refused, Err(RevokeAllError::Refused));
    drive(
        RevokeAllOperation::new(config(fixture.realm_admin)),
        &fixture.context,
    )
    .await
    .unwrap();
    let cutoff = crate::auth::bearer_token::realm_user_cutoff(
        &fixture.context.storage_handle,
        fixture.member.realm_id,
        &fixture.member,
    )
    .await
    .unwrap();
    assert_eq!(cutoff, Some(fixture.now + 1));
}
