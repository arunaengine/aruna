//! Tests sessions at the limit: the oldest are signed out, service accounts keep the limit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::effects::StorageEffect;
use ulid::Ulid;

use crate::session::create::pure_tests::session_config;
use crate::session::index::MAX_USER_SESSIONS;

/// A realm whose user `session_config` creates sessions for, on real storage.
async fn session_realm() -> (
    tempfile::TempDir,
    crate::driver::DriverContext,
    CreateSessionConfig,
    aruna_core::id::NodeId,
) {
    use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
    let dir = tempfile::tempdir().unwrap();
    let storage = aruna_storage::storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let context = crate::driver::DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(aruna_tasks::TaskHandle::new()),
        compute_handle: None,
    };
    let config = session_config();
    let node_id = iroh::SecretKey::from_bytes(&[5u8; 32]).public();
    let actor = aruna_core::structs::identity::auth::Actor {
        node_id,
        user_id: config.user_id,
        realm_id: config.realm_id,
    };
    let realm = CreateRealmConfig {
        actor,
        realm_description: "session realm".to_string(),
        oidc_providers: Vec::new(),
        node_location: None,
        node_weight: None,
        node_labels: Default::default(),
    };
    crate::driver::drive(CreateRealmOperation::new(realm), &context)
        .await
        .unwrap();
    (dir, context, config, node_id)
}

/// Stores the user document of `config`'s user with the given attributes.
async fn store_user(
    context: &crate::driver::DriverContext,
    config: &CreateSessionConfig,
    node_id: aruna_core::id::NodeId,
    attributes: std::collections::HashMap<String, String>,
) {
    use aruna_core::document::DocumentTarget;
    let user = aruna_core::structs::identity::user::User {
        user_id: config.user_id,
        name: "user".to_string(),
        subject_ids: Vec::new(),
        alias_user_ids: Default::default(),
        attributes,
    };
    let target = DocumentTarget::User {
        user_id: config.user_id,
    };
    let actor = aruna_core::structs::identity::auth::Actor {
        node_id,
        user_id: config.user_id,
        realm_id: config.realm_id,
    };
    let write = StorageEffect::Write {
        key_space: target.storage_keyspace().to_string(),
        key: target.storage_key(),
        value: user.to_bytes(&actor).unwrap().into(),
        txn_id: None,
    };
    context.storage_handle.send_storage_effect(write).await;
}

// Signing out works past the 64 self-service revocations a user may hold: the rest move
// the user's cutoff, and the next sign-in still works.
#[tokio::test]
async fn revokes_past_capacity() {
    use crate::driver::drive;
    use crate::session::revoke::RevokeSessionOperation;
    let (_dir, context, config, node_id) = session_realm().await;
    let now = aruna_core::time::unix_timestamp_secs();
    let base = now - 1_000;
    let mut sessions = Vec::new();
    for offset in 0..100 {
        let session = CreateSessionConfig {
            time: base + offset,
            expiry: now + 3_600,
            ..config.clone()
        };
        let created = drive(CreateSessionOperation::new(session), &context)
            .await
            .unwrap();
        sessions.push(created.session);
    }
    let actor = aruna_core::structs::identity::auth::Actor {
        node_id,
        user_id: config.user_id,
        realm_id: config.realm_id,
    };
    for session in &sessions {
        let revoke = RevokeSessionOperation::new(actor.clone(), session.sid.clone(), now);
        let revoked = drive(revoke, &context).await.unwrap();
        assert!(revoked.is_some_and(|revoked| revoked.revoked));
    }
    let cutoff = crate::auth::bearer_token::realm_user_cutoff(
        &context.storage_handle,
        config.realm_id,
        &config.user_id,
    )
    .await
    .unwrap();
    // The last revoked session and every older one are cut off.
    assert_eq!(cutoff, Some(base + 100));
    let next = CreateSessionConfig {
        time: now,
        expiry: now + 3_600,
        ..config
    };
    create_making_room(&context, node_id, next).await.unwrap();
}

// A service account, or one whose document is not here yet, at the limit is refused as
// before; none of its sessions are ended.
#[tokio::test]
async fn service_keeps_limit() {
    use crate::driver::drive;
    for known in [true, false] {
        let (_dir, context, config, node_id) = session_realm().await;
        if known {
            let service = std::collections::HashMap::from([(
                aruna_core::user::validation::SERVICE_GROUP_ATTRIBUTE.to_string(),
                Ulid::from_bytes([8; 16]).to_string(),
            )]);
            store_user(&context, &config, node_id, service).await;
        }
        let now = aruna_core::time::unix_timestamp_secs();
        let base = now - 1_000;
        for offset in 0..MAX_USER_SESSIONS as u64 {
            let session = CreateSessionConfig {
                time: base + offset,
                expiry: now + 3_600,
                ..config.clone()
            };
            drive(CreateSessionOperation::new(session), &context)
                .await
                .unwrap();
        }
        let next = CreateSessionConfig {
            time: now,
            expiry: now + 3_600,
            ..config.clone()
        };
        let refused = create_making_room(&context, node_id, next).await;
        assert_eq!(
            refused,
            Err(CreateSessionError::LimitReached { oldest: base })
        );
        let cutoff = crate::auth::bearer_token::realm_user_cutoff(
            &context.storage_handle,
            config.realm_id,
            &config.user_id,
        )
        .await
        .unwrap();
        assert_eq!(cutoff, None);
    }
}

// A sign-in right after a sign-out moved the cutoff into the next second is issued at the
// cutoff, so its token is not refused at once.
#[tokio::test]
async fn issues_at_cutoff() {
    use crate::driver::drive;
    let (_dir, context, config, node_id) = session_realm().await;
    let now = aruna_core::time::unix_timestamp_secs();
    let actor = aruna_core::structs::identity::auth::Actor {
        node_id,
        user_id: config.user_id,
        realm_id: config.realm_id,
    };
    let raise = super::super::revoke::cutoff_revocation(actor, now, now);
    drive(
        crate::auth::revoke_token::RevokeTokenOperation::new(raise),
        &context,
    )
    .await
    .unwrap();
    let next = CreateSessionConfig {
        time: now,
        expiry: now + 3_600,
        ..config
    };
    let created = create_making_room(&context, node_id, next).await.unwrap();
    assert_eq!(created.session.created_at, now + 1);
    assert_eq!(created.session.expires_at, now + 3_600);
}

// A user at the session limit still signs in: the oldest session is signed out instead.
#[tokio::test]
async fn full_user_login() {
    use crate::driver::drive;
    let (_dir, context, config, node_id) = session_realm().await;
    store_user(&context, &config, node_id, Default::default()).await;
    // The cutoff is read against the wall clock, so the sessions are recent.
    let now = aruna_core::time::unix_timestamp_secs();
    let base = now - 1_000;
    let mut first = None;
    for offset in 0..MAX_USER_SESSIONS as u64 {
        let session = CreateSessionConfig {
            time: base + offset,
            expiry: now + 3_600,
            ..config.clone()
        };
        let created = drive(CreateSessionOperation::new(session), &context)
            .await
            .unwrap();
        first.get_or_insert(created.session);
    }
    let next = CreateSessionConfig {
        time: now,
        expiry: now + 3_600,
        ..config.clone()
    };
    let refused = drive(CreateSessionOperation::new(next.clone()), &context).await;
    assert_eq!(
        refused,
        Err(CreateSessionError::LimitReached { oldest: base })
    );

    create_making_room(&context, node_id, next).await.unwrap();

    let cutoff = crate::auth::bearer_token::realm_user_cutoff(
        &context.storage_handle,
        config.realm_id,
        &config.user_id,
    )
    .await
    .unwrap();
    assert_eq!(cutoff, Some(base + 1));
    assert!(first.unwrap().created_at < base + 1);
}
