//! Tests the realm email profile writes: vault entries, kept passwords, tombstones and refusals.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::document_repository::GetConfigOperation;
use crate::driver::{DriverContext, drive};
use crate::testing::path_context;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId, RealmNodeKind};
use ed25519_dalek::SigningKey;
use tempfile::tempdir;

fn actor(seed: u8) -> Actor {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    Actor {
        node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
        user_id: UserId::local(Ulid::from_bytes([1u8; 16]), realm_id),
        realm_id,
    }
}

fn key() -> RealmVaultKey {
    RealmVaultKey::derive(&SigningKey::from_bytes(&[5u8; 32]))
}

fn input(username: Option<&str>, password: Option<&str>) -> EmailInput {
    EmailInput {
        host: "smtp.example.org".to_string(),
        port: Some(587),
        tls: SmtpTls::Starttls,
        username: username.map(str::to_string),
        password: password.map(|password| SecretBytes::new(password.as_bytes().to_vec())),
        from: "Aruna <aruna@example.org>".to_string(),
    }
}

fn email_config(actor: &Actor, profile: Option<EmailInput>) -> SetEmailConfig {
    SetEmailConfig {
        actor: actor.clone(),
        auth_context: AuthContext {
            user_id: actor.user_id,
            realm_id: actor.realm_id,
            path_restrictions: None,
            session: None,
        },
        vault_key: key(),
        profile,
    }
}

async fn write(ctx: &DriverContext, key_space: &str, key: Key, value: Vec<u8>) {
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key,
            value: value.into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected event: {other:?}"),
    }
}

/// Seeds a config where `kind` describes the actor's node and makes the actor a realm admin.
async fn seed(ctx: &DriverContext, actor: &Actor, kind: RealmNodeKind) {
    let mut config = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
    config.ensure_node(actor.node_id, kind);
    let target = DocumentTarget::RealmConfig {
        realm_id: actor.realm_id,
    };
    let value = config.to_bytes(actor).unwrap();
    write(ctx, target.storage_keyspace(), target.storage_key(), value).await;
    let mut document = RealmAuthorizationDocument::default_realm_doc(actor.realm_id);
    for role in document.roles.values_mut() {
        role.assigned_users.insert(actor.user_id);
    }
    let key = (*actor.realm_id.as_bytes()).into();
    write(ctx, AUTH_KEYSPACE, key, document.to_bytes(actor).unwrap()).await;
}

async fn set(
    ctx: &DriverContext,
    actor: &Actor,
    profile: Option<EmailInput>,
) -> Result<Option<RealmEmailProfile>, SetEmailError> {
    drive(SetEmailOperation::new(email_config(actor, profile)), ctx).await
}

fn opened(config: &RealmConfigDocument) -> Vec<Vec<u8>> {
    (config.vault.iter())
        .map(|record| {
            key()
                .open(&config.realm_id, record)
                .unwrap()
                .expose()
                .to_vec()
        })
        .collect()
}

#[tokio::test]
async fn stores_profile_entry() {
    // Profile and sealed password land together; a replace without password keeps the entry.
    let dir = tempdir().unwrap();
    let ctx = path_context(dir.path().to_str().unwrap());
    let actor = actor(1);
    seed(&ctx, &actor, RealmNodeKind::Management).await;

    let first = set(&ctx, &actor, Some(input(Some("mailer"), Some("secret-1"))))
        .await
        .unwrap()
        .unwrap();
    let config = drive(GetConfigOperation::new(actor.realm_id), &ctx)
        .await
        .unwrap();
    assert_eq!(config.email.as_ref(), Some(&first));
    let password = first.password.expect("a password reference");
    assert_eq!(config.vault.len(), 1);
    assert_eq!(config.vault[0].revision, password.revision);
    assert_eq!(opened(&config), vec![b"secret-1".to_vec()]);

    let kept = set(&ctx, &actor, Some(input(Some("mailer"), None)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(kept.password, Some(password));
    assert_ne!(kept.revision, first.revision);

    let rotated = set(&ctx, &actor, Some(input(Some("mailer"), Some("secret-2"))))
        .await
        .unwrap()
        .unwrap();
    let rotated_password = rotated.password.unwrap();
    assert_eq!(rotated_password.entry, password.entry);
    assert_ne!(rotated_password.revision, password.revision);
    let config = drive(GetConfigOperation::new(actor.realm_id), &ctx)
        .await
        .unwrap();
    assert_eq!(opened(&config), vec![b"secret-2".to_vec()]);

    let renamed = set(&ctx, &actor, Some(input(Some("other"), None))).await;
    assert_eq!(renamed, Err(SetEmailError::PasswordRequired));
    let orphan = set(&ctx, &actor, Some(input(None, Some("secret-3")))).await;
    assert!(matches!(orphan, Err(SetEmailError::InvalidProfile { .. })));

    assert_eq!(set(&ctx, &actor, None).await, Ok(None));
    let config = drive(GetConfigOperation::new(actor.realm_id), &ctx)
        .await
        .unwrap();
    assert_eq!(config.email, None);
    assert!(config.vault.is_empty());
}

#[tokio::test]
async fn refuses_server_node() {
    let dir = tempdir().unwrap();
    let ctx = path_context(dir.path().to_str().unwrap());
    let actor = actor(1);
    seed(&ctx, &actor, RealmNodeKind::Server).await;
    let refused = set(&ctx, &actor, Some(input(None, None))).await;
    assert_eq!(refused, Err(SetEmailError::NotManagementNode));
}

#[test]
fn conflict_reruns_change() {
    // A commit lost to a background config write reruns the whole change, reading again.
    let conflict = SetEmailError::from(IoError::Conflict);
    assert!(EmailWrite::is_conflict(&conflict));
    assert_eq!(EmailWrite::conflict_retries(), COMMIT_ATTEMPTS as u32 - 1);
    let write = EmailWrite::new(Arc::new(email_config(&actor(1), None)));
    assert_eq!(write.transaction(), TransactionMode::Write);
}

#[test]
fn write_seals_password() {
    // One transaction reads the config and writes it with the sealed password and its profile.
    let actor = actor(1);
    let mut config = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
    config.ensure_node(actor.node_id, RealmNodeKind::Management);
    let profile = input(Some("mailer"), Some("secret-1"));
    let mut write = EmailWrite::new(Arc::new(email_config(&actor, Some(profile))));
    write.first().unwrap();
    let read = IoResult::Values(vec![Some(config.to_bytes(&actor).unwrap().into()), None]);
    let Ok(Next::Call(IoCall::Write(rows))) = write.next(read) else {
        panic!("expected the config write");
    };
    let target = DocumentTarget::RealmConfig {
        realm_id: actor.realm_id,
    };
    let (_, _, value) = rows
        .iter()
        .find(|(space, key, _)| space == target.storage_keyspace() && *key == target.storage_key())
        .expect("the config is written");
    let written = RealmConfigDocument::from_bytes(value).unwrap();
    assert_eq!(opened(&written), vec![b"secret-1".to_vec()]);
    let Ok(Next::Then { output, calls }) = write.next(IoResult::Done) else {
        panic!("expected the profile with the drain wake");
    };
    assert_eq!(output, written.email);
    assert!(matches!(calls.as_slice(), [IoCall::Effect(_)]));
}

#[test]
fn denied_is_terminal() {
    let mut operation = SetEmailOperation::new(email_config(&actor(1), None));
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let denied = IoResult::Child(ChildOutcome::new(Ok::<_, AuthorizationError>(false)));
    assert_eq!(operation.next(denied), Err(SetEmailError::Unauthorized));
}

/// Two management nodes set a first profile without seeing each other.
fn concurrent_firsts() -> (AdminDocumentState, BTreeSet<RealmVaultEntry>) {
    let target = AdminDocumentTarget::RealmConfig {
        realm_id: actor(1).realm_id,
    };
    let base = AdminDocumentState::new(target);
    let mut merged = base.clone();
    let mut entries = BTreeSet::new();
    let mut rivals = Vec::new();
    for seed in [1u8, 2] {
        let mut state = base.clone();
        let operation = EmailWrite::new(Arc::new(email_config(
            &actor(seed),
            Some(input(Some("mailer"), Some("first"))),
        )));
        let (operations, profile) = operation.email_writes(&state).unwrap();
        entries.insert(profile.unwrap().password.unwrap().entry);
        for operation in operations {
            rivals.push(state.apply_operation(&actor(seed), operation).unwrap());
        }
    }
    for event in &rivals {
        merged.apply(event).unwrap();
    }
    assert_eq!(merged.materialized_realm_email(), None);
    assert_eq!(merged.materialized_realm_vault().len(), 2);
    (merged, entries)
}

fn apply_change(
    state: &mut AdminDocumentState,
    profile: Option<EmailInput>,
) -> Option<RealmEmailProfile> {
    let operation = EmailWrite::new(Arc::new(email_config(&actor(1), profile)));
    let (operations, profile) = operation.email_writes(state).unwrap();
    for operation in operations {
        state.apply_operation(&actor(1), operation).unwrap();
    }
    profile
}

#[test]
fn delete_tombstones_candidates() {
    // A delete after a profile conflict leaves no competing password fetchable.
    let (mut state, entries) = concurrent_firsts();
    assert_eq!(apply_change(&mut state, None), None);
    assert_eq!(state.materialized_realm_email(), None);
    assert!(state.materialized_realm_vault().is_empty());
    for entry in entries {
        let path = aruna_core::reducer::vault_entry_path(&entry);
        assert_eq!(state.user_subject_ids[&path].value, None);
    }
}

#[test]
fn resolving_put_wins() {
    // The write that resolves the conflict mints its own entry and tombstones both candidates.
    let (mut state, entries) = concurrent_firsts();
    let resolved = apply_change(&mut state, Some(input(Some("mailer"), Some("final"))));
    let password = resolved.unwrap().password.unwrap();
    assert!(!entries.contains(&password.entry));
    assert!(state.materialized_realm_email().is_some());
    let live: Vec<_> = (state.materialized_realm_vault().iter())
        .map(|record| (record.entry, record.revision))
        .collect();
    assert_eq!(live, vec![(password.entry, password.revision)]);
}
