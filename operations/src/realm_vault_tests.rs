//! Tests realm secret fetch checks, local opens, the node vault cache and its pruning.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::realm_vault::{RealmVaultEntry, RealmVaultPurpose};
use aruna_core::structs::identity::realm::RealmId;
use ed25519_dalek::SigningKey;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::TempDir;

const SECRET: &[u8] = b"canary-9e41";

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn key() -> RealmVaultKey {
    RealmVaultKey::derive(&SigningKey::from_bytes(&[5u8; 32]))
}

/// Node 1 manages, node 2 serves, node 3 is a device; the config holds one revision of an entry.
fn realm_config(revision: u8) -> (RealmConfigDocument, RealmSecretRef) {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(node(1), RealmNodeKind::Management);
    config.ensure_node(node(2), RealmNodeKind::Server);
    let owner = UserId::nil(realm_id);
    config.ensure_node(node(3), RealmNodeKind::User { owner });
    let entry = RealmVaultEntry::new(RealmVaultPurpose::SmtpPassword, Ulid::from_bytes([4; 16]));
    let reference = RealmSecretRef {
        entry,
        revision: Ulid::from_bytes([revision; 16]),
    };
    let secret = SecretBytes::new(SECRET.to_vec());
    config.vault.push(RealmVaultRecord {
        entry,
        revision: reference.revision,
        sealed: key()
            .seal(&realm_id, entry, reference.revision, &secret)
            .unwrap(),
        changed_by: owner,
        changed_at_ms: 1,
    });
    (config, reference)
}

fn context() -> (TempDir, DriverContext) {
    let dir = tempfile::tempdir().unwrap();
    let storage_handle = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    storage_handle.open_vault(aruna_core::node_vault::NodeVaultKey::random());
    let context = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    (dir, context)
}

#[test]
fn serves_infrastructure_only() {
    let (config, reference) = realm_config(2);
    assert!(check_fetch(&config, node(1), node(2), reference).is_ok());
    assert!(check_fetch(&config, node(1), node(1), reference).is_ok());
    let refused = |local, peer, reference| check_fetch(&config, local, peer, reference).err();
    assert_eq!(
        refused(node(1), node(3), reference),
        Some(MetadataReadError::Forbidden)
    );
    assert_eq!(
        refused(node(1), node(9), reference),
        Some(MetadataReadError::Forbidden)
    );
    assert_eq!(
        refused(node(2), node(1), reference),
        Some(MetadataReadError::Unavailable)
    );
    let older = RealmSecretRef {
        revision: Ulid::from_bytes([1; 16]),
        ..reference
    };
    assert_eq!(
        refused(node(1), node(2), older),
        Some(MetadataReadError::Forbidden)
    );
    let newer = RealmSecretRef {
        revision: Ulid::from_bytes([3; 16]),
        ..reference
    };
    assert_eq!(
        refused(node(1), node(2), newer),
        Some(MetadataReadError::Unavailable)
    );
}

#[tokio::test]
async fn opens_caches_refetches() {
    let (_dir, ctx) = context();
    let (config, reference) = realm_config(2);
    let fetches = AtomicUsize::new(0);
    let fetches = &fetches;
    let fetch = || async move {
        fetches.fetch_add(1, Ordering::SeqCst);
        Ok(SecretBytes::new(SECRET.to_vec()))
    };

    // A key holder opens the record itself and caches nothing.
    let opened = secret_from(&ctx, Some(&key()), &config, reference, fetch)
        .await
        .unwrap();
    assert_eq!(opened.expose(), SECRET);
    assert_eq!(fetches.load(Ordering::SeqCst), 0);

    // Other nodes fetch once, then answer from the node vault cache.
    for _ in 0..2 {
        let read = secret_from(&ctx, None, &config, reference, fetch)
            .await
            .unwrap();
        assert_eq!(read.expose(), SECRET);
    }
    assert_eq!(fetches.load(Ordering::SeqCst), 1);

    // A rotation misses the cache and fetches again.
    let (rotated, newer) = realm_config(3);
    secret_from(&ctx, None, &rotated, newer, fetch)
        .await
        .unwrap();
    assert_eq!(fetches.load(Ordering::SeqCst), 2);

    // A reference ahead of the replicated config waits for replication.
    let ahead = secret_from(&ctx, None, &config, newer, fetch).await;
    assert!(matches!(ahead, Err(RealmSecretError::NotReplicated)));
}

#[tokio::test]
async fn prunes_stale_rows() {
    let (_dir, ctx) = context();
    let (config, reference) = realm_config(2);
    let fetch = || async { Ok(SecretBytes::new(SECRET.to_vec())) };
    secret_from(&ctx, None, &config, reference, fetch)
        .await
        .unwrap();
    assert_eq!(prune_secret_cache(&ctx, &config).await, Ok(0));

    let (rotated, _) = realm_config(3);
    assert_eq!(prune_secret_cache(&ctx, &rotated).await, Ok(1));
    let event = ctx
        .storage_handle
        .send_effect(read_secret(cache_entry(reference), None))
        .await;
    let row = parse_secret(event, cache_entry(reference), |bytes| Ok(bytes.to_vec())).unwrap();
    assert_eq!(row, None);
}
