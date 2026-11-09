//! Tests realm secret fetch checks, local opens, the node vault cache and its pruning, and how
//! management nodes store, confirm and answer notification settings.
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

mod settings {
    use super::*;
    use aruna_core::document::DocumentSyncRevision;
    use aruna_core::keyspaces::EMAIL_SETTINGS_KEYSPACE;
    use aruna_core::realm_vault::seal_setting;
    use aruna_core::structs::execution::email::EmailPreference;
    use aruna_core::structs::execution::setting::setting_key;
    use aruna_core::structs::placement::record::PlacementRef;
    use std::collections::BTreeSet;

    const ADDRESS: &[u8] = b"ada@example.org";

    fn realm_key() -> SigningKey {
        SigningKey::from_bytes(&[5u8; 32])
    }

    /// Node 1 manages, nodes 2 and 4 serve, node 3 is a device; the realm id is the realm key.
    fn config() -> RealmConfigDocument {
        let realm_id = RealmId::from_bytes(realm_key().verifying_key().to_bytes());
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(node(1), RealmNodeKind::Management);
        config.ensure_node(node(2), RealmNodeKind::Server);
        let owner = UserId::nil(realm_id);
        config.ensure_node(node(3), RealmNodeKind::User { owner });
        config.ensure_node(node(4), RealmNodeKind::Server);
        config
    }

    fn owner() -> UserId {
        UserId::local(Ulid::from_bytes([6u8; 16]), config().realm_id)
    }

    /// An email setting of `owner()`; on settings seal the address.
    fn email(id: u8, version: u64, on: bool) -> (DocumentTarget, Vec<u8>) {
        let id = Ulid::from_bytes([id; 16]);
        let secret = SecretBytes::new(ADDRESS.to_vec());
        let sealed = on.then(|| {
            seal_setting(
                &config().realm_id,
                SettingChannel::Email,
                &owner(),
                id,
                &secret,
            )
            .unwrap()
        });
        let categories = match on {
            true => BTreeSet::from(["share.upload".to_string()]),
            false => BTreeSet::new(),
        };
        let preference = EmailPreference {
            preference_id: id,
            user_id: owner(),
            version,
            categories,
            sealed,
            created_at_ms: 1,
        };
        let target = SettingChannel::Email.target(owner(), id);
        (target, preference.to_bytes().unwrap())
    }

    fn change(actor: NodeId) -> DocumentChange {
        DocumentChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::from_bytes([7u8; 16]),
                actor,
                updated_at_ms: 1,
            },
            kind: DocumentChangeKind::Upsert,
            placement: PlacementRef::NIL,
        }
    }

    async fn store(
        context: &DriverContext,
        peer: NodeId,
        setting: (DocumentTarget, Vec<u8>),
    ) -> Result<StoreAnswer, MetadataReadError> {
        let (target, bytes) = setting;
        store_setting(
            &context.storage_handle,
            &config(),
            peer,
            target,
            bytes,
            change(peer),
        )
        .await
    }

    async fn answer(context: &DriverContext) -> SettingAnswer {
        let key = RealmSealKey::derive(&realm_key());
        let storage = &context.storage_handle;
        read_setting(storage, &key, &config(), owner(), SettingChannel::Email)
            .await
            .unwrap()
    }

    async fn confirm(
        context: &DriverContext,
        peer: NodeId,
        target: DocumentTarget,
    ) -> Result<(), MetadataReadError> {
        confirm_setting(&context.storage_handle, &config(), peer, target).await
    }

    fn winner_id(answer: &SettingAnswer) -> Option<Ulid> {
        let winner = answer.winner.as_ref()?;
        Some(SettingChannel::Email.head(&winner.bytes).unwrap().id)
    }

    #[tokio::test]
    async fn store_needs_higher() {
        let (_dir, context) = context();
        assert_eq!(
            store(&context, node(2), email(1, 1, true)).await,
            Ok(StoreAnswer::Stored)
        );
        let same_version = store(&context, node(2), email(2, 1, false)).await;
        assert_eq!(same_version, Ok(StoreAnswer::Conflict { max_version: 1 }));
        assert_eq!(
            store(&context, node(2), email(3, 2, false)).await,
            Ok(StoreAnswer::Stored)
        );
        assert_eq!(answer(&context).await.max_version, 2);
    }

    #[tokio::test]
    async fn store_replay_confirms() {
        let (_dir, context) = context();
        let setting = email(1, 1, true);
        assert_eq!(
            store(&context, node(2), setting.clone()).await,
            Ok(StoreAnswer::Stored)
        );
        // A retried request with the same bytes and revision counts as stored.
        assert_eq!(
            store(&context, node(2), setting).await,
            Ok(StoreAnswer::Stored)
        );
    }

    #[tokio::test]
    async fn store_conflict_rejected() {
        let (_dir, context) = context();
        let (target, bytes) = email(1, 1, false);
        assert_eq!(
            store(&context, node(2), (target.clone(), bytes.clone())).await,
            Ok(StoreAnswer::Stored)
        );
        let changed = store(&context, node(2), email(1, 1, true)).await;
        assert_eq!(changed, Ok(StoreAnswer::Conflict { max_version: 1 }));
        // The first copy stays: confirming moves the off setting.
        confirm(&context, node(2), target).await.unwrap();
        let stored = super::answer_bytes(&answer(&context).await);
        assert_eq!(stored, Some(bytes));
    }

    #[tokio::test]
    async fn store_actor_bound() {
        let (_dir, context) = context();
        let realm = config();
        let (target, bytes) = email(1, 1, true);
        let foreign = change(node(4));
        let stored = store_setting(
            &context.storage_handle,
            &realm,
            node(2),
            target,
            bytes,
            foreign,
        );
        assert_eq!(stored.await, Err(MetadataReadError::Forbidden));
    }

    #[tokio::test]
    async fn store_generation_checked() {
        let (_dir, context) = context();
        let realm = config();
        for kind in [DocumentChangeKind::Upsert, DocumentChangeKind::Delete] {
            let (target, bytes) = email(1, 1, true);
            let mut later = change(node(2));
            later.kind = kind;
            later.current.generation = 2;
            let stored = store_setting(
                &context.storage_handle,
                &realm,
                node(2),
                target,
                bytes,
                later,
            );
            assert_eq!(stored.await, Err(MetadataReadError::Forbidden));
        }
        let (target, _) = email(1, 1, true);
        let broken = store_setting(
            &context.storage_handle,
            &realm,
            node(2),
            target,
            vec![1, 2],
            change(node(2)),
        );
        assert_eq!(broken.await, Err(MetadataReadError::Forbidden));
    }

    #[tokio::test]
    async fn store_refuses_device() {
        let (_dir, context) = context();
        for peer in [node(3), node(9)] {
            assert_eq!(
                store(&context, peer, email(1, 1, true)).await,
                Err(MetadataReadError::Forbidden)
            );
        }
        let read = serve_setting(&context, None, node(2), owner(), SettingChannel::Email);
        assert_eq!(
            read.await,
            Err(MetadataReadError::Unavailable),
            "only key holders answer"
        );
    }

    #[tokio::test]
    async fn confirm_moves_tentative() {
        let (_dir, context) = context();
        let (target, bytes) = email(1, 1, true);
        store(&context, node(2), (target.clone(), bytes.clone()))
            .await
            .unwrap();
        assert_eq!(
            answer(&context).await.winner,
            None,
            "a stored setting is tentative"
        );
        confirm(&context, node(2), target.clone()).await.unwrap();
        let confirmed = answer(&context).await;
        let winner = confirmed.winner.unwrap();
        assert_eq!(winner.bytes, bytes);
        assert_eq!(winner.secret.unwrap().expose(), ADDRESS);
        let storage = &context.storage_handle;
        let tentative = tentative_settings(storage, SettingChannel::Email, &owner(), None).await;
        assert!(tentative.unwrap().is_empty());
        let read = StorageEffect::Read {
            key_space: SYNC_REVISION_KEYSPACE.to_string(),
            key: sync_revision_key(&target),
            txn_id: None,
        };
        let revision = match storage.send_storage_effect(read).await {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            other => panic!("unexpected revision read: {other:?}"),
        };
        assert!(
            revision.is_some(),
            "the confirmed setting keeps its revision"
        );
    }

    #[tokio::test]
    async fn confirm_actor_bound() {
        let (_dir, context) = context();
        let (target, bytes) = email(1, 1, true);
        store(&context, node(2), (target.clone(), bytes))
            .await
            .unwrap();
        assert_eq!(
            confirm(&context, node(4), target.clone()).await,
            Err(MetadataReadError::Forbidden)
        );
        assert_eq!(
            confirm(&context, node(3), target).await,
            Err(MetadataReadError::Forbidden)
        );
    }

    #[tokio::test]
    async fn confirm_unknown_rejected() {
        let (_dir, context) = context();
        let (target, _) = email(1, 1, true);
        assert_eq!(
            confirm(&context, node(2), target).await,
            Err(MetadataReadError::NotFound)
        );
    }

    #[tokio::test]
    async fn confirm_repeat_succeeds() {
        let (_dir, context) = context();
        let (target, bytes) = email(1, 1, true);
        store(&context, node(2), (target.clone(), bytes))
            .await
            .unwrap();
        confirm(&context, node(2), target.clone()).await.unwrap();
        assert_eq!(confirm(&context, node(2), target).await, Ok(()));
    }

    #[tokio::test]
    async fn tentative_never_decides() {
        let (_dir, context) = context();
        let (off, _) = email(1, 1, false);
        store(&context, node(2), email(1, 1, false)).await.unwrap();
        confirm(&context, node(2), off).await.unwrap();
        store(&context, node(2), email(2, 2, true)).await.unwrap();
        let read = answer(&context).await;
        assert_eq!(winner_id(&read), Some(Ulid::from_bytes([1; 16])));
        assert!(
            read.winner.unwrap().secret.is_none(),
            "the confirmed off setting decides"
        );
    }

    #[tokio::test]
    async fn tentative_raises_version() {
        let (_dir, context) = context();
        store(&context, node(2), email(1, 1, true)).await.unwrap();
        store(&context, node(2), email(2, 2, true)).await.unwrap();
        let read = answer(&context).await;
        assert_eq!((read.max_version, read.winner), (2, None));
    }

    #[tokio::test]
    async fn sync_counts_confirmed() {
        // A setting that arrives through sync is confirmed, and drops its tentative copy.
        let (_dir, context) = context();
        let (target, bytes) = email(1, 1, true);
        store(&context, node(2), (target.clone(), bytes.clone()))
            .await
            .unwrap();
        let row = (
            EMAIL_SETTINGS_KEYSPACE.to_string(),
            setting_key(&owner(), Ulid::from_bytes([1; 16])).into(),
            bytes.clone().into(),
        );
        let revision = sync_revision_entry(&target, &change(node(2))).unwrap();
        let write = StorageEffect::BatchWrite {
            writes: vec![row, revision],
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(write).await;
        assert_eq!(
            winner_id(&answer(&context).await),
            Some(Ulid::from_bytes([1; 16]))
        );
        assert_eq!(
            store(&context, node(2), (target, bytes)).await,
            Ok(StoreAnswer::Stored)
        );
        let storage = &context.storage_handle;
        let tentative = tentative_settings(storage, SettingChannel::Email, &owner(), None).await;
        assert!(tentative.unwrap().is_empty());
    }
}

/// The confirmed winner bytes of an answer.
fn answer_bytes(answer: &SettingAnswer) -> Option<Vec<u8>> {
    answer.winner.as_ref().map(|winner| winner.bytes.clone())
}
