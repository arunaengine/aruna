//! Tests realm secret fetch checks, local opens, the node vault cache and its pruning, and how
//! management nodes store, confirm and answer notification settings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::realm_vault::{RealmVaultEntry, RealmVaultKey, RealmVaultPurpose};
use aruna_core::structs::identity::realm::RealmId;
use ed25519_dalek::SigningKey;
use tempfile::TempDir;

use crate::driver::{DriverContext, drive};
use crate::tests::policy::realm_view;
use aruna_core::handle::Handle;

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
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
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

fn stored(value: Option<Vec<u8>>) -> IoResult {
    IoResult::Values(vec![value.map(Into::into)])
}

fn event(event: Event) -> IoResult {
    IoResult::Event(Box::new(event))
}

fn opened(result: Result<SecretBytes, RealmVaultError>) -> IoResult {
    event(Event::Storage(StorageEvent::RealmOpened { result }))
}

fn cached(row: Option<Vec<u8>>, reference: RealmSecretRef) -> IoResult {
    event(Event::Storage(StorageEvent::VaultResult {
        entry: cache_entry(reference),
        secret: row.map(SecretBytes::new),
    }))
}

/// The cache row of `reference`, as a fetch writes it.
fn cache_row(reference: RealmSecretRef) -> Vec<u8> {
    [
        &[reference.entry.purpose.tag()][..],
        &reference.revision.to_bytes(),
        SECRET,
    ]
    .concat()
}

/// The answer of the fetch child.
fn fetched(fetched: Fetched<SecretBytes>) -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<_, FetchError>(fetched)))
}

fn effect<T>(effect: Effect) -> Next<T> {
    Next::Call(IoCall::Effect(Box::new(effect)))
}

#[test]
fn key_holder_opens() {
    // A key holder opens the record itself and neither caches nor fetches.
    let (config, reference) = realm_config(2);
    let mut operation = ReadSecretOperation::new(&config, reference);
    let record = current_record(&config, reference).unwrap();
    let open = Effect::Storage(StorageEffect::RealmOpen(Box::new(record_sealed(
        &config, record,
    ))));
    let secret = SecretBytes::new(SECRET.to_vec());
    assert_eq!(operation.first(), Ok(effect(open)));
    assert_eq!(
        operation.next(opened(Ok(secret.clone()))),
        Ok(Next::Done(secret))
    );
}

#[test]
fn fetches_then_caches() {
    // Without the keys and a cached row, the management nodes are asked and the answer cached.
    let (config, reference) = realm_config(2);
    let mut operation = ReadSecretOperation::new(&config, reference);
    operation.first().unwrap();
    assert_eq!(
        operation.next(opened(Err(RealmVaultError::NoKey))),
        Ok(effect(read_secret(cache_entry(reference), None)))
    );
    let fetch = operation.next(cached(None, reference));
    assert!(matches!(fetch, Ok(Next::Call(IoCall::Child(_)))));
    let found = Fetched::Found {
        holder: node(1),
        value: SecretBytes::new(SECRET.to_vec()),
    };
    let write = write_secret(
        cache_entry(reference),
        SecretBytes::new(cache_row(reference)),
        None,
    );
    assert_eq!(operation.next(fetched(found)), Ok(effect(write)));
    let written = event(Event::Storage(StorageEvent::WriteResult {
        key: Vec::new().into(),
    }));
    let Ok(Next::Done(secret)) = operation.next(written) else {
        panic!("expected the fetched secret");
    };
    assert_eq!(secret.expose(), SECRET);
}

#[test]
fn cache_answers_current() {
    // A cached row of the current revision answers; a rotated revision misses and fetches.
    let (config, reference) = realm_config(2);
    let mut operation = ReadSecretOperation::new(&config, reference);
    operation.first().unwrap();
    operation.next(opened(Err(RealmVaultError::NoKey))).unwrap();
    let Ok(Next::Done(secret)) = operation.next(cached(Some(cache_row(reference)), reference))
    else {
        panic!("expected the cached secret");
    };
    assert_eq!(secret.expose(), SECRET);

    let (rotated, newer) = realm_config(3);
    let mut operation = ReadSecretOperation::new(&rotated, newer);
    operation.first().unwrap();
    operation.next(opened(Err(RealmVaultError::NoKey))).unwrap();
    let fetch = operation.next(cached(Some(cache_row(reference)), newer));
    assert!(matches!(fetch, Ok(Next::Call(IoCall::Child(_)))));
}

#[test]
fn read_failures_end() {
    // A reference ahead of the config waits for replication; refusals and odd answers fail.
    let (config, _) = realm_config(2);
    let (_, newer) = realm_config(3);
    let mut ahead = ReadSecretOperation::new(&config, newer);
    assert_eq!(ahead.first(), Err(RealmSecretError::NotReplicated));

    let (config, reference) = realm_config(2);
    let mut refused = ReadSecretOperation::new(&config, reference);
    refused.first().unwrap();
    refused.next(opened(Err(RealmVaultError::NoKey))).unwrap();
    refused.next(cached(None, reference)).unwrap();
    assert!(matches!(
        refused.next(fetched(Fetched::Denied)),
        Err(RealmSecretError::Unavailable(_))
    ));

    let mut odd = ReadSecretOperation::new(&config, reference);
    odd.first().unwrap();
    assert!(odd.next(stored(None)).is_err());
}

#[test]
fn serves_peer_secret() {
    // A management node answers a server peer with the opened current revision.
    let (config, reference) = realm_config(2);
    let request = ServeRequest::Secret(reference);
    let mut operation = ServeSecretOperation::new(config.realm_id, node(1), node(2), request);
    let target = DocumentTarget::RealmConfig {
        realm_id: config.realm_id,
    };
    let read = (target.storage_keyspace().to_string(), target.storage_key());
    assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(vec![read]))));
    let record = current_record(&config, reference).unwrap();
    let open = Effect::Storage(StorageEffect::RealmOpen(Box::new(record_sealed(
        &config, record,
    ))));
    let (config_row, _) = realm_view(&config, UserId::nil(config.realm_id));
    let secret = SecretBytes::new(SECRET.to_vec());
    assert_eq!(
        operation.next(stored(Some(config_row.to_vec()))),
        Ok(effect(open))
    );
    assert_eq!(
        operation.next(opened(Ok(secret.clone()))),
        Ok(Next::Done(Ok(secret)))
    );
}

#[test]
fn serve_refusals_answer() {
    // A device peer is refused before anything opens; a node without keys is unavailable.
    let (config, reference) = realm_config(2);
    let (config_row, _) = realm_view(&config, UserId::nil(config.realm_id));
    let serve = |peer| {
        let request = ServeRequest::Secret(reference);
        let mut operation = ServeSecretOperation::new(config.realm_id, node(1), peer, request);
        operation.first().unwrap();
        operation
    };
    let mut device = serve(node(3));
    assert_eq!(
        device.next(stored(Some(config_row.to_vec()))),
        Ok(Next::Done(Err(MetadataReadError::Forbidden)))
    );
    let mut keyless = serve(node(2));
    keyless.next(stored(Some(config_row.to_vec()))).unwrap();
    assert_eq!(
        keyless.next(opened(Err(RealmVaultError::NoKey))),
        Ok(Next::Done(Err(MetadataReadError::Unavailable)))
    );
}

#[tokio::test]
async fn prunes_stale_rows() {
    let (_dir, ctx) = context();
    let (config, reference) = realm_config(2);
    let write = write_secret(
        cache_entry(reference),
        SecretBytes::new(cache_row(reference)),
        None,
    );
    ctx.storage_handle.send_effect(write).await;
    assert_eq!(drive(PruneCacheOperation::new(&config), &ctx).await, Ok(0));

    let (rotated, _) = realm_config(3);
    assert_eq!(drive(PruneCacheOperation::new(&rotated), &ctx).await, Ok(1));
    let event = ctx
        .storage_handle
        .send_effect(read_secret(cache_entry(reference), None))
        .await;
    let row = parse_secret(event, cache_entry(reference), |bytes| Ok(bytes.to_vec())).unwrap();
    assert_eq!(row, None);
}

mod settings {
    use super::*;
    use aruna_core::realm_vault::seal_setting;
    use aruna_core::register::RegisterStamp;
    use aruna_core::structs::execution::email::EmailPreference;
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

    /// An email setting of `owner()` written by node 2; on settings seal the address.
    fn email(id: u8, on: bool) -> (DocumentTarget, Vec<u8>) {
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
            stamp: RegisterStamp {
                counter: 1,
                node_id: node(2),
                write_id: id,
            },
            categories,
            sealed,
            created_at_ms: 1,
        };
        let target = SettingChannel::Email.target(owner(), id);
        (target, preference.to_bytes().unwrap())
    }

    fn open(
        peer: NodeId,
        setting: &(DocumentTarget, Vec<u8>),
    ) -> Result<SecretBytes, MetadataReadError> {
        let keys = aruna_core::realm_vault::RealmKeys::derive(&realm_key());
        let sealed = sealed_setting(&config(), peer, &setting.0, &setting.1)?;
        sealed.open(&keys).map_err(setting_error)
    }

    #[test]
    fn open_returns_secret() {
        let setting = email(1, true);
        for peer in [node(1), node(2), node(4)] {
            assert_eq!(open(peer, &setting).unwrap().expose(), ADDRESS);
        }
    }

    #[test]
    fn open_refuses_device() {
        let setting = email(1, true);
        for peer in [node(3), node(9)] {
            assert_eq!(
                open(peer, &setting).err(),
                Some(MetadataReadError::Forbidden)
            );
        }
    }

    #[test]
    fn open_checks_document() {
        // The sealed secret is bound to its target, so it opens only under the original one.
        let (_, bytes) = email(1, true);
        let moved = (
            SettingChannel::Email.target(owner(), Ulid::from_bytes([2; 16])),
            bytes,
        );
        assert_eq!(
            open(node(2), &moved).err(),
            Some(MetadataReadError::Forbidden)
        );
        let broken = (email(1, true).0, vec![1, 2]);
        assert_eq!(
            open(node(2), &broken).err(),
            Some(MetadataReadError::Forbidden)
        );
        let off = email(1, false);
        assert_eq!(open(node(2), &off).err(), Some(MetadataReadError::NotFound));
    }

    #[test]
    fn setting_needs_key() {
        // A management node without the realm keys cannot open a setting for a peer.
        let config = config();
        let (target, bytes) = email(1, true);
        let request = ServeRequest::Setting { target, bytes };
        let mut operation = ServeSecretOperation::new(config.realm_id, node(1), node(2), request);
        operation.first().unwrap();
        let (config_row, _) = realm_view(&config, UserId::nil(config.realm_id));
        let open = operation.next(stored(Some(config_row.to_vec())));
        assert!(matches!(open, Ok(Next::Call(IoCall::Effect(_)))));
        assert_eq!(
            operation.next(opened(Err(RealmVaultError::NoKey))),
            Ok(Next::Done(Err(MetadataReadError::Unavailable)))
        );
    }
}
