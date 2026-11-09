//! Tests who may store and fetch artifact copies, and that only exact published bytes are used.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::structs::identity::realm::RealmId;
use tempfile::{TempDir, tempdir};

const FIXTURE: &[u8] = include_bytes!("../../../plugins/tests/fixture.wasm");

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn published(bytes: &[u8]) -> PublishedPlugin {
    let manifest = check_artifact(bytes).unwrap();
    PublishedPlugin {
        name: manifest.name.clone(),
        version: manifest.version.clone(),
        digest: ArtifactDigest::of(bytes).to_string(),
        size: bytes.len() as u64,
        manifest: serde_json::to_string(&manifest).unwrap(),
        published_by: UserId::nil(RealmId::from_bytes([1u8; 32])),
        published_at_ms: 1,
    }
}

/// Nodes 1 and 2 manage, node 3 serves, node 4 is a device; the fixture is published.
fn realm_config() -> RealmConfigDocument {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(node(1), RealmNodeKind::Management);
    config.ensure_node(node(2), RealmNodeKind::Management);
    config.ensure_node(node(3), RealmNodeKind::Server);
    let owner = UserId::nil(realm_id);
    config.ensure_node(node(4), RealmNodeKind::User { owner });
    config.plugins.push(published(FIXTURE));
    config
}

fn context() -> (TempDir, DriverContext) {
    let dir = tempdir().unwrap();
    let ctx = DriverContext {
        storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    (dir, ctx)
}

#[test]
fn store_peers() {
    let config = realm_config();
    assert_eq!(check_store(&config, node(1), node(2)), Ok(()));
    assert_eq!(
        check_store(&config, node(1), node(3)),
        Err(MetadataReadError::Forbidden)
    );
    assert_eq!(
        check_store(&config, node(1), node(4)),
        Err(MetadataReadError::Forbidden)
    );
    assert_eq!(
        check_store(&config, node(3), node(1)),
        Err(MetadataReadError::Unavailable)
    );
}

#[test]
fn fetch_peers() {
    let config = realm_config();
    let digest = ArtifactDigest::of(FIXTURE).to_string();
    assert_eq!(check_fetch(&config, node(1), node(3), &digest), Ok(()));
    assert_eq!(check_fetch(&config, node(1), node(2), &digest), Ok(()));
    let device = check_fetch(&config, node(1), node(4), &digest);
    assert_eq!(device, Err(MetadataReadError::Forbidden));
    let unknown = check_fetch(&config, node(1), node(9), &digest);
    assert_eq!(unknown, Err(MetadataReadError::Forbidden));
    let unpublished = format!("sha256:{}", "ab".repeat(32));
    let unpublished = check_fetch(&config, node(1), node(3), &unpublished);
    assert_eq!(unpublished, Err(MetadataReadError::NotFound));
    let server = check_fetch(&config, node(3), node(1), &digest);
    assert_eq!(server, Err(MetadataReadError::Unavailable));
}

#[test]
fn exact_bytes() {
    let entry = published(FIXTURE);
    assert!(check_published(FIXTURE, &entry).is_ok());
    let mut longer = FIXTURE.to_vec();
    longer.push(0);
    assert!(matches!(
        check_published(&longer, &entry),
        Err(ArtifactError::Mismatch)
    ));
    let mut other = entry.clone();
    other.manifest = other.manifest.replace("fixture", "fixturf");
    assert!(matches!(
        check_published(FIXTURE, &other),
        Err(ArtifactError::Mismatch)
    ));
    let mut sized = entry;
    sized.size += 1;
    assert!(matches!(
        check_published(FIXTURE, &sized),
        Err(ArtifactError::Mismatch)
    ));
}

#[tokio::test]
async fn fetch_checks_copy() {
    let (_dir, ctx) = context();
    let config = realm_config();
    let digest = ArtifactDigest::of(FIXTURE).to_string();
    let missing = fetch_artifact(&ctx, &config, &digest).await;
    assert!(
        matches!(missing, Err(ArtifactError::Unavailable(_))),
        "{missing:?}"
    );
    store_local(&ctx, &digest, b"tampered").await.unwrap();
    let tampered = fetch_artifact(&ctx, &config, &digest).await;
    assert!(
        matches!(tampered, Err(ArtifactError::Unavailable(_))),
        "a bad local copy is not used"
    );
    store_local(&ctx, &digest, FIXTURE).await.unwrap();
    let fetched = fetch_artifact(&ctx, &config, &digest).await.unwrap();
    assert_eq!(fetched, FIXTURE);
    let unpublished = format!("sha256:{}", "ab".repeat(32));
    let refused = fetch_artifact(&ctx, &config, &unpublished).await;
    assert!(
        matches!(refused, Err(ArtifactError::NotPublished)),
        "{refused:?}"
    );
}
