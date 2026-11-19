//! Tests who may store and fetch artifact copies, and that only exact published bytes are used.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::plugins::calls::*;
use crate::plugins::runs::tests::opened;
use aruna_core::UserId;
use aruna_core::structs::identity::realm::RealmId;

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
    let (_dir, ctx) = opened();
    let config = realm_config();
    let digest = ArtifactDigest::of(FIXTURE).to_string();
    let missing = fetch_artifact(&ctx, &config, &digest).await;
    assert!(
        matches!(missing, Err(ArtifactError::Unavailable(_))),
        "{missing:?}"
    );
    store_local(&ctx, &config, &digest, b"tampered", 1)
        .await
        .unwrap();
    let tampered = fetch_artifact(&ctx, &config, &digest).await;
    assert!(
        matches!(tampered, Err(ArtifactError::Unavailable(_))),
        "a bad local copy is not used"
    );
    store_local(&ctx, &config, &digest, FIXTURE, 1)
        .await
        .unwrap();
    let fetched = fetch_artifact(&ctx, &config, &digest).await.unwrap();
    assert_eq!(fetched.bytes, FIXTURE);
    let unpublished = format!("sha256:{}", "ab".repeat(32));
    let refused = fetch_artifact(&ctx, &config, &unpublished).await;
    assert!(
        matches!(refused, Err(ArtifactError::NotPublished)),
        "{refused:?}"
    );
}

mod operations {
    use super::*;
    use crate::plugins::copy_rows::{chunk_count, chunk_of};
    use crate::plugins::fixtures::{config_result, is_child, is_effect, peer_says, peer_silent};
    use crate::plugins::rows::tests::child_result;

    fn digest() -> String {
        ArtifactDigest::of(FIXTURE).to_string()
    }

    fn serving(seed: u8) -> ServingNode {
        (RealmId::from_bytes([1u8; 32]), node(seed))
    }

    fn fetch(local: Option<NodeId>) -> FetchArtifact {
        FetchArtifact::new(&realm_config(), &digest(), local).unwrap()
    }

    fn no_local() -> IoResult {
        child_result(Ok::<Option<Assembled>, ArtifactError>(None))
    }

    fn chunk_from(peer: NodeId, index: u32) -> IoResult {
        let result = Ok(chunk_of(&digest(), FIXTURE, index));
        peer_says(peer, &MetadataTransportMessage::FetchedArtifact { result })
    }

    #[test]
    fn fetch_keeps_local() {
        let unknown = FetchArtifact::new(&realm_config(), "sha256:00", None);
        assert_eq!(unknown.unwrap_err(), ArtifactError::NotPublished);
        let mut read = fetch(None);
        assert!(is_child(&read.first()));
        let memory = reserve(FIXTURE.len()).unwrap();
        let copy = Assembled::new(FIXTURE.to_vec(), memory);
        let local = child_result(Ok::<_, ArtifactError>(Some(copy)));
        let Ok(Next::Done(found)) = read.next(local) else {
            panic!("a checked local copy is the answer");
        };
        assert!(!found.fresh);
    }

    #[test]
    fn fetch_assembles_chunks() {
        let mut read = fetch(Some(node(1)));
        read.first().unwrap();
        assert!(is_effect(&read.next(no_local())));
        let count = chunk_count(FIXTURE.len() as u64);
        for index in 0..count - 1 {
            assert!(is_effect(&read.next(chunk_from(node(2), index))));
        }
        let Ok(Next::Done(fetched)) = read.next(chunk_from(node(2), count - 1)) else {
            panic!("the last chunk completes the copy");
        };
        assert!(fetched.fresh);
        assert_eq!(fetched.copy.bytes, FIXTURE);
    }

    #[test]
    fn fetch_tries_peers() {
        let mut read = fetch(None);
        read.first().unwrap();
        assert!(is_effect(&read.next(no_local())));
        let wrong = u32::MAX;
        assert!(is_effect(&read.next(chunk_from(node(1), wrong))));
        let answer = read.next(peer_silent(node(2)));
        assert!(matches!(answer, Err(ArtifactError::Unavailable(_))));
    }

    fn stored_chunk() -> Chunk {
        chunk_of(&digest(), FIXTURE, 0)
    }

    #[test]
    fn store_checks_peer() {
        let mut stranger = ServeStore::new(serving(1), node(3), stored_chunk(), 5);
        assert_eq!(stranger.hold_scope(), None);
        assert!(is_child(&stranger.first()));
        let refused = stranger.next(config_result(realm_config()));
        assert_eq!(refused, Err(MetadataReadError::Forbidden));
    }

    #[test]
    fn store_keeps_chunk() {
        let mut store = ServeStore::new(serving(1), node(2), stored_chunk(), 5);
        store.first().unwrap();
        assert!(is_child(&store.next(config_result(realm_config()))));
        let kept = child_result(Ok::<bool, ArtifactError>(true));
        assert_eq!(store.next(kept), Ok(Next::Done(true)));
        let mut wrong = ServeStore::new(serving(1), node(2), stored_chunk(), 5);
        wrong.first().unwrap();
        wrong.next(config_result(realm_config())).unwrap();
        let bad = child_result(Err::<bool, _>(ArtifactError::Mismatch));
        assert_eq!(wrong.next(bad), Err(MetadataReadError::Forbidden));
    }

    #[test]
    fn serve_reads_chunk() {
        let mut serve = ServeFetch::new(serving(1), node(3), digest(), 0);
        assert!(is_child(&serve.first()));
        assert!(is_child(&serve.next(config_result(realm_config()))));
        let found = Ok::<_, ArtifactError>(Some((3u64, vec![1u8, 2, 3])));
        let Ok(Next::Done(chunk)) = serve.next(child_result(found)) else {
            panic!("a kept chunk is the answer");
        };
        assert_eq!((chunk.index, chunk.total), (0, 3));
        let mut absent = ServeFetch::new(serving(1), node(3), digest(), 0);
        absent.first().unwrap();
        absent.next(config_result(realm_config())).unwrap();
        let none = child_result(Ok::<Option<(u64, Vec<u8>)>, ArtifactError>(None));
        assert_eq!(absent.next(none), Err(MetadataReadError::NotFound));
    }

    #[test]
    fn repair_lists_missing() {
        let mut repair = RepairCopies::new(serving(1), 5);
        assert!(is_child(&repair.first()));
        assert!(is_child(&repair.next(config_result(realm_config()))));
        let swept = child_result(Ok::<usize, ArtifactError>(1));
        assert!(is_child(&repair.next(swept)));
        let absent = child_result(Ok::<Option<u64>, ArtifactError>(None));
        let Ok(Next::Done(plan)) = repair.next(absent) else {
            panic!("the last lookup ends the plan");
        };
        assert_eq!(plan.missing, [digest()]);
    }

    #[test]
    fn repair_serves_sweep() {
        let mut repair = RepairCopies::new(serving(3), 5);
        repair.first().unwrap();
        repair.next(config_result(realm_config())).unwrap();
        let swept = child_result(Ok::<usize, ArtifactError>(0));
        let Ok(Next::Done(plan)) = repair.next(swept) else {
            panic!("a server only sweeps");
        };
        assert!(plan.missing.is_empty());
    }
}
