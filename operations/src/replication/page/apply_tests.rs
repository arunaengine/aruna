//! Tests that a member applies a reference page in one transaction, admits it like a single
//! publication, replays it without counting twice and leaves replacements to single delivery.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, ROOT_RECORD_KEYSPACE, S3_BUCKET_KEYSPACE,
    USAGE_STATS_KEYSPACE,
};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::{
    PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BucketInfo, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::distribution::{
    DistributionSnapshot, OriginalEpoch, Publication,
};
use aruna_core::structs::storage::replication::ReplicationItemKind;
use aruna_core::structs::storage::usage::{BucketCounters, usage_bucket_key};
use aruna_storage::StorageHandle;
use tempfile::TempDir;
use ulid::Ulid;

use super::*;
use crate::distribution::test_record::{binding, member, realm, replicated, secret, store_record};
use crate::distribution::test_store::{context, install, open};
use crate::driver::drive;
use crate::replication::protocol::VersionReplicationManifest;

struct Member {
    _dir: TempDir,
    handle: StorageHandle,
    snapshot: DistributionSnapshot,
    publication: Publication,
}

/// Member 2's bucket of the two-shard set, holding a copy of member 1's lower range.
async fn member_two(policies: bool) -> Member {
    let (dir, handle) = open();
    let (snapshot, bytes) = replicated(true, 1);
    store_record(&handle, snapshot.set_id(), bytes).await;
    install(&handle, snapshot.set_id(), &snapshot.config.ranges).await;
    let mut bucket = BucketInfo {
        group_id: snapshot.config.owner_group,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Default::default(),
        distribution: Some(binding(&snapshot, 2, 5)),
    };
    if policies {
        bucket.placement_policies =
            vec![aruna_core::structs::placement::policy::PlacementPolicyRef {
                policy_id: Ulid::from_bytes([6; 16]),
                digest: [6; 32],
            }];
    }
    let write = StorageEffect::Write {
        key_space: S3_BUCKET_KEYSPACE.to_string(),
        key: b"spire".to_vec().into(),
        value: bucket.to_bytes().unwrap().into(),
        txn_id: None,
    };
    handle.send_storage_effect(write).await;
    let range = &snapshot.config.ranges[0];
    let (_, deliveries) = snapshot.key_deliveries(member(1), "a").unwrap();
    let publication = snapshot.publication(range, &deliveries[0]);
    Member {
        _dir: dir,
        handle,
        snapshot,
        publication,
    }
}

/// An external HTTP source of `key`, the kind whose root a member stores by digest.
pub(crate) fn source(key: &str) -> VersionSourceBinding {
    VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::Http,
            public_config: HashMap::from([(
                "endpoint".to_string(),
                "https://example.org".to_string(),
            )]),
            source_path: key.to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
            namespace: None,
            root: None,
        },
        connector_id: None,
    }
}

/// A reference record of `key` from an HTTP source, current at `generation`.
pub(super) fn record(
    key: &str,
    version: u8,
    generation: u64,
    publication: &Publication,
) -> VersionReplicationManifest {
    let source = source(key);
    VersionReplicationManifest {
        bucket: "spire".to_string(),
        key: key.to_string(),
        version_id: Ulid::from_parts(1_700_000_000_000, u128::from(version)),
        group_id: Ulid::from_bytes([90; 16]),
        kind: ReplicationItemKind::Materialized,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        current_version: true,
        current_version_generation: Some(generation),
        auth_context: AuthContext::anonymous(realm()),
        blob: None,
        source: Some(source),
        multipart: None,
        reference_intent: true,
        origin: None,
        upstream_sources: Vec::new(),
        writer_auth_context: None,
        reference_metadata: Some(SourceMetadata {
            content_length: 100,
            content_type: None,
            etag: Some(format!("etag-{key}")),
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        }),
        metadata: HashMap::new(),
        reference_advance: None,
        reference_advance_count: Some(0),
        placement_policies: Vec::new(),
        original_epoch: OriginalEpoch(publication.epoch.0),
        version_generation: generation,
        logical_identity: None,
        distribution: Some(publication.clone()),
        version_origin: None,
        version_hop: None,
        sync_source: None,
    }
}

fn page(member: &Member, manifests: Vec<VersionReplicationManifest>) -> ReferencePage {
    ReferencePage {
        bucket: "spire".to_string(),
        publication: member.publication.clone(),
        manifests,
    }
}

async fn apply(member: &Member, page: ReferencePage, sender: u8) -> PageAnswer {
    let input = ApplyInput {
        page,
        publisher: secret(sender).public(),
        realm_id: realm(),
    };
    page_answer(drive(PageApplyOperation::new(input), &context(&member.handle)).await)
}

async fn read(handle: &StorageHandle, key_space: &str, key: Vec<u8>) -> Option<Vec<u8>> {
    let read = StorageEffect::Read {
        key_space: key_space.to_string(),
        key: key.into(),
        txn_id: None,
    };
    match handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value.map(|v| v.to_vec()),
        other => panic!("unexpected read answer {other:?}"),
    }
}

async fn counters(handle: &StorageHandle) -> BucketCounters {
    let value = read(handle, USAGE_STATS_KEYSPACE, usage_bucket_key("spire")).await;
    value.map_or_else(BucketCounters::default, |value| {
        BucketCounters::from_bytes(&value).unwrap()
    })
}

async fn head(handle: &StorageHandle, key: &str) -> Option<CurrentVersionPointer> {
    let key = BlobHeadKey::new("spire", key).to_bytes().unwrap();
    let value = read(handle, BLOB_HEAD_KEYSPACE, key).await?;
    Some(CurrentVersionPointer::from_bytes(&value).unwrap())
}

#[tokio::test]
async fn applies_page_once() {
    let member = member_two(false).await;
    let manifests: Vec<_> = (["a", "b", "c"].iter().enumerate())
        .map(|(index, key)| record(key, index as u8 + 1, 1, &member.publication))
        .collect();
    let answer = apply(&member, page(&member, manifests.clone()), 1).await;
    let applied = PageAnswer::Applied {
        applied: 3,
        single: Vec::new(),
    };
    assert_eq!(answer, applied);
    for manifest in &manifests {
        let pointer = head(&member.handle, &manifest.key).await.unwrap();
        assert_eq!(pointer.version_id, manifest.version_id);
        let key = VersionKey::new("spire", &manifest.key, manifest.version_id);
        let stored = read(
            &member.handle,
            BLOB_VERSIONS_KEYSPACE,
            key.to_bytes().unwrap(),
        )
        .await;
        assert!(stored.is_some(), "every record is stored");
    }
    let counted = counters(&member.handle).await;
    assert_eq!((counted.objects, counted.versions), (3, 3));
    assert_eq!(counted.referenced_bytes, 300);
    let root = root_digest(&manifests[0]);
    assert!(
        read(&member.handle, ROOT_RECORD_KEYSPACE, root)
            .await
            .is_some(),
        "the shared source root is stored once by digest"
    );
    // A replay after a lost answer is acknowledged again without counting twice.
    let answer = apply(&member, page(&member, manifests), 1).await;
    assert_eq!(answer, applied);
    assert_eq!(counters(&member.handle).await, counted);
}

/// The digest of a record's source root, as the member stores it.
fn root_digest(manifest: &VersionReplicationManifest) -> Vec<u8> {
    use aruna_core::structs::storage::distribution::source_root::RootEntry;
    let source = manifest.source.as_ref().unwrap();
    RootEntry::of(source)
        .unwrap()
        .root
        .digest()
        .unwrap()
        .to_vec()
}

#[tokio::test]
async fn refuses_other_sender() {
    let member = member_two(false).await;
    let manifests = vec![record("a", 1, 1, &member.publication)];
    let answer = apply(&member, page(&member, manifests), 3).await;
    assert!(matches!(answer, PageAnswer::Refused { retry: false, .. }));
    assert!(
        head(&member.handle, "a").await.is_none(),
        "nothing is written"
    );
    assert_eq!(counters(&member.handle).await, BucketCounters::default());
}

#[tokio::test]
async fn missing_record_retries() {
    // Before the record arrives the same page passes later.
    let member = member_two(false).await;
    let delete = StorageEffect::Delete {
        key_space: aruna_core::keyspaces::DISTRIBUTION_KEYSPACE.to_string(),
        key: member.snapshot.set_id().to_bytes().to_vec().into(),
        txn_id: None,
    };
    member.handle.send_storage_effect(delete).await;
    let manifests = vec![record("a", 1, 1, &member.publication)];
    let answer = apply(&member, page(&member, manifests), 1).await;
    assert!(matches!(answer, PageAnswer::Refused { retry: true, .. }));
}

#[tokio::test]
async fn newer_head_kept() {
    // An older record is stored without taking the head of a newer one.
    let member = member_two(false).await;
    let newer = record("a", 2, 5, &member.publication);
    let older = record("a", 1, 3, &member.publication);
    apply(&member, page(&member, vec![newer.clone()]), 1).await;
    let answer = apply(&member, page(&member, vec![older]), 1).await;
    assert!(matches!(answer, PageAnswer::Applied { applied: 1, .. }));
    let pointer = head(&member.handle, "a").await.unwrap();
    assert_eq!(pointer.version_id, newer.version_id);
    let counted = counters(&member.handle).await;
    assert_eq!((counted.objects, counted.versions), (1, 2));
}

#[tokio::test]
async fn newer_record_replaces() {
    let member = member_two(false).await;
    let older = record("a", 1, 3, &member.publication);
    let newer = record("a", 2, 4, &member.publication);
    apply(&member, page(&member, vec![older]), 1).await;
    apply(&member, page(&member, vec![newer.clone()]), 1).await;
    let pointer = head(&member.handle, "a").await.unwrap();
    assert_eq!(pointer.version_id, newer.version_id);
    let counted = counters(&member.handle).await;
    assert_eq!((counted.objects, counted.versions), (1, 2), "one live key");
}

#[tokio::test]
async fn invalid_page_refused() {
    let member = member_two(false).await;
    let twice = vec![
        record("a", 1, 1, &member.publication),
        record("a", 2, 2, &member.publication),
    ];
    let answer = apply(&member, page(&member, twice), 1).await;
    assert!(matches!(answer, PageAnswer::Refused { retry: false, .. }));
    let mut foreign = record("b", 3, 1, &member.publication);
    foreign.writer_auth_context = Some(AuthContext::anonymous(realm()));
    let answer = apply(&member, page(&member, vec![foreign]), 1).await;
    assert!(matches!(answer, PageAnswer::Refused { retry: false, .. }));
}

#[tokio::test]
async fn governed_bucket_single() {
    // A bucket with placement rules leaves its records to the single path and its gate.
    let member = member_two(true).await;
    let manifests = vec![record("a", 1, 1, &member.publication)];
    let ids = vec![manifests[0].version_id];
    let answer = apply(&member, page(&member, manifests), 1).await;
    assert_eq!(
        answer,
        PageAnswer::Applied {
            applied: 0,
            single: ids
        }
    );
    assert!(head(&member.handle, "a").await.is_none());
}

#[tokio::test]
async fn changed_record_single() {
    // A stored row of the same id with other content is a replacement for the single path.
    let member = member_two(false).await;
    let stored = record("a", 1, 1, &member.publication);
    apply(&member, page(&member, vec![stored.clone()]), 1).await;
    let mut changed = stored;
    changed.reference_metadata.as_mut().unwrap().content_length = 7;
    let answer = apply(&member, page(&member, vec![changed.clone()]), 1).await;
    assert_eq!(
        answer,
        PageAnswer::Applied {
            applied: 0,
            single: vec![changed.version_id]
        }
    );
}
