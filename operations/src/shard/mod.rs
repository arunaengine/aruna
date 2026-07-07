use aruna_core::document::{ShardManifest, ShardManifestEntry, shard_topic_id};
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::SHARD_MANIFEST_KEYSPACE;
use aruna_core::storage_entries::shard_manifest_prefix;
use aruna_core::structs::{PlacementRef, RealmId};
use aruna_core::types::Key;
use aruna_core::util::unix_timestamp_millis;
use aruna_net::NetHandle;
use byteview::ByteView;
use irokle::Storage as _;

use crate::driver::DriverContext;

const SHARD_MANIFEST_SCAN_PAGE: usize = 512;

/// Builds the local node's [`ShardManifest`] for one shard on demand: a prefix
/// scan of the manifest keyspace for the entry set, plus the shard topic's
/// irokle `sync_fingerprint` (digest) and persisted `ActorClock` (cursor). Never
/// persisted — a new holder fetches a co-holder's over the shard ALPN and
/// compares digests. A digest match against a co-holder means convergence.
pub async fn assemble_shard_manifest(
    context: &DriverContext,
    realm_id: RealmId,
    placement: PlacementRef,
) -> Result<ShardManifest, String> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or_else(|| "cannot assemble shard manifest without a net handle".to_string())?;
    let entries = scan_shard_manifest_entries(context, &placement).await?;
    let topic = shard_topic_id(realm_id, &placement);
    let (digest, cursor) = topic_digest_and_cursor(net_handle, topic);
    Ok(ShardManifest {
        placement,
        holder: net_handle.node_id(),
        entries,
        cursor,
        digest,
        updated_at_ms: unix_timestamp_millis(),
    })
}

async fn scan_shard_manifest_entries(
    context: &DriverContext,
    placement: &PlacementRef,
) -> Result<Vec<ShardManifestEntry>, String> {
    let prefix = ByteView::from(shard_manifest_prefix(placement));
    let mut entries = Vec::new();
    let mut start_after: Option<Key> = None;
    loop {
        let batch = match context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: SHARD_MANIFEST_KEYSPACE.to_string(),
                prefix: Some(prefix.clone()),
                start: start_after.take().map(IterStart::After),
                limit: SHARD_MANIFEST_SCAN_PAGE,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                start_after = next_start_after;
                values
            }
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(format!("failed to scan shard manifest: {error}"));
            }
            other => {
                return Err(format!("unexpected shard manifest iter result: {other:?}"));
            }
        };
        for (_, value) in &batch {
            let entry: ShardManifestEntry = postcard::from_bytes(value)
                .map_err(|error| format!("failed to decode shard manifest entry: {error}"))?;
            entries.push(entry);
        }
        if start_after.is_none() {
            break;
        }
    }
    Ok(entries)
}

// Digest and cursor come straight from irokle: a topic with no local genesis
// yields the zero digest and an empty cursor, which a non-holder compares as
// "not converged" rather than erroring.
fn topic_digest_and_cursor(net_handle: &NetHandle, topic: irokle::TopicId) -> ([u8; 32], Vec<u8>) {
    let node = net_handle.document_sync_node();
    let digest = node
        .sync_fingerprint(topic)
        .map(|fingerprint| fingerprint.fingerprint)
        .unwrap_or([0u8; 32]);
    let cursor = node
        .storage()
        .actor_clock(&topic)
        .ok()
        .and_then(|clock| postcard::to_allocvec(&clock).ok())
        .unwrap_or_default();
    (digest, cursor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::document::{DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncRevision};
    use aruna_core::storage_entries::shard_manifest_write_entry;
    use aruna_net::{DiscoveryMethod, NetConfig, RelayMethod};
    use aruna_storage::FjallStorage;
    use std::sync::Arc;
    use tempfile::TempDir;
    use ulid::Ulid;

    fn placement(shard: u32) -> PlacementRef {
        PlacementRef {
            strategy_id: Ulid::from_bytes([9; 16]),
            epoch: 0,
            shard,
        }
    }

    fn lifecycle_change(placement: PlacementRef, seed: u8) -> DocumentSyncChange {
        DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: u64::from(seed),
                event_id: Ulid::from_bytes([seed; 16]),
                actor: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
                updated_at_ms: u64::from(seed),
            },
            kind: DocumentSyncChangeKind::Upsert,
            placement,
        }
    }

    async fn write_manifest_row(storage: &aruna_storage::StorageHandle, shard: u32, doc: u8) {
        let target = aruna_core::document::DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: Ulid::from_bytes([doc; 16]),
        };
        let (key_space, key, value) =
            shard_manifest_write_entry(&target, &lifecycle_change(placement(shard), doc))
                .unwrap()
                .unwrap();
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected manifest write event: {other:?}"),
        }
    }

    async fn spawn_context() -> (TempDir, Arc<DriverContext>) {
        let dir = tempfile::tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
                realm_id: RealmId::from_bytes([5u8; 32]),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .expect("net handle");
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: Some(net),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        (dir, context)
    }

    #[tokio::test]
    async fn scan_isolates_entries_by_shard() {
        let (_dir, context) = spawn_context().await;
        write_manifest_row(&context.storage_handle, 3, 1).await;
        write_manifest_row(&context.storage_handle, 3, 2).await;
        write_manifest_row(&context.storage_handle, 4, 3).await;

        let shard3 = scan_shard_manifest_entries(&context, &placement(3))
            .await
            .expect("scan shard 3");
        let shard4 = scan_shard_manifest_entries(&context, &placement(4))
            .await
            .expect("scan shard 4");

        assert_eq!(shard3.len(), 2);
        assert_eq!(shard4.len(), 1);
        assert!(shard3.iter().all(|entry| entry.target
            != aruna_core::document::DocumentSyncTarget::MetadataDocumentLifecycle {
                document_id: Ulid::from_bytes([3; 16])
            }));
    }

    #[tokio::test]
    async fn assembled_digest_and_cursor_track_the_live_shard_topic() {
        let (_dir, context) = spawn_context().await;
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let placement = placement(7);
        write_manifest_row(&context.storage_handle, 7, 1).await;

        // Ensure the shard topic genesis so the topic has a well-defined
        // fingerprint and clock to read back.
        let net = context.net_handle.as_ref().unwrap();
        let topic = shard_topic_id(realm_id, &placement);
        net.ensure_document_sync_topics(&[topic], Vec::new())
            .expect("ensure topic");

        let manifest = assemble_shard_manifest(&context, realm_id, placement)
            .await
            .expect("assemble manifest");

        assert_eq!(manifest.placement, placement);
        assert_eq!(manifest.holder, net.node_id());
        assert_eq!(manifest.entries.len(), 1);
        // The digest and cursor are read straight from the live shard topic.
        let expected_digest = net
            .document_sync_node()
            .sync_fingerprint(topic)
            .expect("fingerprint")
            .fingerprint;
        assert_eq!(manifest.digest, expected_digest);
        assert!(postcard::from_bytes::<irokle::ActorClock>(&manifest.cursor).is_ok());
    }
}
