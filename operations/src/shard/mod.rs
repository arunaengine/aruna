//! Builds the local node's shard manifest from a prefix scan plus the topic digest and cursor.
//! It also groups the shard client, inbound server and verification modules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod client;
pub mod incoming;
pub(crate) mod manifest;
pub mod protocol;
pub(crate) mod pull;
pub mod recovery;
pub mod restore;
pub mod restore_plan;
pub mod verify;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{ShardManifest, ShardManifestEntry, shard_topic_id};
use aruna_core::effects::{Effect, NetEffect, ShardNetEffect};
use aruna_core::events::{Event, NetEvent, ShardNetEvent};
use aruna_core::operation::Operation;
use aruna_core::rows::event;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use thiserror::Error;

use manifest::{ManifestScanError, ManifestScanOperation};

/// Why a shard manifest could not be built or fetched.
#[derive(Clone, Debug, Error, PartialEq)]
#[error("{0}")]
pub struct ManifestError(pub String);

impl From<IoError> for ManifestError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

pub(crate) fn shard_call(effect: ShardNetEffect) -> IoCall {
    let effect = Effect::Net(NetEffect::Shard(Box::new(effect)));
    IoCall::Effect(Box::new(effect))
}

pub(crate) fn shard_answer(result: IoResult) -> Result<ShardNetEvent, IoError> {
    match event(result)? {
        Event::Net(NetEvent::Shard(answer)) => Ok(*answer),
        other => Err(IoError::Unexpected {
            expected: "a shard answer",
            got: other.label(),
        }),
    }
}

/// Builds the local node's [`ShardManifest`] for one shard on demand: a prefix
/// scan of the manifest keyspace for the entry set, plus the shard topic's
/// irokle fingerprint (digest) and persisted clock (cursor). Never persisted.
#[derive(Debug, PartialEq)]
pub struct AssembleManifestOperation {
    realm_id: RealmId,
    placement: PlacementRef,
    now_ms: u64,
    entries: Option<Vec<ShardManifestEntry>>,
}

impl AssembleManifestOperation {
    pub fn new(realm_id: RealmId, placement: PlacementRef) -> Self {
        Self {
            realm_id,
            placement,
            now_ms: 0,
            entries: None,
        }
    }
}

impl Operation for AssembleManifestOperation {
    type Output = ShardManifest;
    type Error = ManifestError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<ShardManifest>, ManifestError> {
        let scan = ManifestScanOperation::new(&self.placement);
        Ok(Next::Call(IoCall::child(scan)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ShardManifest>, ManifestError> {
        let Some(entries) = self.entries.take() else {
            let entries = result
                .child::<Vec<ShardManifestEntry>, ManifestScanError>()?
                .map_err(|error| ManifestError(error.to_string()))?;
            self.entries = Some(entries);
            let topic = shard_topic_id(self.realm_id, &self.placement);
            return Ok(Next::Call(shard_call(ShardNetEffect::TopicFacts(topic))));
        };
        match shard_answer(result)? {
            ShardNetEvent::Facts {
                holder,
                digest,
                cursor,
            } => Ok(Next::Done(ShardManifest {
                placement: self.placement,
                holder,
                entries,
                cursor,
                digest,
                updated_at_ms: self.now_ms,
            })),
            _ => Err(ManifestError(
                "cannot assemble shard manifest without a net handle".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::document::{DocumentChange, DocumentChangeKind, DocumentSyncRevision};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::storage_entries::shard_manifest_entry;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::FjallStorage;
    use std::sync::Arc;
    use tempfile::TempDir;
    use ulid::Ulid;

    fn placement(shard: u32) -> PlacementRef {
        PlacementRef {
            strategy_id: Ulid::from_bytes([9; 16]),
            shard,
        }
    }

    fn lifecycle_change(placement: PlacementRef, seed: u8) -> DocumentChange {
        DocumentChange {
            base: None,
            current: DocumentSyncRevision {
                generation: u64::from(seed),
                event_id: Ulid::from_bytes([seed; 16]),
                actor: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
                updated_at_ms: u64::from(seed),
            },
            kind: DocumentChangeKind::Upsert,
            placement,
        }
    }

    async fn write_manifest_row(storage: &aruna_storage::StorageHandle, shard: u32, doc: u8) {
        let target = aruna_core::document::DocumentTarget::MetadataDocumentLifecycle {
            document_id: Ulid::from_bytes([doc; 16]),
        };
        let (key_space, key, value) =
            shard_manifest_entry(&target, &lifecycle_change(placement(shard), doc))
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
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        });
        (dir, context)
    }

    #[tokio::test]
    async fn scan_isolates_shard() {
        let (_dir, context) = spawn_context().await;
        write_manifest_row(&context.storage_handle, 3, 1).await;
        write_manifest_row(&context.storage_handle, 3, 2).await;
        write_manifest_row(&context.storage_handle, 4, 3).await;

        let shard3 = drive(ManifestScanOperation::new(&placement(3)), &context)
            .await
            .expect("scan shard 3");
        let shard4 = drive(ManifestScanOperation::new(&placement(4)), &context)
            .await
            .expect("scan shard 4");

        assert_eq!(shard3.len(), 2);
        assert_eq!(shard4.len(), 1);
        assert!(shard3.iter().all(|entry| entry.target
            != aruna_core::document::DocumentTarget::MetadataDocumentLifecycle {
                document_id: Ulid::from_bytes([3; 16])
            }));
    }

    #[tokio::test]
    async fn digest_tracks_topic() {
        let (_dir, context) = spawn_context().await;
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let placement = placement(7);
        write_manifest_row(&context.storage_handle, 7, 1).await;

        // Ensure the shard topic genesis so the topic has a well-defined
        // fingerprint and clock to read back.
        let net = context.net_handle.as_ref().unwrap();
        let topic = shard_topic_id(realm_id, &placement);
        net.ensure_sync_topics(&[topic], Vec::new())
            .await
            .expect("ensure topic");

        let manifest = drive(
            AssembleManifestOperation::new(realm_id, placement),
            &context,
        )
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

    #[tokio::test]
    async fn frontier_root_stable() {
        let (_dir, context) = spawn_context().await;
        let net = context.net_handle.as_ref().unwrap();
        let topic = irokle::TopicId::from_bytes([8; 32]);
        let node = net.document_sync_node();
        let actor_id = irokle::actor_id_for(topic, node.peer_id());
        let oplog = irokle::oplog::Oplog::with_storage(node.storage().clone());
        oplog
            .create_topic_genesis(
                topic,
                actor_id,
                irokle::TopicGenesis {
                    event_type_id: "frontier-test".to_string(),
                    initial_peers: std::collections::BTreeSet::new(),
                    replication_policy: irokle::ReplicationPolicy::all(),
                },
                node.signer(),
            )
            .unwrap();
        oplog
            .create_event_op(
                topic,
                actor_id,
                irokle::EventEnvelope {
                    type_id: "frontier-test".to_string(),
                    payload: vec![1].into(),
                },
                node.signer(),
            )
            .unwrap();
        let mut frontier = irokle::ActorClock::default();
        frontier.observe(actor_id, 2);
        let root = net.shard_frontier_root(topic, &frontier).unwrap();
        oplog
            .create_event_op(
                topic,
                actor_id,
                irokle::EventEnvelope {
                    type_id: "frontier-test".to_string(),
                    payload: vec![2].into(),
                },
                node.signer(),
            )
            .unwrap();
        assert_eq!(root, net.shard_frontier_root(topic, &frontier).unwrap());
    }

    #[test]
    fn manifest_needs_network() {
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let mut operation = AssembleManifestOperation::new(realm_id, placement(1));
        operation.first().unwrap();
        let entries = Ok::<_, ManifestScanError>(Vec::<ShardManifestEntry>::new());
        let scanned = IoResult::Child(aruna_core::calls::ChildOutcome::new(entries));
        assert!(matches!(
            operation.next(scanned),
            Ok(Next::Call(IoCall::Effect(_)))
        ));
        let event = Event::Net(NetEvent::Shard(Box::new(ShardNetEvent::NoNetwork)));
        assert!(matches!(
            operation.next(IoResult::Event(Box::new(event))),
            Err(ManifestError(_))
        ));
    }
}
