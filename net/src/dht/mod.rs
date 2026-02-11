pub mod bootstrap;
pub mod client;
pub mod kbucket;
pub mod lookup;
pub mod rpc;
pub mod server;
pub mod storage;

use std::sync::Arc;
use std::time::Duration;

use aruna_core::events::DhtEntry;
use aruna_core::id::{DhtKeyId, NodeId};
use aruna_core::util::unix_timestamp_secs;
use aruna_storage::StorageHandle;
use iroh::Endpoint;
use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;

use self::client::DhtClient;
use self::kbucket::{InsertResult, K, PeerInfo, RoutingTable};
use self::lookup::{find_node, get_value, put_value};
use self::server::DhtServer;
use self::storage::{DhtStorage, StoredEntry};
use crate::error::Result;
use aruna_core::id::NodeIdExt;

pub struct DhtService {
    local_id: NodeId,
    secret_key: iroh::SecretKey,
    client: DhtClient,
    server: Arc<DhtServer>,
    storage: Arc<DhtStorage>,
    routing_table: Arc<RwLock<RoutingTable>>,
    #[allow(dead_code)]
    shutdown: CancellationToken,
}

impl DhtService {
    pub async fn new(
        endpoint: Endpoint,
        storage_handle: StorageHandle,
        shutdown: CancellationToken,
    ) -> Result<Self> {
        // NodeId is now iroh::PublicKey directly
        let local_id = endpoint.id();
        let secret_key = endpoint.secret_key().clone();
        let storage = Arc::new(DhtStorage::new(storage_handle));
        let routing_table = Arc::new(RwLock::new(RoutingTable::new(local_id)));

        let client = DhtClient::new(endpoint.clone());
        let server = Arc::new(DhtServer::new(
            local_id,
            routing_table.clone(),
            storage.clone(),
        ));

        // Spawn periodic cleanup task
        let storage_for_cleanup = storage.clone();
        let shutdown_for_cleanup = shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300)); // 5 minutes
            loop {
                tokio::select! {
                    _ = shutdown_for_cleanup.cancelled() => break,
                    _ = interval.tick() => {
                        storage_for_cleanup.cleanup_all().await;
                    }
                }
            }
        });

        Ok(Self {
            local_id,
            secret_key,
            client,
            server,
            storage,
            routing_table,
            shutdown,
        })
    }

    /// Bootstrap the DHT with known peers (accepts raw bytes for backward compatibility)
    pub async fn bootstrap(&self, nodes: &[[u8; 32]]) -> Result<()> {
        let node_ids: Vec<NodeId> = nodes
            .iter()
            .filter_map(|b| NodeId::from_bytes(b).ok())
            .collect();
        bootstrap::bootstrap(&self.client, self.routing_table.clone(), &node_ids).await
    }

    /// Bootstrap the DHT with NodeId peers
    pub async fn bootstrap_nodes(&self, nodes: &[NodeId]) -> Result<()> {
        bootstrap::bootstrap(&self.client, self.routing_table.clone(), nodes).await
    }

    /// Store a value in the DHT
    pub async fn put(&self, key: &DhtKeyId, value: Vec<u8>, ttl: Duration) -> Result<()> {
        let ttl_secs = ttl.as_secs();
        let expires_at = unix_timestamp_secs().saturating_add(ttl_secs);

        let mut signed_data = Vec::with_capacity(32 + value.len() + 8);
        signed_data.extend_from_slice(key.as_bytes());
        signed_data.extend_from_slice(&value);
        signed_data.extend_from_slice(&ttl_secs.to_le_bytes());
        let signature = self.secret_key.sign(&signed_data).to_bytes();

        // Store locally first
        let entry = StoredEntry {
            publisher: self.local_id,
            value: value.clone(),
            expires_at,
            signature: Some(signature),
        };
        self.storage.put(key, entry).await;

        // Find k closest nodes to the key
        let initial = self.routing_table.read().closest(key.as_bytes(), K);

        if initial.is_empty() {
            tracing::warn!(
                ?key,
                "DHT put: no peers in routing table, value stored locally only"
            );
            return Ok(());
        }

        let closest = find_node(&self.client, initial, key.as_bytes()).await;
        let attempted = closest.len().min(K);

        if attempted == 0 {
            tracing::warn!(?key, "DHT put: lookup produced no replication targets");
            return Ok(());
        }

        let stored = put_value(
            &self.client,
            closest,
            key,
            value,
            ttl_secs,
            &self.local_id,
            Some(signature),
        )
        .await;

        if stored == 0 {
            tracing::warn!(
                ?key,
                attempted,
                "DHT put stored value locally but failed remote replication"
            );
        } else if stored < attempted {
            tracing::warn!(
                ?key,
                stored,
                attempted,
                "DHT put only partially replicated value"
            );
        }

        Ok(())
    }

    /// Get values from the DHT
    pub async fn get(&self, key: &DhtKeyId) -> Result<Vec<DhtEntry>> {
        // Check local storage first
        let local_entries = self.storage.get(key).await;

        // Also query the network
        let initial = self.routing_table.read().closest(key.as_bytes(), K);

        let mut all_entries: Vec<DhtEntry> = local_entries
            .into_iter()
            .map(|e| DhtEntry {
                node_id: e.publisher,
                value: e.value,
                expires_at: e.expires_at,
            })
            .collect();

        if !initial.is_empty() {
            let (network_values, _closest) = get_value(&self.client, initial, key).await;

            for sv in network_values {
                // Deduplicate by publisher
                if !all_entries.iter().any(|e| e.node_id == sv.publisher) {
                    all_entries.push(DhtEntry {
                        node_id: sv.publisher,
                        value: sv.value,
                        expires_at: sv.expires_at,
                    });
                }
            }
        }

        Ok(all_entries)
    }

    /// Get the DHT server for handling incoming requests
    pub fn server(&self) -> Arc<DhtServer> {
        self.server.clone()
    }

    /// Get the local node ID
    pub fn local_id(&self) -> NodeId {
        self.local_id
    }

    /// Get routing table size
    pub fn routing_table_size(&self) -> usize {
        self.routing_table.read().len()
    }

    /// Insert peer with eviction handling.
    /// If bucket is full, pings oldest peer and evicts if unresponsive.
    pub async fn insert_peer(&self, node_id: NodeId) {
        let result = self.routing_table.write().insert(PeerInfo::new(node_id));

        match result {
            InsertResult::Inserted | InsertResult::Updated => {}
            InsertResult::BucketFull { oldest, pending } => {
                if let Some(oldest_peer) = oldest {
                    // Try to ping the oldest peer
                    let is_alive = self
                        .client
                        .ping(&oldest_peer.node_id)
                        .await
                        .unwrap_or(false);

                    if is_alive {
                        // Oldest peer is alive, keep it, discard pending
                    } else {
                        // Oldest peer is dead, evict and insert pending
                        let bucket_idx = self.local_id.bucket_index(&oldest_peer.node_id);
                        if bucket_idx < 256 {
                            self.routing_table.write().evict_oldest(bucket_idx);
                            self.routing_table.write().insert(pending);
                        }
                    }
                }
            }
        }
    }

    /// Add a peer to routing table (called when we learn about new peers)
    pub async fn add_peer(&self, node_id: NodeId) {
        self.insert_peer(node_id).await;
    }
}

impl std::fmt::Debug for DhtService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DhtService")
            .field("local_id", &self.local_id)
            .field("routing_table_size", &self.routing_table_size())
            .finish()
    }
}
