use aruna_net::MaybeSignedAddr;
use aruna_net::{Kademlia, actor_handle::NetworkActorHandle};
use ed25519_dalek::{Signer, SigningKey};
use iroh::{NodeAddr, NodeId};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;
mod error;

#[derive(Clone)]
pub struct Realm {
    key: Arc<SigningKey>,
    network: NetworkActorHandle,
    kademlia: Kademlia,
    store: Arc<RealmStore>,
}

pub struct RealmStore {
    // Map NodeId to NodeAddr to allow for address changes
    realm_nodes: RwLock<HashMap<NodeId, NodeAddr>>,
}

impl RealmStore {
    pub fn new() -> Self {
        Self {
            realm_nodes: RwLock::new(HashMap::new()),
        }
    }

    pub fn add_realm_node(&self, id: NodeId, addr: NodeAddr) {
        let mut realm_nodes = self.realm_nodes.write();
        realm_nodes.insert(id, addr);
    }

    pub fn get_realm_node_ids(&self) -> Vec<NodeId> {
        let realm_nodes = self.realm_nodes.read();
        realm_nodes.keys().cloned().collect()
    }

    pub fn get_realm_addrs(&self) -> Vec<NodeAddr> {
        let realm_nodes = self.realm_nodes.read();
        realm_nodes.values().cloned().collect()
    }

    pub fn get_node_addr(&self, id: &NodeId) -> Option<NodeAddr> {
        let realm_nodes = self.realm_nodes.read();
        realm_nodes.get(id).cloned()
    }

    pub fn update(&self, nodes: HashMap<NodeId, NodeAddr>) {
        let mut realm_nodes = self.realm_nodes.write();
        for (id, addr) in nodes {
            realm_nodes.insert(id, addr);
        }
    }
}

impl Realm {
    pub async fn new(
        key: SigningKey,
        network: NetworkActorHandle,
    ) -> Result<Self, error::RealmError> {
        let key = Arc::new(key);
        let store = Arc::new(RealmStore::new());
        let kademlia = network.get_kademlia_actor_handle().await.map_err(|_| {
            error::RealmError::InitError("Failed to get Kademlia actor handle".to_string())
        })?;

        Ok(Self {
            network,
            kademlia,
            store,
            key,
        })
    }

    // Sign a node address with the realm's private key
    fn sign_address(&self, addr: &NodeAddr) -> Result<Vec<u8>, error::RealmError> {
        Ok(self
            .key
            .sign(&postcard::to_allocvec(addr)?)
            .to_bytes()
            .to_vec())
    }

    // Verify a signature using the realm's public key
    fn verify_address_signature(&self, addr: &NodeAddr, signature: &[u8]) -> bool {
        let addr_bytes = match postcard::to_allocvec(addr) {
            Ok(bytes) => bytes,
            Err(_) => return false,
        };

        if signature.len() != 64 {
            return false;
        }

        let sig_bytes: [u8; 64] = match signature.try_into() {
            Ok(array) => array,
            Err(_) => return false,
        };

        let signature = ed25519_dalek::Signature::from_bytes(&sig_bytes);

        self.key.verify(&addr_bytes, &signature).is_ok()
    }

    // Get our own node address from the network
    async fn get_own_address(&self) -> Result<NodeAddr, error::RealmError> {
        self.network.get_node_addr().await.map_err(|e| {
            error::RealmError::NetworkError(format!("Failed to get own address: {}", e))
        })
    }

    // Get our own node ID
    async fn get_own_node_id(&self) -> Result<NodeId, error::RealmError> {
        let own_addr = self.get_own_address().await?;
        Ok(own_addr.node_id)
    }

    // Store our address in the Kademlia DHT
    async fn refresh_own_address(&self) -> Result<(), error::RealmError> {
        let own_addr = self.get_own_address().await?;
        let signature = self.sign_address(&own_addr)?;

        // Convert public key to bytes for use as the DHT key
        let key = self.key.verifying_key().as_bytes().clone();

        // Store in Kademlia
        self.kademlia
            .store(key, own_addr.clone(), Some(signature))
            .await
            .map_err(|e| {
                error::RealmError::KademliaError(format!(
                    "Failed to store address in Kademlia: {}",
                    e
                ))
            })?;

        // Add to local store
        self.store.add_realm_node(own_addr.node_id, own_addr);

        Ok(())
    }

    // Lookup realm members from Kademlia
    async fn get_latest_members(&self) -> Result<HashMap<NodeId, NodeAddr>, error::RealmError> {
        let key = self.key.verifying_key().as_bytes().clone();

        let find_result = self.kademlia.find(key, false).await.map_err(|e| {
            error::RealmError::KademliaError(format!("Failed to find realm members: {}", e))
        })?;

        let mut members = HashMap::new();

        for MaybeSignedAddr { addr, signature } in find_result.value {
            // Verify the signature before adding the address

            let Some(signature) = signature else {
                warn!("No signature found for address: {:?}", addr);
                continue;
            };

            if self.verify_address_signature(&addr, &signature) {
                members.insert(addr.node_id, addr);
            } else {
                warn!("Invalid signature for address: {:?}", addr);
            }
        }

        Ok(members)
    }

    // Check if our node is in the list of realm members
    async fn is_own_node_in_realm(
        &self,
        members: &HashMap<NodeId, NodeAddr>,
    ) -> Result<bool, error::RealmError> {
        let own_id = self.get_own_node_id().await?;
        Ok(members.contains_key(&own_id))
    }

    // The main realm update loop
    pub async fn realm_updater(realm: Realm) {
        let update_interval = Duration::from_secs(10 * 60); // 10 minutes

        tokio::spawn(async move {
            loop {
                // Lookup realm members
                match realm.get_latest_members().await {
                    Ok(members) => {
                        // Check if our node is in the list
                        match realm.is_own_node_in_realm(&members).await {
                            Ok(in_realm) => {
                                if !in_realm {
                                    // Our address is not in the realm, refresh it
                                    if let Err(e) = realm.refresh_own_address().await {
                                        eprintln!("Failed to refresh own address: {}", e);
                                    }
                                }
                            }
                            Err(e) => eprintln!("Failed to check own node: {}", e),
                        }

                        realm.store.update(members);
                    }
                    Err(e) => eprintln!("Failed to lookup realm members: {}", e),
                }

                // Wait for the next update
                tokio::time::sleep(update_interval).await;
            }
        });
    }

    // Get all realm member IDs
    pub fn get_realm_member_ids(&self) -> Vec<NodeId> {
        self.store.get_realm_node_ids()
    }

    // Get all realm member addresses
    pub fn get_realm_member_addrs(&self) -> Vec<NodeAddr> {
        self.store.get_realm_addrs()
    }

    // Get address for a specific node ID
    pub fn get_node_addr(&self, id: &NodeId) -> Option<NodeAddr> {
        self.store.get_node_addr(id)
    }
}
