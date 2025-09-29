use aruna_net::Kademlia;
use aruna_net::MaybeSignedAddr;
use ed25519_dalek::{Signer, SigningKey};
use iroh::{NodeAddr, NodeId};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::trace;
use tracing::{debug, error, info, warn};

pub mod error;

#[derive(Clone)]
pub struct Realm {
    key: Arc<SigningKey>,
    kademlia: Kademlia,
    store: Arc<RealmStore>,
    node_addr: NodeAddr,
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
        node_addr: NodeAddr,
        kademlia: Kademlia,
    ) -> Result<Self, error::RealmError> {
        let key = Arc::new(key);
        let store = Arc::new(RealmStore::new());

        let realm = Self {
            kademlia,
            node_addr,
            store,
            key,
        };

        // Automatically start the updater loop
        realm.start_updater();

        Ok(realm)
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
        trace!("Verifying address signature for {:?}", addr);
        let addr_bytes = match postcard::to_allocvec(addr) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("{}", e.to_string());
                return false;
            }
        };

        if signature.len() != 64 {
            error!("Signature len does not match");
            return false;
        }

        let sig_bytes: [u8; 64] = match signature.try_into() {
            Ok(array) => array,
            Err(e) => {
                error!("{}", e.to_string());
                return false;
            }
        };

        let signature = ed25519_dalek::Signature::from_bytes(&sig_bytes);
        match self.key.verify(&addr_bytes, &signature) {
            Ok(_) => true,
            Err(e) => {
                error!("{}", e.to_string());
                false
            }
        }
    }

    // Get our own node address from the network
    fn get_own_address(&self) -> NodeAddr {
        self.node_addr.clone()
    }

    // Get our own node ID
    fn get_own_node_id(&self) -> NodeId {
        self.node_addr.node_id.clone()
    }

    // Store our address in the Kademlia DHT
    async fn refresh_own_address(&self) -> Result<(), error::RealmError> {
        let own_addr = self.get_own_address();
        let signature = self.sign_address(&own_addr)?;

        // Convert public key to bytes for use as the DHT key
        let key = self.key.verifying_key().as_bytes().clone();

        trace!("Storing own address in Kademlia: {:?}", own_addr);

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

        info!("Successfully refreshed own address in the realm");

        Ok(())
    }

    // Lookup realm members from Kademlia with signature verification
    async fn get_latest_members(&self) -> Result<HashMap<NodeId, NodeAddr>, error::RealmError> {
        let key = self.key.verifying_key().as_bytes().clone();

        let find_result = self
            .kademlia
            .find_at_closest_nodes(key)
            .await
            .map_err(|e| {
                error::RealmError::KademliaError(format!("Failed to find realm members: {}", e))
            })?;

        let mut members = HashMap::new();
        let mut invalid_signatures = 0;

        for MaybeSignedAddr { addr, signature } in find_result {
            // Verify the signature before adding the address
            let Some(signature) = signature else {
                warn!("No signature found for address: {:?}", addr);
                continue;
            };

            if self.verify_address_signature(&addr, &signature) {
                debug!("Valid realm member found: {:?}", addr);
                members.insert(addr.node_id, addr);
            } else {
                warn!("Invalid signature for address: {:?}", addr);
                invalid_signatures += 1;
            }
        }

        if invalid_signatures > 0 {
            warn!(
                "Detected {} address(es) with invalid signatures",
                invalid_signatures
            );
        }

        Ok(members)
    }

    // Check if our node is in the list of realm members
    async fn is_own_node_in_realm(
        &self,
        members: &HashMap<NodeId, NodeAddr>,
    ) -> Result<bool, error::RealmError> {
        let own_id = self.get_own_node_id();
        Ok(members.contains_key(&own_id))
    }

    // Start the realm updater loop in a background task
    pub fn start_updater(&self) {
        // Clone self for the async task
        let realm = self.clone();

        info!("Starting realm updater background task");

        // Spawn the updater task
        tokio::spawn(async move {
            let update_interval = Duration::from_secs(10 * 60); // 10 minutes

            loop {
                // Lookup realm members
                match realm.get_latest_members().await {
                    Ok(members) => {
                        let member_count = members.len();
                        debug!("Found {} realm members", member_count);

                        // Check if our node is in the list
                        match realm.is_own_node_in_realm(&members).await {
                            Ok(in_realm) => {
                                if !in_realm {
                                    debug!("Our node is not in the realm, refreshing address");
                                    // Our address is not in the realm, refresh it
                                    if let Err(e) = realm.refresh_own_address().await {
                                        error!("Failed to refresh own address: {}", e);
                                    }
                                } else {
                                    debug!("Our node is already in the realm");
                                }
                            }
                            Err(e) => error!("Failed to check if own node is in realm: {}", e),
                        }

                        realm.store.update(members);
                    }
                    Err(e) => error!("Failed to lookup realm members: {}", e),
                }

                // Wait for the next update
                debug!(
                    "Waiting for next realm update cycle in {:?}",
                    update_interval
                );
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

    // Force an immediate update of the realm membership
    pub async fn update_now(&self) -> Result<(), error::RealmError> {
        let members = self.get_latest_members().await?;

        // Check if our node is in the list
        let in_realm = self.is_own_node_in_realm(&members).await?;
        if !in_realm {
            // Our address is not in the realm, refresh it
            self.refresh_own_address().await?;
        }

        self.store.update(members);
        Ok(())
    }

    // Get the realm's public key
    pub fn realm_public_key(&self) -> ed25519_dalek::VerifyingKey {
        self.key.verifying_key()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_net::actor::NetworkActorBuilder;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use tracing::debug;
    use tracing_subscriber::EnvFilter;

    // Helper function to set up tracing for tests
    fn init_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive("aruna_net=debug".parse().unwrap())
                    .add_directive("aruna_realm=trace".parse().unwrap()),
            )
            .with_test_writer()
            .with_file(true)
            .with_line_number(true)
            .try_init();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_realm_basic_functionality() {
        init_tracing();

        // Create two network nodes
        let node1 = NetworkActorBuilder::new(None)
            .await
            .add_bind_addr_v4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 0))
            .build(vec![])
            .await
            .unwrap();

        let node1_addr = node1.get_node_addr().await.unwrap();
        debug!("Node 1 address: {:?}", node1_addr);

        let node2 = NetworkActorBuilder::new(None)
            .await
            .add_bind_addr_v4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 0))
            .build(vec![node1_addr.clone()])
            .await
            .unwrap();

        let node2_addr = node2.get_node_addr().await.unwrap();
        debug!("Node 2 address: {:?}", node2_addr);

        // Generate a keypair for the realm
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);

        // Create a realm with node1
        let realm1 = Realm::new(
            signing_key.clone(),
            node1_addr.clone(),
            node1.get_kademlia_actor_handle().await.unwrap(),
        )
        .await
        .unwrap();

        // Manually refresh node1's address in the DHT
        realm1.refresh_own_address().await.unwrap();

        // Create a realm with node2 using the same key
        let realm2 = Realm::new(
            signing_key,
            node2_addr.clone(),
            node2.get_kademlia_actor_handle().await.unwrap(),
        )
        .await
        .unwrap();

        // Manually update realm2
        realm2.update_now().await.unwrap();

        // Verify that realm2 can see realm1
        let members = realm2.get_realm_member_addrs();
        assert!(members.contains(&node1_addr));

        // Manually refresh node2's address
        realm2.refresh_own_address().await.unwrap();

        // Update realm1 to see changes
        realm1.update_now().await.unwrap();

        // Verify that realm1 can now see both nodes
        let members1 = realm1.get_realm_member_addrs();
        assert!(members1.contains(&node1_addr));
        assert!(members1.contains(&node2_addr));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_signature_verification() {
        init_tracing();

        // Create a network node
        let node = NetworkActorBuilder::new(None)
            .await
            .add_bind_addr_v4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 0))
            .build(vec![])
            .await
            .unwrap();

        let node_addr = node.get_node_addr().await.unwrap();

        // Generate a realm key
        let mut csprng = OsRng;
        let realm_key = SigningKey::generate(&mut csprng);

        // Generate a different key
        let different_key = SigningKey::generate(&mut csprng);

        // Create a legitimate realm
        let realm = Realm::new(
            realm_key.clone(),
            node_addr.clone(),
            node.get_kademlia_actor_handle().await.unwrap(),
        )
        .await
        .unwrap();

        // Sign with the correct key
        let correct_signature = realm.sign_address(&node_addr).unwrap();

        // Verify correct signature
        assert!(realm.verify_address_signature(&node_addr, &correct_signature));

        // Create a signature with a different key
        let different_signature = different_key
            .sign(&postcard::to_allocvec(&node_addr).unwrap())
            .to_bytes()
            .to_vec();

        // Verify different signature (should fail)
        assert!(!realm.verify_address_signature(&node_addr, &different_signature));

        // Create a corrupted signature
        let mut corrupted_signature = correct_signature.clone();
        corrupted_signature[0] = corrupted_signature[0].wrapping_add(1);

        // Verify corrupted signature (should fail)
        assert!(!realm.verify_address_signature(&node_addr, &corrupted_signature));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multiple_realm_nodes() {
        init_tracing();

        // Create three network nodes in a line topology: 1 <-> 2 <-> 3
        let node1 = NetworkActorBuilder::new(None)
            .await
            .add_bind_addr_v4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 0))
            .build(vec![])
            .await
            .unwrap();

        let node1_addr = node1.get_node_addr().await.unwrap();

        let node2 = NetworkActorBuilder::new(None)
            .await
            .add_bind_addr_v4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 0))
            .build(vec![node1_addr.clone()])
            .await
            .unwrap();

        let node2_addr = node2.get_node_addr().await.unwrap();

        let node3 = NetworkActorBuilder::new(None)
            .await
            .add_bind_addr_v4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 0))
            .build(vec![node2_addr.clone()])
            .await
            .unwrap();

        let node3_addr = node3.get_node_addr().await.unwrap();

        // Wait for nodes to connect
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Generate a keypair for the realm
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);

        // Create realms with each node
        let realm1 = Realm::new(
            signing_key.clone(),
            node1_addr.clone(),
            node1.get_kademlia_actor_handle().await.unwrap(),
        )
        .await
        .unwrap();
        let realm2 = Realm::new(
            signing_key.clone(),
            node2_addr.clone(),
            node2.get_kademlia_actor_handle().await.unwrap(),
        )
        .await
        .unwrap();
        let realm3 = Realm::new(
            signing_key.clone(),
            node3_addr.clone(),
            node3.get_kademlia_actor_handle().await.unwrap(),
        )
        .await
        .unwrap();

        // Manually refresh addresses in the DHT
        realm1.refresh_own_address().await.unwrap();
        realm2.refresh_own_address().await.unwrap();
        realm3.refresh_own_address().await.unwrap();

        // Wait for DHT propagation
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Update each realm
        realm1.update_now().await.unwrap();
        realm2.update_now().await.unwrap();
        realm3.update_now().await.unwrap();

        // Check that all nodes can see each other
        let members1 = realm1.get_realm_member_addrs();
        let members2 = realm2.get_realm_member_addrs();
        let members3 = realm3.get_realm_member_addrs();

        assert_eq!(members1.len(), 3);
        assert_eq!(members2.len(), 3);
        assert_eq!(members3.len(), 3);

        assert!(members1.contains(&node1_addr));
        assert!(members1.contains(&node2_addr));
        assert!(members1.contains(&node3_addr));

        assert!(members2.contains(&node1_addr));
        assert!(members2.contains(&node2_addr));
        assert!(members2.contains(&node3_addr));

        assert!(members3.contains(&node1_addr));
        assert!(members3.contains(&node2_addr));
        assert!(members3.contains(&node3_addr));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_malicious_realm_rejection() {
        init_tracing();

        // Create two network nodes
        let legitimate_node = NetworkActorBuilder::new(None)
            .await
            .add_bind_addr_v4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 0))
            .build(vec![])
            .await
            .unwrap();

        let legitimate_addr = legitimate_node.get_node_addr().await.unwrap();

        let attacker_node = NetworkActorBuilder::new(None)
            .await
            .add_bind_addr_v4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 0))
            .build(vec![legitimate_addr.clone()])
            .await
            .unwrap();

        let attacker_addr = attacker_node.get_node_addr().await.unwrap();

        // Generate realm key and attacker key
        let mut csprng = OsRng;
        let realm_key = SigningKey::generate(&mut csprng);
        let attacker_key = SigningKey::generate(&mut csprng);

        // Create legitimate realm
        let legitimate_realm = Realm::new(
            realm_key.clone(),
            legitimate_addr.clone(),
            legitimate_node.get_kademlia_actor_handle().await.unwrap(),
        )
        .await
        .unwrap();

        // Create attacker realm with different key
        // (simulating an attacker trying to hijack the realm)
        let attacker_realm = Realm::new(
            attacker_key.clone(),
            attacker_addr.clone(),
            attacker_node.get_kademlia_actor_handle().await.unwrap(),
        )
        .await
        .unwrap();

        // Legitimate node stores its address properly
        legitimate_realm.refresh_own_address().await.unwrap();

        // Wait for propagation
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Attacker tries to store its address with its own signature (will be rejected by legitimate nodes)
        attacker_realm.refresh_own_address().await.unwrap();

        // Wait for propagation
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Update legitimate realm
        legitimate_realm.update_now().await.unwrap();

        // Legitimate realm should not include attacker node
        let members = legitimate_realm.get_realm_member_addrs();
        assert!(members.contains(&legitimate_addr));
        assert!(!members.contains(&attacker_addr));
    }
}
