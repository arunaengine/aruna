use std::sync::Arc;

use aruna_net::actor_handle::NetworkActorHandle;
use ed25519_dalek::{SecretKey, VerifyingKey};
use iroh::NodeAddr;
use parking_lot::RwLock;


#[derive(Clone)]
pub struct Realm {
    private_key: Arc<SecretKey>,
    public_key: Arc<VerifyingKey>,
    network: NetworkActorHandle,
    store: Arc<RealmStore>,
}

pub struct RealmStore {
    realm_addr: RwLock<Vec<NodeAddr>>,
}

impl RealmStore {
    pub fn new() -> Self {
        Self {
            realm_addr: RwLock::new(Vec::new()),
        }
    }

    pub fn add_realm_addr(&mut self, addr: NodeAddr) {
        let mut realm_addr = self.realm_addr.write();
        realm_addr.push(addr);
    }

    pub fn get_realm_addrs(&self) -> Vec<NodeAddr> {
        let realm_addr = self.realm_addr.read();
        realm_addr.clone()
    }
}

impl Realm {
    pub fn new(
        private_key: SecretKey,
        public_key: VerifyingKey,
        network: NetworkActorHandle,
    ) -> Self {
        let private_key = Arc::new(private_key);
        let public_key = Arc::new(public_key);
        let store = Arc::new(RealmStore::new());
        Self {
            private_key,
            public_key,
            network,
            store,
        }
    }
}
