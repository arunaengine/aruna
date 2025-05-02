use aruna_net::actor_handle::NetworkActorHandle;
use ed25519_dalek::{SecretKey, VerifyingKey};
use iroh::NodeAddr;

pub struct Realm {
    private_key: SecretKey,
    public_key: VerifyingKey,
    network: NetworkActorHandle,
}

pub struct RealmStore {
    realm_addr: Vec<NodeAddr>,
}

impl Realm {
    pub fn new(
        private_key: SecretKey,
        public_key: VerifyingKey,
        network: NetworkActorHandle,
    ) -> Self {
        Self {
            private_key,
            public_key,
            network,
        }
    }
}
