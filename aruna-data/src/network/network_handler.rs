use aruna_net::{
    Kademlia, MaybeSignedAddr,
    actor_handle::{NetworkActorHandle, ReceiveStreams},
};
use aruna_permission::paths::RealmKey;
use aruna_realm::Realm;
use ed25519_dalek::SigningKey;
use iroh::{
    NodeAddr, PublicKey,
    endpoint::{RecvStream, SendStream},
};
use tracing::warn;

use crate::error::ArunaDataError;

#[derive(Clone)]
pub struct NetworkHandler {
    realm_key: RealmKey,
    node_addr: NodeAddr,
    kademlia: Kademlia,
    network: NetworkActorHandle,
    realm: Realm,
}

impl NetworkHandler {
    #[tracing::instrument(level = "trace", skip(network, kademlia, realm_key))]
    pub async fn new(
        network: NetworkActorHandle,
        kademlia: Kademlia,
        realm_key: RealmKey,
    ) -> Result<Self, ArunaDataError> {
        let node_addr = network
            .get_node_addr()
            .await
            .map_err(|e| ArunaDataError::ServerError(e.to_string()))?;
        let realm = Realm::new(
            SigningKey::from_bytes(&realm_key),
            node_addr.clone(),
            kademlia.clone(),
        )
        .await?;
        realm.update_now().await?;
        Ok(NetworkHandler {
            realm_key,
            node_addr,
            network,
            kademlia,
            realm,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_node_addr(&self) -> NodeAddr {
        self.node_addr.clone()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_realm_key(&self) -> RealmKey {
        self.realm_key
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn receive(&self) -> Result<ReceiveStreams, ArunaDataError> {
        self.network
            .receive()
            .await
            .map_err(|e| ArunaDataError::ServerError(e.to_string()))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn create_stream(
        &self,
        node_id: PublicKey,
    ) -> Result<(SendStream, RecvStream), ArunaDataError> {
        self.network
            .create_stream(node_id)
            .await
            .map_err(|e| ArunaDataError::ServerError(e.to_string()))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn store(&self, hash: blake3::Hash) -> Result<(), ArunaDataError> {
        // TODO: Evaluate if this is really used in the right place
        warn!("Storing unsigned value");
        self.kademlia
            .store(*hash.as_bytes(), self.get_node_addr(), None)
            .await
            .map_err(|e| ArunaDataError::ServerError(e.to_string()))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn find(&self, hash: blake3::Hash) -> Result<Vec<MaybeSignedAddr>, ArunaDataError> {
        self.kademlia
            .find_value(*hash.as_bytes())
            .await
            .map_err(|e| ArunaDataError::ServerError(e.to_string()))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_realm_nodes(&self) -> Result<Vec<NodeAddr>, ArunaDataError> {
        let query = self.realm.get_realm_member_addrs();
        // try one refresh if no members are found
        let members = if query.len() <= 1 {
            self.realm
                .update_now()
                .await
                .map_err(|e| ArunaDataError::ServerError(e.to_string()))?;
            self.realm.get_realm_member_addrs()
        } else {
            query
        };
        Ok(members)
    }
}
