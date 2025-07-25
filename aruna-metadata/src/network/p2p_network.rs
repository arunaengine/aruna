use crate::{
    error::ArunaMetadataError,
    models::requests::ForwardResponse,
    network::util::send_message,
    persistence::
        search::generic::Search
    ,
    transactions::controller::Controller,
};
use aruna_net::{
    actor::NetworkActorBuilder,
    actor_handle::{NetworkActorHandle, ReceiveStreams},
};
use aruna_permission::{Action, Path};
use aruna_realm::Realm;
use aruna_storage::storage::store::Store;
use ed25519_dalek::SigningKey;
use iroh::{NodeAddr, PublicKey, SecretKey};
use std::{net::SocketAddrV4, sync::Arc};
use tracing::{Instrument, error};
use ulid::Ulid;
use super::{network_trait::{AuthorizeResponse, Body, MetadataMessage, Network, ReplicationSubject, Response, METADATA_PROTOCOL_ID}, util::read_message};

pub struct NetworkConfig {
    pub secret_key: Option<SecretKey>,
    pub socket_addr: SocketAddrV4,
    pub bootstrap_nodes: Vec<NodeAddr>,
    pub realm_key: SigningKey,
}

pub struct P2PNetwork {
    pub chandler: NetworkActorHandle,
    realm_handler: Realm,
}

#[async_trait::async_trait]
impl Network for P2PNetwork {
    type Config = NetworkConfig;
    type Stream = ReceiveStreams;

    #[tracing::instrument(level = "trace", skip(config))]
    async fn new(config: Self::Config) -> Result<Self, ArunaMetadataError> {
        let init_handle = NetworkActorBuilder::new(config.secret_key)
            .await
            .add_bind_addr_v4(config.socket_addr)
            .build(config.bootstrap_nodes)
            .await?;

        let chandler = init_handle.new_actor_handle(METADATA_PROTOCOL_ID).await?;

        let self_addr = init_handle.get_node_addr().await?;
        let kademlia = init_handle.get_kademlia_actor_handle().await?;
        let realm_handler = Realm::new(config.realm_key, self_addr, kademlia).await?;
        realm_handler.update_now().await?;

        Ok(P2PNetwork {
            chandler,
            realm_handler,
        })
    }

    #[tracing::instrument(level = "trace", skip(self, controller))]
    async fn start_actor<St, Se, N>(
        self: Arc<Self>,
        controller: Arc<Controller<St, Se, N>>,
    ) -> Result<(), ArunaMetadataError>
    where
        for<'a> St: Store<'a> + 'static,
        Se: Search + 'static,
        N: Network,
    {
        let network = self.clone();
        let controller = controller.clone();
        tokio::spawn(
            async move {
                loop {
                    let mut recv_stream = match self.chandler.receive().await {
                        Ok(s) => s,
                        Err(err) => {
                            error!("{err}");
                            continue;
                        }
                    };
                    let controller = controller.clone();
                    let network = network.clone();
                    tokio::spawn(
                        async move {
                            if let Err(err) = network
                                .dispatch_messages::<St, Se, N>(
                                    &mut recv_stream,
                                    controller.as_ref(),
                                )
                                .await
                            {
                                error!("{err}");
                            }
                        }
                        .in_current_span(),
                    );
                }
            }
            .in_current_span(),
        );
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_addr(&self) -> Result<NodeAddr, ArunaMetadataError> {
        self.chandler
            .get_node_addr()
            .await
            .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn find(&self, subject_hash: &[u8; 32]) -> Result<Vec<NodeAddr>, ArunaMetadataError> {
        Ok(self
            .chandler
            .get_kademlia_actor_handle()
            .await?
            .find_value(*subject_hash)
            .await?
            .iter()
            .map(|m| m.addr().clone())
            .collect())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn find_verified(
        &self,
        subject_hash: &[u8; 32],
    ) -> Result<Vec<NodeAddr>, ArunaMetadataError> {
        let members = self.realm_handler.get_realm_member_addrs();
        let find_results = self
            .chandler
            .get_kademlia_actor_handle()
            .await?
            .find_value(*subject_hash)
            .await?
            .iter()
            .map(|m| m.addr().clone())
            .filter(|m| members.contains(m))
            .collect();

        Ok(find_results)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_realm_key(&self) -> Result<[u8; 32], ArunaMetadataError> {
        Ok(*self.realm_handler.realm_public_key().as_bytes())
    }
    #[tracing::instrument(level = "trace", skip(self, subject, stream))]
    async fn sync(
        &self,
        stream: &mut Self::Stream,
        subject: ReplicationSubject,
        subject_hash: &[u8; 32],
        doc_id: Vec<u8>,
        path: Path,
        target_node: NodeAddr,
    ) -> Result<Option<Vec<u8>>, ArunaMetadataError> {
        let (sdx, recv) = (&mut stream.send_stream, &mut stream.recv_stream);
        let node_id = self.chandler.get_node_addr().await?;

        let mut message = MetadataMessage {
            from: *node_id.node_id.as_bytes(),
            to: [0u8; 32],
            subject: *subject_hash,
            body: Body::Replicate {
                id: doc_id,
                path,
                sync_message: subject,
            },
        };

        message.to = *target_node.node_id.as_bytes();
        send_message(message, sdx).await?;

        let response = match read_message(recv).await?.body {
            Body::Response(Response::SyncResponse(result)) => result,
            e => {
                return Err(ArunaMetadataError::NetworkError(format!(
                    "Got wrong response {e:?}, expected Body::Response"
                )));
            }
        };

        Ok(response)
    }

    #[tracing::instrument(level = "trace", skip(self, subject_hash))]
    async fn forward(
        &self,
        body: Body,
        subject_hash: &[u8; 32],
        target_node: NodeAddr,
    ) -> Result<ForwardResponse, ArunaMetadataError> {
        let node_addr = self.chandler.get_node_addr().await?;

        let message = MetadataMessage {
            from: *node_addr.node_id.as_bytes(),
            to: *target_node.node_id.as_bytes(),
            subject: *subject_hash,
            body,
        };

        let (mut sdx, mut recv) = self.chandler.create_stream(target_node.node_id).await?;

        send_message(message, &mut sdx).await?;
        sdx.finish()
            .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;

        let response = match read_message(&mut recv).await?.body {
            Body::Response(Response::ForwardResponse(result)) => result,
            e => {
                return Err(ArunaMetadataError::NetworkError(format!(
                    "Got wrong response {e:?}, expected Body::Response"
                )));
            }
        };
        Ok(*response)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn store(&self, subject_id: &[u8; 32]) -> Result<(), ArunaMetadataError> {
        let node_addr = self
            .chandler
            .get_node_addr()
            .await
            .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;

        let kademlia = self.chandler.get_kademlia_actor_handle().await?;
        let subject_id = *subject_id;
        tokio::spawn(
            async move {
                // Calc hash
                kademlia.store(subject_id, node_addr, None).await?;
                Ok::<(), ArunaMetadataError>(())
            }
            .in_current_span(),
        );
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_realm_nodes(&self) -> Result<Vec<NodeAddr>, ArunaMetadataError> {
        let query = self.realm_handler.get_realm_member_addrs();
        // try one refresh if no members are found
        let members = if query.len() <= 1 {
            self.realm_handler.update_now().await?;
            self.realm_handler.get_realm_member_addrs()
        } else {
            query
        };
        Ok(members)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn update_realm(&self) -> Result<(), ArunaMetadataError> {
        self.realm_handler.update_now().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn store_in_realm(&self, _subject_id: &[u8; 32]) -> Result<(), ArunaMetadataError> {
        todo!()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn create_stream(&self, target: PublicKey) -> Result<Self::Stream, ArunaMetadataError> {
        let (send, rcv) = self.chandler.create_stream(target).await?;
        Ok(ReceiveStreams {
            sender: target,
            send_stream: send,
            recv_stream: rcv,
        })
    }

    #[tracing::instrument(level = "trace", skip(self, stream))]
    async fn finish_stream(&self, mut stream: Self::Stream) -> Result<(), ArunaMetadataError> {
        stream
            .send_stream
            .finish()
            .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
        Ok(())
    }

    async fn authorize(
        &self,
        subject_hash: &[u8; 32],
        token: Option<String>,
        action: Action,
        subject_id: Ulid,
        target_node: NodeAddr,
    ) -> Result<AuthorizeResponse, ArunaMetadataError> {
        let mut stream = self.create_stream(target_node.node_id).await?;
        let self_addr = self.get_addr().await?;
        let (sdx, recv) = (&mut stream.send_stream, &mut stream.recv_stream);

        let message = MetadataMessage {
            from: *self_addr.node_id.as_bytes(),
            to: *target_node.node_id.as_bytes(),
            subject: *subject_hash,
            body: Body::Authorize {
                token,
                action,
                resource_id: subject_id,
            },
        };
        send_message(message, sdx).await?;

        let response = match read_message(recv).await?.body {
            Body::Response(Response::AuthorizeResponse(result)) => result,
            e => {
                return Err(ArunaMetadataError::NetworkError(format!(
                    "Got wrong response {e:?}, expected Body::Response"
                )));
            }
        };
        stream
            .send_stream
            .finish()
            .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;

        Ok(response)
    }
}

