use crate::{
    error::ArunaMetadataError,
    models::requests::{ForwardRequest, ForwardResponse},
    persistence::search::search::Search,
    transactions::{controller::Controller, request::Request},
};
use aruna_net::{actor::NetworkActorBuilder, actor_handle::NetworkActorHandle};
use aruna_realm::Realm;
use aruna_storage::storage::store::Store;
use ed25519_dalek::SigningKey;
use iroh::{NodeAddr, PublicKey, SecretKey};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddrV4, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, info, trace, warn};
use ulid::Ulid;

pub static METADATA_PROTOCOL_ID: u32 = 3;
pub static REPLICATION_POLICY: usize = 1;

#[derive(Serialize, Deserialize, Clone, Debug)]
//pub struct MetadataMessage<R: Request> {
pub struct MetadataMessage {
    pub from: [u8; 32],    // Node ID
    pub to: [u8; 32],      // Node ID
    pub subject: [u8; 32], // Object or User ID
    pub body: Body,
    // TODO:
    //pub request: R,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Body {
    Replicate {
        id: Ulid,
        sync_message: ReplicationSubject,
    },
    Request {
        token: Option<String>,
        request: ForwardRequest,
    },
    Response(Response),
    Empty,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Response {
    ForwardResponse(ForwardResponse),
    SyncResponse(Option<Vec<u8>>),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ReplicationSubject {
    Group(Option<Vec<u8>>),
    User(Option<Vec<u8>>),
    Object(Option<Vec<u8>>),
}

#[async_trait::async_trait]
pub trait Network: Sync + Send + Sized {
    type Config;
    async fn new(config: Self::Config) -> Result<Self, ArunaMetadataError>;
    async fn get_addr(&self) -> Result<NodeAddr, ArunaMetadataError>;
    async fn sync(
        &self,
        subject: ReplicationSubject,
        subject_id: &Ulid,
        target_node: NodeAddr,
    ) -> Result<Option<Vec<u8>>, ArunaMetadataError>;

    async fn start_actor<St, Se, N>(
        self: Arc<Self>,
        controller: Arc<Controller<St, Se, N>>,
    ) -> Result<(), ArunaMetadataError>
    where
        for<'a> St: Store<'a>,
        Se: Search,
        N: Network;

    async fn forward(
        &self,
        body: Body,
        subject_id: &Ulid,
        target_node: NodeAddr,
    ) -> Result<ForwardResponse, ArunaMetadataError>;
    async fn find(&self, subject_id: &Ulid) -> Result<Vec<NodeAddr>, ArunaMetadataError>;
    async fn find_verified(&self, subject_id: &Ulid) -> Result<Vec<NodeAddr>, ArunaMetadataError>;
    async fn get_realm_nodes(&self) -> Result<Vec<NodeAddr>, ArunaMetadataError>;
    async fn store(&self, subject_id: &[u8; 32]) -> Result<(), ArunaMetadataError>;
    async fn store_in_realm(&self, subject_id: &Ulid) -> Result<(), ArunaMetadataError>;
}

pub struct NetworkDummy {
    self_id: NodeAddr,
}

#[async_trait::async_trait]
impl Network for NetworkDummy {
    type Config = ();
    async fn new(_config: Self::Config) -> Result<Self, ArunaMetadataError> {
        Ok(NetworkDummy {
            self_id: NodeAddr::new(
                PublicKey::from_bytes(&[0u8; 32])
                    .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?,
            ),
        })
    }
    async fn get_addr(&self) -> Result<NodeAddr, ArunaMetadataError> {
        Ok(self.self_id.clone())
    }

    async fn sync(
        &self,
        _subject: ReplicationSubject,
        _subject_id: &Ulid,
        _target_node: NodeAddr,
    ) -> Result<Option<Vec<u8>>, ArunaMetadataError> {
        Ok(None)
    }

    async fn start_actor<St, Se, N>(
        self: Arc<Self>,
        _controller: Arc<Controller<St, Se, N>>,
    ) -> Result<(), ArunaMetadataError>
    where
        for<'a> St: Store<'a>,
        Se: Search,
        N: Network,
    {
        Ok(())
    }

    async fn find(&self, _subject_id: &Ulid) -> Result<Vec<NodeAddr>, ArunaMetadataError> {
        Ok(vec![self.get_addr().await?])
    }

    async fn find_verified(&self, _subject_id: &Ulid) -> Result<Vec<NodeAddr>, ArunaMetadataError> {
        Ok(vec![self.get_addr().await?])
    }
    async fn forward(
        &self,
        _body: Body,
        _subject_id: &Ulid,
        _target_node: NodeAddr,
    ) -> Result<ForwardResponse, ArunaMetadataError> {
        Err(ArunaMetadataError::NetworkError(
            "DummyNetwork cannot forward messages".to_string(),
        ))
    }
    async fn store(&self, _subject_id: &[u8; 32]) -> Result<(), ArunaMetadataError> {
        Ok(())
    }

    async fn get_realm_nodes(&self) -> Result<Vec<NodeAddr>, ArunaMetadataError> {
        Ok(vec![self.self_id.clone()])
    }

    async fn store_in_realm(&self, _subject_id: &Ulid) -> Result<(), ArunaMetadataError> {
        Ok(())
    }
}

pub struct NetworkConfig {
    pub secret_key: Option<SecretKey>,
    pub socket_addr: SocketAddrV4,
    pub bootstrap_nodes: Vec<NodeAddr>,
    pub realm_key: Option<SigningKey>,
}

pub struct P2PNetwork {
    chandler: NetworkActorHandle,
    realm_handler: Option<Realm>,
}

#[async_trait::async_trait]
impl Network for P2PNetwork {
    type Config = NetworkConfig;

    async fn new(config: Self::Config) -> Result<Self, ArunaMetadataError> {
        let init_handle = NetworkActorBuilder::new(config.secret_key)
            .await
            .add_bind_addr_v4(config.socket_addr)
            .build(config.bootstrap_nodes)
            .await?;

        let chandler = init_handle.new_actor_handle(METADATA_PROTOCOL_ID).await?;
        let mut p2p = P2PNetwork {
            chandler,
            realm_handler: None,
        };

        if let Some(key) = config.realm_key {
            let self_addr = init_handle.get_node_addr().await?;
            let kademlia = init_handle.get_kademlia_actor_handle().await?;
            let realm = Realm::new(key, self_addr, kademlia).await?;
            realm.update_now().await?;
            p2p.realm_handler = Some(realm);
        }
        Ok(p2p)
    }

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
        tokio::spawn(async move {
            loop {
                if let Err(err) = network.dispatch_messages::<St, Se, N>(&controller).await {
                    error!("{err}");
                }
            }
        });
        Ok(())
    }

    async fn get_addr(&self) -> Result<NodeAddr, ArunaMetadataError> {
        self.chandler
            .get_node_addr()
            .await
            .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))
    }
    async fn find(&self, subject_id: &Ulid) -> Result<Vec<NodeAddr>, ArunaMetadataError> {
        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(subject_id.to_bytes().as_slice());
        let id_hash = chunk_hasher.finalize();
        Ok(self
            .chandler
            .get_kademlia_actor_handle()
            .await?
            .find_value(*id_hash.as_bytes())
            .await?
            .iter()
            .map(|m| m.addr().clone())
            .collect())
    }

    async fn find_verified(&self, subject_id: &Ulid) -> Result<Vec<NodeAddr>, ArunaMetadataError> {
        let Some(realm) = &self.realm_handler else {
            warn!("No realm configured!");
            return self.find(subject_id).await;
        };

        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(subject_id.to_bytes().as_slice());
        let id_hash = chunk_hasher.finalize();

        let members = realm.get_realm_member_addrs();
        let find_results = self
            .chandler
            .get_kademlia_actor_handle()
            .await?
            .find_value(*id_hash.as_bytes())
            .await?
            .iter()
            .map(|m| m.addr().clone())
            .filter(|m| members.contains(m))
            .collect();

        Ok(find_results)
    }

    #[tracing::instrument(level = "trace", skip(self, subject))]
    async fn sync(
        &self,
        subject: ReplicationSubject,
        subject_id: &Ulid,
        target_node: NodeAddr,
    ) -> Result<Option<Vec<u8>>, ArunaMetadataError> {
        let node_id = self.chandler.get_node_addr().await?;

        // Calc hash
        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(subject_id.to_bytes().as_slice());
        let id_hash = chunk_hasher.finalize();

        let mut message = MetadataMessage {
            from: *node_id.node_id.as_bytes(),
            to: [0u8; 32],
            subject: *id_hash.as_bytes(),
            body: Body::Replicate {
                id: *subject_id,
                sync_message: subject,
            },
        };
        trace!("{message:?}");

        // Distribute message to closest nodes
        message.to = *target_node.node_id.as_bytes();
        let msg = postcard::to_allocvec(&message)?;

        let (mut sdx, mut recv) = self.chandler.create_stream(target_node.node_id).await?;
        sdx.write_u32(msg.len() as u32).await?;
        sdx.write_all(msg.as_slice())
            .await
            .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
        //chandler.store(*id_hash.as_bytes(), node).await?; // TODO: Move this to handle_stream in persistence
        sdx.finish()
            .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;

        let len = recv.read_u32().await?;
        trace!(?len);
        let mut buf = vec![0; len as usize];
        recv.read_exact(&mut buf)
            .await
            .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
        let response = match postcard::from_bytes::<MetadataMessage>(&buf)
            .map_err(|e| {
                ArunaMetadataError::NetworkError(format!("Failed to deserialize response: {e:#}"))
            })?
            .body
        {
            Body::Response(Response::SyncResponse(result)) => result,
            e @ _ => {
                return Err(ArunaMetadataError::NetworkError(format!(
                    "Got wrong response {e:?}, expected Body::Response"
                )));
            }
        };
        trace!("Got response");

        // TODO:
        // - [] Retries? -> If err, try again, if failed 5 go next node
        // - [] Replicate metadata to specific nodes
        // - [X] Store when create
        // - [] Store when rcv replicate msg
        Ok(response)
    }

    #[tracing::instrument(level = "trace", skip(self, subject_id))]
    async fn forward(
        &self,
        body: Body,
        subject_id: &Ulid,
        target_node: NodeAddr,
    ) -> Result<ForwardResponse, ArunaMetadataError> {
        let node_addr = self.chandler.get_node_addr().await?;

        // Calc hash
        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(subject_id.to_bytes().as_slice());
        let id_hash = chunk_hasher.finalize();

        let message = MetadataMessage {
            from: *node_addr.node_id.as_bytes(),
            to: *target_node.node_id.as_bytes(),
            subject: *id_hash.as_bytes(),
            body,
        };

        trace!("Serialized");

        let msg = postcard::to_allocvec(&message)?;
        let (mut sdx, mut recv) = self.chandler.create_stream(target_node.node_id).await?;
        sdx.write_u32(msg.len() as u32).await?;
        sdx.write_all(msg.as_slice())
            .await
            .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
        sdx.finish()
            .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;

        trace!("Sent, waiting for response");

        let len = recv.read_u32().await?;
        trace!(?len);
        let mut buf = vec![0; len as usize];
        recv.read_exact(&mut buf)
            .await
            .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
        let response = match postcard::from_bytes::<MetadataMessage>(&buf)
            .map_err(|e| {
                ArunaMetadataError::NetworkError(format!("Failed to deserialize response: {e:#}"))
            })?
            .body
        {
            Body::Response(Response::ForwardResponse(result)) => result,
            e @ _ => {
                return Err(ArunaMetadataError::NetworkError(format!(
                    "Got wrong response {e:?}, expected Body::Response"
                )));
            }
        };
        trace!("Got response");

        Ok(response)
    }

    async fn store(&self, subject_id: &[u8; 32]) -> Result<(), ArunaMetadataError> {
        let node_addr = self
            .chandler
            .get_node_addr()
            .await
            .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;

        let kademlia = self.chandler.get_kademlia_actor_handle().await?;
        let subject_id = *subject_id;
        tokio::spawn(async move {
            // Calc hash
            kademlia.store(subject_id, node_addr, None).await?;
            Ok::<(), ArunaMetadataError>(())
        });
        Ok(())
    }

    async fn get_realm_nodes(&self) -> Result<Vec<NodeAddr>, ArunaMetadataError> {
        match &self.realm_handler {
            Some(realm) => {
                let query = realm.get_realm_member_addrs();
                // try one refresh if no members are found
                let members = if query.len() <= 1 {
                    realm.update_now().await?;
                    realm.get_realm_member_addrs()
                } else {
                    query
                };
                Ok(members)
            }
            None => Ok(vec![self.get_addr().await?]),
        }
    }

    async fn store_in_realm(&self, _subject_id: &Ulid) -> Result<(), ArunaMetadataError> {
        todo!()
    }
}

impl P2PNetwork {
    pub async fn dispatch_messages<St, Se, N>(
        &self,
        controller: &Controller<St, Se, N>,
    ) -> Result<(), ArunaMetadataError>
    where
        for<'a> St: Store<'a>,
        Se: Search,
        N: Network,
    {
        let mut recv_stream = self.chandler.receive().await?;
        while let Ok(len) = recv_stream.recv_stream.read_u32().await {
            trace!("Got something");
            let mut buf = vec![0; len as usize];

            // TODO:
            // - dispatch into API requests
            recv_stream
                .recv_stream
                .read_exact(&mut buf)
                .await
                .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
            let message = postcard::from_bytes::<MetadataMessage>(&buf).map_err(|e| {
                ArunaMetadataError::NetworkError(format!("Failed to deserialize message: {e:#}"))
            })?;
            trace!("{message:?}");
            match message.body {
                Body::Replicate { id, sync_message } => {
                    let node_id = PublicKey::from_bytes(&message.from).map_err(|e| {
                        ArunaMetadataError::ConversionError {
                            from: "&[u8]".to_string(),
                            to: "PublicKey".to_string(),
                        }
                    })?;
                    match controller
                        .persistence
                        .handle_replication(node_id, id, sync_message)
                        .await
                    {
                        Ok(res) => {
                            let response = postcard::to_allocvec(&MetadataMessage {
                                from: message.to,
                                to: message.from,
                                subject: message.subject,
                                body: Body::Response(Response::SyncResponse(res)),
                            })?;
                            recv_stream.send_stream.write_u32(response.len() as u32).await?;
                            recv_stream.send_stream.write_all(response.as_slice())
                                .await
                                .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
                            recv_stream
                                .send_stream
                                .finish()
                                .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
                            self.store(&message.subject).await?;
                        }
                        Err(err) => return Err(err),
                    }
                }
                Body::Request { token, request } => {
                    let user = match token {
                        Some(id) => {
                            match controller
                                .persistence
                                .get_user(&Ulid::from_string(&id).map_err(|e| {
                                    ArunaMetadataError::DeserializeError(e.to_string())
                                })?)
                                .await
                            {
                                Ok(user) => user,
                                Err(e) => {
                                    error!(?e);
                                    return Err(e);
                                }
                            }
                        }
                        None => None,
                    };
                    let body = match request {
                        ForwardRequest::GetResource(req) => {
                            Body::Response(Response::ForwardResponse(ForwardResponse::GetResource(
                                req.run_request(user, controller).await,
                            )))
                        }
                        ForwardRequest::UpdateResource(req) => Body::Response(
                            Response::ForwardResponse(ForwardResponse::UpdateResource(
                                req.run_request(user, controller).await,
                            )),
                        ),
                        ForwardRequest::Search(req) => Body::Response(Response::ForwardResponse(
                            ForwardResponse::Search(req.run_request(user, controller).await),
                        )),
                    };
                    let message = MetadataMessage {
                        from: message.to,
                        to: message.from,
                        subject: message.subject,
                        body,
                    };

                    let response = postcard::to_allocvec(&message)?;

                    recv_stream
                        .send_stream
                        .write_u32(response.len() as u32)
                        .await?;
                    recv_stream
                        .send_stream
                        .write_all(&response)
                        .await
                        .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
                }
                Body::Response { .. } => {
                    todo!("Backchannel for updated merged docs or sync protocol");
                    // Nothing to do here, there are currently no messages that send responses
                    // after replication/forwarding back
                }
                Body::Empty => {
                    // TODO: Nothing to do here, maybe remove enum variant
                }
            }
        }

        Ok(())
    }
}
