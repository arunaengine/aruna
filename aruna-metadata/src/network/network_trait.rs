use crate::{
    error::ArunaMetadataError,
    models::requests::{ForwardRequest, ForwardResponse},
    network::util::send_message,
    persistence::{
        persistence::tables::{GROUPS_DB_NAME, RESOURCE_DB_NAME, USER_DB_NAME},
        search::search::Search,
    },
    transactions::{controller::Controller, request::Request},
};
use aruna_net::{
    actor::NetworkActorBuilder,
    actor_handle::{NetworkActorHandle, ReceiveStreams},
};
use aruna_permission::Path;
use aruna_realm::Realm;
use aruna_storage::storage::store::Store;
use ed25519_dalek::SigningKey;
use iroh::{NodeAddr, PublicKey, SecretKey};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddrV4, sync::Arc};
use tracing::error;
use ulid::Ulid;

use super::util::read_message;

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
        id: Vec<u8>,
        path: Path,
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
    type Stream: Send + Sync + Sized;
    async fn new(config: Self::Config) -> Result<Self, ArunaMetadataError>;
    async fn get_addr(&self) -> Result<NodeAddr, ArunaMetadataError>;
    async fn sync(
        &self,
        stream: &mut Self::Stream,
        subject: ReplicationSubject,
        subject_hash: &[u8; 32],
        doc_id: Vec<u8>,
        path: Path,
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
    async fn create_stream(&self, target: PublicKey) -> Result<Self::Stream, ArunaMetadataError>;
    async fn finish_stream(&self, stream: Self::Stream) -> Result<(), ArunaMetadataError>;
    async fn find(&self, subject_hash: &[u8; 32]) -> Result<Vec<NodeAddr>, ArunaMetadataError>;
    async fn find_verified(
        &self,
        subject_hash: &[u8; 32],
    ) -> Result<Vec<NodeAddr>, ArunaMetadataError>;
    async fn get_realm_nodes(&self) -> Result<Vec<NodeAddr>, ArunaMetadataError>;
    async fn store(&self, subject_hash: &[u8; 32]) -> Result<(), ArunaMetadataError>;
    // This is needed for testing
    #[allow(dead_code)]
    async fn update_realm(&self) -> Result<(), ArunaMetadataError>;
    async fn store_in_realm(&self, subject_id: &[u8; 32]) -> Result<(), ArunaMetadataError>;
    async fn get_realm_key(&self) -> Result<[u8; 32], ArunaMetadataError>;
}

pub struct NetworkDummy {
    self_id: NodeAddr,
    realm_key: [u8; 32],
}

#[async_trait::async_trait]
impl Network for NetworkDummy {
    type Config = ();
    type Stream = ();
    async fn new(_config: Self::Config) -> Result<Self, ArunaMetadataError> {
        Ok(NetworkDummy {
            self_id: NodeAddr::new(
                PublicKey::from_bytes(&[0u8; 32])
                    .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?,
            ),
            realm_key: [0u8; 32],
        })
    }
    async fn get_addr(&self) -> Result<NodeAddr, ArunaMetadataError> {
        Ok(self.self_id.clone())
    }

    async fn create_stream(&self, _target: PublicKey) -> Result<Self::Stream, ArunaMetadataError> {
        Ok(())
    }

    async fn finish_stream(&self, _stream: Self::Stream) -> Result<(), ArunaMetadataError> {
        Ok(())
    }
    async fn sync(
        &self,
        _stream: &mut Self::Stream,
        _subject: ReplicationSubject,
        _subject_hash: &[u8; 32],
        _doc_id: Vec<u8>,
        _path: Path,
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

    async fn find(&self, _subject_hash: &[u8; 32]) -> Result<Vec<NodeAddr>, ArunaMetadataError> {
        Ok(vec![self.get_addr().await?])
    }

    async fn find_verified(
        &self,
        _subject_hash: &[u8; 32],
    ) -> Result<Vec<NodeAddr>, ArunaMetadataError> {
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
    async fn update_realm(&self) -> Result<(), ArunaMetadataError> {
        Ok(())
    }

    async fn get_realm_nodes(&self) -> Result<Vec<NodeAddr>, ArunaMetadataError> {
        Ok(vec![self.self_id.clone()])
    }

    async fn store_in_realm(&self, _subject_hash: &[u8; 32]) -> Result<(), ArunaMetadataError> {
        Ok(())
    }

    async fn get_realm_key(&self) -> Result<[u8; 32], ArunaMetadataError> {
        Ok(self.realm_key)
    }
}

pub struct NetworkConfig {
    pub secret_key: Option<SecretKey>,
    pub socket_addr: SocketAddrV4,
    pub bootstrap_nodes: Vec<NodeAddr>,
    pub realm_key: SigningKey,
}

pub struct P2PNetwork {
    chandler: NetworkActorHandle,
    realm_handler: Realm,
}

#[async_trait::async_trait]
impl Network for P2PNetwork {
    type Config = NetworkConfig;
    type Stream = ReceiveStreams;

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
                let mut recv_stream = match self.chandler.receive().await {
                    Ok(s) => s,
                    Err(err) => {
                        error!("{err}");
                        continue;
                    }
                };
                let controller = controller.clone();
                let network = network.clone();
                tokio::spawn(async move {
                    if let Err(err) = network
                        .dispatch_messages::<St, Se, N>(&mut recv_stream, controller.as_ref())
                        .await
                    {
                        error!("{err}");
                    }
                });
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
        Ok(self.realm_handler.realm_public_key().as_bytes().clone())
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
            e @ _ => {
                return Err(ArunaMetadataError::NetworkError(format!(
                    "Got wrong response {e:?}, expected Body::Response"
                )));
            }
        };

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

        let (mut sdx, mut recv) = self.chandler.create_stream(target_node.node_id).await?;

        send_message(message, &mut sdx).await?;
        sdx.finish()
            .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;

        let response = match read_message(&mut recv).await?.body {
            Body::Response(Response::ForwardResponse(result)) => result,
            e @ _ => {
                return Err(ArunaMetadataError::NetworkError(format!(
                    "Got wrong response {e:?}, expected Body::Response"
                )));
            }
        };
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

    async fn update_realm(&self) -> Result<(), ArunaMetadataError> {
        self.realm_handler.update_now().await?;
        Ok(())
    }

    async fn store_in_realm(&self, _subject_id: &[u8; 32]) -> Result<(), ArunaMetadataError> {
        todo!()
    }

    async fn create_stream(&self, target: PublicKey) -> Result<Self::Stream, ArunaMetadataError> {
        let (send, rcv) = self.chandler.create_stream(target).await?;
        Ok(ReceiveStreams {
            sender: target,
            send_stream: send,
            recv_stream: rcv,
        })
    }

    async fn finish_stream(&self, mut stream: Self::Stream) -> Result<(), ArunaMetadataError> {
        stream
            .send_stream
            .finish()
            .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
        Ok(())
    }
}

impl P2PNetwork {
    async fn handle_replication_messages<St, Se, N>(
        &self,
        message: MetadataMessage,
        sync_message: ReplicationSubject,
        subject_id: Vec<u8>,
        path: Path,
        recv_stream: &mut ReceiveStreams,
        controller: &Controller<St, Se, N>,
    ) -> Result<(), ArunaMetadataError>
    where
        for<'a> St: Store<'a>,
        Se: Search,
        N: Network,
    {
        // Init sync
        let node_id = PublicKey::from_bytes(&message.from).map_err(|_e| {
            ArunaMetadataError::ConversionError {
                from: "&[u8]".to_string(),
                to: "PublicKey".to_string(),
            }
        })?;
        let table = match sync_message {
            ReplicationSubject::Group(_) => GROUPS_DB_NAME,
            ReplicationSubject::User(_) => USER_DB_NAME,
            ReplicationSubject::Object(_) => RESOURCE_DB_NAME,
        };
        let mut doc = controller
            .persistence
            .get_or_create_doc(subject_id.clone(), table)
            .await?;

        // Poll sync response
        let sync_response = controller
            .persistence
            .handle_replication(node_id, subject_id, path, sync_message, &mut doc)
            .await?;

        // Send response either with Some(_) or None
        send_message(
            MetadataMessage {
                from: message.to,
                to: message.from,
                subject: message.subject,
                body: Body::Response(Response::SyncResponse(sync_response)),
            },
            &mut recv_stream.send_stream,
        )
        .await?;

        // Start sync loop
        'inner: loop {
            let MetadataMessage {
                from,
                to,
                subject,
                body:
                    Body::Replicate {
                        id,
                        path,
                        sync_message,
                    },
            } = read_message(&mut recv_stream.recv_stream).await?
            else {
                return Err(ArunaMetadataError::ServerError(
                    "Unexpected Message type".to_string(),
                ));
            };

            let sync_response = controller
                .persistence
                .handle_replication(node_id, id, path.clone(), sync_message.clone(), &mut doc)
                .await?;

            send_message(
                MetadataMessage {
                    from: to,
                    to: from,
                    subject,
                    body: Body::Response(Response::SyncResponse(sync_response.clone())),
                },
                &mut recv_stream.send_stream,
            )
            .await?;

            if sync_response.is_none() {
                match sync_message {
                    ReplicationSubject::Group(None) => {
                        controller
                            .persistence
                            .handle_group_merges(doc.save())
                            .await?;
                    }
                    ReplicationSubject::User(None) => {
                        controller
                            .persistence
                            .handle_user_merges(doc.save())
                            .await?;
                        self.store(&subject).await?;
                    }
                    ReplicationSubject::Object(None) => {
                        controller
                            .persistence
                            .handle_object_merges(path, doc.save())
                            .await?;
                        self.store(&subject).await?;
                    }
                    _ => continue,
                }
                recv_stream
                    .send_stream
                    .finish()
                    .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
                break 'inner;
            }
        }
        Ok(())
    }

    pub async fn dispatch_messages<St, Se, N>(
        &self,
        recv_stream: &mut ReceiveStreams,
        controller: &Controller<St, Se, N>,
    ) -> Result<(), ArunaMetadataError>
    where
        for<'a> St: Store<'a>,
        Se: Search,
        N: Network,
    {
        while let Ok(msg) = read_message(&mut recv_stream.recv_stream).await {
            match msg.body {
                Body::Replicate {
                    ref id,
                    ref path,
                    ref sync_message,
                } => {
                    self.handle_replication_messages(
                        msg.clone(),
                        sync_message.clone(),
                        id.clone(),
                        path.clone(),
                        recv_stream,
                        controller,
                    )
                    .await?;
                }
                Body::Request { token, request } => {
                    let body = match request {
                        ForwardRequest::GetResource(req) => {
                            match req.authorize(token, controller).await {
                                Ok(auth_ctx) => Body::Response(Response::ForwardResponse(
                                    ForwardResponse::GetResource(
                                        req.clone().run_request(auth_ctx, controller).await,
                                    ),
                                )),
                                Err(e) => Body::Response(Response::ForwardResponse(
                                    ForwardResponse::GetResource(Err(e)),
                                )),
                            }
                        }
                        ForwardRequest::UpdateResource(req) => {
                            match req.authorize(token, controller).await {
                                Ok(auth_ctx) => Body::Response(Response::ForwardResponse(
                                    ForwardResponse::UpdateResource(
                                        req.clone().run_request(auth_ctx, controller).await,
                                    ),
                                )),
                                Err(e) => Body::Response(Response::ForwardResponse(
                                    ForwardResponse::UpdateResource(Err(e)),
                                )),
                            }
                        }
                        ForwardRequest::Search(req) => {
                            match req.authorize(token, controller).await {
                                Ok(auth_ctx) => Body::Response(Response::ForwardResponse(
                                    ForwardResponse::Search(
                                        req.clone().run_request(auth_ctx, controller).await,
                                    ),
                                )),
                                Err(e) => Body::Response(Response::ForwardResponse(
                                    ForwardResponse::Search(Err(e)),
                                )),
                            }
                        }
                    };
                    send_message(
                        MetadataMessage {
                            from: msg.to,
                            to: msg.from,
                            subject: msg.subject,
                            body,
                        },
                        &mut recv_stream.send_stream,
                    )
                    .await?;
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
