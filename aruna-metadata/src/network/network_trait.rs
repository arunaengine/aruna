use crate::{
    error::ArunaMetadataError,
    models::requests::{ForwardRequest, ForwardResponse},
    persistence::
        search::generic::Search
    ,
    transactions::controller::Controller,
};
use aruna_permission::{Action, Path};
use aruna_storage::storage::store::Store;
use iroh::{NodeAddr, PublicKey};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ulid::Ulid;

pub const METADATA_PROTOCOL_ID: u32 = 3;
pub const REPLICATION_POLICY: usize = 2;

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
    Authorize {
        token: Option<String>,
        action: Action,
        resource_id: Ulid,
    },
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
    ForwardResponse(Box<ForwardResponse>),
    SyncResponse(Option<Vec<u8>>),
    AuthorizeResponse(AuthorizeResponse),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum AuthorizeResponse {
    Path(Path),
    Public,
    Error(ArunaMetadataError),
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
        subject_hash: &[u8; 32],
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
    async fn authorize(
        &self,
        subject_hash: &[u8; 32],
        token: Option<String>,
        action: Action,
        subject_id: Ulid,
        target_node: NodeAddr,
    ) -> Result<AuthorizeResponse, ArunaMetadataError>;
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
        _subject_hash: &[u8; 32],
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
    async fn authorize(
        &self,
        _subject_hash: &[u8; 32],
        _token: Option<String>,
        _action: Action,
        _subject_id: Ulid,
        _target_node: NodeAddr,
    ) -> Result<AuthorizeResponse, ArunaMetadataError> {
        Ok(AuthorizeResponse::Public)
    }
}

