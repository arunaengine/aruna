use super::{
    auth::TokenHandler,
    request::{Request, SerializedResponse, WriteRequest},
};
use crate::{error::ArunaError, logerr, storage::store::Store, transactions::request::Requester};
use serde::{Deserialize, Serialize};
use std::fs;
use std::{net::SocketAddr, sync::Arc};
use synevi::storage::LmdbStore;
use synevi::Node as SyneviNode;
use synevi::SyneviResult;
use synevi::{network::GrpcNetwork, SyneviError, Transaction};
use tokio::sync::RwLock;
use tracing::trace;
use ulid::Ulid;

type ConsensusNode = RwLock<Option<Arc<SyneviNode<GrpcNetwork, Arc<Controller>, LmdbStore>>>>;

pub struct Controller {
    pub(super) store: Arc<Store>,
    pub(super) node: ConsensusNode,
}

pub type KeyConfig = (u32, String, String);

impl Controller {
    #[tracing::instrument(level = "trace", skip(key_config))]
    pub async fn new(
        path: String,
        node_id: Ulid,
        serial: u16,
        store_addr: SocketAddr,
        init_node: Option<String>,
        key_config: KeyConfig,
    ) -> Result<Arc<Self>, ArunaError> {
        let store = Store::new(path.clone(), key_config)?;

        let controller = Arc::new(Controller {
            store: Arc::new(store),
            node: RwLock::new(None),
        });

        let path = format!("{path}/events");
        fs::create_dir_all(&path)?;
        let synevi_lmdb: synevi::storage::LmdbStore =
            synevi::storage::LmdbStore::new(path, serial)?;

        let node = match init_node {
            Some(init_node) => SyneviNode::new_with_member(
                node_id,
                serial,
                GrpcNetwork::new(
                    store_addr,
                    format!("http://{}", store_addr),
                    node_id,
                    serial,
                ),
                controller.clone(),
                synevi_lmdb,
                init_node,
            )
            .await
            .inspect_err(logerr!())?,
            None => SyneviNode::new(
                node_id,
                serial,
                GrpcNetwork::new(
                    store_addr,
                    format!("http://{}", store_addr),
                    node_id,
                    serial,
                ),
                controller.clone(),
                synevi_lmdb,
            )
            .await
            .inspect_err(logerr!())?,
        };

        *controller.node.write().await = Some(node);

        Ok(controller)
    }

    pub fn get_store(&self) -> Arc<Store> {
        self.store.clone()
    }

    #[tracing::instrument(level = "trace", skip(self, transaction))]
    pub async fn transaction<R: WriteRequest>(
        &self,
        transaction_id: u128,
        transaction: &R,
    ) -> Result<SerializedResponse, ArunaError> {
        // This dyn cast ist necessary because otherwise typetag will not work
        let transaction = ArunaTransaction(
            bincode::serialize(transaction as &dyn WriteRequest).inspect_err(logerr!())?,
        );

        self.node
            .read()
            .await
            .as_ref()
            .ok_or_else(|| {
                tracing::error!("Node not set");
                ArunaError::ServerError("Node not set".to_string())
            })?
            .clone()
            .transaction(transaction_id, transaction)
            .await?
    }

    #[tracing::instrument(level = "trace", skip(self, request))]
    pub(crate) async fn request<R: Request>(
        &self,
        request: R,
        token: Option<String>,
    ) -> Result<R::Response, ArunaError> {
        tracing::info!("Received request");
        let requester: Option<Requester> = self.authorize_token(token, &request).await?;
        tracing::trace!(?requester, "Requester authorized");
        request.run_request(requester, self).await
    }

    #[tracing::instrument(level = "trace", skip(self, transaction))]
    pub async fn process_transaction(
        &self,
        id: u128,
        transaction: ArunaTransaction,
    ) -> Result<SerializedResponse, ArunaError> {
        tracing::trace!(?id, "Deserializing transaction");
        tracing::debug!(transaction = ?transaction.0.len());
        let tx: Box<dyn WriteRequest> =
            bincode::deserialize(&transaction.0).inspect_err(logerr!())?;
        tx.execute(id, self).await
    }

    pub fn get_token_handler(&self) -> TokenHandler {
        TokenHandler::new(self.store.clone())
    }
}

#[async_trait::async_trait]
impl synevi::Executor for Controller {
    type Tx = ArunaTransaction;
    async fn execute(&self, id: u128, transaction: Self::Tx) -> SyneviResult<Self> {
        trace!("Executing transaction");
        Ok(self.process_transaction(id, transaction).await)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArunaTransaction(pub Vec<u8>);

impl Transaction for ArunaTransaction {
    type TxErr = ArunaError;
    type TxOk = SerializedResponse;

    fn as_bytes(&self) -> Vec<u8> {
        self.0.clone()
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SyneviError>
    where
        Self: Sized,
    {
        Ok(Self(bytes))
    }
}
