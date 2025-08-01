use crate::api_json::request::Request;
use crate::io::io_handler::BLOCK_SIZE;
use crate::io::messages::{MessageType, ReplicationMessage};
use crate::network::network_handler::NetworkHandler;
use crate::util::bao_tree::{FuturesAsyncReaderWrapper, SendStreamWrapper};
use crate::util::opendal::get_data_async_reader;
use crate::{IOHandler, error::ArunaDataError};
use aruna_permission::UserIdentity;
use aruna_storage::storage::store::Store;
use bao_tree::io::fsm::{encode_ranges_validated, CreateOutboard};
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::round_up_to_chunks;
use bao_tree::ByteRanges;
use bytes::BytesMut;
use iroh::NodeAddr;
use ulid::Ulid;
use std::sync::Arc;
use tracing::{debug, error, trace};
use anyhow::anyhow;

use super::io_handler::ObjectInfo;

#[derive(Clone)]
pub struct Controller<St>
where
    for<'a> St: Store<'a> + 'static,
{
    pub io_handler: Arc<IOHandler<St>>,
    pub network: NetworkHandler,
}

impl<St> Controller<St>
where
    for<'a> St: Store<'a> + 'static,
{
    #[tracing::instrument(level = "trace", skip(io_handler, network))]
    pub async fn new(io_handler: Arc<IOHandler<St>>, network: NetworkHandler) -> Self {
        let controller = Self {
            io_handler,
            network,
        };
        controller.clone().run().await;
        controller
    }
    #[tracing::instrument(level = "trace", skip(self, request, token))]
    pub async fn request<R: Request<St>>(
        &self,
        request: R,
        token: Option<String>,
    ) -> Result<R::Response, ArunaDataError> {
        match request.forward_or_return(&token, self).await? {
            Some(response) => Ok(response),
            None => {
                let user_identity = match token {
                    Some(token) => Some(
                        self.get_identity(token)
                            .await
                            .map_err(|_| ArunaDataError::Unauthorized)?,
                    ),
                    None => None,
                };

                let result = request.run_request(user_identity, self).await?;
                Ok(result)
            }
        }
    }

    pub async fn run(self) {
        tokio::spawn(async move {
            let network = self.network.clone();
            //let self_addr = self.network.get_node_addr().clone();
            loop {
                tokio::select! {
                        // Handle incoming streams
                        Ok(inc_stream) = network.receive() => {
                            let io_handler = self.io_handler.clone();
                            let network = network.clone();

                            trace!("{:?} Received stream from: {:?}", network.get_node_addr(), inc_stream.sender);
                            tokio::spawn(
                                async move {
                                    if let Err(e) = io_handler.handle_incoming_p2p_stream(
                                        inc_stream,
                                        network.get_realm_key(),
                                        network.get_node_addr(),

                                    ).await{
                                        error!("Failed to handle incoming stream: {e:#}");
                                    }
                                }
                            );
                        }

                        // Check for shutdown signal
                        _ = tokio::signal::ctrl_c() => {
                            break;
                        }
                    }
            }
        });
    }

    pub async fn get_identity(&self, token: String) -> anyhow::Result<UserIdentity> {
        let store = self.io_handler.store.clone();
        let token_handler = self.io_handler.token_handler.clone();
        // TODO: Query user from kademlia
        tokio::task::spawn_blocking(move || -> anyhow::Result<UserIdentity> {
            let txn = store.create_txn(false)?;
            let user_identity = token_handler
                .write()
                .get_identity(&token, &store, &txn)?;
            store.commit(txn)?;
            Ok(user_identity)
        })
        .await?
    }

    pub async fn bao_tree_replicate(
        &self,
        user_id: UserIdentity,
        group_id: Ulid,
        replication_id: Ulid,
        replication_node: NodeAddr,
        replication_path: Option<String>,
        object_info: &ObjectInfo,
    ) -> anyhow::Result<()> {
        // Create backend storage operator
        let operator = self.io_handler.get_operator(
            &object_info.storage_root,
        )
        .await?;

        // Get data stream
        let stream = get_data_async_reader(
            &operator,
            &object_info.storage_path,
            None,
            Some(BLOCK_SIZE.bytes()),
        )
        .await?;
        let mut stream_wrapper = FuturesAsyncReaderWrapper {
            stream,
            info: object_info.clone(),
        };
        debug!("Fetched data stream from backend.");
        let mut outboard =
            PreOrderOutboard::<BytesMut>::create(&mut stream_wrapper, BLOCK_SIZE).await?;

        // Send replication init
        let (mut sx, mut rx) = self.network.create_stream(replication_node.node_id).await?;
        let init_request = ReplicationMessage {
            id: replication_id,
            sender: self.network.get_node_addr(),
            msg_type: MessageType::InitReplicationRequest {
                user_id,
                group_id,
                path: replication_path,
                size: object_info.file_size,
                root: outboard.root,
            },
        };
        init_request.send(&mut sx).await?;
        debug!("Sent replication init message to {:?}", replication_node);

        // Wait for response if replication node accepts
        let response = ReplicationMessage::read(&mut rx).await?;
        if let MessageType::InitReplicationResponse { ack, reason } = response.msg_type {
            if !ack {
                return Err(anyhow!(
                    "Replication node {:?} rejected replication request: {:?}",
                    replication_node,
                    reason.ok_or_else(|| "No reason provided")
                ));
            }
        }

        // Send bao_tree encoded chunks
        let ranges = ByteRanges::from(0..object_info.file_size);
        let ranges = round_up_to_chunks(&ranges);
        debug!("Chunk Ranges: {:#?}", ranges.boundaries());

        let mut sx_wrapper = SendStreamWrapper(sx);
        encode_ranges_validated(stream_wrapper, &mut outboard, &ranges, &mut sx_wrapper).await?;
        debug!("Sent all chunks.");

        Ok(())
    }
}
