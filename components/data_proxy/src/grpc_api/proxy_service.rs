use crate::{
    auth::auth_helpers::get_token_from_md,
    caching::cache::Cache,
    data_backends::storage_backend::StorageBackend,
    replication::replication_message_handler::ReplicationMessage,
    s3_frontend::utils::replication_sink::ReplicationSink,
    structs::{Object, ObjectLocation, PubKey},
    CONFIG,
};
use anyhow::{anyhow, Result};
use bytes::{BufMut, BytesMut};
use pithos_lib::{
    helpers::footer_parser::{Footer, FooterParser},
    streamreadwrite::GenericStreamReadWriter,
    transformer::ReadWriter,
    transformers::{
        decrypt::ChaCha20Dec, footer::FooterGenerator, footer_updater::FooterUpdater,
        zstd_decomp::ZstdDec,
    },
};

use aruna_rust_api::api::dataproxy::services::v2::{
    dataproxy_replication_service_server::DataproxyReplicationService, error_message::Error,
    ErrorMessage, Handshake, RetryChunkMessage, Skip,
};
use aruna_rust_api::api::dataproxy::services::v2::{
    pull_replication_request::Message, pull_replication_response, ChunkAckMessage, InfoAckMessage,
    InitMessage, PullReplicationRequest, PullReplicationResponse, PushReplicationRequest,
    PushReplicationResponse,
};
use async_channel::{Receiver, Sender};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use std::{
    collections::{HashMap, HashSet},
    //sync::Mutex,
};
use std::{str::FromStr, sync::Arc};
use tokio::{pin, sync::Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;
use tracing::{error, info_span, trace, Instrument};

#[derive(Clone)]
pub struct DataproxyReplicationServiceImpl {
    pub cache: Arc<Cache>,
    pub sender: Sender<ReplicationMessage>,
    pub backend: Arc<Box<dyn StorageBackend>>,
}

impl DataproxyReplicationServiceImpl {
    #[tracing::instrument(level = "trace", skip(cache))]
    pub fn new(
        cache: Arc<Cache>,
        sender: Sender<ReplicationMessage>,
        backend: Arc<Box<dyn StorageBackend>>,
    ) -> Self {
        Self {
            cache,
            sender,
            backend,
        }
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
enum AckSync {
    ObjectInit(DieselUlid),
    ObjectChunk(DieselUlid, i64), // Object id, completed_chunks, chunks_idx
    Finish,
}

#[tonic::async_trait]
impl DataproxyReplicationService for DataproxyReplicationServiceImpl {
    type PullReplicationStream = ReceiverStream<Result<PullReplicationResponse, tonic::Status>>;
    /// PullReplication
    ///
    /// Status: BETA
    ///
    /// Creates a replication request
    #[tracing::instrument(level = "trace", skip(self, request))]
    async fn pull_replication(
        &self,
        request: tonic::Request<Streaming<PullReplicationRequest>>,
    ) -> Result<tonic::Response<Self::PullReplicationStream>, tonic::Status> {
        trace!("Received request: {request:?}");
        let (metadata, _, mut request) = request.into_parts();
        let token = get_token_from_md(&metadata).map_err(|_| {
            error!(error = "Token not found");
            tonic::Status::unauthenticated("Token not found")
        })?;

        // Sends initial Vec<(object, location)> to sync/ack/stream handlers
        let (object_input_send, object_input_rcv) = async_channel::bounded(5);

        // Sends ack messages to the ack handler
        let (object_ack_send, object_ack_rcv) = async_channel::bounded(100);

        // Sends the finished ack hash map to the output handler to compare send/ack
        let (object_sync_send, object_sync_rcv) = async_channel::bounded(1);

        // Handles output stream
        let (object_output_send, object_output_rcv) = tokio::sync::mpsc::channel(255);

        // Error and retry handling for ArunaStreamReadWriter
        let (retry_send, retry_rcv) = async_channel::bounded(5);

        let finished_state_handler = Arc::new(Mutex::new(false));
        let finished_state_clone = finished_state_handler.clone();

        let (id, pk) = self.get_endpoint_from_token(&token).await?;
        let pk = crate::auth::crypto::ed25519_to_x25519_pubkey(&pk.key)
            .map_err(|_| tonic::Status::internal("Unable to convert pubkey"))?;

        // Receiving loop
        let proxy_replication_service = self.clone();
        object_output_send
            .send(Ok(PullReplicationResponse {
                message: Some(pull_replication_response::Message::Handshake(Handshake {})),
            }))
            .await
            .map_err(|_| tonic::Status::internal("Error sending handshake response"))?;
        let output_sender = object_output_send.clone();

        tokio::spawn(async move {
            loop {
                match request.message().await {
                    Ok(message) => {
                        match message {
                            Some(message) => {
                                let PullReplicationRequest { message } = message;
                                match message {
                                    Some(message) => match message {
                                        Message::InitMessage(init) => {
                                            trace!(?init);
                                            let msg = proxy_replication_service
                                                .check_permissions(init, id)
                                                .await;
                                            object_input_send.send(msg).await.map_err(|e| {
                                                error!(error = ?e, msg = e.to_string());
                                                e
                                            })?;
                                        }
                                        Message::InfoAckMessage(InfoAckMessage { object_id }) => {
                                            trace!(info_ack_for=?object_id);
                                            let object_id = DieselUlid::from_str(&object_id)?;
                                            // Send object init into acknowledgement sync handler

                                            object_ack_send
                                                .send(AckSync::ObjectInit(object_id))
                                                .await
                                                .map_err(|e| {
                                                    error!(error = ?e, msg = e.to_string());
                                                    e
                                                })?;
                                        }
                                        Message::ChunkAckMessage(ChunkAckMessage {
                                            object_id,
                                            chunk_idx,
                                        }) => {
                                            let object_id = DieselUlid::from_str(&object_id)?;
                                            // Send object init into acknowledgement sync handler
                                            object_ack_send
                                                .send(AckSync::ObjectChunk(object_id, chunk_idx))
                                                .await
                                                .map_err(|e| {
                                                    error!(error = ?e, msg = e.to_string());
                                                    e
                                                })?;
                                            retry_send.send(None).await.map_err(|e| {
                                                error!(error = ?e, msg = e.to_string());
                                                e
                                            })?;
                                        }
                                        Message::ErrorMessage(ErrorMessage { error }) => {
                                            if let Some(err) = error {
                                                match err {
                                                    Error::RetryChunk(RetryChunkMessage {
                                                        object_id,
                                                        chunk_idx,
                                                    }) => {
                                                        let msg = Some((chunk_idx, object_id));
                                                        retry_send.send(msg).await.map_err(|e| {
                                                    tracing::error!(error = ?e, msg = e.to_string());
                                                    e
                                                })?;
                                                    }
                                                    Error::Abort(_) => {
                                                        error!(error = "Aborted sync");
                                                        return Err(anyhow!("Aborted sync"));
                                                    }
                                                    Error::RetryObjectId(object_id) => {
                                                        let ulid =
                                                    DieselUlid::from_str(&object_id).inspect_err(|&e| {
                                                        tracing::error!(error = ?e, msg = e.to_string());
                                                    })?;
                                                        let (object, location) =
                                                            proxy_replication_service
                                                                .cache
                                                                .get_resource_cloned(&ulid, true)
                                                                .await?;
                                                        if let Some(auth) =
                                                            proxy_replication_service
                                                                .cache
                                                                .auth
                                                                .read()
                                                                .await
                                                                .as_ref()
                                                        {
                                                            // Returns claims.sub as id -> Can return UserIds or DataproxyIds
                                                            // -> UserIds cannot be found in object.endpoints, so this should be safe
                                                            let (dataproxy_id, _, _) = auth
                                                                .check_permissions(&token)
                                                                .map_err(|_| {
                                                                    error!(
                                                                error =
                                                                    "DataProxy not authenticated"
                                                            );
                                                                    tonic::Status::unauthenticated(
                                                                "DataProxy not authenticated",
                                                            )
                                                                })?;
                                                            if !object
                                                                .endpoints
                                                                .iter()
                                                                .any(|ep| ep.id == dataproxy_id)
                                                            {
                                                                trace!("Endpoint has no permission to replicate object");
                                                                output_sender
                                                                .send(Err(
                                                                    tonic::Status::unauthenticated(
                                                                        "Access denied",
                                                                    )
                                                                ))
                                                                .await
                                                                .map_err(|e| {
                                                                    tracing::error!(error = ?e, msg = e.to_string());
                                                                    e
                                                                })?;
                                                            } else {
                                                                object_input_send
                                                                .send(Ok(vec![(
                                                                    object,
                                                                    location
                                                                        .ok_or_else(|| {
                                                                            error!("No object location found");
                                                                            anyhow!(
                                                                    "No object location found"
                                                                )})?
                                                                )]))
                                                                .await
                                                                .map_err(|e| {
                                                                    tracing::error!(error = ?e, msg = e.to_string());
                                                                    e
                                                                })?;
                                                            };
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Message::FinishMessage(_) => {
                                            {
                                                let mut lock = finished_state_clone.lock().await;
                                                *lock = true;
                                            }
                                            object_ack_send.send(AckSync::Finish).await.map_err(
                                                |e| {
                                                    error!(error = ?e, msg = e.to_string());
                                                    e
                                                },
                                            )?;
                                            return Ok(());
                                        }
                                    },
                                    _ => {
                                        error!(
                                            error = "No message provided in PullReplicationRequest"
                                        );
                                        return Err(anyhow!(
                                            "No message provided in PullReplicationRequest"
                                        ));
                                    }
                                }
                            }
                            None => {
                                error!(error = "No message provided in PullReplicationRequest");
                                return Err(anyhow!(
                                    "No message provided in PullReplicationRequest"
                                ));
                            }
                        };
                    }
                    Err(err) => {
                        trace!(?err);
                        return Err(anyhow!(err));
                    }
                }
            }
            // Ok::<(), anyhow::Error>(())
        });

        // Ack sync loop
        tokio::spawn(async move {
            let mut sync_map: HashSet<AckSync> = HashSet::new();
            while let Ok(ref ack_msg) = object_ack_rcv.recv().await {
                //trace!(?ack_msg);
                match ack_msg {
                    init @ AckSync::ObjectInit(_) => {
                        sync_map.insert(init.clone());
                    }
                    chunk @ AckSync::ObjectChunk(..) => {
                        sync_map.insert(chunk.clone());
                    }
                    AckSync::Finish => {
                        object_sync_send.send(sync_map.clone()).await.map_err(|e| {
                            error!(error = ?e, msg = e.to_string());
                            e
                        })?;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        });

        // Responding loop
        let proxy_replication_service = DataproxyReplicationServiceImpl {
            cache: self.cache.clone(),
            sender: self.sender.clone(),
            backend: self.backend.clone(),
        };

        let pubkey = pk;
        tokio::spawn(async move {
            loop {
                match object_input_rcv.recv().await {
                    // Errors
                    Err(err) => {
                        return if *finished_state_handler.lock().await {
                            // Technically no error, because rcv was closed because sender finished
                            Ok(())
                        } else {
                            trace!(?err);
                            Err(anyhow!("Receiving from closed channel"))
                        };
                    }
                    Ok(Err(err)) => {
                        error!("{err}");
                        // Here probably proper error matching is needed to
                        // separate unauthorized from other errors
                        object_output_send
                            .send(Err(tonic::Status::unauthenticated(
                                "Unauthorized to pull objects",
                            )))
                            .await
                            .map_err(|e| {
                                error!(error = ?e, msg = e.to_string());
                                e
                            })?;
                    }

                    // Objects
                    Ok(Ok(objects)) => {
                        trace!(?objects);
                        // Store access-checked objects
                        let mut stored_objects: HashMap<DieselUlid, usize> = HashMap::default();
                        for (object, location) in objects {
                            if *finished_state_handler.lock().await {
                                trace!("finished called in match statement");
                                return Ok(());
                            }

                            if location.is_temporary {
                                object_output_send
                                    .send(Ok(PullReplicationResponse {
                                        message: Some(pull_replication_response::Message::Skip(
                                            Skip {
                                                object_id: object.id.to_string(),
                                            },
                                        )),
                                    }))
                                    .await
                                    .map_err(|e| {
                                        error!(error = ?e, msg = e.to_string());
                                        e
                                    })?;
                                continue;
                            }

                            trace!(?object, ?location);
                            // Need to keep track when to create an object, and when to only update the location
                            // Get chunk size from blocklist
                            let max_blocks = location.count_blocks();
                            trace!(max_blocks);
                            stored_objects.insert(object.id, max_blocks);
                            // Send ObjectInfo into stream
                            object_output_send
                                .send(Ok(PullReplicationResponse {
                                    message: Some(pull_replication_response::Message::ObjectInfo(
                                        aruna_rust_api::api::dataproxy::services::v2::ObjectInfo {
                                            object_id: object.id.to_string(),
                                            chunks: max_blocks as i64,
                                            compressed_size: location.disk_content_len,
                                            raw_size: location.raw_content_len,
                                            extra: None,
                                        },
                                    )),
                                }))
                                .await
                                .map_err(|e| {
                                    error!(error = ?e, msg = e.to_string());
                                    e
                                })?;

                            // Send data into stream
                            proxy_replication_service
                                .send_object(
                                    object.id.to_string(),
                                    pubkey,
                                    location,
                                    object_output_send.clone(),
                                    retry_rcv.clone(),
                                )
                                .await
                                .map_err(|e| {
                                    error!(error = ?e, msg = e.to_string());
                                    e
                                })?;
                            trace!("Send data into stream");
                        }
                        // Check if any message was unacknowledged
                        if let Ok(ack_msgs) = object_sync_rcv.recv().await {
                            for (id, max_blocks) in stored_objects.iter() {
                                if !ack_msgs.contains(&AckSync::ObjectInit(*id)) {
                                    object_output_send
                                        .send(Err(tonic::Status::not_found(
                                            "Unacknowledged ObjectInit found",
                                        )))
                                        .await
                                        .map_err(|e| {
                                            error!(error = ?e, msg = e.to_string());
                                            e
                                        })?;
                                }
                                let max_blocks = *max_blocks as i64;
                                for chunk in 0..max_blocks {
                                    if !ack_msgs.contains(&AckSync::ObjectChunk(*id, chunk)) {
                                        object_output_send
                                            .send(Err(tonic::Status::not_found(
                                                "Unacknowledged ObjectChunk found",
                                            )))
                                            .await
                                            .map_err(|e| {
                                                error!(error = ?e, msg = e.to_string());
                                                e
                                            })?;
                                    }
                                }
                            }
                        }
                    }
                };
            }
        });

        let grpc_response: tonic::Response<
            ReceiverStream<std::result::Result<PullReplicationResponse, tonic::Status>>,
        > = tonic::Response::new(ReceiverStream::new(object_output_rcv));
        Ok(grpc_response)
    }

    /// PushReplication
    ///
    /// Status: BETA
    ///
    /// Provides the necessary url to init replication
    #[tracing::instrument(level = "trace", skip(self, _request))]
    async fn push_replication(
        &self,
        _request: tonic::Request<PushReplicationRequest>,
    ) -> Result<tonic::Response<PushReplicationResponse>, tonic::Status> {
        // TODO
        // 1. query permissions
        // 2. validate endpoint that tries sending these
        // 3. validate if i need these objects
        // 4. send message to replication handler with DataInfos
        error!("InitReplication not implemented");
        Err(tonic::Status::unimplemented("Currently not implemented"))
    }
}

impl DataproxyReplicationServiceImpl {
    async fn get_endpoint_from_token(
        &self,
        token: &str,
    ) -> Result<(DieselUlid, PubKey), tonic::Status> {
        // 2. check if proxy has permissions to pull everything
        if let Some(auth) = self.cache.auth.read().await.as_ref() {
            // Returns claims.sub as id -> Can return UserIds or DataproxyIds
            // -> UserIds cannot be found in object.endpoints, so this should be safe
            let (dataproxy_id, _, pk) = auth.check_permissions(token).map_err(|_| {
                error!(error = "DataProxy not authenticated");
                tonic::Status::unauthenticated("DataProxy not authenticated")
            })?;

            return Ok((dataproxy_id, pk));
        }
        Err(tonic::Status::unauthenticated(
            "DataProxy not authenticated",
        ))
    }

    async fn check_permissions(
        &self,
        init: InitMessage,
        endpoint_id: DieselUlid,
    ) -> Result<Vec<(Object, ObjectLocation)>, tonic::Status> {
        let ids = init
            .object_ids
            .iter()
            .map(|id| {
                DieselUlid::from_str(id).map_err(|e| {
                    error!(error = ?e, msg = e.to_string());
                    tonic::Status::invalid_argument("Invalid id provided")
                })
            })
            .collect::<tonic::Result<Vec<DieselUlid>, tonic::Status>>()?;

        // 1. get all objects & endpoints from server
        let mut objects = Vec::new();
        let object_endpoint_map = DashMap::new();
        for id in ids {
            if let Ok((object, location)) = self.cache.get_resource_cloned(&id, false).await {
                let location = location.as_ref().ok_or_else(|| {
                    error!("No location found for object");
                    tonic::Status::not_found("No location found for object")
                })?;
                objects.push((object.clone(), location.clone()));
                object_endpoint_map.insert(object.id, object.endpoints.clone());
            }
        }

        if !object_endpoint_map.iter().all(|map| {
            let (_, eps) = map.pair();
            eps.iter().any(|ep| ep.id == endpoint_id)
        }) {
            error!("Unauthorized DataProxy request");
            return Err(tonic::Status::unauthenticated(
                "DataProxy is not allowed to access requested objects",
            ));
        };
        Ok(objects)
    }

    async fn send_object(
        &self,
        object_id: String,
        pubkey: [u8; 32],
        location: ObjectLocation,
        sender: tokio::sync::mpsc::Sender<Result<PullReplicationResponse, tonic::Status>>,
        error_rcv: Receiver<Option<(i64, String)>>, // contains chunk_idx and object_id
    ) -> Result<()> {
        // Create channel for get_object
        let (object_sender, object_receiver) = async_channel::bounded(255);

        trace!(location=?location, "send object location");
        let footer = if location.is_pithos() {
            Some(self.get_footer(location.clone()).await?)
        } else {
            None
        };

        let location_clone = location.clone();

        // Spawn get_object
        let backend = self.backend.clone();
        tokio::spawn(
            async move {
                backend
                    .get_object(location_clone, None, object_sender)
                    .await
                    .map_err(|e| {
                        error!(error = ?e, msg = e.to_string());
                        e
                    })
            }
            .instrument(info_span!("get_object")),
        );

        // Spawn final part
        let _ = tokio::spawn(
            async move {
                pin!(object_receiver);
                let mut asrw = GenericStreamReadWriter::new_with_sink(
                    // Receive get_object
                    object_receiver,
                    // ReplicationSink sends into stream via sender
                    ReplicationSink::new(
                        object_id,
                        location.count_blocks(),
                        sender.clone(),
                        error_rcv,
                    ),
                );

                if let Some(key) = location.get_encryption_key() {
                    // Add decryption transformer
                    if !location.is_pithos() {
                        asrw = asrw.add_transformer(ChaCha20Dec::new_with_fixed(key).map_err(
                            |e| {
                                error!(error = ?e, msg = e.to_string());
                                e
                            },
                        )?);
                    }
                }

                if location.is_compressed() && !location.is_pithos() {
                    // Add decompression transformer
                    asrw = asrw.add_transformer(ZstdDec::new());
                }

                if let Some(footer) = footer {
                    // Add footer transformer
                    asrw = asrw.add_transformer(FooterUpdater::new(vec![pubkey], footer));
                } else {
                    asrw = asrw.add_transformer(FooterGenerator::new(None));
                }
                // Add decryption transformer
                asrw.process().await.map_err(|e| {
                    error!(error = ?e, msg = e.to_string());
                    e
                })?;

                Ok::<(), anyhow::Error>(())
            }
            .instrument(info_span!("query_data")),
        )
        .await
        .map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;

        Ok(())
    }

    async fn get_footer(&self, location: ObjectLocation) -> Result<Footer, anyhow::Error> {
        let (footer_sender, footer_receiver) = async_channel::bounded(100);
        self.backend
            .get_object(
                location.clone(),
                Some("bytes=-131072".to_string()),
                footer_sender,
            )
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;

        let mut buf = BytesMut::new();
        while let Ok(Ok(bytes)) = footer_receiver.try_recv() {
            buf.put(bytes)
        }
        let readers_priv_key = CONFIG.proxy.clone().get_private_key_x25519()?;

        let parser: Footer = FooterParser::new(&buf)?
            .add_recipient(&readers_priv_key)
            .parse()?
            .try_into()?;
        Ok(parser)
    }
}
