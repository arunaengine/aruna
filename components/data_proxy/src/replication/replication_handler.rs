use crate::structs::{FileFormat, VersionVariant};
use crate::CONFIG;
use crate::{
    caching::cache::Cache, data_backends::storage_backend::StorageBackend,
    s3_frontend::utils::buffered_s3_sink::BufferedS3Sink, structs::ObjectLocation,
};
use ahash::{HashSet, RandomState};
use anyhow::{anyhow, Result};
use aruna_rust_api::api::dataproxy::services::v2::{Empty, ObjectInfo, ReplicationStatus, Skip};
use aruna_rust_api::api::{
    dataproxy::services::v2::{
        error_message, pull_replication_request::Message,
        pull_replication_response::Message as ResponseMessage, Chunk, ChunkAckMessage,
        InfoAckMessage, InitMessage, PullReplicationRequest, RetryChunkMessage,
    },
    storage::services::v2::UpdateReplicationStatusRequest,
};
use async_channel::{Receiver, Sender};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use md5::{Digest, Md5};
use pithos_lib::transformers::footer_extractor::FooterExtractor;
use pithos_lib::{streamreadwrite::GenericStreamReadWriter, transformer::ReadWriter};
use std::{str::FromStr, sync::Arc};
use tokio::pin;
use tokio::sync::RwLock;
use tracing::{error, info_span, trace, Instrument};

pub struct ReplicationMessage {
    pub direction: Direction,
    pub endpoint_id: DieselUlid,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub enum Direction {
    #[allow(dead_code)]
    Push(DieselUlid),
    Pull(DieselUlid),
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub enum RcvSync {
    Info(DieselUlid, i64),  // object_id and how many chunks
    Chunk(DieselUlid, i64), // object_id and which chunk
    Finish,
}
pub struct DataChunk {
    pub object_id: String,
    pub chunk_idx: i64,
    pub data: Vec<u8>,
    pub checksum: String,
}

pub struct ReplicationHandler {
    pub receiver: Receiver<ReplicationMessage>,
    pub backend: Arc<Box<dyn StorageBackend>>,
    pub cache: Arc<Cache>,
    pub self_id: String,
}

#[derive(Clone, Debug)]
struct ObjectState {
    sender: Sender<DataChunk>,
    receiver: Receiver<DataChunk>,
    state: ObjectStateStatus,
}

#[derive(Clone, Debug)]
pub enum ObjectStateStatus {
    NotReceived,
    Infos { max_chunks: i64, size: i64 },
}

impl ObjectState {
    pub fn new(sender: Sender<DataChunk>, receiver: Receiver<DataChunk>) -> Self {
        ObjectState {
            sender,
            receiver,
            state: ObjectStateStatus::NotReceived,
        }
    }

    pub fn update_state(&mut self, max_chunks: i64, size: i64) {
        self.state = ObjectStateStatus::Infos { max_chunks, size };
    }

    pub fn is_synced(&self) -> bool {
        !matches! {self.state, ObjectStateStatus::NotReceived}
    }

    pub fn get_size(&self) -> Option<i64> {
        if let ObjectStateStatus::Infos { size, .. } = self.state {
            Some(size)
        } else {
            None
        }
    }

    pub fn get_rcv(&self) -> Receiver<DataChunk> {
        self.receiver.clone()
    }

    pub fn get_chunks(&self) -> Result<i64> {
        if let ObjectStateStatus::Infos { max_chunks, .. } = self.state {
            if max_chunks > 0 {
                Ok(max_chunks)
            } else {
                Err(anyhow!("Invalid max chunks received"))
            }
        } else {
            Err(anyhow!("Not synced"))
        }
    }

    pub fn get_sdx(&self) -> Sender<DataChunk> {
        self.sender.clone()
    }
}

type ObjectHandler = Arc<DashMap<String, Arc<RwLock<ObjectState>>, RandomState>>;
impl ReplicationHandler {
    #[tracing::instrument(level = "trace", skip(cache, backend, receiver))]
    pub fn new(
        receiver: Receiver<ReplicationMessage>,
        backend: Arc<Box<dyn StorageBackend>>,
        self_id: String,
        cache: Arc<Cache>,
    ) -> Self {
        Self {
            receiver,
            backend,
            self_id,
            cache,
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn run(self) -> Result<()> {
        // Has EndpointID: [Pull(object_id), Pull(object_id) ,...]
        let queue: Arc<DashMap<DieselUlid, Vec<Direction>, RandomState>> =
            Arc::new(DashMap::default());

        // Push messages into DashMap for further processing
        let queue_clone = queue.clone();
        let receiver = self.receiver.clone();
        let receive = tokio::spawn(async move {
            while let Ok(ReplicationMessage {
                direction,
                endpoint_id,
            }) = receiver.recv().await
            {
                if queue_clone.contains_key(&endpoint_id) {
                    queue_clone.alter(&endpoint_id, |_, mut objects| {
                        objects.push(direction.clone());
                        objects
                    });
                } else {
                    queue_clone.insert(endpoint_id, vec![direction.clone()]);
                }
                trace!(?queue_clone);
            }
        });

        let batch_processing_interval =
            std::time::Duration::from_secs(CONFIG.proxy.replication_interval.unwrap_or(30));
        trace!(?batch_processing_interval);
        // Process DashMap entries in batches
        let process: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
            loop {
                // Process batches every 30 seconds
                tokio::time::sleep(batch_processing_interval).await;
                let batch = queue.clone();

                let result = match self.process(batch).await {
                    Ok(res) => res,
                    Err(err) => {
                        tracing::error!(error = ?err, msg = err.to_string());
                        continue;
                    }
                };
                // Remove processed entries from shared map
                for (id, objects) in result {
                    queue.alter(&id, |_, directions| {
                        directions
                            .into_iter()
                            .filter(|dir| !objects.contains(dir))
                            .collect::<Vec<Direction>>()
                            .clone()
                    });
                    let mut is_empty = false;
                    if let Some(entry) = queue.get(&id) {
                        if entry.is_empty() {
                            is_empty = true;
                        }
                    }
                    if is_empty {
                        queue.remove(&id);
                    }
                }
            }
        });
        // Run both tasks simultaneously
        let (_, result) = tokio::try_join!(receive, process).map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;
        result?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    // TODO
    // - Push logic
    async fn process(
        &self,
        batch: Arc<DashMap<DieselUlid, Vec<Direction>, RandomState>>,
    ) -> Result<Vec<(DieselUlid, Vec<Direction>)>> {
        // Vec for collecting all processed and finished endpoint batches
        let mut result = Vec::new();

        // Iterates over each endpoint
        for endpoint in batch.iter() {
            let self_id = self.self_id.clone();
            // Collects all objects for each direction
            let pull: Vec<DieselUlid> = endpoint
                .iter()
                .filter_map(|object| match object {
                    Direction::Pull(id) => Some(*id),
                    Direction::Push(_) => None,
                })
                .collect();
            // TODO: Push is currently not implemented
            let _push: Vec<DieselUlid> = endpoint
                .iter()
                .filter_map(|object| match object {
                    Direction::Push(id) => Some(*id),
                    Direction::Pull(_) => None,
                })
                .collect();
            // This is the initial message for the data transmission stream
            let init_request = PullReplicationRequest {
                message: Some(Message::InitMessage(InitMessage {
                    dataproxy_id: self_id.clone(),
                    object_ids: pull.iter().map(|o| o.to_string()).collect(),
                })),
            };
            if let Some(query_handler) = self.cache.aruna_client.read().await.as_ref() {
                let endpoint_id = *endpoint.key();
                // This query handler returns a channel for sending messages into the input stream
                // and the response stream
                let (request_sender, mut response_stream) = query_handler
                    .pull_replication(init_request, endpoint_id)
                    .await
                    .map_err(|e| {
                        tracing::error!(error = ?e, msg = e.to_string());
                        e
                    })?;

                // This is the init message for object processing
                let (start_sender, start_receiver) = async_channel::bounded(1);
                // This channel is used to collect all processed objects and chunks
                let (sync_sender, sync_receiver) = async_channel::bounded(100);
                // This channel is only used to transmit the sync result to compare
                // received vs requested objects
                let (finish_sender, finish_receiver) = async_channel::bounded(1);

                // This map collects for each object_id a channel for data transmission
                // TODO: This could be used to make parallel requests later
                let object_handler_map: ObjectHandler = Arc::new(DashMap::default());
                for object in pull {
                    query_handler
                        .update_replication_status(UpdateReplicationStatusRequest {
                            object_id: object.to_string(),
                            endpoint_id: self_id.clone(),
                            status: ReplicationStatus::Running as i32,
                        })
                        .await
                        .map_err(|e| {
                            tracing::error!(error = ?e, msg = e.to_string());
                            e
                        })?;
                    let (object_sdx, object_rcv) = async_channel::bounded(100);
                    object_handler_map.insert(
                        object.to_string(),
                        Arc::new(RwLock::new(ObjectState::new(
                            object_sdx.clone(),
                            object_rcv.clone(),
                        ))),
                    );
                }

                trace!(?object_handler_map);
                // Response handler:
                // This is used to handle all requests and responses
                // to the other data proxy
                let data_map = object_handler_map.clone();
                let sync_sender_clone = sync_sender.clone();
                let request_sender_clone = request_sender.clone();
                tokio::spawn(async move {
                    let mut counter = 0;
                    while let Some(response) = response_stream.message().await? {
                        match response.message {
                            Some(ResponseMessage::Handshake(_)) => {
                                continue;
                            }
                            Some(ResponseMessage::Skip(Skip { object_id })) => {
                                // As long as servers are sending skip before any object info this should be safe
                                data_map.remove(&object_id);
                                if data_map.is_empty() {
                                    // send finish, if no object was processed
                                    sync_sender_clone.send(RcvSync::Finish).await.map_err(|e| {
                                        tracing::error!(error = ?e, msg = e.to_string());
                                        e
                                    })?;
                                    break;
                                }
                            }
                            Some(ResponseMessage::ObjectInfo(ObjectInfo {
                                object_id,
                                chunks,
                                raw_size,
                                ..
                            })) => {
                                counter += 1;
                                trace!(object_id, chunks, raw_size);
                                // If ObjectInfo is sent, an init msg is collected in sync ...
                                let id = DieselUlid::from_str(&object_id).inspect_err(|&e| {
                                    tracing::error!(error = ?e, msg = e.to_string());
                                })?;
                                if let Some(entry) = data_map.get(&object_id) {
                                    let mut guard = entry.write().await;
                                    guard.update_state(chunks, raw_size);
                                } else {
                                    // If no entry is found, abort sync
                                    request_sender_clone
                                        .send(
                                            PullReplicationRequest {
                                                message: Some(
                                                    Message::ErrorMessage(
                                                        aruna_rust_api::api::dataproxy::services::v2::ErrorMessage {
                                                            error: Some(
                                                                error_message::Error::Abort(Empty{})
                                                            )
                                                        }
                                                    )
                                                )
                                            }
                                        )
                                        .await.map_err(|e| {
                                        tracing::error!(error = ?e, msg = e.to_string());
                                        e
                                    })?;
                                }
                                sync_sender_clone
                                    .send(RcvSync::Info(id, chunks))
                                    .await
                                    .map_err(|e| {
                                        tracing::error!(error = ?e, msg = e.to_string());
                                        e
                                    })?;
                                // ... and then ObjectInfo gets acknowledged
                                request_sender_clone
                                    .send(PullReplicationRequest {
                                        message: Some(Message::InfoAckMessage(InfoAckMessage {
                                            object_id,
                                        })),
                                    })
                                    .await
                                    .map_err(|e| {
                                        tracing::error!(error = ?e, msg = e.to_string());
                                        e
                                    })?;
                                // This is needed to keep backend task in sync
                                if counter == 1 {
                                    start_sender.send(true).await.inspect_err(|&e| {
                                        tracing::error!(error = ?e, msg = e.to_string());
                                    })?;
                                }
                            }
                            Some(ResponseMessage::Chunk(Chunk {
                                object_id,
                                chunk_idx,
                                data,
                                checksum,
                            })) => {
                                // If an entry is created inside the object_handler_map ...
                                if let Some(entry) = data_map.get(&object_id) {
                                    let sender = entry.read().await.get_sdx();
                                    // Chunks get processed
                                    let chunk = DataChunk {
                                        object_id: object_id.clone(),
                                        chunk_idx,
                                        data,
                                        checksum,
                                    };
                                    sender.send(chunk).await?;
                                    let id = DieselUlid::from_str(&object_id)?;
                                    // Message is send to sync
                                    sync_sender_clone
                                        .send(RcvSync::Chunk(id, chunk_idx))
                                        .await
                                        .map_err(|e| {
                                            tracing::error!(error = ?e, msg = e.to_string());
                                            e
                                        })?;
                                    // Message is acknowledged
                                    request_sender_clone
                                        .send(PullReplicationRequest {
                                            message: Some(Message::ChunkAckMessage(
                                                ChunkAckMessage {
                                                    object_id,
                                                    chunk_idx,
                                                },
                                            )),
                                        })
                                        .await
                                        .map_err(|e| {
                                            tracing::error!(error = ?e, msg = e.to_string());
                                            e
                                        })?;
                                } else {
                                    // If no entry is found, ObjectInfo was not send
                                    request_sender_clone
                                        .send(
                                            PullReplicationRequest {
                                                message: Some(
                                                    Message::ErrorMessage(
                                                        aruna_rust_api::api::dataproxy::services::v2::ErrorMessage {
                                                            error: Some(
                                                                error_message::Error::RetryObjectId(
                                                                    object_id,
                                                                )
                                                            )
                                                        }
                                                    )
                                                )
                                            }
                                        )
                                        .await.map_err(|e| {
                                            tracing::error!(error = ?e, msg = e.to_string());
                                            e
                                        })?;
                                }
                            }
                            Some(ResponseMessage::FinishMessage(..)) => return Ok(()),
                            None => {
                                return Err(anyhow!(
                                    "No message provided in PullReplicationResponse"
                                ))
                            }
                        }
                    }
                    // Ok::<(), anyhow::Error>(())
                    Err(anyhow!("Stream closed without FinishMessage"))
                });

                // Sync handler
                tokio::spawn(async move {
                    let mut sync = HashSet::default();
                    // Every InfoMsg and ChunkMsg is stored
                    while let Ok(msg) = sync_receiver.recv().await {
                        match msg {
                            info @ RcvSync::Info(..) => {
                                sync.insert(info);
                            }
                            chunk @ RcvSync::Chunk(..) => {
                                sync.insert(chunk);
                            }
                            // If finish is called, all stored messages will be returned
                            RcvSync::Finish => {
                                finish_sender.send(sync.clone()).await.map_err(|e| {
                                    tracing::error!(error = ?e, msg = e.to_string());
                                    e
                                })?;
                            }
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                });

                // Process each object
                let cache = self.cache.clone();
                let backend = self.backend.clone();
                let query_handler = query_handler.clone();
                let request_sdx = request_sender.clone();
                let finished_objects: Arc<DashMap<Direction, bool, RandomState>> =
                    Arc::new(DashMap::default()); // Syncs if object is already synced
                let finished_clone = finished_objects.clone();
                tokio::spawn(async move {
                    // For now, every entry of the object_handler_map is processed
                    // consecutively
                    while start_receiver.recv().await.is_ok() {
                        let mut batch_counter = 0;
                        loop {
                            batch_counter += 1;
                            let mut batch = Vec::new();
                            for entry in object_handler_map.iter() {
                                let (key, value) = entry.pair();
                                batch.push((key.clone(), value.clone()));
                            }
                            for (id, object_state) in batch.iter() {
                                trace!("processing: {}", id);
                                let object_id = DieselUlid::from_str(id)?;

                                // The object gets queried
                                let (object, location) =
                                    cache.get_resource_cloned(&object_id, false).await?;
                                trace!(?object);

                                let mut location = if location.is_some() {
                                    finished_clone.insert(Direction::Pull(object_id), true);
                                    object_handler_map.remove(id);
                                    continue;
                                } else if !object_state.read().await.is_synced() {
                                    trace!("skipping object");
                                    continue;
                                } else {
                                    let parent = {
                                        let mut current_version_id = object_id;
                                        if let Some(version) = &object.versions {
                                            version.iter().next().map(|v| {
                                                if let VersionVariant::IsVersion(id) = v {
                                                    current_version_id = *id
                                                }
                                            });
                                        }
                                        cache.get_single_parent(&current_version_id).await.map_err(
                                            |e| {
                                                error!(error = ?e, msg = e.to_string());
                                                e
                                            },
                                        )?
                                    };
                                    backend
                                        .initialize_location(
                                            &object,
                                            object_state.read().await.get_size(),
                                            parent,
                                            false,
                                        )
                                        .await
                                        .map_err(|e| {
                                            tracing::error!(error = ?e, msg = e.to_string());
                                            e
                                        })?
                                };
                                trace!("Load into backend");
                                // Send Chunks get processed
                                ReplicationHandler::load_into_backend(
                                    object_state.read().await.get_rcv(),
                                    request_sdx.clone(),
                                    sync_sender.clone(),
                                    &mut location,
                                    backend.clone(),
                                    object_state.read().await.get_chunks()?,
                                )
                                .await
                                .map_err(|e| {
                                    tracing::error!(error = ?e, msg = e.to_string());
                                    e
                                })?;

                                trace!("Upsert object");
                                // TODO: This should probably happen after checking if all chunks were processed
                                // Sync with cache and db
                                cache.upsert_object(object.clone()).await?;

                                cache.add_location_with_binding(object.id, location).await?;

                                trace!("Update status");
                                // Send UpdateStatus to server
                                query_handler
                                    .update_replication_status(UpdateReplicationStatusRequest {
                                        object_id: object.id.to_string(),
                                        endpoint_id: self_id.clone(),
                                        status: ReplicationStatus::Finished as i32,
                                    })
                                    .await
                                    .map_err(|e| {
                                        tracing::error!(error = ?e, msg = e.to_string());
                                        e
                                    })?;
                                {
                                    trace!("before entry remove");
                                    object_handler_map.remove(id);
                                    trace!("after entry remove");
                                }
                                trace!( msg="Removed entry from map", map = ?object_handler_map);
                            }
                            if object_handler_map.is_empty() {
                                trace!("Object handler map is empty, finishing replication... ");
                                // Check if all chunks found in object infos are also processed
                                sync_sender.send(RcvSync::Finish).await.map_err(|e| {
                                    tracing::error!(error = ?e, msg = e.to_string());
                                    e
                                })?;
                                break;
                            } else if batch_counter > 20 {
                                // Exit after arbitrary number of tries
                                request_sdx.send(
                                    PullReplicationRequest {
                                                    message: Some(
                                                        Message::ErrorMessage(
                                                            aruna_rust_api::api::dataproxy::services::v2::ErrorMessage {
                                                                error: Some(
                                                                    error_message::Error::Abort(Empty{})
                                                                )
                                                            }
                                                        )
                                                    )
                                                }
                                        ).await.map_err(|e| {
                                            tracing::error!(error = ?e, msg = e.to_string());
                                            e
                                        })?;
                                sync_sender.send(RcvSync::Finish).await.map_err(|e| {
                                    tracing::error!(error = ?e, msg = e.to_string());
                                    e
                                })?;
                                break;
                            }
                        }
                    }

                    Ok::<(), anyhow::Error>(())
                });

                //TODO:
                // - If error, maybe set endpoint_status for each failed object to Error?
                // -> Then we do not have to do this additional check while loading into backend
                // -> User initiated replications then need to be implemented
                //let mut finished_objects = Vec::new();
                while let Ok(finished) = finish_receiver.recv().await {
                    // Collection ObjectInfo
                    let inits = finished.iter().filter_map(|msg| match msg {
                        RcvSync::Info(object_id, chunks) => Some((object_id, chunks)),
                        _ => None,
                    });
                    // For each object, check if all chunks were processed
                    for (object_id, chunks) in inits {
                        let collected = finished
                            .iter()
                            .filter_map(|msg| match msg {
                                RcvSync::Chunk(id, idx) if object_id == id => Some(idx),
                                _ => None,
                            })
                            .collect::<Vec<_>>()
                            .len();
                        if *chunks as usize != collected {
                            // Look if already synced
                            if let Some(object) = finished_objects.get(&Direction::Pull(*object_id))
                            {
                                let (_, is_synced) = object.pair();
                                if *is_synced {
                                    continue;
                                } else {
                                    trace!("Not all chunks received, aborting ...");
                                    // Send abort message if not all chunks were processed
                                    request_sender
                                            .send(
                                                PullReplicationRequest {
                                                    message: Some(
                                                        Message::ErrorMessage(
                                                            aruna_rust_api::api::dataproxy::services::v2::ErrorMessage {
                                                                error: Some(
                                                                    error_message::Error::Abort(Empty{})
                                                                )
                                                            }
                                                        )
                                                    )
                                                }
                                            )
                                            .await.map_err(|e| {
                                                tracing::error!(error = ?e, msg = e.to_string());
                                                e
                                            })?;
                                    return Err(anyhow!("Not all chunks received, aborting sync"));
                                }
                            } else {
                                trace!("Not all chunks received, aborting ...");
                                // Send abort message if not all chunks were processed
                                request_sender
                                            .send(
                                                PullReplicationRequest {
                                                    message: Some(
                                                        Message::ErrorMessage(
                                                            aruna_rust_api::api::dataproxy::services::v2::ErrorMessage {
                                                                error: Some(
                                                                    error_message::Error::Abort(Empty{})
                                                                )
                                                            }
                                                        )
                                                    )
                                                }
                                            )
                                            .await.map_err(|e| {
                                                tracing::error!(error = ?e, msg = e.to_string());
                                                e
                                            })?;
                                return Err(anyhow!("Not all chunks received, aborting sync"));
                            }
                        }
                        finished_objects.insert(Direction::Pull(*object_id), false);
                    }
                    // Send finish message if everything was processed
                    request_sender
                        .send(PullReplicationRequest {
                            message: Some(Message::FinishMessage(Empty {})),
                        })
                        .await
                        .map_err(|e| {
                            tracing::error!(error = ?e, msg = e.to_string());
                            e
                        })?;
                }
                trace!("Writing results");
                if let Some(map) = Arc::into_inner(finished_objects) {
                    let (objects, _): (Vec<Direction>, Vec<bool>) = map.into_iter().unzip();
                    let finished_objects = Vec::from_iter(objects);
                    result.push((endpoint_id, finished_objects));
                    // It is not that much of a problem if this does not get written, because it
                    // will be skipped when the next batch gets processed by the replication
                    // handler
                };
            };
            // Write endpoint into results
            //result.push(*endpoint.key());
        }

        trace!(?result);
        Ok(result)
    }
    async fn load_into_backend(
        data_receiver: Receiver<DataChunk>,
        stream_sender: tokio::sync::mpsc::Sender<PullReplicationRequest>,
        sync_sender: Sender<RcvSync>,
        location: &mut ObjectLocation,
        backend: Arc<Box<dyn StorageBackend>>,
        max_chunks: i64,
    ) -> Result<()> {
        let mut expected = 0;
        let mut retry_counter = 0;

        trace!("Starting chunk processing");
        let (data_sender, data_stream) = async_channel::bounded(1000);
        tokio::spawn(
            async move {
                while let Ok(data) = data_receiver.recv().await {
                    let _trace_message = format!(
                        "Received chunk with idx {:?} for object with id {:?} and size {}, expected {}, max chunks {}",
                        data.chunk_idx,
                        data.object_id,
                        data.data.len(),
                        expected,
                        max_chunks,
                    );
                    //trace!(trace_message);
                    let chunk = bytes::Bytes::from_iter(data.data.into_iter());
                    // Check if chunk is missing
                    let idx = data.chunk_idx;

                    if idx != expected {
                        if retry_counter > 5 {
                            trace!("Exceeded retries");
                            return Err(anyhow!(
                                "Exceeded retries for chunk because of skipped chunk"
                            ));
                        } else {
                            // TODO:
                            // RetryChunk message
                            trace!("MissingChunk: Retry chunk {}", expected);
                            stream_sender
                            .send(PullReplicationRequest {
                                message: Some(Message::ErrorMessage(
                                    aruna_rust_api::api::dataproxy::services::v2::ErrorMessage {
                                        error: Some(error_message::Error::RetryChunk(
                                            RetryChunkMessage {
                                                object_id: data.object_id,
                                                chunk_idx: expected, // TODO: previous
                                            },
                                        )),
                                    },
                                )),
                            })
                            .await
                            .map_err(|e| {
                                tracing::error!(error = ?e, msg = e.to_string());
                                e
                            })?;
                            retry_counter += 1;
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                            continue;
                        }
                    } else {
                        expected += 1;
                    };

                    // Check checksum of chunk:
                    let hash = data.checksum;
                    // - create a Md5 hasher instance
                    let mut hasher = Md5::new();
                    // - process input message
                    hasher.update(&chunk);
                    // - acquire hash digest in the form of GenericArray,
                    //   which in this case is equivalent to [u8; 16]
                    let result = hasher.finalize();
                    let calculated_hash = hex::encode(result);
                    if calculated_hash != hash {
                        if retry_counter > 5 {
                            trace!("Exceeded retries");
                            return Err(anyhow!(
                                "Exceeded retries for chunk because of differing checksums"
                            ));
                        } else {
                            // TODO:
                            // RetryChunk message
                            trace!("HashError: Retry chunk {}", expected);
                            stream_sender
                            .send(PullReplicationRequest {
                                message: Some(Message::ErrorMessage(
                                    aruna_rust_api::api::dataproxy::services::v2::ErrorMessage {
                                        error: Some(error_message::Error::RetryChunk(
                                            RetryChunkMessage {
                                                object_id: data.object_id,
                                                chunk_idx: data.chunk_idx,
                                            },
                                        )),
                                    },
                                )),
                            })
                            .await
                            .map_err(|e| {
                                tracing::error!(error = ?e, msg = e.to_string());
                                e
                            })?;
                            retry_counter += 1;
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                            continue;
                        }
                    }

                    data_sender.send(Ok(chunk)).await.map_err(|e| {
                        tracing::error!(error = ?e, msg = e.to_string());
                        e
                    })?;

                    // Message is send to sync
                    sync_sender
                        .send(RcvSync::Chunk(
                            DieselUlid::from_str(&data.object_id).inspect_err(|&e| {
                                tracing::error!(error = ?e, msg = e.to_string());
                            })?,
                            data.chunk_idx,
                        ))
                        .await
                        .map_err(|e| {
                            tracing::error!(error = ?e, msg = e.to_string());
                            e
                        })?;
                    if (idx + 1) == max_chunks {
                        return Ok(());
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
            .instrument(info_span!("replication chunk receiver")),
        );

        let location_clone = location.clone();
        pin!(data_stream);
        let mut awr = GenericStreamReadWriter::new_with_sink(
            data_stream,
            BufferedS3Sink::new(
                backend.clone(),
                location_clone.clone(),
                None,
                None,
                false,
                None,
                false,
            )
            .0,
        );

        let (extractor, rx) = FooterExtractor::new(Some(CONFIG.proxy.get_private_key_x25519()?));

        awr = awr.add_transformer(extractor);

        awr.process().await.map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;

        let footer = rx.try_recv().inspect_err(|&e| {
            tracing::error!(error = ?e, msg = e.to_string());
        })?;
        if let Some(keys) = footer.encryption_keys {
            if let Some((key, _)) = keys.keys.first() {
                location.file_format = FileFormat::Pithos(*key);
            } else {
                return Err(anyhow!("Unable to extract key"));
            }
        } else {
            return Err(anyhow!("Unable to extract keys"));
        };

        // TODO:
        // Fetch calculated hashes

        // Put infos into location
        location.disk_content_len = footer.eof_metadata.disk_file_size as i64;
        location.disk_hash = Some(hex::encode(footer.eof_metadata.disk_hash_sha256));
        trace!(location = ?location);

        Ok(())
    }
}
