use crate::CONFIG;
use crate::{caching::cache::Cache, data_backends::storage_backend::StorageBackend};
use ahash::RandomState;
use anyhow::{anyhow, Result};
use async_channel::{Receiver, Sender};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use std::sync::Arc;
use tracing::trace;

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
pub(super) struct ObjectState {
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
}
