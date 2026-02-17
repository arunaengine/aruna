use std::collections::HashMap;
use std::sync::Arc;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::id::NodeId;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use iroh::Endpoint;
use iroh::endpoint::{RecvStream, SendStream};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::constants::{MAX_MESSAGE_SIZE, RPC_TIMEOUT};
use super::protocol::{DhtIo, DhtIoError, DhtIoRequest, InboundId};
use super::rpc::{
    DHT_ALPN, DhtRequest, DhtResponse, decode_request, decode_response, encode_request,
    encode_response,
};
use super::storage::{CLEANUP_PAGE_SIZE, DHT_KEYSPACE, decode_entries, encode_entries};

pub type InboundDhtStream = (SendStream, RecvStream, NodeId);

#[derive(Debug)]
pub struct DhtIoDispatcher {
    endpoint: Endpoint,
    storage: StorageHandle,
    request_rx: mpsc::Receiver<DhtIoRequest>,
    inbound_rx: mpsc::Receiver<InboundDhtStream>,
    io_tx: mpsc::Sender<DhtIo>,
    shutdown: CancellationToken,
    inbound_contexts: Arc<Mutex<HashMap<InboundId, SendStream>>>,
    next_inbound_id: InboundId,
}

impl DhtIoDispatcher {
    pub fn new(
        endpoint: Endpoint,
        storage: StorageHandle,
        request_rx: mpsc::Receiver<DhtIoRequest>,
        inbound_rx: mpsc::Receiver<InboundDhtStream>,
        io_tx: mpsc::Sender<DhtIo>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            endpoint,
            storage,
            request_rx,
            inbound_rx,
            io_tx,
            shutdown,
            inbound_contexts: Arc::new(Mutex::new(HashMap::new())),
            next_inbound_id: 1,
        }
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => break,
                maybe_req = self.request_rx.recv() => {
                    let Some(req) = maybe_req else { break; };
                    self.handle_request(req).await;
                }
                maybe_inbound = self.inbound_rx.recv() => {
                    let Some(inbound) = maybe_inbound else { break; };
                    self.handle_inbound_stream(inbound).await;
                }
            }
        }

        let _ = self.io_tx.try_send(DhtIo::DispatcherClosed);
    }

    async fn handle_inbound_stream(&mut self, inbound: InboundDhtStream) {
        let (send, mut recv, peer) = inbound;
        let inbound_id = self.next_inbound_id;
        self.next_inbound_id = self.next_inbound_id.saturating_add(1);

        self.inbound_contexts.lock().await.insert(inbound_id, send);
        let _ = self.io_tx.try_send(DhtIo::PeerSeen { peer });

        let io_tx = self.io_tx.clone();
        let contexts = self.inbound_contexts.clone();
        tokio::spawn(async move {
            match read_request_from_stream(&mut recv).await {
                Ok(request) => {
                    if io_tx
                        .send(DhtIo::InboundRequest {
                            inbound_id,
                            peer,
                            request,
                        })
                        .await
                        .is_err()
                    {
                        let _ = contexts.lock().await.remove(&inbound_id);
                    }
                }
                Err(_) => {
                    let _ = contexts.lock().await.remove(&inbound_id);
                    let _ = io_tx.send(DhtIo::InboundDropped { inbound_id }).await;
                }
            }
        });
    }

    async fn handle_request(&mut self, request: DhtIoRequest) {
        match request {
            DhtIoRequest::RpcRequest {
                request_id,
                peer,
                request,
                ..
            } => {
                let endpoint = self.endpoint.clone();
                let io_tx = self.io_tx.clone();
                tokio::spawn(async move {
                    match rpc_request(endpoint, peer, request).await {
                        Ok(response) => {
                            let _ = io_tx
                                .send(DhtIo::RpcResponse {
                                    request_id,
                                    peer,
                                    response,
                                })
                                .await;
                        }
                        Err(error) => {
                            let _ = io_tx
                                .send(DhtIo::RpcError {
                                    request_id,
                                    peer,
                                    error,
                                })
                                .await;
                        }
                    }
                });
            }
            DhtIoRequest::RpcResponse {
                inbound_id,
                response,
            } => {
                let maybe_send = self.inbound_contexts.lock().await.remove(&inbound_id);
                let io_tx = self.io_tx.clone();
                tokio::spawn(async move {
                    if let Some(mut send) = maybe_send {
                        let _ = write_response_to_stream(&mut send, &response).await;
                    }
                    let _ = io_tx.send(DhtIo::InboundDropped { inbound_id }).await;
                });
            }
            DhtIoRequest::DropInbound { inbound_id } => {
                let _ = self.inbound_contexts.lock().await.remove(&inbound_id);
                let _ = self.io_tx.try_send(DhtIo::InboundDropped { inbound_id });
            }
            DhtIoRequest::StorageRead { storage_id, key } => {
                let storage = self.storage.clone();
                let io_tx = self.io_tx.clone();
                tokio::spawn(async move {
                    let effect = Effect::Storage(StorageEffect::Read {
                        key_space: DHT_KEYSPACE.to_string(),
                        key: ByteView::from(key.as_bytes().as_slice()),
                        txn_id: None,
                    });

                    match storage.send_effect(effect).await {
                        Event::Storage(StorageEvent::ReadResult {
                            value: Some(data), ..
                        }) => {
                            let _ = io_tx
                                .send(DhtIo::StorageReadResult {
                                    storage_id,
                                    entries: decode_entries(&data),
                                })
                                .await;
                        }
                        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
                            let _ = io_tx
                                .send(DhtIo::StorageReadResult {
                                    storage_id,
                                    entries: Vec::new(),
                                })
                                .await;
                        }
                        Event::Storage(StorageEvent::Error { error }) => {
                            let _ = io_tx
                                .send(DhtIo::StorageError {
                                    storage_id,
                                    error: DhtIoError::Storage(error.to_string()),
                                })
                                .await;
                        }
                        other => {
                            let _ = io_tx
                                .send(DhtIo::StorageError {
                                    storage_id,
                                    error: DhtIoError::Storage(format!(
                                        "unexpected storage read event: {other:?}"
                                    )),
                                })
                                .await;
                        }
                    }
                });
            }
            DhtIoRequest::StorageWrite {
                storage_id,
                key,
                entries,
            } => {
                let storage = self.storage.clone();
                let io_tx = self.io_tx.clone();
                tokio::spawn(async move {
                    let Some(bytes) = encode_entries(&entries) else {
                        let _ = io_tx
                            .send(DhtIo::StorageError {
                                storage_id,
                                error: DhtIoError::Storage(
                                    "serialize dht entries failed".to_string(),
                                ),
                            })
                            .await;
                        return;
                    };

                    let effect = Effect::Storage(StorageEffect::Write {
                        key_space: DHT_KEYSPACE.to_string(),
                        key: ByteView::from(key.as_bytes().as_slice()),
                        value: ByteView::from(bytes),
                        txn_id: None,
                    });

                    match storage.send_effect(effect).await {
                        Event::Storage(StorageEvent::WriteResult { .. }) => {
                            let _ = io_tx.send(DhtIo::StorageWriteResult { storage_id }).await;
                        }
                        Event::Storage(StorageEvent::Error { error }) => {
                            let _ = io_tx
                                .send(DhtIo::StorageError {
                                    storage_id,
                                    error: DhtIoError::Storage(error.to_string()),
                                })
                                .await;
                        }
                        other => {
                            let _ = io_tx
                                .send(DhtIo::StorageError {
                                    storage_id,
                                    error: DhtIoError::Storage(format!(
                                        "unexpected storage write event: {other:?}"
                                    )),
                                })
                                .await;
                        }
                    }
                });
            }
            DhtIoRequest::StorageDelete { storage_id, key } => {
                let storage = self.storage.clone();
                let io_tx = self.io_tx.clone();
                tokio::spawn(async move {
                    let effect = Effect::Storage(StorageEffect::Delete {
                        key_space: DHT_KEYSPACE.to_string(),
                        key: ByteView::from(key.as_bytes().as_slice()),
                        txn_id: None,
                    });

                    match storage.send_effect(effect).await {
                        Event::Storage(StorageEvent::DeleteResult { .. }) => {
                            let _ = io_tx.send(DhtIo::StorageDeleteResult { storage_id }).await;
                        }
                        Event::Storage(StorageEvent::Error { error }) => {
                            let _ = io_tx
                                .send(DhtIo::StorageError {
                                    storage_id,
                                    error: DhtIoError::Storage(error.to_string()),
                                })
                                .await;
                        }
                        other => {
                            let _ = io_tx
                                .send(DhtIo::StorageError {
                                    storage_id,
                                    error: DhtIoError::Storage(format!(
                                        "unexpected storage delete event: {other:?}"
                                    )),
                                })
                                .await;
                        }
                    }
                });
            }
            DhtIoRequest::StorageIter {
                storage_id,
                start_after,
                limit,
            } => {
                let storage = self.storage.clone();
                let io_tx = self.io_tx.clone();
                tokio::spawn(async move {
                    let effect = Effect::Storage(StorageEffect::Iter {
                        key_space: DHT_KEYSPACE.to_string(),
                        prefix: None,
                        start_after: start_after.map(ByteView::from),
                        limit: if limit == 0 { CLEANUP_PAGE_SIZE } else { limit },
                        txn_id: None,
                    });

                    match storage.send_effect(effect).await {
                        Event::Storage(StorageEvent::IterResult {
                            values,
                            next_start_after,
                        }) => {
                            let decoded_values = values
                                .into_iter()
                                .map(|(key, value)| (key.as_ref().to_vec(), decode_entries(&value)))
                                .collect();

                            let _ = io_tx
                                .send(DhtIo::StorageIterResult {
                                    storage_id,
                                    values: decoded_values,
                                    next_start_after: next_start_after.map(|k| k.as_ref().to_vec()),
                                })
                                .await;
                        }
                        Event::Storage(StorageEvent::Error { error }) => {
                            let _ = io_tx
                                .send(DhtIo::StorageError {
                                    storage_id,
                                    error: DhtIoError::Storage(error.to_string()),
                                })
                                .await;
                        }
                        other => {
                            let _ = io_tx
                                .send(DhtIo::StorageError {
                                    storage_id,
                                    error: DhtIoError::Storage(format!(
                                        "unexpected storage iter event: {other:?}"
                                    )),
                                })
                                .await;
                        }
                    }
                });
            }
        }
    }
}

async fn rpc_request(
    endpoint: Endpoint,
    peer: NodeId,
    request: DhtRequest,
) -> Result<DhtResponse, DhtIoError> {
    let conn = tokio::time::timeout(RPC_TIMEOUT, endpoint.connect(peer, DHT_ALPN))
        .await
        .map_err(|_| DhtIoError::Timeout)?
        .map_err(|err| DhtIoError::Network(err.to_string()))?;

    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|err| DhtIoError::Network(err.to_string()))?;

    let request_bytes = encode_request(&request)
        .map_err(|err| DhtIoError::InvalidResponse(format!("encode request failed: {err}")))?;
    let len = (request_bytes.len() as u32).to_be_bytes();
    send.write_all(&len)
        .await
        .map_err(|err| DhtIoError::Network(err.to_string()))?;
    send.write_all(&request_bytes)
        .await
        .map_err(|err| DhtIoError::Network(err.to_string()))?;
    send.finish()
        .map_err(|err| DhtIoError::Network(err.to_string()))?;

    let mut len_buf = [0u8; 4];
    tokio::time::timeout(RPC_TIMEOUT, recv.read_exact(&mut len_buf))
        .await
        .map_err(|_| DhtIoError::Timeout)?
        .map_err(|err| DhtIoError::Network(err.to_string()))?;

    let response_len = u32::from_be_bytes(len_buf) as usize;
    if response_len > MAX_MESSAGE_SIZE {
        return Err(DhtIoError::InvalidResponse(
            "response too large".to_string(),
        ));
    }

    let mut response_bytes = vec![0u8; response_len];
    tokio::time::timeout(RPC_TIMEOUT, recv.read_exact(&mut response_bytes))
        .await
        .map_err(|_| DhtIoError::Timeout)?
        .map_err(|err| DhtIoError::Network(err.to_string()))?;

    decode_response(&response_bytes)
        .map_err(|err| DhtIoError::InvalidResponse(format!("decode response failed: {err}")))
}

async fn read_request_from_stream(recv: &mut RecvStream) -> Result<DhtRequest, DhtIoError> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|err| DhtIoError::Network(err.to_string()))?;

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_MESSAGE_SIZE {
        return Err(DhtIoError::InvalidResponse("request too large".to_string()));
    }

    let mut req_bytes = vec![0u8; len];
    recv.read_exact(&mut req_bytes)
        .await
        .map_err(|err| DhtIoError::Network(err.to_string()))?;

    decode_request(&req_bytes)
        .map_err(|err| DhtIoError::InvalidResponse(format!("decode request failed: {err}")))
}

async fn write_response_to_stream(
    send: &mut SendStream,
    response: &DhtResponse,
) -> Result<(), DhtIoError> {
    let response_bytes = encode_response(response)
        .map_err(|err| DhtIoError::InvalidResponse(format!("encode response failed: {err}")))?;

    if response_bytes.len() > MAX_MESSAGE_SIZE {
        return Err(DhtIoError::InvalidResponse(
            "response too large".to_string(),
        ));
    }

    let len = (response_bytes.len() as u32).to_be_bytes();
    send.write_all(&len)
        .await
        .map_err(|err| DhtIoError::Network(err.to_string()))?;
    send.write_all(&response_bytes)
        .await
        .map_err(|err| DhtIoError::Network(err.to_string()))?;
    send.finish()
        .map_err(|err| DhtIoError::Network(err.to_string()))?;

    Ok(())
}
