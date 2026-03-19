use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::alpn::Alpn;
use aruna_core::automerge::{
    AutomergeEffect, AutomergeEvent, AutomergeInit, AutomergeRejectReason, AutomergeSyncError,
    AutomergeSyncFeature,
};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use async_trait::async_trait;
use automerge::AutoCommit;
use automerge::sync::{self, SyncDoc};
use tokio::sync::Mutex;
use ulid::Ulid;

use super::protocol::{AutomergeTransportMessage, read_message, write_message};

const SYNC_IO_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_SYNC_ROUNDS: usize = 256;

#[derive(Clone)]
pub struct AutomergeHandle {
    inner: Arc<AutomergeInner>,
}

struct AutomergeInner {
    net_handle: Option<NetHandle>,
    active_syncs: Mutex<HashMap<Ulid, ActiveSync>>,
}

struct ActiveSync {
    peer: aruna_core::NodeId,
    stream: BiStream,
    direction: SyncDirection,
    remote_init: Option<AutomergeInit>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SyncDirection {
    Inbound,
    Outbound,
}

impl AutomergeHandle {
    pub fn new(net_handle: Option<NetHandle>) -> Self {
        Self {
            inner: Arc::new(AutomergeInner {
                net_handle,
                active_syncs: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub async fn register_inbound_stream(
        &self,
        stream: BiStream,
        peer: aruna_core::NodeId,
    ) -> Ulid {
        let sync_id = Ulid::new();
        let sync = ActiveSync {
            peer,
            stream,
            direction: SyncDirection::Inbound,
            remote_init: None,
        };
        self.store_active_sync(sync_id, sync).await;
        sync_id
    }

    async fn store_active_sync(&self, sync_id: Ulid, sync: ActiveSync) {
        self.inner.active_syncs.lock().await.insert(sync_id, sync);
    }

    async fn take_active_sync(&self, sync_id: Ulid) -> Result<ActiveSync, AutomergeSyncError> {
        self.remove_active_sync(sync_id).await.ok_or_else(|| {
            AutomergeSyncError::Protocol(format!("automerge sync {sync_id} not found"))
        })
    }

    async fn remove_active_sync(&self, sync_id: Ulid) -> Option<ActiveSync> {
        self.inner.active_syncs.lock().await.remove(&sync_id)
    }

    async fn start_outbound_sync(
        &self,
        peer: aruna_core::NodeId,
        init: AutomergeInit,
    ) -> AutomergeEvent {
        let Some(net_handle) = self.inner.net_handle.clone() else {
            return AutomergeEvent::SyncRejected {
                sync_id: Ulid::new(),
                document: Some(init.document),
                error: AutomergeSyncError::Network("network handle unavailable".to_string()),
            };
        };

        let sync_id = Ulid::new();
        let document = init.document.clone();
        let stream = match net_handle.open_stream(peer, Alpn::Automerge).await {
            Ok(stream) => stream,
            Err(err) => {
                return AutomergeEvent::SyncRejected {
                    sync_id,
                    document: Some(document),
                    error: AutomergeSyncError::Network(err.to_string()),
                };
            }
        };

        let mut sync = ActiveSync {
            peer,
            stream,
            direction: SyncDirection::Outbound,
            remote_init: None,
        };

        if let Err(error) = write_transport_message(
            &mut sync.stream,
            &AutomergeTransportMessage::Init(init.clone()),
        )
        .await
        {
            close_stream(&mut sync.stream).await;
            return AutomergeEvent::SyncRejected {
                sync_id,
                document: Some(document),
                error,
            };
        }

        match read_transport_message(&mut sync.stream).await {
            Ok(AutomergeTransportMessage::Init(remote_init)) => {
                sync.remote_init = Some(remote_init.clone());
                self.store_active_sync(sync_id, sync).await;
                AutomergeEvent::SyncInitialized {
                    sync_id,
                    peer,
                    remote_init,
                }
            }
            Ok(AutomergeTransportMessage::Reject(reason)) => {
                close_stream(&mut sync.stream).await;
                AutomergeEvent::SyncRejected {
                    sync_id,
                    document: Some(document),
                    error: reject_reason_to_error(reason),
                }
            }
            Ok(_) => {
                close_stream(&mut sync.stream).await;
                AutomergeEvent::SyncRejected {
                    sync_id,
                    document: Some(document),
                    error: AutomergeSyncError::InvalidInit,
                }
            }
            Err(error) => {
                close_stream(&mut sync.stream).await;
                AutomergeEvent::SyncRejected {
                    sync_id,
                    document: Some(document),
                    error,
                }
            }
        }
    }

    async fn start_inbound_sync(&self, sync_id: Ulid) -> AutomergeEvent {
        let mut sync = match self.take_active_sync(sync_id).await {
            Ok(sync) => sync,
            Err(error) => {
                return AutomergeEvent::SyncRejected {
                    sync_id,
                    document: None,
                    error,
                };
            }
        };

        let peer = sync.peer;
        match read_transport_message(&mut sync.stream).await {
            Ok(AutomergeTransportMessage::Init(remote_init)) => {
                sync.remote_init = Some(remote_init.clone());
                self.store_active_sync(sync_id, sync).await;
                AutomergeEvent::SyncInitialized {
                    sync_id,
                    peer,
                    remote_init,
                }
            }
            Ok(AutomergeTransportMessage::Reject(reason)) => {
                let document = sync.remote_init.as_ref().map(|init| init.document.clone());
                close_stream(&mut sync.stream).await;
                AutomergeEvent::SyncRejected {
                    sync_id,
                    document,
                    error: reject_reason_to_error(reason),
                }
            }
            Ok(_) => {
                close_stream(&mut sync.stream).await;
                AutomergeEvent::SyncRejected {
                    sync_id,
                    document: None,
                    error: AutomergeSyncError::InvalidInit,
                }
            }
            Err(error) => {
                close_stream(&mut sync.stream).await;
                AutomergeEvent::SyncRejected {
                    sync_id,
                    document: None,
                    error,
                }
            }
        }
    }

    async fn run_sync(
        &self,
        sync_id: Ulid,
        local_document: Vec<u8>,
        response_init: Option<AutomergeInit>,
    ) -> AutomergeEvent {
        let mut sync = match self.take_active_sync(sync_id).await {
            Ok(sync) => sync,
            Err(error) => {
                return AutomergeEvent::SyncRejected {
                    sync_id,
                    document: response_init.map(|init| init.document),
                    error,
                };
            }
        };

        let remote_init = match sync.remote_init.clone() {
            Some(remote_init) => remote_init,
            None => {
                let document = response_init.as_ref().map(|init| init.document.clone());
                close_stream(&mut sync.stream).await;
                return AutomergeEvent::SyncRejected {
                    sync_id,
                    document,
                    error: AutomergeSyncError::InvalidInit,
                };
            }
        };

        if let Some(local_init) = response_init.as_ref()
            && let Err(error) = write_transport_message(
                &mut sync.stream,
                &AutomergeTransportMessage::Init(local_init.clone()),
            )
            .await
        {
            let document = Some(local_init.document.clone());
            close_stream(&mut sync.stream).await;
            return AutomergeEvent::SyncRejected {
                sync_id,
                document,
                error,
            };
        }

        let document = response_init
            .as_ref()
            .map(|init| init.document.clone())
            .unwrap_or_else(|| remote_init.document.clone());

        let mut doc = match load_document(&local_document) {
            Ok(doc) => doc,
            Err(error) => {
                close_stream(&mut sync.stream).await;
                return AutomergeEvent::SyncRejected {
                    sync_id,
                    document: Some(document),
                    error,
                };
            }
        };

        let before_heads = doc.get_heads();
        let result = match sync.direction {
            SyncDirection::Outbound | SyncDirection::Inbound => {
                run_sync_rounds(&mut sync.stream, &mut doc, &remote_init).await
            }
        };

        match result {
            Ok(()) => {
                close_stream(&mut sync.stream).await;
                let after_heads = doc.get_heads();
                let changed = before_heads != after_heads;
                let updated_document = doc.save();
                AutomergeEvent::SyncFinished {
                    sync_id,
                    document,
                    before_heads,
                    after_heads,
                    updated_document,
                    changed,
                }
            }
            Err(error) => {
                close_stream(&mut sync.stream).await;
                AutomergeEvent::SyncRejected {
                    sync_id,
                    document: Some(document),
                    error,
                }
            }
        }
    }

    async fn reject_sync(&self, sync_id: Ulid, reason: AutomergeRejectReason) -> AutomergeEvent {
        let Some(sync) = self.remove_active_sync(sync_id).await else {
            return AutomergeEvent::SyncRejected {
                sync_id,
                document: None,
                error: reject_reason_to_error(reason),
            };
        };
        let mut sync = sync;
        let document = sync.remote_init.as_ref().map(|init| init.document.clone());
        let _ = write_transport_message(
            &mut sync.stream,
            &AutomergeTransportMessage::Reject(reason.clone()),
        )
        .await;
        close_stream(&mut sync.stream).await;
        AutomergeEvent::SyncRejected {
            sync_id,
            document,
            error: reject_reason_to_error(reason),
        }
    }

    async fn close_sync(&self, sync_id: Ulid) -> AutomergeEvent {
        if let Some(sync) = self.remove_active_sync(sync_id).await {
            let mut sync = sync;
            close_stream(&mut sync.stream).await;
        }
        AutomergeEvent::SyncClosed { sync_id }
    }
}

impl std::fmt::Debug for AutomergeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AutomergeHandle").finish()
    }
}

#[async_trait]
impl Handle for AutomergeHandle {
    async fn send_effect(&self, effect: Effect) -> Event {
        match effect {
            Effect::Automerge(effect) => {
                let event = match effect {
                    AutomergeEffect::StartOutboundSync { peer, init } => {
                        self.start_outbound_sync(peer, init).await
                    }
                    AutomergeEffect::StartInboundSync { sync_id } => {
                        self.start_inbound_sync(sync_id).await
                    }
                    AutomergeEffect::RunSync {
                        sync_id,
                        local_document,
                        response_init,
                    } => self.run_sync(sync_id, local_document, response_init).await,
                    AutomergeEffect::RejectSync { sync_id, reason } => {
                        self.reject_sync(sync_id, reason).await
                    }
                    AutomergeEffect::CloseSync { sync_id } => self.close_sync(sync_id).await,
                };
                Event::Automerge(event)
            }
            _ => Event::Automerge(AutomergeEvent::SyncRejected {
                sync_id: Ulid::new(),
                document: None,
                error: AutomergeSyncError::Protocol(
                    "invalid effect for automerge handle".to_string(),
                ),
            }),
        }
    }
}

async fn run_sync_rounds(
    stream: &mut BiStream,
    doc: &mut AutoCommit,
    remote_init: &AutomergeInit,
) -> Result<(), AutomergeSyncError> {
    if doc.get_heads() == remote_init.heads {
        write_transport_message(stream, &AutomergeTransportMessage::Done).await?;
        let _ = read_done_or_close(stream).await;
        return Ok(());
    }

    let mut state = fresh_sync_state(remote_init);
    let mut sent_done = false;
    let mut received_done = false;
    let mut rounds = 0usize;

    loop {
        rounds += 1;
        if rounds > MAX_SYNC_ROUNDS {
            return Err(AutomergeSyncError::Protocol(
                "automerge sync exceeded maximum rounds".to_string(),
            ));
        }

        if !sent_done {
            if let Some(message) = doc.sync().generate_sync_message(&mut state) {
                write_transport_message(stream, &AutomergeTransportMessage::Sync(message.encode()))
                    .await?;
            } else {
                write_transport_message(stream, &AutomergeTransportMessage::Done).await?;
                sent_done = true;
                if received_done {
                    return Ok(());
                }
            }
        }

        let message = match read_transport_message(stream).await {
            Ok(message) => message,
            Err(AutomergeSyncError::Network(_)) if sent_done => return Ok(()),
            Err(error) => return Err(error),
        };

        match message {
            AutomergeTransportMessage::Sync(bytes) => {
                received_done = false;
                let message = sync::Message::decode(&bytes)
                    .map_err(|err| AutomergeSyncError::Protocol(err.to_string()))?;
                doc.sync()
                    .receive_sync_message(&mut state, message)
                    .map_err(|err| AutomergeSyncError::Protocol(err.to_string()))?;
                sent_done = false;
            }
            AutomergeTransportMessage::Done => {
                received_done = true;
                if sent_done {
                    return Ok(());
                }
            }
            AutomergeTransportMessage::Reject(reason) => {
                return Err(reject_reason_to_error(reason));
            }
            AutomergeTransportMessage::Init(_) => return Err(AutomergeSyncError::InvalidFrame),
        }
    }
}

async fn read_done_or_close(stream: &mut BiStream) -> Result<(), AutomergeSyncError> {
    match read_transport_message(stream).await {
        Ok(AutomergeTransportMessage::Done) => Ok(()),
        Ok(AutomergeTransportMessage::Reject(reason)) => Err(reject_reason_to_error(reason)),
        Ok(_) => Err(AutomergeSyncError::InvalidFrame),
        Err(AutomergeSyncError::Network(_)) => Ok(()),
        Err(error) => Err(error),
    }
}

fn fresh_sync_state(init: &AutomergeInit) -> sync::State {
    let mut state = sync::State::new();
    state.their_capabilities = Some(init.capabilities.iter().map(map_capability).collect());
    state
}

fn map_capability(capability: &AutomergeSyncFeature) -> sync::Capability {
    match capability {
        AutomergeSyncFeature::MessageV1 => sync::Capability::MessageV1,
        AutomergeSyncFeature::MessageV2 => sync::Capability::MessageV2,
        AutomergeSyncFeature::InitAuthProof => sync::Capability::Unknown(0x10),
    }
}

fn load_document(bytes: &[u8]) -> Result<AutoCommit, AutomergeSyncError> {
    if bytes.is_empty() {
        return Ok(AutoCommit::new());
    }
    AutoCommit::load(bytes).map_err(|_err| AutomergeSyncError::InvalidDocument)
}

async fn close_stream(stream: &mut BiStream) {
    let _ = stream.0.finish();
}

async fn write_transport_message(
    stream: &mut BiStream,
    message: &AutomergeTransportMessage,
) -> Result<(), AutomergeSyncError> {
    tokio::time::timeout(SYNC_IO_TIMEOUT, write_message(stream, message))
        .await
        .map_err(|_| {
            AutomergeSyncError::Network("timed out writing automerge message".to_string())
        })?
}

async fn read_transport_message(
    stream: &mut BiStream,
) -> Result<AutomergeTransportMessage, AutomergeSyncError> {
    tokio::time::timeout(SYNC_IO_TIMEOUT, read_message(stream))
        .await
        .map_err(|_| {
            AutomergeSyncError::Network("timed out waiting for automerge message".to_string())
        })?
}

fn reject_reason_to_error(reason: AutomergeRejectReason) -> AutomergeSyncError {
    match reason {
        AutomergeRejectReason::Unauthorized => AutomergeSyncError::Unauthorized,
        AutomergeRejectReason::DocumentNotFound => AutomergeSyncError::DocumentNotFound,
        AutomergeRejectReason::InvalidDocument => AutomergeSyncError::InvalidDocument,
        AutomergeRejectReason::InvalidInit => AutomergeSyncError::InvalidInit,
        AutomergeRejectReason::InternalError => {
            AutomergeSyncError::Protocol("remote rejected sync".to_string())
        }
    }
}
