use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aruna_core::events::DhtEntry;
use aruna_core::id::{DhtKeyId, NodeId};
use aruna_storage::StorageHandle;
use iroh::Endpoint;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::constants::{
    CMD_CHANNEL_CAPACITY, DRIVER_IO_EVENT_CAPACITY, DRIVER_IO_REQUEST_CAPACITY,
    INBOUND_STREAM_CAPACITY,
};
use super::driver::DhtDriver;
use super::io_dispatcher::{DhtIoDispatcher, InboundDhtStream};
use super::protocol::{CallerId, DhtCmd, DhtIoError, DhtOutput, DhtOutputValue};
use super::state::DhtStateMachine;
use super::storage::now_unix_secs;
use crate::error::{NetError, Result};

type CallerOutcome = std::result::Result<DhtOutputValue, DhtIoError>;
type PendingCallers = Arc<Mutex<HashMap<CallerId, oneshot::Sender<CallerOutcome>>>>;

#[derive(Debug)]
pub struct DhtRuntime {
    pub inbound_stream_tx: mpsc::Sender<InboundDhtStream>,
    pub tasks: Vec<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct DhtHandle {
    cmd_tx: mpsc::Sender<DhtCmd>,
    pending_callers: PendingCallers,
    next_caller_id: Arc<AtomicU64>,
    local_id: NodeId,
}

impl std::fmt::Debug for DhtHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DhtHandle")
            .field("local_id", &self.local_id)
            .finish()
    }
}

impl DhtHandle {
    pub fn spawn(
        endpoint: Endpoint,
        storage: StorageHandle,
        shutdown: CancellationToken,
    ) -> Result<(Self, DhtRuntime)> {
        let local_id = endpoint.id();
        let secret_key = endpoint.secret_key().clone();

        let (cmd_tx, cmd_rx) = mpsc::channel(CMD_CHANNEL_CAPACITY);
        let (io_request_tx, io_request_rx) = mpsc::channel(DRIVER_IO_REQUEST_CAPACITY);
        let (io_event_tx, io_event_rx) = mpsc::channel(DRIVER_IO_EVENT_CAPACITY);
        let (inbound_stream_tx, inbound_stream_rx) = mpsc::channel(INBOUND_STREAM_CAPACITY);
        let (output_tx, output_rx) = mpsc::unbounded_channel();

        let pending_callers: PendingCallers = Arc::new(Mutex::new(HashMap::new()));
        let broker_callers = pending_callers.clone();

        let state = DhtStateMachine::new(local_id, secret_key, now_unix_secs());
        let driver = DhtDriver::new(
            state,
            cmd_rx,
            io_event_rx,
            io_request_tx,
            output_tx,
            shutdown.child_token(),
        );

        let dispatcher = DhtIoDispatcher::new(
            endpoint,
            storage,
            io_request_rx,
            inbound_stream_rx,
            io_event_tx,
            shutdown.child_token(),
        );

        let broker_task = tokio::spawn(async move {
            run_output_broker(output_rx, broker_callers).await;
        });
        let driver_task = tokio::spawn(async move {
            driver.run().await;
        });
        let dispatcher_task = tokio::spawn(async move {
            dispatcher.run().await;
        });

        let handle = Self {
            cmd_tx,
            pending_callers,
            next_caller_id: Arc::new(AtomicU64::new(1)),
            local_id,
        };

        Ok((
            handle,
            DhtRuntime {
                inbound_stream_tx,
                tasks: vec![broker_task, driver_task, dispatcher_task],
            },
        ))
    }

    pub fn local_id(&self) -> NodeId {
        self.local_id
    }

    pub fn add_peer(&self, node_id: NodeId) -> Result<()> {
        self.try_enqueue(DhtCmd::AddPeer { node_id })
    }

    pub async fn put(&self, key: &DhtKeyId, value: Vec<u8>, ttl: Duration) -> Result<()> {
        match self
            .request(|caller_id| DhtCmd::Put {
                key: *key,
                value,
                ttl,
                caller_id: Some(caller_id),
            })
            .await?
        {
            DhtOutputValue::Unit => Ok(()),
            other => Err(NetError::Dht(format!(
                "unexpected DHT put output: {other:?}"
            ))),
        }
    }

    pub async fn get(&self, key: &DhtKeyId) -> Result<Vec<DhtEntry>> {
        match self
            .request(|caller_id| DhtCmd::Get {
                key: *key,
                caller_id: Some(caller_id),
            })
            .await?
        {
            DhtOutputValue::GetValues(values) => Ok(values),
            other => Err(NetError::Dht(format!(
                "unexpected DHT get output: {other:?}"
            ))),
        }
    }

    pub async fn bootstrap(&self, nodes: &[[u8; 32]]) -> Result<()> {
        let node_ids: Vec<NodeId> = nodes
            .iter()
            .filter_map(|bytes| NodeId::from_bytes(bytes).ok())
            .collect();
        self.bootstrap_nodes(&node_ids).await
    }

    pub async fn bootstrap_nodes(&self, nodes: &[NodeId]) -> Result<()> {
        match self
            .request(|caller_id| DhtCmd::Bootstrap {
                nodes: nodes.to_vec(),
                caller_id: Some(caller_id),
            })
            .await?
        {
            DhtOutputValue::Unit => Ok(()),
            other => Err(NetError::Dht(format!(
                "unexpected DHT bootstrap output: {other:?}"
            ))),
        }
    }

    pub async fn routing_table_size(&self) -> Result<usize> {
        match self
            .request(|caller_id| DhtCmd::RoutingTableSize {
                caller_id: Some(caller_id),
            })
            .await?
        {
            DhtOutputValue::RoutingTableSize(size) => Ok(size),
            other => Err(NetError::Dht(format!(
                "unexpected DHT routing table output: {other:?}"
            ))),
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        match self
            .request(|caller_id| DhtCmd::Shutdown {
                caller_id: Some(caller_id),
            })
            .await?
        {
            DhtOutputValue::Unit => Ok(()),
            other => Err(NetError::Dht(format!(
                "unexpected DHT shutdown output: {other:?}"
            ))),
        }
    }

    fn try_enqueue(&self, cmd: DhtCmd) -> Result<()> {
        match self.cmd_tx.try_send(cmd) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                Err(NetError::Dht(DhtIoError::QueueFull.to_string()))
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(NetError::Dht(DhtIoError::Shutdown.to_string()))
            }
        }
    }

    async fn request<F>(&self, make_cmd: F) -> Result<DhtOutputValue>
    where
        F: FnOnce(CallerId) -> DhtCmd,
    {
        let caller_id = self.next_caller_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel::<CallerOutcome>();

        self.pending_callers.lock().insert(caller_id, tx);

        let cmd = make_cmd(caller_id);
        if let Err(err) = self.try_enqueue(cmd) {
            self.pending_callers.lock().remove(&caller_id);
            return Err(err);
        }

        match rx.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(NetError::Dht(error.to_string())),
            Err(_) => Err(NetError::ChannelClosed),
        }
    }
}

async fn run_output_broker(
    mut output_rx: mpsc::UnboundedReceiver<DhtOutput>,
    pending_callers: PendingCallers,
) {
    while let Some(output) = output_rx.recv().await {
        match output {
            DhtOutput::Completed { caller_id, result } => {
                if let Some(sender) = pending_callers.lock().remove(&caller_id) {
                    let _ = sender.send(Ok(result));
                }
            }
            DhtOutput::Failed { caller_id, error } => {
                if let Some(sender) = pending_callers.lock().remove(&caller_id) {
                    let _ = sender.send(Err(error));
                }
            }
            DhtOutput::ShutdownComplete => {
                let waiting: Vec<_> = pending_callers.lock().drain().collect();
                for (_, sender) in waiting {
                    let _ = sender.send(Err(DhtIoError::Shutdown));
                }
            }
        }
    }

    let waiting: Vec<_> = pending_callers.lock().drain().collect();
    for (_, sender) in waiting {
        let _ = sender.send(Err(DhtIoError::Shutdown));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(seed: u8) -> NodeId {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        iroh::SecretKey::from_bytes(&seed_bytes).public()
    }

    #[test]
    fn add_peer_returns_queue_full_when_channel_saturated() {
        let (cmd_tx, _cmd_rx) = mpsc::channel(1);
        let local_id = make_node(1);
        let handle = DhtHandle {
            cmd_tx,
            pending_callers: Arc::new(Mutex::new(HashMap::new())),
            next_caller_id: Arc::new(AtomicU64::new(1)),
            local_id,
        };

        handle
            .add_peer(make_node(2))
            .expect("first add_peer should fill channel");
        let err = handle
            .add_peer(make_node(3))
            .expect_err("second add_peer should hit queue full");

        assert!(
            matches!(err, NetError::Dht(message) if message == DhtIoError::QueueFull.to_string())
        );
    }
}
