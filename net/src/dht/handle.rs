use std::time::Duration;

use aruna_core::events::DhtEntry;
use aruna_core::id::{DhtKeyId, NodeId};
use aruna_core::structs::RealmId;
use aruna_storage::StorageHandle;
use crossfire::{TrySendError, mpsc};
use iroh::Endpoint;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::trace;

use super::constants::{CMD_CHANNEL_CAPACITY, INBOUND_STREAM_CAPACITY};
use super::driver::{CallerOutcome, DhtDriver, DriverCmd, DriverCmdSender, InboundSender};
use super::protocol::{DhtIoError, DhtOutputValue};
use super::state::DhtStateMachine;
use super::storage::now_unix_secs;
use crate::connection_pool::ConnectionPool;
use crate::error::{NetError, Result};
use crate::telemetry::current_trace_context;

#[derive(Debug)]
pub(crate) struct DhtSpawnResources {
    pub inbound_stream_tx: InboundSender,
    pub tasks: Vec<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct DhtHandle {
    cmd_tx: DriverCmdSender,
    shutdown: CancellationToken,
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
    #[tracing::instrument(
        name = "dht.handle.spawn",
        level = "debug",
        skip(endpoint, storage, shutdown)
    )]
    pub(crate) fn spawn(
        endpoint: Endpoint,
        storage: StorageHandle,
        connection_pool: ConnectionPool,
        shutdown: CancellationToken,
    ) -> Result<(Self, DhtSpawnResources)> {
        let local_id = endpoint.id();
        let secret_key = endpoint.secret_key().clone();

        let (cmd_tx, cmd_rx) = mpsc::bounded_blocking_async(CMD_CHANNEL_CAPACITY);
        let (inbound_stream_tx, inbound_stream_rx) =
            mpsc::bounded_blocking_async(INBOUND_STREAM_CAPACITY);

        let state = DhtStateMachine::new(local_id, secret_key, now_unix_secs());
        let driver = DhtDriver::new(
            state,
            endpoint,
            storage,
            connection_pool,
            cmd_rx,
            inbound_stream_rx,
            shutdown.clone(),
        );

        let driver_task = tokio::spawn(async move {
            driver.run().await;
        });

        let handle = Self {
            cmd_tx,
            shutdown,
            local_id,
        };

        Ok((
            handle,
            DhtSpawnResources {
                inbound_stream_tx,
                tasks: vec![driver_task],
            },
        ))
    }

    pub fn local_id(&self) -> NodeId {
        self.local_id
    }

    #[tracing::instrument(name = "dht.handle.add_peer", level = "debug", skip(self), fields(node_id = %node_id))]
    pub fn add_peer(&self, node_id: NodeId) -> Result<()> {
        self.try_enqueue(DriverCmd::AddPeer { node_id })
    }

    #[tracing::instrument(
        name = "dht.handle.put",
        level = "debug",
        skip(self, value),
        fields(key = %key, realm_id = %realm_id, value_len = value.len(), ttl_secs = ttl.as_secs())
    )]
    pub async fn put(
        &self,
        key: &DhtKeyId,
        realm_id: RealmId,
        value: Vec<u8>,
        ttl: Duration,
    ) -> Result<()> {
        trace!(
            event = "dht.put.started",
            key = %key,
            realm_id = %realm_id,
            value_len = value.len(),
            ttl_secs = ttl.as_secs(),
            "Starting DHT put"
        );
        match self
            .request(|reply| DriverCmd::Put {
                key: *key,
                realm_id,
                value,
                ttl,
                trace_context: current_trace_context(),
                reply,
            })
            .await?
        {
            DhtOutputValue::Unit => {
                trace!(event = "dht.put.completed", key = %key, "Completed DHT put");
                Ok(())
            }
            other => Err(NetError::Dht(format!(
                "unexpected DHT put output: {other:?}"
            ))),
        }
    }

    #[tracing::instrument(
        name = "dht.handle.get",
        level = "debug",
        skip(self),
        fields(key = %key, realm_id = ?realm_filter)
    )]
    pub async fn get(
        &self,
        key: &DhtKeyId,
        realm_filter: Option<RealmId>,
    ) -> Result<Vec<DhtEntry>> {
        trace!(
            event = "dht.get.started",
            key = %key,
            realm_id = ?realm_filter,
            "Starting DHT get"
        );
        match self
            .request(|reply| DriverCmd::Get {
                key: *key,
                realm_filter,
                trace_context: current_trace_context(),
                reply,
            })
            .await?
        {
            DhtOutputValue::GetValues(values) => {
                trace!(
                    event = "dht.get.completed",
                    key = %key,
                    result_count = values.len(),
                    "Completed DHT get"
                );
                Ok(values)
            }
            other => Err(NetError::Dht(format!(
                "unexpected DHT get output: {other:?}"
            ))),
        }
    }

    #[tracing::instrument(
        name = "dht.handle.bootstrap",
        level = "debug",
        skip(self, nodes),
        fields(node_count = nodes.len())
    )]
    pub async fn bootstrap(&self, nodes: &[[u8; 32]]) -> Result<()> {
        let node_ids: Vec<NodeId> = nodes
            .iter()
            .filter_map(|bytes| NodeId::from_bytes(bytes).ok())
            .collect();
        self.bootstrap_nodes(&node_ids).await
    }

    #[tracing::instrument(
        name = "dht.handle.bootstrap_nodes",
        level = "debug",
        skip(self, nodes),
        fields(node_count = nodes.len())
    )]
    pub async fn bootstrap_nodes(&self, nodes: &[NodeId]) -> Result<()> {
        match self
            .request(|reply| DriverCmd::Bootstrap {
                nodes: nodes.to_vec(),
                trace_context: current_trace_context(),
                reply,
            })
            .await?
        {
            DhtOutputValue::Unit => Ok(()),
            other => Err(NetError::Dht(format!(
                "unexpected DHT bootstrap output: {other:?}"
            ))),
        }
    }

    #[tracing::instrument(name = "dht.handle.routing_table_size", level = "debug", skip(self))]
    pub async fn routing_table_size(&self) -> Result<usize> {
        match self
            .request(|reply| DriverCmd::RoutingTableSize {
                trace_context: current_trace_context(),
                reply,
            })
            .await?
        {
            DhtOutputValue::RoutingTableSize(size) => Ok(size),
            other => Err(NetError::Dht(format!(
                "unexpected DHT routing table output: {other:?}"
            ))),
        }
    }

    #[tracing::instrument(name = "dht.handle.shutdown", level = "debug", skip(self))]
    pub async fn shutdown(&self) -> Result<()> {
        self.shutdown.cancel();
        Ok(())
    }

    #[tracing::instrument(name = "dht.handle.enqueue", level = "trace", skip(self), fields(command = ?cmd))]
    fn try_enqueue(&self, cmd: DriverCmd) -> Result<()> {
        match self.cmd_tx.try_send(cmd) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(NetError::Dht(DhtIoError::QueueFull.to_string())),
            Err(TrySendError::Disconnected(_)) => {
                Err(NetError::Dht(DhtIoError::Shutdown.to_string()))
            }
        }
    }

    #[tracing::instrument(name = "dht.handle.request", level = "debug", skip(self, make_cmd))]
    async fn request<F>(&self, make_cmd: F) -> Result<DhtOutputValue>
    where
        F: FnOnce(oneshot::Sender<CallerOutcome>) -> DriverCmd,
    {
        let (tx, rx) = oneshot::channel::<CallerOutcome>();
        let cmd = make_cmd(tx);
        self.try_enqueue(cmd)?;

        match rx.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(NetError::Dht(error.to_string())),
            Err(_) => Err(NetError::ChannelClosed),
        }
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
        let (cmd_tx, _cmd_rx) = mpsc::bounded_blocking_async(1);
        let local_id = make_node(1);
        let handle = DhtHandle {
            cmd_tx,
            shutdown: CancellationToken::new(),
            local_id,
        };

        handle
            .add_peer(make_node(2))
            .expect("first add_peer should fill channel");
        let err = handle
            .add_peer(make_node(3))
            .expect_err("second add_peer should hit queue full");

        assert!(matches!(
            err,
            NetError::Dht(message) if message == super::super::protocol::DhtIoError::QueueFull.to_string()
        ));
    }
}
