use std::{collections::HashMap, sync::Arc};

use iroh::{endpoint::Connection, Endpoint, NodeId};
use tokio::sync::RwLock;

use crate::ARUNA_NET_ALPN;

pub struct ConnectionHandler {
    endpoint: Endpoint,
    connections: RwLock<HashMap<NodeId, Connection>>,
}


impl ConnectionHandler {
    pub fn new(endpoint: Endpoint) -> Arc<Self> {
        Arc::new(Self {
            endpoint,
            connections: RwLock::new(HashMap::new()),
        })
    }

    pub async fn get_or_create_connection(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Connection> {
        {
            let connections = self.connections.read().await;
            if let Some(connection) = connections.get(&node_id) {
                return Ok(connection.clone());
            }
        }
        let mut connections = self.connections.write().await;
        let connection = self.endpoint.connect(node_id, ARUNA_NET_ALPN).await?;
        connections.insert(node_id, connection.clone());
        Ok(connection)
    }
}