use std::sync::Arc;

use aruna_core::alpn::Alpn;
use aruna_core::id::{NodeId, TopicId};
use aruna_net::InboundEventHandler;
use aruna_net::streams::BiStream;
use async_trait::async_trait;
use tracing::{error, warn};

use crate::driver::{DriverContext, drive};
use crate::incoming_automerge::IncomingAutomergeOperation;
use crate::incoming_bao::IncomingBaoOperation;
use crate::incoming_gossip::IncomingGossipOperation;

#[derive(Debug)]
struct OperationsInboundHandler {
    context: Arc<DriverContext>,
}

impl OperationsInboundHandler {
    fn new(context: Arc<DriverContext>) -> Self {
        Self { context }
    }
}

pub fn initialize_net_incoming(context: Arc<DriverContext>) {
    let Some(net_handle) = context.net_handle.clone() else {
        warn!("Cannot initialize inbound handling without net handle");
        return;
    };

    net_handle.set_inbound_handler(Arc::new(OperationsInboundHandler::new(context)));
}

#[async_trait]
impl InboundEventHandler for OperationsInboundHandler {
    async fn handle_gossip_message(&self, topic: TopicId, sender: NodeId, data: Vec<u8>) {
        let op = IncomingGossipOperation::new(topic, sender, data);
        if let Err(err) = drive(op, self.context.as_ref()).await {
            error!(error = ?err, "Failed to process inbound gossip event");
        }
    }

    async fn handle_incoming_stream(&self, alpn: Alpn, stream: BiStream, node_id: NodeId) {
        match alpn {
            Alpn::Bao => {
                let op = IncomingBaoOperation::new(stream, node_id);
                if let Err(err) = drive(op, self.context.as_ref()).await {
                    error!(error = ?err, "Failed to process inbound bao stream event");
                }
            }
            Alpn::Automerge => {
                let op = IncomingAutomergeOperation::new(stream, node_id);
                if let Err(err) = drive(op, self.context.as_ref()).await {
                    error!(error = ?err, "Failed to process inbound automerge stream event");
                }
            }
            Alpn::Dht | Alpn::Gossip => {
                warn!(
                    node_id = %node_id,
                    "Ignoring inbound stream for non-stream ALPN"
                );
            }
        }
    }
}
