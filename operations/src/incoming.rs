use std::sync::Arc;

use aruna_core::events::Event;
use aruna_core::id::{NodeId, TopicId};
use aruna_core::operation::Operation;
use aruna_net::{InboundEventHandler, InboundNetEvent};
use async_trait::async_trait;
use smallvec::smallvec;
use thiserror::Error;
use tracing::{error, info, warn};

use crate::driver::{DriverContext, drive};

#[derive(Debug, Error)]
pub enum IncomingOperationError {
    #[error("Incoming operation did not finish")]
    NotFinished,
}

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
    async fn handle_inbound(&self, event: InboundNetEvent) {
        match event {
            InboundNetEvent::GossipMessage {
                topic,
                sender,
                data,
            } => {
                let op = IncomingGossipMessageOperation::new(topic, sender, data);
                if let Err(err) = drive(op, self.context.as_ref()).await {
                    error!(error = ?err, "Failed to process inbound gossip event");
                }
            }
            InboundNetEvent::StreamOpened { stream_id, node_id } => {
                let op = IncomingStreamOpenedOperation::new(stream_id, node_id);
                if let Err(err) = drive(op, self.context.as_ref()).await {
                    error!(error = ?err, "Failed to process inbound stream-open event");
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct IncomingGossipMessageOperation {
    topic: TopicId,
    sender: NodeId,
    data: Vec<u8>,
    complete: bool,
}

impl IncomingGossipMessageOperation {
    pub fn new(topic: TopicId, sender: NodeId, data: Vec<u8>) -> Self {
        Self {
            topic,
            sender,
            data,
            complete: false,
        }
    }
}

impl Operation for IncomingGossipMessageOperation {
    type Output = ();
    type Error = IncomingOperationError;

    fn start(&mut self) -> aruna_core::types::Effects {
        info!(
            topic = %self.topic,
            sender = %self.sender,
            bytes = self.data.len(),
            data = ?self.data,
            "Received inbound gossip message"
        );
        self.complete = true;
        smallvec![]
    }

    fn step(&mut self, _event: Event) -> aruna_core::types::Effects {
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.complete
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.complete {
            Ok(())
        } else {
            Err(IncomingOperationError::NotFinished)
        }
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        self.complete = true;
        smallvec![]
    }
}

#[derive(Debug)]
pub struct IncomingStreamOpenedOperation {
    stream_id: u64,
    node_id: NodeId,
    complete: bool,
}

impl IncomingStreamOpenedOperation {
    pub fn new(stream_id: u64, node_id: NodeId) -> Self {
        Self {
            stream_id,
            node_id,
            complete: false,
        }
    }
}

impl Operation for IncomingStreamOpenedOperation {
    type Output = ();
    type Error = IncomingOperationError;

    fn start(&mut self) -> aruna_core::types::Effects {
        info!(
            stream_id = self.stream_id,
            node_id = %self.node_id,
            "Received inbound stream opened event"
        );
        self.complete = true;
        smallvec![]
    }

    fn step(&mut self, _event: Event) -> aruna_core::types::Effects {
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.complete
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.complete {
            Ok(())
        } else {
            Err(IncomingOperationError::NotFinished)
        }
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        self.complete = true;
        smallvec![]
    }
}
