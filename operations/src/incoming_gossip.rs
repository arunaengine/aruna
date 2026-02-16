use aruna_core::events::Event;
use aruna_core::id::{NodeId, TopicId};
use aruna_core::operation::Operation;
use smallvec::smallvec;
use std::convert::Infallible;
use tracing::info;

#[derive(Debug)]
pub struct IncomingGossipOperation {
    topic: TopicId,
    sender: NodeId,
    data: Vec<u8>,
}

impl IncomingGossipOperation {
    pub fn new(topic: TopicId, sender: NodeId, data: Vec<u8>) -> Self {
        Self {
            topic,
            sender,
            data,
        }
    }
}

impl Operation for IncomingGossipOperation {
    type Output = ();
    type Error = Infallible;

    fn start(&mut self) -> aruna_core::types::Effects {
        info!(
            topic = %self.topic,
            sender = %self.sender,
            bytes = self.data.len(),
            "Received inbound gossip message"
        );
        smallvec![]
    }

    fn step(&mut self, _event: Event) -> aruna_core::types::Effects {
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        true
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        Ok(())
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        smallvec![]
    }
}
