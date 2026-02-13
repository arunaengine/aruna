use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_net::streams::BiStream;
use smallvec::smallvec;
use std::convert::Infallible;
use tracing::info;

pub struct IncomingAutomergeOperation {
    stream: Option<BiStream>,
    node_id: NodeId,
}

impl IncomingAutomergeOperation {
    pub fn new(stream: BiStream, node_id: NodeId) -> Self {
        Self {
            stream: Some(stream),
            node_id,
        }
    }
}

impl Operation for IncomingAutomergeOperation {
    type Output = ();
    type Error = Infallible;

    fn start(&mut self) -> aruna_core::types::Effects {
        let _ = self.stream.take();
        info!(
            node_id = %self.node_id,
            "Received inbound Automerge stream"
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
