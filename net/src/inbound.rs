use aruna_core::id::{NodeId, TopicId};

#[derive(Debug)]
pub enum InboundNetEvent {
    GossipMessage {
        topic: TopicId,
        sender: NodeId,
        data: Vec<u8>,
    },
    StreamOpened {
        stream_id: u64,
        node_id: NodeId,
    },
}
