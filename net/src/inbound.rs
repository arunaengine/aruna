use aruna_core::id::{NodeId, TopicId};
use aruna_core::state_machine::StateMachineConfig;

#[derive(Debug)]
pub enum InboundNetEvent {
    GossipMessage {
        topic: TopicId,
        sender: NodeId,
        data: Vec<u8>,
        state_machine: StateMachineConfig,
    },
    StreamOpened {
        stream_id: u64,
        node_id: NodeId,
    },
}
