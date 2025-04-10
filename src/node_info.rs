use crate::STALE_NODE_THRESHOLD;
use iroh::NodeAddr;
use std::time::Instant;

/// Node info struct - Contains node information with last_seen timestamp
#[derive(Clone)]
pub struct NodeInfo {
    pub addr: NodeAddr,
    pub last_seen: Instant,
}

impl NodeInfo {
    /// Create a new NodeInfo with the current timestamp
    pub fn new(addr: NodeAddr) -> Self {
        Self {
            addr,
            last_seen: Instant::now(),
        }
    }

    /// Update the last_seen timestamp to now
    pub fn update_last_seen(&mut self) {
        self.last_seen = Instant::now();
    }

    /// Check if this node is stale (hasn't been seen recently)
    pub fn is_stale(&self) -> bool {
        self.last_seen.elapsed() > STALE_NODE_THRESHOLD
    }
}
