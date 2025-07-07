use crate::K_BUCKET_SIZE;
use crate::kademlia::node_info::NodeInfo;
use iroh::{NodeAddr, NodeId};

/// K-Bucket structure that stores NodeInfo directly in a fixed-size array
#[derive(Clone, Debug)]
pub struct KBucket {
    nodes: [Option<NodeInfo>; K_BUCKET_SIZE],
}

impl KBucket {
    /// Create a new empty K-bucket
    #[tracing::instrument(level = "trace")]
    pub fn new() -> Self {
        // Initialize with None values
        Self {
            nodes: std::array::from_fn(|_| None),
        }
    }

    /// Update the k-bucket with a node.
    ///
    /// If the node already exists, updates its last_seen timestamp.
    /// If the bucket is full, returns the least recently seen node for pinging.
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn update(&mut self, info: NodeInfo) -> Option<(NodeAddr, usize)> {
        let node_id = &info.addr.node_id;

        // If node exists, update its last_seen timestamp
        if let Some(pos) = self.find_node(node_id) {
            self.nodes[pos]
                .as_mut()
                .expect("This should exist")
                .update_last_seen();
            return None;
        }

        // If an empty slot exists, add the node
        if let Some(pos) = self.find_empty_slot() {
            self.nodes[pos] = Some(info);
            return None;
        }

        // If bucket is full, return the least recently seen node for ping
        if let Some(pos) = self.find_least_recently_seen() {
            // Return the address of the least recently seen node
            let lrs_addr = self.nodes[pos]
                .as_ref()
                .map(|info| (info.addr.clone(), pos));

            // Replace with the new node
            self.nodes[pos] = Some(info);

            return lrs_addr;
        }

        // This should never happen as long as K_BUCKET_SIZE > 0
        None
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn replace_node(&mut self, pos: usize, info: NodeInfo) {
        // Replace the node at the given position
        self.nodes[pos] = Some(info);
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn refresh_node(&mut self, pos: usize) {
        // Refresh the last seen time of the node at the given position
        if let Some(info) = self.nodes[pos].as_mut() {
            info.update_last_seen();
        }
    }

    /// Find a node in the bucket by its ID
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn find_node(&self, node_id: &NodeId) -> Option<usize> {
        self.nodes.iter().position(|opt_info| {
            opt_info
                .as_ref()
                .is_some_and(|info| &info.addr.node_id == node_id)
        })
    }

    /// Find an empty slot in the bucket
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn find_empty_slot(&self) -> Option<usize> {
        self.nodes.iter().position(|opt_info| opt_info.is_none())
    }

    /// Find the least recently seen node in the bucket
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn find_least_recently_seen(&self) -> Option<usize> {
        let mut oldest_pos = None;
        let mut oldest_time = None;

        for (i, opt_info) in self.nodes.iter().enumerate() {
            if let Some(info) = opt_info {
                if oldest_time.is_none() || info.last_seen < oldest_time.unwrap() {
                    oldest_time = Some(info.last_seen);
                    oldest_pos = Some(i);
                }
            }
        }

        oldest_pos
    }

    /// Get all nodes in this bucket
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_nodes(&self) -> Vec<NodeAddr> {
        self.nodes
            .iter()
            .filter_map(|opt_info| opt_info.as_ref().map(|info| info.addr.clone()))
            .collect()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_stale_nodes(&self) -> Vec<NodeAddr> {
        self.nodes
            .iter()
            .filter_map(|opt_info| {
                opt_info
                    .as_ref()
                    .filter(|info| info.is_stale())
                    .map(|info| info.addr.clone())
            })
            .collect()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn remove_node(&mut self, node_id: &NodeId) {
        if let Some(pos) = self.find_node(node_id) {
            self.nodes[pos] = None;
        }
    }
}
