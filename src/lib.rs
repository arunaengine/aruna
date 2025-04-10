use std::time::Duration;

// Constants
pub const K_BUCKET_SIZE: usize = 5;
pub const ALPHA: usize = 3;
pub const BETA: usize = 3;
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
pub const STALE_NODE_THRESHOLD: Duration = Duration::from_secs(300); // 5 minutes

// Module declarations
mod k_bucket;
mod kademlia;
mod messages;
mod node_info;
mod utils;

// Re-exports for public API
pub use kademlia::Kademlia;
pub use node_info::NodeInfo;
pub use utils::calculate_distance;
