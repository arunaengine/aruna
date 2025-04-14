use std::time::Duration;

// Constants
pub const K_BUCKET_SIZE: usize = 5;
pub const ALPHA: usize = 3;
pub const BETA: usize = 3;
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
pub const STALE_NODE_THRESHOLD: Duration = Duration::from_secs(300); // 5 minutes
pub const ARUNA_NET_ALPN: &[u8] = b"aruna-net/0.1.0";

// Module declarations
pub mod connection_handler;
mod kademlia;

pub use connection_handler::ConnectionHandler;
pub use connection_handler::ConnectionHandlerBuilder;
pub use kademlia::kademlia::Kademlia;
pub use kademlia::messages::FindResult;
pub use connection_handler::ProtocolHandler;
