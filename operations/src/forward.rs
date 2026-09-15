//! Forwarding mechanics shared by every domain's inbound adapter: peer
//! authorization, holder transport, document replay checks, and write routing.
#[path = "forward_authorize.rs"]
pub mod authorize;
#[path = "forward_replay.rs"]
pub mod replay;
#[path = "forward_routing.rs"]
pub mod routing;
#[path = "forward_transport.rs"]
pub mod transport;
