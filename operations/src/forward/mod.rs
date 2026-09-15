//! Forwarding mechanics shared by every domain's inbound adapter: peer
//! authorization, holder transport, document replay checks, and write routing.
pub mod authorize;
pub mod replay;
pub mod routing;
pub mod transport;
