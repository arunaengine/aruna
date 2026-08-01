// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
#![allow(clippy::result_large_err)]

pub mod auth;
pub mod cors;
pub mod csp;
pub mod error;
pub mod forwarded;
pub mod openapi;
pub mod ops;
mod portal;
pub mod routes;
pub mod s3;
pub mod server;
pub mod server_state;
pub mod telemetry;
