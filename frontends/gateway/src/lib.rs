//! The stateless applet gateway: serves every applet host from the node's edge routes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod policy;
pub mod settings;

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("invalid gateway settings: {0}")]
    Settings(String),
    #[error("the gateway listener failed")]
    Listen(#[source] std::io::Error),
}
