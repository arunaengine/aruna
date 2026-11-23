//! Defines the network configuration: bind address, realm, discovery, relay and sync settings.
//! Re-exports the discovery, relay and endpoint address text types that core defines.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::SocketAddr;
use std::path::PathBuf;

use ::irokle::net::IrohRuntimeConfig;
use aruna_core::id::NodeId;
pub use aruna_core::network_config::{
    DiscoveryMethod, RelayMethod, format_endpoint_config, parse_endpoint_config,
};
use aruna_core::persistence::FjallPersistPolicy;
use aruna_core::structs::identity::realm::RealmId;
use iroh::EndpointAddr;

#[derive(Clone)]
pub struct NetConfig {
    pub bind_addr: SocketAddr,
    pub secret_key: Option<iroh::SecretKey>,
    pub realm_id: RealmId,
    pub peer_nodes: Vec<NodeId>,
    pub peer_endpoints: Vec<EndpointAddr>,
    pub temporary_bootstrap_active: bool,
    pub discovery_method: DiscoveryMethod,
    pub relay_method: RelayMethod,
    pub max_uni_streams: Option<u64>,
    pub max_bidi_streams: Option<u64>,
    pub sync_storage_path: Option<PathBuf>,
    pub document_sync_runtime: Option<IrohRuntimeConfig>,
    pub fjall_persist_policy: FjallPersistPolicy,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            secret_key: None,
            realm_id: RealmId::from_bytes([0u8; 32]),
            peer_nodes: vec![],
            peer_endpoints: vec![],
            temporary_bootstrap_active: false,
            discovery_method: DiscoveryMethod::N0Dns,
            relay_method: RelayMethod::N0,
            max_bidi_streams: None,
            max_uni_streams: None,
            sync_storage_path: None,
            document_sync_runtime: None,
            fjall_persist_policy: FjallPersistPolicy::default(),
        }
    }
}

impl std::fmt::Debug for NetConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetConfig")
            .field("bind_addr", &self.bind_addr)
            .field("has_secret_key", &self.secret_key.is_some())
            .field("realm_id", &self.realm_id)
            .field("peer_nodes", &self.peer_nodes.len())
            .field("peer_endpoints", &self.peer_endpoints.len())
            .field(
                "temporary_bootstrap_active",
                &self.temporary_bootstrap_active,
            )
            .field("discovery_method", &self.discovery_method)
            .field("relay_method", &self.relay_method)
            .field("fjall_persist_policy", &self.fjall_persist_policy)
            .finish()
    }
}
