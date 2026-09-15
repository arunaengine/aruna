//! Network configuration: bind address, realm identity, discovery and relay
//! selection, and durable document-sync runtime settings. Endpoint address
//! parsing and formatting live here beside the `NetConfig` shape they encode.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use ::irokle::net::IrohRuntimeConfig;
use aruna_core::id::NodeId;
use aruna_core::structs::RealmId;
use aruna_storage::FjallPersistPolicy;
use iroh::{EndpointAddr, TransportAddr};

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
    pub max_concurrent_uni_streams: Option<u64>,
    pub max_concurrent_bidi_streams: Option<u64>,
    pub document_sync_storage_path: Option<PathBuf>,
    pub document_sync_runtime: Option<IrohRuntimeConfig>,
    pub fjall_persist_policy: FjallPersistPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiscoveryMethod {
    None,
    N0Dns,
    CustomDns(Vec<String>),
    DhtSigned {
        ttl: Duration,
        refresh_after: Duration,
    },
    Ordered(Vec<DiscoveryMethod>),
}

impl DiscoveryMethod {
    pub fn ordered(methods: Vec<Self>) -> Self {
        let mut flattened = Vec::new();
        for method in methods {
            match method {
                Self::None => {}
                Self::Ordered(methods) => flattened.extend(methods),
                other => flattened.push(other),
            }
        }

        match flattened.len() {
            0 => Self::None,
            1 => flattened.remove(0),
            _ => Self::Ordered(flattened),
        }
    }

    pub fn enabled_methods(&self) -> Vec<String> {
        self.leaf_methods()
            .into_iter()
            .filter_map(|method| match method {
                Self::None | Self::Ordered(_) => None,
                Self::N0Dns => Some("n0_dns".to_string()),
                Self::CustomDns(_) => Some("custom_dns".to_string()),
                Self::DhtSigned { .. } => Some("dht_signed".to_string()),
            })
            .collect()
    }

    pub(crate) fn leaf_methods(&self) -> Vec<&DiscoveryMethod> {
        let mut methods = Vec::new();
        self.append_leaf_methods(&mut methods);
        methods
    }

    fn append_leaf_methods<'a>(&'a self, methods: &mut Vec<&'a DiscoveryMethod>) {
        match self {
            Self::Ordered(ordered) => {
                for method in ordered {
                    method.append_leaf_methods(methods);
                }
            }
            method => methods.push(method),
        }
    }

    pub(crate) fn dht_signed_config(&self) -> Option<(Duration, Duration)> {
        self.leaf_methods()
            .into_iter()
            .find_map(|method| match method {
                Self::DhtSigned { ttl, refresh_after } => Some((*ttl, *refresh_after)),
                _ => None,
            })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RelayMethod {
    None,
    N0,
    Custom(Vec<String>),
    N0WithCustom(Vec<String>),
}

impl RelayMethod {
    pub fn method_name(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::N0 => "n0",
            Self::Custom(_) => "custom",
            Self::N0WithCustom(_) => "n0+custom",
        }
    }

    pub fn relay_urls(&self) -> Vec<String> {
        match self {
            Self::Custom(relays) | Self::N0WithCustom(relays) => relays.clone(),
            _ => Vec::new(),
        }
    }

    pub fn with_additional_relays(self, additional: Vec<String>) -> Self {
        if additional.is_empty() {
            return self;
        }

        match self {
            Self::None => Self::Custom(unique_relay_urls(additional)),
            Self::N0 => Self::N0WithCustom(unique_relay_urls(additional)),
            Self::Custom(relays) => Self::Custom(merge_relay_urls(relays, additional)),
            Self::N0WithCustom(relays) => Self::N0WithCustom(merge_relay_urls(relays, additional)),
        }
    }
}

fn merge_relay_urls(mut relays: Vec<String>, additional: Vec<String>) -> Vec<String> {
    relays.extend(additional);
    unique_relay_urls(relays)
}

fn unique_relay_urls(relays: Vec<String>) -> Vec<String> {
    let mut unique = Vec::new();
    for relay in relays {
        let relay = relay.trim();
        if relay.is_empty() || unique.iter().any(|existing| existing == relay) {
            continue;
        }
        unique.push(relay.to_string());
    }
    unique
}

pub fn format_endpoint_config(endpoint_addr: &EndpointAddr) -> String {
    let mut parts = Vec::with_capacity(endpoint_addr.addrs.len() + 1);
    parts.push(endpoint_addr.id.to_string());
    parts.extend(endpoint_addr.addrs.iter().map(|addr| match addr {
        TransportAddr::Relay(url) => format!("relay:{url}"),
        TransportAddr::Ip(addr) => format!("ip:{addr}"),
        _ => format!("{addr}"),
    }));
    parts.join(";")
}

pub fn parse_endpoint_config(value: &str) -> std::result::Result<EndpointAddr, String> {
    let mut parts = value
        .split(';')
        .map(str::trim)
        .filter(|part| !part.is_empty());
    let node_id = parts
        .next()
        .ok_or_else(|| "missing endpoint id".to_string())?
        .parse::<iroh::PublicKey>()
        .map_err(|error| error.to_string())?;
    let mut addrs = Vec::new();
    for part in parts {
        if let Some(value) = part.strip_prefix("relay:") {
            addrs.push(TransportAddr::Relay(
                value
                    .parse::<iroh::RelayUrl>()
                    .map_err(|error| error.to_string())?,
            ));
        } else if let Some(value) = part.strip_prefix("ip:") {
            addrs.push(TransportAddr::Ip(
                SocketAddr::from_str(value).map_err(|error| error.to_string())?,
            ));
        } else if part.starts_with("http://") || part.starts_with("https://") {
            addrs.push(TransportAddr::Relay(
                part.parse::<iroh::RelayUrl>()
                    .map_err(|error| error.to_string())?,
            ));
        } else {
            addrs.push(TransportAddr::Ip(
                SocketAddr::from_str(part).map_err(|error| error.to_string())?,
            ));
        }
    }
    if addrs.is_empty() {
        return Err("endpoint address must include at least one relay or ip address".to_string());
    }
    Ok(EndpointAddr::from_parts(node_id, addrs))
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
            max_concurrent_bidi_streams: None,
            max_concurrent_uni_streams: None,
            document_sync_storage_path: None,
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
