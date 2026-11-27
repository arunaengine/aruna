//! Reads the gateway settings from the environment: node URLs, the edge key and the listener.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;

use crate::GatewayError;

/// Hosts the decision cache keeps when the operator sets no other number.
const DEFAULT_HOSTS: usize = 10_000;
/// The listener when the operator names none; a pod sets `0.0.0.0:8090`.
const DEFAULT_LISTEN: &str = "127.0.0.1:8090";

/// Everything the gateway holds: it keeps no applet state of its own.
#[derive(Clone)]
pub struct Settings {
    /// The key the node expects in `X-Aruna-Edge-Key`.
    pub edge_key: String,
    /// Node API base URLs, asked in order for a host decision.
    pub upstreams: Vec<String>,
    pub listen: SocketAddr,
    /// A proxy in front whose `X-Forwarded-For` names the client.
    pub trusted_proxy: Option<IpAddr>,
    /// Hosts the decision cache keeps at most.
    pub host_cache: NonZeroUsize,
}

// Shows everything except the edge key.
impl fmt::Debug for Settings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Settings")
            .field("upstreams", &self.upstreams)
            .field("listen", &self.listen)
            .field("trusted_proxy", &self.trusted_proxy)
            .field("host_cache", &self.host_cache)
            .finish_non_exhaustive()
    }
}

impl Settings {
    pub fn from_env() -> Result<Self, GatewayError> {
        Self::read(|name| std::env::var(name).ok())
    }

    /// Parses the settings from `lookup`, which returns one environment value by name.
    pub fn read(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, GatewayError> {
        let invalid = |reason: &str| GatewayError::Settings(reason.to_string());
        let edge_key = lookup("APPLETS_EDGE_KEY")
            .filter(|key| !key.is_empty())
            .ok_or_else(|| invalid("APPLETS_EDGE_KEY is not set"))?;
        let upstreams: Vec<String> = (lookup("ARUNA_GATEWAY_UPSTREAMS").unwrap_or_default())
            .split(',')
            .map(|base| base.trim().trim_end_matches('/').to_string())
            .filter(|base| !base.is_empty())
            .collect();
        if upstreams.is_empty() {
            return Err(invalid("ARUNA_GATEWAY_UPSTREAMS names no node URL"));
        }
        if let Some(base) = upstreams.iter().find(|base| url::Url::parse(base).is_err()) {
            return Err(invalid(&format!("upstream {base} is not a URL")));
        }
        let listen = (lookup("ARUNA_GATEWAY_LISTEN").unwrap_or(DEFAULT_LISTEN.to_string()))
            .parse()
            .map_err(|_| invalid("ARUNA_GATEWAY_LISTEN is not an address and port"))?;
        let trusted_proxy = match lookup("ARUNA_GATEWAY_TRUSTED_PROXY") {
            Some(proxy) => Some(
                proxy
                    .parse()
                    .map_err(|_| invalid("ARUNA_GATEWAY_TRUSTED_PROXY is not an IP address"))?,
            ),
            None => None,
        };
        let host_cache = match lookup("ARUNA_GATEWAY_HOST_CACHE") {
            Some(count) => count
                .parse()
                .map_err(|_| invalid("ARUNA_GATEWAY_HOST_CACHE is not a positive number"))?,
            None => NonZeroUsize::new(DEFAULT_HOSTS).unwrap_or(NonZeroUsize::MIN),
        };
        Ok(Self {
            edge_key,
            upstreams,
            listen,
            trusted_proxy,
            host_cache,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(pairs: &[(&str, &str)]) -> Result<Settings, GatewayError> {
        Settings::read(|name| {
            (pairs.iter())
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        })
    }

    #[test]
    fn reads_required_values() {
        let settings = read(&[
            ("APPLETS_EDGE_KEY", "secret"),
            (
                "ARUNA_GATEWAY_UPSTREAMS",
                "http://node-1:8080/, http://node-2:8080",
            ),
        ])
        .expect("valid settings");
        assert_eq!(
            settings.upstreams,
            ["http://node-1:8080", "http://node-2:8080"]
        );
        assert_eq!(settings.listen.to_string(), DEFAULT_LISTEN);
        assert!(!format!("{settings:?}").contains("secret"));
        assert!(read(&[("ARUNA_GATEWAY_UPSTREAMS", "http://node:8080")]).is_err());
        assert!(read(&[("APPLETS_EDGE_KEY", "secret")]).is_err());
        let bad_cache = [
            ("APPLETS_EDGE_KEY", "secret"),
            ("ARUNA_GATEWAY_UPSTREAMS", "http://node:8080"),
            ("ARUNA_GATEWAY_HOST_CACHE", "0"),
        ];
        assert!(read(&bad_cache).is_err());
    }
}
