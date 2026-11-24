//! The stateless applet gateway: serves every applet host from the node's edge routes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod hosts;
mod policy;
pub mod settings;
mod upstream;

use std::sync::Arc;
use std::time::Instant;

use crate::hosts::HostCache;
use crate::settings::Settings;
use crate::upstream::{Answer, Upstream};

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("invalid gateway settings: {0}")]
    Settings(String),
    #[error("the gateway listener failed")]
    Listen(#[source] std::io::Error),
}

/// What every request shares: the settings, the node client and the decision cache.
pub struct Gateway {
    settings: Settings,
    upstream: Upstream,
    hosts: HostCache,
}

impl Gateway {
    pub fn new(settings: Settings) -> Result<Arc<Self>, GatewayError> {
        let upstream = Upstream::new(settings.upstreams.clone(), settings.edge_key.clone())?;
        Ok(Arc::new(Self {
            hosts: HostCache::new(settings.host_cache),
            upstream,
            settings,
        }))
    }

    /// The decision for a normalized host, from the cache or the node.
    async fn decide(&self, host: &str) -> Result<Option<Arc<Answer>>, reqwest::Error> {
        if let Some(cached) = self.hosts.get(host, Instant::now()) {
            return Ok(cached);
        }
        let answer = self.upstream.decide(host).await?.map(Arc::new);
        self.hosts.put(host, answer.clone(), Instant::now());
        Ok(answer)
    }
}
