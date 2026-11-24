//! Follows a node's applet notices and drops the cached decisions they name.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use reqwest::{Method, StatusCode};
use serde::Deserialize;
use tracing::debug;

use crate::Gateway;
use crate::hosts::normalize;
use crate::upstream::Upstream;

/// The first wait after a failed poll; each further failure doubles it up to `RETRY_MAX`.
const RETRY_FIRST: Duration = Duration::from_secs(5);
const RETRY_MAX: Duration = Duration::from_secs(300);
/// The gap between polls, so a node that answers at once is not asked in a tight loop.
const POLL_GAP: Duration = Duration::from_secs(1);
/// A long poll the node holds open answers within this time.
const POLL_TIMEOUT: Duration = Duration::from_secs(120);

/// One page of `GET /edge/v1/notices?after=<cursor>`.
#[derive(Deserialize)]
struct NoticePage {
    cursor: String,
    #[serde(default)]
    reset: bool,
    #[serde(default)]
    notices: Vec<Notice>,
}

#[derive(Deserialize)]
struct Notice {
    applet_id: String,
    #[serde(default)]
    hosts: Vec<String>,
}

/// Polls node `node` for as long as the gateway runs. Until a node offers notices, the 30
/// second bound of cached decisions alone keeps them current.
pub async fn follow(gateway: Arc<Gateway>, node: usize) {
    let mut cursor: Option<String> = None;
    let mut delay = RETRY_FIRST;
    loop {
        let path = match &cursor {
            Some(after) => Upstream::with_query("/edge/v1/notices", &[("after", after)]),
            None => "/edge/v1/notices".to_string(),
        };
        let sent = (gateway.upstream.request(node, Method::GET, &path)).timeout(POLL_TIMEOUT);
        let outcome = match sent.send().await {
            Ok(response) if response.status() == StatusCode::GONE => {
                gateway.hosts.clear();
                cursor = None;
                Ok(())
            }
            Ok(response) if response.status().is_success() => {
                match response.json::<NoticePage>().await {
                    Ok(page) => {
                        apply(&gateway, &page);
                        cursor = Some(page.cursor);
                        Ok(())
                    }
                    Err(error) => Err(error.to_string()),
                }
            }
            Ok(response) => Err(format!("status {}", response.status())),
            Err(error) => Err(error.to_string()),
        };
        match outcome {
            Ok(()) => {
                delay = RETRY_FIRST;
                tokio::time::sleep(POLL_GAP).await;
            }
            Err(reason) => {
                debug!(node, %reason, "applet notices are not available");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(RETRY_MAX);
            }
        }
    }
}

fn apply(gateway: &Gateway, page: &NoticePage) {
    if page.reset {
        gateway.hosts.clear();
        return;
    }
    for notice in &page.notices {
        let hosts: Vec<String> = notice.hosts.iter().filter_map(|h| normalize(h)).collect();
        gateway.hosts.forget(&notice.applet_id, &hosts);
    }
}
