//! Calls the node's edge routes with the edge key: host decisions, files and runtime calls.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use reqwest::{Method, RequestBuilder, StatusCode};
use serde::Deserialize;

use crate::GatewayError;

/// How long the gateway waits for a node to start answering.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// What the edge serves now for an applet host.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Serving {
    Live,
    Preview,
    FeaturePaused,
    Paused,
    Gone,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Access {
    Private,
    GroupMembers,
    Public,
}

/// The node's answer for one host, read from `GET /edge/v1/hosts/{host}`.
#[derive(Clone, Debug, Deserialize)]
pub struct Decision {
    pub applet_id: String,
    #[serde(default)]
    pub host_node: String,
    pub custom_domain: bool,
    pub serving: Serving,
    pub version: Option<u32>,
    pub access: Access,
    #[serde(default)]
    pub frame_ancestors: Vec<String>,
    #[serde(default)]
    pub guests: bool,
    #[serde(default)]
    pub guests_in_frames: bool,
    pub portal_origin: Option<String>,
    pub max_age_s: u32,
}

/// A host decision with the node that gave it; that node also serves the applet's files.
#[derive(Clone, Debug)]
pub struct Answer {
    pub decision: Decision,
    pub node: usize,
}

/// The node API client of the gateway.
#[derive(Clone)]
pub struct Upstream {
    client: reqwest::Client,
    bases: Vec<String>,
    key: String,
}

impl Upstream {
    pub fn new(bases: Vec<String>, key: String) -> Result<Self, GatewayError> {
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| GatewayError::Settings(error.to_string()))?;
        Ok(Self { client, bases, key })
    }

    pub fn nodes(&self) -> usize {
        self.bases.len()
    }

    /// A request to `path` on node `node`, carrying the edge key.
    pub fn request(&self, node: usize, method: Method, path: &str) -> RequestBuilder {
        let base = self.bases.get(node).map(String::as_str).unwrap_or_default();
        (self.client.request(method, format!("{base}{path}"))).header("x-aruna-edge-key", &self.key)
    }

    /// `path` with the encoded query `pairs`.
    pub fn with_query(path: &str, pairs: &[(&str, &str)]) -> String {
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        query.extend_pairs(pairs);
        format!("{path}?{}", query.finish())
    }

    /// Asks each node in order; the first that knows the host answers. `None` means no node
    /// serves it, and an error means no node could say.
    pub async fn decide(&self, host: &str) -> Result<Option<Answer>, reqwest::Error> {
        let mut failure = None;
        let mut missing = false;
        for node in 0..self.bases.len() {
            let sent = self.request(node, Method::GET, &format!("/edge/v1/hosts/{host}"));
            let response = match sent.send().await {
                Ok(response) => response,
                Err(error) => {
                    failure = Some(error);
                    continue;
                }
            };
            match response.status() {
                StatusCode::NOT_FOUND => missing = true,
                status if status.is_success() => {
                    let decision = response.json::<Decision>().await?;
                    return Ok(Some(Answer { decision, node }));
                }
                _ => failure = response.error_for_status().err(),
            }
        }
        match (missing, failure) {
            (false, Some(error)) => Err(error),
            _ => Ok(None),
        }
    }
}
