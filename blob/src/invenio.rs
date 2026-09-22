//! Transfers Invenio JSON and file bodies through the node's screened HTTP client.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::stream::{BackendStream, StreamError};
use bytes::Bytes;
use reqwest::{Method, Response, Url};
use serde_json::Value;
use thiserror::Error;

use crate::blob::BlobHandle;

const JSON_ACCEPT: &str = "application/vnd.inveniordm.v1+json, application/json;q=0.9";

#[derive(Debug, Error)]
pub enum InvenioError {
    #[error("invalid repository URL or cross-origin link")]
    InvalidUrl,
    #[error("repository request refused by egress policy")]
    Egress,
    #[error("repository transport failed")]
    Transport,
    #[error("repository returned HTTP {0}")]
    Status(u16),
    #[error("repository response exceeds the metadata limit")]
    Limit,
    #[error("repository returned invalid JSON")]
    Json,
}

pub struct InvenioClient<'a> {
    blob: &'a BlobHandle,
    endpoint: Url,
    token: Option<String>,
    metadata_limit: u64,
}

impl<'a> InvenioClient<'a> {
    pub fn new(
        blob: &'a BlobHandle,
        endpoint: &str,
        token: Option<String>,
        metadata_limit: u64,
    ) -> Result<Self, InvenioError> {
        let mut endpoint = Url::parse(endpoint).map_err(|_| InvenioError::InvalidUrl)?;
        if !matches!(endpoint.scheme(), "https" | "http")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(InvenioError::InvalidUrl);
        }
        let path = format!("{}/", endpoint.path().trim_end_matches('/'));
        endpoint.set_path(&path);
        Ok(Self {
            blob,
            endpoint,
            token,
            metadata_limit,
        })
    }

    pub fn endpoint(&self) -> &str {
        self.endpoint.as_str()
    }

    pub fn url(&self, segments: &[&str]) -> Result<Url, InvenioError> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .map_err(|_| InvenioError::InvalidUrl)?
            .pop_if_empty()
            .extend(segments);
        Ok(url)
    }

    pub fn link(&self, link: &str) -> Result<Url, InvenioError> {
        let url = self
            .endpoint
            .join(link)
            .map_err(|_| InvenioError::InvalidUrl)?;
        if url.origin() != self.endpoint.origin()
            || !url.path().starts_with(self.endpoint.path())
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(InvenioError::InvalidUrl);
        }
        Ok(url)
    }

    fn request(&self, method: Method, url: Url) -> Result<reqwest::RequestBuilder, InvenioError> {
        self.link(url.as_str())?;
        let mut request = self
            .blob
            .repository_request(method, url)
            .map_err(|_| InvenioError::Egress)?
            .timeout(Duration::from_secs(1800));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        Ok(request)
    }

    pub async fn json(
        &self,
        method: Method,
        url: Url,
        body: Option<&Value>,
    ) -> Result<Value, InvenioError> {
        let mut request = self
            .request(method, url)?
            .header("Accept", JSON_ACCEPT)
            .timeout(Duration::from_secs(120));
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await.map_err(|_| InvenioError::Transport)?;
        self.read_json(response).await
    }

    async fn read_json(&self, mut response: Response) -> Result<Value, InvenioError> {
        check_status(&response)?;
        if response
            .content_length()
            .is_some_and(|size| size > self.metadata_limit)
        {
            return Err(InvenioError::Limit);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| InvenioError::Transport)?
        {
            if chunk.len() as u64 > self.metadata_limit.saturating_sub(body.len() as u64) {
                return Err(InvenioError::Limit);
            }
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&body).map_err(|_| InvenioError::Json)
    }

    pub async fn download(&self, url: Url) -> Result<Response, InvenioError> {
        let response = self
            .request(Method::GET, url)?
            .header("Accept", "application/octet-stream")
            .send()
            .await
            .map_err(|_| InvenioError::Transport)?;
        check_status(&response)?;
        Ok(response)
    }

    pub async fn upload(
        &self,
        url: Url,
        size: u64,
        stream: BackendStream<Result<Bytes, StreamError>>,
    ) -> Result<(), InvenioError> {
        let response = self
            .request(Method::PUT, url)?
            .header("Accept", JSON_ACCEPT)
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", size)
            .body(reqwest::Body::wrap_stream(stream))
            .send()
            .await
            .map_err(|_| InvenioError::Transport)?;
        check_status(&response)
    }
}

fn check_status(response: &Response) -> Result<(), InvenioError> {
    if response.status().is_success() {
        Ok(())
    } else {
        Err(InvenioError::Status(response.status().as_u16()))
    }
}
