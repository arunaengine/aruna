//! Transfers Invenio JSON and file bodies through the node's screened HTTP client.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::IpAddr;
use std::time::Duration;

use aruna_core::errors::StagingSourceError;
use aruna_core::invenio::{REFERENCE_FILE, REFERENCE_RECORD};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::source_access::{ResolvedSourceAccess, SourceMetadata};
use bytes::Bytes;
use reqwest::{Method, Response, Url};
use serde_json::Value;
use thiserror::Error;

use crate::blob::BlobHandle;
use crate::egress::EgressGuard;

const REDIRECT_HOPS: usize = 5;
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
    #[error("repository redirected too often")]
    Redirects,
    #[error("repository tokens require HTTPS outside loopback")]
    InsecureToken,
}

pub struct InvenioClient<'a> {
    egress: &'a EgressGuard,
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
        Self::with_guard(blob.egress(), endpoint, token, metadata_limit)
    }

    fn with_guard(
        egress: &'a EgressGuard,
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
        if token.is_some() && !secure_transport(&endpoint) {
            return Err(InvenioError::InsecureToken);
        }
        let path = format!("{}/", endpoint.path().trim_end_matches('/'));
        endpoint.set_path(&path);
        Ok(Self {
            egress,
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

    // File bodies rely on the egress idle timeout; only metadata calls set a total deadline.
    fn request(&self, method: Method, url: Url) -> Result<reqwest::RequestBuilder, InvenioError> {
        self.link(url.as_str())?;
        let mut request = self
            .egress
            .repository_request(method, url)
            .map_err(|_| InvenioError::Egress)?;
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

    pub async fn update(
        &self,
        url: Url,
        body: &Value,
        revision: u64,
    ) -> Result<Value, InvenioError> {
        let response = self
            .request(Method::PUT, url)?
            .header("Accept", JSON_ACCEPT)
            .header("If-Match", revision.to_string())
            .json(body)
            .timeout(Duration::from_secs(120))
            .send()
            .await
            .map_err(|_| InvenioError::Transport)?;
        self.read_json(response).await
    }

    pub async fn download(&self, url: Url) -> Result<Response, InvenioError> {
        self.content(Method::GET, url, None, None).await
    }

    /// Follows storage redirects for file content only and screens every hop.
    /// The token is sent only while the target stays on the repository origin.
    async fn content(
        &self,
        method: Method,
        url: Url,
        timeout: Option<Duration>,
        range: Option<&std::ops::Range<u64>>,
    ) -> Result<Response, InvenioError> {
        let mut url = self.link(url.as_str())?;
        let mut authorized = true;
        for _ in 0..=REDIRECT_HOPS {
            authorized &= url.origin() == self.endpoint.origin();
            let mut request = self
                .egress
                .repository_request(method.clone(), url.clone())
                .map_err(|_| InvenioError::Egress)?
                .header("Accept", "*/*");
            if let Some(timeout) = timeout {
                request = request.timeout(timeout);
            }
            if let Some(range) = range {
                let last = range.end.saturating_sub(1);
                request = request.header("Range", format!("bytes={}-{last}", range.start));
            }
            if let Some(token) = self.token.as_ref().filter(|_| authorized) {
                request = request.bearer_auth(token);
            }
            let response = request.send().await.map_err(|_| InvenioError::Transport)?;
            if !matches!(response.status().as_u16(), 301 | 302 | 303 | 307 | 308) {
                check_status(&response)?;
                return Ok(response);
            }
            let next = response
                .headers()
                .get("location")
                .and_then(|location| location.to_str().ok())
                .and_then(|location| url.join(location).ok())
                .ok_or(InvenioError::InvalidUrl)?;
            if !matches!(next.scheme(), "https" | "http")
                || (url.scheme() == "https" && next.scheme() != "https")
                || !next.username().is_empty()
                || next.password().is_some()
            {
                return Err(InvenioError::InvalidUrl);
            }
            url = next;
        }
        Err(InvenioError::Redirects)
    }

    pub async fn head(
        &self,
        url: Url,
    ) -> Result<aruna_core::structs::execution::source_access::SourceMetadata, InvenioError> {
        let response = self
            .content(Method::HEAD, url, Some(Duration::from_secs(120)), None)
            .await?;
        let headers = response.headers();
        let text = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());
        Ok(
            aruna_core::structs::execution::source_access::SourceMetadata {
                content_length: text("content-length")
                    .and_then(|value| value.parse().ok())
                    .ok_or(InvenioError::Json)?,
                content_type: text("content-type").map(str::to_string),
                etag: text("etag").map(str::to_string),
                last_modified: text("last-modified")
                    .and_then(|value| chrono::DateTime::parse_from_rfc2822(value).ok())
                    .map(Into::into),
                source_version: None,
            },
        )
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

/// Reads a referenced record file with the connector token, following screened storage redirects.
pub(crate) async fn head_reference(
    guard: &EgressGuard,
    access: &ResolvedSourceAccess,
) -> Result<SourceMetadata, StagingSourceError> {
    let (client, url) = reference_client(guard, access)?;
    client.head(url).await.map_err(reference_error)
}

pub(crate) async fn read_reference(
    guard: &EgressGuard,
    access: &ResolvedSourceAccess,
    range: Option<std::ops::Range<u64>>,
) -> Result<(SourceMetadata, BackendStream<Result<Bytes, StreamError>>), StagingSourceError> {
    let (client, url) = reference_client(guard, access)?;
    let metadata = client.head(url.clone()).await.map_err(reference_error)?;
    if range.as_ref().is_some_and(|range| range.start >= range.end) {
        return Err(StagingSourceError::ReadError("empty range".into()));
    }
    let response = client
        .content(Method::GET, url, None, range.as_ref())
        .await
        .map_err(reference_error)?;
    // A server that ignores the range would return the whole file at the wrong offset.
    if range.is_some() && response.status().as_u16() != 206 {
        return Err(StagingSourceError::ReadError(
            "repository ignored the byte range".into(),
        ));
    }
    Ok((metadata, BackendStream::new(response.bytes_stream())))
}

fn reference_client<'a>(
    guard: &'a EgressGuard,
    access: &ResolvedSourceAccess,
) -> Result<(InvenioClient<'a>, Url), StagingSourceError> {
    let ResolvedSourceAccess::OpenDal { config, .. } = access;
    let value = |key: &str| {
        config
            .get(key)
            .ok_or_else(|| StagingSourceError::OperatorCreationFailed(format!("missing {key}")))
    };
    let client =
        InvenioClient::with_guard(guard, value("endpoint")?, config.get("token").cloned(), 0)
            .map_err(|error| StagingSourceError::OperatorCreationFailed(error.to_string()))?;
    let url = client
        .url(&[
            "records",
            value(REFERENCE_RECORD)?,
            "files",
            value(REFERENCE_FILE)?,
            "content",
        ])
        .map_err(|error| StagingSourceError::OperatorCreationFailed(error.to_string()))?;
    Ok((client, url))
}

fn reference_error(error: InvenioError) -> StagingSourceError {
    match error {
        InvenioError::Status(404 | 410) => StagingSourceError::NotFound,
        InvenioError::Status(401 | 403) => StagingSourceError::AccessDenied,
        error => StagingSourceError::ReadError(error.to_string()),
    }
}

fn secure_transport(url: &Url) -> bool {
    let host = url.host_str().unwrap_or_default();
    url.scheme() == "https"
        || host.eq_ignore_ascii_case("localhost")
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn check_status(response: &Response) -> Result<(), InvenioError> {
    if response.status().is_success() {
        Ok(())
    } else {
        Err(InvenioError::Status(response.status().as_u16()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_plain_token() {
        for (endpoint, secure) in [
            ("https://zenodo.org/api/", true),
            ("http://127.0.0.2:5000/api/", true),
            ("http://[::1]/api/", true),
            ("http://LOCALHOST/api/", true),
            ("http://zenodo.org/api/", false),
            ("http://10.0.0.1/api/", false),
            ("http://localhost.example.org/api/", false),
        ] {
            assert_eq!(
                secure_transport(&Url::parse(endpoint).unwrap()),
                secure,
                "{endpoint}"
            );
        }
    }
}
