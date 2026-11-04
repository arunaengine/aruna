//! Sends authenticated REST requests to the signed-in node or to another node of the realm.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::{ApiError, CliError};
use reqwest::{Method, RequestBuilder, Response};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use url::Url;

/// One node's REST API with the caller's bearer token.
#[derive(Clone, Debug)]
pub struct Api {
    http: reqwest::Client,
    base: Url,
    token: Option<String>,
    pub node_id: Option<String>,
}

impl Api {
    pub fn new(base: &str, token: Option<String>) -> Result<Self, CliError> {
        let http = reqwest::Client::builder()
            .user_agent(concat!("aruna-cli/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self {
            http,
            base: api_base(base)?,
            token,
            node_id: None,
        })
    }

    pub fn base(&self) -> &str {
        self.base.as_str()
    }

    /// The same caller on another node's API base.
    pub fn with_base(&self, base: &str, node_id: &str) -> Result<Self, CliError> {
        Ok(Self {
            http: self.http.clone(),
            base: api_base(base)?,
            token: self.token.clone(),
            node_id: Some(node_id.to_string()),
        })
    }

    /// The same node without a token, for reads that should only see public data.
    pub fn anonymous(&self) -> Self {
        Self {
            token: None,
            ..self.clone()
        }
    }

    pub fn request(&self, method: Method, path: &str) -> Result<RequestBuilder, CliError> {
        let url = self
            .base
            .join(path.trim_start_matches('/'))
            .map_err(|error| CliError::usage(format!("bad API path {path}: {error}")))?;
        let builder = self.http.request(method, url);
        Ok(match &self.token {
            Some(token) => builder.bearer_auth(token),
            None => builder,
        })
    }

    /// A request to an absolute URL the API returned, such as a job's status URL.
    pub fn request_url(&self, method: Method, url: &str) -> Result<RequestBuilder, CliError> {
        let url =
            Url::parse(url).map_err(|error| CliError::usage(format!("bad URL {url}: {error}")))?;
        let builder = self.http.request(method, url);
        Ok(match &self.token {
            Some(token) => builder.bearer_auth(token),
            None => builder,
        })
    }

    pub async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T, CliError> {
        read_json(self.request(Method::GET, path)?.query(query).send().await?).await
    }

    pub async fn send<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<T, CliError> {
        read_json(self.request(method, path)?.json(body).send().await?).await
    }

    pub async fn delete(&self, path: &str) -> Result<(), CliError> {
        check(self.request(Method::DELETE, path)?.send().await?).await?;
        Ok(())
    }

    /// The API base a realm node publishes; the signed-in node keeps this base.
    pub async fn for_node(&self, node_id: &str) -> Result<Self, CliError> {
        if self.node_id.as_deref() == Some(node_id) {
            return Ok(self.clone());
        }
        let realm: RealmNodes = self.get("system/realm", &[]).await?;
        let url = realm
            .nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .and_then(|node| node.info.as_ref()?.urls.api.clone())
            .ok_or_else(|| {
                CliError::routing(format!(
                    "node {node_id} publishes no API address, so the CLI cannot reach it"
                ))
            })?;
        self.with_base(&url, node_id)
    }
}

/// Accepts a node origin or its `/api/v1` base and returns the base with a trailing slash.
pub fn api_base(raw: &str) -> Result<Url, CliError> {
    let mut url = Url::parse(raw.trim())
        .map_err(|error| CliError::usage(format!("invalid API URL {raw}: {error}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(CliError::usage("the API URL must use http or https"));
    }
    let path = url.path().trim_end_matches('/').to_string();
    let path = match path.ends_with("/api/v1") {
        true => format!("{path}/"),
        false => format!("{path}/api/v1/"),
    };
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

/// The JSON body of a successful answer, or the mapped API error.
pub async fn read_json<T: DeserializeOwned>(response: Response) -> Result<T, CliError> {
    let response = check(response).await?;
    let bytes = response.bytes().await?;
    // A 204 or empty body reads as JSON null, so `()` and `Option` targets work.
    let bytes = if bytes.is_empty() {
        &b"null"[..]
    } else {
        &bytes
    };
    Ok(serde_json::from_slice(bytes)?)
}

/// Passes a successful response on and turns any other into an `ApiError`.
pub async fn check(response: Response) -> Result<Response, CliError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let path = response.url().path().to_string();
    let text = response.text().await.unwrap_or_default();
    Err(ApiError::from_body(status.as_u16(), &path, &text).into())
}

#[derive(Debug, Deserialize)]
struct RealmNodes {
    #[serde(default)]
    nodes: Vec<RealmNode>,
}

#[derive(Debug, Deserialize)]
struct RealmNode {
    node_id: String,
    info: Option<NodeDocument>,
}

#[derive(Debug, Deserialize)]
struct NodeDocument {
    urls: NodeUrls,
}

#[derive(Debug, Default, Deserialize)]
struct NodeUrls {
    api: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_api_base() {
        for raw in [
            "https://node.example",
            "https://node.example/",
            "https://node.example/api/v1",
            "https://node.example/api/v1/",
        ] {
            assert_eq!(
                api_base(raw).unwrap().as_str(),
                "https://node.example/api/v1/"
            );
        }
        assert_eq!(
            api_base("http://127.0.0.1:8080/sub").unwrap().as_str(),
            "http://127.0.0.1:8080/sub/api/v1/"
        );
        assert!(api_base("ftp://node.example").is_err());
        assert!(api_base("not a url").is_err());
    }
}
