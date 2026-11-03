//! Checks that both public descriptor routes of a realm return the registered descriptor.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_blob::egress::EgressGuard;
use aruna_core::federation::{RealmDescriptor, Signed, valid_federation_url};
use serde::{Deserialize, Serialize};
use url::Url;

const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_BODY: usize = 64 * 1024;

/// One descriptor route that did not prove the registered descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteProblem {
    /// `api`, `portal`, or `descriptor` when the URLs themselves are invalid.
    pub route: String,
    /// A short reason from a fixed set, safe to show publicly.
    pub reason: String,
    /// The URL and the full error chain; only registry admins see it.
    pub detail: String,
}

impl RouteProblem {
    /// The public form: route and short reason, never addresses or error chains.
    pub fn summary(&self) -> String {
        format!("{} route: {}", self.route, self.reason)
    }
}

/// A failed check: the public short reason and the private detail.
struct Failure {
    reason: String,
    detail: String,
}

impl Failure {
    fn new(reason: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            detail: detail.into(),
        }
    }
}

/// The problems of the descriptor routes for `descriptor`; none means verified.
pub async fn verify_routes(
    egress: &EgressGuard,
    descriptor: &Signed<RealmDescriptor>,
) -> Vec<RouteProblem> {
    let Some((api, portal)) = descriptor_urls(&descriptor.payload) else {
        return vec![RouteProblem {
            route: "descriptor".to_string(),
            reason: "URLs are not valid federation URLs".to_string(),
            detail: String::new(),
        }];
    };
    let (api_check, portal_check) = tokio::join!(
        check(egress, api.clone(), descriptor),
        check(egress, portal.clone(), descriptor)
    );
    [("api", api, api_check), ("portal", portal, portal_check)]
        .into_iter()
        .filter_map(|(route, url, check)| {
            check.err().map(|failure| RouteProblem {
                route: route.to_string(),
                reason: failure.reason,
                detail: format!("{url}: {}", failure.detail),
            })
        })
        .collect()
}

fn descriptor_urls(descriptor: &RealmDescriptor) -> Option<(Url, Url)> {
    if !valid_federation_url(&descriptor.api_url) || !valid_federation_url(&descriptor.portal_url) {
        return None;
    }
    let mut api = descriptor.api_url.clone();
    if !api.path().ends_with('/') {
        api.set_path(&format!("{}/", api.path()));
    }
    Some((
        api.join("system/realm/descriptor").ok()?,
        descriptor
            .portal_url
            .join("/.well-known/aruna-realm")
            .ok()?,
    ))
}

async fn check(
    egress: &EgressGuard,
    url: Url,
    descriptor: &Signed<RealmDescriptor>,
) -> Result<(), Failure> {
    let served = fetch(egress, url).await?;
    if served == *descriptor {
        return Ok(());
    }
    if served.payload.realm_id != descriptor.payload.realm_id {
        let reason = "serves the descriptor of another realm";
        return Err(Failure::new(reason, reason));
    }
    Err(Failure::new(
        "serves another descriptor",
        format!(
            "serves issued_at {}, registered {}",
            served.payload.issued_at, descriptor.payload.issued_at
        ),
    ))
}

/// The short reason for a failed request; reqwest knows timeouts and connect failures.
fn request_reason(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "request timed out"
    } else if error.is_connect() {
        "connection failed"
    } else if error.is_redirect() {
        "redirect refused"
    } else {
        "request failed"
    }
}

async fn fetch(egress: &EgressGuard, url: Url) -> Result<Signed<RealmDescriptor>, Failure> {
    let mut response = egress
        .request(url)
        .map_err(|error| Failure::new("refused by the egress policy", error.to_string()))?
        .timeout(FETCH_TIMEOUT)
        .send()
        .await
        .map_err(|error| Failure::new(request_reason(&error), error_chain(&error)))?;
    if !response.status().is_success() {
        let status = response.status().as_u16();
        return Err(Failure::new(
            format!("answered {status}"),
            format!("answered {status}"),
        ));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| Failure::new("body failed", error_chain(&error)))?
    {
        if body.len() + chunk.len() > MAX_BODY {
            let detail = format!("body exceeds {MAX_BODY} bytes");
            return Err(Failure::new("body too large", detail));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body)
        .map_err(|error| Failure::new("body is not a descriptor", error.to_string()))
}

/// The error with its sources, since reqwest keeps the connect cause in the chain.
fn error_chain(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::tests::registration;
    use aruna_core::egress::EgressPolicy;
    use axum::routing::get;
    use axum::{Json, Router};
    use tokio::net::TcpListener;

    async fn bind() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        (listener, base)
    }

    /// Serves the two descriptor routes; a missing portal descriptor answers 404.
    fn serve(
        listener: TcpListener,
        api: Signed<RealmDescriptor>,
        portal: Option<Signed<RealmDescriptor>>,
    ) {
        let mut router = Router::new().route(
            "/api/v1/system/realm/descriptor",
            get(move || async move { Json(api) }),
        );
        if let Some(portal) = portal {
            router = router.route(
                "/.well-known/aruna-realm",
                get(move || async move { Json(portal) }),
            );
        }
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    }

    fn guard() -> EgressGuard {
        EgressGuard::new(EgressPolicy::loopback()).unwrap()
    }

    fn descriptor(base: &str) -> Signed<RealmDescriptor> {
        registration(base, 1).payload.descriptor
    }

    #[tokio::test]
    async fn both_routes_verify() {
        let (listener, base) = bind().await;
        let descriptor = descriptor(&base);
        serve(listener, descriptor.clone(), Some(descriptor.clone()));
        assert!(verify_routes(&guard(), &descriptor).await.is_empty());
    }

    #[tokio::test]
    async fn missing_portal_unverified() {
        // A realm whose portal route is not reachable stays unverified.
        let (listener, base) = bind().await;
        let descriptor = descriptor(&base);
        serve(listener, descriptor.clone(), None);
        let problems = verify_routes(&guard(), &descriptor).await;
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert_eq!(problems[0].summary(), "portal route: answered 404");
        assert!(problems[0].detail.contains("/.well-known/aruna-realm"));
    }

    #[tokio::test]
    async fn other_descriptor_unverified() {
        // Routes serving another signed descriptor do not prove the registered one.
        let (listener, base) = bind().await;
        let other = descriptor("https://other.example.org");
        serve(listener, other.clone(), Some(other));
        let problems = verify_routes(&guard(), &descriptor(&base)).await;
        assert_eq!(problems.len(), 2, "{problems:?}");
        assert_eq!(problems[0].reason, "serves another descriptor");
    }

    #[tokio::test]
    async fn strict_policy_unverified() {
        // The production policy never reaches loopback, so such a realm stays unverified.
        let (listener, base) = bind().await;
        let descriptor = descriptor(&base);
        serve(listener, descriptor.clone(), Some(descriptor.clone()));
        let strict = EgressGuard::new(EgressPolicy::strict()).unwrap();
        let problems = verify_routes(&strict, &descriptor).await;
        assert_eq!(problems.len(), 2, "{problems:?}");
        // The public reason names no address; the detail keeps it for admins.
        assert_eq!(
            problems[0].summary(),
            "api route: refused by the egress policy"
        );
        assert!(!problems[0].reason.contains("127.0.0.1"));
        assert!(problems[0].detail.contains("127.0.0.1"), "{problems:?}");
    }
}
