//! Checks that both public descriptor routes of a realm return the registered descriptor.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_blob::egress::EgressGuard;
use aruna_core::federation::{RealmDescriptor, Signed, valid_federation_url};
use url::Url;

const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_BODY: usize = 64 * 1024;

/// The reasons the descriptor routes do not prove `descriptor`; none means verified.
pub async fn verify_routes(
    egress: &EgressGuard,
    descriptor: &Signed<RealmDescriptor>,
) -> Vec<String> {
    let Some((api, portal)) = descriptor_urls(&descriptor.payload) else {
        return vec!["descriptor URLs are not valid federation URLs".to_string()];
    };
    let (api_check, portal_check) = tokio::join!(
        check(egress, api.clone(), descriptor),
        check(egress, portal.clone(), descriptor)
    );
    [(api, api_check), (portal, portal_check)]
        .into_iter()
        .filter_map(|(url, check)| check.err().map(|reason| format!("{url}: {reason}")))
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
) -> Result<(), String> {
    let served = fetch(egress, url).await?;
    if served == *descriptor {
        return Ok(());
    }
    if served.payload.realm_id != descriptor.payload.realm_id {
        return Err("serves the descriptor of another realm".to_string());
    }
    Err(format!(
        "serves another descriptor (issued_at {}, registered {})",
        served.payload.issued_at, descriptor.payload.issued_at
    ))
}

async fn fetch(egress: &EgressGuard, url: Url) -> Result<Signed<RealmDescriptor>, String> {
    let mut response = egress
        .request(url)
        .map_err(|error| format!("refused by the egress policy: {error}"))?
        .timeout(FETCH_TIMEOUT)
        .send()
        .await
        .map_err(|error| format!("request failed: {}", error_chain(&error)))?;
    if !response.status().is_success() {
        return Err(format!("answered {}", response.status()));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| format!("body failed: {}", error_chain(&error)))?
    {
        if body.len() + chunk.len() > MAX_BODY {
            return Err(format!("body exceeds {MAX_BODY} bytes"));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|error| format!("body is not a descriptor: {error}"))
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
        assert!(
            problems[0].contains("aruna-realm: answered 404"),
            "{problems:?}"
        );
    }

    #[tokio::test]
    async fn other_descriptor_unverified() {
        // Routes serving another signed descriptor do not prove the registered one.
        let (listener, base) = bind().await;
        let other = descriptor("https://other.example.org");
        serve(listener, other.clone(), Some(other));
        let problems = verify_routes(&guard(), &descriptor(&base)).await;
        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(problems[0].contains("another descriptor"), "{problems:?}");
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
        assert!(problems[0].contains("egress policy"), "{problems:?}");
    }
}
