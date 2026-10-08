//! Checks that both public descriptor routes of a realm return the registered descriptor.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_blob::egress::EgressGuard;
use aruna_core::federation::{RealmDescriptor, Signed, valid_federation_url};
use url::Url;

const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_BODY: usize = 64 * 1024;

/// True only when the API and portal routes both serve exactly this signed descriptor.
pub async fn verify_routes(egress: &EgressGuard, descriptor: &Signed<RealmDescriptor>) -> bool {
    let Some((api, portal)) = descriptor_urls(&descriptor.payload) else {
        return false;
    };
    let (api, portal) = tokio::join!(fetch(egress, api), fetch(egress, portal));
    api.as_ref() == Some(descriptor) && portal.as_ref() == Some(descriptor)
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

async fn fetch(egress: &EgressGuard, url: Url) -> Option<Signed<RealmDescriptor>> {
    let mut response = egress
        .request(url)
        .ok()?
        .timeout(FETCH_TIMEOUT)
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.ok()? {
        if body.len() + chunk.len() > MAX_BODY {
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).ok()
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
        assert!(verify_routes(&guard(), &descriptor).await);
    }

    #[tokio::test]
    async fn missing_portal_unverified() {
        // A realm whose portal route is not reachable stays unverified.
        let (listener, base) = bind().await;
        let descriptor = descriptor(&base);
        serve(listener, descriptor.clone(), None);
        assert!(!verify_routes(&guard(), &descriptor).await);
    }

    #[tokio::test]
    async fn other_descriptor_unverified() {
        // Routes serving another signed descriptor do not prove the registered one.
        let (listener, base) = bind().await;
        let other = descriptor("https://other.example.org");
        serve(listener, other.clone(), Some(other));
        assert!(!verify_routes(&guard(), &descriptor(&base)).await);
    }

    #[tokio::test]
    async fn strict_policy_unverified() {
        // The production policy never reaches loopback, so such a realm stays unverified.
        let (listener, base) = bind().await;
        let descriptor = descriptor(&base);
        serve(listener, descriptor.clone(), Some(descriptor.clone()));
        let strict = EgressGuard::new(EgressPolicy::strict()).unwrap();
        assert!(!verify_routes(&strict, &descriptor).await);
    }
}
