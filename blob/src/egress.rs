//! Connect-time enforcement of the egress policy: a URL check alone is
//! defeated by redirects and DNS rebinding, so tenant traffic runs through
//! clients that can only ever reach vetted addresses.

use crate::error::BlobLibError;
use aruna_core::egress::{EgressError, EgressPolicy};
use opendal::layers::HttpClientLayer;
use opendal::raw::{HttpBody, HttpClient, HttpFetch};
use opendal::{Buffer, ErrorKind};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use reqwest::{Url, redirect};
use std::fmt;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Idle bound, not a deadline: it resets on every byte, so a stalled tenant
/// endpoint is dropped while a multi-gigabyte transfer keeps running.
const READ_TIMEOUT: Duration = Duration::from_secs(60);
const REDIRECT_HOPS: usize = 5;

type LookupFuture = Pin<Box<dyn Future<Output = io::Result<Vec<SocketAddr>>> + Send>>;
type Lookup = Arc<dyn Fn(String) -> LookupFuture + Send + Sync>;
type BoxedError = Box<dyn std::error::Error + Send + Sync>;

fn system_lookup() -> Lookup {
    Arc::new(|host: String| {
        Box::pin(async move { Ok(tokio::net::lookup_host((host.as_str(), 0)).await?.collect()) })
    })
}

/// Screens a literal host. A name returns `Ok` here and is screened again
/// against its resolved addresses when the connection is opened.
fn screen_host(policy: &EgressPolicy, host: &str) -> Result<(), EgressError> {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    match host.parse::<IpAddr>() {
        Ok(address) => policy.check(address),
        Err(_) => Ok(()),
    }
}

struct ScreenedResolver {
    policy: EgressPolicy,
    lookup: Lookup,
}

impl Resolve for ScreenedResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let policy = self.policy.clone();
        let lookup = self.lookup.clone();
        let host = name.as_str().to_string();
        Box::pin(async move {
            let resolved = lookup(host.clone()).await.map_err(|error| {
                Box::new(EgressError::ResolveFailed {
                    host: host.clone(),
                    reason: error.to_string(),
                }) as BoxedError
            })?;
            let allowed: Vec<SocketAddr> = resolved
                .into_iter()
                .filter(|address| policy.check(address.ip()).is_ok())
                .collect();
            if allowed.is_empty() {
                return Err(Box::new(EgressError::NoAllowedAddress(host)) as BoxedError);
            }
            Ok(Box::new(allowed.into_iter()) as Addrs)
        })
    }
}

/// Screens opendal's own request targets, which include IP-literal credential
/// endpoints that hyper connects to without consulting the DNS resolver.
#[derive(Clone)]
struct ScreenedFetch {
    client: reqwest::Client,
    policy: EgressPolicy,
}

fn denied(error: EgressError) -> opendal::Error {
    opendal::Error::new(
        ErrorKind::PermissionDenied,
        "egress policy denied the target",
    )
    .set_source(error)
}

impl HttpFetch for ScreenedFetch {
    async fn fetch(
        &self,
        request: http::Request<Buffer>,
    ) -> opendal::Result<http::Response<HttpBody>> {
        // reqwest's URL parser reads `2852039166` and `127.1` as addresses the
        // raw substring never shows, so the screen runs on the parsed host.
        let uri = request.uri().to_string();
        let Some(host) = Url::parse(&uri)
            .ok()
            .and_then(|url| url.host_str().map(str::to_string))
        else {
            return Err(denied(EgressError::MissingHost(uri)));
        };
        screen_host(&self.policy, &host).map_err(denied)?;
        self.client.fetch(request).await
    }
}

/// Bundles the egress policy with the clients that enforce it.
#[derive(Clone)]
pub struct EgressGuard {
    policy: EgressPolicy,
    opendal: reqwest::Client,
    plain: reqwest::Client,
}

impl fmt::Debug for EgressGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EgressGuard")
            .field("policy", &self.policy)
            .finish()
    }
}

impl EgressGuard {
    pub fn new(policy: EgressPolicy) -> Result<Self, BlobLibError> {
        Self::build(policy, system_lookup())
    }

    fn build(policy: EgressPolicy, lookup: Lookup) -> Result<Self, BlobLibError> {
        Ok(Self {
            opendal: guarded_client(policy.clone(), lookup.clone(), None, READ_TIMEOUT)?,
            plain: guarded_client(policy.clone(), lookup, Some(REDIRECT_HOPS), READ_TIMEOUT)?,
            policy,
        })
    }

    /// Replaces the client opendal uses for data-plane requests *and* for the
    /// credential fetches every builder threads through `AccessorInfoHttpSend`.
    pub fn layer(&self) -> HttpClientLayer {
        HttpClientLayer::new(HttpClient::with(ScreenedFetch {
            client: self.opendal.clone(),
            policy: self.policy.clone(),
        }))
    }

    /// Screened GET builder for plain HTTP fetches that are not opendal calls.
    pub fn request(&self, url: Url) -> Result<reqwest::RequestBuilder, EgressError> {
        let host = url
            .host_str()
            .ok_or_else(|| EgressError::MissingHost(url.to_string()))?;
        screen_host(&self.policy, host)?;
        Ok(self.plain.get(url))
    }
}

/// A hop may never weaken the transport the caller chose: once the original
/// request was HTTPS, an HTTP target is refused. reqwest keeps sensitive
/// headers on same-host-and-port hops, so a downgrade would leak credentials.
fn scheme_downgraded(previous: &[Url], next: &Url) -> bool {
    previous.first().is_some_and(|url| url.scheme() == "https") && next.scheme() != "https"
}

fn guarded_client(
    policy: EgressPolicy,
    lookup: Lookup,
    hops: Option<usize>,
    read_timeout: Duration,
) -> Result<reqwest::Client, BlobLibError> {
    let screen_policy = policy.clone();
    let redirect = match hops {
        None => redirect::Policy::none(),
        Some(hops) => redirect::Policy::custom(move |attempt| {
            if attempt.previous().len() >= hops {
                return attempt.stop();
            }
            if scheme_downgraded(attempt.previous(), attempt.url()) {
                let target = attempt.url().to_string();
                return attempt.error(EgressError::SchemeDowngrade(target));
            }
            let verdict = match attempt.url().host_str() {
                Some(host) => screen_host(&screen_policy, host),
                None => Err(EgressError::MissingHost(attempt.url().to_string())),
            };
            match verdict {
                Ok(()) => attempt.follow(),
                Err(error) => attempt.error(error),
            }
        }),
    };

    Ok(reqwest::Client::builder()
        .dns_resolver(Arc::new(ScreenedResolver { policy, lookup }))
        .redirect(redirect)
        .no_proxy()
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(read_timeout)
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        .build()?)
}

#[cfg(test)]
#[path = "egress_tests.rs"]
mod tests;
