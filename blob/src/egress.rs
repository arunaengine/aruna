//! Connect-time enforcement of the egress policy.
//!
//! A URL check alone is defeated by redirects and DNS rebinding, so tenant
//! traffic runs through clients that can only ever reach vetted addresses.

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

impl HttpFetch for ScreenedFetch {
    async fn fetch(
        &self,
        request: http::Request<Buffer>,
    ) -> opendal::Result<http::Response<HttpBody>> {
        if let Some(host) = request.uri().host() {
            screen_host(&self.policy, host).map_err(|error| {
                opendal::Error::new(
                    ErrorKind::PermissionDenied,
                    "egress policy denied the target",
                )
                .set_source(error)
            })?;
        }
        self.client.fetch(request).await
    }
}

/// Bundles the egress policy with the clients that enforce it.
#[derive(Clone)]
pub struct EgressGuard {
    policy: EgressPolicy,
    opendal: reqwest::Client,
    plain: reqwest::Client,
    lookup: Lookup,
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
            opendal: guarded_client(policy.clone(), lookup.clone(), None)?,
            plain: guarded_client(policy.clone(), lookup.clone(), Some(REDIRECT_HOPS))?,
            policy,
            lookup,
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

    /// Preflight screen for protocols no HTTP client can cover, such as FTP.
    /// Every resolved address must pass; a rebind after this point is the
    /// documented residual.
    pub async fn screen(&self, endpoint: &str) -> Result<(), EgressError> {
        let url =
            Url::parse(endpoint).map_err(|_| EgressError::MissingHost(endpoint.to_string()))?;
        let host = url
            .host_str()
            .ok_or_else(|| EgressError::MissingHost(endpoint.to_string()))?;
        let literal = host.trim_start_matches('[').trim_end_matches(']');
        if let Ok(address) = literal.parse::<IpAddr>() {
            return self.policy.check(address);
        }
        let resolved =
            (self.lookup)(host.to_string())
                .await
                .map_err(|error| EgressError::ResolveFailed {
                    host: host.to_string(),
                    reason: error.to_string(),
                })?;
        if resolved.is_empty() {
            return Err(EgressError::NoAllowedAddress(host.to_string()));
        }
        for address in resolved {
            self.policy.check(address.ip())?;
        }
        Ok(())
    }
}

fn guarded_client(
    policy: EgressPolicy,
    lookup: Lookup,
    hops: Option<usize>,
) -> Result<reqwest::Client, BlobLibError> {
    let screen_policy = policy.clone();
    let redirect = match hops {
        None => redirect::Policy::none(),
        Some(hops) => redirect::Policy::custom(move |attempt| {
            if attempt.previous().len() >= hops {
                return attempt.stop();
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
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::{Operator, services};
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Loopback server that counts accepted connections and records the raw
    /// request bytes, so both "did it connect" and "what did it send" are testable.
    struct TestServer {
        address: SocketAddr,
        hits: Arc<AtomicUsize>,
        seen: Arc<Mutex<Vec<String>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl TestServer {
        async fn spawn(response: String) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let hits = Arc::new(AtomicUsize::new(0));
            let seen = Arc::new(Mutex::new(Vec::new()));
            let counter = hits.clone();
            let recorder = seen.clone();
            let task = tokio::spawn(async move {
                while let Ok((mut socket, _)) = listener.accept().await {
                    counter.fetch_add(1, Ordering::SeqCst);
                    let response = response.clone();
                    let recorder = recorder.clone();
                    tokio::spawn(async move {
                        let mut buffer = vec![0u8; 4096];
                        let mut request = Vec::new();
                        loop {
                            let Ok(read) = socket.read(&mut buffer).await else {
                                return;
                            };
                            if read == 0 {
                                return;
                            }
                            request.extend_from_slice(&buffer[..read]);
                            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                                break;
                            }
                        }
                        recorder
                            .lock()
                            .unwrap()
                            .push(String::from_utf8_lossy(&request).into_owned());
                        let _ = socket.write_all(response.as_bytes()).await;
                        let _ = socket.shutdown().await;
                    });
                }
            });
            Self {
                address,
                hits,
                seen,
                task,
            }
        }

        fn hits(&self) -> usize {
            self.hits.load(Ordering::SeqCst)
        }

        fn seen(&self) -> Vec<String> {
            self.seen.lock().unwrap().clone()
        }

        fn url(&self, path: &str) -> Url {
            Url::parse(&format!(
                "http://backend.test:{}{path}",
                self.address.port()
            ))
            .unwrap()
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn denial(error: reqwest::Error) -> String {
        let mut reason = String::new();
        let mut source: Option<&dyn std::error::Error> = Some(&error);
        while let Some(current) = source {
            reason = current.to_string();
            source = current.source();
        }
        reason
    }

    fn ok_body(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn redirect_to(location: &str) -> String {
        format!(
            "HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        )
    }

    fn fixed_lookup(address: SocketAddr) -> Lookup {
        Arc::new(move |_host| Box::pin(async move { Ok(vec![address]) }))
    }

    /// Answers a different address on the second call, so a screen that ran only
    /// before the connect would be bypassed.
    fn rebinding_lookup(first: SocketAddr, second: SocketAddr) -> Lookup {
        let calls = Arc::new(AtomicUsize::new(0));
        Arc::new(move |_host| {
            let index = calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(vec![if index == 0 { first } else { second }]) })
        })
    }

    fn host_lookup(routes: Vec<(&'static str, SocketAddr)>) -> Lookup {
        Arc::new(move |host: String| {
            let found = routes
                .iter()
                .find(|(name, _)| *name == host)
                .map(|(_, address)| *address);
            Box::pin(async move {
                found
                    .map(|address| vec![address])
                    .ok_or(io::ErrorKind::NotFound.into())
            })
        })
    }

    #[tokio::test]
    async fn strict_blocks_connect() {
        // Zero accepts is the proof; the loopback fixture is the counterfactual.
        let server = TestServer::spawn(ok_body("data")).await;
        let url = server.url("/probe");

        let strict =
            EgressGuard::build(EgressPolicy::strict(), fixed_lookup(server.address)).unwrap();
        strict
            .request(url.clone())
            .unwrap()
            .send()
            .await
            .unwrap_err();
        assert_eq!(server.hits(), 0);

        let fixture =
            EgressGuard::build(EgressPolicy::loopback(), fixed_lookup(server.address)).unwrap();
        let response = fixture.request(url).unwrap().send().await.unwrap();

        assert!(response.status().is_success());
        assert_eq!(server.hits(), 1);
    }

    #[tokio::test]
    async fn blocks_rebound_host() {
        // The name passes once, then resolves to a denied address.
        let server = TestServer::spawn(ok_body("data")).await;
        let denied: SocketAddr = "169.254.169.254:80".parse().unwrap();
        let guard = EgressGuard::build(
            EgressPolicy::loopback(),
            rebinding_lookup(server.address, denied),
        )
        .unwrap();

        guard
            .request(server.url("/probe"))
            .unwrap()
            .send()
            .await
            .unwrap();
        guard
            .request(server.url("/probe"))
            .unwrap()
            .send()
            .await
            .unwrap_err();

        assert_eq!(server.hits(), 1);
    }

    #[tokio::test]
    async fn refuses_denied_redirect() {
        let server = TestServer::spawn(redirect_to("http://169.254.169.254/latest")).await;
        let guard =
            EgressGuard::build(EgressPolicy::loopback(), fixed_lookup(server.address)).unwrap();

        let error = guard
            .request(server.url("/probe"))
            .unwrap()
            .send()
            .await
            .unwrap_err();

        assert!(denial(error).contains("not a public unicast destination"));
        assert_eq!(server.hits(), 1);
    }

    #[tokio::test]
    async fn strips_redirect_auth() {
        // reqwest drops Authorization across hosts; pin it instead of assuming it.
        let target = TestServer::spawn(ok_body("data")).await;
        let entry = TestServer::spawn(redirect_to(&format!(
            "http://second.test:{}/next",
            target.address.port()
        )))
        .await;
        let guard = EgressGuard::build(
            EgressPolicy::loopback(),
            host_lookup(vec![
                ("first.test", entry.address),
                ("second.test", target.address),
            ]),
        )
        .unwrap();
        let url = Url::parse(&format!("http://first.test:{}/start", entry.address.port())).unwrap();

        let response = guard
            .request(url)
            .unwrap()
            .bearer_auth("node-token")
            .send()
            .await
            .unwrap();

        assert!(response.status().is_success());
        assert!(entry.seen()[0].contains("node-token"));
        assert!(
            !target.seen()[0]
                .to_ascii_lowercase()
                .contains("authorization")
        );
    }

    #[tokio::test]
    async fn blocks_literal_target() {
        // hyper connects to IP literals without the resolver, so the opendal
        // fetcher is what screens them.
        let server = TestServer::spawn(ok_body("data")).await;
        let guard =
            EgressGuard::build(EgressPolicy::loopback(), fixed_lookup(server.address)).unwrap();
        let operator = Operator::from_iter::<services::Http>(HashMap::from([(
            "endpoint".to_string(),
            "http://169.254.169.254".to_string(),
        )]))
        .unwrap()
        .layer(guard.layer())
        .finish();

        let error = operator.stat("token").await.unwrap_err();

        assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        assert_eq!(server.hits(), 0);
    }

    #[tokio::test]
    async fn screen_rejects_denied() {
        let guard = EgressGuard::build(
            EgressPolicy::strict(),
            fixed_lookup("10.0.0.5:21".parse().unwrap()),
        )
        .unwrap();

        guard.screen("ftp://files.test:21").await.unwrap_err();
        guard.screen("ftp://169.254.169.254:21").await.unwrap_err();
    }
}
