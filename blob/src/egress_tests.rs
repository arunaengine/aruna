//! Tests that the egress guard blocks denied addresses, rebinding, redirects and stalled peers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

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

    let strict = EgressGuard::build(EgressPolicy::strict(), fixed_lookup(server.address)).unwrap();
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
    let guard = EgressGuard::build(EgressPolicy::loopback(), fixed_lookup(server.address)).unwrap();

    let error = guard
        .request(server.url("/probe"))
        .unwrap()
        .send()
        .await
        .unwrap_err();

    assert!(denial(error).contains("not a public unicast destination"));
    assert_eq!(server.hits(), 1);
}

#[test]
fn refuses_scheme_downgrade() {
    // Only the original scheme matters: an https start pins https hops.
    let https = Url::parse("https://backend.test/start").unwrap();
    let http = Url::parse("http://backend.test/start").unwrap();
    assert!(scheme_downgraded(
        std::slice::from_ref(&https),
        &Url::parse("http://backend.test/next").unwrap()
    ));
    assert!(!scheme_downgraded(
        std::slice::from_ref(&https),
        &Url::parse("https://other.test/next").unwrap()
    ));
    assert!(!scheme_downgraded(
        std::slice::from_ref(&http),
        &Url::parse("http://backend.test/next").unwrap()
    ));
    assert!(!scheme_downgraded(
        std::slice::from_ref(&http),
        &Url::parse("https://backend.test/next").unwrap()
    ));
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
    let guard = EgressGuard::build(EgressPolicy::loopback(), fixed_lookup(server.address)).unwrap();
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
async fn drops_stalled_peer() {
    // The peer accepts and then never answers, so only a read bound ends it.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            held.push(socket);
        }
    });
    let client = guarded_client(
        EgressPolicy::loopback(),
        fixed_lookup(address),
        None,
        Duration::from_millis(50),
    )
    .unwrap();

    let error = client
        .get(Url::parse(&format!("http://backend.test:{}/probe", address.port())).unwrap())
        .send()
        .await
        .unwrap_err();

    assert!(error.is_timeout(), "expected a timeout, got {error:?}");
    task.abort();
}
