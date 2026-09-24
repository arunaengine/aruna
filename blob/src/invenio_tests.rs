//! Tests Invenio token transport and that reference reads fail when the origin changed or is gone.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::egress::EgressPolicy;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use futures::TryStreamExt;
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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

/// Answers HEAD with `head` and GET with `get`, as a repository file origin would.
struct Origin {
    endpoint: String,
    task: tokio::task::JoinHandle<()>,
}

impl Origin {
    async fn spawn(head: String, get: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/api/", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut request = Vec::new();
                let mut chunk = [0; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    match socket.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(read) => request.extend_from_slice(&chunk[..read]),
                    }
                }
                let answer = if request.starts_with(b"HEAD ") {
                    &head
                } else {
                    &get
                };
                let _ = socket.write_all(answer.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        Self { endpoint, task }
    }

    fn access(&self) -> ResolvedSourceAccess {
        ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Invenio,
            config: HashMap::from([
                ("endpoint".to_string(), self.endpoint.clone()),
                (REFERENCE_RECORD.to_string(), "abcde-12345".to_string()),
                (REFERENCE_FILE.to_string(), "data.txt".to_string()),
            ]),
            path: "content".to_string(),
            version: None,
        }
    }
}

impl Drop for Origin {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn answer(status: &str, headers: &str, body: &str) -> String {
    format!("HTTP/1.1 {status}\r\n{headers}Connection: close\r\n\r\n{body}")
}

fn observed() -> String {
    answer("200 OK", "Content-Length: 5\r\nETag: \"v1\"\r\n", "")
}

async fn read(
    origin: &Origin,
    range: Option<std::ops::Range<u64>>,
) -> Result<Vec<u8>, StagingSourceError> {
    let guard = EgressGuard::new(EgressPolicy::loopback()).unwrap();
    let (_, stream) = read_reference(&guard, &origin.access(), range).await?;
    let chunks: Vec<Bytes> = stream
        .try_collect()
        .await
        .map_err(|error| StagingSourceError::ReadError(error.to_string()))?;
    Ok(chunks.concat())
}

#[tokio::test]
async fn reference_serves_observed() {
    let full = answer("200 OK", "Content-Length: 5\r\nETag: \"v1\"\r\n", "hello");
    let origin = Origin::spawn(observed(), full).await;
    assert_eq!(read(&origin, None).await.unwrap(), b"hello");

    let part = answer(
        "206 Partial Content",
        "Content-Length: 3\r\nContent-Range: bytes 1-3/5\r\nETag: \"v1\"\r\n",
        "ell",
    );
    let origin = Origin::spawn(observed(), part).await;
    assert_eq!(read(&origin, Some(1..4)).await.unwrap(), b"ell");
}

#[tokio::test]
async fn reference_change_fails() {
    // Each body differs from the HEAD observation in size, total length or ETag.
    let changed = [
        (answer("200 OK", "Content-Length: 6\r\n", "hello!"), None),
        (
            answer("200 OK", "Content-Length: 5\r\nETag: \"v2\"\r\n", "HELLO"),
            None,
        ),
        (
            answer(
                "206 Partial Content",
                "Content-Length: 3\r\nContent-Range: bytes 1-3/6\r\n",
                "ell",
            ),
            Some(1..4),
        ),
    ];
    for (get, range) in changed {
        let origin = Origin::spawn(observed(), get.clone()).await;
        assert!(
            matches!(
                read(&origin, range).await,
                Err(StagingSourceError::SourceUnstable)
            ),
            "{get}"
        );
    }
}

#[tokio::test]
async fn reference_gone_fails() {
    // A missing, refused or failing origin is an error, never an empty success.
    let cases = [
        (
            answer("404 Not Found", "Content-Length: 0\r\n", ""),
            observed(),
        ),
        (
            answer("403 Forbidden", "Content-Length: 0\r\n", ""),
            observed(),
        ),
        (
            answer("503 Service Unavailable", "Content-Length: 0\r\n", ""),
            observed(),
        ),
        (
            observed(),
            answer("500 Internal Server Error", "Content-Length: 0\r\n", ""),
        ),
    ];
    let expected = |error: &StagingSourceError| match error {
        StagingSourceError::NotFound | StagingSourceError::AccessDenied => true,
        StagingSourceError::ReadError(message) => message.contains("HTTP 50"),
        _ => false,
    };
    for (head, get) in cases {
        let origin = Origin::spawn(head.clone(), get).await;
        let error = read(&origin, None).await.unwrap_err();
        assert!(expected(&error), "{head}: {error}");
    }
}

/// Answers each GET by its request path.
async fn routes(route: fn(&str) -> String) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/api/", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut request = Vec::new();
            let mut chunk = [0; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                match socket.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(read) => request.extend_from_slice(&chunk[..read]),
                }
            }
            let text = String::from_utf8_lossy(&request);
            let path = text.split(' ').nth(1).unwrap_or_default().to_string();
            let _ = socket.write_all(route(&path).as_bytes()).await;
            let _ = socket.shutdown().await;
        }
    });
    (endpoint, task)
}

#[tokio::test]
async fn json_follows_redirects() {
    let (endpoint, task) = routes(|path| match path {
        "/api/records/parent" => answer(
            "302 Found",
            "Location: /api/records/latest\r\nContent-Length: 0\r\n",
            "",
        ),
        "/api/records/latest" => answer("200 OK", "Content-Length: 13\r\n", r#"{"id":"late"}"#),
        "/api/records/foreign" => answer(
            "302 Found",
            "Location: http://127.0.0.2:9/api/records/x\r\nContent-Length: 0\r\n",
            "",
        ),
        "/api/records/html" => answer(
            "301 Moved",
            "Location: /records/latest\r\nContent-Length: 0\r\n",
            "",
        ),
        _ => answer(
            "302 Found",
            "Location: /api/records/loop\r\nContent-Length: 0\r\n",
            "",
        ),
    })
    .await;
    let guard = EgressGuard::new(EgressPolicy::loopback()).unwrap();
    let client = InvenioClient::with_guard(&guard, &endpoint, Some("token".into()), 1024).unwrap();
    let get = async |id: &str| {
        client
            .json(Method::GET, client.url(&["records", id]).unwrap(), None)
            .await
    };
    assert_eq!(get("parent").await.unwrap()["id"], "late");
    // Other origins and pages outside the API are refused, so the token never leaves it.
    assert!(matches!(
        get("foreign").await,
        Err(InvenioError::InvalidUrl)
    ));
    assert!(matches!(get("html").await, Err(InvenioError::InvalidUrl)));
    assert!(matches!(get("loop").await, Err(InvenioError::Redirects)));
    task.abort();
}
