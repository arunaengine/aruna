//! Sends the request an operation composed for another member of the realm and streams the
//! member's answer. All member-to-member HTTP goes through the client here.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::io;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::Duration;

use aruna_core::jobs::ByteStream;
use aruna_core::member_read::{
    MemberAnswer, MemberBody, MemberFetch, MemberMethod, MemberSend, TrailerSeal, framed_stream,
};
use aruna_core::stream::BackendStream;
use aruna_core::structs::storage::distribution::delegation::BodyTrailer;
use futures_util::{Stream, StreamExt, stream};
use reqwest::{Body, Method, Response};
use tracing::warn;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Bytes of a refusal read at most, enough for its error document.
const MAX_REFUSAL_BYTES: usize = 64 * 1024;

/// Reaches other members at the addresses the realm's own records publish, so it passes no
/// egress screen.
pub(crate) static MEMBERS: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .build()
        .unwrap_or_default()
});

/// A seal for a backend run without a node key.
pub(crate) struct NoSeal;

impl TrailerSeal for NoSeal {
    fn seal(&self, _: &BodyTrailer, _: &[u8; 16]) -> Option<Vec<u8>> {
        None
    }
}

/// The start of a refusal; a broken body leaves what arrived.
async fn refusal_text(mut response: Response) -> String {
    let mut body = Vec::new();
    while body.len() < MAX_REFUSAL_BYTES {
        match response.chunk().await {
            Ok(Some(chunk)) => body.extend_from_slice(&chunk),
            Ok(None) | Err(_) => break,
        }
    }
    String::from_utf8_lossy(&body).into_owned()
}

fn body_stream(response: Response) -> ByteStream {
    // The lock only lends the stream `Sync`; one task polls it.
    let body = (response.bytes_stream()).map(|item| item.map_err(io::Error::other));
    let body = Mutex::new(Box::pin(body));
    BackendStream::new(stream::poll_fn(move |cx| {
        let mut body = body.lock().unwrap_or_else(PoisonError::into_inner);
        body.as_mut().poll_next(cx)
    }))
}

fn answered(response: Response) -> MemberBody {
    let headers = (response.headers().iter())
        .filter_map(|(name, value)| {
            Some((name.as_str().to_string(), value.to_str().ok()?.to_string()))
        })
        .collect();
    MemberBody {
        status: response.status().as_u16(),
        headers,
        stream: body_stream(response),
    }
}

fn method_of(method: MemberMethod) -> Method {
    match method {
        MemberMethod::Get => Method::GET,
        MemberMethod::Head => Method::HEAD,
        MemberMethod::Put => Method::PUT,
        MemberMethod::Post => Method::POST,
        MemberMethod::Delete => Method::DELETE,
    }
}

async fn send_fetch(request: MemberFetch, seal: Arc<dyn TrailerSeal>) -> MemberAnswer {
    let any_status = request.any_status;
    let mut outgoing = MEMBERS.request(method_of(request.method), &request.url);
    for (name, value) in &request.headers {
        outgoing = outgoing.header(name.as_str(), value.as_str());
    }
    outgoing = match request.body {
        MemberSend::Empty => outgoing,
        MemberSend::Bytes(bytes) => outgoing.body(bytes),
        MemberSend::Framed {
            stream,
            nonce,
            checksum,
        } => {
            let framed = framed_stream(stream, nonce, checksum, seal);
            outgoing.body(Body::wrap_stream(framed.0))
        }
    };
    let response = match outgoing.send().await {
        Ok(response) => response,
        Err(error) => {
            let connect = error.is_connect();
            let error = error.without_url();
            warn!(connect, error = %error, "A member did not take a request");
            return MemberAnswer::Silent;
        }
    };
    let status = response.status();
    if status.is_success() || any_status {
        return MemberAnswer::Body(answered(response));
    }
    MemberAnswer::Refused {
        status: status.as_u16(),
        body: refusal_text(response).await,
    }
}

/// Sends `request` to a member; `seal` signs the trailer of a framed body.
pub async fn fetch_member(request: MemberFetch, seal: Arc<dyn TrailerSeal>) -> MemberAnswer {
    match request.answer_limit {
        Some(limit) => tokio::time::timeout(limit, send_fetch(request, seal))
            .await
            .unwrap_or(MemberAnswer::Late),
        None => send_fetch(request, seal).await,
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// A local server that answers one request with `response` and hands back the request.
    async fn server(response: String) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/spire/a", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut seen = Vec::new();
            while !seen.windows(4).any(|end| end == b"\r\n\r\n") {
                let mut chunk = [0; 1024];
                match socket.read(&mut chunk).await.unwrap() {
                    0 => break,
                    read => seen.extend_from_slice(&chunk[..read]),
                }
            }
            socket.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&seen).into_owned()
        });
        (url, task)
    }

    fn reply(status: &str, headers: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\n{headers}content-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn fetch(url: String, method: MemberMethod) -> MemberFetch {
        MemberFetch {
            method,
            url,
            headers: vec![("x-test".to_string(), "sent".to_string())],
            body: MemberSend::Empty,
            answer_limit: Some(Duration::from_secs(30)),
            any_status: false,
        }
    }

    async fn send(request: MemberFetch) -> MemberAnswer {
        fetch_member(request, Arc::new(NoSeal)).await
    }

    #[tokio::test]
    async fn body_streams_headers() {
        let version = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let headers = format!("x-amz-version-id: {version}\r\n");
        let (url, task) = server(reply("206 Partial Content", &headers, "bytes")).await;
        let MemberAnswer::Body(body) = send(fetch(url, MemberMethod::Get)).await else {
            panic!("a body is answered");
        };
        assert_eq!(body.status, 206);
        assert_eq!(body.version().map(|id| id.to_string()).unwrap(), version);
        let chunks: Vec<_> = body.stream.0.collect().await;
        let bytes: Vec<u8> = (chunks.into_iter())
            .flat_map(|chunk| chunk.unwrap().to_vec())
            .collect();
        assert_eq!(bytes, b"bytes");
        let seen = task.await.unwrap().to_lowercase();
        assert!(seen.starts_with("get /spire/a") && seen.contains("x-test: sent"));
    }

    #[tokio::test]
    async fn head_method_sent() {
        let answer = "HTTP/1.1 200 OK\r\ncontent-length: 4\r\nconnection: close\r\n\r\n";
        let (url, task) = server(answer.to_string()).await;
        let MemberAnswer::Body(body) = send(fetch(url, MemberMethod::Head)).await else {
            panic!("a head is answered");
        };
        assert_eq!(body.header("content-length"), Some("4"));
        assert!(task.await.unwrap().starts_with("HEAD /spire/a"));
    }

    #[tokio::test]
    async fn refusal_names_code() {
        let body = "<Error><Code>AccessDenied</Code><Message>no</Message></Error>";
        let (url, _task) = server(reply("403 Forbidden", "", body)).await;
        let answer = send(fetch(url, MemberMethod::Get)).await;
        let expected = MemberAnswer::Refused {
            status: 403,
            body: body.to_string(),
        };
        assert_eq!(answer, expected);
    }

    #[tokio::test]
    async fn closed_port_silent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/spire/a", listener.local_addr().unwrap());
        drop(listener);
        let answer = send(fetch(url, MemberMethod::Get)).await;
        assert_eq!(answer, MemberAnswer::Silent);
    }

    #[tokio::test]
    async fn any_status_streams() {
        let (url, _task) = server(reply("404 Not Found", "", "gone")).await;
        let mut request = fetch(url, MemberMethod::Get);
        request.any_status = true;
        let MemberAnswer::Body(body) = send(request).await else {
            panic!("the refusal streams");
        };
        assert_eq!(body.status, 404);
    }

    #[tokio::test]
    async fn body_is_sent() {
        let (url, task) = server(reply("200 OK", "", "")).await;
        let mut request = fetch(url, MemberMethod::Put);
        request
            .headers
            .push(("content-length".to_string(), "4".to_string()));
        request.body = MemberSend::Bytes(bytes::Bytes::from_static(b"data"));
        assert!(matches!(send(request).await, MemberAnswer::Body(_)));
        assert!(task.await.unwrap().starts_with("PUT /spire/a"));
    }
}
