//! Sends the GET or HEAD an operation composed for another member of a distributed bucket and
//! streams the member's answer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::io;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use aruna_core::jobs::ByteStream;
use aruna_core::member_read::{MemberAnswer, MemberBody, MemberFetch};
use aruna_core::outbound::xml_field;
use aruna_core::stream::BackendStream;
use futures_util::{Stream, StreamExt, stream};
use reqwest::{Method, Response};
use tracing::warn;

use crate::OutboundHandle;

/// How long a member may take to start its answer; the body then streams without a limit.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(30);
/// Bytes of a refusal read at most, enough for its S3 error document.
const MAX_REFUSAL_BYTES: usize = 64 * 1024;

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

impl OutboundHandle {
    async fn send_fetch(&self, request: MemberFetch) -> MemberAnswer {
        let method = if request.head {
            Method::HEAD
        } else {
            Method::GET
        };
        let mut outgoing = self.inner.members.request(method, &request.url);
        for (name, value) in &request.headers {
            outgoing = outgoing.header(name.as_str(), value.as_str());
        }
        let response = match outgoing.send().await {
            Ok(response) => response,
            Err(error) => {
                let connect = error.is_connect();
                let error = error.without_url();
                warn!(connect, error = %error, "A member did not take a forwarded read");
                return MemberAnswer::Silent;
            }
        };
        let status = response.status();
        if status.is_success() {
            return MemberAnswer::Body(answered(response));
        }
        let body = refusal_text(response).await;
        MemberAnswer::Refused {
            status: status.as_u16(),
            code: xml_field(&body, "Code"),
            message: xml_field(&body, "Message").unwrap_or_default(),
        }
    }

    /// Sends `request` to a member. Member addresses come from the realm's own records, so
    /// this client passes no egress screen.
    pub async fn fetch_member(&self, request: MemberFetch) -> MemberAnswer {
        tokio::time::timeout(ANSWER_TIMEOUT, self.send_fetch(request))
            .await
            .unwrap_or(MemberAnswer::Late)
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::egress::EgressPolicy;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    fn handle() -> OutboundHandle {
        OutboundHandle::new(reqwest::Client::new(), EgressPolicy::loopback())
    }

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

    fn fetch(url: String, head: bool) -> MemberFetch {
        let headers = vec![("x-test".to_string(), "sent".to_string())];
        MemberFetch { head, url, headers }
    }

    #[tokio::test]
    async fn body_streams_headers() {
        let version = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let headers = format!("x-amz-version-id: {version}\r\n");
        let (url, task) = server(reply("206 Partial Content", &headers, "bytes")).await;
        let MemberAnswer::Body(body) = handle().fetch_member(fetch(url, false)).await else {
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
        let MemberAnswer::Body(body) = handle().fetch_member(fetch(url, true)).await else {
            panic!("a head is answered");
        };
        assert_eq!(body.header("content-length"), Some("4"));
        assert!(task.await.unwrap().starts_with("HEAD /spire/a"));
    }

    #[tokio::test]
    async fn refusal_names_code() {
        let body = "<Error><Code>AccessDenied</Code><Message>no</Message></Error>";
        let (url, _task) = server(reply("403 Forbidden", "", body)).await;
        let answer = handle().fetch_member(fetch(url, false)).await;
        let expected = MemberAnswer::Refused {
            status: 403,
            code: Some("AccessDenied".to_string()),
            message: "no".to_string(),
        };
        assert_eq!(answer, expected);
    }

    #[tokio::test]
    async fn closed_port_silent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/spire/a", listener.local_addr().unwrap());
        drop(listener);
        let answer = handle().fetch_member(fetch(url, false)).await;
        assert_eq!(answer, MemberAnswer::Silent);
    }
}
