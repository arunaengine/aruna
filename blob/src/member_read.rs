//! Sends the GET an operation composed for another member of a distributed bucket and streams
//! the member's answer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::io;
use std::sync::{LazyLock, Mutex, PoisonError};
use std::time::Duration;

use aruna_core::member_read::{MemberAnswer, MemberGet};
use aruna_core::outbound::xml_field;
use aruna_core::stream::BackendStream;
use futures::{Stream, StreamExt, stream};
use reqwest::Response;
use tracing::warn;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a member may take to start its answer; the body then streams without a limit.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(30);
/// Bytes of a refusal read at most, enough for its S3 error document.
const MAX_REFUSAL_BYTES: usize = 64 * 1024;
const VERSION_HEADER: &str = "x-amz-version-id";

static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .build()
        .unwrap_or_default()
});

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

fn body_stream(response: Response) -> aruna_core::jobs::ByteStream {
    // The lock only lends the stream `Sync`; one task polls it.
    let body = response
        .bytes_stream()
        .map(|item| item.map_err(io::Error::other));
    let body = Mutex::new(Box::pin(body));
    BackendStream::new(stream::poll_fn(move |cx| {
        let mut body = body.lock().unwrap_or_else(PoisonError::into_inner);
        body.as_mut().poll_next(cx)
    }))
}

async fn answer(request: MemberGet) -> MemberAnswer {
    let mut outgoing = CLIENT.get(&request.url);
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
        let version = (response.headers().get(VERSION_HEADER))
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let stream = body_stream(response);
        return MemberAnswer::Body { version, stream };
    }
    MemberAnswer::Refused {
        status: status.as_u16(),
        code: xml_field(&refusal_text(response).await, "Code"),
    }
}

/// Sends `request`; a member that does not start answering in time counts as silent.
pub(crate) async fn get_member(request: MemberGet) -> MemberAnswer {
    tokio::time::timeout(ANSWER_TIMEOUT, answer(request))
        .await
        .unwrap_or(MemberAnswer::Silent)
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

    fn get(url: String) -> MemberGet {
        let headers = vec![("x-test".to_string(), "sent".to_string())];
        MemberGet { url, headers }
    }

    #[tokio::test]
    async fn body_streams_version() {
        let (url, task) = server(reply("200 OK", "x-amz-version-id: v1\r\n", "bytes")).await;
        let MemberAnswer::Body { version, stream } = get_member(get(url)).await else {
            panic!("a body is answered");
        };
        assert_eq!(version.as_deref(), Some("v1"));
        let chunks: Vec<_> = stream.0.collect().await;
        let bytes: Vec<u8> = (chunks.into_iter())
            .flat_map(|chunk| chunk.unwrap().to_vec())
            .collect();
        assert_eq!(bytes, b"bytes");
        assert!(task.await.unwrap().to_lowercase().contains("x-test: sent"));
    }

    #[tokio::test]
    async fn refusal_names_code() {
        let body = "<Error><Code>AccessDenied</Code></Error>";
        let (url, _task) = server(reply("403 Forbidden", "", body)).await;
        let answer = get_member(get(url)).await;
        let code = Some("AccessDenied".to_string());
        assert_eq!(answer, MemberAnswer::Refused { status: 403, code });
    }

    #[tokio::test]
    async fn closed_port_silent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/spire/a", listener.local_addr().unwrap());
        drop(listener);
        assert_eq!(get_member(get(url)).await, MemberAnswer::Silent);
    }
}
