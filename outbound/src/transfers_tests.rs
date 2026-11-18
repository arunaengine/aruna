//! Tests transfer calls against a local server: the screen, the counted body and the reply bound.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::egress::EgressPolicy;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::*;

/// A server that answers each connection with `answer` after reading the request head.
async fn server(answer: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut seen = [0u8; 4096];
            let _ = socket.read(&mut seen).await;
            let _ = socket.write_all(answer.as_bytes()).await;
        }
    });
    format!("http://{address}/file")
}

fn handle(policy: EgressPolicy) -> OutboundHandle {
    OutboundHandle::new(reqwest::Client::new(), policy)
}

fn request(method: &str, url: &str) -> OutgoingRequest {
    OutgoingRequest {
        method: method.to_string(),
        url: url.parse().unwrap(),
        headers: vec![("x-test".to_string(), "1".to_string())],
    }
}

fn no_bytes() -> UploadBody {
    let empty = futures_util::stream::empty::<Result<Bytes, std::io::Error>>();
    UploadBody {
        bytes: BackendStream::new(empty),
        size: 0,
        form: None,
    }
}

const OK: &str = "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 4\r\n\r\nACGT";

async fn read_all(mut body: TransferBody) -> Result<Vec<u8>, StreamError> {
    let mut read = Vec::new();
    while let Some(chunk) = body.next().await {
        read.extend_from_slice(&chunk?);
    }
    Ok(read)
}

#[tokio::test]
async fn download_counts_bytes() {
    let url = server(OK).await;
    let outbound = handle(EgressPolicy::loopback());
    let opened = outbound.open_download(request("GET", &url), None).await;
    let opened = opened.unwrap();
    assert_eq!((opened.status, opened.length), (200, Some(4)));
    assert_eq!(opened.content_type.as_deref(), Some("text/plain"));
    assert_eq!(read_all(opened.body).await.unwrap(), b"ACGT");
    assert_eq!(opened.meter.pulled(), 4);
    assert!(!opened.meter.over());
}

#[tokio::test]
async fn download_stops_at_bound() {
    let url = server(OK).await;
    let outbound = handle(EgressPolicy::loopback());
    let opened = outbound.open_download(request("GET", &url), Some(2)).await;
    let opened = opened.unwrap();
    assert!(read_all(opened.body).await.is_err());
    assert!(opened.meter.over());
    assert!(!opened.meter.broken());
}

#[tokio::test]
async fn upload_keeps_reply() {
    let url = server(OK).await;
    let outbound = handle(EgressPolicy::loopback());
    let answer = outbound
        .send_upload(request("PUT", &url), no_bytes(), 16)
        .await;
    let answer = answer.unwrap();
    assert_eq!((answer.status, answer.body), (200, Some(b"ACGT".to_vec())));
    let cut = outbound
        .send_upload(request("PUT", &url), no_bytes(), 2)
        .await;
    assert_eq!(cut.unwrap().body, None);
}

#[tokio::test]
async fn screen_refuses_host() {
    let outbound = handle(EgressPolicy::default());
    let refused = outbound
        .open_download(request("GET", "http://127.0.0.1:9/file"), None)
        .await;
    assert!(matches!(refused, Err(TransferError::Refused(_))));
}

#[tokio::test]
async fn closed_port_not_connected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let outbound = handle(EgressPolicy::loopback());
    let url = format!("http://{address}/file");
    let failed = outbound
        .send_upload(request("PUT", &url), no_bytes(), 16)
        .await;
    assert!(matches!(failed, Err(TransferError::NotConnected(_))));
}
