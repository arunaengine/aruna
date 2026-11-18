//! Streams the bytes of plugin transfers through the egress screen: a download's answer as a
//! counted body, and an object as the body or file part of an upload.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use aruna_core::injection::OutgoingRequest;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::job::UploadForm;
use bytes::Bytes;
use futures_util::StreamExt;

use crate::OutboundHandle;
use crate::connection::screened;

/// The bytes of a transfer body in one stream.
pub type TransferBody = BackendStream<Result<Bytes, StreamError>>;

/// Why a transfer has no answer.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TransferError {
    /// The request cannot be made at all.
    #[error("{0}")]
    Refused(String),
    /// Nothing reached the service.
    #[error("{0}")]
    NotConnected(String),
    /// The service may have seen the request.
    #[error("{0}")]
    Broken(String),
}

/// What a download's body did so far.
#[derive(Debug, Default)]
pub struct TransferMeter {
    pulled: AtomicU64,
    over: AtomicBool,
    broken: AtomicBool,
}

impl TransferMeter {
    /// Bytes read from the service.
    pub fn pulled(&self) -> u64 {
        self.pulled.load(Ordering::Relaxed)
    }

    /// Whether the body passed its bound.
    pub fn over(&self) -> bool {
        self.over.load(Ordering::Relaxed)
    }

    /// Whether the body broke off.
    pub fn broken(&self) -> bool {
        self.broken.load(Ordering::Relaxed)
    }
}

/// A download whose answer started.
pub struct OpenedDownload {
    pub status: u16,
    pub length: Option<u64>,
    pub content_type: Option<String>,
    pub body: TransferBody,
    pub meter: Arc<TransferMeter>,
}

/// An object to send: its bytes and size, as the file part of `form` when given.
pub struct UploadBody {
    pub bytes: TransferBody,
    pub size: u64,
    pub form: Option<UploadForm>,
}

/// The answer to an upload; `body` is `None` when it broke off or passed its bound.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadAnswer {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

impl OutboundHandle {
    fn transfer(
        &self,
        method: reqwest::Method,
        request: OutgoingRequest,
    ) -> Result<reqwest::RequestBuilder, TransferError> {
        let client = (&self.inner.http, &self.inner.policy);
        let builder = screened(client, method, request.url);
        let builder = builder.map_err(|error| TransferError::Refused(error.to_string()))?;
        Ok(
            (request.headers.iter()).fold(builder, |builder, (name, value)| {
                builder.header(name, value)
            }),
        )
    }

    /// Sends a GET and answers its status and a body that ends with an error past `bound`.
    pub async fn open_download(
        &self,
        request: OutgoingRequest,
        bound: Option<u64>,
    ) -> Result<OpenedDownload, TransferError> {
        let builder = self.transfer(reqwest::Method::GET, request)?;
        let response = builder.send().await.map_err(send_failure)?;
        let content_type = (response.headers().get(reqwest::header::CONTENT_TYPE))
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let (status, length) = (response.status().as_u16(), response.content_length());
        let meter = Arc::new(TransferMeter::default());
        let body = counted(response, bound, meter.clone());
        Ok(OpenedDownload {
            status,
            length,
            content_type,
            body,
            meter,
        })
    }

    /// Sends the object and reads at most `max_reply` bytes of the answer.
    pub async fn send_upload(
        &self,
        request: OutgoingRequest,
        upload: UploadBody,
        max_reply: usize,
    ) -> Result<UploadAnswer, TransferError> {
        let method = reqwest::Method::from_bytes(request.method.as_bytes());
        let method = method.unwrap_or(reqwest::Method::PUT);
        let builder = self.transfer(method, request)?;
        let UploadBody { bytes, size, form } = upload;
        let body = reqwest::Body::wrap_stream(bytes);
        let builder = match form {
            None => (builder.header(reqwest::header::CONTENT_LENGTH, size)).body(body),
            Some(form) => {
                let file = reqwest::multipart::Part::stream_with_length(body, size);
                let file = file.file_name(form.file_name);
                let parts = (form.fields.into_iter())
                    .fold(reqwest::multipart::Form::new(), |parts, (name, value)| {
                        parts.text(name, value)
                    });
                builder.multipart(parts.part(form.file_field, file))
            }
        };
        let mut response = builder.send().await.map_err(send_failure)?;
        let status = response.status().as_u16();
        let headers: Vec<(String, String)> = (response.headers().iter())
            .filter_map(|(name, value)| Some((name.to_string(), value.to_str().ok()?.to_string())))
            .collect();
        let mut answer = Vec::new();
        // A reply that breaks off or passes the bound is not kept, so none looks whole.
        let body = loop {
            match response.chunk().await {
                Ok(Some(chunk)) if answer.len() + chunk.len() <= max_reply => {
                    answer.extend_from_slice(&chunk);
                }
                Ok(None) => break Some(answer),
                Ok(Some(_)) | Err(_) => break None,
            }
        };
        Ok(UploadAnswer {
            status,
            headers,
            body,
        })
    }
}

fn send_failure(error: reqwest::Error) -> TransferError {
    let connect = error.is_connect();
    let shown = error.without_url().to_string();
    match connect {
        true => TransferError::NotConnected(shown),
        false => TransferError::Broken(shown),
    }
}

/// The answer's body as a stream that counts its bytes, ends with an error past `bound`, and
/// notes a broken transfer apart from a refused one.
fn counted(
    response: reqwest::Response,
    bound: Option<u64>,
    meter: Arc<TransferMeter>,
) -> TransferBody {
    let stream = response.bytes_stream().map(move |chunk| {
        let chunk = chunk.map_err(|error| {
            meter.broken.store(true, Ordering::Relaxed);
            std::io::Error::other(error.without_url().to_string())
        })?;
        let read = chunk.len() as u64;
        let total = meter.pulled.fetch_add(read, Ordering::Relaxed) + read;
        if bound.is_some_and(|bound| total > bound) {
            meter.over.store(true, Ordering::Relaxed);
            return Err(std::io::Error::other(
                "the answer is larger than its size limit",
            ));
        }
        Ok(chunk)
    });
    BackendStream::new(stream)
}

#[cfg(test)]
#[path = "transfers_tests.rs"]
mod tests;
