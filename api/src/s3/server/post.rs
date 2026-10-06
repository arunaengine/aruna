//! Guards POST form tokens before dispatch and keeps their multipart dumps out of logs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::identity::s3_session::S3Session;
use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use http::{Method, header};
use s3s::{HttpRequest, S3Result, s3_error};
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::span::{Attributes, Id, Record};
use tracing::{Dispatch, Event, Metadata, Subscriber};
use tracing_core::span::Current;

const FORM_LIMIT: usize = 20 * 1024 * 1024;

pub(super) async fn prepare(request: &mut HttpRequest) -> S3Result<bool> {
    if request.method() != Method::POST {
        return Ok(false);
    }
    let Some(mime) = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<mime::Mime>().ok())
        .filter(|mime| mime.type_() == mime::MULTIPART && mime.subtype() == mime::FORM_DATA)
    else {
        return Ok(false);
    };
    let boundary = mime
        .get_param(mime::BOUNDARY)
        .ok_or_else(|| s3_error!(MalformedPOSTRequest))?;
    let marker = format!("--{}\r\n", boundary.as_str());
    let mut body = std::mem::take(request.body_mut());
    let mut prefix = Vec::new();
    loop {
        if let Some(token) = inspect(&prefix, marker.as_bytes())? {
            let replay: s3s::stream::DynByteStream = Box::pin(Replay {
                prefix: Some(prefix.into()),
                body,
            });
            *request.body_mut() = s3s::Body::from(replay);
            return Ok(token);
        }
        let bytes = body
            .next()
            .await
            .ok_or_else(|| s3_error!(MalformedPOSTRequest))?
            .map_err(|_| s3_error!(MalformedPOSTRequest))?;
        if prefix.len().saturating_add(bytes.len()) > FORM_LIMIT {
            return Err(s3_error!(MalformedPOSTRequest));
        }
        prefix.extend_from_slice(&bytes);
    }
}

fn inspect(mut bytes: &[u8], marker: &[u8]) -> S3Result<Option<bool>> {
    if bytes.len() < 2 {
        return Ok(None);
    }
    if let Some(rest) = bytes.strip_prefix(b"\r\n") {
        bytes = rest;
    }
    if bytes.len() < marker.len() {
        return Ok(None);
    }
    bytes = bytes
        .strip_prefix(marker)
        .ok_or_else(|| s3_error!(MalformedPOSTRequest))?;
    let delimiter = [b"\r\n".as_slice(), marker].concat();
    let (mut token, mut signature_v4, mut signature_v2) = (false, false, false);
    let (mut key_v4, mut key_v2) = (None, None);
    for _ in 0..1000 {
        let mut headers = [httparse::EMPTY_HEADER; 2];
        let (offset, headers) = match httparse::parse_headers(bytes, &mut headers) {
            Ok(httparse::Status::Partial) => return Ok(None),
            Ok(httparse::Status::Complete(parsed)) => parsed,
            Err(_) => return Err(s3_error!(MalformedPOSTRequest)),
        };
        let disposition = headers
            .iter()
            .rev()
            .find(|header| header.name.eq_ignore_ascii_case("content-disposition"))
            .and_then(|header| std::str::from_utf8(header.value).ok())
            .ok_or_else(|| s3_error!(MalformedPOSTRequest))?;
        let (name, suffix) = disposition
            .strip_prefix("form-data; name=\"")
            .and_then(|value| value.split_once('"'))
            .filter(|(name, _)| !name.is_empty())
            .ok_or_else(|| s3_error!(MalformedPOSTRequest))?;
        if !suffix.is_empty()
            && !suffix
                .strip_prefix("; filename=\"")
                .and_then(|value| value.strip_suffix('"'))
                .is_some_and(|value| !value.is_empty() && !value.contains('"'))
        {
            return Err(s3_error!(MalformedPOSTRequest));
        }
        if name.eq_ignore_ascii_case("file") {
            let key = if signature_v4 {
                key_v4
            } else if signature_v2 {
                key_v2
            } else {
                None
            };
            if token && !key.is_some_and(S3Session::is_session_key) {
                return Err(s3_error!(
                    InvalidToken,
                    "Token credentials accept the security token only as a header"
                ));
            }
            return Ok(Some(token));
        }
        bytes = &bytes[offset..];
        let Some(end) = bytes
            .windows(delimiter.len())
            .position(|window| window == delimiter)
        else {
            return Ok(None);
        };
        if end > 1024 * 1024 {
            return Err(s3_error!(MalformedPOSTRequest));
        }
        let value =
            std::str::from_utf8(&bytes[..end]).map_err(|_| s3_error!(MalformedPOSTRequest))?;
        if name.eq_ignore_ascii_case("x-amz-security-token") {
            token = true;
        } else if name.eq_ignore_ascii_case("x-amz-credential") {
            key_v4 = value.split('/').next();
        } else if name.eq_ignore_ascii_case("awsaccesskeyid") {
            key_v2 = Some(value);
        } else if name.eq_ignore_ascii_case("x-amz-signature") {
            signature_v4 = true;
        } else if name.eq_ignore_ascii_case("signature") {
            signature_v2 = true;
        }
        bytes = &bytes[end + delimiter.len()..];
    }
    Err(s3_error!(MalformedPOSTRequest))
}

struct Replay {
    prefix: Option<Bytes>,
    body: s3s::Body,
}

impl Stream for Replay {
    type Item = Result<Bytes, s3s::StdError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(prefix) = self.prefix.take() {
            return Poll::Ready(Some(Ok(prefix)));
        }
        Pin::new(&mut self.body).poll_next(context)
    }
}

impl s3s::stream::ByteStream for Replay {
    fn remaining_length(&self) -> s3s::stream::RemainingLength {
        use hyper::body::Body;
        let prefix = self.prefix.as_ref().map_or(0, Bytes::len);
        let hint = Body::size_hint(&self.body);
        s3s::stream::RemainingLength::new(
            prefix.saturating_add(hint.lower() as usize),
            hint.upper()
                .map(|upper| prefix.saturating_add(upper as usize)),
        )
    }
}

pub(super) fn safe_dispatch() -> Dispatch {
    Dispatch::new(PostLogs(tracing::dispatcher::get_default(Clone::clone)))
}

struct PostLogs(Dispatch);

fn multipart_dump(metadata: &Metadata<'_>) -> bool {
    metadata.target().starts_with("s3s::") && metadata.fields().field("multipart").is_some()
}

impl Subscriber for PostLogs {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        !multipart_dump(metadata) && self.0.enabled(metadata)
    }

    fn register_callsite(
        &self,
        metadata: &'static Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        self.0.register_callsite(metadata);
        tracing::subscriber::Interest::sometimes()
    }

    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        self.0.new_span(attributes)
    }

    fn record(&self, span: &Id, values: &Record<'_>) {
        self.0.record(span, values);
    }

    fn record_follows_from(&self, span: &Id, follows: &Id) {
        self.0.record_follows_from(span, follows);
    }

    fn event(&self, event: &Event<'_>) {
        if !multipart_dump(event.metadata()) {
            self.0.event(event);
        }
    }

    fn enter(&self, span: &Id) {
        self.0.enter(span);
    }

    fn exit(&self, span: &Id) {
        self.0.exit(span);
    }

    fn clone_span(&self, span: &Id) -> Id {
        self.0.clone_span(span)
    }

    fn try_close(&self, span: Id) -> bool {
        self.0.try_close(span)
    }

    fn current_span(&self) -> Current {
        self.0.current_span()
    }
}
