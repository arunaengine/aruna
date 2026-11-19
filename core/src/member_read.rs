//! One HTTP request to another member of the realm: what an operation composes, how its body
//! travels and what the member answered.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::io;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use futures::stream;
use ulid::Ulid;

use crate::jobs::ByteStream;
use crate::stream::BackendStream;
use crate::structs::storage::distribution::delegation::{BodyTrailer, body_hasher};

const VERSION_HEADER: &str = "x-amz-version-id";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemberMethod {
    Get,
    Head,
    Put,
    Post,
    Patch,
    Delete,
}

impl MemberMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Head => "HEAD",
            Self::Put => "PUT",
            Self::Post => "POST",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        }
    }

    /// The method an HTTP request names, when a member request can carry it.
    pub fn parse(name: &str) -> Option<Self> {
        [
            Self::Get,
            Self::Head,
            Self::Put,
            Self::Post,
            Self::Patch,
            Self::Delete,
        ]
        .into_iter()
        .find(|method| method.as_str() == name)
    }
}

/// Reads the client's verified trailing checksum once a forwarded body ended.
#[derive(Clone)]
pub struct TrailerValue(pub Arc<dyn Fn() -> Option<String> + Send + Sync>);

impl std::fmt::Debug for TrailerValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TrailerValue")
    }
}

/// Equal only to itself: a reader has no comparable state.
impl PartialEq for TrailerValue {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// The body a request sends.
#[derive(Debug, PartialEq)]
pub enum MemberSend {
    Empty,
    Bytes(Bytes),
    /// Streams and ends with a trailer this node signs over its length and digest.
    Framed {
        stream: ByteStream,
        nonce: [u8; 16],
        checksum: Option<TrailerValue>,
    },
}

/// One request the operation composed; the outbound backend sends it as it is. Its headers carry
/// credentials and its url names the key, so neither shows in a `Debug` print.
#[derive(PartialEq)]
pub struct MemberFetch {
    pub method: MemberMethod,
    pub url: String,
    pub headers: Vec<(String, String)>,
    /// Headers a relay passes on byte for byte, as it received them.
    pub raw_headers: Vec<(String, Vec<u8>)>,
    pub body: MemberSend,
    /// How long the member may take to start its answer.
    pub answer_limit: Option<Duration>,
    /// How long the whole exchange may take, the answer's body included.
    pub whole_limit: Option<Duration>,
    /// Hands back a refusal like any other answer, its body streaming through.
    pub any_status: bool,
    /// Takes a redirect as the answer instead of following it.
    pub no_redirect: bool,
}

impl std::fmt::Debug for MemberFetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MemberFetch")
    }
}

/// A member's answer: its status, its headers with lowercase names and its bytes.
#[derive(Debug, PartialEq)]
pub struct MemberBody {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub stream: ByteStream,
}

impl MemberBody {
    /// The first value of the header `name`, given in lowercase.
    pub fn header(&self, name: &str) -> Option<&str> {
        (self.headers.iter())
            .find(|(header, _)| header == name)
            .map(|(_, value)| value.as_str())
    }

    /// The version the member names for these bytes.
    pub fn version(&self) -> Option<Ulid> {
        Ulid::from_string(self.header(VERSION_HEADER)?).ok()
    }
}

/// What a member answered a [`MemberFetch`].
#[derive(Debug, PartialEq)]
pub enum MemberAnswer {
    Body(MemberBody),
    /// The member refused, with its status and the start of its answer.
    Refused {
        status: u16,
        body: String,
    },
    /// The member could not be reached, so it never saw the request.
    Silent,
    /// The member did not answer in time.
    Late,
    /// The request went out but no answer came back, so the member may have acted on it.
    Lost,
}

/// Seals the trailer of a framed body with this node's key.
pub trait TrailerSeal: Send + Sync {
    fn seal(&self, trailer: &BodyTrailer, nonce: &[u8; 16]) -> Option<Vec<u8>>;
}

/// `body` followed by its sealed trailer, with the client's trailing checksum if any.
pub fn framed_stream(
    mut body: ByteStream,
    nonce: [u8; 16],
    checksum: Option<TrailerValue>,
    seal: Arc<dyn TrailerSeal>,
) -> ByteStream {
    let mut hasher = body_hasher();
    let mut length = 0u64;
    let mut done = false;
    BackendStream::new(stream::poll_fn(move |cx| {
        if done {
            return Poll::Ready(None);
        }
        match body.0.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                hasher.update(&bytes);
                length += bytes.len() as u64;
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(error))) => {
                done = true;
                Poll::Ready(Some(Err(io::Error::other(error))))
            }
            Poll::Ready(None) => {
                done = true;
                let trailer = BodyTrailer {
                    length,
                    digest: *hasher.finalize().as_bytes(),
                    checksum: checksum.as_ref().and_then(|value| (value.0)()),
                };
                Poll::Ready(Some(match seal.seal(&trailer, &nonce) {
                    Some(bytes) => Ok(Bytes::from(bytes)),
                    None => Err(io::Error::other("the body trailer could not be signed")),
                }))
            }
            Poll::Pending => Poll::Pending,
        }
    }))
}
