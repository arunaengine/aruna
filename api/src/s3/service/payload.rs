//! Frames a forwarded body with a trailer the issuing node signs over its length and digest,
//! and checks that trailer on the coordinator before the write can commit (D33).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// A body that was cut, extended or replaced fails the stream, so the write never commits.

use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use aruna_blob::hash::Hasher;
use aruna_core::NodeId;
use aruna_core::structs::checksum::ChecksumAlgorithm;
use aruna_core::structs::storage::distribution::delegation::{
    BodyTrailer, TRAILER_BYTES, body_hasher,
};
use aruna_operations::distribution::delegate::sign_body;
use std::collections::HashMap;

use crate::s3::checksum::validate_trailer_headers;
use crate::s3::util::map_checksum_algorithm;
use aruna_operations::driver::DriverContext;
use bytes::Bytes;
use futures_core::Stream;
use s3s::dto::StreamingBlob;
use s3s::{S3Result, s3_error};

/// Reads the client's verified trailing checksum value once the body ended.
pub(super) type ChecksumValue = Arc<dyn Fn() -> Option<String> + Send + Sync>;

/// Appends the signed trailer once `body` ends, with the client's trailing checksum if any.
pub(super) fn frame_body(
    mut body: StreamingBlob,
    context: Arc<DriverContext>,
    nonce: [u8; 16],
    checksum: Option<ChecksumValue>,
) -> StreamingBlob {
    let mut hasher = body_hasher();
    let mut length = 0u64;
    let mut done = false;
    StreamingBlob::wrap::<_, std::io::Error>(futures_util::stream::poll_fn(move |cx| {
        if done {
            return Poll::Ready(None);
        }
        match Pin::new(&mut body).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                hasher.update(&bytes);
                length += bytes.len() as u64;
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(error))) => {
                done = true;
                Poll::Ready(Some(Err(std::io::Error::other(error))))
            }
            Poll::Ready(None) => {
                done = true;
                let trailer = BodyTrailer {
                    length,
                    digest: *hasher.finalize().as_bytes(),
                    checksum: checksum.as_ref().and_then(|value| value()),
                };
                let encoded = sign_body(&context, &nonce, &trailer)
                    .and_then(|signature| trailer.encode(&signature).ok());
                Poll::Ready(Some(match encoded {
                    Some(bytes) => Ok(Bytes::copy_from_slice(&bytes)),
                    None => Err(std::io::Error::other(
                        "the body trailer could not be signed",
                    )),
                }))
            }
            Poll::Pending => Poll::Pending,
        }
    }))
}

/// Whose trailer a framed body must end with.
#[derive(Clone, Debug)]
pub(super) struct BodyCheck {
    pub issuer: NodeId,
    pub nonce: [u8; 16],
    /// The client's trailing checksum the trailer must carry, and whether it is composite.
    pub checksum: Option<(ChecksumAlgorithm, bool)>,
}

/// Passes the body without its trailer and fails the stream unless the trailer is the
/// issuer's signature over exactly the bytes passed.
pub(super) fn unframe_body(mut body: StreamingBlob, check: BodyCheck) -> StreamingBlob {
    let mut held: Vec<u8> = Vec::new();
    let mut hasher = body_hasher();
    let mut sums = check.checksum.map(|_| Hasher::new());
    let mut length = 0u64;
    let mut done = false;
    StreamingBlob::wrap::<_, std::io::Error>(futures_util::stream::poll_fn(move |cx| {
        loop {
            if done {
                return Poll::Ready(None);
            }
            match Pin::new(&mut body).poll_next(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    held.extend_from_slice(&bytes);
                    if held.len() <= TRAILER_BYTES {
                        continue;
                    }
                    let passed: Vec<u8> = held.drain(..held.len() - TRAILER_BYTES).collect();
                    hasher.update(&passed);
                    if let Some(sums) = sums.as_mut() {
                        sums.update(&passed);
                    }
                    length += passed.len() as u64;
                    return Poll::Ready(Some(Ok(Bytes::from(passed))));
                }
                Poll::Ready(Some(Err(error))) => {
                    done = true;
                    return Poll::Ready(Some(Err(std::io::Error::other(error))));
                }
                Poll::Ready(None) => {
                    done = true;
                    let digest = *hasher.finalize().as_bytes();
                    let sums = sums.take().map(|sums| sums.to_map());
                    return match trailer_holds(&check, &held, (length, digest), sums) {
                        true => Poll::Ready(None),
                        false => Poll::Ready(Some(Err(std::io::Error::other(
                            "the forwarded body does not match its signed trailer",
                        )))),
                    };
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }))
}

fn trailer_holds(
    check: &BodyCheck,
    held: &[u8],
    (length, digest): (u64, [u8; 32]),
    sums: Option<HashMap<String, Vec<u8>>>,
) -> bool {
    let Some((trailer, signature)) = BodyTrailer::decode(held) else {
        return false;
    };
    let signed = trailer.length == length
        && trailer.digest == digest
        && check
            .issuer
            .verify(&trailer.signing_bytes(&check.nonce), &signature)
            .is_ok();
    let checksum = match (check.checksum, trailer.checksum, sums) {
        (None, None, _) => true,
        (Some((algorithm, composite)), Some(value), Some(sums)) => {
            checksum_holds(algorithm, composite, &value, &sums)
        }
        _ => false,
    };
    signed && checksum
}

/// Whether the client's trailing checksum matches the bytes the coordinator received.
fn checksum_holds(
    algorithm: ChecksumAlgorithm,
    composite: bool,
    value: &str,
    sums: &HashMap<String, Vec<u8>>,
) -> bool {
    let Some(name) = map_checksum_algorithm(algorithm) else {
        return false;
    };
    let name = format!("x-amz-checksum-{}", name.as_str().to_ascii_lowercase());
    let mut headers = http::HeaderMap::new();
    match (
        http::HeaderName::from_bytes(name.as_bytes()),
        http::HeaderValue::from_str(value),
    ) {
        (Ok(name), Ok(value)) => headers.insert(name, value),
        _ => return false,
    };
    validate_trailer_headers(algorithm, composite, &headers, sums).is_ok()
}

/// The length of a framed body without its trailer.
pub(super) fn unframed_length(length: Option<i64>) -> S3Result<Option<i64>> {
    length
        .map(|length| {
            length
                .checked_sub(TRAILER_BYTES as i64)
                .filter(|length| *length >= 0)
                .ok_or_else(|| s3_error!(IncompleteBody, "The forwarded body lacks its trailer"))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    fn secret() -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[4; 32])
    }

    /// A body framed as the issuer would, with a trailer over `signed` instead of `sent`.
    fn framed(sent: &[u8], signed: &[u8]) -> Vec<u8> {
        framed_with(sent, signed, None)
    }

    fn framed_with(sent: &[u8], signed: &[u8], checksum: Option<String>) -> Vec<u8> {
        let trailer = BodyTrailer {
            length: signed.len() as u64,
            digest: *body_hasher().update(signed).finalize().as_bytes(),
            checksum,
        };
        let signature = secret().sign(&trailer.signing_bytes(&[7; 16]));
        let mut bytes = sent.to_vec();
        bytes.extend_from_slice(&trailer.encode(&signature).unwrap());
        bytes
    }

    async fn read(bytes: Vec<u8>, chunk: usize) -> Result<Vec<u8>, String> {
        read_checked(bytes, chunk, None).await
    }

    async fn read_checked(
        bytes: Vec<u8>,
        chunk: usize,
        checksum: Option<(ChecksumAlgorithm, bool)>,
    ) -> Result<Vec<u8>, String> {
        let chunks: Vec<Result<Bytes, std::io::Error>> = bytes
            .chunks(chunk)
            .map(|part| Ok(Bytes::copy_from_slice(part)))
            .collect();
        let body = StreamingBlob::wrap(futures_util::stream::iter(chunks));
        let check = BodyCheck {
            issuer: secret().public(),
            nonce: [7; 16],
            checksum,
        };
        let mut stream = unframe_body(body, check);
        let mut out = Vec::new();
        while let Some(part) = stream.next().await {
            out.extend_from_slice(&part.map_err(|error| error.to_string())?);
        }
        Ok(out)
    }

    #[tokio::test]
    async fn signed_body_passes() {
        let body = b"hello distributed world".to_vec();
        for chunk in [1, 7, 500] {
            assert_eq!(read(framed(&body, &body), chunk).await, Ok(body.clone()));
        }
    }

    #[tokio::test]
    async fn substituted_body_fails() {
        assert!(
            read(framed(b"other bytes", b"hello bytes"), 3)
                .await
                .is_err()
        );
        assert!(read(framed(b"hello", b"hello bytes"), 3).await.is_err());
        assert!(read(b"short".to_vec(), 3).await.is_err());
        assert!(unframed_length(Some(5)).is_err());
        assert_eq!(
            unframed_length(Some(TRAILER_BYTES as i64 + 2)).unwrap(),
            Some(2)
        );
    }

    #[tokio::test]
    async fn trailing_checksum_checked() {
        use base64::Engine;
        let body = b"hello".to_vec();
        let mut sums = Hasher::new();
        sums.update(&body);
        let crc = sums.to_map()[aruna_core::structs::checksum::HASH_CRC32].clone();
        let crc = base64::engine::general_purpose::STANDARD.encode(crc);
        let check = Some((ChecksumAlgorithm::Crc32, false));
        let good = framed_with(&body, &body, Some(crc));
        assert_eq!(read_checked(good, 2, check).await, Ok(body.clone()));
        let wrong = framed_with(&body, &body, Some("AAAAAA==".to_string()));
        assert!(read_checked(wrong, 2, check).await.is_err());
        // A trailer without the declared checksum fails as well.
        assert!(read_checked(framed(&body, &body), 2, check).await.is_err());
    }
}
