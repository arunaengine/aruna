//! Frames a forwarded body with a trailer the issuing node signs over its length and digest,
//! and checks that trailer on the coordinator before the write can commit (D33). A body that
//! was cut, extended or replaced fails the stream, so the write never commits.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use aruna_core::NodeId;
use aruna_core::structs::storage::distribution::delegation::{
    BodyTrailer, TRAILER_BYTES, body_hasher,
};
use aruna_operations::distribution::delegate::sign_body;
use aruna_operations::driver::DriverContext;
use bytes::Bytes;
use futures_core::Stream;
use s3s::dto::StreamingBlob;
use s3s::{S3Result, s3_error};

/// Appends the signed trailer once `body` ends.
pub(super) fn frame_body(
    mut body: StreamingBlob,
    context: Arc<DriverContext>,
    nonce: [u8; 16],
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
                    checksum: None,
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
}

/// Passes the body without its trailer and fails the stream unless the trailer is the
/// issuer's signature over exactly the bytes passed.
pub(super) fn unframe_body(mut body: StreamingBlob, check: BodyCheck) -> StreamingBlob {
    let mut held: Vec<u8> = Vec::new();
    let mut hasher = body_hasher();
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
                    return match trailer_holds(&check, &held, length, digest) {
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

fn trailer_holds(check: &BodyCheck, held: &[u8], length: u64, digest: [u8; 32]) -> bool {
    let Some((trailer, signature)) = BodyTrailer::decode(held) else {
        return false;
    };
    trailer.length == length
        && trailer.digest == digest
        && check
            .issuer
            .verify(&trailer.signing_bytes(&check.nonce), &signature)
            .is_ok()
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
        let trailer = BodyTrailer {
            length: signed.len() as u64,
            digest: *body_hasher().update(signed).finalize().as_bytes(),
            checksum: None,
        };
        let signature = secret().sign(&trailer.signing_bytes(&[7; 16]));
        let mut bytes = sent.to_vec();
        bytes.extend_from_slice(&trailer.encode(&signature).unwrap());
        bytes
    }

    async fn read(bytes: Vec<u8>, chunk: usize) -> Result<Vec<u8>, String> {
        let chunks: Vec<Result<Bytes, std::io::Error>> = bytes
            .chunks(chunk)
            .map(|part| Ok(Bytes::copy_from_slice(part)))
            .collect();
        let body = StreamingBlob::wrap(futures_util::stream::iter(chunks));
        let check = BodyCheck {
            issuer: secret().public(),
            nonce: [7; 16],
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
}
