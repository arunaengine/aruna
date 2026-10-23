//! Passes source bytes through and fails the stream at its end when they miss a recorded digest.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_blob::hash::Hasher;
use aruna_core::stream::{BackendStream, BoxStream, StreamError};
use aruna_core::structs::checksum::{ExpectedChecksum, unmatched_checksum};
use bytes::Bytes;
use futures_util::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// The bytes of `stream`, checked against `expected` once complete; a mismatch is the last item.
pub(crate) fn verified(
    stream: BackendStream<Result<Bytes, StreamError>>,
    expected: Vec<ExpectedChecksum>,
) -> BackendStream<Result<Bytes, StreamError>> {
    BackendStream(Box::pin(Verified {
        inner: stream.0,
        hasher: Some(Hasher::new()),
        expected,
    }))
}

struct Verified {
    inner: BoxStream<'static, Result<Bytes, StreamError>>,
    hasher: Option<Hasher>,
    expected: Vec<ExpectedChecksum>,
}

impl Stream for Verified {
    type Item = Result<Bytes, StreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.hasher.is_none() {
            return Poll::Ready(None);
        }
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                if let Some(hasher) = this.hasher.as_mut() {
                    hasher.update(&bytes);
                }
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(error))) => {
                this.hasher = None;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                let hashes = this.hasher.take().map(|hasher| hasher.to_map());
                let failed = hashes.and_then(|hashes| unmatched_checksum(&this.expected, &hashes));
                Poll::Ready(failed.map(|failed| {
                    let message = format!(
                        "source bytes do not match the recorded {} digest",
                        failed.algorithm.hash_key()
                    );
                    Err(StreamError(Box::new(std::io::Error::other(message))))
                }))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::checksum::ChecksumAlgorithm;
    use futures_util::{StreamExt, stream};

    fn body() -> BackendStream<Result<Bytes, StreamError>> {
        BackendStream::new(stream::iter(vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"hel")),
            Ok(Bytes::from_static(b"lo")),
        ]))
    }

    fn md5(digest: Vec<u8>) -> Vec<ExpectedChecksum> {
        let algorithm = ChecksumAlgorithm::Md5;
        vec![ExpectedChecksum { algorithm, digest }]
    }

    #[tokio::test]
    async fn digest_checked_last() {
        let hashes = Hasher::new_with_bytes(b"hello").to_map();
        let matching = md5(hashes[aruna_core::structs::checksum::HASH_MD5].clone());
        let items: Vec<_> = verified(body(), matching).collect().await;
        assert_eq!(items.len(), 2);
        assert!(items.iter().all(Result::is_ok));

        let items: Vec<_> = verified(body(), md5(vec![0; 16])).collect().await;
        assert_eq!(items.len(), 3);
        assert!(items[..2].iter().all(Result::is_ok));
        assert!(items[2].is_err());
    }
}
