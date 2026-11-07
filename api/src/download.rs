//! Admits downloads against rate limits and streams bodies under idle limits, file bodies also
//! under a lifetime limit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::rate_limit::{LocalKey, LocalPermit};
use crate::server::state::ServerState;
use aruna_core::stream::{BackendStream, StreamError};
use axum::body::Body;
use bytes::Bytes;
use futures_util::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, mpsc};

const CHANNEL_SIZE: usize = 1;
const IDLE_LIMIT: Duration = Duration::from_secs(20);
const LIFETIME_LIMIT: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdmissionError {
    Total,
    User,
}

pub(crate) struct DownloadPermit {
    total: OwnedSemaphorePermit,
    user: Option<LocalPermit>,
}

pub(crate) fn admit(state: &ServerState, key: LocalKey) -> Result<DownloadPermit, AdmissionError> {
    let total = state.try_acquire_download().ok_or(AdmissionError::Total)?;
    let user = match state.rate_limits().try_acquire_local(key) {
        Some(permit) => Some(permit),
        None => {
            drop(total);
            return Err(AdmissionError::User);
        }
    };
    Ok(DownloadPermit { total, user })
}

pub(crate) fn body(
    source: BackendStream<Result<Bytes, StreamError>>,
    permit: DownloadPermit,
) -> Body {
    let (sender, receiver) = mpsc::channel(CHANNEL_SIZE);
    tokio::spawn(pump(source, sender, permit));
    Body::from_stream(futures_util::stream::unfold(
        receiver,
        |mut receiver| async move { receiver.recv().await.map(|item| (item, receiver)) },
    ))
}

/// A body without a lifetime bound, for archives that take as long as their selection. Only a
/// client that stops reading for the idle limit stalls it; then, or on any other cut, the body
/// ends with an error, never with a clean end a client could take for a complete archive.
pub(crate) fn archive_body(
    source: BackendStream<Result<Bytes, StreamError>>,
    permit: DownloadPermit,
) -> Body {
    let (sender, receiver) = mpsc::channel(CHANNEL_SIZE);
    let finished = Arc::new(AtomicBool::new(false));
    tokio::spawn(archive_pump(source, sender, permit, finished.clone()));
    Body::from_stream(futures_util::stream::unfold(
        Some(receiver),
        move |receiver| {
            let finished = finished.clone();
            async move {
                let mut receiver = receiver?;
                match receiver.recv().await {
                    Some(item) => Some((item, Some(receiver))),
                    None if finished.load(Ordering::Acquire) => None,
                    None => Some((Err(StreamError(Box::new(cut()))), None)),
                }
            }
        },
    ))
}

fn cut() -> std::io::Error {
    std::io::Error::other("the archive download was cut before its end")
}

async fn archive_pump(
    mut source: BackendStream<Result<Bytes, StreamError>>,
    sender: mpsc::Sender<Result<Bytes, StreamError>>,
    permit: DownloadPermit,
    finished: Arc<AtomicBool>,
) {
    let _permit = permit;
    // Waiting for the source is the archive's own work, so only a pending send counts.
    while let Some(item) = source.next().await {
        let failed = item.is_err();
        match tokio::time::timeout(IDLE_LIMIT, sender.send(item)).await {
            Ok(Ok(())) if !failed => {}
            _ => return,
        }
    }
    finished.store(true, Ordering::Release);
}

async fn pump(
    mut source: BackendStream<Result<Bytes, StreamError>>,
    sender: mpsc::Sender<Result<Bytes, StreamError>>,
    permit: DownloadPermit,
) {
    let DownloadPermit { total, user } = permit;
    let _total = total;
    let _user = user;
    let deadline = tokio::time::sleep(LIFETIME_LIMIT);
    tokio::pin!(deadline);
    let idle = tokio::time::sleep(IDLE_LIMIT);
    tokio::pin!(idle);

    loop {
        let Some(item) = (tokio::select! {
            _ = &mut deadline => None,
            _ = &mut idle => None,
            item = source.next() => item,
        }) else {
            break;
        };
        let failed = item.is_err();
        let send = sender.send(item);
        tokio::pin!(send);
        let sent = tokio::select! {
            _ = &mut deadline => false,
            _ = &mut idle => false,
            result = &mut send => result.is_ok(),
        };
        if !sent {
            break;
        }
        idle.as_mut()
            .reset(tokio::time::Instant::now() + IDLE_LIMIT);
        if failed {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CHANNEL_SIZE, DownloadPermit, IDLE_LIMIT, archive_body, body};
    use aruna_core::stream::BackendStream;
    use axum::body::to_bytes;
    use bytes::Bytes;
    use futures_util::stream;
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    #[tokio::test(start_paused = true)]
    async fn releases_on_stall() {
        let limit = Arc::new(Semaphore::new(1));
        let permit = limit.clone().try_acquire_owned().expect("permit");
        let source = BackendStream::new(stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"one")),
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"two")),
        ]));
        let _body = body(
            source,
            DownloadPermit {
                total: permit,
                user: None,
            },
        );
        tokio::task::yield_now().await;
        assert_eq!(limit.available_permits(), 0);
        tokio::time::advance(IDLE_LIMIT).await;
        tokio::task::yield_now().await;
        assert_eq!(limit.available_permits(), 1);
        assert_eq!(CHANNEL_SIZE, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn forwards_stream_error() {
        let limit = Arc::new(Semaphore::new(1));
        let permit = limit.clone().try_acquire_owned().expect("permit");
        let source = BackendStream::new(stream::iter(vec![Err::<Bytes, _>(
            std::io::Error::other("source failed"),
        )]));
        let result = to_bytes(
            body(
                source,
                DownloadPermit {
                    total: permit,
                    user: None,
                },
            ),
            1024,
        )
        .await;
        tokio::task::yield_now().await;
        assert!(result.is_err());
        assert_eq!(limit.available_permits(), 1);
    }

    fn permit(limit: &Arc<Semaphore>) -> DownloadPermit {
        DownloadPermit {
            total: limit.clone().try_acquire_owned().expect("permit"),
            user: None,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_archive_errors() {
        let limit = Arc::new(Semaphore::new(1));
        let source = BackendStream::new(stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"one")),
            Ok(Bytes::from_static(b"two")),
            Ok(Bytes::from_static(b"three")),
        ]));
        let archive = archive_body(source, permit(&limit));
        // Nobody reads, so the pump waits on a full channel until the idle limit cuts it.
        tokio::task::yield_now().await;
        tokio::time::advance(IDLE_LIMIT).await;
        tokio::task::yield_now().await;
        assert_eq!(limit.available_permits(), 1);
        assert!(
            to_bytes(archive, 1024).await.is_err(),
            "a cut is never a clean end"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn slow_archive_continues() {
        let limit = Arc::new(Semaphore::new(1));
        let late = stream::once(async {
            tokio::time::sleep(IDLE_LIMIT * 3).await;
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"late"))
        });
        let source = BackendStream::new(late);
        let archive = archive_body(source, permit(&limit));
        let bytes = to_bytes(archive, 1024)
            .await
            .expect("a slow source is no stall");
        assert_eq!(bytes, Bytes::from_static(b"late"));
        tokio::task::yield_now().await;
        assert_eq!(limit.available_permits(), 1);
    }
}
