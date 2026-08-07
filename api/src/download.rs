use crate::rate_limit::{LocalKey, LocalPermit};
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::stream::{BackendStream, StreamError};
use axum::body::Body;
use bytes::Bytes;
use futures_util::StreamExt;
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

pub(crate) fn admit(
    state: &ServerState,
    user: Option<UserId>,
) -> Result<DownloadPermit, AdmissionError> {
    let total = state.try_acquire_download().ok_or(AdmissionError::Total)?;
    let user = match user {
        Some(user) => match state.rate_limits().try_acquire_local(LocalKey::User(user)) {
            Some(permit) => Some(permit),
            None => {
                drop(total);
                return Err(AdmissionError::User);
            }
        },
        None => None,
    };
    Ok(DownloadPermit { total, user })
}

pub(crate) fn body(
    source: BackendStream<Result<Bytes, StreamError>>,
    permit: DownloadPermit,
) -> Body {
    let (sender, receiver) = mpsc::channel(CHANNEL_SIZE);
    tokio::spawn(pump(source, sender, permit));
    Body::from_stream(receiver)
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
    use super::{DownloadPermit, IDLE_LIMIT, body};
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
}
