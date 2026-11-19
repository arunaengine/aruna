//! Wraps a backend byte stream in a boxed type with one error kind, a success callback and the
//! plain transforms operations put on a stream they pass on.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

pub type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + Sync + 'a>>;
pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug)]
pub struct StreamError(pub BoxError);

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Error for StreamError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.0.source()
    }
}

pub struct BackendStream<T>(pub BoxStream<'static, T>);

type CompletionFuture = Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + 'static>>;
type CompletionCallback = Box<dyn FnOnce() -> CompletionFuture + Send + Sync + 'static>;

impl<T> BackendStream<Result<T, StreamError>> {
    pub fn new<S, E>(stream: S) -> Self
    where
        S: Stream<Item = Result<T, E>> + Send + Sync + 'static,
        E: Error + Send + Sync + 'static,
    {
        let mapped = stream.map(|result| result.map_err(|e| StreamError(Box::new(e))));
        BackendStream(Box::pin(mapped))
    }

    pub fn new_from_boxed<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<T, BoxError>> + Send + Sync + 'static,
    {
        let mapped = stream.map(|result| result.map_err(StreamError));
        BackendStream(Box::pin(mapped))
    }

    pub fn on_success<F>(self, callback: F) -> Self
    where
        F: FnOnce() + Send + Sync + 'static,
        T: 'static,
    {
        BackendStream(Box::pin(StreamCompletionCallback {
            inner: self,
            callback: Some(Box::new(callback)),
            failed: false,
        }))
    }

    pub fn on_success_async<F, Fut>(self, callback: F) -> Self
    where
        F: FnOnce() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), StreamError>> + Send + 'static,
        T: 'static,
    {
        BackendStream(Box::pin(AsyncCompletionCallback {
            inner: self,
            callback: Some(Box::new(move || Box::pin(callback()))),
            pending: Mutex::new(None),
            failed: false,
            completed: false,
        }))
    }
}

impl<T: Send + Sync + 'static> BackendStream<Result<T, StreamError>> {
    /// A stream that ends at once.
    pub fn empty() -> Self {
        BackendStream(Box::pin(futures::stream::empty()))
    }

    /// A stream of the one `item`.
    pub fn once(item: T) -> Self {
        BackendStream(Box::pin(futures::stream::iter([Ok(item)])))
    }

    /// Keeps `guard` alive until the stream is dropped.
    pub fn keep<G: Send + Sync + 'static>(self, guard: G) -> Self {
        BackendStream(Box::pin(self.0.map(move |item| {
            let _guard = &guard;
            item
        })))
    }
}

impl BackendStream<Result<Bytes, StreamError>> {
    /// Adds the length of each chunk to `counter` as the reader takes it.
    pub fn counted(self, counter: Arc<AtomicU64>) -> Self {
        BackendStream(Box::pin(self.0.inspect(move |chunk| {
            if let Ok(chunk) = chunk {
                counter.fetch_add(chunk.len() as u64, Ordering::Relaxed);
            }
        })))
    }

    /// The bytes of `range` from a stream of the whole object; the rest is skipped.
    pub fn slice(self, range: Range<u64>) -> Self {
        let sliced = self
            .0
            .scan(0u64, move |offset, chunk| {
                let start = *offset;
                let item = match chunk {
                    Ok(_) if start >= range.end => return futures::future::ready(None),
                    Ok(chunk) => {
                        let len = chunk.len() as u64;
                        *offset = start + len;
                        let from = range.start.saturating_sub(start).min(len) as usize;
                        let to = (range.end - start).min(len) as usize;
                        (from < to).then(|| Ok(chunk.slice(from..to)))
                    }
                    Err(error) => Some(Err(error)),
                };
                futures::future::ready(Some(item))
            })
            .filter_map(futures::future::ready);
        BackendStream(Box::pin(sliced))
    }
}

struct StreamCompletionCallback<T> {
    inner: BackendStream<Result<T, StreamError>>,
    callback: Option<Box<dyn FnOnce() + Send + Sync + 'static>>,
    failed: bool,
}

impl<T> Unpin for StreamCompletionCallback<T> {}

impl<T> Stream for StreamCompletionCallback<T> {
    type Item = Result<T, StreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(Ok(item))),
            Poll::Ready(Some(Err(error))) => {
                self.failed = true;
                self.callback = None;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                if !self.failed
                    && let Some(callback) = self.callback.take()
                {
                    callback();
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

struct AsyncCompletionCallback<T> {
    inner: BackendStream<Result<T, StreamError>>,
    callback: Option<CompletionCallback>,
    pending: Mutex<Option<CompletionFuture>>,
    failed: bool,
    completed: bool,
}

impl<T> Unpin for AsyncCompletionCallback<T> {}

impl<T> Stream for AsyncCompletionCallback<T> {
    type Item = Result<T, StreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.completed {
            return Poll::Ready(None);
        }

        if let Some(mut pending) = self
            .pending
            .get_mut()
            .expect("completion future mutex poisoned")
            .take()
        {
            return match pending.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    self.completed = true;
                    Poll::Ready(None)
                }
                Poll::Ready(Err(error)) => {
                    self.completed = true;
                    Poll::Ready(Some(Err(error)))
                }
                Poll::Pending => {
                    *self
                        .pending
                        .get_mut()
                        .expect("completion future mutex poisoned") = Some(pending);
                    Poll::Pending
                }
            };
        }

        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(Ok(item))),
            Poll::Ready(Some(Err(error))) => {
                self.failed = true;
                self.callback = None;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                if self.failed {
                    self.completed = true;
                    return Poll::Ready(None);
                }
                if let Some(callback) = self.callback.take() {
                    *self
                        .pending
                        .get_mut()
                        .expect("completion future mutex poisoned") = Some(callback());
                    self.poll_next(cx)
                } else {
                    self.completed = true;
                    Poll::Ready(None)
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> fmt::Debug for BackendStream<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackendStream")
            .field("stream", &"<opaque>")
            .finish()
    }
}

impl<T> std::ops::Deref for BackendStream<T> {
    type Target = BoxStream<'static, T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for BackendStream<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

//Note: Just to satisfy PartialEq of Effect and Event
impl<T> PartialEq for BackendStream<T> {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::addr_eq(
            self.0.as_ref().get_ref() as *const _,
            other.0.as_ref().get_ref() as *const _,
        )
    }
}

impl<T> Stream for BackendStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::BackendStream;
    use futures::{StreamExt, stream};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn on_success_completion() {
        futures::executor::block_on(async {
            let calls = Arc::new(AtomicUsize::new(0));
            let calls_for_callback = calls.clone();
            let mut stream = BackendStream::new(stream::iter(vec![Ok::<_, std::io::Error>(1)]))
                .on_success(move || {
                    calls_for_callback.fetch_add(1, Ordering::SeqCst);
                });

            assert_eq!(stream.next().await.unwrap().unwrap(), 1);
            assert!(stream.next().await.is_none());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn on_success_error() {
        futures::executor::block_on(async {
            let calls = Arc::new(AtomicUsize::new(0));
            let calls_for_callback = calls.clone();
            let mut stream = BackendStream::new(stream::iter(vec![Err::<i32, _>(
                std::io::Error::other("boom"),
            )]))
            .on_success(move || {
                calls_for_callback.fetch_add(1, Ordering::SeqCst);
            });

            assert!(stream.next().await.unwrap().is_err());
            assert!(stream.next().await.is_none());
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        });
    }

    #[test]
    fn on_async_completion() {
        futures::executor::block_on(async {
            let calls = Arc::new(AtomicUsize::new(0));
            let calls_for_callback = calls.clone();
            let mut stream = BackendStream::new(stream::iter(vec![Ok::<_, std::io::Error>(1)]))
                .on_success_async(move || async move {
                    calls_for_callback.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                });

            assert_eq!(stream.next().await.unwrap().unwrap(), 1);
            assert!(stream.next().await.is_none());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn on_async_error() {
        futures::executor::block_on(async {
            let mut stream = BackendStream::new(stream::iter(vec![Ok::<_, std::io::Error>(1)]))
                .on_success_async(move || async move {
                    Err(super::StreamError(Box::new(std::io::Error::other(
                        "callback failed",
                    ))))
                });

            assert_eq!(stream.next().await.unwrap().unwrap(), 1);
            assert!(stream.next().await.unwrap().is_err());
            assert!(stream.next().await.is_none());
        });
    }
}
