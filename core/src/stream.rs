use futures::Stream;
use std::error::Error;
use std::fmt;
use std::pin::Pin;
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

impl<T> BackendStream<Result<T, StreamError>> {
    pub fn new<S, E>(stream: S) -> Self
    where
        S: Stream<Item = Result<T, E>> + Send + Sync + 'static,
        E: Error + Send + Sync + 'static,
    {
        use futures::StreamExt;
        let mapped = stream.map(|result| result.map_err(|e| StreamError(Box::new(e))));
        BackendStream(Box::pin(mapped))
    }

    pub fn new_from_boxed<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<T, BoxError>> + Send + Sync + 'static,
    {
        use futures::StreamExt;
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
    fn on_success_runs_after_stream_completion() {
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
    fn on_success_does_not_run_after_stream_error() {
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
}
