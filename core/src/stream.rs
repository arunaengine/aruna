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

impl<T> Stream for BackendStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}
