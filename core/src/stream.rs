use futures::Stream;
use futures::stream::BoxStream;
use std::error::Error;
use std::pin::Pin;
use std::task::{Context, Poll};

pub type BoxError = Box<dyn Error + Send + Sync>;

pub struct BackendStream<T>(pub BoxStream<'static, T>);

impl<T> BackendStream<Result<T, BoxError>> {
    pub fn new<S, E>(stream: S) -> Self
    where
        S: Stream<Item = Result<T, E>> + Unpin + Send + Sync + 'static,
        E: Into<BoxError>,
    {
        use futures::StreamExt;

        let boxed = stream.map(|result| result.map_err(Into::into)).boxed();

        BackendStream(boxed)
    }
}

impl<T> std::fmt::Debug for BackendStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendStream")
            .field("stream", &"<opaque>")
            .finish()
    }
}

// Implement Deref to use it like a BoxStream
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
