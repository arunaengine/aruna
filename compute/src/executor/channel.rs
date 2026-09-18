//! Wraps a channel receiver so transfer chunks can be read as a stream.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::Stream;
use tokio::sync::mpsc;

/// Carries transfer chunks across task borders.
pub(crate) struct ChannelStream<T>(pub(crate) mpsc::Receiver<T>);

impl<T> Stream for ChannelStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.0.poll_recv(cx)
    }
}
