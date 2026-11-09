//! Hands finished TLS connections to the service of their route, next to its plain listener.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::SocketAddr;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::server::TlsStream;
use tokio_util::either::Either;

use super::HostCheck;

/// Finished handshakes one route may queue before a sender has to wait.
const ROUTE_QUEUE: usize = 64;

/// The TLS connections of one route, with the client's TCP address.
pub struct RoutedListener {
    receiver: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
    check: HostCheck,
}

impl RoutedListener {
    /// A bounded channel for one route: the HTTPS front sends, the service accepts.
    pub fn channel(check: HostCheck) -> (mpsc::Sender<(TlsStream<TcpStream>, SocketAddr)>, Self) {
        Self::bounded(check, ROUTE_QUEUE)
    }

    pub(crate) fn bounded(
        check: HostCheck,
        capacity: usize,
    ) -> (mpsc::Sender<(TlsStream<TcpStream>, SocketAddr)>, Self) {
        let (sender, receiver) = mpsc::channel(capacity);
        (sender, Self { receiver, check })
    }

    /// Waits forever once the front is gone, so the service ends only on its shutdown token.
    async fn accept(&mut self) -> (TlsStream<TcpStream>, SocketAddr) {
        match self.receiver.recv().await {
            Some(accepted) => accepted,
            None => std::future::pending().await,
        }
    }
}

/// A service's plain listener and, with native HTTPS, the TLS connections of its route.
pub struct CombinedListener {
    plain: TcpListener,
    routed: Option<RoutedListener>,
}

impl CombinedListener {
    pub fn new(plain: TcpListener, routed: Option<RoutedListener>) -> Self {
        Self { plain, routed }
    }

    /// The route check for TLS connections; `None` when this service has no HTTPS route.
    pub fn host_check(&self) -> Option<HostCheck> {
        self.routed.as_ref().map(|routed| routed.check.clone())
    }
}

impl From<TcpListener> for CombinedListener {
    fn from(plain: TcpListener) -> Self {
        Self::new(plain, None)
    }
}

impl axum::serve::Listener for CombinedListener {
    type Io = Either<TcpStream, TlsStream<TcpStream>>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let plain = &mut self.plain;
        match self.routed.as_mut() {
            None => {
                let (stream, peer) = axum::serve::Listener::accept(plain).await;
                (Either::Left(stream), peer)
            }
            Some(routed) => tokio::select! {
                (stream, peer) = axum::serve::Listener::accept(plain) => (Either::Left(stream), peer),
                (stream, peer) = routed.accept() => (Either::Right(stream), peer),
            },
        }
    }

    /// The plain address, which interface registration advertises as before.
    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.plain.local_addr()
    }
}
