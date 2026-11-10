//! Hands finished TLS connections to the service of their route, next to its plain listener.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ConnectInfo;
use axum::serve::{IncomingStream, Listener};
use axum::{Extension, Router};
use rustls::ServerConfig;
use rustls::server::Acceptor;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_rustls::LazyConfigAcceptor;
use tokio_rustls::server::TlsStream;
use tokio_util::either::Either;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower::{Layer, service_fn};
use tracing::{debug, info};

use super::certificate::CertificateStore;
use super::{HostCheck, HostRoute};

/// Finished handshakes one route may queue before a sender has to wait.
const ROUTE_QUEUE: usize = 64;
/// Open handshakes at once; a connection beyond them is dropped at accept.
const HANDSHAKE_PERMITS: usize = 1024;
/// One bound for the ClientHello, the handshake and the wait for queue space.
const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);

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

/// Serves `plain` on plain connections and `tls`, when set, on TLS ones. Both
/// get the client's TCP address as `ConnectInfo`, as axum's connect-info service adds it.
pub(crate) async fn serve_routers(
    listener: CombinedListener,
    plain: Router,
    tls: Option<Router>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let connections = service_fn(move |incoming: IncomingStream<'_, CombinedListener>| {
        let selected = match (incoming.io(), &tls) {
            (Either::Right(_), Some(tls)) => tls.clone(),
            _ => plain.clone(),
        };
        let peer = ConnectInfo(*incoming.remote_addr());
        std::future::ready(Ok::<_, Infallible>(Extension(peer).layer(selected)))
    });
    axum::serve(listener, connections)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
}

/// What every handshake task of one front shares.
struct Front {
    store: Arc<CertificateStore>,
    configs: HashMap<HostRoute, Arc<ServerConfig>>,
    senders: HashMap<HostRoute, mpsc::Sender<(TlsStream<TcpStream>, SocketAddr)>>,
    permits: Arc<Semaphore>,
    deadline: Duration,
}

/// The HTTPS front and the tasks native HTTPS adds. Handed-over connections
/// belong to their service, so this handle owns only accepts and handshakes.
pub struct HttpsHandle {
    tasks: Vec<JoinHandle<()>>,
    handshakes: TaskTracker,
    abort_handshakes: CancellationToken,
}

impl HttpsHandle {
    /// Starts accepting on `listener`. Fails only when a TLS configuration
    /// cannot be built, before any task starts.
    pub fn spawn(
        listener: TcpListener,
        store: Arc<CertificateStore>,
        senders: HashMap<HostRoute, mpsc::Sender<(TlsStream<TcpStream>, SocketAddr)>>,
        shutdown: CancellationToken,
    ) -> Result<Self, rustls::Error> {
        Self::spawn_with(
            listener,
            store,
            senders,
            shutdown,
            HANDSHAKE_PERMITS,
            HANDSHAKE_DEADLINE,
        )
    }

    pub(crate) fn spawn_with(
        listener: TcpListener,
        store: Arc<CertificateStore>,
        senders: HashMap<HostRoute, mpsc::Sender<(TlsStream<TcpStream>, SocketAddr)>>,
        shutdown: CancellationToken,
        permits: usize,
        deadline: Duration,
    ) -> Result<Self, rustls::Error> {
        let mut configs = HashMap::new();
        for route in senders.keys() {
            configs.insert(*route, server_config(store.clone(), *route)?);
        }
        let front = Arc::new(Front {
            store,
            configs,
            senders,
            permits: Arc::new(Semaphore::new(permits)),
            deadline,
        });
        let handshakes = TaskTracker::new();
        let abort_handshakes = CancellationToken::new();
        if let Ok(address) = listener.local_addr() {
            info!(address = %address, "HTTPS listener running");
        }
        let task = tokio::spawn(accept_loop(
            listener,
            front,
            handshakes.clone(),
            shutdown,
            abort_handshakes.clone(),
        ));
        Ok(Self {
            tasks: vec![task],
            handshakes,
            abort_handshakes,
        })
    }

    /// Whether any task has ended, for the readiness check before the supervision loop.
    pub fn is_finished(&self) -> bool {
        self.tasks.iter().any(JoinHandle::is_finished)
    }

    /// Adds a task, such as the redirect or the reload, to this handle's supervision and drain.
    pub fn attach(&mut self, task: JoinHandle<()>) {
        self.tasks.push(task);
    }

    /// Resolves when any task ends, for the supervision select. Pends without tasks.
    pub async fn exit(&mut self) -> String {
        if self.tasks.is_empty() {
            return std::future::pending().await;
        }
        let (result, index, _) = futures_util::future::select_all(self.tasks.iter_mut()).await;
        self.tasks.remove(index);
        match result {
            Ok(()) => "HTTPS server stopped unexpectedly".to_string(),
            Err(error) if error.is_cancelled() => "HTTPS server aborted".to_string(),
            Err(error) => format!("HTTPS server panicked: {error}"),
        }
    }

    /// Stops every task and handshake at once; does not wait.
    pub fn abort(&self) {
        self.abort_handshakes.cancel();
        for task in &self.tasks {
            task.abort();
        }
    }

    /// Waits for every task and open handshake while the handle stays owned.
    pub async fn wait_until_released(&mut self) {
        // Drops each handle once joined, so a call cancelled by a deadline can run again.
        while let Some(task) = self.tasks.first_mut() {
            let _ = task.await;
            self.tasks.remove(0);
        }
        self.handshakes.close();
        self.handshakes.wait().await;
    }
}

/// HTTP/2 for REST and portal; S3 serves HTTP/1.1 only, as on its plain listener.
fn server_config(
    store: Arc<CertificateStore>,
    route: HostRoute,
) -> Result<Arc<ServerConfig>, rustls::Error> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_cert_resolver(store);
    config.alpn_protocols = match route {
        HostRoute::S3 => vec![b"http/1.1".to_vec()],
        HostRoute::Rest | HostRoute::Portal => vec![b"h2".to_vec(), b"http/1.1".to_vec()],
    };
    Ok(Arc::new(config))
}

async fn accept_loop(
    mut listener: TcpListener,
    front: Arc<Front>,
    handshakes: TaskTracker,
    shutdown: CancellationToken,
    abort: CancellationToken,
) {
    loop {
        let (socket, peer) = tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = Listener::accept(&mut listener) => accepted,
        };
        let Ok(permit) = front.permits.clone().try_acquire_owned() else {
            debug!(peer = %peer, "Dropped an HTTPS connection: too many open handshakes");
            continue;
        };
        handshakes.spawn(handshake(
            socket,
            peer,
            permit,
            front.clone(),
            shutdown.clone(),
            abort.clone(),
        ));
    }
}

/// Reads the SNI name, finishes the handshake for its route and queues the
/// stream. The permit is held until the stream is queued or dropped.
async fn handshake(
    socket: TcpStream,
    peer: SocketAddr,
    _permit: OwnedSemaphorePermit,
    front: Arc<Front>,
    shutdown: CancellationToken,
    abort: CancellationToken,
) {
    let handoff = async {
        let start = LazyConfigAcceptor::new(Acceptor::default(), socket)
            .await
            .map_err(|error| error.to_string())?;
        let route = start
            .client_hello()
            .server_name()
            .and_then(|name| front.store.routes().route_for(name))
            .ok_or_else(|| "no routed SNI name".to_string())?;
        let (Some(config), Some(sender)) = (front.configs.get(&route), front.senders.get(&route))
        else {
            return Err(format!("{route:?} is not served here"));
        };
        let stream = start
            .into_stream(config.clone())
            .await
            .map_err(|error| error.to_string())?;
        sender
            .send((stream, peer))
            .await
            .map_err(|_| "the service stopped accepting".to_string())
    };
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => {}
        _ = abort.cancelled() => {}
        outcome = tokio::time::timeout(front.deadline, handoff) => match outcome {
            Ok(Ok(())) => {}
            Ok(Err(reason)) => debug!(peer = %peer, reason = %reason, "HTTPS handshake refused"),
            Err(_) => debug!(peer = %peer, "HTTPS handshake timed out"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A drain cut short by its deadline is aborted and joined again without polling a done task.
    #[tokio::test]
    async fn rejoin_after_deadline() {
        let done = tokio::spawn(async {});
        while !done.is_finished() {
            tokio::task::yield_now().await;
        }
        let mut handle = HttpsHandle {
            tasks: vec![done, tokio::spawn(std::future::pending())],
            handshakes: TaskTracker::new(),
            abort_handshakes: CancellationToken::new(),
        };
        let cut = tokio::time::timeout(Duration::from_millis(50), handle.wait_until_released());
        assert!(cut.await.is_err(), "the pending task holds the drain");
        handle.abort();
        tokio::time::timeout(Duration::from_secs(10), handle.wait_until_released())
            .await
            .expect("the aborted task releases the drain");
        assert!(handle.tasks.is_empty());
    }
}
