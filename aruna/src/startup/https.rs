//! Starts native HTTPS for the node: certificate check, sockets, route channels and reload.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use aruna_api::https::certificate::CertificateStore;
use aruna_api::https::listener::{HttpsHandle, RoutedListener};
use aruna_api::https::{HostCheck, HostRoute};
use aruna_core::shutdown::Shutdown;
use rustls::pki_types::UnixTime;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::server::TlsStream;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::settings::HttpsSettings;

/// How often the certificate files are checked for a renewal; `SIGHUP` checks at once.
const RELOAD_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Native HTTPS after its fallible steps: a checked certificate pair, the
/// signal handler, both bound sockets and the route channels.
pub(crate) struct Prepared {
    store: Arc<CertificateStore>,
    listener: TcpListener,
    redirect: Option<TcpListener>,
    hangup: Hangup,
    senders: HashMap<HostRoute, mpsc::Sender<(TlsStream<TcpStream>, std::net::SocketAddr)>>,
    routed: HashMap<HostRoute, RoutedListener>,
}

/// Runs every step that can fail before any service starts, so a bad
/// certificate or a taken port stops startup instead of serving plaintext.
pub(crate) async fn prepare(
    settings: Option<&HttpsSettings>,
) -> Result<Option<Prepared>, Box<dyn std::error::Error>> {
    let Some(settings) = settings else {
        return Ok(None);
    };
    let routes = Arc::new(settings.routes.clone());
    let store = CertificateStore::load(
        settings.certificate.clone(),
        settings.key.clone(),
        settings.ca.clone(),
        routes.clone(),
        UnixTime::now(),
    )?;
    let hangup = Hangup::install()?;
    let listener = TcpListener::bind(settings.address).await?;
    let redirect = match settings.redirect {
        Some(address) => Some(TcpListener::bind(address).await?),
        None => None,
    };
    let mut senders = HashMap::new();
    let mut routed = HashMap::new();
    for route in [HostRoute::Rest, HostRoute::S3, HostRoute::Portal] {
        if routes.contains(route) {
            let (sender, listener) = RoutedListener::channel(HostCheck::new(routes.clone(), route));
            senders.insert(route, sender);
            routed.insert(route, listener);
        }
    }
    Ok(Some(Prepared {
        store: Arc::new(store),
        listener,
        redirect,
        hangup,
        senders,
        routed,
    }))
}

impl Prepared {
    /// The TLS connections of a route, for that service's combined listener.
    pub(crate) fn take(&mut self, route: HostRoute) -> Option<RoutedListener> {
        self.routed.remove(&route)
    }

    /// Starts the front, the redirect and the reload task under one handle.
    pub(crate) fn start(self, shutdown: &Shutdown) -> Result<HttpsHandle, rustls::Error> {
        let token = shutdown.token();
        let routes = self.store.routes().clone();
        let mut handle = HttpsHandle::spawn(
            self.listener,
            self.store.clone(),
            self.senders,
            token.clone(),
        )?;
        if let Some(redirect) = self.redirect {
            let token = token.clone();
            handle.attach(tokio::spawn(async move {
                if let Err(error) = aruna_api::https::redirect::serve(redirect, routes, token).await
                {
                    error!(error = %error, "HTTPS redirect listener stopped");
                }
            }));
        }
        handle.attach(tokio::spawn(reload_loop(self.store, self.hangup, token)));
        Ok(handle)
    }
}

/// Checks the files every interval and on `SIGHUP`. A refused pair keeps the
/// active one; the store logs each outcome.
async fn reload_loop(
    store: Arc<CertificateStore>,
    mut hangup: Hangup,
    shutdown: CancellationToken,
) {
    let mut interval = tokio::time::interval(RELOAD_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = interval.tick() => {}
            () = hangup.received() => info!("Checking the certificate files after SIGHUP"),
        }
        let store = store.clone();
        let check = tokio::task::spawn_blocking(move || store.reload(UnixTime::now()));
        if let Err(error) = check.await {
            error!(error = %error, "The certificate check failed to run");
        }
    }
}

/// `SIGHUP` on Unix. Other platforms rely on the interval check alone.
struct Hangup {
    #[cfg(unix)]
    signal: tokio::signal::unix::Signal,
}

impl Hangup {
    /// Installed in the fallible startup phase, and only with native HTTPS.
    fn install() -> std::io::Result<Self> {
        #[cfg(unix)]
        {
            let kind = tokio::signal::unix::SignalKind::hangup();
            tokio::signal::unix::signal(kind).map(|signal| Self { signal })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    async fn received(&mut self) {
        #[cfg(unix)]
        if self.signal.recv().await.is_some() {
            return;
        }
        std::future::pending::<()>().await;
    }
}
