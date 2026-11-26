//! The NFS listener: per-peer handshake and connection limits, the RFC 9289 probe and TLS
//! upgrade, host admission, the RPC loop, the lease sweeper and revocation, with a drain handle.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use aruna_admission::RateLimits;
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, info, warn};

use crate::backend::{HostIdentity, NfsBackend};
use crate::compound::{CompoundOutcome, ConnectionInfo, Engine, EngineConfig};
use crate::filehandle::{FilehandleKey, KeyError};
use crate::record::{read_record, write_record};
use crate::rpc::{
    AcceptStat, AuthStat, COMPOUND_PROCEDURE, CallFault, DisabledGssAcceptor, GssAcceptor,
    NFS_PROGRAM, NFS_VERSION, NULL_PROCEDURE, RpcCall, RpcCred, RpcPrincipal, accepted_reply,
    denied_reply, flavor, mismatch_reply, parse_call, starttls_reply, status_reply, success_reply,
};
use crate::session::ServerLimits;
use crate::tls::{
    ConnectionRegistry, NfsTlsConfig, SUNRPC_ALPN, TlsSetupError, fingerprint, server_config,
};
use crate::types::IO_LIMIT;

/// The cleartext probe is one small NULL call.
const PROBE_LIMIT: usize = 4096;
/// Request headroom beyond the largest WRITE payload.
const REQUEST_HEADROOM: u32 = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum NfsServerError {
    #[error(transparent)]
    Tls(#[from] TlsSetupError),
    #[error(transparent)]
    Key(#[from] KeyError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Operator settings of one NFS server.
#[derive(Clone)]
pub struct NfsServerConfig {
    pub address: SocketAddr,
    pub tls: NfsTlsConfig,
    /// The durable node secret; filehandle and session keys are derived from it.
    pub node_secret: [u8; 32],
    pub lease_secs: u32,
    /// Largest RPC record accepted before any of it is buffered.
    pub max_record_bytes: usize,
    /// Bound for the probe, the TLS handshake and host admission together.
    pub handshake_deadline: Duration,
}

impl NfsServerConfig {
    pub fn new(address: SocketAddr, tls: NfsTlsConfig, node_secret: [u8; 32]) -> Self {
        Self {
            address,
            tls,
            node_secret,
            lease_secs: 90,
            max_record_bytes: (IO_LIMIT + REQUEST_HEADROOM) as usize,
            handshake_deadline: Duration::from_secs(10),
        }
    }
}

impl std::fmt::Debug for NfsServerConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NfsServerConfig")
            .field("address", &self.address)
            .field("tls", &self.tls)
            .field("lease_secs", &self.lease_secs)
            .field("max_record_bytes", &self.max_record_bytes)
            .finish_non_exhaustive()
    }
}

/// What every connection task of one server shares.
struct Shared<B> {
    engine: Engine<B>,
    acceptor: Option<TlsAcceptor>,
    limits: Arc<RateLimits>,
    registry: ConnectionRegistry,
    gss: Arc<dyn GssAcceptor>,
    loopback_cleartext: bool,
    max_record: usize,
    deadline: Duration,
}

pub struct NfsServer<B> {
    config: NfsServerConfig,
    backend: Arc<B>,
    limits: Arc<RateLimits>,
    gss: Arc<dyn GssAcceptor>,
}

impl<B: NfsBackend> NfsServer<B> {
    pub fn new(config: NfsServerConfig, backend: Arc<B>, limits: Arc<RateLimits>) -> Self {
        Self {
            config,
            backend,
            limits,
            gss: Arc::new(DisabledGssAcceptor),
        }
    }

    /// Installs an RPCSEC_GSS acceptor; without one every GSS credential is refused.
    pub fn with_gss(mut self, gss: Arc<dyn GssAcceptor>) -> Self {
        self.gss = gss;
        self
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<NfsServerHandle, NfsServerError> {
        let listener = TcpListener::bind(self.config.address).await?;
        self.run_with_listener(listener, shutdown)
    }

    /// Accepts on `listener` until `shutdown`. Fails only when the TLS or key setup fails,
    /// before any task starts.
    pub fn run_with_listener(
        self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> Result<NfsServerHandle, NfsServerError> {
        let acceptor = if self.config.tls.loopback_cleartext
            && self.config.tls.cert_pem.is_empty()
            && self.config.tls.key_pem.is_empty()
        {
            None
        } else {
            Some(TlsAcceptor::from(server_config(&self.config.tls)?))
        };
        let key = FilehandleKey::derive(&self.config.node_secret)?;
        let boot = boot_tag();
        let session_key = blake3::derive_key("aruna-nfs-session-v1", &self.config.node_secret);
        let owner = blake3::derive_key("aruna-nfs-server-owner-v1", &self.config.node_secret);
        let max_record = self.config.max_record_bytes;
        let record_limit = u32::try_from(max_record).unwrap_or(u32::MAX);
        let engine = Engine::new(
            self.backend.clone(),
            key,
            EngineConfig {
                lease: Duration::from_secs(u64::from(self.config.lease_secs.max(1))),
                limits: ServerLimits {
                    max_request: record_limit,
                    max_response: record_limit,
                },
                boot,
                session_key: blake3::keyed_hash(&session_key, &boot.to_be_bytes()).into(),
                server_owner: Bytes::copy_from_slice(&owner[..16]),
            },
        );
        let shared = Arc::new(Shared {
            engine,
            acceptor,
            limits: self.limits,
            registry: ConnectionRegistry::default(),
            gss: self.gss,
            loopback_cleartext: self.config.tls.loopback_cleartext,
            max_record,
            deadline: self.config.handshake_deadline,
        });
        let connections = TaskTracker::new();
        let abort_connections = CancellationToken::new();
        let local = listener.local_addr().ok();
        if let Some(address) = local {
            info!(address = %address, "NFS listener running");
        }
        let task = tokio::spawn(run_server(
            listener,
            shared.clone(),
            connections.clone(),
            shutdown,
            abort_connections.clone(),
        ));
        Ok(NfsServerHandle {
            task: Some(task),
            connections,
            abort_connections,
            local,
        })
    }
}

/// A boot tag that differs between server instances, so earlier ids are unknown afterwards.
fn boot_tag() -> u32 {
    let ulid = ulid::Ulid::generate();
    (ulid.0 >> 96) as u32 ^ ulid.0 as u32
}

/// Completion boundary for one NFS server, as for the S3 server: the accept task ends with
/// the accept loop while connection tasks stay owned here until they release.
pub struct NfsServerHandle {
    task: Option<JoinHandle<()>>,
    connections: TaskTracker,
    abort_connections: CancellationToken,
    local: Option<SocketAddr>,
}

impl NfsServerHandle {
    pub fn is_finished(&self) -> bool {
        self.task.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// The accepted connections whose tasks still run.
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local
    }

    /// Stops accepting and closes every connection; does not wait.
    pub fn abort(&self) {
        self.abort_connections.cancel();
        if let Some(task) = self.task.as_ref() {
            task.abort();
        }
    }

    /// Resolves when the accept task exits, for supervision selects. Its result is consumed
    /// once; a later call pends while the handle keeps owning the connection tasks.
    pub async fn exit(&mut self, label: &str) -> String {
        let Some(task) = self.task.as_mut() else {
            return std::future::pending().await;
        };
        let outcome = match (&mut *task).await {
            Ok(()) => "stopped unexpectedly".to_string(),
            Err(error) if error.is_cancelled() => "aborted".to_string(),
            Err(error) => format!("panicked: {error}"),
        };
        self.task = None;
        format!("{label} server {outcome}")
    }

    /// Waits for the accept task and every connection task while the handle stays owned.
    /// Dropping this future at a deadline leaves the owner able to abort and wait again.
    pub async fn wait_until_released(&mut self) {
        if let Some(task) = self.task.as_mut() {
            let _ = (&mut *task).await;
        }
        self.task = None;
        self.connections.close();
        let in_flight = self.connections.len();
        if in_flight > 0 {
            info!(in_flight, "Draining in-flight NFS connections");
        }
        self.connections.wait().await;
    }

    pub async fn wait(mut self) {
        self.wait_until_released().await;
    }
}

/// The accept loop, the lease sweeper and the revocation listener, which all end on shutdown.
async fn run_server<B: NfsBackend>(
    listener: TcpListener,
    shared: Arc<Shared<B>>,
    connections: TaskTracker,
    shutdown: CancellationToken,
    abort: CancellationToken,
) {
    let mut revocations = shared.engine.backend.revocations();
    let period = (shared.engine.lease / 4).max(Duration::from_secs(1));
    let mut sweeper = tokio::time::interval(period);
    sweeper.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut subscribed = true;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((socket, peer)) => {
                    connections.spawn(serve_connection(
                        shared.clone(),
                        socket,
                        peer,
                        shutdown.clone(),
                        abort.clone(),
                    ));
                }
                Err(error) => {
                    // Out of descriptors or memory; pausing keeps a failing accept from spinning.
                    warn!(%error, "NFS accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
            _ = sweeper.tick() => shared.engine.sweep(Instant::now()).await,
            revocation = revocations.recv(), if subscribed => match revocation {
                Ok(revocation) => {
                    if let crate::backend::Revocation::Host(host_id)
                    | crate::backend::Revocation::HostRotated(host_id) = &revocation
                    {
                        shared.registry.close_host(*host_id);
                    }
                    shared.engine.revoke(&revocation).await;
                }
                Err(RecvError::Lagged(missed)) => {
                    warn!(missed, "NFS revocations lagged; each request still checks its binding");
                }
                Err(RecvError::Closed) => subscribed = false,
            },
        }
    }
}

/// The stream a connection serves after admission.
enum Admitted {
    Tls(
        Box<tokio_rustls::server::TlsStream<TcpStream>>,
        HostIdentity,
    ),
    Plain(TcpStream, HostIdentity, Bytes),
}

async fn serve_connection<B: NfsBackend>(
    shared: Arc<Shared<B>>,
    socket: TcpStream,
    peer: SocketAddr,
    shutdown: CancellationToken,
    abort: CancellationToken,
) {
    let ip = peer.ip();
    if shared.limits.check_handshake(ip).is_err() {
        debug!(peer = %peer, "Dropped an NFS connection: handshake rate");
        return;
    }
    let Some(_permit) = shared.limits.try_acquire_peer(ip) else {
        debug!(peer = %peer, "Dropped an NFS connection: too many connections");
        return;
    };
    let admitted = tokio::select! {
        _ = abort.cancelled() => return,
        result = tokio::time::timeout(shared.deadline, admit(&shared, socket, ip)) => match result {
            Ok(Some(admitted)) => admitted,
            Ok(None) | Err(_) => return,
        },
    };
    match admitted {
        Admitted::Tls(stream, host) => {
            serve_rpc(&shared, *stream, host, ip, None, shutdown, abort).await;
        }
        Admitted::Plain(stream, host, first) => {
            serve_rpc(&shared, stream, host, ip, Some(first), shutdown, abort).await;
        }
    }
}

/// Reads the RFC 9289 probe, answers STARTTLS and finishes TLS with `sunrpc` ALPN and a host
/// certificate. A loopback peer may skip TLS only when the operator allows it.
async fn admit<B: NfsBackend>(
    shared: &Shared<B>,
    mut socket: TcpStream,
    ip: IpAddr,
) -> Option<Admitted> {
    let record = read_record(&mut socket, PROBE_LIMIT).await.ok()??;
    let call = match parse_call(record.clone()) {
        Ok(call) => call,
        Err(fault) => {
            if let Some(reply) = fault_reply(fault) {
                let _ = write_record(&mut socket, &reply).await;
            }
            return None;
        }
    };
    if !call.is_probe() {
        if shared.loopback_cleartext && ip.is_loopback() {
            let host = shared.engine.backend.loopback_host(ip).await?;
            return Some(Admitted::Plain(socket, host, record));
        }
        let _ = write_record(&mut socket, &denied_reply(call.xid, AuthStat::TooWeak)).await;
        return None;
    }
    let Some(acceptor) = &shared.acceptor else {
        // Without a certificate (loopback-only cleartext mode), decline STARTTLS per RFC 9289.
        let _ = write_record(&mut socket, &success_reply(call.xid, &[])).await;
        return None;
    };
    write_record(&mut socket, &starttls_reply(call.xid))
        .await
        .ok()?;
    let mut stream = acceptor.accept(socket).await.ok()?;
    let (_, connection) = stream.get_ref();
    let alpn = connection.alpn_protocol() == Some(SUNRPC_ALPN);
    let tls13 = connection.protocol_version() == Some(rustls::ProtocolVersion::TLSv1_3);
    let pin = connection
        .peer_certificates()
        .and_then(|chain| chain.first())
        .and_then(|leaf| fingerprint(leaf));
    let Some(pin) = pin.filter(|_| alpn && tls13) else {
        debug!(
            alpn,
            tls13, "Closed an NFS connection without sunrpc ALPN or a client key"
        );
        let _ = stream.shutdown().await;
        return None;
    };
    let Some(host) = shared.engine.backend.enrolled_host(pin, ip).await else {
        debug!("Closed an NFS connection from a key or network no host enrolled");
        let _ = stream.shutdown().await;
        return None;
    };
    Some(Admitted::Tls(Box::new(stream), host))
}

/// Serves RPC records until the peer closes, the host is revoked or the server stops.
async fn serve_rpc<B, S>(
    shared: &Shared<B>,
    mut stream: S,
    host: HostIdentity,
    ip: IpAddr,
    mut pending: Option<Bytes>,
    shutdown: CancellationToken,
    abort: CancellationToken,
) where
    B: NfsBackend,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (id, revoked) = shared.registry.register(host.host_id);
    let connection = ConnectionInfo { id, host, peer: ip };
    loop {
        let record = match pending.take() {
            Some(record) => record,
            None => {
                let read = tokio::select! {
                    _ = abort.cancelled() => break,
                    _ = revoked.cancelled() => break,
                    _ = shutdown.cancelled() => break,
                    read = read_record(&mut stream, shared.max_record) => read,
                };
                match read {
                    Ok(Some(record)) => record,
                    Ok(None) => break,
                    Err(error) => {
                        debug!(%error, "Closed an NFS connection on a bad record");
                        break;
                    }
                }
            }
        };
        let reply = tokio::select! {
            _ = abort.cancelled() => break,
            _ = revoked.cancelled() => break,
            reply = handle_record(shared, &connection, record) => reply,
        };
        let Some(reply) = reply else {
            break;
        };
        let written = tokio::select! {
            _ = abort.cancelled() => break,
            _ = revoked.cancelled() => break,
            written = write_record(&mut stream, &reply) => written,
        };
        if written.is_err() {
            break;
        }
    }
    shared.registry.release(host.host_id, id);
    shared.engine.disconnect(id);
    let _ = stream.shutdown().await;
}

fn fault_reply(fault: CallFault) -> Option<Bytes> {
    match fault {
        CallFault::Unreadable => None,
        CallFault::Mismatch { xid } => Some(mismatch_reply(xid)),
        CallFault::Auth { xid, stat } => Some(denied_reply(xid, stat)),
    }
}

/// Answers one record; `None` closes the connection.
async fn handle_record<B: NfsBackend>(
    shared: &Shared<B>,
    connection: &ConnectionInfo,
    record: Bytes,
) -> Option<Bytes> {
    let call = match parse_call(record) {
        Ok(call) => call,
        Err(fault) => return fault_reply(fault),
    };
    Some(answer(shared, connection, call).await)
}

async fn answer<B: NfsBackend>(
    shared: &Shared<B>,
    connection: &ConnectionInfo,
    call: RpcCall,
) -> Bytes {
    if call.program != NFS_PROGRAM {
        return status_reply(call.xid, AcceptStat::ProgramUnavailable);
    }
    if call.version != NFS_VERSION {
        return status_reply(call.xid, AcceptStat::ProgramMismatch);
    }
    let (principal, verifier) = match &call.cred {
        RpcCred::Tls => return denied_reply(call.xid, AuthStat::BadCredential),
        RpcCred::None if call.procedure == NULL_PROCEDURE => (RpcPrincipal::Anonymous, None),
        RpcCred::None => return denied_reply(call.xid, AuthStat::TooWeak),
        RpcCred::Sys(sys) => (
            RpcPrincipal::Sys {
                uid: sys.uid,
                gid: sys.gid,
            },
            None,
        ),
        RpcCred::Gss(header) => match shared.gss.accept(header) {
            Ok((principal, verifier)) => (principal, Some(verifier)),
            Err(stat) => return denied_reply(call.xid, stat),
        },
    };
    let reply = |body: &[u8]| match &verifier {
        Some(verifier) => accepted_reply(
            call.xid,
            (flavor::GSS, &verifier[..]),
            AcceptStat::Success,
            body,
        ),
        None => success_reply(call.xid, body),
    };
    match call.procedure {
        NULL_PROCEDURE => reply(&[]),
        COMPOUND_PROCEDURE => {
            let owner = connection.host.owner;
            let work = shared.limits.try_acquire_work(owner);
            let admitted = work.is_some() && shared.limits.check_principal(owner).is_ok();
            let outcome = shared
                .engine
                .compound(connection, principal, call.body, admitted, Instant::now())
                .await;
            drop(work);
            match outcome {
                CompoundOutcome::Reply(body) => reply(&body),
                CompoundOutcome::Denied(stat) => denied_reply(call.xid, stat),
                CompoundOutcome::Garbage => status_reply(call.xid, AcceptStat::GarbageArgs),
            }
        }
        _ => status_reply(call.xid, AcceptStat::ProcedureUnavailable),
    }
}

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;
