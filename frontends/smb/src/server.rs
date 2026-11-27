//! The SMB server: TCP accept with per-peer slots, one task per connection with framing,
//! decryption, compound and async request handling, and a drainable server handle.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use aruna_admission::RateLimits;
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, info, warn};

use crate::backend::{Revocation, SmbBackend};
use crate::crypto::{self, unseal};
use crate::dispatch::{Context, Element, Reply};
use crate::ntlm::MicPolicy;
use crate::session::{ConnectionState, Session, Stage, Waiting};
use crate::status;
use crate::wire::{
    self, Command, FLAG_ASYNC, FLAG_RELATED, FLAG_RESPONSE, HEADER_SIZE, Header, LEGACY_ID,
    TRANSFORM_ID, TRANSPORT_LIMIT, TransformHeader,
};

/// Responses queued for the socket writer of one connection.
const OUTGOING_DEPTH: usize = 64;
/// Slack above the largest READ, WRITE or transaction for headers and the transform.
const FRAME_SLACK: usize = 64 * 1024;
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// Server settings. Sizes are the limits advertised in NEGOTIATE.
#[derive(Clone, Debug)]
pub struct SmbConfig {
    /// NetBIOS computer name reported in the NTLM challenge.
    pub computer: String,
    pub domain: String,
    pub mic_policy: MicPolicy,
    pub max_transact: u32,
    pub max_read: u32,
    pub max_write: u32,
    /// Most credits outstanding on one connection.
    pub credit_limit: u16,
    /// How long a request may run before the client gets an interim STATUS_PENDING.
    pub interim_delay: Duration,
}

impl Default for SmbConfig {
    fn default() -> Self {
        Self {
            computer: "ARUNA".to_owned(),
            domain: "ARUNA".to_owned(),
            mic_policy: MicPolicy::Require,
            max_transact: 8 * 1024 * 1024,
            max_read: 8 * 1024 * 1024,
            max_write: 8 * 1024 * 1024,
            credit_limit: 512,
            interim_delay: Duration::from_millis(250),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SmbServerError {
    #[error("SMB listener error: {0}")]
    Io(#[from] std::io::Error),
    #[error("the operating system random source failed")]
    Random,
}

/// A protocol violation or transport failure that ends the connection.
#[derive(Debug)]
pub(crate) struct Fatal(pub &'static str);

pub(crate) fn random_bytes<const N: usize>() -> Result<[u8; N], Fatal> {
    let mut out = [0u8; N];
    getrandom::fill(&mut out).map_err(|_| Fatal("random source failed"))?;
    Ok(out)
}

/// Settings shared by every connection of one server.
pub(crate) struct Shared {
    pub config: SmbConfig,
    pub limits: Arc<RateLimits>,
    pub server_guid: [u8; 16],
}

impl Shared {
    pub fn new(config: SmbConfig, limits: Arc<RateLimits>) -> Result<Self, SmbServerError> {
        let server_guid = random_bytes().map_err(|_| SmbServerError::Random)?;
        Ok(Self {
            config,
            limits,
            server_guid,
        })
    }

    fn frame_limit(&self) -> usize {
        let largest = self
            .config
            .max_write
            .max(self.config.max_read)
            .max(self.config.max_transact) as usize;
        largest.saturating_add(FRAME_SLACK).min(TRANSPORT_LIMIT)
    }
}

/// One client connection and everything it owns.
pub(crate) struct Connection<B> {
    pub backend: Arc<B>,
    pub shared: Arc<Shared>,
    pub peer: IpAddr,
    pub state: Mutex<ConnectionState>,
    outgoing: mpsc::Sender<Vec<u8>>,
    tasks: TaskTracker,
    closing: CancellationToken,
    abort: CancellationToken,
}

/// An embedded SMB 3.1.1 server over one backend.
pub struct SmbServer<B> {
    backend: Arc<B>,
    shared: Arc<Shared>,
}

impl<B: SmbBackend> SmbServer<B> {
    pub fn new(
        backend: Arc<B>,
        limits: Arc<RateLimits>,
        config: SmbConfig,
    ) -> Result<Self, SmbServerError> {
        Ok(Self {
            backend,
            shared: Arc::new(Shared::new(config, limits)?),
        })
    }

    /// Accepts until `shutdown` is cancelled. Connection tasks are tracked by the returned
    /// handle so graceful and forced shutdown can await their release.
    pub fn run_with_listener(
        self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> Result<(SocketAddr, SmbServerHandle), SmbServerError> {
        let local_addr = listener.local_addr()?;
        let connections = TaskTracker::new();
        let tracker = connections.clone();
        let abort_connections = CancellationToken::new();
        let abort = abort_connections.clone();
        let backend = self.backend;
        let shared = self.shared;
        let server = async move {
            loop {
                let (socket, peer) = tokio::select! {
                    _ = shutdown.cancelled() => break,
                    accepted = listener.accept() => match accepted {
                        Ok(pair) => pair,
                        Err(error) => {
                            warn!(%error, "SMB accept failed");
                            tokio::time::sleep(ACCEPT_BACKOFF).await;
                            continue;
                        }
                    },
                };
                let Some(slot) = shared.limits.try_acquire_peer(peer.ip()) else {
                    debug!(%peer, "SMB peer at its connection limit");
                    drop(socket);
                    continue;
                };
                if let Err(error) = socket.set_nodelay(true) {
                    debug!(%error, "SMB socket option failed");
                }
                let backend = backend.clone();
                let shared = shared.clone();
                let shutdown = shutdown.clone();
                let abort = abort.clone();
                tracker.spawn(async move {
                    let _slot = slot;
                    serve_stream(backend, shared, socket, peer.ip(), shutdown, abort).await;
                });
            }
        };
        let task = tokio::spawn(server);
        info!("SMB server is running at {local_addr}");
        Ok((
            local_addr,
            SmbServerHandle {
                task: Some(task),
                connections,
                abort_connections,
            },
        ))
    }

    pub async fn run(
        self,
        address: &str,
        shutdown: CancellationToken,
    ) -> Result<SmbServerHandle, SmbServerError> {
        let listener = TcpListener::bind(address).await?;
        let (_, handle) = self.run_with_listener(listener, shutdown)?;
        Ok(handle)
    }
}

/// Completion boundary for one bound SMB server, like the S3 server handle: the accept
/// task ends with the accept loop while connection children stay owned here.
pub struct SmbServerHandle {
    /// `None` once `exit` consumed the accept task's result.
    task: Option<JoinHandle<()>>,
    connections: TaskTracker,
    abort_connections: CancellationToken,
}

impl SmbServerHandle {
    pub fn is_finished(&self) -> bool {
        self.task.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// The accepted connections whose tasks still run.
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    /// Stops accepting and asks every connection to close; does not wait.
    pub fn abort(&self) {
        self.abort_connections.cancel();
        if let Some(task) = self.task.as_ref() {
            task.abort();
        }
    }

    /// Resolves when the accept loop exits, for supervision selects. The accept result is
    /// consumed once; a later call pends while the handle keeps owning connections.
    pub async fn exit(&mut self, label: &str) -> String {
        let Some(task) = self.task.as_mut() else {
            return std::future::pending().await;
        };
        let outcome = match (&mut *task).await {
            Ok(()) => "stopped unexpectedly".to_owned(),
            Err(error) if error.is_cancelled() => "aborted".to_owned(),
            Err(error) => format!("panicked: {error}"),
        };
        self.task = None;
        format!("{label} server {outcome}")
    }

    /// Waits for the accept loop and every connection task to release while the handle
    /// stays owned. Dropping this future leaves the owner able to abort and wait again.
    pub async fn wait_until_released(&mut self) {
        if let Some(task) = self.task.as_mut() {
            let _ = (&mut *task).await;
        }
        self.task = None;
        self.connections.close();
        let in_flight = self.connections.len();
        if in_flight > 0 {
            info!(in_flight, "Draining in-flight SMB connections");
        }
        self.connections.wait().await;
    }

    pub async fn wait(mut self) {
        self.wait_until_released().await;
    }
}

async fn next_revocation(
    receiver: &mut Option<broadcast::Receiver<Revocation>>,
) -> Option<Revocation> {
    let Some(inner) = receiver.as_mut() else {
        return std::future::pending().await;
    };
    match inner.recv().await {
        Ok(revocation) => Some(revocation),
        Err(RecvError::Lagged(_)) => None,
        Err(RecvError::Closed) => {
            *receiver = None;
            std::future::pending().await
        }
    }
}

/// Serves one connection until the peer leaves, a protocol violation, `shutdown` (finish
/// in-flight work, then close every open) or `abort` (stop at once).
pub(crate) async fn serve_stream<B, S>(
    backend: Arc<B>,
    shared: Arc<Shared>,
    stream: S,
    peer: IpAddr,
    shutdown: CancellationToken,
    abort: CancellationToken,
) where
    B: SmbBackend,
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (outgoing, mut queue) = mpsc::channel::<Vec<u8>>(OUTGOING_DEPTH);
    let writer_task = tokio::spawn(async move {
        while let Some(framed) = queue.recv().await {
            if wire::write_frame(&mut writer, &framed).await.is_err() {
                break;
            }
        }
        let _ = writer.shutdown().await;
    });
    let limit = shared.frame_limit();
    let (frame_sender, mut frames) = mpsc::channel::<Vec<u8>>(1);
    let reader_task = tokio::spawn(async move {
        loop {
            match wire::read_frame(&mut reader, limit).await {
                Ok(Some(message)) => {
                    if frame_sender.send(message).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    debug!(%error, "SMB frame refused");
                    break;
                }
            }
        }
    });
    let credit_limit = shared.config.credit_limit;
    let connection = Arc::new(Connection {
        backend: backend.clone(),
        shared,
        peer,
        state: Mutex::new(ConnectionState::new(credit_limit)),
        outgoing,
        tasks: TaskTracker::new(),
        closing: CancellationToken::new(),
        abort: abort.clone(),
    });
    let mut revocations = Some(backend.revocations());
    loop {
        tokio::select! {
            biased;
            _ = abort.cancelled() => break,
            _ = shutdown.cancelled() => break,
            _ = connection.closing.cancelled() => break,
            revoked = next_revocation(&mut revocations) => connection.revoked(revoked).await,
            frame = frames.recv() => {
                let Some(message) = frame else {
                    break;
                };
                let result = tokio::select! {
                    biased;
                    _ = abort.cancelled() => break,
                    result = connection.receive(message) => result,
                };
                if let Err(Fatal(reason)) = result {
                    debug!(reason, %peer, "closing SMB connection");
                    break;
                }
            }
        }
    }
    reader_task.abort();
    for waiting in connection.state.lock().waiting.values() {
        if waiting.lock {
            waiting.token.cancel();
        }
    }
    connection.tasks.close();
    tokio::select! {
        _ = connection.tasks.wait() => {}
        _ = abort.cancelled() => {}
    }
    if !abort.is_cancelled() {
        let opens = connection.state.lock().take_opens(|_| true);
        connection.close_all(opens).await;
    }
    drop(connection);
    if abort.is_cancelled() {
        writer_task.abort();
    }
    let _ = writer_task.await;
}

/// Commands that may outlive the interim delay and then answer asynchronously.
fn may_wait(command: u16) -> bool {
    matches!(
        Command::from_code(command),
        Some(Command::Read | Command::Write | Command::Lock | Command::Flush)
    )
}

impl<B: SmbBackend> Connection<B> {
    async fn receive(self: &Arc<Self>, message: Vec<u8>) -> Result<(), Fatal> {
        if message.starts_with(&LEGACY_ID) {
            return Err(Fatal("SMB1 negotiation is refused"));
        }
        let (plain, sealed) = if message.starts_with(&TRANSFORM_ID) {
            let transform =
                TransformHeader::decode(&message).map_err(|_| Fatal("transform header"))?;
            let (cipher, key) = {
                let state = self.state.lock();
                let cipher = state.cipher().ok_or(Fatal("encrypted before NEGOTIATE"))?;
                match state
                    .sessions
                    .get(&transform.session_id)
                    .map(|session| &session.stage)
                {
                    Some(Stage::Ready(ready)) => (cipher, ready.keys.decryption.clone()),
                    _ => return Err(Fatal("encrypted message for an unknown session")),
                }
            };
            let plain = unseal(cipher, &key, &message).map_err(|_| Fatal("decryption failed"))?;
            (plain, Some(transform.session_id))
        } else {
            (message, None)
        };
        let elements = wire::split_compound(&plain)
            .map_err(|_| Fatal("malformed compound"))?
            .into_iter()
            .map(|bytes| {
                Header::decode(bytes).map(|header| Element {
                    header,
                    bytes: bytes.to_vec(),
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| Fatal("malformed header"))?;
        {
            let mut state = self.state.lock();
            for element in &elements {
                if element.header.flags & FLAG_RESPONSE != 0 {
                    return Err(Fatal("response sent to the server"));
                }
                if element.header.command == Command::Cancel.code() {
                    continue;
                }
                if !state
                    .credits
                    .consume(element.header.message_id, element.header.credit_charge)
                {
                    return Err(Fatal("MessageId outside the credit window"));
                }
            }
        }
        match sealed {
            None => self.receive_plain(elements).await,
            Some(session_id) => self.receive_sealed(session_id, elements).await,
        }
    }

    /// Before encryption only NEGOTIATE, SESSION_SETUP, CANCEL and ECHO without a session
    /// are served; plaintext on an authenticated session closes the connection.
    async fn receive_plain(self: &Arc<Self>, mut elements: Vec<Element>) -> Result<(), Fatal> {
        if elements.len() != 1 {
            return Err(Fatal("unencrypted compound request"));
        }
        let element = elements.remove(0);
        let header = element.header.clone();
        let context = Context::of(&header);
        let command = Command::from_code(header.command);
        let negotiated = self.state.lock().negotiated.is_some();
        if !negotiated && command != Some(Command::Negotiate) {
            return Err(Fatal("request before NEGOTIATE"));
        }
        match command {
            Some(Command::Negotiate) => {
                let reply = self.negotiate(&element)?;
                let accepted = reply.status == status::SUCCESS;
                let message = self.build(&header, &reply, &context);
                if accepted {
                    self.state.lock().preauth.update(&message);
                }
                self.transmit(message, None).await?;
                if accepted {
                    Ok(())
                } else {
                    Err(Fatal("no acceptable dialect, hash or cipher"))
                }
            }
            Some(Command::SessionSetup) => {
                let reply = self.session_setup(&element).await?;
                let session_id = reply.session_id.unwrap_or(header.session_id);
                let mut message = self.build(&header, &reply, &context);
                if reply.status == status::MORE_PROCESSING {
                    let mut state = self.state.lock();
                    if let Some(Session {
                        stage: Stage::Handshake(handshake),
                        ..
                    }) = state.sessions.get_mut(&session_id)
                    {
                        handshake.preauth.update(&message);
                    }
                }
                if reply.signed {
                    self.sign(session_id, &mut message)?;
                }
                self.transmit(message, None).await
            }
            // Plaintext never reaches the requests of an encrypted session; CANCEL has no reply.
            Some(Command::Cancel) => {
                if !self.is_ready(header.session_id) {
                    self.cancel(&element, header.session_id);
                }
                Ok(())
            }
            Some(Command::Echo) if header.session_id == 0 => {
                let reply = self.execute(&element, context).await;
                let message = self.build(&header, &reply, &context);
                self.transmit(message, None).await
            }
            _ => {
                let ready = self.is_ready(header.session_id);
                let code = if ready {
                    status::ACCESS_DENIED
                } else {
                    status::USER_SESSION_DELETED
                };
                let message = self.build(&header, &Reply::fail(code), &context);
                self.transmit(message, None).await?;
                if ready {
                    Err(Fatal("unencrypted request on an encrypted session"))
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn receive_sealed(
        self: &Arc<Self>,
        session_id: u64,
        elements: Vec<Element>,
    ) -> Result<(), Fatal> {
        let mut work = Vec::with_capacity(elements.len());
        for element in elements {
            if !element.header.is_related() && element.header.session_id != session_id {
                return Err(Fatal("session differs from the transform"));
            }
            match Command::from_code(element.header.command) {
                Some(Command::Negotiate) => return Err(Fatal("NEGOTIATE after authentication")),
                Some(Command::Cancel) => self.cancel(&element, session_id),
                _ => work.push(element),
            }
        }
        if work.len() == 1 && may_wait(work[0].header.command) {
            self.spawn_waiting(work.remove(0), session_id);
            return Ok(());
        }
        if work.is_empty() {
            return Ok(());
        }
        let lock = Command::Lock.code();
        if work.iter().any(|element| element.header.command == lock) {
            self.spawn_compound(work, session_id);
            return Ok(());
        }
        self.run_compound(session_id, work).await
    }

    /// A compound with a LOCK runs on its own task, so the connection still reads CANCEL and
    /// the end of the stream while the lock waits; each LOCK is cancellable by its MessageId.
    fn spawn_compound(self: &Arc<Self>, elements: Vec<Element>, session_id: u64) {
        let locks: Vec<u64> = elements
            .iter()
            .filter(|element| element.header.command == Command::Lock.code())
            .map(|element| element.header.message_id)
            .collect();
        let mut state = self.state.lock();
        for message_id in &locks {
            let waiting = Waiting {
                session_id,
                async_id: None,
                token: CancellationToken::new(),
                lock: true,
            };
            state.waiting.insert(*message_id, waiting);
        }
        drop(state);
        let connection = Arc::clone(self);
        self.tasks.spawn(async move {
            let abort = connection.abort.clone();
            let result = tokio::select! {
                biased;
                _ = abort.cancelled() => Ok(()),
                result = connection.run_compound(session_id, elements) => result,
            };
            let mut state = connection.state.lock();
            for message_id in &locks {
                state.waiting.remove(message_id);
            }
            drop(state);
            if let Err(Fatal(reason)) = result {
                debug!(reason, "closing SMB connection");
                connection.closing.cancel();
            }
        });
    }

    /// Runs compound elements in order, propagating SessionId, TreeId and the created
    /// FileId to related elements, and answers with one sealed compound.
    async fn run_compound(
        self: &Arc<Self>,
        session_id: u64,
        elements: Vec<Element>,
    ) -> Result<(), Fatal> {
        let mut chain = Context::default();
        let mut messages = Vec::with_capacity(elements.len());
        let mut ends_session = false;
        for (index, element) in elements.iter().enumerate() {
            let related = element.header.is_related();
            let reply = if related && index == 0 {
                Reply::fail(status::INVALID_PARAMETER)
            } else {
                if !related {
                    chain = Context::of(&element.header);
                }
                let token = {
                    let state = self.state.lock();
                    let waiting = state.waiting.get(&element.header.message_id);
                    waiting.map(|waiting| waiting.token.clone())
                };
                match token {
                    Some(token) => tokio::select! {
                        biased;
                        reply = self.execute(element, chain) => reply,
                        _ = token.cancelled() => Reply::fail(status::CANCELLED),
                    },
                    None => self.execute(element, chain).await,
                }
            };
            if reply.terminate {
                return Err(Fatal("client view of the negotiation differs"));
            }
            if let Some(tree_id) = reply.tree_id {
                chain.tree_id = tree_id;
            }
            if let Some(file_id) = reply.file_id {
                chain.related_file = Some(file_id);
                chain.related_failure = None;
            } else if element.header.command == Command::Create.code()
                && status::is_error(reply.status)
            {
                chain.related_file = None;
                chain.related_failure = Some(reply.status);
            }
            ends_session |= reply.ends_session;
            messages.push(self.build(&element.header, &reply, &chain));
        }
        self.transmit(wire::join_compound(messages), Some(session_id))
            .await?;
        if ends_session {
            let opens = self.state.lock().remove_session(session_id);
            self.close_all(opens).await;
        }
        Ok(())
    }

    fn spawn_waiting(self: &Arc<Self>, element: Element, session_id: u64) {
        let token = CancellationToken::new();
        self.state.lock().waiting.insert(
            element.header.message_id,
            Waiting {
                session_id,
                async_id: None,
                token: token.clone(),
                lock: element.header.command == Command::Lock.code(),
            },
        );
        let connection = Arc::clone(self);
        self.tasks.spawn(async move {
            let abort = connection.abort.clone();
            tokio::select! {
                biased;
                _ = abort.cancelled() => {}
                _ = connection.run_waiting(element, session_id, token) => {}
            }
        });
    }

    /// Answers at once when the work finishes within the interim delay; otherwise sends an
    /// interim STATUS_PENDING with an AsyncId and always follows it with a final response.
    async fn run_waiting(
        self: &Arc<Self>,
        element: Element,
        session_id: u64,
        token: CancellationToken,
    ) {
        let context = Context::of(&element.header);
        let message_id = element.header.message_id;
        let work = self.execute(&element, context);
        tokio::pin!(work);
        let delay = tokio::time::sleep(self.shared.config.interim_delay);
        tokio::pin!(delay);
        let early = tokio::select! {
            biased;
            reply = &mut work => Some(reply),
            _ = token.cancelled() => Some(Reply::fail(status::CANCELLED)),
            _ = &mut delay => None,
        };
        let message = match early {
            Some(reply) => self.build(&element.header, &reply, &context),
            None => {
                let async_id = self.interim(&element.header, session_id).await;
                let Some(async_id) = async_id else {
                    self.state.lock().waiting.remove(&message_id);
                    return;
                };
                let reply = tokio::select! {
                    biased;
                    reply = &mut work => reply,
                    _ = token.cancelled() => {
                        self.notify_cancel(&element, context).await;
                        Reply::fail(status::CANCELLED)
                    }
                };
                self.compose(&element.header, &reply, &context, Some(async_id), 0)
            }
        };
        self.state.lock().waiting.remove(&message_id);
        if let Err(Fatal(reason)) = self.transmit(message, Some(session_id)).await {
            debug!(reason, "SMB final response not sent");
            self.closing.cancel();
        }
    }

    /// Sends the interim response and returns its AsyncId, or `None` when the connection
    /// can no longer send.
    async fn interim(&self, request: &Header, session_id: u64) -> Option<u64> {
        let (async_id, credits) = {
            let mut state = self.state.lock();
            let async_id = state.next_async;
            state.next_async += 1;
            if let Some(waiting) = state.waiting.get_mut(&request.message_id) {
                waiting.async_id = Some(async_id);
            }
            (async_id, state.credits.grant(request.credits))
        };
        let pending = Reply::fail(status::PENDING);
        let message = self.compose(
            request,
            &pending,
            &Context::of(request),
            Some(async_id),
            credits,
        );
        match self.transmit(message, Some(session_id)).await {
            Ok(()) => Some(async_id),
            Err(Fatal(reason)) => {
                debug!(reason, "SMB interim response not sent");
                self.closing.cancel();
                None
            }
        }
    }

    /// Whether `session_id` names an authenticated session, which takes encrypted requests only.
    fn is_ready(&self, session_id: u64) -> bool {
        matches!(
            self.state
                .lock()
                .sessions
                .get(&session_id)
                .map(|session| &session.stage),
            Some(Stage::Ready(_))
        )
    }

    /// Cancels the waiting request a CANCEL names by AsyncId or MessageId in its session.
    fn cancel(&self, element: &Element, session_id: u64) {
        let state = self.state.lock();
        let header = &element.header;
        let target = if header.flags & FLAG_ASYNC != 0 {
            state.waiting.values().find(|waiting| {
                waiting.async_id == Some(header.async_id) && waiting.session_id == session_id
            })
        } else {
            state
                .waiting
                .get(&header.message_id)
                .filter(|waiting| waiting.session_id == session_id)
        };
        if let Some(waiting) = target {
            waiting.token.cancel();
        }
    }

    async fn revoked(&self, revocation: Option<Revocation>) {
        match revocation {
            Some(revocation) => {
                let opens = self.state.lock().expire_key(&revocation.access_key);
                self.close_all(opens).await;
            }
            None => {
                let sessions: Vec<_> = {
                    let state = self.state.lock();
                    state
                        .sessions
                        .iter()
                        .filter_map(|(session_id, session)| match &session.stage {
                            Stage::Ready(ready) if !ready.expired => {
                                Some((*session_id, ready.credential.clone()))
                            }
                            _ => None,
                        })
                        .collect()
                };
                for (session_id, credential) in sessions {
                    if self.backend.admit(&credential).await.is_err() {
                        let opens = self.state.lock().expire_session(session_id);
                        self.close_all(opens).await;
                    }
                }
            }
        }
    }

    fn sign(&self, session_id: u64, message: &mut [u8]) -> Result<(), Fatal> {
        let state = self.state.lock();
        let algorithm = state
            .negotiated
            .as_ref()
            .map(|negotiated| negotiated.signing)
            .ok_or(Fatal("signing before NEGOTIATE"))?;
        match state
            .sessions
            .get(&session_id)
            .map(|session| &session.stage)
        {
            Some(Stage::Ready(ready)) => crypto::sign(algorithm, &ready.keys.signing, message)
                .map_err(|_| Fatal("signing failed")),
            _ => Err(Fatal("no session key to sign with")),
        }
    }

    /// The response message for a request, granting credits from the window.
    fn build(&self, request: &Header, reply: &Reply, context: &Context) -> Vec<u8> {
        let credits = self.state.lock().credits.grant(request.credits);
        self.compose(request, reply, context, None, credits)
    }

    fn compose(
        &self,
        request: &Header,
        reply: &Reply,
        context: &Context,
        async_id: Option<u64>,
        credits: u16,
    ) -> Vec<u8> {
        let mut flags = FLAG_RESPONSE | (request.flags & FLAG_RELATED);
        if async_id.is_some() {
            flags |= FLAG_ASYNC;
        }
        let header = Header {
            credit_charge: request.credit_charge,
            status: reply.status,
            command: request.command,
            credits,
            flags,
            next_command: 0,
            message_id: request.message_id,
            async_id: async_id.unwrap_or(0),
            tree_id: reply.tree_id.unwrap_or(context.tree_id),
            session_id: reply.session_id.unwrap_or(context.session_id),
            signature: [0u8; 16],
        };
        let mut message = Vec::with_capacity(HEADER_SIZE + reply.body.len());
        header.encode(&mut message);
        message.extend_from_slice(&reply.body);
        message
    }

    /// Seals the message for `seal` (a session id) when given, frames it and queues it.
    /// Running out of nonces ends the connection before any nonce could repeat.
    async fn transmit(&self, message: Vec<u8>, seal: Option<u64>) -> Result<(), Fatal> {
        let packet = match seal {
            None => message,
            Some(session_id) => {
                let sealed = {
                    let mut state = self.state.lock();
                    match state
                        .sessions
                        .get_mut(&session_id)
                        .map(|session| &mut session.stage)
                    {
                        Some(Stage::Ready(ready)) => ready.sealer.seal(session_id, &message),
                        _ => return Err(Fatal("no keys to seal a response")),
                    }
                };
                sealed.map_err(|_| Fatal("session nonce space exhausted"))?
            }
        };
        let framed = wire::frame(&packet).map_err(|_| Fatal("response too large"))?;
        self.outgoing
            .send(framed)
            .await
            .map_err(|_| Fatal("connection writer closed"))
    }
}

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;
