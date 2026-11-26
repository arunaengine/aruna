//! The RFC 8881 COMPOUND dispatcher: operation position rules, short-circuit on the first
//! error, slot replay with a fresh grant check, and the session and client operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::time::Instant;
use tracing::debug;
use ulid::Ulid;

use crate::arguments::{self, ExchangeArgs, Operation, SequenceArgs, SessionArgs};
use crate::backend::{
    Caller, ChangeInfo, HostIdentity, LockRange, MountExport, NfsBackend, ObjectRef, ReplayAccess,
    ReplayCheck, ReplayGrant, Revocation,
};
use crate::filehandle::{Filehandle, FilehandleKey};
use crate::rpc::{AuthStat, RpcPrincipal};
use crate::session::{
    CONFIRMED_FLAG, ChannelAttributes, CreateRequest, NON_PNFS_FLAG, RequestShape, Sender,
    ServerLimits, SessionTable, SlotDecision, SlotReply, SlotRequest,
};
use crate::state::{Cleanup, Revoked, StateTable};
use crate::types::{NfsOp4, NfsStat4, OPAQUE_LIMIT, Stateid};
use crate::xdr::{XdrReader, XdrWriter};

/// Idle leases after which a client with no requests is forgotten entirely.
const IDLE_LEASES: u32 = 8;
/// Room kept for the compound header and one status when checking the reply size.
const REPLY_RESERVE: usize = 512;
/// Room a cached compound keeps for the result of a non-read operation before running it.
const OPERATION_RESERVE: usize = 128;
/// An operation code and status, the result of a failed operation without a body.
const STATUS_SIZE: usize = 8;
/// `channel_dir_from_server4` value for the fore channel.
const FORE_CHANNEL: u32 = 1;
/// `channel_dir_from_client4` value asking for the back channel only.
const BACK_ONLY: u32 = 2;

/// Every volatile table behind one lock, which is never held across an await.
pub struct Tables {
    pub sessions: SessionTable,
    pub states: StateTable,
}

/// One established connection: its id, the enrolled host it proved and its peer address.
#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    pub id: u64,
    pub host: HostIdentity,
    pub peer: IpAddr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompoundOutcome {
    /// An encoded COMPOUND4res.
    Reply(Bytes),
    /// A cached reply that may no longer be released: refused at RPC admission.
    Denied(AuthStat),
    /// The compound header itself does not decode.
    Garbage,
}

/// How one operation failed. `keep` retains the result body an error carries, such as the
/// conflicting lock of LOCK or the empty attribute mask of SETATTR.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpError {
    pub status: NfsStat4,
    pub keep: bool,
}

impl From<NfsStat4> for OpError {
    fn from(status: NfsStat4) -> Self {
        Self {
            status,
            keep: false,
        }
    }
}

impl From<crate::xdr::XdrError> for OpError {
    fn from(_: crate::xdr::XdrError) -> Self {
        NfsStat4::BadXdr.into()
    }
}

pub type OpResult = Result<(), OpError>;

/// The current or saved filehandle of a compound.
#[derive(Clone, Debug)]
pub enum Current {
    None,
    Root,
    Mounts,
    Object {
        export: Arc<MountExport>,
        object: ObjectRef,
        /// An expired volatile handle; its FHEXPIRED waits for the first operation using it.
        expired: bool,
    },
}

/// A usable current filehandle.
#[derive(Clone, Debug)]
pub enum Target {
    Root,
    Mounts,
    Object {
        export: Arc<MountExport>,
        object: ObjectRef,
    },
}

/// The slot a compound holds; dropping it unfinished leaves an uncached reply, so a lost
/// connection never blocks the slot or lets the mutation run twice.
struct SlotLease {
    tables: Arc<Mutex<Tables>>,
    session: [u8; 16],
    slot: u32,
    digest: [u8; 32],
    done: bool,
}

impl SlotLease {
    fn finish(mut self, reply: SlotReply) {
        self.tables
            .lock()
            .sessions
            .finish(&self.session, self.slot, reply);
        self.done = true;
    }
}

impl Drop for SlotLease {
    fn drop(&mut self) {
        if !self.done {
            let reply = SlotReply::Uncached {
                digest: self.digest,
            };
            self.tables
                .lock()
                .sessions
                .finish(&self.session, self.slot, reply);
        }
    }
}

struct SessionUse {
    client_id: u64,
    cache: bool,
    max_cached: usize,
    lease: SlotLease,
}

/// What one compound used, for the replay check of its cached reply.
#[derive(Default)]
pub struct GrantTrace {
    pub mounts: Vec<Ulid>,
    pub checks: Vec<ReplayCheck>,
}

impl GrantTrace {
    /// Records one object use that a replay of the reply must be admitted for again.
    pub fn record(&mut self, object: ObjectRef, access: ReplayAccess) {
        let check = ReplayCheck { object, access };
        if !self.checks.contains(&check) {
            self.checks.push(check);
        }
    }
}

pub struct Context<'a> {
    pub connection: &'a ConnectionInfo,
    pub principal: RpcPrincipal,
    pub current: Current,
    pub saved: Current,
    pub current_stateid: Option<Stateid>,
    pub saved_stateid: Option<Stateid>,
    pub exports: HashMap<Ulid, Arc<MountExport>>,
    pub grant: GrantTrace,
    pub now: Instant,
    pub max_response: usize,
    session: Option<SessionUse>,
    /// A retry of an uncached reply: the operation after SEQUENCE answers RETRY_UNCACHED_REP.
    uncached: bool,
}

impl Context<'_> {
    /// Installs a new current filehandle and clears the current stateid.
    pub fn set_current(&mut self, current: Current) {
        self.current = current;
        self.current_stateid = None;
    }

    pub fn target(&self) -> Result<Target, NfsStat4> {
        match &self.current {
            Current::None => Err(NfsStat4::NoFilehandle),
            Current::Root => Ok(Target::Root),
            Current::Mounts => Ok(Target::Mounts),
            Current::Object { expired: true, .. } => Err(NfsStat4::FilehandleExpired),
            Current::Object { export, object, .. } => Ok(Target::Object {
                export: export.clone(),
                object: *object,
            }),
        }
    }

    pub fn caller(&self, export: &Arc<MountExport>) -> Caller {
        Caller {
            host: self.connection.host,
            peer: self.connection.peer,
            export: export.clone(),
        }
    }

    /// The cached reply size when the client asked to cache this compound.
    fn cache_limit(&self) -> Option<usize> {
        let session = self.session.as_ref().filter(|session| session.cache)?;
        Some(session.max_cached)
    }

    /// The largest reply this compound may build, which a cached compound also keeps to.
    pub fn reply_limit(&self) -> usize {
        let limit = self.cache_limit().unwrap_or(usize::MAX);
        self.max_response.min(limit)
    }

    pub fn client_id(&self) -> Result<u64, NfsStat4> {
        self.session
            .as_ref()
            .map(|session| session.client_id)
            .ok_or(NfsStat4::NotInSession)
    }

    fn sender(&self) -> Sender<'_> {
        Sender {
            host_id: self.connection.host.host_id,
            connection: self.connection.id,
            principal: &self.principal,
        }
    }

    /// Resolves the current-stateid special value; other special stateids pass unchanged.
    pub fn resolve_stateid(&self, stateid: Stateid) -> Result<Stateid, NfsStat4> {
        if stateid == Stateid::CURRENT {
            return self.current_stateid.ok_or(NfsStat4::BadStateid);
        }
        Ok(stateid)
    }
}

/// Engine settings fixed for one server instance. It holds a key, so it never prints.
#[derive(Clone)]
pub struct EngineConfig {
    pub lease: Duration,
    pub limits: ServerLimits,
    /// Distinguishes this server instance; earlier sessions, clients and stateids are unknown.
    pub boot: u32,
    pub session_key: [u8; 32],
    pub server_owner: Bytes,
}

/// The NFSv4.1 protocol engine shared by every connection of one server.
pub struct Engine<B> {
    pub(crate) backend: Arc<B>,
    pub(crate) key: FilehandleKey,
    pub(crate) tables: Arc<Mutex<Tables>>,
    pub(crate) lease: Duration,
    limits: ServerLimits,
    session_key: [u8; 32],
    server_owner: Bytes,
    sessions_made: AtomicU64,
}

impl<B: NfsBackend> Engine<B> {
    pub fn new(backend: Arc<B>, key: FilehandleKey, config: EngineConfig) -> Self {
        Self {
            backend,
            key,
            tables: Arc::new(Mutex::new(Tables {
                sessions: SessionTable::new(config.boot),
                states: StateTable::new(config.boot),
            })),
            lease: config.lease,
            limits: config.limits,
            session_key: config.session_key,
            server_owner: config.server_owner,
            sessions_made: AtomicU64::new(0),
        }
    }

    /// Runs one COMPOUND body. `admitted` is false when the principal has no work slot left,
    /// which answers DELAY before any operation runs.
    pub async fn compound(
        &self,
        connection: &ConnectionInfo,
        principal: RpcPrincipal,
        body: Bytes,
        admitted: bool,
        now: Instant,
    ) -> CompoundOutcome {
        let size = body.len();
        let digest = *blake3::hash(&body).as_bytes();
        let mut reader = XdrReader::new(body);
        let (Ok(tag), Ok(minor), Ok(count)) =
            (reader.opaque(OPAQUE_LIMIT), reader.u32(), reader.u32())
        else {
            return CompoundOutcome::Garbage;
        };
        if minor != 1 {
            return CompoundOutcome::Reply(encode_compound(NfsStat4::MinorMismatch, &tag, 0, &[]));
        }
        let mut ctx = Context {
            connection,
            principal,
            current: Current::None,
            saved: Current::None,
            current_stateid: None,
            saved_stateid: None,
            exports: HashMap::new(),
            grant: GrantTrace::default(),
            now,
            max_response: self.limits.max_response as usize,
            session: None,
            uncached: false,
        };
        let shape = RequestShape {
            size,
            operations: count as usize,
        };
        let mut results = XdrWriter::new();
        let mut done = 0u32;
        let mut status = NfsStat4::Ok;
        for index in 0..count {
            let Ok(code) = reader.u32() else {
                return CompoundOutcome::Garbage;
            };
            let header = results.len();
            let reply_code = NfsOp4::from_code(code).unwrap_or(NfsOp4::Illegal);
            results.put_u32(reply_code.code());
            results.put_u32(NfsStat4::Ok.code());
            done += 1;
            let result: OpResult = match arguments::decode(code, &mut reader) {
                Err(error) => Err(error.into()),
                Ok(_) if !admitted && index == 0 => Err(NfsStat4::Delay.into()),
                Ok(_) if ctx.uncached => Err(NfsStat4::RetryUncached.into()),
                Ok(operation) => match position(&operation, index, count) {
                    Err(error) => Err(error.into()),
                    Ok(()) => match operation {
                        Operation::Sequence(args) => {
                            match self
                                .sequence(&mut ctx, &args, shape, digest, &mut results)
                                .await
                            {
                                SequenceFlow::Run(result) => result,
                                SequenceFlow::Replay(bytes) => {
                                    return CompoundOutcome::Reply(bytes);
                                }
                                SequenceFlow::Deny(stat) => return CompoundOutcome::Denied(stat),
                            }
                        }
                        operation => {
                            let large = matches!(
                                operation,
                                Operation::Read { .. }
                                    | Operation::Readdir(_)
                                    | Operation::Getattr(_)
                            );
                            // A cached compound never runs a change whose result it cannot keep.
                            let tight = ctx.cache_limit().is_some_and(|limit| {
                                !large
                                    && reply_size(&tag, results.len()) + OPERATION_RESERVE > limit
                            });
                            let result = match tight {
                                true => Err(NfsStat4::ReplyUncacheable.into()),
                                false => self.execute(&mut ctx, operation, &mut results).await,
                            };
                            let kept = result.as_ref().err().is_none_or(|error| error.keep);
                            let uncacheable = ctx.cache_limit().is_some_and(|limit| {
                                reply_size(&tag, results.len()) + STATUS_SIZE > limit
                            });
                            if kept && uncacheable {
                                Err(NfsStat4::ReplyUncacheable.into())
                            } else if result.is_ok()
                                && large
                                && results.len() + REPLY_RESERVE > ctx.max_response
                            {
                                Err(NfsStat4::ReplyTooBig.into())
                            } else {
                                result
                            }
                        }
                    },
                },
            };
            if let Err(error) = result {
                if !error.keep {
                    results.truncate(header + 8);
                }
                results.patch_u32(header + 4, error.status.code());
                status = error.status;
                break;
            }
        }
        let reply = encode_compound(status, &tag, done, results.as_slice());
        if let Some(session) = ctx.session.take() {
            let slot = if session.cache && reply.len() <= session.max_cached {
                SlotReply::Cached {
                    digest,
                    bytes: reply.clone(),
                    grant: Box::new(replay_grant(connection, &ctx.principal, &ctx.grant)),
                }
            } else {
                SlotReply::Uncached { digest }
            };
            session.lease.finish(slot);
        }
        CompoundOutcome::Reply(reply)
    }

    async fn execute(
        &self,
        ctx: &mut Context<'_>,
        operation: Operation,
        out: &mut XdrWriter,
    ) -> OpResult {
        match operation {
            Operation::ExchangeId(args) => self.exchange_id(ctx, args, out),
            Operation::CreateSession(args) => self.create_session(ctx, args, out).await,
            Operation::DestroySession(session) => {
                let destroyed = self
                    .tables
                    .lock()
                    .sessions
                    .destroy_session(ctx.sender(), &session);
                Ok(destroyed?)
            }
            Operation::BindConnection { session, direction } => {
                if direction == BACK_ONLY {
                    return Err(NfsStat4::Invalid.into());
                }
                self.tables.lock().sessions.bind(ctx.sender(), &session)?;
                out.put_fixed(&session);
                out.put_u32(FORE_CHANNEL);
                out.put_bool(false);
                Ok(())
            }
            Operation::BackchannelCtl => Ok(()),
            Operation::DestroyClientid(client_id) => {
                let cleanups = {
                    let mut tables = self.tables.lock();
                    tables.sessions.destroy_client(ctx.sender(), client_id)?;
                    tables.states.purge_client(client_id)
                };
                self.run_cleanups(cleanups).await;
                Ok(())
            }
            Operation::ReclaimComplete(_) => {
                let client_id = ctx.client_id()?;
                let completed = self.tables.lock().sessions.reclaim_complete(client_id);
                Ok(completed?)
            }
            Operation::Sequence(_) => Err(NfsStat4::SequencePosition.into()),
            Operation::Putrootfh => {
                ctx.set_current(Current::Root);
                Ok(())
            }
            Operation::Putfh(raw) => self.putfh(ctx, raw).await,
            Operation::Getfh => {
                let handle = self.seal(&ctx.target()?);
                out.put_opaque(&handle);
                Ok(())
            }
            Operation::Savefh => {
                ctx.target()?;
                ctx.saved = ctx.current.clone();
                ctx.saved_stateid = ctx.current_stateid;
                Ok(())
            }
            Operation::Restorefh => {
                if matches!(ctx.saved, Current::None) {
                    return Err(NfsStat4::RestoreFilehandle.into());
                }
                ctx.current = ctx.saved.clone();
                ctx.current_stateid = ctx.saved_stateid;
                Ok(())
            }
            Operation::Lookup(name) => self.lookup(ctx, &name).await,
            Operation::Lookupp => self.lookup_parent(ctx).await,
            Operation::Access(requested) => self.check_access(ctx, requested, out).await,
            Operation::Getattr(request) => self.getattr(ctx, &request, out).await,
            Operation::Setattr {
                stateid,
                attributes,
            } => self.setattr(ctx, stateid, &attributes, out).await,
            Operation::Verify(attributes) => self.verify(ctx, &attributes, true).await,
            Operation::Nverify(attributes) => self.verify(ctx, &attributes, false).await,
            Operation::Read {
                stateid,
                offset,
                count,
            } => self.read(ctx, stateid, offset, count, out).await,
            Operation::Write {
                stateid,
                offset,
                stable,
                data,
            } => self.write(ctx, stateid, (offset, stable), data, out).await,
            Operation::Commit { offset, count } => self.commit(ctx, offset, count, out).await,
            Operation::Create {
                kind,
                name,
                attributes,
            } => self.create(ctx, kind, &name, &attributes, out).await,
            Operation::Remove(name) => self.remove(ctx, &name, out).await,
            Operation::Rename { old, new } => self.rename(ctx, &old, &new, out).await,
            Operation::Readdir(args) => self.readdir(ctx, &args, out).await,
            Operation::Secinfo(name) => self.secinfo(ctx, Some(&name), out).await,
            Operation::SecinfoNoName(style) => self.secinfo_style(ctx, style, out).await,
            Operation::Readlink => {
                ctx.target()?;
                Err(NfsStat4::Invalid.into())
            }
            Operation::Open(args) => self.open(ctx, args, out).await,
            Operation::OpenDowngrade {
                stateid,
                access,
                deny,
            } => self.open_downgrade(ctx, stateid, (access, deny), out).await,
            Operation::Close(stateid) => self.close(ctx, stateid, out).await,
            Operation::Lock(args) => self.lock(ctx, args, out).await,
            Operation::Lockt { kind, range, owner } => {
                self.lock_test(ctx, kind, range, owner, out).await
            }
            Operation::Locku { stateid, range } => self.unlock(ctx, stateid, range, out).await,
            Operation::TestStateid(stateids) => self.test_stateids(ctx, &stateids, out),
            Operation::FreeStateid(stateid) => self.free_stateid(ctx, stateid),
            Operation::Delegreturn => Err(NfsStat4::BadStateid.into()),
            Operation::Unsupported(_) => Err(NfsStat4::NotSupported.into()),
            Operation::Illegal => Err(NfsStat4::OperationIllegal.into()),
        }
    }

    async fn sequence(
        &self,
        ctx: &mut Context<'_>,
        args: &SequenceArgs,
        shape: RequestShape,
        digest: [u8; 32],
        out: &mut XdrWriter,
    ) -> SequenceFlow {
        let request = SlotRequest {
            session: args.session,
            slot: args.slot,
            sequence: args.sequence,
            highest: args.highest,
            digest,
            shape,
        };
        let decision = self
            .tables
            .lock()
            .sessions
            .begin(ctx.sender(), request, ctx.now);
        match decision {
            SlotDecision::Fail(status) => SequenceFlow::Run(Err(status.into())),
            SlotDecision::Replay { bytes, mut grant } => {
                if grant.host != ctx.connection.host || grant.principal != ctx.principal {
                    return SequenceFlow::Deny(AuthStat::TooWeak);
                }
                grant.peer = ctx.connection.peer;
                match self.backend.recheck_replay(&grant).await {
                    Ok(()) => SequenceFlow::Replay(bytes),
                    Err(status) => {
                        debug!(?status, "Refused a cached NFS reply on its replay check");
                        SequenceFlow::Deny(AuthStat::TooWeak)
                    }
                }
            }
            SlotDecision::Uncached { client_id, highest } => {
                ctx.uncached = true;
                let flags = self.tables.lock().states.status_flags(client_id);
                put_sequence(out, args, highest, flags);
                SequenceFlow::Run(Ok(()))
            }
            SlotDecision::Execute {
                client_id,
                fore,
                highest,
            } => {
                ctx.session = Some(SessionUse {
                    client_id,
                    cache: args.cache,
                    max_cached: fore.max_cached as usize,
                    lease: SlotLease {
                        tables: self.tables.clone(),
                        session: args.session,
                        slot: args.slot,
                        digest,
                        done: false,
                    },
                });
                ctx.max_response = fore.max_response as usize;
                let flags = self.tables.lock().states.status_flags(client_id);
                put_sequence(out, args, highest, flags);
                SequenceFlow::Run(Ok(()))
            }
        }
    }

    fn exchange_id(
        &self,
        ctx: &mut Context<'_>,
        args: ExchangeArgs,
        out: &mut XdrWriter,
    ) -> OpResult {
        let result = self.tables.lock().sessions.exchange(
            ctx.sender(),
            args.owner,
            args.verifier,
            ctx.now,
        )?;
        let mut flags = NON_PNFS_FLAG;
        if result.confirmed {
            flags |= CONFIRMED_FLAG;
        }
        out.put_u64(result.client_id);
        out.put_u32(result.sequence);
        out.put_u32(flags);
        out.put_u32(0);
        out.put_u64(0);
        out.put_opaque(&self.server_owner);
        out.put_opaque(&self.server_owner);
        out.put_u32(0);
        Ok(())
    }

    async fn create_session(
        &self,
        ctx: &mut Context<'_>,
        args: SessionArgs,
        out: &mut XdrWriter,
    ) -> OpResult {
        let request = CreateRequest {
            client_id: args.client_id,
            sequence: args.sequence,
            session_id: self.session_id(),
            fore: args.fore,
            back: args.back,
            limits: self.limits,
        };
        let (created, cleanups) = {
            let mut tables = self.tables.lock();
            let (created, replaced) =
                tables
                    .sessions
                    .create_session(ctx.sender(), request, ctx.now)?;
            let mut cleanups = Vec::new();
            for client_id in replaced {
                cleanups.extend(tables.states.purge_client(client_id));
            }
            (created, cleanups)
        };
        self.run_cleanups(cleanups).await;
        out.put_fixed(&created.session_id);
        out.put_u32(created.sequence);
        out.put_u32(created.flags);
        put_channel(out, &created.fore);
        put_channel(out, &created.back);
        Ok(())
    }

    /// An unpredictable session id: a keyed hash of a per-server counter.
    fn session_id(&self) -> [u8; 16] {
        let count = self.sessions_made.fetch_add(1, Ordering::Relaxed);
        let hash = blake3::keyed_hash(&self.session_key, &count.to_be_bytes());
        let mut id = [0u8; 16];
        id.copy_from_slice(&hash.as_bytes()[..16]);
        id
    }

    pub(crate) fn seal(&self, target: &Target) -> [u8; crate::filehandle::HANDLE_SIZE] {
        let handle = match target {
            Target::Root => Filehandle::pseudo_root(),
            Target::Mounts => Filehandle::mounts(),
            Target::Object { export, object } => Filehandle::object(export, object.object_id),
        };
        self.key.seal(&handle)
    }

    /// The live binding `mount_id` for this request, checked once per compound.
    pub(crate) async fn export(
        &self,
        ctx: &mut Context<'_>,
        mount_id: Ulid,
    ) -> Result<Arc<MountExport>, NfsStat4> {
        if let Some(export) = ctx.exports.get(&mount_id) {
            return Ok(export.clone());
        }
        let export = self
            .backend
            .mount_export(&ctx.connection.host, ctx.connection.peer, mount_id)
            .await?;
        if export.mount_id != mount_id || export.host_id != ctx.connection.host.host_id {
            return Err(NfsStat4::Stale);
        }
        let export = Arc::new(export);
        ctx.exports.insert(mount_id, export.clone());
        ctx.grant.mounts.push(mount_id);
        Ok(export)
    }

    /// Revokes expired leases, forgets long idle clients and releases what they held. Runs
    /// on a timer and before a conflict is reported, so an idle holder never blocks others.
    pub async fn sweep(&self, now: Instant) {
        let cleanups = {
            let mut tables = self.tables.lock();
            let mut cleanups = Vec::new();
            for client_id in tables.sessions.expired(now, self.lease) {
                cleanups.extend(
                    tables
                        .states
                        .revoke(|entry| entry.client_id == client_id, Revoked::Expired),
                );
            }
            for client_id in tables.sessions.forget_idle(now, self.lease * IDLE_LEASES) {
                cleanups.extend(tables.states.purge_client(client_id));
            }
            cleanups
        };
        self.run_cleanups(cleanups).await;
    }

    /// Applies one revocation: a host loses its clients, sessions and state; a binding or a
    /// key loses only the state made through it.
    pub async fn revoke(&self, revocation: &Revocation) {
        let cleanups = {
            let mut tables = self.tables.lock();
            match revocation {
                Revocation::Host(host_id) => {
                    let mut cleanups = Vec::new();
                    for client_id in tables.sessions.remove_host(*host_id) {
                        cleanups.extend(tables.states.purge_client(client_id));
                    }
                    cleanups
                }
                Revocation::HostRotated(_) => Vec::new(),
                Revocation::Mount(mount_id) => tables
                    .states
                    .revoke(|entry| entry.object.mount_id == *mount_id, Revoked::Admin),
                Revocation::AccessKey(key) => tables.states.revoke(
                    |entry| entry.caller.export.access_key == *key,
                    Revoked::Admin,
                ),
            }
        };
        self.run_cleanups(cleanups).await;
    }

    pub fn disconnect(&self, connection: u64) {
        self.tables.lock().sessions.disconnect(connection);
    }

    pub(crate) async fn run_cleanups(&self, cleanups: Vec<Cleanup>) {
        for cleanup in cleanups {
            let result = match &cleanup {
                Cleanup::Close {
                    caller,
                    object,
                    owner,
                } => self.backend.close(caller, *object, owner).await,
                Cleanup::Unlock {
                    caller,
                    object,
                    owner,
                } => self
                    .backend
                    .unlock(caller, *object, owner, LockRange::ALL)
                    .await
                    .map(|_| ()),
            };
            if let Err(status) = result {
                debug!(?status, "An NFS state cleanup did not complete");
            }
        }
    }
}

enum SequenceFlow {
    Run(OpResult),
    Replay(Bytes),
    Deny(AuthStat),
}

/// RFC 8881 position rules: SEQUENCE first, and the session-less operations alone.
fn position(operation: &Operation, index: u32, count: u32) -> Result<(), NfsStat4> {
    let alone = matches!(
        operation,
        Operation::ExchangeId(_)
            | Operation::CreateSession(_)
            | Operation::DestroySession(_)
            | Operation::BindConnection { .. }
            | Operation::DestroyClientid(_)
    );
    if index == 0 {
        if matches!(operation, Operation::Sequence(_)) {
            return Ok(());
        }
        if alone {
            return if count == 1 {
                Ok(())
            } else {
                Err(NfsStat4::NotOnlyOperation)
            };
        }
        return Err(NfsStat4::NotInSession);
    }
    match operation {
        Operation::Sequence(_) => Err(NfsStat4::SequencePosition),
        Operation::BindConnection { .. } => Err(NfsStat4::NotOnlyOperation),
        _ => Ok(()),
    }
}

fn replay_grant(
    connection: &ConnectionInfo,
    principal: &RpcPrincipal,
    trace: &GrantTrace,
) -> ReplayGrant {
    ReplayGrant {
        host: connection.host,
        peer: connection.peer,
        principal: principal.clone(),
        mounts: trace.mounts.clone(),
        checks: trace.checks.clone(),
    }
}

/// The encoded size of a COMPOUND4res with `tag` and `results` bytes of results.
fn reply_size(tag: &[u8], results: usize) -> usize {
    12 + tag.len().div_ceil(4) * 4 + results
}

/// The SEQUENCE4resok body.
fn put_sequence(out: &mut XdrWriter, args: &SequenceArgs, highest: u32, flags: u32) {
    out.put_fixed(&args.session);
    out.put_u32(args.sequence);
    out.put_u32(args.slot);
    out.put_u32(highest);
    out.put_u32(highest);
    out.put_u32(flags);
}

fn encode_compound(status: NfsStat4, tag: &[u8], count: u32, results: &[u8]) -> Bytes {
    let mut writer = XdrWriter::new();
    writer.put_u32(status.code());
    writer.put_opaque(tag);
    writer.put_u32(count);
    writer.put_raw(results);
    writer.into_bytes()
}

fn put_channel(out: &mut XdrWriter, channel: &ChannelAttributes) {
    out.put_u32(channel.header_pad);
    out.put_u32(channel.max_request);
    out.put_u32(channel.max_response);
    out.put_u32(channel.max_cached);
    out.put_u32(channel.max_operations);
    out.put_u32(channel.max_requests);
    out.put_u32(0);
}

pub fn put_stateid(out: &mut XdrWriter, stateid: &Stateid) {
    out.put_u32(stateid.seqid);
    out.put_fixed(&stateid.other);
}

pub fn put_change(out: &mut XdrWriter, change: &ChangeInfo) {
    out.put_bool(change.atomic);
    out.put_u64(change.before);
    out.put_u64(change.after);
}

#[cfg(test)]
#[path = "compound_tests.rs"]
mod tests;
