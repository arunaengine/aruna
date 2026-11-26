//! Per-command handlers of an authenticated session: tree connect, create and close, data,
//! locks, IOCTL, directory enumeration and information queries and changes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_admission::SlotPermit;
use bytes::Bytes;

use crate::access::{self, INFO_FILE, INFO_FILESYSTEM, INFO_SECURITY, Rights};
use crate::backend::{
    BasicChange, Credential, Disposition, FileInfo, InfoChange, LockRange, ObjectKind, OpenRequest,
    Scope, ShareTarget, SmbBackend, TreeGrant, WriteRequest,
};
use crate::info::{self, VolumeFacts, class};
use crate::messages::{
    self, CreateContext, FileTimes, LOCK_EXCLUSIVE, LOCK_IMMEDIATE, LOCK_SHARED, LOCK_UNLOCK,
};
use crate::path::{join_path, parse_path};
use crate::rename::parse_rename;
use crate::server::Connection;
use crate::session::{Listing, Open, OpenView, Tree};
use crate::status;
use crate::wire::{Command, FileId, Header, RELATED_FILE, utf16_text};

const DIRECTORY_FILE: u32 = 0x0000_0001;
const NON_DIRECTORY: u32 = 0x0000_0040;
const DELETE_ON_CLOSE: u32 = 0x0000_1000;
const OPEN_BY_ID: u32 = 0x0000_2000;
const NO_CACHING: u32 = 0x0000_0030;
const ENCRYPT_SHARE: u32 = 0x0000_8000;
const TREE_EXTENSION: u16 = 0x0004;
const VALIDATE_NEGOTIATE: u32 = 0x0014_0204;
const PIPE_WAIT: u32 = 0x0011_0018;
const DFS_REFERRALS: u32 = 0x0006_0194;
const DFS_EXTENDED: u32 = 0x0006_01B0;
const NETWORK_INTERFACES: u32 = 0x0014_01FC;
const DISPOSITION_DELETE: u32 = 0x0000_0001;
const DISPOSITION_POSIX: u32 = 0x0000_0002;
const DISPOSITION_IMAGE: u32 = 0x0000_0004;
const DISPOSITION_CLOSE: u32 = 0x0000_0008;
const DISPOSITION_READONLY: u32 = 0x0000_0010;
const MAXIMAL_ACCESS: &[u8] = b"MxAc";
const DURABLE_RECONNECT: &[u8] = b"DHnC";
const DURABLE_SECOND: &[u8] = b"DH2C";
/// Payload bytes covered by one credit of a multi-credit request.
const CREDIT_BYTES: u64 = 65_536;

/// One element of a request after compound splitting.
#[derive(Clone, Debug)]
pub(crate) struct Element {
    pub header: Header,
    pub bytes: Vec<u8>,
}

/// Session, tree and related FileId an element runs with after compound propagation.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Context {
    pub session_id: u64,
    pub tree_id: u32,
    pub related_file: Option<FileId>,
    pub related_failure: Option<u32>,
}

impl Context {
    pub fn of(header: &Header) -> Self {
        Self {
            session_id: header.session_id,
            tree_id: header.tree_id,
            related_file: None,
            related_failure: None,
        }
    }
}

/// The outcome of one element before header and credits are added.
#[derive(Debug, Default)]
pub(crate) struct Reply {
    pub status: u32,
    pub body: Vec<u8>,
    pub file_id: Option<FileId>,
    pub session_id: Option<u64>,
    pub tree_id: Option<u32>,
    /// Sign with the session signing key (the final SESSION_SETUP response).
    pub signed: bool,
    /// Remove the session after this response is sealed (LOGOFF).
    pub ends_session: bool,
    /// Close the connection without answering.
    pub terminate: bool,
}

impl Reply {
    pub fn ok(body: Vec<u8>) -> Self {
        Self {
            status: status::SUCCESS,
            body,
            ..Self::default()
        }
    }

    pub fn fail(code: u32) -> Self {
        Self {
            status: code,
            body: messages::error_body(&[]),
            ..Self::default()
        }
    }

    pub fn with(code: u32, body: Vec<u8>) -> Self {
        Self {
            status: code,
            body,
            ..Self::default()
        }
    }
}

type Outcome = Result<Reply, u32>;

fn invalid<E>(_: E) -> u32 {
    status::INVALID_PARAMETER
}

fn backend_status(error: crate::backend::BackendError) -> u32 {
    error.status()
}

fn file_times(info: &FileInfo, attributes: u32) -> FileTimes {
    FileTimes {
        created: info.created,
        accessed: info.accessed,
        written: info.written,
        changed: info.changed,
        allocation: info.allocation,
        size: info.size,
        attributes,
    }
}

/// A FILETIME field of FileBasicInformation: zero and the two negative markers leave the
/// timestamp unchanged.
fn time_field(value: u64) -> Option<u64> {
    if value == 0 || value >= u64::MAX - 1 {
        None
    } else {
        Some(value)
    }
}

fn read_u64(bytes: &[u8], at: usize) -> u64 {
    let mut value = [0u8; 8];
    value.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(value)
}

/// What an authorized command holds while it runs.
struct Authorized {
    _permit: SlotPermit,
}

impl<B: SmbBackend> Connection<B> {
    /// Runs one element and maps a status error to an ERROR response.
    pub(crate) async fn execute(self: &Arc<Self>, element: &Element, context: Context) -> Reply {
        let Some(command) = Command::from_code(element.header.command) else {
            return Reply::fail(status::INVALID_PARAMETER);
        };
        let outcome = match command {
            Command::TreeConnect => self.tree_connect(element, context).await,
            Command::TreeDisconnect => self.tree_disconnect(element, context).await,
            Command::Logoff => self.logoff(element, context).await,
            Command::Create => self.create(element, context).await,
            Command::Close => self.close(element, context).await,
            Command::Flush => self.flush(element, context).await,
            Command::Read => self.read(element, context).await,
            Command::Write => self.write(element, context).await,
            Command::Lock => self.lock(element, context).await,
            Command::Ioctl => self.ioctl(element, context).await,
            Command::Echo => messages::parse_empty(&element.bytes)
                .map(|()| Reply::ok(messages::empty_body()))
                .map_err(invalid),
            Command::QueryDirectory => self.query_directory(element, context).await,
            Command::QueryInfo => self.query_info(element, context).await,
            Command::SetInfo => self.set_info(element, context).await,
            Command::ChangeNotify => Err(status::NOT_SUPPORTED),
            Command::OplockBreak => Err(status::INVALID_PARAMETER),
            Command::SessionSetup => Err(status::NOT_SUPPORTED),
            Command::Negotiate | Command::Cancel => Err(status::INVALID_PARAMETER),
        };
        outcome.unwrap_or_else(Reply::fail)
    }

    /// Resolves the session credential, takes a per-principal work permit and runs live
    /// admission; a failed admission expires the session.
    async fn authorize(self: &Arc<Self>, session_id: u64) -> Result<(Credential, Authorized), u32> {
        let credential = self.state.lock().credential(session_id)?;
        let permit = self
            .shared
            .limits
            .try_acquire_work(credential.principal)
            .ok_or(status::INSUFFICIENT_RESOURCES)?;
        if let Err(error) = self.backend.admit(&credential).await {
            tracing::debug!(%error, "SMB session admission failed");
            let closing = self.state.lock().expire_session(session_id);
            self.close_all(closing).await;
            return Err(status::NETWORK_SESSION_EXPIRED);
        }
        Ok((credential, Authorized { _permit: permit }))
    }

    /// Resolves the FileId of a request, following a related compound chain.
    async fn open_for(
        self: &Arc<Self>,
        context: Context,
        file_id: FileId,
    ) -> Result<(OpenView, Authorized), u32> {
        let file_id = if file_id == RELATED_FILE {
            match (context.related_file, context.related_failure) {
                (Some(file_id), _) => file_id,
                (None, Some(failure)) => return Err(failure),
                (None, None) => return Err(status::FILE_CLOSED),
            }
        } else {
            file_id
        };
        let (_, authorized) = self.authorize(context.session_id).await?;
        let view = self
            .state
            .lock()
            .open(context.session_id, context.tree_id, file_id)?;
        Ok((view, authorized))
    }

    pub(crate) async fn close_all(&self, opens: Vec<(Scope, u64)>) {
        for (scope, handle) in opens {
            if let Err(error) = self.backend.close(&scope, handle).await {
                tracing::debug!(%error, "SMB cleanup close failed");
            }
        }
    }

    async fn tree_connect(self: &Arc<Self>, element: &Element, context: Context) -> Outcome {
        let request = messages::parse_tree(&element.bytes).map_err(invalid)?;
        if request.flags & TREE_EXTENSION != 0 {
            return Err(status::NOT_SUPPORTED);
        }
        let (credential, _authorized) = self.authorize(context.session_id).await?;
        let text = utf16_text(&request.path).ok_or(status::BAD_NETWORK_NAME)?;
        let share = text
            .strip_prefix("\\\\")
            .and_then(|rest| rest.split_once('\\'))
            .map(|(_, share)| share)
            .filter(|share| !share.contains('\\'))
            .ok_or(status::BAD_NETWORK_NAME)?;
        let target = ShareTarget::route(share).ok_or(status::BAD_NETWORK_NAME)?;
        let grant = self
            .backend
            .connect_tree(&credential, &target)
            .await
            .map_err(|error| match error {
                crate::backend::BackendError::NotFound => status::BAD_NETWORK_NAME,
                other => other.status(),
            })?;
        let maximal = access::allowed_mask(
            Rights {
                read: true,
                write: true,
                delete: true,
            },
            grant.read_only,
        );
        let tree_id = {
            let mut state = self.state.lock();
            let session = state
                .sessions
                .get_mut(&context.session_id)
                .ok_or(status::USER_SESSION_DELETED)?;
            let tree_id = session.next_tree;
            session.next_tree = session.next_tree.wrapping_add(1).max(1);
            session.trees.insert(
                tree_id,
                Tree {
                    share: target,
                    grant,
                },
            );
            tree_id
        };
        let mut reply = Reply::ok(messages::tree_body(NO_CACHING | ENCRYPT_SHARE, maximal));
        reply.tree_id = Some(tree_id);
        Ok(reply)
    }

    async fn tree_disconnect(self: &Arc<Self>, element: &Element, context: Context) -> Outcome {
        messages::parse_empty(&element.bytes).map_err(invalid)?;
        let closing = {
            let mut state = self.state.lock();
            state.credential(context.session_id)?;
            let session = state
                .sessions
                .get_mut(&context.session_id)
                .ok_or(status::USER_SESSION_DELETED)?;
            if session.trees.remove(&context.tree_id).is_none() {
                return Err(status::NETWORK_NAME_DELETED);
            }
            state.take_opens(|open| {
                open.session_id == context.session_id && open.tree_id == context.tree_id
            })
        };
        self.close_all(closing).await;
        Ok(Reply::ok(messages::empty_body()))
    }

    async fn logoff(self: &Arc<Self>, element: &Element, context: Context) -> Outcome {
        messages::parse_empty(&element.bytes).map_err(invalid)?;
        let closing = {
            let mut state = self.state.lock();
            state.credential(context.session_id)?;
            state.take_opens(|open| open.session_id == context.session_id)
        };
        self.close_all(closing).await;
        let mut reply = Reply::ok(messages::empty_body());
        reply.ends_session = true;
        Ok(reply)
    }

    fn scope_of(
        &self,
        context: Context,
        credential: Credential,
    ) -> Result<(Scope, TreeGrant), u32> {
        let (share, grant) = self
            .state
            .lock()
            .tree(context.session_id, context.tree_id)?;
        Ok((Scope { credential, share }, grant))
    }

    async fn create(self: &Arc<Self>, element: &Element, context: Context) -> Outcome {
        let request = messages::parse_create(&element.bytes).map_err(invalid)?;
        let (credential, _authorized) = self.authorize(context.session_id).await?;
        let (scope, grant) = self.scope_of(context, credential)?;
        let disposition =
            Disposition::from_code(request.disposition).ok_or(status::INVALID_PARAMETER)?;
        if request.impersonation > 3 {
            return Err(status::INVALID_PARAMETER);
        }
        let wants_directory = request.options & DIRECTORY_FILE != 0;
        let wants_file = request.options & NON_DIRECTORY != 0;
        if wants_directory && wants_file {
            return Err(status::INVALID_PARAMETER);
        }
        if request.options & OPEN_BY_ID != 0 {
            return Err(status::NOT_SUPPORTED);
        }
        if request
            .contexts
            .iter()
            .any(|context| context.name == DURABLE_RECONNECT || context.name == DURABLE_SECOND)
        {
            return Err(status::NAME_NOT_FOUND);
        }
        let name = utf16_text(&request.name).ok_or(status::NAME_INVALID)?;
        if name.starts_with('\\') {
            return Err(status::INVALID_PARAMETER);
        }
        let components = parse_path(&name)?;
        let path = join_path(&components);
        let lookup = self
            .backend
            .lookup(&scope, &path)
            .await
            .map_err(backend_status)?;
        let writable = lookup.rights.write && !grant.read_only;
        match lookup.kind {
            Some(kind) => {
                if wants_directory && kind == ObjectKind::File {
                    return Err(status::NOT_DIRECTORY);
                }
                if wants_file && kind == ObjectKind::Directory {
                    return Err(status::IS_DIRECTORY);
                }
                if disposition == Disposition::Create {
                    return Err(status::NAME_COLLISION);
                }
                if disposition.truncates() {
                    if kind == ObjectKind::Directory {
                        return Err(status::INVALID_PARAMETER);
                    }
                    if !writable {
                        return Err(status::ACCESS_DENIED);
                    }
                }
            }
            None => {
                if !disposition.creates() {
                    return Err(status::NAME_NOT_FOUND);
                }
                if !writable || components.is_empty() {
                    return Err(status::ACCESS_DENIED);
                }
            }
        }
        let allowed = access::allowed_mask(lookup.rights, grant.read_only);
        let granted = access::grant(request.desired_access, allowed)?;
        let delete_on_close = request.options & DELETE_ON_CLOSE != 0;
        if delete_on_close && granted & access::DELETE == 0 {
            return Err(status::ACCESS_DENIED);
        }
        let kind = if wants_directory {
            Some(ObjectKind::Directory)
        } else if wants_file {
            Some(ObjectKind::File)
        } else {
            None
        };
        let opened = self
            .backend
            .open(
                &scope,
                OpenRequest {
                    path: path.clone(),
                    disposition,
                    kind,
                    granted,
                    share_access: request.share_access,
                    delete_on_close,
                },
            )
            .await
            .map_err(backend_status)?;
        let file_id = {
            let mut state = self.state.lock();
            let volatile = state.next_volatile;
            state.next_volatile += 1;
            let file_id = FileId {
                persistent: volatile ^ 0x5A5A_0000_0000_0000,
                volatile,
            };
            state.opens.insert(
                volatile,
                Open {
                    session_id: context.session_id,
                    tree_id: context.tree_id,
                    persistent: file_id.persistent,
                    handle: opened.handle,
                    granted,
                    kind: opened.info.kind,
                    path,
                    scope,
                    read_only: grant.read_only,
                    listing: None,
                },
            );
            file_id
        };
        let mut contexts = Vec::new();
        if request
            .contexts
            .iter()
            .any(|context| context.name == MAXIMAL_ACCESS)
        {
            let mut data = status::SUCCESS.to_le_bytes().to_vec();
            data.extend_from_slice(&allowed.to_le_bytes());
            contexts.push(CreateContext {
                name: MAXIMAL_ACCESS.to_vec(),
                data,
            });
        }
        let attributes = info::attributes(&opened.info, grant.read_only);
        let body = messages::create_body(
            opened.action.code(),
            &file_times(&opened.info, attributes),
            file_id,
            &messages::context_chain(&contexts),
        );
        let mut reply = Reply::ok(body);
        reply.file_id = Some(file_id);
        Ok(reply)
    }

    async fn close(self: &Arc<Self>, element: &Element, context: Context) -> Outcome {
        let request = messages::parse_close(&element.bytes).map_err(invalid)?;
        let file_id = if request.file_id == RELATED_FILE {
            context
                .related_file
                .ok_or(context.related_failure.unwrap_or(status::FILE_CLOSED))?
        } else {
            request.file_id
        };
        let (scope, handle, read_only) = {
            let mut state = self.state.lock();
            state.credential(context.session_id)?;
            state.open(context.session_id, context.tree_id, file_id)?;
            let open = state
                .opens
                .remove(&file_id.volatile)
                .ok_or(status::FILE_CLOSED)?;
            (open.scope, open.handle, open.read_only)
        };
        let admitted = self.authorize(context.session_id).await;
        let closed = self.backend.close(&scope, handle).await;
        admitted?;
        let info = closed.map_err(backend_status)?;
        let times = file_times(&info, info::attributes(&info, read_only));
        let body = if request.flags & messages::CLOSE_POSTQUERY != 0 {
            messages::close_body(Some(&times))
        } else {
            messages::close_body(None)
        };
        Ok(Reply::ok(body))
    }

    async fn flush(self: &Arc<Self>, element: &Element, context: Context) -> Outcome {
        let file_id = messages::parse_flush(&element.bytes).map_err(invalid)?;
        let (open, _authorized) = self.open_for(context, file_id).await?;
        access::check_flush(open.granted, open.kind == ObjectKind::Directory)?;
        self.backend
            .flush(&open.scope, open.handle)
            .await
            .map_err(backend_status)?;
        Ok(Reply::ok(messages::lock_body()))
    }

    fn check_charge(&self, header: &Header, length: u64) -> Result<(), u32> {
        let needed = length.div_ceil(CREDIT_BYTES).max(1);
        if u64::from(header.credit_charge.max(1)) < needed {
            return Err(status::INVALID_PARAMETER);
        }
        Ok(())
    }

    async fn read(self: &Arc<Self>, element: &Element, context: Context) -> Outcome {
        let request = messages::parse_read(&element.bytes).map_err(invalid)?;
        let (open, _authorized) = self.open_for(context, request.file_id).await?;
        access::check_read(open.granted)?;
        if open.kind == ObjectKind::Directory {
            return Err(status::INVALID_DEVICE_REQUEST);
        }
        if request.channel != 0 || request.length > self.shared.config.max_read {
            return Err(status::INVALID_PARAMETER);
        }
        self.check_charge(&element.header, u64::from(request.length))?;
        request
            .offset
            .checked_add(u64::from(request.length))
            .filter(|end| *end <= i64::MAX as u64)
            .ok_or(status::INVALID_PARAMETER)?;
        if request.length == 0 {
            return Ok(Reply::ok(messages::read_body(&[])));
        }
        let data = self
            .backend
            .read(&open.scope, open.handle, request.offset, request.length)
            .await
            .map_err(backend_status)?;
        if data.is_empty() || (data.len() as u64) < u64::from(request.minimum) {
            return Err(status::END_OF_FILE);
        }
        Ok(Reply::ok(messages::read_body(&data)))
    }

    async fn write(self: &Arc<Self>, element: &Element, context: Context) -> Outcome {
        let request = messages::parse_write(&element.bytes).map_err(invalid)?;
        let (open, _authorized) = self.open_for(context, request.file_id).await?;
        let (mode, offset) = access::check_write(open.granted, request.offset, request.data.len())?;
        if open.kind == ObjectKind::Directory {
            return Err(status::INVALID_DEVICE_REQUEST);
        }
        if request.channel != 0 || request.data.len() > self.shared.config.max_write as usize {
            return Err(status::INVALID_PARAMETER);
        }
        self.check_charge(&element.header, request.data.len() as u64)?;
        if request.data.is_empty() {
            return Ok(Reply::ok(messages::write_body(0)));
        }
        let count = self
            .backend
            .write(
                &open.scope,
                open.handle,
                WriteRequest {
                    offset,
                    data: Bytes::from(request.data),
                    mode,
                    through: request.flags & messages::WRITE_THROUGH != 0,
                },
            )
            .await
            .map_err(backend_status)?;
        Ok(Reply::ok(messages::write_body(count)))
    }

    async fn lock(self: &Arc<Self>, element: &Element, context: Context) -> Outcome {
        let request = messages::parse_lock(&element.bytes).map_err(invalid)?;
        let (open, _authorized) = self.open_for(context, request.file_id).await?;
        if open.kind == ObjectKind::Directory {
            return Err(status::INVALID_PARAMETER);
        }
        access::require_any(
            open.granted,
            access::FILE_READ_DATA | access::FILE_WRITE_DATA,
        )?;
        let unlock = request.locks[0].flags & LOCK_UNLOCK != 0;
        let mut ranges = Vec::with_capacity(request.locks.len());
        for entry in &request.locks {
            if unlock {
                if entry.flags != LOCK_UNLOCK {
                    return Err(status::INVALID_PARAMETER);
                }
            } else {
                let mode = entry.flags & !LOCK_IMMEDIATE;
                if mode != LOCK_SHARED && mode != LOCK_EXCLUSIVE {
                    return Err(status::INVALID_PARAMETER);
                }
                if request.locks.len() > 1 && entry.flags & LOCK_IMMEDIATE == 0 {
                    return Err(status::INVALID_PARAMETER);
                }
            }
            if entry.length > 0 && entry.offset.checked_add(entry.length - 1).is_none() {
                return Err(status::INVALID_LOCK_RANGE);
            }
            ranges.push(LockRange {
                offset: entry.offset,
                length: entry.length,
                exclusive: entry.flags & LOCK_EXCLUSIVE != 0,
            });
        }
        let result = if unlock {
            self.backend.unlock(&open.scope, open.handle, ranges).await
        } else {
            let wait = request.locks.len() == 1 && request.locks[0].flags & LOCK_IMMEDIATE == 0;
            self.backend
                .lock(&open.scope, open.handle, ranges, wait)
                .await
        };
        result.map_err(backend_status)?;
        Ok(Reply::ok(messages::lock_body()))
    }

    async fn ioctl(self: &Arc<Self>, element: &Element, context: Context) -> Outcome {
        let request = messages::parse_ioctl(&element.bytes).map_err(invalid)?;
        let _ = self.authorize(context.session_id).await?;
        if request.flags & messages::IOCTL_FSCTL == 0 {
            return Err(status::NOT_SUPPORTED);
        }
        match request.control {
            VALIDATE_NEGOTIATE => self.validate_negotiate(&request.input, request.control),
            PIPE_WAIT | NETWORK_INTERFACES => Err(status::NOT_SUPPORTED),
            DFS_REFERRALS | DFS_EXTENDED => Err(status::FS_DRIVER_REQUIRED),
            _ => Err(status::INVALID_DEVICE_REQUEST),
        }
    }

    /// FSCTL_VALIDATE_NEGOTIATE_INFO: echoes the negotiated values when the client's view
    /// matches and ends the connection when it does not.
    fn validate_negotiate(&self, input: &[u8], control: u32) -> Outcome {
        let state = self.state.lock();
        let Some(negotiated) = state.negotiated.as_ref() else {
            return Ok(Reply {
                terminate: true,
                ..Reply::default()
            });
        };
        let mut expected = negotiated.client_capabilities.to_le_bytes().to_vec();
        expected.extend_from_slice(&negotiated.client_guid);
        expected.extend_from_slice(&negotiated.client_mode.to_le_bytes());
        expected.extend_from_slice(&(negotiated.dialects.len() as u16).to_le_bytes());
        for dialect in &negotiated.dialects {
            expected.extend_from_slice(&dialect.to_le_bytes());
        }
        if input != expected.as_slice() {
            return Ok(Reply {
                terminate: true,
                ..Reply::default()
            });
        }
        let mut output = crate::handshake::SERVER_CAPABILITIES.to_le_bytes().to_vec();
        output.extend_from_slice(&self.shared.server_guid);
        output.extend_from_slice(&crate::handshake::SECURITY_MODE.to_le_bytes());
        output.extend_from_slice(&crate::handshake::DIALECT.to_le_bytes());
        Ok(Reply::ok(messages::ioctl_body(
            control,
            RELATED_FILE,
            &output,
        )))
    }

    async fn query_directory(self: &Arc<Self>, element: &Element, context: Context) -> Outcome {
        let request = messages::parse_directory(&element.bytes).map_err(invalid)?;
        let (open, _authorized) = self.open_for(context, request.file_id).await?;
        if open.kind != ObjectKind::Directory {
            return Err(status::INVALID_PARAMETER);
        }
        access::check_list(open.granted)?;
        let probe = FileInfo {
            kind: ObjectKind::File,
            size: 0,
            allocation: 0,
            created: 0,
            accessed: 0,
            written: 0,
            changed: 0,
            delete_pending: false,
        };
        if info::directory_entry(request.class, "", &probe, 0).is_none() {
            return Err(status::INVALID_INFO_CLASS);
        }
        let pattern = if request.pattern.is_empty() {
            "*".to_owned()
        } else {
            utf16_text(&request.pattern).ok_or(status::NAME_INVALID)?
        };
        if pattern.contains('\\') || pattern.contains('/') {
            return Err(status::NAME_INVALID);
        }
        let restart = request.flags & (messages::RESTART_SCANS | messages::REOPEN) != 0;
        let mut listing = {
            let mut state = self.state.lock();
            let entry = state
                .opens
                .get_mut(&open.volatile)
                .ok_or(status::FILE_CLOSED)?;
            match entry.listing.take() {
                Some(listing) if !restart => listing,
                _ => Listing {
                    pattern: pattern.clone(),
                    ..Listing::default()
                },
            }
        };
        let limit = request.output_limit.min(self.shared.config.max_transact) as usize;
        let result = self
            .fill_listing(&open, &mut listing, request.class, request.flags, limit)
            .await;
        let returned_before = listing.returned;
        {
            let mut state = self.state.lock();
            if let Some(entry) = state.opens.get_mut(&open.volatile) {
                entry.listing = Some(listing);
            }
        }
        let buffer = result?;
        if buffer.is_empty() {
            return Err(if returned_before {
                status::NO_MORE_FILES
            } else {
                status::NO_SUCH_FILE
            });
        }
        Ok(Reply::ok(messages::output_body(&buffer)))
    }

    async fn fill_listing(
        &self,
        open: &OpenView,
        listing: &mut Listing,
        file_class: u8,
        flags: u8,
        limit: usize,
    ) -> Result<Vec<u8>, u32> {
        let mut buffer = Vec::new();
        let mut last = None;
        let pattern = listing.pattern.clone();
        loop {
            if listing.buffered.is_empty() && !listing.exhausted {
                let page = self
                    .backend
                    .list(&open.scope, open.handle, listing.cursor.take())
                    .await
                    .map_err(backend_status)?;
                listing.buffered.extend(
                    page.entries
                        .into_iter()
                        .filter(|entry| info::pattern_matches(&pattern, &entry.name)),
                );
                listing.exhausted = page.next.is_none();
                listing.cursor = page.next;
                continue;
            }
            let Some(entry) = listing.buffered.front() else {
                break;
            };
            let attributes = info::attributes(&entry.info, open.read_only);
            let encoded = info::directory_entry(file_class, &entry.name, &entry.info, attributes)
                .ok_or(status::INVALID_INFO_CLASS)?;
            match info::append_entry(&mut buffer, last, &encoded, limit) {
                Some(offset) => last = Some(offset),
                None if last.is_none() => return Err(status::INFO_LENGTH_MISMATCH),
                None => break,
            }
            listing.buffered.pop_front();
            listing.returned = true;
            if flags & messages::SINGLE_ENTRY != 0 {
                break;
            }
        }
        Ok(buffer)
    }

    async fn query_info(self: &Arc<Self>, element: &Element, context: Context) -> Outcome {
        let request = messages::parse_query(&element.bytes).map_err(invalid)?;
        let (open, _authorized) = self.open_for(context, request.file_id).await?;
        let need = access::query_need(request.info_type, request.class, request.additional)?;
        access::require_all(open.granted, need)?;
        let limit = request.output_limit as usize;
        let data = match request.info_type {
            INFO_FILE => {
                if request.class == class::FULL_EA {
                    return Err(status::NO_EXTENDED);
                }
                let info = self
                    .backend
                    .query_info(&open.scope, open.handle)
                    .await
                    .map_err(backend_status)?;
                let attributes = info::attributes(&info, open.read_only);
                match request.class {
                    class::BASIC => info::basic(&info, attributes),
                    class::STANDARD => info::standard(&info),
                    class::INTERNAL => info::internal(),
                    class::EA => info::extended(),
                    class::ACCESS => info::access(open.granted),
                    class::POSITION => 0u64.to_le_bytes().to_vec(),
                    class::MODE | class::ALIGNMENT => 0u32.to_le_bytes().to_vec(),
                    class::ALL => {
                        let name = format!("\\{}", open.path.replace('/', "\\"));
                        info::all(&info, attributes, open.granted, &name)
                    }
                    class::NETWORK_OPEN => info::network_open(&info, attributes),
                    class::ATTRIBUTE_TAG => info::attribute_tag(attributes),
                    class::STREAM => info::stream(&info),
                    _ => return Err(status::INVALID_INFO_CLASS),
                }
            }
            INFO_FILESYSTEM => {
                let (_, grant) = self
                    .state
                    .lock()
                    .tree(context.session_id, context.tree_id)?;
                let facts = VolumeFacts {
                    capacity: grant.capacity,
                    available: grant.available,
                    read_only: grant.read_only,
                    serial: (u32::from(self.shared.server_guid[0]) << 24) | context.tree_id,
                };
                info::filesystem(request.class, &facts).ok_or(status::NOT_SUPPORTED)?
            }
            INFO_SECURITY => {
                let data = info::security_descriptor(request.additional, open.granted);
                if data.len() > limit {
                    return Ok(Reply::with(
                        status::BUFFER_TOO_SMALL,
                        messages::error_body(&(data.len() as u32).to_le_bytes()),
                    ));
                }
                data
            }
            _ => return Err(status::INVALID_PARAMETER),
        };
        if data.len() > limit {
            if request.info_type != INFO_FILE || info::fixed_size(request.class) {
                return Err(status::INFO_LENGTH_MISMATCH);
            }
            return Ok(Reply::with(
                status::BUFFER_OVERFLOW,
                messages::output_body(&data[..limit]),
            ));
        }
        Ok(Reply::ok(messages::output_body(&data)))
    }

    async fn set_info(self: &Arc<Self>, element: &Element, context: Context) -> Outcome {
        let request = messages::parse_set(&element.bytes).map_err(invalid)?;
        let (open, _authorized) = self.open_for(context, request.file_id).await?;
        let need = access::set_need(request.info_type, request.class, request.additional)?;
        access::require_all(open.granted, need)?;
        if request.info_type != INFO_FILE {
            return Err(status::NOT_SUPPORTED);
        }
        let buffer = &request.buffer;
        match request.class {
            class::BASIC => {
                if buffer.len() < 36 {
                    return Err(status::INFO_LENGTH_MISMATCH);
                }
                let attributes =
                    u32::from_le_bytes([buffer[32], buffer[33], buffer[34], buffer[35]]);
                if open.kind == ObjectKind::File && attributes & info::ATTRIBUTE_DIRECTORY != 0 {
                    return Err(status::INVALID_PARAMETER);
                }
                let change = BasicChange {
                    created: time_field(read_u64(buffer, 0)),
                    accessed: time_field(read_u64(buffer, 8)),
                    written: time_field(read_u64(buffer, 16)),
                    changed: time_field(read_u64(buffer, 24)),
                    attributes,
                };
                self.change(&open, InfoChange::Basic(change)).await?;
            }
            class::END_OF_FILE | class::ALLOCATION => {
                if buffer.len() < 8 {
                    return Err(status::INFO_LENGTH_MISMATCH);
                }
                if open.kind == ObjectKind::Directory {
                    return Err(status::INVALID_PARAMETER);
                }
                let value = read_u64(buffer, 0);
                if value > i64::MAX as u64 {
                    return Err(status::INVALID_PARAMETER);
                }
                let change = if request.class == class::END_OF_FILE {
                    InfoChange::EndOfFile(value)
                } else {
                    InfoChange::Allocation(value)
                };
                self.change(&open, change).await?;
            }
            class::DISPOSITION => {
                let delete = *buffer.first().ok_or(status::INFO_LENGTH_MISMATCH)? != 0;
                self.backend
                    .set_disposition(&open.scope, open.handle, delete)
                    .await
                    .map_err(backend_status)?;
            }
            class::DISPOSITION_EX => {
                if buffer.len() < 4 {
                    return Err(status::INFO_LENGTH_MISMATCH);
                }
                let flags = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
                let known = DISPOSITION_DELETE
                    | DISPOSITION_POSIX
                    | DISPOSITION_IMAGE
                    | DISPOSITION_CLOSE
                    | DISPOSITION_READONLY;
                if flags & !known != 0 {
                    return Err(status::INVALID_PARAMETER);
                }
                if flags & (DISPOSITION_POSIX | DISPOSITION_IMAGE | DISPOSITION_CLOSE) != 0 {
                    return Err(status::NOT_SUPPORTED);
                }
                self.backend
                    .set_disposition(&open.scope, open.handle, flags & DISPOSITION_DELETE != 0)
                    .await
                    .map_err(backend_status)?;
            }
            class::RENAME => {
                let target = parse_rename(buffer)?;
                let path = join_path(&target.components);
                self.backend
                    .rename(&open.scope, open.handle, &path, target.replace)
                    .await
                    .map_err(backend_status)?;
                if let Some(entry) = self.state.lock().opens.get_mut(&open.volatile) {
                    entry.path = path;
                }
            }
            _ => return Err(status::NOT_SUPPORTED),
        }
        Ok(Reply::ok(messages::set_body()))
    }

    async fn change(&self, open: &OpenView, change: InfoChange) -> Result<(), u32> {
        self.backend
            .set_info(&open.scope, open.handle, change)
            .await
            .map_err(backend_status)
    }

    /// The FileId a waiting request names, for telling the backend about a cancellation.
    pub(crate) fn element_file(element: &Element) -> Option<FileId> {
        match Command::from_code(element.header.command)? {
            Command::Read => messages::parse_read(&element.bytes)
                .ok()
                .map(|request| request.file_id),
            Command::Write => messages::parse_write(&element.bytes)
                .ok()
                .map(|request| request.file_id),
            Command::Lock => messages::parse_lock(&element.bytes)
                .ok()
                .map(|request| request.file_id),
            Command::Flush => messages::parse_flush(&element.bytes).ok(),
            _ => None,
        }
    }

    pub(crate) async fn notify_cancel(&self, element: &Element, context: Context) {
        let Some(file_id) = Self::element_file(element) else {
            return;
        };
        let view = self
            .state
            .lock()
            .open(context.session_id, context.tree_id, file_id);
        if let Ok(view) = view {
            self.backend.cancel(&view.scope, view.handle).await;
        }
    }
}

#[cfg(test)]
#[path = "dispatch_tests.rs"]
mod tests;
