//! Test support: an in-memory `SmbBackend` and an SMB 3.1.1 client with an NTLMv2
//! initiator that drives the engine over an in-process duplex stream.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use aruna_admission::{RateLimits, VfsLimits};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::user::id::UserId;
use bytes::Bytes;
use parking_lot::Mutex;
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio::sync::{Notify, broadcast};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;
use zeroize::Zeroizing;

use crate::access::{Rights, WriteMode, append_allowed};
use crate::backend::{
    BackendError, CreateAction, Credential, CredentialSecret, DirectoryEntry, Disposition,
    FileInfo, InfoChange, ListPage, LockRange, Lookup, ObjectKind, OpenGrant, OpenRequest,
    Revocation, Scope, ShareTarget, SmbBackend, TreeGrant, WriteRequest,
};
use crate::crypto::{
    self, CIPHER_AES128_GCM, CIPHER_AES256_GCM, Cipher, HASH_SHA512, PreauthHash, SIGNING_CMAC,
    SIGNING_GMAC, Sealer, SessionKeys, Signing,
};
use crate::ntlm::{self, SessionSecurity};
use crate::server::{Shared, SmbConfig, serve_stream};
use crate::spnego::{self, ClientToken, NTLMSSP_OID};
use crate::status;
use crate::wire::{self, Command, FileId, HEADER_SIZE, Header, utf16_bytes};

pub(crate) const BUCKET: &str = "data";
pub(crate) const ACCESS_KEY: &str = "AKIA-tester";
pub(crate) const SECRET: &str = "correct horse battery staple";
const SESSION_KEY: [u8; 16] = [0x55; 16];

pub(crate) fn principal(number: u128) -> UserId {
    UserId::local(Ulid::from(number), RealmId([1u8; 32]))
}

pub(crate) fn info_of(entry: &MockEntry) -> FileInfo {
    FileInfo {
        kind: entry.kind,
        size: entry.data.len() as u64,
        allocation: (entry.data.len() as u64).next_multiple_of(4096),
        created: 1,
        accessed: 2,
        written: 3,
        changed: 4,
        delete_pending: entry.delete_pending,
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MockEntry {
    pub kind: ObjectKind,
    pub data: Vec<u8>,
    pub delete_pending: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct MockHandle {
    pub path: String,
    pub delete_on_close: bool,
}

pub(crate) struct MockState {
    pub credentials: HashMap<String, (UserId, String)>,
    pub revoked: HashSet<String>,
    pub rights: Rights,
    pub path_rights: HashMap<String, Rights>,
    pub job_views: HashMap<Ulid, String>,
    pub entries: BTreeMap<String, MockEntry>,
    pub handles: HashMap<u64, MockHandle>,
    pub next_handle: u64,
    pub locks: Vec<(u64, LockRange)>,
    pub calls: Vec<String>,
    pub cancelled: Vec<u64>,
    pub page_size: usize,
}

/// An in-memory backend with one bucket named `data` and configurable job views.
pub(crate) struct MockBackend {
    pub state: Mutex<MockState>,
    revocations: broadcast::Sender<Revocation>,
    released: Notify,
}

impl MockBackend {
    pub fn new() -> Arc<Self> {
        let mut credentials = HashMap::new();
        credentials.insert(ACCESS_KEY.to_owned(), (principal(1), SECRET.to_owned()));
        let mut entries = BTreeMap::new();
        entries.insert(
            String::new(),
            MockEntry {
                kind: ObjectKind::Directory,
                data: Vec::new(),
                delete_pending: false,
            },
        );
        let (revocations, _) = broadcast::channel(16);
        Arc::new(Self {
            state: Mutex::new(MockState {
                credentials,
                revoked: HashSet::new(),
                rights: Rights {
                    read: true,
                    write: true,
                    delete: true,
                },
                path_rights: HashMap::new(),
                job_views: HashMap::new(),
                entries,
                handles: HashMap::new(),
                next_handle: 1,
                locks: Vec::new(),
                calls: Vec::new(),
                cancelled: Vec::new(),
                page_size: 100,
            }),
            revocations,
            released: Notify::new(),
        })
    }

    pub fn put(&self, path: &str, kind: ObjectKind, data: &[u8]) {
        self.state.lock().entries.insert(
            path.to_owned(),
            MockEntry {
                kind,
                data: data.to_vec(),
                delete_pending: false,
            },
        );
    }

    pub fn data(&self, path: &str) -> Option<Vec<u8>> {
        self.state
            .lock()
            .entries
            .get(path)
            .map(|entry| entry.data.clone())
    }

    pub fn revoke(&self, access_key: &str) {
        self.state.lock().revoked.insert(access_key.to_owned());
        let _ = self.revocations.send(Revocation {
            access_key: access_key.to_owned(),
        });
    }

    fn rights_for(state: &MockState, path: &str) -> Rights {
        state.path_rights.get(path).copied().unwrap_or(state.rights)
    }

    fn handle_path(state: &MockState, handle: u64) -> Result<String, BackendError> {
        state
            .handles
            .get(&handle)
            .map(|open| open.path.clone())
            .ok_or(BackendError::NotFound)
    }

    fn conflicts(state: &MockState, handle: u64, ranges: &[LockRange]) -> bool {
        ranges.iter().any(|wanted| {
            state.locks.iter().any(|(owner, held)| {
                *owner != handle
                    && (wanted.exclusive || held.exclusive)
                    && wanted.offset < held.offset.saturating_add(held.length)
                    && held.offset < wanted.offset.saturating_add(wanted.length)
            })
        })
    }
}

fn parent_of(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(parent, _)| parent)
}

impl SmbBackend for MockBackend {
    async fn lookup_secret(
        &self,
        access_key: &str,
    ) -> Result<Option<CredentialSecret>, BackendError> {
        Ok(self
            .state
            .lock()
            .credentials
            .get(access_key)
            .map(|(principal, secret)| CredentialSecret {
                principal: *principal,
                secret: Zeroizing::new(secret.clone()),
            }))
    }

    async fn admit(&self, credential: &Credential) -> Result<(), BackendError> {
        if self.state.lock().revoked.contains(&credential.access_key) {
            return Err(BackendError::Revoked);
        }
        Ok(())
    }

    async fn connect_tree(
        &self,
        credential: &Credential,
        share: &ShareTarget,
    ) -> Result<TreeGrant, BackendError> {
        let state = self.state.lock();
        match share {
            ShareTarget::Bucket(name) if name == BUCKET => Ok(TreeGrant {
                read_only: false,
                capacity: 1 << 40,
                available: 1 << 39,
            }),
            ShareTarget::Bucket(_) => Err(BackendError::NotFound),
            ShareTarget::JobView(job) => {
                if state.job_views.get(job) == Some(&credential.access_key) {
                    Ok(TreeGrant {
                        read_only: true,
                        capacity: 1 << 30,
                        available: 0,
                    })
                } else {
                    Err(BackendError::Denied)
                }
            }
        }
    }

    async fn lookup(&self, _scope: &Scope, path: &str) -> Result<Lookup, BackendError> {
        let state = self.state.lock();
        let kind = state.entries.get(path).map(|entry| entry.kind);
        if kind.is_none() && !state.entries.contains_key(parent_of(path)) {
            return Err(BackendError::PathNotFound);
        }
        Ok(Lookup {
            kind,
            rights: Self::rights_for(&state, path),
        })
    }

    async fn open(&self, _scope: &Scope, request: OpenRequest) -> Result<OpenGrant, BackendError> {
        let mut state = self.state.lock();
        let action = match state.entries.get_mut(&request.path) {
            Some(entry) => {
                if request.kind.is_some_and(|kind| kind != entry.kind) {
                    return Err(if entry.kind == ObjectKind::Directory {
                        BackendError::IsDirectory
                    } else {
                        BackendError::NotDirectory
                    });
                }
                match request.disposition {
                    Disposition::Create => return Err(BackendError::Exists),
                    Disposition::Supersede => {
                        entry.data.clear();
                        CreateAction::Superseded
                    }
                    Disposition::Overwrite | Disposition::OverwriteIf => {
                        entry.data.clear();
                        CreateAction::Overwritten
                    }
                    Disposition::Open | Disposition::OpenIf => CreateAction::Opened,
                }
            }
            None => {
                if !request.disposition.creates() {
                    return Err(BackendError::NotFound);
                }
                let parent = parent_of(&request.path).to_owned();
                if state.entries.get(&parent).map(|entry| entry.kind) != Some(ObjectKind::Directory)
                {
                    return Err(BackendError::PathNotFound);
                }
                state.entries.insert(
                    request.path.clone(),
                    MockEntry {
                        kind: request.kind.unwrap_or(ObjectKind::File),
                        data: Vec::new(),
                        delete_pending: false,
                    },
                );
                CreateAction::Created
            }
        };
        let handle = state.next_handle;
        state.next_handle += 1;
        state.handles.insert(
            handle,
            MockHandle {
                path: request.path.clone(),
                delete_on_close: request.delete_on_close,
            },
        );
        state.calls.push(format!("open {}", request.path));
        let info = info_of(&state.entries[&request.path]);
        Ok(OpenGrant {
            handle,
            action,
            info,
        })
    }

    async fn close(&self, _scope: &Scope, handle: u64) -> Result<FileInfo, BackendError> {
        let mut state = self.state.lock();
        let open = state
            .handles
            .remove(&handle)
            .ok_or(BackendError::NotFound)?;
        state.locks.retain(|(owner, _)| *owner != handle);
        state.calls.push(format!("close {}", open.path));
        let entry = state.entries.get(&open.path).cloned();
        let info = entry.as_ref().map(info_of).ok_or(BackendError::NotFound)?;
        let last = !state.handles.values().any(|other| other.path == open.path);
        if last && (open.delete_on_close || entry.is_some_and(|entry| entry.delete_pending)) {
            state.entries.remove(&open.path);
        }
        drop(state);
        self.released.notify_waiters();
        Ok(info)
    }

    async fn read(
        &self,
        _scope: &Scope,
        handle: u64,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, BackendError> {
        let state = self.state.lock();
        let path = Self::handle_path(&state, handle)?;
        let data = &state.entries.get(&path).ok_or(BackendError::NotFound)?.data;
        let start = (offset as usize).min(data.len());
        let end = start.saturating_add(length as usize).min(data.len());
        Ok(Bytes::copy_from_slice(&data[start..end]))
    }

    async fn write(
        &self,
        _scope: &Scope,
        handle: u64,
        request: WriteRequest,
    ) -> Result<u32, BackendError> {
        let mut state = self.state.lock();
        let path = Self::handle_path(&state, handle)?;
        let entry = state.entries.get_mut(&path).ok_or(BackendError::NotFound)?;
        let end_of_file = entry.data.len() as u64;
        let offset = request.offset.unwrap_or(end_of_file);
        if request.mode == WriteMode::Append && !append_allowed(offset, end_of_file) {
            return Err(BackendError::Denied);
        }
        let start = offset as usize;
        let end = start + request.data.len();
        if entry.data.len() < end {
            entry.data.resize(end, 0);
        }
        entry.data[start..end].copy_from_slice(&request.data);
        state.calls.push(format!("write {path} {offset}"));
        Ok(request.data.len() as u32)
    }

    async fn flush(&self, _scope: &Scope, handle: u64) -> Result<(), BackendError> {
        let mut state = self.state.lock();
        let path = Self::handle_path(&state, handle)?;
        state.calls.push(format!("flush {path}"));
        Ok(())
    }

    async fn query_info(&self, _scope: &Scope, handle: u64) -> Result<FileInfo, BackendError> {
        let state = self.state.lock();
        let path = Self::handle_path(&state, handle)?;
        state
            .entries
            .get(&path)
            .map(info_of)
            .ok_or(BackendError::NotFound)
    }

    async fn set_info(
        &self,
        _scope: &Scope,
        handle: u64,
        change: InfoChange,
    ) -> Result<(), BackendError> {
        let mut state = self.state.lock();
        let path = Self::handle_path(&state, handle)?;
        let entry = state.entries.get_mut(&path).ok_or(BackendError::NotFound)?;
        if let InfoChange::EndOfFile(length) = change {
            entry.data.resize(length as usize, 0);
        }
        state.calls.push(format!("set {path} {change:?}"));
        Ok(())
    }

    async fn rename(
        &self,
        _scope: &Scope,
        handle: u64,
        target: &str,
        replace: bool,
    ) -> Result<(), BackendError> {
        let mut state = self.state.lock();
        let source = Self::handle_path(&state, handle)?;
        state.calls.push(format!("rename {source} {target}"));
        if source == target {
            return Ok(());
        }
        if state.entries.contains_key(target) && !replace {
            return Err(BackendError::Exists);
        }
        if !state.entries.contains_key(parent_of(target)) {
            return Err(BackendError::PathNotFound);
        }
        let entry = state
            .entries
            .remove(&source)
            .ok_or(BackendError::NotFound)?;
        state.entries.insert(target.to_owned(), entry);
        if let Some(open) = state.handles.get_mut(&handle) {
            open.path = target.to_owned();
        }
        Ok(())
    }

    async fn set_disposition(
        &self,
        _scope: &Scope,
        handle: u64,
        delete: bool,
    ) -> Result<(), BackendError> {
        let mut state = self.state.lock();
        let path = Self::handle_path(&state, handle)?;
        state.calls.push(format!("disposition {path} {delete}"));
        let entry = state.entries.get_mut(&path).ok_or(BackendError::NotFound)?;
        entry.delete_pending = delete;
        Ok(())
    }

    async fn list(
        &self,
        _scope: &Scope,
        handle: u64,
        cursor: Option<String>,
    ) -> Result<ListPage, BackendError> {
        let mut state = self.state.lock();
        let path = Self::handle_path(&state, handle)?;
        let prefix = if path.is_empty() {
            String::new()
        } else {
            format!("{path}/")
        };
        let names: Vec<(String, FileInfo)> = state
            .entries
            .iter()
            .filter_map(|(key, entry)| {
                let name = key.strip_prefix(&prefix)?;
                (!key.is_empty() && !name.is_empty() && !name.contains('/'))
                    .then(|| (name.to_owned(), info_of(entry)))
            })
            .filter(|(name, _)| cursor.as_ref().is_none_or(|after| name > after))
            .collect();
        let page: Vec<DirectoryEntry> = names
            .iter()
            .take(state.page_size)
            .map(|(name, info)| DirectoryEntry {
                name: name.clone(),
                info: *info,
            })
            .collect();
        let next = (names.len() > page.len())
            .then(|| page.last().map(|entry| entry.name.clone()))
            .flatten();
        state.calls.push(format!("list {path}"));
        Ok(ListPage {
            entries: page,
            next,
        })
    }

    async fn lock(
        &self,
        _scope: &Scope,
        handle: u64,
        ranges: Vec<LockRange>,
        wait: bool,
    ) -> Result<(), BackendError> {
        loop {
            let released = self.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            {
                let mut state = self.state.lock();
                if !Self::conflicts(&state, handle, &ranges) {
                    for range in &ranges {
                        state.locks.push((handle, *range));
                    }
                    return Ok(());
                }
                if !wait {
                    return Err(BackendError::LockRefused);
                }
            }
            released.await;
        }
    }

    async fn unlock(
        &self,
        _scope: &Scope,
        handle: u64,
        ranges: Vec<LockRange>,
    ) -> Result<(), BackendError> {
        {
            let mut state = self.state.lock();
            for range in &ranges {
                let position = state
                    .locks
                    .iter()
                    .position(|(owner, held)| {
                        *owner == handle
                            && held.offset == range.offset
                            && held.length == range.length
                    })
                    .ok_or(BackendError::NotLocked)?;
                state.locks.remove(position);
            }
        }
        self.released.notify_waiters();
        Ok(())
    }

    async fn cancel(&self, _scope: &Scope, handle: u64) {
        self.state.lock().cancelled.push(handle);
    }

    fn revocations(&self) -> broadcast::Receiver<Revocation> {
        self.revocations.subscribe()
    }
}

/// Limits generous enough that no test hits them by accident.
pub(crate) fn test_limits() -> Arc<RateLimits> {
    Arc::new(RateLimits::new(60_000, 10_000, 60_000, 10_000))
}

pub(crate) fn tight_limits(burst: u32) -> Arc<RateLimits> {
    Arc::new(
        RateLimits::new(60_000, 10_000, 60_000, 10_000).with_vfs_limits(VfsLimits {
            peer_connections: 4,
            principal_operations: 64,
            handshake_per_minute: 1,
            handshake_burst: burst,
        }),
    )
}

/// One running in-process connection and the tokens that stop it.
pub(crate) struct Harness {
    pub shutdown: CancellationToken,
    pub abort: CancellationToken,
    pub task: JoinHandle<()>,
}

pub(crate) fn start(
    backend: Arc<MockBackend>,
    config: SmbConfig,
    limits: Arc<RateLimits>,
) -> (TestClient, Harness) {
    let (client, server) = tokio::io::duplex(1 << 22);
    let shared = Arc::new(Shared::new(config, limits).expect("random source works"));
    let shutdown = CancellationToken::new();
    let abort = CancellationToken::new();
    let task = tokio::spawn(serve_stream(
        backend,
        shared,
        server,
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        shutdown.clone(),
        abort.clone(),
    ));
    (
        TestClient::new(client),
        Harness {
            shutdown,
            abort,
            task,
        },
    )
}

/// A connection logged in as the default credential with the bucket share connected.
pub(crate) async fn logged_in() -> (TestClient, Harness, Arc<MockBackend>) {
    let backend = MockBackend::new();
    let (mut client, harness) = start(backend.clone(), SmbConfig::default(), test_limits());
    client.login().await;
    (client, harness, backend)
}

/// One decoded response.
#[derive(Clone, Debug)]
pub(crate) struct Response {
    pub header: Header,
    pub raw: Vec<u8>,
}

impl Response {
    pub fn status(&self) -> u32 {
        self.header.status
    }

    pub fn body(&self) -> &[u8] {
        &self.raw[HEADER_SIZE..]
    }

    /// The FileId of a CREATE response.
    pub fn file_id(&self) -> FileId {
        let mut reader = wire::Reader::new(&self.body()[64..80]);
        FileId::read(&mut reader).expect("CREATE response carries a FileId")
    }

    /// The output buffer of a QUERY_INFO or QUERY_DIRECTORY response.
    pub fn output(&self) -> Vec<u8> {
        let body = self.body();
        let length = u32::from_le_bytes([body[4], body[5], body[6], body[7]]) as usize;
        body[8..8 + length].to_vec()
    }

    /// The data of a READ response.
    pub fn data(&self) -> Vec<u8> {
        let body = self.body();
        let length = u32::from_le_bytes([body[4], body[5], body[6], body[7]]) as usize;
        body[16..16 + length].to_vec()
    }
}

/// How the client wraps its NTLM tokens.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Wrapping {
    /// SPNEGO with the NTLM MIC and mechListMIC.
    Spnego,
    /// SPNEGO without either MIC, like a legacy client.
    Bare,
    Raw,
}

pub(crate) struct ClientKeys {
    pub keys: SessionKeys,
    pub sealer: Sealer,
}

/// An SMB 3.1.1 client over the test duplex stream.
pub(crate) struct TestClient {
    pub stream: DuplexStream,
    pub next_message: u64,
    pub preauth: PreauthHash,
    pub session_id: u64,
    pub tree_id: u32,
    pub cipher: Cipher,
    pub signing: Signing,
    pub keys: Option<ClientKeys>,
    pub server_mic: Option<Vec<u8>>,
}

pub(crate) fn negotiate_request(dialects: &[u16], contexts: &[(u16, Vec<u8>)]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&36u16.to_le_bytes());
    body.extend_from_slice(&(dialects.len() as u16).to_le_bytes());
    body.extend_from_slice(&1u16.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&0x7fu32.to_le_bytes());
    body.extend_from_slice(&[0x11; 16]);
    let offset_at = body.len();
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&(contexts.len() as u16).to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    for dialect in dialects {
        body.extend_from_slice(&dialect.to_le_bytes());
    }
    if !contexts.is_empty() {
        while !(HEADER_SIZE + body.len()).is_multiple_of(8) {
            body.push(0);
        }
        let offset = (HEADER_SIZE + body.len()) as u32;
        body[offset_at..offset_at + 4].copy_from_slice(&offset.to_le_bytes());
        for (index, (kind, data)) in contexts.iter().enumerate() {
            if index > 0 {
                while !(HEADER_SIZE + body.len()).is_multiple_of(8) {
                    body.push(0);
                }
            }
            body.extend_from_slice(&kind.to_le_bytes());
            body.extend_from_slice(&(data.len() as u16).to_le_bytes());
            body.extend_from_slice(&0u32.to_le_bytes());
            body.extend_from_slice(data);
        }
    }
    body
}

pub(crate) fn list_context(kind: u16, values: &[u16]) -> (u16, Vec<u8>) {
    let mut data = (values.len() as u16).to_le_bytes().to_vec();
    if kind == 0x0001 {
        data.extend_from_slice(&32u16.to_le_bytes());
    }
    for value in values {
        data.extend_from_slice(&value.to_le_bytes());
    }
    if kind == 0x0001 {
        data.extend_from_slice(&[0x22; 32]);
    }
    (kind, data)
}

pub(crate) fn default_contexts() -> Vec<(u16, Vec<u8>)> {
    vec![
        list_context(0x0001, &[HASH_SHA512]),
        list_context(0x0002, &[CIPHER_AES256_GCM, CIPHER_AES128_GCM]),
        list_context(0x0008, &[SIGNING_GMAC, SIGNING_CMAC]),
    ]
}

/// The cipher and signing algorithm a NEGOTIATE response selected.
pub(crate) fn negotiated_choice(raw: &[u8]) -> (Cipher, Signing) {
    let body = &raw[HEADER_SIZE..];
    let count = u16::from_le_bytes([body[6], body[7]]) as usize;
    let mut position = u32::from_le_bytes([body[60], body[61], body[62], body[63]]) as usize;
    let (mut cipher, mut signing) = (Cipher::Aes128Gcm, Signing::AesCmac);
    for index in 0..count {
        if index > 0 {
            position = position.next_multiple_of(8);
        }
        let kind = u16::from_le_bytes([raw[position], raw[position + 1]]);
        let length = u16::from_le_bytes([raw[position + 2], raw[position + 3]]) as usize;
        let data = &raw[position + 8..position + 8 + length];
        let first = u16::from_le_bytes([data[2], data[3]]);
        match kind {
            0x0002 => cipher = Cipher::from_code(first).expect("a GCM cipher"),
            0x0008 => signing = Signing::from_code(first).expect("a signing algorithm"),
            _ => {}
        }
        position += 8 + length;
    }
    (cipher, signing)
}

pub(crate) fn ntlm_negotiate() -> Vec<u8> {
    let flags = ntlm::REQUIRED
        | ntlm::KEY_EXCHANGE
        | ntlm::REQUEST_TARGET
        | ntlm::NTLM
        | ntlm::ALWAYS_SIGN
        | ntlm::SIGN
        | ntlm::SEAL
        | ntlm::VERSION
        | ntlm::NEGOTIATE_56;
    let mut out = ntlm::SIGNATURE.to_vec();
    out.extend_from_slice(&1u32.to_le_bytes());
    out.extend_from_slice(&flags.to_le_bytes());
    for _ in 0..2 {
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&40u32.to_le_bytes());
    }
    out.extend_from_slice(&[10, 0, 0x61, 0x4a, 0, 0, 0, 15]);
    out
}

/// Options of a generated AUTHENTICATE message.
#[derive(Clone, Copy, Debug)]
pub(crate) struct AuthOptions<'a> {
    pub user: &'a str,
    pub secret: &'a str,
    pub mic: bool,
    pub legacy: bool,
}

/// Builds an AUTHENTICATE message answering `challenge`; returns it with the exported
/// session key.
pub(crate) fn ntlm_authenticate(
    negotiate: &[u8],
    challenge: &[u8],
    options: AuthOptions<'_>,
) -> (Vec<u8>, [u8; 16]) {
    let server_challenge: [u8; 8] = challenge[24..32].try_into().expect("challenge bytes");
    let info_length = u16::from_le_bytes([challenge[40], challenge[41]]) as usize;
    let info_offset =
        u32::from_le_bytes([challenge[44], challenge[45], challenge[46], challenge[47]]) as usize;
    let target_info = &challenge[info_offset..info_offset + info_length];
    let pairs = ntlm::av_pairs(target_info).expect("target info parses");
    let mut timestamp = [0u8; 8];
    let mut info = Vec::new();
    for (id, value) in &pairs {
        if *id == ntlm::AV_TIMESTAMP {
            timestamp.copy_from_slice(value);
        }
        info.extend_from_slice(&id.to_le_bytes());
        info.extend_from_slice(&(value.len() as u16).to_le_bytes());
        info.extend_from_slice(value);
    }
    if options.mic {
        info.extend_from_slice(&ntlm::AV_FLAGS.to_le_bytes());
        info.extend_from_slice(&4u16.to_le_bytes());
        info.extend_from_slice(&ntlm::MIC_PRESENT.to_le_bytes());
    }
    info.extend_from_slice(&[0u8; 4]);
    let domain = "WORKGROUP";
    let mut blob = vec![1u8, 1, 0, 0, 0, 0, 0, 0];
    blob.extend_from_slice(&timestamp);
    blob.extend_from_slice(&[0xaa; 8]);
    blob.extend_from_slice(&[0u8; 4]);
    blob.extend_from_slice(&info);
    blob.extend_from_slice(&[0u8; 4]);
    let hash = ntlm::nt_hash(options.secret);
    let key = ntlm::response_key(&hash, options.user, domain).expect("key derivation");
    let proof = ntlm::hmac_md5(key.as_slice(), &[&server_challenge, &blob]).expect("proof");
    let nt_response = if options.legacy {
        vec![0x42; 24]
    } else {
        let mut response = proof.to_vec();
        response.extend_from_slice(&blob);
        response
    };
    let base = ntlm::hmac_md5(key.as_slice(), &[&proof]).expect("base key");
    let mut encrypted = SESSION_KEY;
    ntlm::rc4_apply(&base, &mut encrypted).expect("rc4");
    let flags = u32::from_le_bytes([challenge[20], challenge[21], challenge[22], challenge[23]]);
    let payloads = [
        vec![0u8; 24],
        nt_response,
        utf16_bytes(domain),
        utf16_bytes(options.user),
        utf16_bytes("CLIENT"),
        encrypted.to_vec(),
    ];
    let mut out = ntlm::SIGNATURE.to_vec();
    out.extend_from_slice(&3u32.to_le_bytes());
    let mut offset = 88u32;
    for payload in &payloads {
        out.extend_from_slice(&(payload.len() as u16).to_le_bytes());
        out.extend_from_slice(&(payload.len() as u16).to_le_bytes());
        out.extend_from_slice(&offset.to_le_bytes());
        offset += payload.len() as u32;
    }
    out.extend_from_slice(&flags.to_le_bytes());
    out.extend_from_slice(&[10, 0, 0x61, 0x4a, 0, 0, 0, 15]);
    out.extend_from_slice(&[0u8; 16]);
    for payload in &payloads {
        out.extend_from_slice(payload);
    }
    if options.mic {
        let mic = ntlm::hmac_md5(&SESSION_KEY, &[negotiate, challenge, &out]).expect("mic");
        out[72..88].copy_from_slice(&mic);
    }
    (out, SESSION_KEY)
}

impl TestClient {
    pub fn new(stream: DuplexStream) -> Self {
        Self {
            stream,
            next_message: 0,
            preauth: PreauthHash::default(),
            session_id: 0,
            tree_id: 0,
            cipher: Cipher::Aes128Gcm,
            signing: Signing::AesCmac,
            keys: None,
            server_mic: None,
        }
    }

    pub fn header(&mut self, command: Command, charge: u16) -> Header {
        let header = Header {
            credit_charge: charge,
            command: command.code(),
            credits: 64,
            message_id: self.next_message,
            session_id: self.session_id,
            tree_id: self.tree_id,
            ..Header::default()
        };
        self.next_message += u64::from(charge.max(1));
        header
    }

    pub fn message(header: &Header, body: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_SIZE + body.len());
        header.encode(&mut out);
        out.extend_from_slice(body);
        out
    }

    pub async fn send_raw(&mut self, message: &[u8]) {
        let framed = wire::frame(message).expect("frame fits");
        self.stream.write_all(&framed).await.expect("send");
    }

    /// The next frame, or `None` once the server closed the connection.
    pub async fn receive_raw(&mut self) -> Option<Vec<u8>> {
        wire::read_frame(&mut self.stream, wire::TRANSPORT_LIMIT)
            .await
            .ok()
            .flatten()
    }

    /// The next response, decrypted when the session is encrypted.
    pub async fn receive(&mut self) -> Option<Response> {
        let raw = self.receive_raw().await?;
        let raw = if raw.starts_with(&wire::TRANSFORM_ID) {
            let keys = self.keys.as_ref().expect("encrypted response needs keys");
            crypto::unseal(self.cipher, &keys.keys.encryption, &raw).expect("response decrypts")
        } else {
            raw
        };
        let header = Header::decode(&raw).expect("response header");
        Some(Response { header, raw })
    }

    pub async fn plain(&mut self, command: Command, body: &[u8]) -> Response {
        let header = self.header(command, 1);
        let message = Self::message(&header, body);
        self.send_raw(&message).await;
        self.receive().await.expect("response")
    }

    pub async fn negotiate_with(
        &mut self,
        dialects: &[u16],
        contexts: &[(u16, Vec<u8>)],
    ) -> Option<Response> {
        let header = self.header(Command::Negotiate, 1);
        let message = Self::message(&header, &negotiate_request(dialects, contexts));
        self.preauth.update(&message);
        self.send_raw(&message).await;
        let response = self.receive().await?;
        if response.status() == status::SUCCESS {
            self.preauth.update(&response.raw);
            let (cipher, signing) = negotiated_choice(&response.raw);
            self.cipher = cipher;
            self.signing = signing;
        }
        Some(response)
    }

    pub async fn negotiate(&mut self) -> Response {
        self.negotiate_with(
            &[0x0202, 0x0210, 0x0300, 0x0302, 0x0311],
            &default_contexts(),
        )
        .await
        .expect("negotiate response")
    }

    async fn setup_leg(&mut self, security: &[u8], session: &mut PreauthHash) -> Response {
        let header = self.header(Command::SessionSetup, 1);
        let mut body = Vec::new();
        body.extend_from_slice(&25u16.to_le_bytes());
        body.push(0);
        body.push(1);
        body.extend_from_slice(&0u32.to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes());
        body.extend_from_slice(&((HEADER_SIZE + 24) as u16).to_le_bytes());
        body.extend_from_slice(&(security.len() as u16).to_le_bytes());
        body.extend_from_slice(&0u64.to_le_bytes());
        body.extend_from_slice(security);
        let message = Self::message(&header, &body);
        session.update(&message);
        self.send_raw(&message).await;
        let response = self.receive().await.expect("setup response");
        if response.status() == status::MORE_PROCESSING {
            session.update(&response.raw);
            self.session_id = response.header.session_id;
        }
        response
    }

    fn security_buffer(response: &Response) -> Vec<u8> {
        let body = response.body();
        let offset = u16::from_le_bytes([body[4], body[5]]) as usize;
        let length = u16::from_le_bytes([body[6], body[7]]) as usize;
        if length == 0 {
            return Vec::new();
        }
        response.raw[offset..offset + length].to_vec()
    }

    /// Runs SESSION_SETUP to completion and returns the final response; on success the
    /// client keeps the derived keys and checks the server's signature.
    pub async fn setup(&mut self, user: &str, secret: &str, wrapping: Wrapping) -> Response {
        let mut session = self.preauth.clone();
        let negotiate = ntlm_negotiate();
        let first = match wrapping {
            Wrapping::Raw => negotiate.clone(),
            _ => spnego::encode_init(&[NTLMSSP_OID], Some(negotiate.as_slice())),
        };
        let response = self.setup_leg(&first, &mut session).await;
        if response.status() != status::MORE_PROCESSING {
            return response;
        }
        let token = Self::security_buffer(&response);
        let challenge = match wrapping {
            Wrapping::Raw => token,
            _ => match spnego::parse_client(&token).expect("server token parses") {
                ClientToken::Response(reply) => reply.token.expect("challenge token"),
                other => panic!("unexpected server token {other:?}"),
            },
        };
        let (auth, session_key) = ntlm_authenticate(
            &negotiate,
            &challenge,
            AuthOptions {
                user,
                secret,
                mic: wrapping != Wrapping::Bare,
                legacy: false,
            },
        );
        let list = spnego::ntlm_list();
        let mut security = SessionSecurity::new(&session_key).expect("ntlm security");
        let second = match wrapping {
            Wrapping::Raw => auth,
            Wrapping::Bare => spnego::encode_response(None, false, Some(auth.as_slice()), None),
            Wrapping::Spnego => {
                let mic = security.client_mic(0, &list).expect("client mic");
                spnego::encode_response(None, false, Some(auth.as_slice()), Some(mic.as_slice()))
            }
        };
        let response = self.setup_leg(&second, &mut session).await;
        if response.status() != status::SUCCESS {
            return response;
        }
        self.session_id = response.header.session_id;
        let keys = SessionKeys::derive(&session_key, session.value(), self.cipher)
            .expect("key derivation");
        assert!(
            crypto::verify(self.signing, &keys.signing, &response.raw).expect("verify runs"),
            "the final SESSION_SETUP response is signed with the derived key"
        );
        if wrapping != Wrapping::Raw {
            let token = Self::security_buffer(&response);
            if let Ok(ClientToken::Response(reply)) = spnego::parse_client(&token)
                && let Some(mic) = reply.list_mic
            {
                let expected = security.server_mic(0, &list).expect("server mic");
                assert_eq!(mic, expected.to_vec(), "server mechListMIC verifies");
                self.server_mic = Some(mic);
            }
        }
        let sealer = Sealer::new(self.cipher, keys.decryption.clone());
        self.keys = Some(ClientKeys { keys, sealer });
        response
    }

    /// Negotiates, authenticates as the default credential and connects the bucket share.
    pub async fn login(&mut self) {
        self.negotiate().await;
        let response = self.setup(ACCESS_KEY, SECRET, Wrapping::Spnego).await;
        assert_eq!(response.status(), status::SUCCESS);
        let tree = self.connect(BUCKET).await;
        assert_eq!(tree.status(), status::SUCCESS);
        self.tree_id = tree.header.tree_id;
    }

    pub fn seal(&mut self, message: &[u8]) -> Vec<u8> {
        let session_id = self.session_id;
        let keys = self.keys.as_mut().expect("sealing needs keys");
        keys.sealer.seal(session_id, message).expect("seal")
    }

    pub async fn send_sealed(&mut self, message: &[u8]) {
        let packet = self.seal(message);
        self.send_raw(&packet).await;
    }

    /// Sends one encrypted request and returns the first response to it.
    pub async fn call(&mut self, command: Command, body: &[u8]) -> Response {
        self.call_charged(command, 1, body).await
    }

    pub async fn call_charged(&mut self, command: Command, charge: u16, body: &[u8]) -> Response {
        let header = self.header(command, charge);
        let message = Self::message(&header, body);
        self.send_sealed(&message).await;
        self.receive().await.expect("response")
    }

    pub async fn connect(&mut self, share: &str) -> Response {
        let path = utf16_bytes(&format!("\\\\server\\{share}"));
        let mut body = Vec::new();
        body.extend_from_slice(&9u16.to_le_bytes());
        body.extend_from_slice(&0u16.to_le_bytes());
        body.extend_from_slice(&((HEADER_SIZE + 8) as u16).to_le_bytes());
        body.extend_from_slice(&(path.len() as u16).to_le_bytes());
        body.extend_from_slice(&path);
        self.call(Command::TreeConnect, &body).await
    }

    pub async fn create(
        &mut self,
        name: &str,
        desired: u32,
        disposition: u32,
        options: u32,
    ) -> Response {
        self.call(
            Command::Create,
            &create_body(name, desired, disposition, options),
        )
        .await
    }

    pub async fn close(&mut self, file_id: FileId) -> Response {
        self.call(Command::Close, &close_body(file_id, 0)).await
    }
}

pub(crate) fn create_body(name: &str, desired: u32, disposition: u32, options: u32) -> Vec<u8> {
    let name = utf16_bytes(name);
    let mut body = Vec::new();
    body.extend_from_slice(&57u16.to_le_bytes());
    body.push(0);
    body.push(0);
    body.extend_from_slice(&2u32.to_le_bytes());
    body.extend_from_slice(&[0u8; 16]);
    body.extend_from_slice(&desired.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&7u32.to_le_bytes());
    body.extend_from_slice(&disposition.to_le_bytes());
    body.extend_from_slice(&options.to_le_bytes());
    body.extend_from_slice(&((HEADER_SIZE + 56) as u16).to_le_bytes());
    body.extend_from_slice(&(name.len() as u16).to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    if name.is_empty() {
        body.push(0);
    } else {
        body.extend_from_slice(&name);
    }
    body
}

pub(crate) fn close_body(file_id: FileId, flags: u16) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&24u16.to_le_bytes());
    body.extend_from_slice(&flags.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    file_id.write(&mut body);
    body
}

pub(crate) fn read_body(file_id: FileId, offset: u64, length: u32) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&49u16.to_le_bytes());
    body.push(0x50);
    body.push(0);
    body.extend_from_slice(&length.to_le_bytes());
    body.extend_from_slice(&offset.to_le_bytes());
    file_id.write(&mut body);
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    body.push(0);
    body
}

pub(crate) fn write_body(file_id: FileId, offset: u64, data: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&49u16.to_le_bytes());
    body.extend_from_slice(&((HEADER_SIZE + 48) as u16).to_le_bytes());
    body.extend_from_slice(&(data.len() as u32).to_le_bytes());
    body.extend_from_slice(&offset.to_le_bytes());
    file_id.write(&mut body);
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    if data.is_empty() {
        body.push(0);
    } else {
        body.extend_from_slice(data);
    }
    body
}

pub(crate) fn flush_body(file_id: FileId) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&24u16.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    file_id.write(&mut body);
    body
}

pub(crate) fn lock_body(file_id: FileId, locks: &[(u64, u64, u32)]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&48u16.to_le_bytes());
    body.extend_from_slice(&(locks.len() as u16).to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    file_id.write(&mut body);
    for (offset, length, flags) in locks {
        body.extend_from_slice(&offset.to_le_bytes());
        body.extend_from_slice(&length.to_le_bytes());
        body.extend_from_slice(&flags.to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes());
    }
    body
}

pub(crate) fn query_body(
    info_type: u8,
    file_class: u8,
    additional: u32,
    file_id: FileId,
    limit: u32,
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&41u16.to_le_bytes());
    body.push(info_type);
    body.push(file_class);
    body.extend_from_slice(&limit.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&additional.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    file_id.write(&mut body);
    body.push(0);
    body
}

pub(crate) fn set_body(info_type: u8, file_class: u8, file_id: FileId, buffer: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&33u16.to_le_bytes());
    body.push(info_type);
    body.push(file_class);
    body.extend_from_slice(&(buffer.len() as u32).to_le_bytes());
    body.extend_from_slice(&((HEADER_SIZE + 32) as u16).to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    file_id.write(&mut body);
    body.extend_from_slice(buffer);
    if buffer.is_empty() {
        body.push(0);
    }
    body
}

pub(crate) fn directory_body(
    file_class: u8,
    flags: u8,
    file_id: FileId,
    pattern: &str,
    limit: u32,
) -> Vec<u8> {
    let pattern = utf16_bytes(pattern);
    let mut body = Vec::new();
    body.extend_from_slice(&33u16.to_le_bytes());
    body.push(file_class);
    body.push(flags);
    body.extend_from_slice(&0u32.to_le_bytes());
    file_id.write(&mut body);
    body.extend_from_slice(&((HEADER_SIZE + 32) as u16).to_le_bytes());
    body.extend_from_slice(&(pattern.len() as u16).to_le_bytes());
    body.extend_from_slice(&limit.to_le_bytes());
    if pattern.is_empty() {
        body.push(0);
    } else {
        body.extend_from_slice(&pattern);
    }
    body
}

/// A FileRenameInformation buffer for `name`.
pub(crate) fn rename_buffer(replace: bool, name: &str) -> Vec<u8> {
    let encoded = utf16_bytes(name);
    let mut out = vec![u8::from(replace), 0, 0, 0, 0, 0, 0, 0];
    out.extend_from_slice(&0u64.to_le_bytes());
    out.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
    out.extend_from_slice(&encoded);
    while out.len() < crate::rename::MINIMUM_SIZE {
        out.push(0);
    }
    out
}
