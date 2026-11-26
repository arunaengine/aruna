//! An in-memory NFS backend for protocol tests: one host and export, a small file tree, share
//! reservations, byte-range locks, a replay gate and a revocation channel.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::Arc;

use aruna_core::structs::identity::realm::RealmId;
use aruna_core::user::id::UserId;
use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::broadcast;
use ulid::Ulid;

use crate::backend::{
    AttributeChange, Caller, ChangeInfo, CreateMode, CreateReply, DirectoryEntry, DirectoryPage,
    HandleStatus, HostIdentity, IoState, LockDenial, LockError, LockKind, LockRange, MountExport,
    NfsBackend, ObjectAttributes, ObjectRef, OwnerKey, ReadReply, ReadSource, ReplayGrant,
    Revocation, ShareRequest, WriteReply,
};
use crate::types::{NfsStat4, NfsTime, NfsType4, StableHow, share};

pub const ROOT: u64 = 1;
pub const DIRECTORY_VERIFIER: [u8; 8] = [9; 8];
pub const WRITE_VERIFIER: [u8; 8] = [5; 8];

pub struct Node {
    pub name: String,
    pub parent: u64,
    pub kind: NfsType4,
    pub data: Vec<u8>,
    pub mode: u32,
    pub change: u64,
    pub verifier: Option<[u8; 8]>,
}

#[derive(Default)]
pub struct MockState {
    pub hosts: HashMap<[u8; 32], HostIdentity>,
    pub blocked: HashSet<IpAddr>,
    pub loopback: Option<HostIdentity>,
    pub exports: HashMap<Ulid, MountExport>,
    pub nodes: HashMap<(Ulid, u64), Node>,
    pub next_object: u64,
    pub expired: HashSet<u64>,
    pub stale: HashSet<u64>,
    pub shares: HashMap<ObjectRef, HashMap<OwnerKey, (u32, u32)>>,
    pub locks: Vec<(ObjectRef, OwnerKey, LockKind, LockRange)>,
    pub replay_denied: bool,
    /// The last grant a replay was checked against.
    pub replayed: Option<ReplayGrant>,
    /// The share each create held for its owner, in order.
    pub created: Vec<ShareRequest>,
    /// The I/O owner of each WRITE: its lock owner when it named one, else its open owner.
    pub writers: Vec<OwnerKey>,
    pub writes: usize,
    pub creates: usize,
    pub closes: usize,
    /// Closes that still fail with DELAY before one ends its open.
    pub close_failures: usize,
}

pub struct MockBackend {
    pub state: Mutex<MockState>,
    pub events: broadcast::Sender<Revocation>,
}

/// The ids a test needs: the enrolled host and its two exports.
#[derive(Clone, Debug)]
pub struct Fixture {
    pub host: HostIdentity,
    pub export: MountExport,
    pub other: MountExport,
}

pub fn user() -> UserId {
    UserId::new(Ulid::from_parts(1, 5), RealmId([1; 32]))
}

fn export_for(host: &HostIdentity, mount_id: Ulid, access_key: &str) -> MountExport {
    MountExport {
        mount_id,
        host_id: host.host_id,
        principal: host.owner,
        access_key: access_key.to_string(),
        incarnation: 7,
        root: ROOT,
        read_only: false,
        persistent: false,
    }
}

fn range_end(range: LockRange) -> u64 {
    if range.length == u64::MAX {
        u64::MAX
    } else {
        range.offset.saturating_add(range.length)
    }
}

fn overlaps(first: LockRange, second: LockRange) -> bool {
    first.offset < range_end(second) && second.offset < range_end(first)
}

impl MockBackend {
    /// A backend with one host pinning `pin` and two exports of it, each with an empty root.
    pub fn with_host(pin: [u8; 32]) -> (Arc<Self>, Fixture) {
        let host = HostIdentity {
            host_id: Ulid::from_parts(2, 1),
            owner: user(),
            fingerprint: Some(pin),
        };
        let export = export_for(&host, Ulid::from_parts(3, 1), "key-one");
        let other = export_for(&host, Ulid::from_parts(3, 2), "key-two");
        let mut state = MockState {
            next_object: ROOT + 1,
            ..MockState::default()
        };
        state.hosts.insert(pin, host);
        for mount in [&export, &other] {
            state.exports.insert(mount.mount_id, mount.clone());
            state.nodes.insert(
                (mount.mount_id, ROOT),
                Node {
                    name: String::new(),
                    parent: ROOT,
                    kind: NfsType4::Directory,
                    data: Vec::new(),
                    mode: 0o755,
                    change: 1,
                    verifier: None,
                },
            );
        }
        let (events, _) = broadcast::channel(16);
        let backend = Arc::new(Self {
            state: Mutex::new(state),
            events,
        });
        let fixture = Fixture {
            host,
            export,
            other,
        };
        (backend, fixture)
    }

    /// Adds a file under the root of `mount_id` and returns its object id.
    pub fn add_file(&self, mount_id: Ulid, name: &str, data: &[u8]) -> u64 {
        let mut state = self.state.lock();
        let id = state.next_object;
        state.next_object += 1;
        state.nodes.insert(
            (mount_id, id),
            Node {
                name: name.to_string(),
                parent: ROOT,
                kind: NfsType4::Regular,
                data: data.to_vec(),
                mode: 0o644,
                change: 1,
                verifier: None,
            },
        );
        id
    }

    fn child(state: &MockState, directory: ObjectRef, name: &str) -> Option<u64> {
        state
            .nodes
            .iter()
            .find(|((mount, id), node)| {
                *mount == directory.mount_id
                    && node.parent == directory.object_id
                    && *id != ROOT
                    && node.name == name
            })
            .map(|((_, id), _)| *id)
    }

    fn attributes_of(node: &Node, fileid: u64) -> ObjectAttributes {
        ObjectAttributes {
            kind: node.kind,
            size: node.data.len() as u64,
            change: node.change,
            fileid,
            mode: node.mode,
            links: 1,
            uid: 1000,
            gid: 1000,
            space_used: node.data.len() as u64,
            accessed: NfsTime::default(),
            modified: NfsTime::default(),
            changed: NfsTime::default(),
        }
    }

    /// Whether an I/O of `need` conflicts with another owner's deny reservation.
    fn denied(state: &MockState, object: ObjectRef, io: &IoState, need: u32) -> bool {
        let own = match io {
            IoState::Bypass => return false,
            IoState::Anonymous => None,
            IoState::Open { owner, .. } => Some(owner),
        };
        state.shares.get(&object).is_some_and(|shares| {
            shares
                .iter()
                .any(|(owner, (_, deny))| Some(owner) != own && deny & need != 0)
        })
    }

    fn conflict(
        state: &MockState,
        object: ObjectRef,
        owner: &OwnerKey,
        kind: LockKind,
        range: LockRange,
    ) -> Option<LockDenial> {
        state
            .locks
            .iter()
            .find(|(held_object, held_owner, held_kind, held_range)| {
                *held_object == object
                    && held_owner != owner
                    && (kind == LockKind::Write || *held_kind == LockKind::Write)
                    && overlaps(*held_range, range)
            })
            .map(|(_, held_owner, held_kind, held_range)| LockDenial {
                range: *held_range,
                kind: *held_kind,
                owner: held_owner.clone(),
            })
    }

    fn insert_child(
        state: &mut MockState,
        directory: ObjectRef,
        name: &str,
        kind: NfsType4,
    ) -> ObjectRef {
        let id = state.next_object;
        state.next_object += 1;
        state.nodes.insert(
            (directory.mount_id, id),
            Node {
                name: name.to_string(),
                parent: directory.object_id,
                kind,
                data: Vec::new(),
                mode: if kind == NfsType4::Directory {
                    0o755
                } else {
                    0o644
                },
                change: 1,
                verifier: None,
            },
        );
        if let Some(parent) = state
            .nodes
            .get_mut(&(directory.mount_id, directory.object_id))
        {
            parent.change += 1;
        }
        ObjectRef {
            mount_id: directory.mount_id,
            object_id: id,
        }
    }
}

impl NfsBackend for MockBackend {
    async fn enrolled_host(&self, fingerprint: [u8; 32], peer: IpAddr) -> Option<HostIdentity> {
        let state = self.state.lock();
        if state.blocked.contains(&peer) {
            return None;
        }
        state.hosts.get(&fingerprint).copied()
    }

    async fn loopback_host(&self, _peer: IpAddr) -> Option<HostIdentity> {
        self.state.lock().loopback
    }

    async fn mount_export(
        &self,
        host: &HostIdentity,
        _peer: IpAddr,
        mount_id: Ulid,
    ) -> Result<MountExport, NfsStat4> {
        self.state
            .lock()
            .exports
            .get(&mount_id)
            .filter(|export| export.host_id == host.host_id)
            .cloned()
            .ok_or(NfsStat4::Stale)
    }

    async fn recheck_replay(&self, grant: &ReplayGrant) -> Result<(), NfsStat4> {
        let mut state = self.state.lock();
        state.replayed = Some(grant.clone());
        if state.replay_denied {
            return Err(NfsStat4::Access);
        }
        Ok(())
    }

    async fn resolve(&self, _caller: &Caller, object: ObjectRef) -> HandleStatus {
        let state = self.state.lock();
        if state.stale.contains(&object.object_id) {
            HandleStatus::Stale
        } else if state.expired.contains(&object.object_id) {
            HandleStatus::Expired
        } else if state
            .nodes
            .contains_key(&(object.mount_id, object.object_id))
        {
            HandleStatus::Live
        } else {
            HandleStatus::Stale
        }
    }

    async fn lookup(
        &self,
        _caller: &Caller,
        directory: ObjectRef,
        name: &str,
    ) -> Result<ObjectRef, NfsStat4> {
        let state = self.state.lock();
        let id = Self::child(&state, directory, name).ok_or(NfsStat4::NoEntry)?;
        Ok(ObjectRef {
            mount_id: directory.mount_id,
            object_id: id,
        })
    }

    async fn parent(&self, _caller: &Caller, object: ObjectRef) -> Result<ObjectRef, NfsStat4> {
        let state = self.state.lock();
        let node = state
            .nodes
            .get(&(object.mount_id, object.object_id))
            .ok_or(NfsStat4::Stale)?;
        Ok(ObjectRef {
            mount_id: object.mount_id,
            object_id: node.parent,
        })
    }

    async fn attributes(
        &self,
        _caller: &Caller,
        object: ObjectRef,
    ) -> Result<ObjectAttributes, NfsStat4> {
        let state = self.state.lock();
        let node = state
            .nodes
            .get(&(object.mount_id, object.object_id))
            .ok_or(NfsStat4::Stale)?;
        Ok(Self::attributes_of(node, object.object_id))
    }

    async fn access(
        &self,
        _caller: &Caller,
        _object: ObjectRef,
        requested: u32,
    ) -> Result<u32, NfsStat4> {
        Ok(requested)
    }

    async fn set_attributes(
        &self,
        _caller: &Caller,
        object: ObjectRef,
        state: &IoState,
        change: &AttributeChange,
    ) -> Result<(), NfsStat4> {
        let mut locked = self.state.lock();
        if change.size.is_some() && Self::denied(&locked, object, state, share::WRITE) {
            return Err(NfsStat4::Locked);
        }
        let node = locked
            .nodes
            .get_mut(&(object.mount_id, object.object_id))
            .ok_or(NfsStat4::Stale)?;
        if let Some(size) = change.size {
            node.data.resize(size as usize, 0);
        }
        if let Some(mode) = change.mode {
            node.mode = mode;
        }
        node.change += 1;
        Ok(())
    }

    async fn read_directory(
        &self,
        _caller: &Caller,
        directory: ObjectRef,
        cookie: u64,
        verifier: [u8; 8],
        limit: usize,
    ) -> Result<DirectoryPage, NfsStat4> {
        if cookie != 0 && verifier != DIRECTORY_VERIFIER {
            return Err(NfsStat4::NotSame);
        }
        let state = self.state.lock();
        let mut children: Vec<(u64, &Node)> = state
            .nodes
            .iter()
            .filter(|((mount, id), node)| {
                *mount == directory.mount_id && node.parent == directory.object_id && *id != ROOT
            })
            .map(|((_, id), node)| (*id, node))
            .collect();
        children.sort_by_key(|(id, _)| *id);
        let after: Vec<(u64, &Node)> = children
            .into_iter()
            .filter(|(id, _)| id + 2 > cookie)
            .collect();
        let eof = after.len() <= limit;
        let entries = after
            .into_iter()
            .take(limit)
            .map(|(id, node)| DirectoryEntry {
                name: node.name.clone(),
                cookie: id + 2,
                object: ObjectRef {
                    mount_id: directory.mount_id,
                    object_id: id,
                },
                attributes: Self::attributes_of(node, id),
            })
            .collect();
        Ok(DirectoryPage {
            entries,
            verifier: DIRECTORY_VERIFIER,
            eof,
        })
    }

    async fn open(
        &self,
        _caller: &Caller,
        object: ObjectRef,
        share: &ShareRequest,
    ) -> Result<(), NfsStat4> {
        let mut state = self.state.lock();
        let node = state
            .nodes
            .get(&(object.mount_id, object.object_id))
            .ok_or(NfsStat4::Stale)?;
        if node.kind == NfsType4::Directory {
            return Err(NfsStat4::IsDirectory);
        }
        let shares = state.shares.entry(object).or_default();
        let conflict = shares.iter().any(|(owner, (access, deny))| {
            *owner != share.owner && (access & share.deny != 0 || deny & share.access != 0)
        });
        if conflict {
            return Err(NfsStat4::ShareDenied);
        }
        shares.insert(share.owner.clone(), (share.access, share.deny));
        Ok(())
    }

    async fn close(
        &self,
        _caller: &Caller,
        object: ObjectRef,
        owner: &OwnerKey,
    ) -> Result<(), NfsStat4> {
        let mut state = self.state.lock();
        state.closes += 1;
        if state.close_failures > 0 {
            state.close_failures -= 1;
            return Err(NfsStat4::Delay);
        }
        if let Some(shares) = state.shares.get_mut(&object) {
            shares.remove(owner);
        }
        Ok(())
    }

    async fn read(
        &self,
        _caller: &Caller,
        object: ObjectRef,
        state: &IoState,
        offset: u64,
        count: u32,
    ) -> Result<ReadReply, NfsStat4> {
        let locked = self.state.lock();
        if Self::denied(&locked, object, state, share::READ) {
            return Err(NfsStat4::Locked);
        }
        let node = locked
            .nodes
            .get(&(object.mount_id, object.object_id))
            .ok_or(NfsStat4::Stale)?;
        if node.kind == NfsType4::Directory {
            return Err(NfsStat4::IsDirectory);
        }
        let start = (offset as usize).min(node.data.len());
        let end = start.saturating_add(count as usize).min(node.data.len());
        Ok(ReadReply {
            data: Bytes::copy_from_slice(&node.data[start..end]),
            eof: end == node.data.len(),
            source: Some(ReadSource {
                key: node.name.clone(),
                version_id: None,
                instance_id: Some(object.object_id),
                base_ranges: Vec::new(),
            }),
        })
    }

    async fn write(
        &self,
        _caller: &Caller,
        object: ObjectRef,
        state: &IoState,
        offset: u64,
        data: Bytes,
        _stable: StableHow,
    ) -> Result<WriteReply, NfsStat4> {
        let mut locked = self.state.lock();
        if Self::denied(&locked, object, state, share::WRITE) {
            return Err(NfsStat4::Locked);
        }
        if let IoState::Open { owner, lock_owner } = state {
            let writer = lock_owner.as_ref().unwrap_or(owner);
            locked.writers.push(writer.clone());
        }
        locked.writes += 1;
        let node = locked
            .nodes
            .get_mut(&(object.mount_id, object.object_id))
            .ok_or(NfsStat4::Stale)?;
        let start = offset as usize;
        if node.data.len() < start + data.len() {
            node.data.resize(start + data.len(), 0);
        }
        node.data[start..start + data.len()].copy_from_slice(&data);
        node.change += 1;
        Ok(WriteReply {
            count: data.len() as u32,
            committed: StableHow::FileSync,
            verifier: WRITE_VERIFIER,
        })
    }

    async fn commit(
        &self,
        _caller: &Caller,
        _object: ObjectRef,
        _offset: u64,
        _count: u32,
    ) -> Result<[u8; 8], NfsStat4> {
        Ok(WRITE_VERIFIER)
    }

    async fn create_file(
        &self,
        _caller: &Caller,
        directory: ObjectRef,
        name: &str,
        mode: CreateMode,
        share: &ShareRequest,
        change: &AttributeChange,
    ) -> Result<CreateReply, NfsStat4> {
        let mut state = self.state.lock();
        state.created.push(share.clone());
        if let Some(id) = Self::child(&state, directory, name) {
            let object = ObjectRef {
                mount_id: directory.mount_id,
                object_id: id,
            };
            let node = state
                .nodes
                .get_mut(&(directory.mount_id, id))
                .ok_or(NfsStat4::Stale)?;
            match mode {
                CreateMode::Guarded => return Err(NfsStat4::Exist),
                CreateMode::Exclusive(verifier) if node.verifier != Some(verifier) => {
                    return Err(NfsStat4::Exist);
                }
                CreateMode::Exclusive(_) => {}
                CreateMode::Unchecked => {
                    if let Some(size) = change.size {
                        node.data.resize(size as usize, 0);
                    }
                }
            }
            return Ok(CreateReply {
                object,
                created: false,
                change: ChangeInfo::default(),
            });
        }
        state.creates += 1;
        let object = Self::insert_child(&mut state, directory, name, NfsType4::Regular);
        if let Some(node) = state.nodes.get_mut(&(object.mount_id, object.object_id)) {
            if let CreateMode::Exclusive(verifier) = mode {
                node.verifier = Some(verifier);
            }
            if let Some(bits) = change.mode {
                node.mode = bits;
            }
        }
        Ok(CreateReply {
            object,
            created: true,
            change: ChangeInfo {
                atomic: true,
                before: 1,
                after: 2,
            },
        })
    }

    async fn create_directory(
        &self,
        _caller: &Caller,
        directory: ObjectRef,
        name: &str,
        _change: &AttributeChange,
    ) -> Result<CreateReply, NfsStat4> {
        let mut state = self.state.lock();
        if Self::child(&state, directory, name).is_some() {
            return Err(NfsStat4::Exist);
        }
        let object = Self::insert_child(&mut state, directory, name, NfsType4::Directory);
        Ok(CreateReply {
            object,
            created: true,
            change: ChangeInfo::default(),
        })
    }

    async fn remove(
        &self,
        _caller: &Caller,
        directory: ObjectRef,
        name: &str,
    ) -> Result<ChangeInfo, NfsStat4> {
        let mut state = self.state.lock();
        let id = Self::child(&state, directory, name).ok_or(NfsStat4::NoEntry)?;
        state.nodes.remove(&(directory.mount_id, id));
        Ok(ChangeInfo::default())
    }

    async fn rename(
        &self,
        _caller: &Caller,
        source: ObjectRef,
        old: &str,
        target: ObjectRef,
        new: &str,
    ) -> Result<(ChangeInfo, ChangeInfo), NfsStat4> {
        let mut state = self.state.lock();
        let id = Self::child(&state, source, old).ok_or(NfsStat4::NoEntry)?;
        if let Some(node) = state.nodes.get_mut(&(source.mount_id, id)) {
            node.parent = target.object_id;
            node.name = new.to_string();
        }
        Ok((ChangeInfo::default(), ChangeInfo::default()))
    }

    async fn lock(
        &self,
        _caller: &Caller,
        object: ObjectRef,
        owner: &OwnerKey,
        kind: LockKind,
        range: LockRange,
    ) -> Result<(), LockError> {
        let mut state = self.state.lock();
        if let Some(denial) = Self::conflict(&state, object, owner, kind, range) {
            return Err(LockError::Denied(denial));
        }
        state.locks.push((object, owner.clone(), kind, range));
        Ok(())
    }

    async fn test_lock(
        &self,
        _caller: &Caller,
        object: ObjectRef,
        owner: &OwnerKey,
        kind: LockKind,
        range: LockRange,
    ) -> Result<(), LockError> {
        let state = self.state.lock();
        match Self::conflict(&state, object, owner, kind, range) {
            Some(denial) => Err(LockError::Denied(denial)),
            None => Ok(()),
        }
    }

    async fn unlock(
        &self,
        _caller: &Caller,
        object: ObjectRef,
        owner: &OwnerKey,
        range: LockRange,
    ) -> Result<bool, NfsStat4> {
        let mut state = self.state.lock();
        state
            .locks
            .retain(|(held_object, held_owner, _, held_range)| {
                !(*held_object == object && held_owner == owner && overlaps(*held_range, range))
            });
        Ok(state
            .locks
            .iter()
            .any(|(held_object, held_owner, _, _)| *held_object == object && held_owner == owner))
    }

    fn revocations(&self) -> broadcast::Receiver<Revocation> {
        self.events.subscribe()
    }
}
