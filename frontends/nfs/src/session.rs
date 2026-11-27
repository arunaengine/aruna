//! Volatile NFSv4.1 clients, sessions and slots: EXCHANGE_ID and CREATE_SESSION sequencing,
//! negotiated channel limits, per-slot replay detection and connection binding per host.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bytes::Bytes;
use tokio::time::Instant;
use ulid::Ulid;

use crate::backend::ReplayGrant;
use crate::rpc::RpcPrincipal;
use crate::types::NfsStat4;

/// Largest slot table one session may negotiate.
pub const SLOT_LIMIT: u32 = 64;
/// Most operations one compound may carry.
pub const OPERATION_LIMIT: u32 = 64;
/// Largest reply a slot keeps for replay, in bytes.
pub const CACHED_LIMIT: u32 = 64 * 1024;
/// RFC 8881 `EXCHGID4_FLAG_*` values this server sets.
pub const CONFIRMED_FLAG: u32 = 0x8000_0000;
pub const NON_PNFS_FLAG: u32 = 0x0001_0000;

/// `channel_attrs4` without the RDMA list, which this TCP server never uses.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ChannelAttributes {
    pub header_pad: u32,
    pub max_request: u32,
    pub max_response: u32,
    pub max_cached: u32,
    pub max_operations: u32,
    pub max_requests: u32,
}

impl ChannelAttributes {
    /// Clamps a client proposal to what this server can hold.
    pub fn negotiate(proposed: &Self, max_request: u32, max_response: u32) -> Self {
        Self {
            header_pad: 0,
            max_request: proposed.max_request.clamp(1024, max_request),
            max_response: proposed.max_response.clamp(1024, max_response),
            max_cached: proposed.max_cached.min(CACHED_LIMIT),
            max_operations: proposed.max_operations.clamp(2, OPERATION_LIMIT),
            max_requests: proposed.max_requests.clamp(1, SLOT_LIMIT),
        }
    }
}

/// The size and length of one compound, checked against the session limits before it runs.
#[derive(Clone, Copy, Debug)]
pub struct RequestShape {
    pub size: usize,
    pub operations: usize,
}

#[derive(Clone, Debug)]
pub struct Client {
    pub client_id: u64,
    pub host_id: Ulid,
    pub owner: Bytes,
    pub verifier: [u8; 8],
    pub principal: RpcPrincipal,
    pub confirmed: bool,
    /// The CREATE_SESSION sequence id the next new session must carry.
    pub sequence: u32,
    pub created: Option<CreatedSession>,
    pub reclaim_complete: bool,
    pub renewed: Instant,
}

/// A CREATE_SESSION result, kept for its replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedSession {
    pub session_id: [u8; 16],
    pub sequence: u32,
    pub flags: u32,
    pub fore: ChannelAttributes,
    pub back: ChannelAttributes,
}

#[derive(Clone, Debug)]
pub enum SlotReply {
    Empty,
    Cached {
        digest: [u8; 32],
        bytes: Bytes,
        grant: Box<ReplayGrant>,
    },
    /// The reply was not kept; a retry gets RETRY_UNCACHED_REP after its SEQUENCE.
    Uncached {
        digest: [u8; 32],
    },
}

#[derive(Clone, Debug)]
pub struct Slot {
    pub sequence: u32,
    pub busy: bool,
    pub reply: SlotReply,
}

#[derive(Clone, Debug)]
pub struct Session {
    pub client_id: u64,
    pub host_id: Ulid,
    pub fore: ChannelAttributes,
    pub slots: Vec<Slot>,
    pub connections: HashSet<u64>,
}

/// What SEQUENCE decided for one slot.
#[derive(Clone, Debug)]
pub enum SlotDecision {
    /// A new request: run it; the slot is busy until `finish`.
    Execute {
        client_id: u64,
        fore: ChannelAttributes,
        highest: u32,
    },
    /// A retry of a cached reply; its grant must be rechecked before release.
    Replay {
        bytes: Bytes,
        grant: Box<ReplayGrant>,
    },
    /// A retry of a reply that was not kept: SEQUENCE succeeds and the next operation gets
    /// RETRY_UNCACHED_REP (RFC 8881 section 2.10.6.1.3).
    Uncached {
        client_id: u64,
        highest: u32,
    },
    Fail(NfsStat4),
}

#[derive(Clone, Debug)]
pub struct ExchangeResult {
    pub client_id: u64,
    pub sequence: u32,
    pub confirmed: bool,
}

/// One SEQUENCE request as the table sees it.
#[derive(Clone, Copy, Debug)]
pub struct SlotRequest {
    pub session: [u8; 16],
    pub slot: u32,
    pub sequence: u32,
    pub highest: u32,
    pub digest: [u8; 32],
    pub shape: RequestShape,
}

/// Who sends a session operation: the enrolled host, its connection and its RPC principal.
#[derive(Clone, Copy, Debug)]
pub struct Sender<'a> {
    pub host_id: Ulid,
    pub connection: u64,
    pub principal: &'a RpcPrincipal,
}

pub struct SessionTable {
    boot: u32,
    next_client: u32,
    clients: HashMap<u64, Client>,
    /// The newest client record of each host and owner; owners never cross hosts.
    owners: HashMap<(Ulid, Bytes), u64>,
    sessions: HashMap<[u8; 16], Session>,
}

impl SessionTable {
    /// `boot` makes client ids of an earlier server instance unknown after a restart.
    pub fn new(boot: u32) -> Self {
        Self {
            boot,
            next_client: 0,
            clients: HashMap::new(),
            owners: HashMap::new(),
            sessions: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub fn client(&self, client_id: u64) -> Option<&Client> {
        self.clients.get(&client_id)
    }

    fn new_client_id(&mut self) -> u64 {
        self.next_client = self.next_client.wrapping_add(1);
        (u64::from(self.boot) << 32) | u64::from(self.next_client)
    }

    /// EXCHANGE_ID: finds or records the client of `owner` on the sending host. A changed
    /// verifier is a client restart and starts a new unconfirmed record.
    pub fn exchange(
        &mut self,
        sender: Sender<'_>,
        owner: Bytes,
        verifier: [u8; 8],
        now: Instant,
    ) -> Result<ExchangeResult, NfsStat4> {
        let key = (sender.host_id, owner.clone());
        if let Some(existing) = self
            .owners
            .get(&key)
            .and_then(|id| self.clients.get_mut(id))
        {
            if existing.verifier == verifier {
                if existing.confirmed && existing.principal != *sender.principal {
                    return Err(NfsStat4::ClientInUse);
                }
                existing.renewed = now;
                return Ok(ExchangeResult {
                    client_id: existing.client_id,
                    sequence: existing.sequence,
                    confirmed: existing.confirmed,
                });
            }
            if !existing.confirmed {
                let stale = existing.client_id;
                self.clients.remove(&stale);
            }
        }
        let client_id = self.new_client_id();
        let client = Client {
            client_id,
            host_id: sender.host_id,
            owner,
            verifier,
            principal: sender.principal.clone(),
            confirmed: false,
            sequence: 1,
            created: None,
            reclaim_complete: false,
            renewed: now,
        };
        self.clients.insert(client_id, client);
        self.owners.insert(key, client_id);
        Ok(ExchangeResult {
            client_id,
            sequence: 1,
            confirmed: false,
        })
    }

    /// CREATE_SESSION. Returns the result and the ids of older records it replaced, whose
    /// state the caller drops.
    pub fn create_session(
        &mut self,
        sender: Sender<'_>,
        request: CreateRequest,
        now: Instant,
    ) -> Result<(CreatedSession, Vec<u64>), NfsStat4> {
        let client = self
            .clients
            .get_mut(&request.client_id)
            .filter(|client| client.host_id == sender.host_id)
            .ok_or(NfsStat4::StaleClient)?;
        if request.sequence == client.sequence.wrapping_sub(1)
            && let Some(created) = client.created.clone()
        {
            return Ok((created, Vec::new()));
        }
        if request.sequence != client.sequence {
            return Err(NfsStat4::SequenceMisordered);
        }
        let fore = ChannelAttributes::negotiate(
            &request.fore,
            request.limits.max_request,
            request.limits.max_response,
        );
        let back = ChannelAttributes {
            max_requests: 1,
            ..ChannelAttributes::negotiate(
                &request.back,
                request.limits.max_request,
                request.limits.max_response,
            )
        };
        let created = CreatedSession {
            session_id: request.session_id,
            sequence: request.sequence,
            flags: 0,
            fore,
            back,
        };
        client.sequence = client.sequence.wrapping_add(1);
        client.created = Some(created.clone());
        client.confirmed = true;
        client.renewed = now;
        let replaced = self.replaced_by(request.client_id);
        for old in &replaced {
            self.drop_client(*old);
        }
        let slot = Slot {
            sequence: 0,
            busy: false,
            reply: SlotReply::Empty,
        };
        self.sessions.insert(
            request.session_id,
            Session {
                client_id: request.client_id,
                host_id: sender.host_id,
                fore,
                slots: vec![slot; fore.max_requests as usize],
                connections: HashSet::from([sender.connection]),
            },
        );
        Ok((created, replaced))
    }

    /// Other records of the same host and owner as `client_id`.
    fn replaced_by(&self, client_id: u64) -> Vec<u64> {
        let Some(client) = self.clients.get(&client_id) else {
            return Vec::new();
        };
        self.clients
            .values()
            .filter(|other| {
                other.client_id != client_id
                    && other.host_id == client.host_id
                    && other.owner == client.owner
            })
            .map(|other| other.client_id)
            .collect()
    }

    fn drop_client(&mut self, client_id: u64) {
        if let Some(client) = self.clients.remove(&client_id) {
            let key = (client.host_id, client.owner);
            if self.owners.get(&key) == Some(&client_id) {
                self.owners.remove(&key);
            }
        }
        self.sessions
            .retain(|_, session| session.client_id != client_id);
    }

    /// SEQUENCE: checks the session, its host and limits, then the slot sequence. A session
    /// of another host is unknown to the sender.
    pub fn begin(
        &mut self,
        sender: Sender<'_>,
        request: SlotRequest,
        now: Instant,
    ) -> SlotDecision {
        let Some(session) = self
            .sessions
            .get_mut(&request.session)
            .filter(|session| session.host_id == sender.host_id)
        else {
            return SlotDecision::Fail(NfsStat4::BadSession);
        };
        let fore = session.fore;
        if request.shape.size > fore.max_request as usize {
            return SlotDecision::Fail(NfsStat4::RequestTooBig);
        }
        if request.shape.operations > fore.max_operations as usize {
            return SlotDecision::Fail(NfsStat4::TooManyOperations);
        }
        let Some(slot) = session.slots.get_mut(request.slot as usize) else {
            return SlotDecision::Fail(NfsStat4::BadSlot);
        };
        if request.highest < request.slot {
            return SlotDecision::Fail(NfsStat4::BadHighSlot);
        }
        let client_id = session.client_id;
        if request.sequence == slot.sequence {
            if slot.busy {
                return SlotDecision::Fail(NfsStat4::Delay);
            }
            return match &slot.reply {
                SlotReply::Cached {
                    digest,
                    bytes,
                    grant,
                } if *digest == request.digest => SlotDecision::Replay {
                    bytes: bytes.clone(),
                    grant: grant.clone(),
                },
                SlotReply::Uncached { digest } if *digest == request.digest => {
                    SlotDecision::Uncached {
                        client_id,
                        highest: fore.max_requests.saturating_sub(1),
                    }
                }
                SlotReply::Empty => SlotDecision::Fail(NfsStat4::SequenceMisordered),
                _ => SlotDecision::Fail(NfsStat4::FalseRetry),
            };
        }
        if request.sequence != slot.sequence.wrapping_add(1) || slot.busy {
            return SlotDecision::Fail(NfsStat4::SequenceMisordered);
        }
        slot.sequence = request.sequence;
        slot.busy = true;
        slot.reply = SlotReply::Empty;
        session.connections.insert(sender.connection);
        let highest = fore.max_requests.saturating_sub(1);
        if let Some(client) = self.clients.get_mut(&client_id) {
            client.renewed = now;
        }
        SlotDecision::Execute {
            client_id,
            fore,
            highest,
        }
    }

    /// Stores the reply of an executed request and frees its slot.
    pub fn finish(&mut self, session: &[u8; 16], slot: u32, reply: SlotReply) {
        if let Some(slot) = self
            .sessions
            .get_mut(session)
            .and_then(|session| session.slots.get_mut(slot as usize))
        {
            slot.busy = false;
            slot.reply = reply;
        }
    }

    pub fn destroy_session(
        &mut self,
        sender: Sender<'_>,
        session: &[u8; 16],
    ) -> Result<(), NfsStat4> {
        match self.sessions.get(session) {
            Some(found) if found.host_id == sender.host_id => {
                self.sessions.remove(session);
                Ok(())
            }
            _ => Err(NfsStat4::BadSession),
        }
    }

    /// BIND_CONN_TO_SESSION, nconnect and trunking: only a connection of the same host binds.
    pub fn bind(&mut self, sender: Sender<'_>, session: &[u8; 16]) -> Result<(), NfsStat4> {
        match self.sessions.get_mut(session) {
            Some(found) if found.host_id == sender.host_id => {
                found.connections.insert(sender.connection);
                Ok(())
            }
            _ => Err(NfsStat4::BadSession),
        }
    }

    pub fn destroy_client(&mut self, sender: Sender<'_>, client_id: u64) -> Result<(), NfsStat4> {
        match self.clients.get(&client_id) {
            Some(client) if client.host_id == sender.host_id => {}
            _ => return Err(NfsStat4::StaleClient),
        }
        if self
            .sessions
            .values()
            .any(|session| session.client_id == client_id)
        {
            return Err(NfsStat4::ClientBusy);
        }
        self.drop_client(client_id);
        Ok(())
    }

    pub fn reclaim_complete(&mut self, client_id: u64) -> Result<(), NfsStat4> {
        let client = self
            .clients
            .get_mut(&client_id)
            .ok_or(NfsStat4::StaleClient)?;
        if client.reclaim_complete {
            return Err(NfsStat4::CompleteAlready);
        }
        client.reclaim_complete = true;
        Ok(())
    }

    /// Unbinds a closed connection from every session.
    pub fn disconnect(&mut self, connection: u64) {
        for session in self.sessions.values_mut() {
            session.connections.remove(&connection);
        }
    }

    /// Clients whose lease ran out at `now`.
    pub fn expired(&self, now: Instant, lease: Duration) -> Vec<u64> {
        self.clients
            .values()
            .filter(|client| now.saturating_duration_since(client.renewed) > lease)
            .map(|client| client.client_id)
            .collect()
    }

    /// Drops clients idle past `limit` with their sessions and returns their ids.
    pub fn forget_idle(&mut self, now: Instant, limit: Duration) -> Vec<u64> {
        let idle: Vec<u64> = self
            .clients
            .values()
            .filter(|client| now.saturating_duration_since(client.renewed) > limit)
            .map(|client| client.client_id)
            .collect();
        for client_id in &idle {
            self.drop_client(*client_id);
        }
        idle
    }

    /// Drops every client and session of a revoked host and returns the client ids.
    pub fn remove_host(&mut self, host_id: Ulid) -> Vec<u64> {
        let clients: Vec<u64> = self
            .clients
            .values()
            .filter(|client| client.host_id == host_id)
            .map(|client| client.client_id)
            .collect();
        for client_id in &clients {
            self.drop_client(*client_id);
        }
        clients
    }
}

/// The CREATE_SESSION inputs after decoding.
#[derive(Clone, Copy, Debug)]
pub struct CreateRequest {
    pub client_id: u64,
    pub sequence: u32,
    pub session_id: [u8; 16],
    pub fore: ChannelAttributes,
    pub back: ChannelAttributes,
    pub limits: ServerLimits,
}

/// The largest request and reply this server accepts on the fore channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServerLimits {
    pub max_request: u32,
    pub max_response: u32,
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
