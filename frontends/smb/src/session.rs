//! State of one SMB connection: the negotiated parameters, the credit window, sessions in
//! handshake or ready, their trees, opens scoped to (session, tree, FileId) and waiting work.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, HashMap, HashSet};

use tokio_util::sync::CancellationToken;

use crate::backend::{Credential, ObjectKind, Scope, ShareTarget, TreeGrant};
use crate::crypto::{Cipher, PreauthHash, Sealer, SessionKeys, Signing};
use crate::ntlm::Challenge;
use crate::status;
use crate::wire::FileId;

/// What NEGOTIATE settled for the connection.
#[derive(Clone, Debug)]
pub(crate) struct Negotiated {
    pub cipher: Cipher,
    pub signing: Signing,
    pub client_guid: [u8; 16],
    pub client_mode: u16,
    pub client_capabilities: u32,
    pub dialects: Vec<u16>,
}

/// How the client wraps its NTLM tokens.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Mechanism {
    /// SPNEGO; `list` is the client's DER MechTypeList and `preferred` whether NTLMSSP was
    /// its first choice.
    Spnego {
        list: Vec<u8>,
        preferred: bool,
    },
    Raw,
}

#[derive(Debug, Default)]
pub(crate) struct Handshake {
    pub preauth: PreauthHash,
    pub mechanism: Option<Mechanism>,
    pub challenge: Option<Challenge>,
}

/// An authenticated session. An expired one keeps its keys so requests still get an
/// encrypted NETWORK_SESSION_EXPIRED answer.
pub(crate) struct Ready {
    pub credential: Credential,
    pub keys: SessionKeys,
    pub sealer: Sealer,
    pub expired: bool,
}

pub(crate) enum Stage {
    Handshake(Handshake),
    Ready(Box<Ready>),
}

pub(crate) struct Tree {
    pub share: ShareTarget,
    pub grant: TreeGrant,
}

pub(crate) struct Session {
    pub stage: Stage,
    pub trees: HashMap<u32, Tree>,
    pub next_tree: u32,
}

impl Session {
    pub fn new(preauth: PreauthHash) -> Self {
        Self {
            stage: Stage::Handshake(Handshake {
                preauth,
                ..Handshake::default()
            }),
            trees: HashMap::new(),
            next_tree: 1,
        }
    }
}

/// Directory enumeration state kept on an open between QUERY_DIRECTORY requests. It holds no
/// entry: `cursor` starts the current page and `emitted` names what it already returned.
#[derive(Debug, Default)]
pub(crate) struct Listing {
    pub pattern: String,
    pub emitted: HashSet<String>,
    pub cursor: Option<String>,
    pub exhausted: bool,
    pub returned: bool,
}

pub(crate) struct Open {
    pub session_id: u64,
    pub tree_id: u32,
    pub persistent: u64,
    pub handle: u64,
    pub granted: u32,
    pub kind: ObjectKind,
    pub path: String,
    pub scope: Scope,
    pub read_only: bool,
    pub listing: Option<Listing>,
}

/// A copy of the fields of one open that a command needs outside the state lock.
#[derive(Clone, Debug)]
pub(crate) struct OpenView {
    pub volatile: u64,
    pub handle: u64,
    pub granted: u32,
    pub kind: ObjectKind,
    pub path: String,
    pub scope: Scope,
    pub read_only: bool,
}

/// A request that may still be cancelled, keyed by MessageId.
pub(crate) struct Waiting {
    pub session_id: u64,
    pub async_id: Option<u64>,
    pub token: CancellationToken,
    /// A waiting LOCK, which a disconnect cancels since it may wait on opens cleanup releases.
    pub lock: bool,
}

/// Granted MessageIds: `[start, end)` minus the ones already used. Each request consumes
/// its CreditCharge ids; a reused or ungranted id is a protocol violation.
#[derive(Debug)]
pub(crate) struct CreditWindow {
    start: u64,
    end: u64,
    used: BTreeSet<u64>,
    limit: u64,
}

impl CreditWindow {
    pub fn new(limit: u16) -> Self {
        Self {
            start: 0,
            end: 1,
            used: BTreeSet::new(),
            limit: u64::from(limit.max(1)),
        }
    }

    fn outstanding(&self) -> u64 {
        self.end - self.start - self.used.len() as u64
    }

    pub fn consume(&mut self, message_id: u64, charge: u16) -> bool {
        let charge = u64::from(charge.max(1));
        let Some(last) = message_id.checked_add(charge) else {
            return false;
        };
        if message_id < self.start || last > self.end {
            return false;
        }
        if self.used.range(message_id..last).next().is_some() {
            return false;
        }
        self.used.extend(message_id..last);
        while self.used.remove(&self.start) {
            self.start += 1;
        }
        true
    }

    /// Grants up to `requested` new credits while keeping at least one outstanding.
    pub fn grant(&mut self, requested: u16) -> u16 {
        let room = self.limit.saturating_sub(self.outstanding());
        let mut granted = u64::from(requested).min(room);
        if granted == 0 && self.outstanding() == 0 {
            granted = 1;
        }
        self.end = self.end.saturating_add(granted);
        granted as u16
    }
}

pub(crate) struct ConnectionState {
    pub negotiated: Option<Negotiated>,
    pub preauth: PreauthHash,
    pub credits: CreditWindow,
    pub sessions: HashMap<u64, Session>,
    pub opens: HashMap<u64, Open>,
    pub waiting: HashMap<u64, Waiting>,
    pub next_volatile: u64,
    pub next_async: u64,
}

impl ConnectionState {
    pub fn new(credit_limit: u16) -> Self {
        Self {
            negotiated: None,
            preauth: PreauthHash::default(),
            credits: CreditWindow::new(credit_limit),
            sessions: HashMap::new(),
            opens: HashMap::new(),
            waiting: HashMap::new(),
            next_volatile: 1,
            next_async: 1,
        }
    }

    /// The credential of a ready session, or the status for a missing, unfinished or
    /// expired one.
    pub fn credential(&self, session_id: u64) -> Result<Credential, u32> {
        match self.sessions.get(&session_id).map(|session| &session.stage) {
            Some(Stage::Ready(ready)) if ready.expired => Err(status::NETWORK_SESSION_EXPIRED),
            Some(Stage::Ready(ready)) => Ok(ready.credential.clone()),
            Some(Stage::Handshake(_)) => Err(status::ACCESS_DENIED),
            None => Err(status::USER_SESSION_DELETED),
        }
    }

    pub fn tree(&self, session_id: u64, tree_id: u32) -> Result<(ShareTarget, TreeGrant), u32> {
        let session = self
            .sessions
            .get(&session_id)
            .ok_or(status::USER_SESSION_DELETED)?;
        let tree = session
            .trees
            .get(&tree_id)
            .ok_or(status::NETWORK_NAME_DELETED)?;
        Ok((tree.share.clone(), tree.grant))
    }

    /// The open behind `file_id`, which must belong to this session and tree.
    pub fn open(&self, session_id: u64, tree_id: u32, file_id: FileId) -> Result<OpenView, u32> {
        let open = self
            .opens
            .get(&file_id.volatile)
            .filter(|open| {
                open.session_id == session_id
                    && open.tree_id == tree_id
                    && open.persistent == file_id.persistent
            })
            .ok_or(status::FILE_CLOSED)?;
        Ok(OpenView {
            volatile: file_id.volatile,
            handle: open.handle,
            granted: open.granted,
            kind: open.kind,
            path: open.path.clone(),
            scope: open.scope.clone(),
            read_only: open.read_only,
        })
    }

    /// Removes every open matching `filter` and returns what the backend must close.
    pub fn take_opens<F>(&mut self, filter: F) -> Vec<(Scope, u64)>
    where
        F: Fn(&Open) -> bool,
    {
        let chosen: Vec<u64> = self
            .opens
            .iter()
            .filter(|(_, open)| filter(open))
            .map(|(volatile, _)| *volatile)
            .collect();
        chosen
            .into_iter()
            .filter_map(|volatile| self.opens.remove(&volatile))
            .map(|open| (open.scope, open.handle))
            .collect()
    }

    /// Expires every ready session of `access_key` and returns their opens to close.
    pub fn expire_key(&mut self, access_key: &str) -> Vec<(Scope, u64)> {
        let mut expired = Vec::new();
        for (session_id, session) in &mut self.sessions {
            if let Stage::Ready(ready) = &mut session.stage
                && ready.credential.access_key == access_key
            {
                ready.expired = true;
                session.trees.clear();
                expired.push(*session_id);
            }
        }
        self.take_opens(|open| expired.contains(&open.session_id))
    }

    /// Expires one ready session, or drops one still in handshake, and returns its opens.
    pub fn expire_session(&mut self, session_id: u64) -> Vec<(Scope, u64)> {
        match self.sessions.get_mut(&session_id) {
            Some(Session {
                stage: Stage::Ready(ready),
                trees,
                ..
            }) => {
                ready.expired = true;
                trees.clear();
            }
            Some(_) => {
                self.sessions.remove(&session_id);
            }
            None => {}
        }
        self.take_opens(|open| open.session_id == session_id)
    }

    /// Removes a session after LOGOFF or a failed handshake and returns its opens.
    pub fn remove_session(&mut self, session_id: u64) -> Vec<(Scope, u64)> {
        self.sessions.remove(&session_id);
        self.take_opens(|open| open.session_id == session_id)
    }

    pub fn cipher(&self) -> Option<Cipher> {
        self.negotiated.as_ref().map(|negotiated| negotiated.cipher)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credits_consume_once() {
        let mut window = CreditWindow::new(8);
        assert!(window.consume(0, 0));
        assert!(!window.consume(0, 1), "a reused MessageId is refused");
        assert_eq!(window.grant(4), 4);
        assert!(!window.consume(5, 1), "an ungranted MessageId is refused");
        assert!(window.consume(3, 1), "ids may arrive out of order");
        assert!(window.consume(1, 2));
        assert!(window.consume(4, 1));
        assert_eq!(window.outstanding(), 0);
    }

    #[test]
    fn credits_respect_limit() {
        let mut window = CreditWindow::new(4);
        assert_eq!(window.grant(100), 3);
        assert_eq!(window.grant(100), 0);
        assert!(window.consume(0, 4));
        assert_eq!(window.grant(0), 1, "at least one credit stays available");
        assert!(
            !window.consume(u64::MAX, 2),
            "overflowing ranges are refused"
        );
    }

    #[test]
    fn multi_credit_charge() {
        let mut window = CreditWindow::new(16);
        window.grant(15);
        assert!(window.consume(0, 4));
        assert!(!window.consume(3, 1));
        assert!(window.consume(4, 1));
    }
}
