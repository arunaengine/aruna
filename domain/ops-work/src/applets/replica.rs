//! Copies applet records onto the holders of their group's records, so an applet stays readable
//! while its host node is away. Only the host node changes an applet; a copy is never copied on.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::applet::hosts::{HostKind, HostRow, host_key};
use aruna_core::applet::record::{Applet, DomainState, applet_key};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{APPLET_HOSTS_KEYSPACE, APPLETS_KEYSPACE};
use aruna_core::metadata::protocol::{
    METADATA_IO_TIMEOUT, MetadataReadError, MetadataTransportMessage, encode_message, peer_call,
    peer_reply,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use tracing::warn;

use super::{AppletError, applet_answer, host_answer, host_row, record_rows};
use crate::workflows::runs::replica::record_holders;

/// The hosts a copy serves: the applet's own host and its active custom domains.
fn served_hosts(applet: &Applet) -> Vec<(String, HostKind)> {
    let custom = (applet.domains.iter())
        .filter(|domain| domain.state == DomainState::Active)
        .map(|domain| (domain.host.clone(), HostKind::Custom));
    std::iter::once((applet.host.clone(), HostKind::Applet))
        .chain(custom)
        .collect()
}

/// Sends the record of an applet this node hosts to every other holder of its group's records.
/// A holder that does not answer is skipped until the next pass. Answers how many stored it.
#[derive(Debug, PartialEq)]
pub struct ReplicateAppletOperation {
    applet: Applet,
    local: (NodeId, RealmId),
    sends: Vec<NodeId>,
    frame: Vec<u8>,
    stored: usize,
    peer: Option<NodeId>,
}

impl ReplicateAppletOperation {
    pub fn new(applet: &Applet, local: (NodeId, RealmId)) -> Self {
        Self {
            applet: applet.clone(),
            local,
            sends: Vec::new(),
            frame: Vec::new(),
            stored: 0,
            peer: None,
        }
    }

    fn send_next(&mut self) -> Result<Next<usize>, AppletError> {
        let Some(node) = self.sends.pop() else {
            return Ok(Next::Done(self.stored));
        };
        self.peer = Some(node);
        let call = peer_call(node, self.frame.clone(), METADATA_IO_TIMEOUT);
        Ok(Next::Call(call))
    }
}

impl Operation for ReplicateAppletOperation {
    type Output = usize;
    type Error = AppletError;

    fn first(&mut self) -> Result<Next<usize>, AppletError> {
        if self.applet.host_node != self.local.0 {
            return Ok(Next::Done(0));
        }
        let read = GetConfigOperation::new(self.local.1);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, AppletError> {
        if let Some(peer) = self.peer.take() {
            match peer_reply(peer, result)? {
                Ok(MetadataTransportMessage::AppletStored { result: Ok(_) }) => self.stored += 1,
                other => {
                    let applet = self.applet.applet_id;
                    warn!(%applet, %peer, ?other, "An applet copy was not stored");
                }
            }
            return self.send_next();
        }
        let config = result.child::<RealmConfigDocument, GetConfigError>()?;
        let config = config.map_err(|error| AppletError::Storage(error.to_string()))?;
        self.sends = record_holders(&config, self.applet.group_id, self.local.0);
        let message = MetadataTransportMessage::StoreApplet {
            applet: Box::new(self.applet.clone()),
        };
        self.frame =
            encode_message(&message).map_err(|error| AppletError::Storage(error.to_string()))?;
        self.send_next()
    }

    fn expected_error(error: &AppletError) -> bool {
        !matches!(error, AppletError::Storage(_))
    }
}

/// Keeps the copy of an applet its host node `peer` sent. It refuses a sender that is not the
/// applet's host or not in the realm, and a node that holds none of the group's records.
#[derive(Debug, PartialEq)]
pub struct StoreAppletOperation {
    peer: NodeId,
    local: (NodeId, RealmId),
    applet: Option<Box<Applet>>,
}

impl StoreAppletOperation {
    pub fn new(peer: NodeId, local: (NodeId, RealmId), applet: Box<Applet>) -> Self {
        Self {
            peer,
            local,
            applet: Some(applet),
        }
    }
}

impl Operation for StoreAppletOperation {
    type Output = u64;
    type Error = MetadataReadError;

    fn first(&mut self) -> Result<Next<u64>, MetadataReadError> {
        let host = self.applet.as_ref().map(|applet| applet.host_node);
        if host != Some(self.peer) || host == Some(self.local.0) {
            return Err(MetadataReadError::Forbidden);
        }
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.local.1,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<u64>, MetadataReadError> {
        let Some(applet) = self.applet.take() else {
            let kept =
                (result.child::<u64, AppletError>()).map_err(|_| MetadataReadError::Unavailable)?;
            return kept.map(Next::Done).map_err(|error| match error {
                AppletError::Forbidden => MetadataReadError::Forbidden,
                _ => MetadataReadError::Unavailable,
            });
        };
        let config = (result.child::<RealmConfigDocument, GetConfigError>())
            .map_err(|_| MetadataReadError::Unavailable)?
            .map_err(|_| MetadataReadError::Unavailable)?;
        let member = config.node_kind(&self.peer).is_some();
        let holders = record_holders(&config, applet.group_id, self.peer);
        if !member || !holders.contains(&self.local.0) {
            return Err(MetadataReadError::Forbidden);
        }
        let seed = applet.applet_id.to_bytes();
        let write = WriteCopyOperation {
            local: self.local.0,
            applet,
            stale: Vec::new(),
            writes: Vec::new(),
            step: CopyStep::Read,
        };
        Ok(Next::Call(IoCall::retry(write, &seed)))
    }

    fn expected_error(error: &MetadataReadError) -> bool {
        matches!(error, MetadataReadError::Forbidden)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CopyStep {
    Read,
    CheckStale,
    Write,
    Delete,
}

/// Writes an applet copy with the host rows it serves in one transaction, unless the stored
/// copy is as new. A host row naming another applet is left alone.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteCopyOperation {
    local: NodeId,
    applet: Box<Applet>,
    stale: Vec<String>,
    writes: Vec<(
        aruna_core::types::KeySpace,
        aruna_core::types::Key,
        aruna_core::types::Value,
    )>,
    step: CopyStep,
}

impl Operation for WriteCopyOperation {
    type Output = u64;
    type Error = AppletError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<u64>, AppletError> {
        let rows = std::iter::once((
            APPLETS_KEYSPACE.to_string(),
            applet_key(self.applet.applet_id),
        ))
        .chain(
            (served_hosts(&self.applet).into_iter())
                .map(|(host, _)| (APPLET_HOSTS_KEYSPACE.to_string(), host_key(&host))),
        )
        .collect();
        Ok(Next::Call(IoCall::Read(rows)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<u64>, AppletError> {
        let revision = self.applet.revision;
        match self.step {
            CopyStep::Delete => {
                result.done()?;
                Ok(Next::Done(revision))
            }
            CopyStep::Write => {
                result.done()?;
                let stale = std::mem::take(&mut self.stale);
                if stale.is_empty() {
                    return Ok(Next::Done(revision));
                }
                let deletes = (stale.iter())
                    .map(|host| (APPLET_HOSTS_KEYSPACE.to_string(), host_key(host)))
                    .collect();
                self.step = CopyStep::Delete;
                Ok(Next::Call(IoCall::Delete(deletes)))
            }
            CopyStep::CheckStale => {
                let values = result.values()?;
                let candidates = std::mem::take(&mut self.stale);
                for (host, row) in candidates.into_iter().zip(values) {
                    let owned =
                        host_answer(row)?.is_some_and(|row| row.applet_id == self.applet.applet_id);
                    if owned {
                        self.stale.push(host);
                    }
                }
                let writes = std::mem::take(&mut self.writes);
                self.step = CopyStep::Write;
                Ok(Next::Call(IoCall::Write(writes)))
            }
            CopyStep::Read => {
                let mut values = result.values()?.into_iter();
                let stored = applet_answer(IoResult::Values(vec![values.next().flatten()]))?;
                if let Some(stored) = &stored {
                    if stored.host_node == self.local {
                        return Err(AppletError::Forbidden);
                    }
                    if stored.revision >= revision {
                        return Ok(Next::Done(stored.revision));
                    }
                }
                let mut writes = record_rows(&self.applet)?;
                let hosts = served_hosts(&self.applet);
                for ((host, kind), row) in hosts.iter().zip(values) {
                    let taken =
                        host_answer(row)?.is_some_and(|row| row.applet_id != self.applet.applet_id);
                    if !taken {
                        let row = HostRow {
                            applet_id: self.applet.applet_id,
                            kind: *kind,
                        };
                        writes.push(host_row(host, row)?);
                    }
                }
                // Hosts the older copy served that this one no longer serves.
                self.stale = (stored.iter().flat_map(served_hosts))
                    .map(|(host, _)| host)
                    .filter(|host| !hosts.iter().any(|(kept, _)| kept == host))
                    .collect();
                if !self.stale.is_empty() {
                    let reads = self
                        .stale
                        .iter()
                        .map(|host| (APPLET_HOSTS_KEYSPACE.to_string(), host_key(host)))
                        .collect();
                    self.writes = writes;
                    self.step = CopyStep::CheckStale;
                    return Ok(Next::Call(IoCall::Read(reads)));
                }
                self.step = CopyStep::Write;
                Ok(Next::Call(IoCall::Write(writes)))
            }
        }
    }

    fn expected_error(error: &AppletError) -> bool {
        matches!(error, AppletError::Forbidden)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::applets::change::tests::applet;
    use aruna_core::applet::record::CustomDomain;
    use byteview::ByteView;

    fn copy(applet: &Applet, local: NodeId) -> WriteCopyOperation {
        WriteCopyOperation {
            local,
            applet: Box::new(applet.clone()),
            stale: Vec::new(),
            writes: Vec::new(),
            step: CopyStep::Read,
        }
    }

    // A newer copy replaces an older one and drops only hosts still owned by it; an older copy,
    // the host's own record, and hosts now owned by another applet are left untouched.
    #[test]
    fn copies_only_newer() {
        let holder = iroh::SecretKey::from_bytes(&[9; 32]).public();
        let mut old = applet();
        old.domains.push(CustomDomain {
            host: "bakta.lab.org".into(),
            nonce: "00".into(),
            state: DomainState::Active,
            failures: 0,
            checked_at_ms: 0,
        });
        let mut new = old.clone();
        new.domains.clear();
        new.revision += 1;
        let stored = Some(ByteView::from(old.to_bytes().unwrap()));
        let owned_host = host_row(
            "bakta.lab.org",
            HostRow {
                applet_id: old.applet_id,
                kind: HostKind::Custom,
            },
        )
        .unwrap()
        .2;
        let mut write = copy(&new, holder);
        write.first().unwrap();
        let check = write
            .next(IoResult::Values(vec![stored.clone(), None]))
            .unwrap();
        assert!(matches!(check, Next::Call(IoCall::Read(_))));
        let answer = write
            .next(IoResult::Values(vec![Some(owned_host)]))
            .unwrap();
        assert!(matches!(answer, Next::Call(IoCall::Write(_))));
        assert_eq!(write.stale, vec!["bakta.lab.org".to_string()]);
        assert!(matches!(
            write.next(IoResult::Done),
            Ok(Next::Call(IoCall::Delete(_)))
        ));

        // If another applet now owns the stale host row, it is NOT deleted.
        let other_host = host_row(
            "bakta.lab.org",
            HostRow {
                applet_id: ulid::Ulid::from_bytes([77; 16]),
                kind: HostKind::Custom,
            },
        )
        .unwrap()
        .2;
        let mut keep_other = copy(&new, holder);
        keep_other.first().unwrap();
        keep_other
            .next(IoResult::Values(vec![stored.clone(), None]))
            .unwrap();
        keep_other
            .next(IoResult::Values(vec![Some(other_host)]))
            .unwrap();
        assert!(keep_other.stale.is_empty());
        assert_eq!(
            keep_other.next(IoResult::Done).unwrap(),
            Next::Done(new.revision)
        );

        let mut stale = copy(&old, holder);
        stale.first().unwrap();
        let newer = Some(ByteView::from(new.to_bytes().unwrap()));
        let kept = stale
            .next(IoResult::Values(vec![newer, None, None]))
            .unwrap();
        assert_eq!(kept, Next::Done(new.revision));

        let mut own = copy(&new, new.host_node);
        own.first().unwrap();
        let refused = own.next(IoResult::Values(vec![stored, None]));
        assert_eq!(refused, Err(AppletError::Forbidden));
    }
}
