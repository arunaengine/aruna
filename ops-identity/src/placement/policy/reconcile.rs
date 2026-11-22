//! Picks the subject and mode of this node's inventory scan from the realm's placement map.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{NODE_SUBJECT_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::node_subject::{
    NODE_SUBJECT_KEY, NodeSubjectRecord, storage_subject,
};
use aruna_core::structs::placement::policy::PlacementSubject;
use aruna_core::structs::storage::blob::ManagedCopyQuarantine;
use aruna_core::types::{Key, KeySpace, Value};

use super::subject::{
    SubjectScanConfig, SubjectScanError, SubjectScanMode, SubjectScanOperation, SubjectScanResult,
};

/// Why the scan runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubjectTrigger {
    /// An observed placement change. A node the realm still places revalidates; one
    /// draining or no longer placed departs under the subject it last advertised.
    Observe,
    /// A reconcile in this mode. A node without a placement entry is left alone.
    Reconcile(SubjectScanMode),
}

/// The mode the scan ran in and its result; none when nothing was scanned.
pub type SubjectSync = Option<(SubjectScanMode, SubjectScanResult)>;

#[derive(Debug, PartialEq)]
enum SyncState {
    Config,
    Local,
    /// The scan runs as a child with this config.
    Scan(SubjectScanConfig),
}

type SyncTurn = Result<Next<SubjectSync>, SubjectScanError>;

/// Reconciles the local subject with the realm's placement map and revalidates the inventory.
/// A realm without a config, or a node without a subject to scan under, scans nothing.
#[derive(Debug, PartialEq)]
pub struct SubjectSyncOperation {
    realm_id: RealmId,
    node_id: NodeId,
    trigger: SubjectTrigger,
    now_ms: u64,
    state: SyncState,
}

impl SubjectSyncOperation {
    pub fn new(realm_id: RealmId, node_id: NodeId, trigger: SubjectTrigger, now_ms: u64) -> Self {
        Self {
            realm_id,
            node_id,
            trigger,
            now_ms,
            state: SyncState::Config,
        }
    }

    fn config(&mut self, value: Option<Value>) -> SyncTurn {
        let Some(value) = value else {
            return Ok(Next::Done(None));
        };
        let config = RealmConfigDocument::from_bytes(value.as_ref())?;
        let entry = config.placement_entry(self.node_id);
        Ok(match (self.trigger, entry) {
            (SubjectTrigger::Observe, Some(entry)) if !entry.draining => self.scan(
                storage_subject(entry, 1),
                SubjectScanMode::Revalidate(ManagedCopyQuarantine::SubjectTransition),
            ),
            (SubjectTrigger::Observe, Some(entry)) => {
                self.scan(storage_subject(entry, 1), SubjectScanMode::Depart)
            }
            // The realm no longer places this node: an ungraceful removal it only
            // learns about through config sync.
            (SubjectTrigger::Observe, None) => {
                self.state = SyncState::Local;
                Next::Call(IoCall::Read(vec![local_row()]))
            }
            (SubjectTrigger::Reconcile(mode), Some(entry)) => {
                self.scan(storage_subject(entry, 1), mode)
            }
            (SubjectTrigger::Reconcile(_), None) => Next::Done(None),
        })
    }

    fn scan(&mut self, observed: PlacementSubject, mode: SubjectScanMode) -> Next<SubjectSync> {
        let config = SubjectScanConfig {
            realm_id: self.realm_id,
            observed,
            mode,
            now_ms: self.now_ms,
        };
        self.state = SyncState::Scan(config.clone());
        Next::Call(IoCall::child(SubjectScanOperation::new(config)))
    }
}

impl Operation for SubjectSyncOperation {
    type Output = SubjectSync;
    type Error = SubjectScanError;

    fn first(&mut self) -> SyncTurn {
        self.state = SyncState::Config;
        let row = (
            REALM_CONFIG_KEYSPACE.to_string(),
            Key::from(self.realm_id.as_bytes().to_vec()),
        );
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> SyncTurn {
        match &self.state {
            SyncState::Config => {
                let [value] = values::<1>(result)?;
                self.config(value)
            }
            SyncState::Local => {
                let [value] = values::<1>(result)?;
                Ok(match local_record(value)? {
                    Some(record) => self.scan(record.subject, SubjectScanMode::Depart),
                    None => Next::Done(None),
                })
            }
            SyncState::Scan(config) => {
                let mode = config.mode;
                let scanned = result.child::<SubjectScanResult, SubjectScanError>()??;
                Ok(Next::Done(Some((mode, scanned))))
            }
        }
    }
}

/// Reads the subject record this node advertises, if it ever advertised one.
#[derive(Debug, Default, PartialEq)]
pub struct LocalSubjectOperation;

impl LocalSubjectOperation {
    pub fn new() -> Self {
        Self
    }
}

impl Operation for LocalSubjectOperation {
    type Output = Option<NodeSubjectRecord>;
    type Error = SubjectScanError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        Ok(Next::Call(IoCall::Read(vec![local_row()])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        let IoResult::Values(values) = result else {
            return Err(SubjectScanError::InvalidEvent("ReadLocalSubject"));
        };
        Ok(Next::Done(local_record(
            values.into_iter().next().flatten(),
        )?))
    }
}

/// The row of the subject record this node advertises.
fn local_row() -> (KeySpace, Key) {
    (
        NODE_SUBJECT_KEYSPACE.to_string(),
        Key::from(NODE_SUBJECT_KEY.to_vec()),
    )
}

fn local_record(value: Option<Value>) -> Result<Option<NodeSubjectRecord>, SubjectScanError> {
    Ok(value
        .map(|value| NodeSubjectRecord::from_bytes(value.as_ref()))
        .transpose()?)
}

#[cfg(test)]
#[path = "reconcile_tests.rs"]
mod tests;
