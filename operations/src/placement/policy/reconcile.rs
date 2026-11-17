//! Picks the subject and mode of this node's inventory scan from the realm's placement map.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{NODE_SUBJECT_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::operation::EffectOperation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::node_subject::{
    NODE_SUBJECT_KEY, NodeSubjectRecord, storage_subject,
};
use aruna_core::structs::placement::policy::PlacementSubject;
use aruna_core::structs::storage::blob::ManagedCopyQuarantine;
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;

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
    Init,
    Config,
    Local,
    Scan {
        mode: SubjectScanMode,
        scan: Box<SubjectScanOperation>,
    },
    Done,
}

/// Reconciles the local subject with the realm's placement map and revalidates the inventory.
/// A realm without a config, or a node without a subject to scan under, scans nothing.
#[derive(Debug, PartialEq)]
pub struct SubjectSyncOperation {
    realm_id: RealmId,
    node_id: NodeId,
    trigger: SubjectTrigger,
    now_ms: u64,
    state: SyncState,
    output: Option<Result<SubjectSync, SubjectScanError>>,
}

impl SubjectSyncOperation {
    pub fn new(realm_id: RealmId, node_id: NodeId, trigger: SubjectTrigger, now_ms: u64) -> Self {
        Self {
            realm_id,
            node_id,
            trigger,
            now_ms,
            state: SyncState::Init,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<SubjectSync, SubjectScanError>) -> Effects {
        self.state = SyncState::Done;
        self.output = Some(output);
        smallvec![]
    }

    fn config(&mut self, value: Option<Value>) -> Effects {
        let config = match value
            .map(|value| RealmConfigDocument::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(Some(config)) => config,
            Ok(None) => return self.finish(Ok(None)),
            Err(error) => return self.finish(Err(error.into())),
        };
        let entry = config.placement_entry(self.node_id);
        match (self.trigger, entry) {
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
                smallvec![local_read()]
            }
            (SubjectTrigger::Reconcile(mode), Some(entry)) => {
                self.scan(storage_subject(entry, 1), mode)
            }
            (SubjectTrigger::Reconcile(_), None) => self.finish(Ok(None)),
        }
    }

    fn local(&mut self, value: Option<Value>) -> Effects {
        match local_record(value) {
            Ok(Some(record)) => self.scan(record.subject, SubjectScanMode::Depart),
            Ok(None) => self.finish(Ok(None)),
            Err(error) => self.finish(Err(error)),
        }
    }

    fn scan(&mut self, observed: PlacementSubject, mode: SubjectScanMode) -> Effects {
        let mut scan = Box::new(SubjectScanOperation::new(SubjectScanConfig {
            realm_id: self.realm_id,
            observed,
            mode,
            now_ms: self.now_ms,
        }));
        let effects = scan.start();
        self.state = SyncState::Scan { mode, scan };
        self.after_scan(effects)
    }

    fn after_scan(&mut self, effects: Effects) -> Effects {
        let complete = matches!(&self.state, SyncState::Scan { scan, .. } if scan.is_complete());
        if !complete {
            return effects;
        }
        let SyncState::Scan { mode, scan } = std::mem::replace(&mut self.state, SyncState::Done)
        else {
            return effects;
        };
        self.finish(scan.finalize().map(|result| Some((mode, result))))
    }
}

impl EffectOperation for SubjectSyncOperation {
    type Output = SubjectSync;
    type Error = SubjectScanError;

    fn start(&mut self) -> Effects {
        self.state = SyncState::Config;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: self.realm_id.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let SyncState::Scan { scan, .. } = &mut self.state {
            let effects = scan.step(event);
            return self.after_scan(effects);
        }
        let read = match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => Some(value),
            _ => None,
        };
        let config = matches!(self.state, SyncState::Config);
        let local = matches!(self.state, SyncState::Local);
        match read {
            Some(value) if config => self.config(value),
            Some(value) if local => self.local(value),
            _ if config => self.finish(Err(SubjectScanError::InvalidEvent("ReadRealmConfig"))),
            _ if local => self.finish(Err(SubjectScanError::InvalidEvent("ReadLocalSubject"))),
            _ => self.finish(Err(SubjectScanError::InvalidEvent("SubjectSync"))),
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(SubjectScanError::InvalidEvent("SubjectSync")))
    }

    fn abort(&mut self) -> Effects {
        match &mut self.state {
            SyncState::Scan { scan, .. } => scan.abort(),
            _ => smallvec![],
        }
    }
}

/// Reads the subject record this node advertises, if it ever advertised one.
#[derive(Debug, Default, PartialEq)]
pub struct LocalSubjectOperation {
    started: bool,
    output: Option<Result<Option<NodeSubjectRecord>, SubjectScanError>>,
}

impl LocalSubjectOperation {
    pub fn new() -> Self {
        Self::default()
    }
}

impl EffectOperation for LocalSubjectOperation {
    type Output = Option<NodeSubjectRecord>;
    type Error = SubjectScanError;

    fn start(&mut self) -> Effects {
        self.started = true;
        smallvec![local_read()]
    }

    fn step(&mut self, event: Event) -> Effects {
        let started = std::mem::replace(&mut self.started, false);
        self.output = Some(match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) if started => {
                local_record(value)
            }
            _ => Err(SubjectScanError::InvalidEvent("ReadLocalSubject")),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(SubjectScanError::InvalidEvent("ReadLocalSubject")))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

fn local_read() -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: NODE_SUBJECT_KEYSPACE.to_string(),
        key: Key::from(NODE_SUBJECT_KEY.to_vec()),
        txn_id: None,
    })
}

fn local_record(value: Option<Value>) -> Result<Option<NodeSubjectRecord>, SubjectScanError> {
    Ok(value
        .map(|value| NodeSubjectRecord::from_bytes(value.as_ref()))
        .transpose()?)
}

#[cfg(test)]
#[path = "reconcile_tests.rs"]
mod tests;
