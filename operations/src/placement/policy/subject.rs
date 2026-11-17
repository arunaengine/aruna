//! Revalidates this node's registered copies after its own placement subject changed.
//! It advances the stored subject, rescans the inventory and decides every registration.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{MANAGED_COPY_KEYSPACE, NODE_SUBJECT_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, done, values};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::node_subject::{NODE_SUBJECT_KEY, NodeSubjectRecord};
use aruna_core::structs::placement::policy::PlacementSubject;
use aruna_core::structs::storage::blob::{
    ManagedCopyKey, ManagedCopyQuarantine, ManagedCopyRecord, ManagedCopyState,
};
use aruna_core::types::{Key, KeySpace, Value};
use thiserror::Error;
use tracing::debug;

use aruna_core::managed_copy::{COPY_PAGE_LIMIT, ManagedCopyError};

use super::gate::{GateContext, PolicyGateError, PolicyGateOutcome, gate_decision, write_gate};
use super::read::ReadPolicyError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubjectScanMode {
    /// An observed subject change, a restart, or an interrupted revalidation.
    Revalidate(ManagedCopyQuarantine),
    /// This node is leaving: every copy is recorded unresolved and none is
    /// restored. Departure is never blocked by what the scan finds.
    Depart,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SubjectScanConfig {
    pub realm_id: RealmId,
    /// The subject this node observes for itself right now.
    pub observed: PlacementSubject,
    pub mode: SubjectScanMode,
    pub now_ms: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SubjectScanResult {
    pub generation: u64,
    pub scanned: usize,
    pub quarantined: usize,
    pub restored: usize,
}

#[derive(Debug, Error, PartialEq)]
pub enum SubjectScanError {
    #[error(transparent)]
    Copy(#[from] ManagedCopyError),
    #[error(transparent)]
    Gate(#[from] PolicyGateError),
    #[error(transparent)]
    Conversion(#[from] aruna_core::errors::ConversionError),
    #[error("unexpected event in state {0}")]
    InvalidEvent(&'static str),
    #[error(transparent)]
    Io(#[from] aruna_core::calls::IoError),
}

#[derive(Debug, PartialEq)]
enum ScanState {
    ReadRecord,
    WriteRecord,
    Scan,
    Gate,
    Transition,
    Clear,
}

type ScanTurn = Result<Next<SubjectScanResult>, SubjectScanError>;

#[derive(Debug, PartialEq)]
pub struct SubjectScanOperation {
    config: SubjectScanConfig,
    state: ScanState,
    record: Option<NodeSubjectRecord>,
    pages: PrefixPages,
    /// Rows of the current page still to decide, in reverse order.
    pending: Vec<ManagedCopyRecord>,
    current: Option<ManagedCopyRecord>,
    result: SubjectScanResult,
}

impl SubjectScanOperation {
    pub fn new(config: SubjectScanConfig) -> Self {
        Self {
            config,
            state: ScanState::ReadRecord,
            record: None,
            pages: PrefixPages::new(
                MANAGED_COPY_KEYSPACE,
                Key::from(Vec::new()),
                COPY_PAGE_LIMIT,
            ),
            pending: Vec::new(),
            current: None,
            result: SubjectScanResult::default(),
        }
    }

    fn gate_context(&self) -> Option<GateContext> {
        // This walk is the revalidation the block waits for, so it evaluates
        // under the new subject instead of refusing itself.
        self.record.as_ref().map(|record| GateContext {
            realm_id: self.config.realm_id,
            subject: record.subject.clone(),
            now_ms: self.config.now_ms,
            admitting: true,
        })
    }

    fn write_record(&mut self, record: NodeSubjectRecord, next: ScanState) -> ScanTurn {
        let value = record.to_bytes()?;
        self.result.generation = record.subject.generation;
        self.record = Some(record);
        self.state = next;
        Ok(Next::Call(IoCall::Write(vec![(
            NODE_SUBJECT_KEYSPACE.to_string(),
            Key::from(NODE_SUBJECT_KEY.to_vec()),
            value.into(),
        )])))
    }

    fn scan_page(&mut self) -> Next<SubjectScanResult> {
        self.state = ScanState::Scan;
        Next::Call(self.pages.call())
    }

    fn next_copy(&mut self) -> ScanTurn {
        let Some(record) = self.pending.pop() else {
            return match self.pages.done() {
                true => self.finish(),
                false => Ok(self.scan_page()),
            };
        };
        self.result.scanned += 1;
        if let SubjectScanMode::Depart = self.config.mode {
            return self.transition(record, ManagedCopyState::UnresolvedDeparted);
        }
        // A copy row names its bucket but not the owning group, so revalidation
        // re-checks the subject alone; ownership is decided when a copy lands.
        match write_gate(self.gate_context().as_ref(), &record.policies, None) {
            Ok(None) => self.decide(record, Ok(())),
            Ok(Some(gate)) => {
                self.current = Some(record);
                self.state = ScanState::Gate;
                Ok(Next::Call(IoCall::child(gate)))
            }
            Err(error) => self.decide(record, Err(error)),
        }
    }

    fn finish_gate(&mut self, result: IoResult) -> ScanTurn {
        let record = self
            .current
            .take()
            .ok_or(SubjectScanError::InvalidEvent("Gate"))?;
        let decision = result
            .child::<PolicyGateOutcome, ReadPolicyError>()?
            .map_err(PolicyGateError::from)
            .and_then(gate_decision);
        self.decide(record, decision)
    }

    /// A copy that no longer satisfies its own refs is quarantined; one that
    /// does is restored, so a completed revalidation can end the transition.
    fn decide(
        &mut self,
        record: ManagedCopyRecord,
        decision: Result<(), PolicyGateError>,
    ) -> ScanTurn {
        // Departure records every row unresolved without deciding it, so a
        // decision taken in that mode is a state error, not a quarantine.
        let quarantine = match self.config.mode {
            SubjectScanMode::Revalidate(reason) => reason,
            SubjectScanMode::Depart => return Err(SubjectScanError::InvalidEvent("Decide")),
        };
        let state = match decision {
            Ok(()) => ManagedCopyState::Registered,
            Err(_) => ManagedCopyState::Quarantined(quarantine),
        };
        match state {
            ManagedCopyState::Registered => self.result.restored += 1,
            _ => self.result.quarantined += 1,
        }
        self.transition(record, state)
    }

    /// A restored copy is rewritten under the subject that just admitted it, so
    /// its row never claims a generation this node no longer advertises.
    fn transition(&mut self, record: ManagedCopyRecord, state: ManagedCopyState) -> ScanTurn {
        let generation = match state {
            ManagedCopyState::Registered => self.result.generation,
            _ => record.subject_generation,
        };
        if record.state == state && record.subject_generation == generation {
            return self.next_copy();
        }
        let mut record = record.stored_under(generation);
        record.state = state;
        self.state = ScanState::Transition;
        Ok(Next::Call(IoCall::Write(vec![copy_row(&record)?])))
    }

    /// Serving resumes only once no non-compliant registration remains, and a
    /// departing node never resumes at all.
    fn finish(&mut self) -> ScanTurn {
        let record = self
            .record
            .clone()
            .ok_or(SubjectScanError::InvalidEvent("Clear"))?;
        if self.result.quarantined > 0 || matches!(self.config.mode, SubjectScanMode::Depart) {
            return Ok(self.complete());
        }
        self.write_record(record.cleared(), ScanState::Clear)
    }

    fn complete(&mut self) -> Next<SubjectScanResult> {
        debug!(
            generation = self.result.generation,
            scanned = self.result.scanned,
            quarantined = self.result.quarantined,
            restored = self.result.restored,
            departing = matches!(self.config.mode, SubjectScanMode::Depart),
            "Placement subject scan finished"
        );
        Next::Done(self.result.clone())
    }

    fn handle_record(&mut self, value: Option<Value>) -> ScanTurn {
        let stored = value
            .map(|value| NodeSubjectRecord::from_bytes(value.as_ref()))
            .transpose()?;
        let Some(stored) = stored else {
            // A node with no record has never advertised: seed it blocked and
            // let this very scan decide whatever inventory already exists.
            let mut seeded = NodeSubjectRecord::seed(self.config.observed.clone())
                .map_err(PolicyGateError::from)?;
            seeded.serving_blocked = true;
            seeded.policy_draining = true;
            return self.write_record(seeded, ScanState::WriteRecord);
        };
        match stored.advance(self.config.observed.clone())? {
            Some(advanced) => self.write_record(advanced, ScanState::WriteRecord),
            None if matches!(self.config.mode, SubjectScanMode::Depart) => {
                // Departure stops new admission even when the subject itself is
                // unchanged, then records the inventory unresolved.
                let mut leaving = stored;
                leaving.serving_blocked = true;
                leaving.policy_draining = true;
                self.write_record(leaving, ScanState::WriteRecord)
            }
            None => {
                self.result.generation = stored.subject.generation;
                let blocked = stored.serving_blocked;
                self.record = Some(stored);
                match blocked {
                    true => Ok(self.scan_page()),
                    // Nothing changed and nothing is blocked: no copy can have
                    // become non-compliant, so the scan costs nothing.
                    false => Ok(self.complete()),
                }
            }
        }
    }

    fn handle_page(&mut self, result: IoResult) -> ScanTurn {
        let rows = self.pages.take(result)?;
        if rows.is_empty() {
            return self.finish();
        }
        let mut pending = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            ManagedCopyKey::from_bytes(key.as_ref()).map_err(ManagedCopyError::from)?;
            pending.push(ManagedCopyRecord::from_bytes(value.as_ref())?);
        }
        pending.reverse();
        self.pending = pending;
        self.next_copy()
    }
}

/// The stored row of one managed copy.
fn copy_row(record: &ManagedCopyRecord) -> Result<(KeySpace, Key, Value), ManagedCopyError> {
    Ok((
        MANAGED_COPY_KEYSPACE.to_string(),
        record.key().to_bytes()?.into(),
        record.to_bytes()?.into(),
    ))
}

impl Operation for SubjectScanOperation {
    type Output = SubjectScanResult;
    type Error = SubjectScanError;

    fn first(&mut self) -> ScanTurn {
        self.state = ScanState::ReadRecord;
        let row = (
            NODE_SUBJECT_KEYSPACE.to_string(),
            Key::from(NODE_SUBJECT_KEY.to_vec()),
        );
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> ScanTurn {
        match self.state {
            ScanState::ReadRecord => {
                let [value] = values::<1>(result)?;
                self.handle_record(value)
            }
            ScanState::WriteRecord => {
                done(result)?;
                Ok(self.scan_page())
            }
            ScanState::Scan => self.handle_page(result),
            ScanState::Gate => self.finish_gate(result),
            ScanState::Transition => {
                done(result)?;
                self.next_copy()
            }
            ScanState::Clear => {
                done(result)?;
                Ok(self.complete())
            }
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{ScanTurn, SubjectScanConfig, SubjectScanMode, SubjectScanOperation};
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::id::NodeId;
    use aruna_core::keyspaces::{MANAGED_COPY_KEYSPACE, NODE_SUBJECT_KEYSPACE};
    use aruna_core::operation::Operation;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::node_subject::NodeSubjectRecord;
    use aruna_core::structs::placement::policy::{PlacementPolicyRef, PlacementSubject};
    use aruna_core::structs::storage::blob::{
        BackendLocation, BackendRef, ManagedCopyQuarantine, ManagedCopyRecord, ManagedCopyState,
        VersionKey,
    };
    use aruna_core::structs::storage::format::StoredFormat;
    use std::collections::{BTreeMap, HashMap};
    use std::time::UNIX_EPOCH;
    use ulid::Ulid;

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[5u8; 32]).public()
    }

    fn subject(location: &str) -> PlacementSubject {
        PlacementSubject {
            node_id: node(),
            generation: 1,
            location: location.to_string(),
            labels: BTreeMap::new(),
            executor_kind: None,
            local_to_controller: true,
        }
    }

    fn copy(state: ManagedCopyState, refs: Vec<PlacementPolicyRef>) -> ManagedCopyRecord {
        ManagedCopyRecord::new(
            VersionKey::new("bucket", "file.txt", Ulid::from_bytes([4u8; 16])),
            node(),
            BackendLocation {
                backend: BackendRef::node_default(),
                storage_class: None,
                root: "/data".to_string(),
                storage_bucket: "aruna".to_string(),
                backend_path: "objects/one".to_string(),
                ulid: Ulid::from_bytes([5u8; 16]),
                format: StoredFormat::default(),
                created_by: Default::default(),
                created_at: UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 3,
                hashes: HashMap::new(),
            },
            refs,
            7,
            state,
        )
        .expect("record builds")
    }

    fn operation(mode: SubjectScanMode, location: &str) -> SubjectScanOperation {
        SubjectScanOperation::new(SubjectScanConfig {
            realm_id: RealmId::from_bytes([3u8; 32]),
            observed: subject(location),
            mode,
            now_ms: 1_000,
        })
    }

    fn stored(record: &NodeSubjectRecord) -> IoResult {
        IoResult::Values(vec![Some(
            record.to_bytes().expect("record encodes").into(),
        )])
    }

    fn page(records: Vec<ManagedCopyRecord>) -> IoResult {
        IoResult::Page {
            rows: records
                .into_iter()
                .map(|record| {
                    (
                        record.key().to_bytes().expect("key encodes").into(),
                        record.to_bytes().expect("record encodes").into(),
                    )
                })
                .collect(),
            next: None,
        }
    }

    fn written(turn: &ScanTurn) -> Option<NodeSubjectRecord> {
        match turn {
            Ok(Next::Call(IoCall::Write(rows))) => match rows.as_slice() {
                [(space, _, value)] if space != MANAGED_COPY_KEYSPACE => {
                    NodeSubjectRecord::from_bytes(value.as_ref()).ok()
                }
                _ => None,
            },
            _ => None,
        }
    }

    /// The managed copy row a turn writes.
    fn copy_write(turn: &ScanTurn) -> ManagedCopyRecord {
        let Ok(Next::Call(IoCall::Write(rows))) = turn else {
            panic!("expected one copy transition");
        };
        let [(_, _, value)] = rows.as_slice() else {
            panic!("expected one copy transition");
        };
        ManagedCopyRecord::from_bytes(value.as_ref()).expect("record decodes")
    }

    #[test]
    fn change_blocks_serving() {
        // The generation advances and serving stops before any copy is looked
        // at, so an unscanned row cannot be served under the new subject.
        let mut operation = operation(
            SubjectScanMode::Revalidate(ManagedCopyQuarantine::SubjectTransition),
            "us-east",
        );
        operation.first().unwrap();
        let record = NodeSubjectRecord::seed(subject("eu-west")).expect("subject is valid");
        let turn = operation.next(stored(&record));

        let advanced = written(&turn).expect("the record is rewritten first");
        assert_eq!(advanced.subject.generation, 2);
        assert!(advanced.serving_blocked);
    }

    #[test]
    fn unchanged_skips_scan() {
        // A reconfiguration that changes nothing must not walk the inventory.
        let mut operation = operation(
            SubjectScanMode::Revalidate(ManagedCopyQuarantine::SubjectTransition),
            "eu-west",
        );
        operation.first().unwrap();
        let record = NodeSubjectRecord::seed(subject("eu-west")).expect("subject is valid");
        assert!(matches!(operation.next(stored(&record)), Ok(Next::Done(_))));
    }

    #[test]
    fn ungoverned_copy_restores() {
        // Nothing governs the copy, so revalidation clears the block without a
        // single policy fetch.
        let mut operation = operation(
            SubjectScanMode::Revalidate(ManagedCopyQuarantine::Rejoin),
            "eu-west",
        );
        operation.first().unwrap();
        let mut record = NodeSubjectRecord::seed(subject("eu-west")).expect("subject is valid");
        record.serving_blocked = true;
        operation.next(stored(&record)).unwrap();

        let quarantined = copy(
            ManagedCopyState::Quarantined(ManagedCopyQuarantine::Rejoin),
            Vec::new(),
        );
        let turn = operation.next(page(vec![quarantined]));
        assert_eq!(copy_write(&turn).state, ManagedCopyState::Registered);

        let turn = operation.next(IoResult::Done);
        assert!(
            !written(&turn)
                .expect("the block is cleared")
                .serving_blocked
        );
    }

    #[test]
    fn departure_marks_unresolved() {
        // Departure never restores and is never blocked by what it finds.
        let mut operation = operation(SubjectScanMode::Depart, "eu-west");
        operation.first().unwrap();
        let record = NodeSubjectRecord::seed(subject("eu-west")).expect("subject is valid");
        operation.next(stored(&record)).unwrap();
        operation.next(IoResult::Done).unwrap();

        let turn = operation.next(page(vec![copy(ManagedCopyState::Registered, Vec::new())]));
        assert_eq!(
            copy_write(&turn).state,
            ManagedCopyState::UnresolvedDeparted
        );
    }

    #[test]
    fn departure_stops_admission() {
        // Departure blocks new admission even when the subject is unchanged, so
        // no governed byte lands here while the inventory is being resolved.
        let mut operation = operation(SubjectScanMode::Depart, "eu-west");
        operation.first().unwrap();
        let record = NodeSubjectRecord::seed(subject("eu-west")).expect("subject is valid");
        let turn = operation.next(stored(&record));

        let leaving = written(&turn).expect("the record is rewritten first");
        assert!(leaving.serving_blocked && leaving.policy_draining);
    }

    #[test]
    fn scan_ignores_jobs() {
        // A receipted execution must survive a transition: the scan only ever
        // writes the subject row and managed-copy rows.
        let mut operation = operation(
            SubjectScanMode::Revalidate(ManagedCopyQuarantine::SubjectTransition),
            "us-east",
        );
        let record = NodeSubjectRecord::seed(subject("eu-west")).expect("subject is valid");
        let turns = [
            operation.first(),
            operation.next(stored(&record)),
            operation.next(IoResult::Done),
            operation.next(page(vec![copy(ManagedCopyState::Registered, Vec::new())])),
        ];

        for turn in turns {
            let spaces: Vec<String> = match turn {
                Ok(Next::Call(IoCall::Write(rows))) => {
                    rows.into_iter().map(|(space, _, _)| space).collect()
                }
                Ok(Next::Call(IoCall::Page { space, .. })) => vec![space],
                _ => continue,
            };
            for space in spaces {
                assert!(
                    space == MANAGED_COPY_KEYSPACE || space == NODE_SUBJECT_KEYSPACE,
                    "the scan touched {space}"
                );
            }
        }
    }

    #[test]
    fn restore_stores_generation() {
        // A restored copy names the subject that just admitted it, so a row
        // stored under the old generation can never pass for the new one.
        let mut operation = operation(
            SubjectScanMode::Revalidate(ManagedCopyQuarantine::Rejoin),
            "eu-west",
        );
        operation.first().unwrap();
        let mut record = NodeSubjectRecord::seed(subject("eu-west")).expect("subject is valid");
        record.subject.generation = 4;
        record.serving_blocked = true;
        operation.next(stored(&record)).unwrap();

        let stale = copy(
            ManagedCopyState::Quarantined(ManagedCopyQuarantine::Rejoin),
            Vec::new(),
        );
        let restored = copy_write(&operation.next(page(vec![stale])));
        assert_eq!(restored.state, ManagedCopyState::Registered);
        assert_eq!(restored.subject_generation, 4);
    }

    #[test]
    fn governed_requires_subject() {
        // The gate runs per registration; a governed copy cannot be restored
        // without resolving its rule.
        let mut operation = operation(
            SubjectScanMode::Revalidate(ManagedCopyQuarantine::SubjectTransition),
            "us-east",
        );
        operation.first().unwrap();
        let record = NodeSubjectRecord::seed(subject("eu-west")).expect("subject is valid");
        operation.next(stored(&record)).unwrap();
        operation.next(IoResult::Done).unwrap();

        let governed = copy(
            ManagedCopyState::Registered,
            vec![PlacementPolicyRef {
                policy_id: Ulid::from_bytes([1u8; 16]),
                digest: [2u8; 32],
            }],
        );
        assert!(
            matches!(
                operation.next(page(vec![governed])),
                Ok(Next::Call(IoCall::Child(_)))
            ),
            "the gate must resolve the rule before the copy is decided"
        );
    }
}
