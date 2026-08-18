//! Bounded local revalidation of this node's registered copies.
//!
//! An observed subject change, a rejoin, and a graceful departure all reduce to
//! the same walk: advance the local subject, stop serving until the inventory
//! has been re-evaluated, then page through every registration and decide it
//! against the new subject. Nothing here waits for a realm-wide acknowledgement.

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{NODE_SUBJECT_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    ManagedCopyQuarantine, ManagedCopyRecord, ManagedCopyState, NODE_SUBJECT_KEY,
    NodeSubjectRecord, PlacementSubject, RealmConfigDocument, RealmId, storage_subject,
};
use aruna_core::types::{Effects, Key, NodeId, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use tracing::debug;

use crate::blob::managed_copy::{
    COPY_PAGE_LIMIT, ManagedCopyError, ManagedCopyPage, scan_effect, transition_effect,
};
use crate::driver::{DriverContext, drive};

use super::gate::{GateContext, PolicyGateError, PolicyGateOperation, gate_decision, write_gate};

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
}

#[derive(Debug, PartialEq)]
enum ScanState {
    Init,
    ReadRecord,
    WriteRecord,
    Scan,
    Gate,
    Transition,
    Clear,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct SubjectScanOperation {
    config: SubjectScanConfig,
    state: ScanState,
    txn_id: Option<TxnId>,
    record: Option<NodeSubjectRecord>,
    cursor: Option<Key>,
    /// Rows of the current page still to decide, in reverse order.
    pending: Vec<ManagedCopyRecord>,
    current: Option<ManagedCopyRecord>,
    gate: Option<PolicyGateOperation>,
    result: SubjectScanResult,
    output: Option<Result<SubjectScanResult, SubjectScanError>>,
}

impl SubjectScanOperation {
    pub fn new(config: SubjectScanConfig) -> Self {
        Self {
            config,
            state: ScanState::Init,
            txn_id: None,
            record: None,
            cursor: None,
            pending: Vec::new(),
            current: None,
            gate: None,
            result: SubjectScanResult::default(),
            output: None,
        }
    }

    pub fn with_txn(mut self, txn_id: TxnId) -> Self {
        self.txn_id = Some(txn_id);
        self
    }

    fn gate_context(&self) -> Option<GateContext> {
        self.record.as_ref().map(|record| GateContext {
            realm_id: self.config.realm_id,
            subject: record.subject.clone(),
            now_ms: self.config.now_ms,
        })
    }

    fn write_record(&mut self, record: NodeSubjectRecord, next: ScanState) -> Effects {
        let value = match record.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.fail(error.into()),
        };
        self.result.generation = record.subject.generation;
        self.record = Some(record);
        self.state = next;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: NODE_SUBJECT_KEYSPACE.to_string(),
            key: Key::from(NODE_SUBJECT_KEY.to_vec()),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }

    fn scan_page(&mut self) -> Effects {
        self.state = ScanState::Scan;
        smallvec![scan_effect(
            None,
            self.cursor.clone(),
            COPY_PAGE_LIMIT,
            self.txn_id,
        )]
    }

    fn next_copy(&mut self) -> Effects {
        let Some(record) = self.pending.pop() else {
            return match self.cursor.is_some() {
                true => self.scan_page(),
                false => self.finish(),
            };
        };
        self.result.scanned += 1;
        if let SubjectScanMode::Depart = self.config.mode {
            return self.transition(record, ManagedCopyState::UnresolvedDeparted);
        }
        match write_gate(self.gate_context().as_ref(), &record.policies) {
            Ok(None) => self.decide(record, Ok(())),
            Ok(Some(mut gate)) => {
                let effects = gate.start();
                let complete = gate.is_complete();
                self.gate = Some(gate);
                self.current = Some(record);
                self.state = ScanState::Gate;
                match complete {
                    true => self.finish_gate(),
                    false => effects,
                }
            }
            Err(error) => self.decide(record, Err(error)),
        }
    }

    fn finish_gate(&mut self) -> Effects {
        let (Some(gate), Some(record)) = (self.gate.take(), self.current.take()) else {
            return self.fail(SubjectScanError::InvalidEvent("Gate"));
        };
        let decision = gate
            .finalize()
            .map_err(PolicyGateError::from)
            .and_then(|outcome| gate_decision(outcome.decision));
        self.decide(record, decision)
    }

    /// A copy that no longer satisfies its own refs is quarantined; one that
    /// does is restored, so a completed revalidation can end the transition.
    fn decide(
        &mut self,
        record: ManagedCopyRecord,
        decision: Result<(), PolicyGateError>,
    ) -> Effects {
        let quarantine = match self.config.mode {
            SubjectScanMode::Revalidate(reason) => reason,
            SubjectScanMode::Depart => ManagedCopyQuarantine::SubjectTransition,
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

    /// A restored copy is re-sealed under the subject that just admitted it, so
    /// its row never claims a generation this node no longer advertises.
    fn transition(&mut self, record: ManagedCopyRecord, state: ManagedCopyState) -> Effects {
        let generation = match state {
            ManagedCopyState::Registered => self.result.generation,
            _ => record.subject_generation,
        };
        if record.state == state && record.subject_generation == generation {
            return self.next_copy();
        }
        let record = record.sealed_under(generation);
        match transition_effect(&record, state, self.txn_id) {
            Ok(effect) => {
                self.state = ScanState::Transition;
                smallvec![effect]
            }
            Err(error) => self.fail(error.into()),
        }
    }

    /// Serving resumes only once no non-compliant registration remains, and a
    /// departing node never resumes at all.
    fn finish(&mut self) -> Effects {
        let Some(record) = self.record.clone() else {
            return self.fail(SubjectScanError::InvalidEvent("Clear"));
        };
        if self.result.quarantined > 0 || matches!(self.config.mode, SubjectScanMode::Depart) {
            return self.complete();
        }
        self.write_record(record.cleared(), ScanState::Clear)
    }

    fn complete(&mut self) -> Effects {
        debug!(
            generation = self.result.generation,
            scanned = self.result.scanned,
            quarantined = self.result.quarantined,
            "Placement subject scan finished"
        );
        self.output = Some(Ok(self.result.clone()));
        self.state = ScanState::Finish;
        smallvec![]
    }

    fn fail(&mut self, error: SubjectScanError) -> Effects {
        self.output = Some(Err(error));
        self.state = ScanState::Error;
        smallvec![]
    }

    fn handle_record(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(SubjectScanError::InvalidEvent("ReadRecord"));
        };
        let stored = match value
            .as_ref()
            .map(|value| NodeSubjectRecord::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(stored) => stored,
            Err(error) => return self.fail(error.into()),
        };
        let Some(stored) = stored else {
            // A node with no record has never advertised: seed it blocked and
            // let this very scan decide whatever inventory already exists.
            let mut seeded = match NodeSubjectRecord::seed(self.config.observed.clone()) {
                Ok(seeded) => seeded,
                Err(error) => return self.fail(PolicyGateError::from(error).into()),
            };
            seeded.serving_blocked = true;
            seeded.policy_draining = true;
            return self.write_record(seeded, ScanState::WriteRecord);
        };
        match stored.advance(self.config.observed.clone()) {
            Ok(Some(advanced)) => self.write_record(advanced, ScanState::WriteRecord),
            Ok(None) if matches!(self.config.mode, SubjectScanMode::Depart) => {
                // Departure stops new admission even when the subject itself is
                // unchanged, then records the inventory unresolved.
                let mut leaving = stored;
                leaving.serving_blocked = true;
                leaving.policy_draining = true;
                self.write_record(leaving, ScanState::WriteRecord)
            }
            Ok(None) => {
                self.record = Some(stored.clone());
                self.result.generation = stored.subject.generation;
                match stored.serving_blocked {
                    true => self.scan_page(),
                    // Nothing changed and nothing is blocked: no copy can have
                    // become non-compliant, so the scan costs nothing.
                    false => self.complete(),
                }
            }
            Err(error) => self.fail(error.into()),
        }
    }

    fn handle_page(&mut self, event: Event) -> Effects {
        let page = match ManagedCopyPage::decode(event) {
            Ok(page) => page,
            Err(error) => return self.fail(error.into()),
        };
        if page.entries.is_empty() {
            self.cursor = None;
            return self.finish();
        }
        self.cursor = page.cursor;
        self.pending = page.entries.into_iter().map(|(_, record)| record).collect();
        self.pending.reverse();
        self.next_copy()
    }
}

impl Operation for SubjectScanOperation {
    type Output = SubjectScanResult;
    type Error = SubjectScanError;

    fn start(&mut self) -> Effects {
        self.state = ScanState::ReadRecord;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: NODE_SUBJECT_KEYSPACE.to_string(),
            key: Key::from(NODE_SUBJECT_KEY.to_vec()),
            txn_id: self.txn_id,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ScanState::ReadRecord => self.handle_record(event),
            ScanState::WriteRecord => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => self.scan_page(),
                _ => self.fail(SubjectScanError::InvalidEvent("WriteRecord")),
            },
            ScanState::Scan => self.handle_page(event),
            ScanState::Gate => {
                let Some(gate) = self.gate.as_mut() else {
                    return self.fail(SubjectScanError::InvalidEvent("Gate"));
                };
                let effects = gate.step(event);
                match gate.is_complete() {
                    true => self.finish_gate(),
                    false => effects,
                }
            }
            ScanState::Transition => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => self.next_copy(),
                _ => self.fail(SubjectScanError::InvalidEvent("Transition")),
            },
            ScanState::Clear => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => self.complete(),
                _ => self.fail(SubjectScanError::InvalidEvent("Clear")),
            },
            ScanState::Init | ScanState::Finish | ScanState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ScanState::Finish | ScanState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(SubjectScanError::InvalidEvent("Finish")))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Reacts to an observed placement change for this node.
///
/// A node the realm still places revalidates its inventory under the observed
/// subject. A node marked draining, or one the realm no longer places at all,
/// departs: it stops admitting governed data immediately and records every
/// local copy unresolved. Departure is best effort and never blocks the change
/// that caused it.
pub async fn observe_placement(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    now_ms: u64,
) -> Result<Option<SubjectScanResult>, SubjectScanError> {
    let Some(config) = read_realm_config(context, realm_id).await? else {
        return Ok(None);
    };
    let (observed, mode) = match config.placement_entry(node_id) {
        Some(entry) if !entry.draining => (
            storage_subject(entry, 1),
            SubjectScanMode::Revalidate(ManagedCopyQuarantine::SubjectTransition),
        ),
        Some(entry) => (storage_subject(entry, 1), SubjectScanMode::Depart),
        // The realm no longer places this node: an ungraceful removal it only
        // learns about through config sync.
        None => match read_local_subject(context).await? {
            Some(record) => (record.subject, SubjectScanMode::Depart),
            None => return Ok(None),
        },
    };
    drive(
        SubjectScanOperation::new(SubjectScanConfig {
            realm_id,
            observed,
            mode,
            now_ms,
        }),
        context,
    )
    .await
    .map(Some)
}

async fn read_realm_config(
    context: &DriverContext,
    realm_id: RealmId,
) -> Result<Option<RealmConfigDocument>, SubjectScanError> {
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: realm_id.as_bytes().to_vec().into(),
            txn_id: None,
        })
        .await
    else {
        return Err(SubjectScanError::InvalidEvent("ReadRealmConfig"));
    };
    Ok(value
        .map(|value| RealmConfigDocument::from_bytes(value.as_ref()))
        .transpose()?)
}

async fn read_local_subject(
    context: &DriverContext,
) -> Result<Option<NodeSubjectRecord>, SubjectScanError> {
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: NODE_SUBJECT_KEYSPACE.to_string(),
            key: Key::from(NODE_SUBJECT_KEY.to_vec()),
            txn_id: None,
        })
        .await
    else {
        return Err(SubjectScanError::InvalidEvent("ReadLocalSubject"));
    };
    Ok(value
        .map(|value| NodeSubjectRecord::from_bytes(value.as_ref()))
        .transpose()?)
}

/// Reconciles the local subject with the realm's placement map and revalidates
/// the inventory. `Ok(None)` means this node has no placement entry, so it may
/// hold nothing governed and advertises no subject at all.
pub async fn sync_subject(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    mode: SubjectScanMode,
    now_ms: u64,
) -> Result<Option<SubjectScanResult>, SubjectScanError> {
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: realm_id.as_bytes().to_vec().into(),
            txn_id: None,
        })
        .await
    else {
        return Err(SubjectScanError::InvalidEvent("ReadRealmConfig"));
    };
    let Some(config) = value
        .map(|value| RealmConfigDocument::from_bytes(value.as_ref()))
        .transpose()?
    else {
        return Ok(None);
    };
    let Some(entry) = config.placement_entry(node_id) else {
        return Ok(None);
    };
    let observed = storage_subject(entry, 1);
    drive(
        SubjectScanOperation::new(SubjectScanConfig {
            realm_id,
            observed,
            mode,
            now_ms,
        }),
        context,
    )
    .await
    .map(Some)
}

#[cfg(test)]
mod tests {
    use super::{SubjectScanConfig, SubjectScanMode, SubjectScanOperation};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{MANAGED_COPY_KEYSPACE, NODE_SUBJECT_KEYSPACE};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        BackendLocation, BackendRef, ManagedCopyQuarantine, ManagedCopyRecord, ManagedCopyState,
        NodeSubjectRecord, PlacementPolicyRef, PlacementSubject, RealmId, VersionKey,
    };
    use aruna_core::types::NodeId;
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
                compressed: false,
                encrypted: false,
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

    fn stored(record: &NodeSubjectRecord) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(record.to_bytes().expect("record encodes").into()),
        })
    }

    fn page(records: Vec<ManagedCopyRecord>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: records
                .into_iter()
                .map(|record| {
                    (
                        record.key().to_bytes().expect("key encodes").into(),
                        record.to_bytes().expect("record encodes").into(),
                    )
                })
                .collect(),
            next_start_after: None,
        })
    }

    fn written(effects: &[Effect]) -> Option<NodeSubjectRecord> {
        match effects.first() {
            Some(Effect::Storage(StorageEffect::Write {
                key_space, value, ..
            })) if key_space != MANAGED_COPY_KEYSPACE => {
                NodeSubjectRecord::from_bytes(value.as_ref()).ok()
            }
            _ => None,
        }
    }

    #[test]
    fn change_blocks_serving() {
        // The generation advances and serving stops before any copy is looked
        // at, so an unscanned row cannot be served under the new subject.
        let mut operation = operation(
            SubjectScanMode::Revalidate(ManagedCopyQuarantine::SubjectTransition),
            "us-east",
        );
        operation.start();
        let record = NodeSubjectRecord::seed(subject("eu-west")).expect("subject is valid");
        let effects = operation.step(stored(&record));

        let advanced = written(&effects).expect("the record is rewritten first");
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
        operation.start();
        let record = NodeSubjectRecord::seed(subject("eu-west")).expect("subject is valid");
        assert!(operation.step(stored(&record)).is_empty());
        assert!(operation.is_complete());
    }

    #[test]
    fn ungoverned_copy_restores() {
        // Nothing governs the copy, so revalidation clears the block without a
        // single policy fetch.
        let mut operation = operation(
            SubjectScanMode::Revalidate(ManagedCopyQuarantine::Rejoin),
            "eu-west",
        );
        operation.start();
        let mut record = NodeSubjectRecord::seed(subject("eu-west")).expect("subject is valid");
        record.serving_blocked = true;
        operation.step(stored(&record));

        let quarantined = copy(
            ManagedCopyState::Quarantined(ManagedCopyQuarantine::Rejoin),
            Vec::new(),
        );
        let effects = operation.step(page(vec![quarantined]));
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected one copy transition");
        };
        assert_eq!(
            ManagedCopyRecord::from_bytes(value.as_ref())
                .expect("record decodes")
                .state,
            ManagedCopyState::Registered
        );

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        }));
        assert!(
            !written(&effects)
                .expect("the block is cleared")
                .serving_blocked
        );
    }

    #[test]
    fn departure_marks_unresolved() {
        // Departure never restores and is never blocked by what it finds.
        let mut operation = operation(SubjectScanMode::Depart, "eu-west");
        operation.start();
        let record = NodeSubjectRecord::seed(subject("eu-west")).expect("subject is valid");
        operation.step(stored(&record));

        let effects = operation.step(page(vec![copy(ManagedCopyState::Registered, Vec::new())]));
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected one copy transition");
        };
        assert_eq!(
            ManagedCopyRecord::from_bytes(value.as_ref())
                .expect("record decodes")
                .state,
            ManagedCopyState::UnresolvedDeparted
        );
    }

    #[test]
    fn departure_stops_admission() {
        // Departure blocks new admission even when the subject is unchanged, so
        // no governed byte lands here while the inventory is being resolved.
        let mut operation = operation(SubjectScanMode::Depart, "eu-west");
        operation.start();
        let record = NodeSubjectRecord::seed(subject("eu-west")).expect("subject is valid");
        let effects = operation.step(stored(&record));

        let leaving = written(&effects).expect("the record is rewritten first");
        assert!(leaving.serving_blocked && leaving.policy_draining);
    }

    #[test]
    fn scan_touches_no_jobs() {
        // A receipted execution must survive a transition: the scan only ever
        // writes the subject row and managed-copy rows.
        let mut operation = operation(
            SubjectScanMode::Revalidate(ManagedCopyQuarantine::SubjectTransition),
            "us-east",
        );
        let mut effects: Vec<Effect> = operation.start().into_iter().collect();
        let record = NodeSubjectRecord::seed(subject("eu-west")).expect("subject is valid");
        effects.extend(operation.step(stored(&record)));
        effects.extend(operation.step(Event::Storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        })));
        effects.extend(operation.step(page(vec![copy(ManagedCopyState::Registered, Vec::new())])));

        for effect in effects {
            let Effect::Storage(
                StorageEffect::Write { key_space, .. } | StorageEffect::Iter { key_space, .. },
            ) = effect
            else {
                continue;
            };
            assert!(
                key_space == MANAGED_COPY_KEYSPACE || key_space == NODE_SUBJECT_KEYSPACE,
                "the scan touched {key_space}"
            );
        }
    }

    #[test]
    fn restore_reseals_generation() {
        // A restored copy names the subject that just admitted it, so a row
        // sealed under the old generation can never pass for the new one.
        let mut operation = operation(
            SubjectScanMode::Revalidate(ManagedCopyQuarantine::Rejoin),
            "eu-west",
        );
        operation.start();
        let mut record = NodeSubjectRecord::seed(subject("eu-west")).expect("subject is valid");
        record.subject.generation = 4;
        record.serving_blocked = true;
        operation.step(stored(&record));

        let stale = copy(
            ManagedCopyState::Quarantined(ManagedCopyQuarantine::Rejoin),
            Vec::new(),
        );
        let effects = operation.step(page(vec![stale]));
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected one copy transition");
        };
        let restored = ManagedCopyRecord::from_bytes(value.as_ref()).expect("record decodes");
        assert_eq!(restored.state, ManagedCopyState::Registered);
        assert_eq!(restored.subject_generation, 4);
    }

    #[test]
    fn governed_copy_needs_subject() {
        // The gate runs per registration; a governed copy cannot be restored
        // without resolving its rule.
        let mut operation = operation(
            SubjectScanMode::Revalidate(ManagedCopyQuarantine::SubjectTransition),
            "us-east",
        );
        operation.start();
        let record = NodeSubjectRecord::seed(subject("eu-west")).expect("subject is valid");
        operation.step(stored(&record));
        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        }));

        let governed = copy(
            ManagedCopyState::Registered,
            vec![PlacementPolicyRef {
                policy_id: Ulid::from_bytes([1u8; 16]),
                digest: [2u8; 32],
            }],
        );
        let effects = operation.step(page(vec![governed]));
        assert!(
            matches!(
                effects.first(),
                Some(Effect::Storage(StorageEffect::Read { .. }))
            ),
            "the gate must resolve the rule before the copy is decided"
        );
    }
}
