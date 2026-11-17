//! Moves a persistent id mapping through mint, failure or withdrawal in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::METADATA_AUDIT_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::metadata_registry::{
    MetadataAuditOperation, MetadataAuditRecord, MetadataRegistryRecord,
};
use aruna_core::structs::{PersistentIdFailure, PersistentIdMapping, secondary_index_deletes};
use aruna_core::types::{Effects, TxnId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use ulid::Ulid;

use crate::metadata::id_mapping::{
    MappingRoute, PersistentIdError, TransitionEntry, mapping_revision, parse_mapping_read,
    read_mapping_effect, transition_actor, transition_entries,
};
use crate::metadata::repository::{metadata_audit_key, read_document_registry};

/// Storage conflicts are optimistic and short-lived; a caller that exhausts these
/// gets a retryable error rather than a lost transition.
const TRANSITION_ATTEMPTS: usize = 4;

/// The change one transition makes to the mapping.
#[derive(Clone, Debug, PartialEq)]
pub enum PidTransition {
    /// Activates the selected identity, or records `Processing` while the registry
    /// row is not projected yet.
    Mint {
        minted_by: UserId,
        minted_at_ms: u64,
    },
    /// Records a terminal provider failure on the same intent.
    Fail { failure: PersistentIdFailure },
    /// Administrator-only withdrawal, audited in the same transaction.
    Withdraw {
        withdrawn_by: UserId,
        reason: String,
        withdrawn_at_ms: u64,
    },
}

/// The authoritative mapping, whether the transition applied, and whether a
/// commit happened so the caller schedules the outbox drain.
#[derive(Clone, Debug, PartialEq)]
pub struct PidOutcome {
    pub mapping: PersistentIdMapping,
    pub applied: bool,
    pub committed: bool,
}

#[derive(Debug, PartialEq)]
enum State {
    Init,
    Start,
    Mapping,
    Registry,
    Fence,
    Write,
    Delete,
    Commit,
    /// Aborts a no-op transaction before reading the stored mapping outside it.
    Release,
    Existing,
    Aborting,
    Finish,
}

/// Runs one transition behind the mapping bucket's write fence, retrying a commit
/// conflict with a fresh transaction. A no-op answers with the stored mapping.
#[derive(Debug, PartialEq)]
pub struct PidTransitionOperation {
    route: Option<MappingRoute>,
    document_id: Ulid,
    transition: PidTransition,
    state: State,
    attempts: usize,
    txn_id: Option<TxnId>,
    mapping: Option<PersistentIdMapping>,
    registry: Option<MetadataRegistryRecord>,
    applied: bool,
    deletes: Vec<(String, ByteView)>,
    output: Option<Result<PidOutcome, PersistentIdError>>,
}

impl PidTransitionOperation {
    pub fn new(route: Option<MappingRoute>, document_id: Ulid, transition: PidTransition) -> Self {
        Self {
            route,
            document_id,
            transition,
            state: State::Init,
            attempts: 0,
            txn_id: None,
            mapping: None,
            registry: None,
            applied: false,
            deletes: Vec::new(),
            output: None,
        }
    }

    fn begin(&mut self) -> Effects {
        self.attempts += 1;
        self.mapping = None;
        self.registry = None;
        self.deletes = Vec::new();
        self.state = State::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn fail(&mut self, error: PersistentIdError) -> Effects {
        self.output = Some(Err(error));
        self.abort()
    }

    fn unexpected(&mut self, event: &Event) -> Effects {
        let message = format!(
            "unexpected persistent id event {} in state {:?}",
            event.label(),
            self.state
        );
        self.fail(PersistentIdError::Unavailable(message))
    }

    fn read(&mut self, state: State, effect: Effect) -> Effects {
        self.state = state;
        smallvec![effect]
    }

    fn read_mapping(&mut self) -> Effects {
        let effect = read_mapping_effect(self.document_id, self.txn_id);
        self.read(State::Mapping, effect)
    }

    fn read_registry(&mut self) -> Effects {
        let effect = read_document_registry(self.document_id, self.txn_id);
        self.read(State::Registry, effect)
    }

    /// Reads the fence, or goes on when the route has no fenced generation.
    fn check_fence(&mut self) -> Effects {
        let Some(route) = self.route.as_ref().filter(|route| route.generation > 0) else {
            return self.fenced();
        };
        let (key_space, key) =
            crate::placement::fence::fence_read(&route.realm_id, &route.placement);
        let effect = Effect::Storage(StorageEffect::Read {
            key_space,
            key,
            txn_id: self.txn_id,
        });
        self.read(State::Fence, effect)
    }

    /// The step after an admitting fence: mint reads the registry, withdraw the
    /// mapping, and a failure applies right away.
    fn fenced(&mut self) -> Effects {
        match self.transition {
            PidTransition::Mint { .. } => self.read_registry(),
            PidTransition::Fail { .. } => self.apply(),
            PidTransition::Withdraw { .. } => self.read_mapping(),
        }
    }

    fn mapping_read(&mut self, event: Event) -> Effects {
        let mapping = match parse_mapping_read(event) {
            Ok(Some(mapping)) => mapping,
            Ok(None) => return self.fail(PersistentIdError::IntentMissing),
            Err(error) => return self.fail(error),
        };
        let skip_mint = mapping.is_active() || mapping.is_retired();
        self.mapping = Some(mapping);
        match self.transition {
            PidTransition::Mint { .. } if skip_mint => self.release(),
            PidTransition::Withdraw { .. } => self.apply(),
            _ => self.check_fence(),
        }
    }

    fn registry_read(&mut self, value: Option<Value>) -> Effects {
        if let PidTransition::Withdraw { .. } = self.transition {
            let Some(bytes) = value else {
                return self.fail(PersistentIdError::DocumentMissing);
            };
            match postcard::from_bytes(bytes.as_ref()) {
                Ok(record) => self.registry = Some(record),
                Err(error) => return self.fail(PersistentIdError::Conversion(error.into())),
            }
            return self.check_fence();
        }
        self.registry = None;
        if value.is_none() {
            // Projection has not produced the row yet: record Processing, retryable.
            return self.mint_processing();
        }
        self.apply()
    }

    fn mint_processing(&mut self) -> Effects {
        let PidTransition::Mint { minted_at_ms, .. } = self.transition else {
            return self.fail(PersistentIdError::Unavailable("not a mint".into()));
        };
        let failure = PersistentIdFailure {
            message: "metadata projection is not readable yet".to_string(),
            retryable: true,
            recorded_at_ms: minted_at_ms,
        };
        let revision = mapping_revision(&self.route, minted_at_ms);
        let Some(mapping) = self.mapping.as_mut() else {
            return self.fail(PersistentIdError::IntentMissing);
        };
        mapping.processing(revision, Some(failure));
        self.applied = false;
        self.write(Vec::new())
    }

    /// Applies the transition to the read mapping and writes it, or releases a no-op.
    fn apply(&mut self) -> Effects {
        let Some(mapping) = self.mapping.as_mut() else {
            return self.fail(PersistentIdError::IntentMissing);
        };
        let changed = match &self.transition {
            PidTransition::Mint {
                minted_by,
                minted_at_ms,
            } => mapping.activate(*minted_by, mapping_revision(&self.route, *minted_at_ms)),
            PidTransition::Fail { failure } => mapping.fail(
                failure.clone(),
                mapping_revision(&self.route, failure.recorded_at_ms),
            ),
            PidTransition::Withdraw {
                withdrawn_by,
                reason,
                withdrawn_at_ms,
            } => mapping.admin_withdraw(
                *withdrawn_by,
                reason.clone(),
                mapping_revision(&self.route, *withdrawn_at_ms),
            ),
        };
        if !changed {
            return self.release();
        }
        self.applied = true;
        let audit = match self.withdraw_audit() {
            Ok(audit) => audit,
            Err(error) => return self.fail(error),
        };
        self.write(audit)
    }

    /// The audit row of a withdrawal; other transitions write none.
    fn withdraw_audit(&self) -> Result<Vec<TransitionEntry>, PersistentIdError> {
        let PidTransition::Withdraw {
            withdrawn_by,
            reason,
            withdrawn_at_ms,
        } = &self.transition
        else {
            return Ok(Vec::new());
        };
        let (Some(record), Some(mapping)) = (self.registry.as_ref(), self.mapping.as_ref()) else {
            return Err(PersistentIdError::DocumentMissing);
        };
        let audit = MetadataAuditRecord {
            realm_id: record.realm_id,
            group_id: record.group_id,
            document_id: self.document_id,
            graph_iri: record.graph_iri.clone(),
            user_id: *withdrawn_by,
            node_id: transition_actor(&self.route),
            operation: MetadataAuditOperation::WithdrawPersistentId,
            occurred_at_ms: *withdrawn_at_ms,
            details: Some(format!("provider=w3id; reason={reason}")),
        };
        let value = postcard::to_allocvec(&audit)
            .map_err(|error| PersistentIdError::Conversion(error.into()))?;
        Ok(vec![(
            METADATA_AUDIT_KEYSPACE.to_string(),
            metadata_audit_key(record.group_id, self.document_id, mapping.revision.event_id),
            ByteView::from(value),
        )])
    }

    fn write(&mut self, extra: Vec<TransitionEntry>) -> Effects {
        let Some(mapping) = self.mapping.as_ref() else {
            return self.fail(PersistentIdError::IntentMissing);
        };
        let mut writes = match transition_entries(&self.route, mapping) {
            Ok(writes) => writes,
            Err(error) => return self.fail(error),
        };
        writes.extend(extra);
        if let PidTransition::Withdraw { .. } = self.transition {
            self.deletes = secondary_index_deletes(mapping);
        }
        self.state = State::Write;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    fn written(&mut self) -> Effects {
        if self.deletes.is_empty() {
            return self.commit();
        }
        self.state = State::Delete;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: std::mem::take(&mut self.deletes),
            txn_id: self.txn_id,
        })]
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(PersistentIdError::Unavailable("transaction missing".into()));
        };
        self.state = State::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn committed(&mut self) -> Effects {
        self.txn_id = None;
        self.state = State::Finish;
        self.output = self.mapping.take().map(|mapping| {
            Ok(PidOutcome {
                mapping,
                applied: self.applied,
                committed: true,
            })
        });
        smallvec![]
    }

    /// Commit errors other than a conflict leave no transaction to abort.
    fn commit_failed(&mut self, error: StorageError) -> Effects {
        self.txn_id = None;
        match error {
            StorageError::TransactionConflict if self.attempts < TRANSITION_ATTEMPTS => {
                self.begin()
            }
            StorageError::TransactionConflict => self.fail(PersistentIdError::Storage(error)),
            error => self.fail(PersistentIdError::Unavailable(error.to_string())),
        }
    }

    fn release(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id.take() else {
            return self.read_existing();
        };
        self.state = State::Release;
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    fn read_existing(&mut self) -> Effects {
        let effect = read_mapping_effect(self.document_id, None);
        self.read(State::Existing, effect)
    }

    fn existing_read(&mut self, event: Event) -> Effects {
        let missing = match self.transition {
            PidTransition::Mint { .. } => PersistentIdError::DocumentMissing,
            PidTransition::Fail { .. } => PersistentIdError::IntentMissing,
            PidTransition::Withdraw { .. } => PersistentIdError::Storage(StorageError::ReadError(
                "persistent id mapping missing".to_string(),
            )),
        };
        self.state = State::Finish;
        self.output = Some(parse_mapping_read(event).and_then(|mapping| {
            let mapping = mapping.ok_or(missing)?;
            Ok(PidOutcome {
                mapping,
                applied: false,
                committed: false,
            })
        }));
        smallvec![]
    }
}

impl Operation for PidTransitionOperation {
    type Output = PidOutcome;
    type Error = PersistentIdError;

    fn start(&mut self) -> Effects {
        self.begin()
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (State::Aborting, _) => {
                self.state = State::Finish;
                smallvec![]
            }
            // A failed abort leaves nothing to undo here; the stored row still answers.
            (State::Release, _) => self.read_existing(),
            (State::Existing, event) => self.existing_read(event),
            (State::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                match self.transition {
                    PidTransition::Withdraw { .. } => self.read_registry(),
                    _ => self.read_mapping(),
                }
            }
            (State::Mapping, event) => self.mapping_read(event),
            (State::Registry, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.registry_read(value)
            }
            (State::Fence, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let generation = self.route.as_ref().map_or(0, |route| route.generation);
                if !crate::placement::fence::admits(value.as_ref(), generation) {
                    return self.fail(PersistentIdError::PlacementFenced);
                }
                self.fenced()
            }
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => self.written(),
            (State::Delete, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                self.commit()
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.committed()
            }
            (State::Commit, Event::Storage(StorageEvent::Error { error })) => {
                self.commit_failed(error)
            }
            (_, Event::Storage(StorageEvent::Error { error })) => {
                self.fail(PersistentIdError::Storage(error))
            }
            (_, event) => self.unexpected(&event),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == State::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(PersistentIdError::Unavailable(
            "persistent id transition did not finish".into(),
        )))
    }

    // An open transaction is aborted and its answer awaited; without one the run ends.
    fn abort(&mut self) -> Effects {
        let txn_id = self.txn_id.take();
        self.state = if txn_id.is_some() {
            State::Aborting
        } else {
            State::Finish
        };
        let abort = |txn_id| Effect::Storage(StorageEffect::AbortTransaction { txn_id });
        txn_id.map(abort).into_iter().collect()
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use crate::metadata::secondary_ids::tests::{mapping, read_event};
    use crate::metadata::timestamp_sweep::state_machine_tests::record_bytes;
    use aruna_core::operation_check::{
        check_abort, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;

    fn document() -> Ulid {
        Ulid::from_bytes([3; 16])
    }

    fn txn() -> TxnId {
        Ulid::from_bytes([9; 16])
    }

    fn user() -> UserId {
        UserId::local(Ulid::from_bytes([2; 16]), RealmId([3; 32]))
    }

    fn mint() -> PidTransition {
        PidTransition::Mint {
            minted_by: user(),
            minted_at_ms: 70,
        }
    }

    fn failure() -> PidTransition {
        PidTransition::Fail {
            failure: PersistentIdFailure {
                message: "provider refused".to_string(),
                retryable: false,
                recorded_at_ms: 80,
            },
        }
    }

    fn started(transition: PidTransition, route: Option<MappingRoute>) -> PidTransitionOperation {
        let mut operation = PidTransitionOperation::new(route, document(), transition);
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: txn(),
        }));
        operation
    }

    fn registry(value: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(Vec::new()),
            value: value.map(ByteView::from),
        })
    }

    fn written() -> Event {
        Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        })
    }

    fn committed() -> Event {
        Event::Storage(StorageEvent::TransactionCommitted { txn_id: txn() })
    }

    fn aborted() -> Event {
        Event::Storage(StorageEvent::TransactionAborted { txn_id: txn() })
    }

    fn storage_error(error: StorageError) -> Event {
        Event::Storage(StorageEvent::Error { error })
    }

    fn is_commit(effects: &Effects) -> bool {
        effects.as_slice()
            == [Effect::Storage(StorageEffect::CommitTransaction {
                txn_id: txn(),
            })]
    }

    fn is_abort(effects: &Effects) -> bool {
        effects.as_slice()
            == [Effect::Storage(StorageEffect::AbortTransaction {
                txn_id: txn(),
            })]
    }

    #[test]
    fn mint_activates_mapping() {
        let mut operation = PidTransitionOperation::new(None, document(), mint());
        assert_eq!(
            operation.start().as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        );
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: txn(),
        }));
        assert_eq!(
            effects.as_slice(),
            [read_mapping_effect(document(), Some(txn()))]
        );
        let effects = operation.step(read_event(Some(&mapping(document()))));
        assert_eq!(
            effects.as_slice(),
            [read_document_registry(document(), Some(txn()))]
        );
        let effects = operation.step(registry(Some(vec![1])));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchWrite {
                txn_id: Some(_),
                ..
            })]
        ));
        assert!(is_commit(&operation.step(written())));
        assert!(operation.step(committed()).is_empty());
        let outcome = operation.finalize().unwrap();
        assert!(outcome.mapping.is_active());
        assert!(outcome.applied && outcome.committed);
    }

    #[test]
    fn unprojected_mint_processing() {
        // Without a registry row the mint commits Processing and reports no activation.
        let mut operation = started(mint(), None);
        operation.step(read_event(Some(&mapping(document()))));
        operation.step(registry(None));
        operation.step(written());
        operation.step(committed());
        let outcome = operation.finalize().unwrap();
        assert!(!outcome.mapping.is_active());
        assert!(!outcome.applied && outcome.committed);
    }

    #[test]
    fn noop_reads_existing() {
        // An active mapping is not minted again; the stored row answers outside the transaction.
        let mut active = mapping(document());
        active.activate(user(), active.revision);
        let mut operation = started(mint(), None);
        assert!(is_abort(&operation.step(read_event(Some(&active)))));
        assert_eq!(
            operation.step(aborted()).as_slice(),
            [read_mapping_effect(document(), None)]
        );
        assert!(operation.step(read_event(Some(&active))).is_empty());
        let outcome = operation.finalize().unwrap();
        assert_eq!(outcome.mapping, active);
        assert!(!outcome.applied && !outcome.committed);

        let mut gone = started(mint(), None);
        gone.step(read_event(Some(&active)));
        gone.step(aborted());
        gone.step(read_event(None));
        assert_eq!(gone.finalize(), Err(PersistentIdError::DocumentMissing));
    }

    #[test]
    fn withdraw_writes_audit() {
        let transition = PidTransition::Withdraw {
            withdrawn_by: user(),
            reason: "court order".to_string(),
            withdrawn_at_ms: 90,
        };
        let mut operation = started(transition.clone(), None);
        let effects = operation.step(registry(Some(record_bytes(document(), 10).to_vec())));
        assert_eq!(
            effects.as_slice(),
            [read_mapping_effect(document(), Some(txn()))]
        );
        let effects = operation.step(read_event(Some(&mapping(document()))));
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected one batch write, got {effects:?}");
        };
        assert!(
            writes
                .iter()
                .any(|(key_space, _, _)| key_space == METADATA_AUDIT_KEYSPACE)
        );
        assert!(is_commit(&operation.step(written())));
        operation.step(committed());
        assert!(operation.finalize().unwrap().mapping.is_retired());

        let mut missing = started(transition, None);
        assert!(is_abort(&missing.step(registry(None))));
        missing.step(aborted());
        assert_eq!(missing.finalize(), Err(PersistentIdError::DocumentMissing));
    }

    #[test]
    fn fence_rejects_failure() {
        let route = MappingRoute {
            realm_id: RealmId([5; 32]),
            placement: PlacementRef {
                strategy_id: Ulid::from_bytes([6; 16]),
                shard: 1,
            },
            peers: Vec::new(),
            actor: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            generation: 3,
        };
        let mut operation = started(failure(), Some(route));
        let effects = operation.step(read_event(Some(&mapping(document()))));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read {
                txn_id: Some(_),
                ..
            })]
        ));
        // A bucket closed at generation 5 no longer admits a transition resolved at 3.
        let fence = registry(Some(5u64.to_be_bytes().to_vec()));
        assert!(is_abort(&operation.step(fence)));
        operation.step(aborted());
        assert_eq!(
            operation.finalize(),
            Err(PersistentIdError::PlacementFenced)
        );
    }

    #[test]
    fn conflict_exhausts_retries() {
        let mut operation = started(failure(), None);
        for attempt in 1..=TRANSITION_ATTEMPTS {
            if attempt > 1 {
                operation.step(Event::Storage(StorageEvent::TransactionStarted {
                    txn_id: txn(),
                }));
            }
            operation.step(read_event(Some(&mapping(document()))));
            operation.step(written());
            let effects = operation.step(storage_error(StorageError::TransactionConflict));
            let restarted = effects.as_slice()
                == [Effect::Storage(StorageEffect::StartTransaction {
                    read: false,
                })];
            assert_eq!(restarted, attempt < TRANSITION_ATTEMPTS);
        }
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize(),
            Err(PersistentIdError::Storage(
                StorageError::TransactionConflict
            ))
        );
    }

    #[test]
    fn storage_errors_fail() {
        // Before a transaction nothing is aborted; inside one the abort runs first.
        let mut unstarted = PidTransitionOperation::new(None, document(), failure());
        unstarted.start();
        assert!(
            unstarted
                .step(storage_error(StorageError::Timeout))
                .is_empty()
        );
        assert_eq!(
            unstarted.finalize(),
            Err(PersistentIdError::Storage(StorageError::Timeout))
        );

        let mut reading = started(failure(), None);
        assert!(is_abort(
            &reading.step(storage_error(StorageError::Timeout))
        ));
        reading.step(aborted());
        assert_eq!(
            reading.finalize(),
            Err(PersistentIdError::Storage(StorageError::Timeout))
        );

        let mut committing = started(failure(), None);
        committing.step(read_event(Some(&mapping(document()))));
        committing.step(written());
        assert!(
            committing
                .step(storage_error(StorageError::Timeout))
                .is_empty()
        );
        assert!(matches!(
            committing.finalize(),
            Err(PersistentIdError::Unavailable(_))
        ));
    }

    #[test]
    fn rejects_other_events() {
        let starting = || {
            let mut operation = PidTransitionOperation::new(None, document(), failure());
            operation.start();
            operation
        };
        let accepted = |event: &Event| {
            matches!(
                event,
                Event::Storage(
                    StorageEvent::TransactionStarted { .. } | StorageEvent::Error { .. }
                )
            )
        };
        for (_, effects, error) in rejects_events(starting, sample_events, accepted) {
            assert!(effects.is_empty());
            assert!(matches!(error, PersistentIdError::Unavailable(_)));
        }
    }

    #[test]
    fn abort_releases_transaction() {
        let mut idle = PidTransitionOperation::new(None, document(), failure());
        check_abort(&mut idle, &[]);
        let mut open = started(failure(), None);
        check_abort(
            &mut open,
            &[Effect::Storage(StorageEffect::AbortTransaction {
                txn_id: txn(),
            })],
        );
    }

    #[test]
    fn finalize_needs_result() {
        rejects_finalize(PidTransitionOperation::new(None, document(), failure()));
        rejects_finalize(started(failure(), None));
    }
}
