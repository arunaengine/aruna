//! Moves a persistent id mapping through mint, failure or withdrawal in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{DOCUMENT_INDEX_KEYSPACE, METADATA_AUDIT_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{decode, done, encoded_row, read_row};
use aruna_core::storage_entries::metadata_document_key;
use aruna_core::structs::storage::metadata_registry::{
    MetadataAuditOperation, MetadataAuditRecord, MetadataRegistryRecord,
};
use aruna_core::structs::{PersistentIdFailure, PersistentIdMapping, secondary_index_deletes};
use aruna_core::types::{Key, KeySpace};
use ulid::Ulid;

use crate::metadata::call_results::read_value;
use crate::metadata::id_mapping::{
    MappingRoute, PersistentIdError, TransitionEntry, fence_answer, fence_call, mapping_answer,
    mapping_call, mapping_revision, transition_actor, transition_entries,
};
use crate::metadata::repository::metadata_audit_key;

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
enum Step {
    Mapping,
    Registry,
    Fence,
    Write,
    Delete,
}

type PidTurn = Result<Next<PidOutcome>, PersistentIdError>;

/// Runs one transition in one transaction behind the mapping bucket's write fence; the runner
/// reruns it after a commit conflict. A no-op answers with the stored mapping and writes nothing.
#[derive(Debug, PartialEq)]
pub struct PidTransitionOperation {
    route: Option<MappingRoute>,
    document_id: Ulid,
    transition: PidTransition,
    step: Step,
    mapping: Option<PersistentIdMapping>,
    registry: Option<MetadataRegistryRecord>,
    applied: bool,
    deletes: Vec<(KeySpace, Key)>,
}

impl PidTransitionOperation {
    pub fn new(route: Option<MappingRoute>, document_id: Ulid, transition: PidTransition) -> Self {
        Self {
            route,
            document_id,
            transition,
            step: Step::Mapping,
            mapping: None,
            registry: None,
            applied: false,
            deletes: Vec::new(),
        }
    }

    fn read_mapping(&mut self) -> PidTurn {
        self.step = Step::Mapping;
        Ok(Next::Call(mapping_call(self.document_id)))
    }

    fn read_registry(&mut self) -> PidTurn {
        self.step = Step::Registry;
        let key = metadata_document_key(self.document_id);
        Ok(Next::Call(read_row(DOCUMENT_INDEX_KEYSPACE, key)))
    }

    /// Reads the fence, or goes on when the route has no fenced generation.
    fn check_fence(&mut self) -> PidTurn {
        let Some(fence) = fence_call(&self.route) else {
            return self.fenced();
        };
        self.step = Step::Fence;
        Ok(Next::Call(fence))
    }

    /// The step after an admitting fence: mint reads the registry, withdraw the
    /// mapping, and a failure applies right away.
    fn fenced(&mut self) -> PidTurn {
        match self.transition {
            PidTransition::Mint { .. } => self.read_registry(),
            PidTransition::Fail { .. } => self.apply(),
            PidTransition::Withdraw { .. } => self.read_mapping(),
        }
    }

    fn mapping_read(&mut self, result: IoResult) -> PidTurn {
        let mapping = mapping_answer(result)?.ok_or(PersistentIdError::IntentMissing)?;
        let skip_mint = mapping.is_active() || mapping.is_retired();
        if matches!(self.transition, PidTransition::Mint { .. }) && skip_mint {
            return Ok(Next::Done(unchanged(mapping)));
        }
        self.mapping = Some(mapping);
        match self.transition {
            PidTransition::Withdraw { .. } => self.apply(),
            _ => self.check_fence(),
        }
    }

    fn registry_read(&mut self, result: IoResult) -> PidTurn {
        let value = read_value(result)?;
        if let PidTransition::Withdraw { .. } = self.transition {
            let value = value.ok_or(PersistentIdError::DocumentMissing)?;
            self.registry = Some(decode(&value)?);
            return self.check_fence();
        }
        if value.is_none() {
            // Projection has not produced the row yet: record Processing, retryable.
            return self.mint_processing();
        }
        self.apply()
    }

    fn mint_processing(&mut self) -> PidTurn {
        let PidTransition::Mint { minted_at_ms, .. } = self.transition else {
            return Err(PersistentIdError::Unavailable("not a mint".into()));
        };
        let failure = PersistentIdFailure {
            message: "metadata projection is not readable yet".to_string(),
            retryable: true,
            recorded_at_ms: minted_at_ms,
        };
        let revision = mapping_revision(&self.route, minted_at_ms);
        let mapping = self
            .mapping
            .as_mut()
            .ok_or(PersistentIdError::IntentMissing)?;
        mapping.processing(revision, Some(failure));
        self.applied = false;
        self.write(Vec::new())
    }

    /// Applies the transition to the read mapping and writes it; a no-op answers unchanged.
    fn apply(&mut self) -> PidTurn {
        let stored = self
            .mapping
            .take()
            .ok_or(PersistentIdError::IntentMissing)?;
        let mut mapping = stored.clone();
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
            return Ok(Next::Done(unchanged(stored)));
        }
        self.applied = true;
        self.mapping = Some(mapping);
        let audit = self.withdraw_audit()?;
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
        let key = metadata_audit_key(record.group_id, self.document_id, mapping.revision.event_id);
        Ok(vec![encoded_row(METADATA_AUDIT_KEYSPACE, key, &audit)?])
    }

    fn write(&mut self, extra: Vec<TransitionEntry>) -> PidTurn {
        let mapping = self
            .mapping
            .as_ref()
            .ok_or(PersistentIdError::IntentMissing)?;
        let mut writes = transition_entries(&self.route, mapping)?;
        writes.extend(extra);
        if let PidTransition::Withdraw { .. } = self.transition {
            self.deletes = secondary_index_deletes(mapping);
        }
        self.step = Step::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn committed(&mut self) -> PidTurn {
        let mapping = self
            .mapping
            .take()
            .ok_or(PersistentIdError::IntentMissing)?;
        Ok(Next::Done(PidOutcome {
            mapping,
            applied: self.applied,
            committed: true,
        }))
    }
}

/// The outcome of a transition that changed nothing.
fn unchanged(mapping: PersistentIdMapping) -> PidOutcome {
    PidOutcome {
        mapping,
        applied: false,
        committed: false,
    }
}

impl Operation for PidTransitionOperation {
    type Output = PidOutcome;
    type Error = PersistentIdError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> PidTurn {
        match self.transition {
            PidTransition::Withdraw { .. } => self.read_registry(),
            _ => self.read_mapping(),
        }
    }

    fn next(&mut self, result: IoResult) -> PidTurn {
        match self.step {
            Step::Mapping => self.mapping_read(result),
            Step::Registry => self.registry_read(result),
            Step::Fence => {
                fence_answer(&self.route, result)?;
                self.fenced()
            }
            Step::Write => {
                done(result)?;
                if self.deletes.is_empty() {
                    return self.committed();
                }
                self.step = Step::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            Step::Delete => {
                done(result)?;
                self.committed()
            }
        }
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use crate::metadata::secondary_ids::tests::mapping;
    use crate::metadata::timestamp_sweep::state_machine_tests::record_bytes;
    use aruna_core::calls::IoError;
    use aruna_core::errors::StorageError;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::types::Value;

    fn document() -> Ulid {
        Ulid::from_bytes([3; 16])
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

    fn withdraw() -> PidTransition {
        PidTransition::Withdraw {
            withdrawn_by: user(),
            reason: "court order".to_string(),
            withdrawn_at_ms: 90,
        }
    }

    fn row(value: Option<Value>) -> IoResult {
        IoResult::Values(vec![value])
    }

    fn stored(mapping: &PersistentIdMapping) -> IoResult {
        row(Some(mapping.to_bytes().unwrap().into()))
    }

    fn registry_call() -> PidTurn {
        let key = metadata_document_key(document());
        Ok(Next::Call(read_row(DOCUMENT_INDEX_KEYSPACE, key)))
    }

    fn writes(turn: PidTurn) -> Vec<TransitionEntry> {
        let Ok(Next::Call(IoCall::Write(writes))) = turn else {
            panic!("expected one write");
        };
        writes
    }

    #[test]
    fn mint_activates_mapping() {
        let mut operation = PidTransitionOperation::new(None, document(), mint());
        assert_eq!(
            operation.transaction(),
            aruna_core::operation::TransactionMode::Write
        );
        assert_eq!(operation.first(), Ok(Next::Call(mapping_call(document()))));
        assert_eq!(
            operation.next(stored(&mapping(document()))),
            registry_call()
        );
        writes(operation.next(row(Some(vec![1].into()))));
        let Ok(Next::Done(outcome)) = operation.next(IoResult::Done) else {
            panic!("expected the outcome");
        };
        assert!(outcome.mapping.is_active());
        assert!(outcome.applied && outcome.committed);
    }

    #[test]
    fn unprojected_mint_processing() {
        // Without a registry row the mint commits Processing and reports no activation.
        let mut operation = PidTransitionOperation::new(None, document(), mint());
        operation.first().unwrap();
        operation.next(stored(&mapping(document()))).unwrap();
        writes(operation.next(row(None)));
        let Ok(Next::Done(outcome)) = operation.next(IoResult::Done) else {
            panic!("expected the outcome");
        };
        assert!(!outcome.mapping.is_active());
        assert!(!outcome.applied && outcome.committed);
    }

    #[test]
    fn noop_answers_stored() {
        // An active mapping is not minted again; the stored row answers and nothing is written.
        let mut active = mapping(document());
        active.activate(user(), active.revision);
        let mut operation = PidTransitionOperation::new(None, document(), mint());
        operation.first().unwrap();
        assert_eq!(
            operation.next(stored(&active)),
            Ok(Next::Done(unchanged(active)))
        );

        let mut missing = PidTransitionOperation::new(None, document(), failure());
        missing.first().unwrap();
        assert_eq!(
            missing.next(row(None)),
            Err(PersistentIdError::IntentMissing)
        );
    }

    #[test]
    fn withdraw_writes_audit() {
        let mut operation = PidTransitionOperation::new(None, document(), withdraw());
        assert_eq!(operation.first(), registry_call());
        let registry = Some(record_bytes(document(), 10));
        assert_eq!(
            operation.next(row(registry)),
            Ok(Next::Call(mapping_call(document())))
        );
        let written = writes(operation.next(stored(&mapping(document()))));
        assert!(
            written
                .iter()
                .any(|(key_space, _, _)| key_space == METADATA_AUDIT_KEYSPACE)
        );
        let retired = loop {
            match operation.next(IoResult::Done) {
                Ok(Next::Call(IoCall::Delete(_))) => continue,
                Ok(Next::Done(outcome)) => break outcome,
                other => panic!("unexpected turn {other:?}"),
            }
        };
        assert!(retired.mapping.is_retired());

        let mut missing = PidTransitionOperation::new(None, document(), withdraw());
        missing.first().unwrap();
        assert_eq!(
            missing.next(row(None)),
            Err(PersistentIdError::DocumentMissing)
        );
    }

    #[test]
    fn fence_rejects_failure() {
        let route = Some(MappingRoute {
            realm_id: RealmId([5; 32]),
            placement: PlacementRef {
                strategy_id: Ulid::from_bytes([6; 16]),
                shard: 1,
            },
            peers: Vec::new(),
            actor: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            generation: 3,
        });
        let mut operation = PidTransitionOperation::new(route.clone(), document(), failure());
        operation.first().unwrap();
        assert_eq!(
            operation.next(stored(&mapping(document()))),
            Ok(Next::Call(fence_call(&route).unwrap()))
        );
        // A bucket closed at generation 5 no longer admits a transition resolved at 3.
        let closed = row(Some(5u64.to_be_bytes().to_vec().into()));
        assert_eq!(
            operation.next(closed),
            Err(PersistentIdError::PlacementFenced)
        );
    }

    #[test]
    fn conflict_reruns() {
        let conflict = PersistentIdError::from(IoError::Conflict);
        assert!(PidTransitionOperation::is_conflict(&conflict));
        assert_eq!(
            PersistentIdError::from(IoError::Storage(StorageError::Timeout)),
            PersistentIdError::Storage(StorageError::Timeout)
        );
        let mut operation = PidTransitionOperation::new(None, document(), failure());
        operation.first().unwrap();
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(PersistentIdError::Unavailable(_))
        ));
    }
}
