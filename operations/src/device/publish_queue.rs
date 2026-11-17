//! Storage shape, keys and limits of the device-local publish queue.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::device::retry::RetryView;
use aruna_core::UserId;
use aruna_core::effects::{Effect, IterStart, StorageEffect, abort_effects};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DEVICE_FENCE_KEYSPACE, DEVICE_INTAKE_KEYSPACE};
use aruna_core::metadata::{AuthToken, MetadataBatch, MetadataBatchSource};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::scan_rows::{ScanRowsError, ScanRowsOperation};
use crate::auth::error::token_grant;

/// Entries one device may hold. A device authors for one person, so the queue
/// is a human-sized backlog rather than an unbounded spool.
pub const MAX_PUBLISH_ENTRIES: usize = 256;

/// Entries one drain pass reads at a time.
pub const PUBLISH_PAGE_SIZE: usize = 64;

/// Forward attempts before a still-retryable entry parks for the owner. An
/// entry that keeps failing must stay visible instead of retrying forever.
pub const MAX_PUBLISH_ATTEMPTS: u32 = 8;

/// What the owner asked the device to publish once the realm is reachable: a
/// create of a new document, or an edit already applied to a local replica.
/// A delete of shared state still needs connectivity and is never queued.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishEntry {
    /// Stable local reference. It never becomes the realm document id.
    pub draft_id: Ulid,
    /// The owner the drain forwards as; the holder re-checks the binding.
    pub owner: UserId,
    pub group_id: GroupId,
    pub document_path: String,
    pub public: bool,
    /// RO-Crate JSON-LD exactly as the owner authored it. Empty for an edit,
    /// whose submission travels inside the kind.
    pub jsonld: String,
    pub created_at_ms: u64,
    pub state: PublishState,
    pub kind: PublishKind,
}

/// What one entry publishes. Appended after the create fields so a create
/// entry keeps describing itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublishKind {
    Create,
    /// An OR-Set change set the owner already applied to this device's replica.
    /// A holder appends it as an `ApplyBatch` event, unchanged.
    Edit {
        document_id: Ulid,
        batch: Box<MetadataBatch>,
        authored: MetadataBatchSource,
    },
}

/// Lifecycle of one queued create.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublishState {
    Pending {
        due_at_ms: u64,
        attempts: u32,
        last_error: Option<String>,
    },
    /// The realm document id is minted before the first forward, so a crash
    /// mid-publish re-forwards the same id and the holder's create-acceptance
    /// fence answers with the original record instead of a second document.
    Publishing {
        document_id: Ulid,
        due_at_ms: u64,
        attempts: u32,
    },
    Published {
        document_id: Ulid,
    },
    Failed {
        reason: String,
        retryable: bool,
        document_id: Option<Ulid>,
    },
}

impl PublishEntry {
    pub fn new(
        draft_id: Ulid,
        owner: UserId,
        group_id: GroupId,
        document_path: String,
        public: bool,
        jsonld: String,
    ) -> Self {
        let now = unix_timestamp_millis();
        Self {
            draft_id,
            owner,
            group_id,
            document_path,
            public,
            jsonld,
            created_at_ms: now,
            state: PublishState::Pending {
                due_at_ms: now,
                attempts: 0,
                last_error: None,
            },
            kind: PublishKind::Create,
        }
    }

    /// One offline edit of a document this device holds a replica of. The batch
    /// is already merged locally; queueing only records that a holder has not
    /// seen it yet.
    pub fn edit(
        draft_id: Ulid,
        owner: UserId,
        record: &MetadataRegistryRecord,
        batch: MetadataBatch,
        authored: MetadataBatchSource,
    ) -> Self {
        let now = unix_timestamp_millis();
        Self {
            draft_id,
            owner,
            group_id: record.group_id,
            document_path: record.document_path.clone(),
            public: record.public,
            jsonld: String::new(),
            created_at_ms: now,
            state: PublishState::Pending {
                due_at_ms: now,
                attempts: 0,
                last_error: None,
            },
            kind: PublishKind::Edit {
                document_id: record.document_id,
                batch: Box::new(batch),
                authored,
            },
        }
    }

    /// The document this entry publishes onto, once it is known.
    pub fn document_id(&self) -> Option<Ulid> {
        match &self.kind {
            PublishKind::Edit { document_id, .. } => Some(*document_id),
            PublishKind::Create => match &self.state {
                PublishState::Publishing { document_id, .. }
                | PublishState::Published { document_id }
                | PublishState::Failed {
                    document_id: Some(document_id),
                    ..
                } => Some(*document_id),
                PublishState::Pending { .. }
                | PublishState::Failed {
                    document_id: None, ..
                } => None,
            },
        }
    }

    /// Re-arms a retryable failure for an explicit owner-requested sync.
    pub fn retry_failed(&self, now_ms: u64) -> Option<PublishState> {
        if !matches!(
            self.state,
            PublishState::Failed {
                retryable: true,
                ..
            }
        ) {
            return None;
        }
        Some(match self.document_id() {
            Some(document_id) => PublishState::Publishing {
                document_id,
                due_at_ms: now_ms,
                attempts: 0,
            },
            None => PublishState::Pending {
                due_at_ms: now_ms,
                attempts: 0,
                last_error: None,
            },
        })
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// Whether the drain may pick this entry up now.
    pub fn is_due(&self, now_ms: u64) -> bool {
        self.state.is_due(now_ms)
    }

    /// Attempts already spent, so a park decision does not have to match on
    /// the state twice.
    pub fn attempts(&self) -> u32 {
        self.state.attempts()
    }
}

/// The same entry under a new lifecycle state; everything the owner authored
/// stays frozen so a retry forwards the identical create.
pub fn entry_with_state(entry: &PublishEntry, state: PublishState) -> PublishEntry {
    PublishEntry {
        state,
        ..entry.clone()
    }
}

/// Draft ids are ULIDs, so the raw key orders the queue by creation time and a
/// forward scan is FIFO.
pub fn publish_key(draft_id: Ulid) -> Key {
    ByteView::from(draft_id.to_bytes().to_vec())
}

pub fn publish_entry(entry: &PublishEntry) -> Result<(String, Key, Value), ConversionError> {
    Ok((
        DEVICE_INTAKE_KEYSPACE.to_string(),
        publish_key(entry.draft_id),
        ByteView::from(entry.to_bytes()?),
    ))
}

/// The issue time of the token that authored one queued draft.
pub fn fence_entry(draft_id: Ulid, issued_at: u64) -> (String, Key, Value) {
    (
        DEVICE_FENCE_KEYSPACE.to_string(),
        publish_key(draft_id),
        ByteView::from(issued_at.to_be_bytes().to_vec()),
    )
}

/// The issue time of the bearer token a write came with. Zero without one, so only an owner
/// without any token cutoff can replay it.
pub fn token_issued_at(auth_token: Option<&AuthToken>) -> u64 {
    match auth_token {
        Some(AuthToken::Bearer(token)) => token_grant(token.as_str())
            .map(|grant| grant.issued_at_secs)
            .unwrap_or(0),
        _ => 0,
    }
}

pub fn scan_publish_queue(start_after: Option<Key>, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: DEVICE_INTAKE_KEYSPACE.to_string(),
        prefix: None,
        start: start_after.map(IterStart::After),
        limit: PUBLISH_PAGE_SIZE,
        txn_id,
    })
}

pub fn read_publish_entry(draft_id: Ulid, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: DEVICE_INTAKE_KEYSPACE.to_string(),
        key: publish_key(draft_id),
        txn_id,
    })
}

#[derive(Debug, Error, PartialEq)]
pub enum ClaimError {
    #[error("unexpected {0} while claiming a queued draft")]
    UnexpectedEvent(&'static str),
    #[error("claiming a queued draft did not finish")]
    NotFinished,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClaimStep {
    Init,
    Start,
    Read,
    Write,
    Commit,
    Abort,
    Finish,
}

/// Stores `next` only while the entry still carries the state the scan read, and outputs
/// whether that committed. A delete committed in between therefore wins instead of the entry
/// coming back as `Publishing`.
#[derive(Debug, PartialEq)]
pub struct ClaimStateOperation {
    entry: PublishEntry,
    next: PublishState,
    step: ClaimStep,
    txn_id: Option<TxnId>,
    output: Option<Result<bool, ClaimError>>,
}

impl ClaimStateOperation {
    pub fn new(entry: PublishEntry, next: PublishState) -> Self {
        Self {
            entry,
            next,
            step: ClaimStep::Init,
            txn_id: None,
            output: None,
        }
    }

    /// Ends with `output`, aborting the transaction first when one is open.
    fn end(&mut self, output: Result<bool, ClaimError>) -> Effects {
        self.output = Some(output);
        match self.txn_id.take() {
            Some(txn_id) => {
                self.step = ClaimStep::Abort;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => {
                self.step = ClaimStep::Finish;
                smallvec![]
            }
        }
    }

    fn write(&mut self, current: Option<PublishEntry>) -> Effects {
        if !current.is_some_and(|current| current.state == self.entry.state) {
            return self.end(Ok(false));
        }
        let claimed = entry_with_state(&self.entry, self.next.clone());
        let Ok((key_space, key, value)) = publish_entry(&claimed) else {
            warn!(draft_id = %self.entry.draft_id, "Failed to encode a queued draft");
            return self.end(Ok(false));
        };
        self.step = ClaimStep::Write;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: self.txn_id,
        })]
    }
}

impl Operation for ClaimStateOperation {
    type Output = bool;
    type Error = ClaimError;

    fn start(&mut self) -> Effects {
        self.step = ClaimStep::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let draft_id = self.entry.draft_id;
        match (self.step, event) {
            (ClaimStep::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.step = ClaimStep::Read;
                smallvec![read_publish_entry(draft_id, Some(txn_id))]
            }
            (ClaimStep::Start, Event::Storage(StorageEvent::Error { .. })) => {
                warn!(%draft_id, "Failed to open a transaction for a queued draft");
                self.end(Ok(false))
            }
            (ClaimStep::Read, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let current = value.and_then(|bytes| PublishEntry::from_bytes(&bytes).ok());
                self.write(current)
            }
            (ClaimStep::Read | ClaimStep::Write, Event::Storage(StorageEvent::Error { .. })) => {
                self.end(Ok(false))
            }
            (ClaimStep::Write, Event::Storage(StorageEvent::WriteResult { .. })) => {
                let Some(txn_id) = self.txn_id.take() else {
                    return self.end(Ok(false));
                };
                self.step = ClaimStep::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (ClaimStep::Commit, event) => {
                let committed = matches!(
                    event,
                    Event::Storage(StorageEvent::TransactionCommitted { .. })
                );
                self.end(Ok(committed))
            }
            (ClaimStep::Abort, _) => {
                self.step = ClaimStep::Finish;
                smallvec![]
            }
            (_, event) => self.end(Err(ClaimError::UnexpectedEvent(event.label()))),
        }
    }

    fn is_complete(&self) -> bool {
        self.step == ClaimStep::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(ClaimError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        abort_effects(&mut self.txn_id)
    }
}

/// Moves every failed, retryable queued draft back into the queue at `now_ms`, each only while
/// it still carries the state the scan read. Outputs how many were moved back.
#[derive(Debug, PartialEq)]
pub struct RequeueFailedOperation {
    now_ms: u64,
    scan: Option<ScanRowsOperation>,
    /// Drafts still to move back with their next state, the next one last.
    pending: Vec<(PublishEntry, PublishState)>,
    claim: Option<ClaimStateOperation>,
    requeued: usize,
    output: Option<Result<usize, ScanRowsError>>,
}

impl RequeueFailedOperation {
    pub fn new(now_ms: u64) -> Self {
        Self {
            now_ms,
            scan: None,
            pending: Vec::new(),
            claim: None,
            requeued: 0,
            output: None,
        }
    }

    fn next_claim(&mut self) -> Effects {
        let Some((entry, state)) = self.pending.pop() else {
            self.output = Some(Ok(self.requeued));
            return smallvec![];
        };
        let mut claim = ClaimStateOperation::new(entry, state);
        let effects = claim.start();
        self.claim = Some(claim);
        effects
    }
}

impl Operation for RequeueFailedOperation {
    type Output = usize;
    type Error = ScanRowsError;

    fn start(&mut self) -> Effects {
        let mut scan = ScanRowsOperation::new(DEVICE_INTAKE_KEYSPACE, None, PUBLISH_PAGE_SIZE);
        let effects = scan.start();
        self.scan = Some(scan);
        effects
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Some(mut claim) = self.claim.take() {
            let effects = claim.step(event);
            if !claim.is_complete() {
                self.claim = Some(claim);
                return effects;
            }
            if claim.finalize() == Ok(true) {
                self.requeued += 1;
            }
            return self.next_claim();
        }
        let Some(mut scan) = self.scan.take() else {
            self.output = Some(Err(ScanRowsError::UnexpectedEvent(event.label())));
            return smallvec![];
        };
        let effects = scan.step(event);
        if !scan.is_complete() {
            self.scan = Some(scan);
            return effects;
        }
        let rows = match scan.finalize() {
            Ok(rows) => rows,
            Err(error) => {
                self.output = Some(Err(error));
                return smallvec![];
            }
        };
        let entries = rows
            .iter()
            .filter_map(|bytes| PublishEntry::from_bytes(bytes).ok());
        self.pending = entries
            .filter_map(|entry| Some((entry.retry_failed(self.now_ms)?, entry)))
            .map(|(state, entry)| (entry, state))
            .collect();
        self.pending.reverse();
        self.next_claim()
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(ScanRowsError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match &mut self.claim {
            Some(claim) => claim.abort(),
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{
        ClaimError, ClaimStateOperation, PUBLISH_PAGE_SIZE, PublishEntry, PublishState,
        RequeueFailedOperation, entry_with_state, publish_entry, publish_key, read_publish_entry,
    };
    use crate::device::scan_rows::ScanRowsOperation;
    use crate::tests::device::record;
    use aruna_core::UserId;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::metadata::{MetadataBatch, MetadataBatchSource, MetadataDot, MetadataQuadOp};
    use aruna_core::operation::Operation;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use craqle::VectorClock;
    use ulid::Ulid;

    fn batch() -> MetadataBatch {
        MetadataBatch {
            graph_iri: record().graph_iri,
            actor: [7u8; 32],
            counter: 1,
            base_clock: VectorClock::default(),
            ops: vec![MetadataQuadOp::Add {
                subject: "<https://example.org/s>".to_string(),
                predicate: "<https://example.org/p>".to_string(),
                object: "\"o\"".to_string(),
                dot: MetadataDot {
                    actor: [7u8; 32],
                    counter: 1,
                },
            }],
            timestamp_millis: 9,
        }
    }

    fn entry() -> PublishEntry {
        PublishEntry::new(
            Ulid::from_parts(1, 1),
            UserId::local(Ulid::from_parts(2, 2), RealmId::from_bytes([7u8; 32])),
            Ulid::from_parts(3, 3),
            "/notes".to_string(),
            false,
            "{}".to_string(),
        )
    }

    #[test]
    fn keys_draft_id() {
        // The raw ULID is the key, so a forward scan is creation order.
        let entry = entry();
        assert_eq!(
            publish_key(entry.draft_id).as_ref(),
            entry.draft_id.to_bytes().as_slice()
        );
    }

    #[test]
    fn round_trips_entry() {
        let entry = entry();
        let bytes = entry.to_bytes().unwrap();
        assert_eq!(PublishEntry::from_bytes(&bytes).unwrap(), entry);
    }

    #[test]
    fn round_trips_edit() {
        // The batch is the only copy of what the owner changed offline, so it
        // has to survive the store byte for byte.
        let entry = PublishEntry::edit(
            Ulid::from_parts(4, 4),
            UserId::local(Ulid::from_parts(5, 5), RealmId::from_bytes([3u8; 32])),
            &record(),
            batch(),
            MetadataBatchSource::UpsertDataEntity {
                jsonld: r#"{"@id":"data.csv"}"#.to_string(),
            },
        );
        let decoded = PublishEntry::from_bytes(&entry.to_bytes().unwrap()).unwrap();
        assert_eq!(decoded, entry);
        assert_eq!(decoded.document_id(), Some(record().document_id));
    }

    #[test]
    fn holds_future_entries() {
        let mut entry = entry();
        entry.state = PublishState::Pending {
            due_at_ms: 1_000,
            attempts: 1,
            last_error: Some("unreachable".to_string()),
        };
        assert!(!entry.is_due(999));
        assert!(entry.is_due(1_000));
    }

    #[test]
    fn settles_terminal_entries() {
        // A published or parked entry stays visible but is never re-forwarded.
        let mut entry = entry();
        entry.state = PublishState::Published {
            document_id: Ulid::from_parts(6, 6),
        };
        assert!(!entry.is_due(u64::MAX));
        entry.state = PublishState::Failed {
            reason: "denied".to_string(),
            retryable: false,
            document_id: None,
        };
        assert!(!entry.is_due(u64::MAX));
        assert!(entry.retry_failed(1_000).is_none());
        entry.state = PublishState::Failed {
            reason: "unreachable".to_string(),
            retryable: true,
            document_id: None,
        };
        assert!(matches!(
            entry.retry_failed(1_000),
            Some(PublishState::Pending {
                due_at_ms: 1_000,
                attempts: 0,
                ..
            })
        ));
        let document_id = Ulid::from_parts(7, 7);
        entry.state = PublishState::Failed {
            reason: "unreachable".to_string(),
            retryable: true,
            document_id: Some(document_id),
        };
        assert!(matches!(
            entry.retry_failed(1_000),
            Some(PublishState::Publishing {
                document_id: retried,
                due_at_ms: 1_000,
                attempts: 0,
            }) if retried == document_id
        ));
    }

    const TXN: Ulid = Ulid::from_bytes([9; 16]);

    fn publishing() -> PublishState {
        PublishState::Publishing {
            document_id: Ulid::from_parts(4, 4),
            due_at_ms: 0,
            attempts: 1,
        }
    }

    fn storage(event: StorageEvent) -> Event {
        Event::Storage(event)
    }

    fn stored(entry: &PublishEntry) -> Event {
        storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(entry.to_bytes().unwrap().into()),
        })
    }

    fn abort() -> Effect {
        Effect::Storage(StorageEffect::AbortTransaction { txn_id: TXN })
    }

    // An entry still in the scanned state is claimed in one transaction.
    #[test]
    fn claims_unchanged_entry() {
        let entry = entry();
        let (key_space, key, value) =
            publish_entry(&entry_with_state(&entry, publishing())).unwrap();
        let write = Effect::Storage(StorageEffect::Write {
            key_space,
            key: key.clone(),
            value,
            txn_id: Some(TXN),
        });
        let mut claim = ClaimStateOperation::new(entry.clone(), publishing());
        check_trace(
            &mut claim,
            &[Effect::Storage(StorageEffect::StartTransaction {
                read: false,
            })],
            vec![
                (
                    storage(StorageEvent::TransactionStarted { txn_id: TXN }),
                    vec![read_publish_entry(entry.draft_id, Some(TXN))],
                ),
                (stored(&entry), vec![write]),
                (
                    storage(StorageEvent::WriteResult { key }),
                    vec![Effect::Storage(StorageEffect::CommitTransaction {
                        txn_id: TXN,
                    })],
                ),
                (
                    storage(StorageEvent::TransactionCommitted { txn_id: TXN }),
                    Vec::new(),
                ),
            ],
        );
        assert_eq!(claim.finalize(), Ok(true));
    }

    // An entry that moved on or went away is left alone and the transaction aborted.
    #[test]
    fn moved_entry_kept() {
        let entry = entry();
        let started = || storage(StorageEvent::TransactionStarted { txn_id: TXN });
        let mut claim = ClaimStateOperation::new(entry.clone(), publishing());
        claim.start();
        claim.step(started());
        check_abort(
            &mut ClaimStateOperation {
                txn_id: Some(TXN),
                ..ClaimStateOperation::new(entry.clone(), publishing())
            },
            &[abort()],
        );
        let moved = entry_with_state(&entry, publishing());
        assert_eq!(claim.step(stored(&moved)).into_vec(), vec![abort()]);
        let aborted = storage(StorageEvent::TransactionAborted { txn_id: TXN });
        assert!(claim.step(aborted).is_empty());
        assert_eq!(claim.finalize(), Ok(false));

        let mut claim = ClaimStateOperation::new(entry.clone(), publishing());
        claim.start();
        claim.step(storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(claim.finalize(), Ok(false));
    }

    // Unstarted claims refuse every event, abort nothing and never finish early.
    #[test]
    fn claim_rejects_unexpected() {
        let fresh = || ClaimStateOperation::new(entry(), publishing());
        let refused = rejects_events(fresh, sample_events, |_| false);
        assert_eq!(refused.len(), sample_events().len());
        assert!(matches!(refused[0].2, ClaimError::UnexpectedEvent(_)));
        rejects_finalize(fresh());
        assert!(fresh().abort().is_empty());
    }

    // A failed, retryable draft is claimed again; other drafts are left alone.
    #[test]
    fn requeues_failed_drafts() {
        let mut failed = entry();
        failed.state = PublishState::Failed {
            reason: "unreachable".to_string(),
            retryable: true,
            document_id: None,
        };
        let pending = entry();
        let next = failed.retry_failed(5).unwrap();
        let rows = [&pending, &failed].map(|entry| {
            let (_, key, value) = publish_entry(entry).unwrap();
            (key, value)
        });
        let page = storage(StorageEvent::IterResult {
            values: rows.to_vec(),
            next_start_after: None,
        });
        let scan = ScanRowsOperation::new(
            aruna_core::keyspaces::DEVICE_INTAKE_KEYSPACE,
            None,
            PUBLISH_PAGE_SIZE,
        )
        .start();
        let claim = ClaimStateOperation::new(failed, next).start();
        let mut requeue = RequeueFailedOperation::new(5);
        check_trace(&mut requeue, &scan, vec![(page, claim.into_vec())]);
        assert!(!requeue.is_complete());

        let mut requeue = RequeueFailedOperation::new(5);
        requeue.start();
        let empty = storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        });
        assert!(requeue.step(empty).is_empty());
        assert_eq!(requeue.finalize(), Ok(0));

        let fresh = || RequeueFailedOperation::new(5);
        assert_eq!(
            rejects_events(fresh, sample_events, |_| false).len(),
            sample_events().len()
        );
        rejects_finalize(fresh());
        assert!(fresh().abort().is_empty());
    }
}
