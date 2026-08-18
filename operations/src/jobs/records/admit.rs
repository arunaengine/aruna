//! The pure admission decision of the append-only store.
//!
//! Nothing here performs I/O: it takes the records this node already stored,
//! the pending records it retained, and one candidate, and returns exactly what
//! must become visible. Admitting a record can only ever admit more pending
//! records, never rewrite or remove a stored one.

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::structs::{
    JobFamilyRecord, JobId, JobRecordEnvelope, JobRecordError, JobRecordKey, LocalExecution,
    RecordVerdict,
};
use tracing::warn;

use super::rows::{ConflictRecord, PendingNeed, PendingRecord};
use super::verify::{EvidenceSet, FamilyView};

/// Records one family may retain pending at once.
pub const MAX_PENDING_RECORDS: usize = 64;
/// Admission attempts one pending record may consume before it is dropped.
pub const MAX_PENDING_ATTEMPTS: u32 = 32;

/// What became of one candidate record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    /// The same key already holds the same canonical digest.
    Duplicate,
    /// Verified against the replicated chain; it may be projected and relayed.
    Authentic,
    /// Proven only against this node's own fenced execution: stored and
    /// projected locally, never relayed as replicated authority.
    Local,
    /// Retained until the named predecessor or the local view arrives.
    Pending(PendingNeed),
    /// The pending store of this family is full, so the record was dropped.
    PendingFull,
    /// The same key already holds different canonical bytes; both are retained.
    Conflict,
    /// Forged or invalid: dropped without a row.
    Rejected(JobRecordError),
}

/// Everything one admission is decided against.
#[derive(Debug)]
pub struct FamilyState<'a> {
    /// `None` when the local holder view cannot be resolved: every candidate
    /// defers instead of being judged against an empty view.
    pub view: Option<&'a FamilyView>,
    /// Records already stored under their own key, for the candidate keys and
    /// every evidence-bearing kind of the family.
    pub stored: &'a BTreeMap<JobRecordKey, JobRecordEnvelope>,
    /// This node's own fenced execution, if it is the one publishing.
    pub local: Option<&'a LocalExecution>,
    pub now_ms: u64,
}

/// One record that becomes visible in this admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmittedRecord {
    pub envelope: JobRecordEnvelope,
    /// Verified against the replicated chain, so it may be relayed. A locally
    /// proven record is stored and projected here and never leaves this node.
    pub authentic: bool,
}

/// The exact writes one admission produces. The caller commits them atomically.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct AppendPlan {
    pub admitted: Vec<AdmittedRecord>,
    pub pending: Vec<(JobRecordKey, PendingRecord)>,
    pub cleared: Vec<JobRecordKey>,
    pub conflicts: Vec<(JobRecordKey, ConflictRecord)>,
}

/// Admits one candidate and then every pending record its arrival unblocks.
/// The cascade is bounded by the pending store, and a record that becomes
/// authentic in one round is evidence for the next.
pub fn plan_append(
    state: &FamilyState<'_>,
    retained: &[(JobRecordKey, PendingRecord)],
    candidate: JobRecordEnvelope,
    relayed_by: Option<NodeId>,
) -> (Admission, AppendPlan) {
    let mut evidence = EvidenceSet::default();
    for envelope in state.stored.values() {
        evidence.insert(&envelope.record);
    }
    let mut plan = AppendPlan::default();
    let mut visible: BTreeMap<JobRecordKey, [u8; 32]> = BTreeMap::new();

    let outcome = admit_one(state, &evidence, &candidate);
    let key = candidate.key();
    match &outcome {
        Admission::Authentic | Admission::Local => {
            evidence.insert(&candidate.record);
            if let Ok(digest) = candidate.digest() {
                visible.insert(key, digest);
            }
            plan.admitted.push(AdmittedRecord {
                authentic: outcome == Admission::Authentic,
                envelope: candidate,
            });
        }
        Admission::Pending(need) => {
            if retained.len() >= MAX_PENDING_RECORDS {
                warn!(kind = ?key.kind, "Dropping a job record: the family pending store is full");
                return (Admission::PendingFull, plan);
            }
            plan.pending.push((
                key,
                PendingRecord {
                    envelope: candidate,
                    need: *need,
                    first_seen_ms: state.now_ms,
                    attempts: 1,
                },
            ));
        }
        Admission::Conflict => {
            let retained_digest = state
                .stored
                .get(&key)
                .and_then(|stored| stored.digest().ok())
                .unwrap_or_default();
            plan.conflicts.push((
                key,
                ConflictRecord {
                    envelope: candidate,
                    retained: retained_digest,
                    observed_at_ms: state.now_ms,
                    relayed_by,
                },
            ));
        }
        Admission::Rejected(error) => {
            warn!(kind = ?key.kind, error = %error, "Refusing an unauthentic job record");
        }
        Admission::Duplicate | Admission::PendingFull => {}
    }

    if !visible.is_empty() {
        cascade(state, retained, &mut evidence, &mut visible, &mut plan);
    }
    (outcome, plan)
}

/// Re-admits retained records in rounds until no further one is unblocked.
fn cascade(
    state: &FamilyState<'_>,
    retained: &[(JobRecordKey, PendingRecord)],
    evidence: &mut EvidenceSet,
    visible: &mut BTreeMap<JobRecordKey, [u8; 32]>,
    plan: &mut AppendPlan,
) {
    let mut open: Vec<&(JobRecordKey, PendingRecord)> = retained.iter().collect();
    for _ in 0..retained.len() {
        let mut settled: Vec<JobRecordKey> = Vec::new();
        for (key, row) in open.iter() {
            let outcome = match visible.get(key) {
                Some(digest) if row.envelope.digest().ok() == Some(*digest) => Admission::Duplicate,
                Some(_) => Admission::Conflict,
                None => admit_one(state, evidence, &row.envelope),
            };
            match outcome {
                Admission::Authentic | Admission::Local => {
                    evidence.insert(&row.envelope.record);
                    if let Ok(digest) = row.envelope.digest() {
                        visible.insert(*key, digest);
                    }
                    plan.admitted.push(AdmittedRecord {
                        envelope: row.envelope.clone(),
                        authentic: outcome == Admission::Authentic,
                    });
                    plan.cleared.push(*key);
                    settled.push(*key);
                }
                Admission::Duplicate | Admission::Rejected(_) | Admission::PendingFull => {
                    plan.cleared.push(*key);
                    settled.push(*key);
                }
                Admission::Conflict => {
                    let retained_digest = visible
                        .get(key)
                        .copied()
                        .or_else(|| {
                            state
                                .stored
                                .get(key)
                                .and_then(|stored| stored.digest().ok())
                        })
                        .unwrap_or_default();
                    plan.conflicts.push((
                        *key,
                        ConflictRecord {
                            envelope: row.envelope.clone(),
                            retained: retained_digest,
                            observed_at_ms: state.now_ms,
                            relayed_by: None,
                        },
                    ));
                    plan.cleared.push(*key);
                    settled.push(*key);
                }
                Admission::Pending(need) => {
                    let attempts = row.attempts.saturating_add(1);
                    if attempts >= MAX_PENDING_ATTEMPTS {
                        plan.cleared.push(*key);
                        settled.push(*key);
                    } else if need != row.need {
                        plan.pending.push((
                            *key,
                            PendingRecord {
                                envelope: row.envelope.clone(),
                                need,
                                first_seen_ms: row.first_seen_ms,
                                attempts,
                            },
                        ));
                    }
                }
            }
        }
        if settled.is_empty() {
            return;
        }
        open.retain(|(key, _)| !settled.contains(key));
    }
}

/// The single ingest gate for one record: idempotent replay, retained conflict,
/// deferred local view, or the record kind's own author and evidence rules.
fn admit_one(
    state: &FamilyState<'_>,
    evidence: &EvidenceSet,
    candidate: &JobRecordEnvelope,
) -> Admission {
    let key = candidate.key();
    let digest = match candidate.digest() {
        Ok(digest) => digest,
        Err(error) => return Admission::Rejected(error),
    };
    if let Some(stored) = state.stored.get(&key) {
        return match stored.digest().ok() == Some(digest) {
            true => Admission::Duplicate,
            false => Admission::Conflict,
        };
    }
    let Some(view) = state.view else {
        return Admission::Pending(PendingNeed::LocalView);
    };
    match candidate.verify(&view.context(evidence.select(&candidate.record), state.local)) {
        Ok(RecordVerdict::Authentic) => Admission::Authentic,
        Ok(RecordVerdict::LocalEvidence) => Admission::Local,
        Ok(RecordVerdict::MissingEvidence(kind)) => Admission::Pending(PendingNeed::Evidence(kind)),
        Err(error) => Admission::Rejected(error),
    }
}

/// Alias every admitted claim binds, so the alias index stays append-only.
pub fn admitted_aliases(plan: &AppendPlan) -> Vec<(JobRecordKey, JobId)> {
    plan.admitted
        .iter()
        .filter_map(|admitted| match &admitted.envelope.record {
            JobFamilyRecord::Claim(claim) => Some((admitted.envelope.key(), claim.job_id)),
            _ => None,
        })
        .collect()
}

/// Records this node published itself and may replicate to the family holders.
/// A locally proven record has no replicated chain behind it, so it is never
/// queued for relay.
pub fn relayable(plan: &AppendPlan, local_node_id: NodeId) -> Vec<JobRecordKey> {
    plan.admitted
        .iter()
        .filter(|admitted| admitted.authentic && admitted.envelope.published_by == local_node_id)
        .map(|admitted| admitted.envelope.key())
        .collect()
}
