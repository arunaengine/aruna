//! Decides without I/O whether one job record is admitted, retained pending, or rejected.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::structs::execution::job::{
    FamilyTombstone, Freshness, JobFamilyRecord, JobId, JobRecordEnvelope, JobRecordError,
    JobRecordKey, LocalExecution, RecordVerdict, spec_freshness,
};
use tracing::warn;

use super::verify::{EvidenceSet, FamilyView};
use aruna_core::structs::execution::job::record_rows::{
    ConflictRecord, PendingNeed, PendingRecord,
};

/// Records one family may retain pending at once.
pub const MAX_PENDING_RECORDS: usize = 64;
/// Admission attempts one pending record may consume before it is dropped.
const MAX_PENDING_ATTEMPTS: u32 = 32;

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
    /// The family is being pruned here; the record is refused for now and offered again later.
    Pruning,
    /// This node holds no accepted spec of the family; the record must come with its spec.
    SpecNeeded,
    /// The spec that would start the family here is older than the horizon. Refused for good.
    Stale,
    /// The spec belongs to an incarnation this node pruned. Refused for good.
    Pruned,
}

/// Everything one admission is decided against.
#[derive(Debug)]
pub struct FamilyState<'a> {
    /// `None` when the local holder view cannot be resolved: every candidate
    /// defers instead of being judged against an empty view.
    pub view: Option<&'a FamilyView>,
    /// Records already stored under their own key: the candidate keys plus the
    /// predecessors every record judged here needs, read exactly or by paging
    /// one whole kind to its end.
    pub stored: &'a BTreeMap<JobRecordKey, JobRecordEnvelope>,
    /// This node's own fenced execution, if it is the one publishing.
    pub local: Option<&'a LocalExecution>,
    pub now_ms: u64,
    /// This node's tombstone of the family, if it pruned it.
    pub tombstone: Option<&'a FamilyTombstone>,
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

    if let Some(refused) = gate(state, &candidate, relayed_by.is_some()) {
        return (refused, plan);
    }
    let key = candidate.key();
    let outcome = match admit_one(state, &evidence, &candidate) {
        Admission::Authentic => fresh(state, &candidate).unwrap_or(Admission::Authentic),
        outcome => outcome,
    };
    if matches!(outcome, Admission::Stale | Admission::Pruned) {
        return (outcome, plan);
    }
    let retained_row = match &outcome {
        Admission::Pending(_) => retained
            .iter()
            .find(|(retained_key, _)| *retained_key == key),
        _ => None,
    };
    let outcome = match (outcome, retained_row) {
        // A re-arrival of the record already retained here is still pending, and
        // never an acknowledgement: the publisher must keep offering it.
        (Admission::Pending(need), Some((_, row))) => match candidate.digest() {
            Ok(digest) if row.envelope.digest().ok() == Some(digest) => Admission::Pending(need),
            Ok(_) => Admission::Conflict,
            Err(error) => Admission::Rejected(error),
        },
        (outcome, _) => outcome,
    };
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
            if retained_row.is_none() && retained.len() >= MAX_PENDING_RECORDS {
                warn!(kind = ?key.kind, "Dropping a job record: the family pending store is full");
                return (Admission::PendingFull, plan);
            }
            plan.pending.push((
                key,
                PendingRecord {
                    envelope: candidate,
                    need: *need,
                    first_seen_ms: retained_row.map_or(state.now_ms, |(_, row)| row.first_seen_ms),
                    attempts: retained_row.map_or(1, |(_, row)| row.attempts),
                },
            ));
        }
        Admission::Conflict => {
            let retained_digest = retained_row
                .and_then(|(_, row)| row.envelope.digest().ok())
                .or_else(|| {
                    state
                        .stored
                        .get(&key)
                        .and_then(|stored| stored.digest().ok())
                })
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
        // The gate answered these before the candidate was judged.
        Admission::Duplicate
        | Admission::PendingFull
        | Admission::Pruning
        | Admission::SpecNeeded
        | Admission::Stale
        | Admission::Pruned => {}
    }

    if !visible.is_empty() {
        cascade(state, retained, &mut evidence, &mut visible, &mut plan);
    }
    (outcome, plan)
}

/// The rules that keep a pruned family from coming back, checked before any record is judged.
/// A peer's record of a family without a held spec waits for its spec instead of pending here.
fn gate(
    state: &FamilyState<'_>,
    candidate: &JobRecordEnvelope,
    relayed: bool,
) -> Option<Admission> {
    if state.tombstone.is_some_and(|tombstone| !tombstone.finished) {
        return Some(Admission::Pruning);
    }
    let spec = matches!(candidate.record, JobFamilyRecord::Spec(_));
    (relayed && !spec && !holds_spec(state)).then_some(Admission::SpecNeeded)
}

/// An authentic spec that would start the family here must be newer than the horizon and than
/// the last record of a pruned incarnation.
fn fresh(state: &FamilyState<'_>, candidate: &JobRecordEnvelope) -> Option<Admission> {
    let JobFamilyRecord::Spec(spec) = &candidate.record else {
        return None;
    };
    if holds_spec(state) {
        return None;
    }
    let pruned = state.tombstone.map(|tombstone| tombstone.last_record_ms);
    match spec_freshness(spec.created_at_ms, state.now_ms, pruned) {
        Freshness::Fresh => None,
        Freshness::Stale => Some(Admission::Stale),
        Freshness::Pruned => Some(Admission::Pruned),
    }
}

fn holds_spec(state: &FamilyState<'_>) -> bool {
    (state.stored.values()).any(|stored| matches!(stored.record, JobFamilyRecord::Spec(_)))
}

/// Admits `spec`, then `candidate` with it, as one plan: the record a peer offered again together
/// with the spec it binds. A stale or pruned spec refuses both for good.
pub fn plan_bundle(
    state: &FamilyState<'_>,
    retained: &[(JobRecordKey, PendingRecord)],
    candidate: JobRecordEnvelope,
    spec: Option<&JobRecordEnvelope>,
    relayed_by: Option<NodeId>,
) -> (Admission, AppendPlan) {
    let Some(spec) = spec.filter(|_| !holds_spec(state)) else {
        return plan_append(state, retained, candidate, relayed_by);
    };
    let (outcome, mut plan) = plan_append(state, retained, spec.clone(), relayed_by);
    match outcome {
        Admission::Authentic => {}
        Admission::Stale | Admission::Pruned => return (outcome, AppendPlan::default()),
        outcome => return (outcome, plan),
    }
    let mut stored = state.stored.clone();
    for admitted in &plan.admitted {
        stored.insert(admitted.envelope.key(), admitted.envelope.clone());
    }
    let next = FamilyState {
        stored: &stored,
        ..*state
    };
    let left: Vec<_> = (retained.iter())
        .filter(|(key, _)| !plan.cleared.contains(key))
        .cloned()
        .collect();
    let (outcome, more) = plan_append(&next, &left, candidate, relayed_by);
    plan.admitted.extend(more.admitted);
    plan.pending.extend(more.pending);
    plan.cleared.extend(more.cleared);
    plan.conflicts.extend(more.conflicts);
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
    let mut still: BTreeMap<JobRecordKey, PendingRecord> = BTreeMap::new();
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
                Admission::Duplicate
                | Admission::Rejected(_)
                | Admission::PendingFull
                | Admission::Pruning
                | Admission::SpecNeeded
                | Admission::Stale
                | Admission::Pruned => {
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
                // A record that stays pending keeps its row, with one more attempt spent, so
                // an unresolvable one cannot be retried forever.
                Admission::Pending(need) => {
                    let attempts = row.attempts.saturating_add(1);
                    if attempts >= MAX_PENDING_ATTEMPTS {
                        plan.cleared.push(*key);
                        settled.push(*key);
                    } else {
                        still.insert(
                            *key,
                            PendingRecord {
                                envelope: row.envelope.clone(),
                                need,
                                first_seen_ms: row.first_seen_ms,
                                attempts,
                            },
                        );
                    }
                }
            }
        }
        if settled.is_empty() {
            break;
        }
        open.retain(|(key, _)| !settled.contains(key));
    }
    plan.pending.extend(
        still
            .into_iter()
            .filter(|(key, _)| !plan.cleared.contains(key) && !visible.contains_key(key)),
    );
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
        // Holder authority moves with membership: an unranked member is judged again,
        // while a non-member can never become a holder and is refused outright.
        Err(JobRecordError::NotHolder(_)) if view.is_member(candidate.published_by) => {
            Admission::Pending(PendingNeed::HolderView)
        }
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
