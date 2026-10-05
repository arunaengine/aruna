//! Audit records of a bucket's key state: unlocks, locks, mode changes and holder changes. They
//! stay on the bucket node and never carry key bytes, grants or vault payloads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::errors::ConversionError;
use crate::{NodeId, UserId};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};
use ulid::{Generator, Ulid};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    Unlock,
    Extend,
    Lock,
    TimedLock,
    RestartLock,
    ModeChange,
    HolderGrant,
    HolderRemoval,
    Rotation,
}

/// A volatile change records its intent before it applies, then its outcome; an intent alone
/// never reads as success.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Intent,
    Applied,
    Failed,
}

/// One audit event, stored in `bucket_audit` under bucket id and its time-ordered event id.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct BucketAuditRecord {
    pub event_id: Ulid,
    pub bucket_id: Ulid,
    pub at_ms: u64,
    pub action: AuditAction,
    /// None for actions of the node itself, such as a timed or restart lock.
    pub actor: Option<UserId>,
    pub node_id: NodeId,
    pub generation: Option<u64>,
    /// The unlock session an unlock, extension or lock names; none for actions of a generation.
    pub session_id: Option<Ulid>,
    /// The event id of the intent an applied or failed outcome completes.
    pub intent_id: Option<Ulid>,
    pub deadline_ms: Option<u64>,
    pub reason: Option<String>,
    pub outcome: AuditOutcome,
}

/// Event ids of this process in issue order, so the records of one bucket sort as they happened
/// even within one millisecond.
static EVENT_IDS: Mutex<Generator> = Mutex::new(Generator::new());

/// The next audit event id at `at_ms`; it sorts after every id issued before it.
pub fn next_event_id(at_ms: u64) -> Ulid {
    let at = SystemTime::UNIX_EPOCH + Duration::from_millis(at_ms);
    let Ok(mut ids) = EVENT_IDS.lock() else {
        return Ulid::from_datetime(at);
    };
    match ids.generate_from_datetime(at) {
        Ok(id) => id,
        Err(overflow) => overflow.commit_overflow_increment(),
    }
}

impl BucketAuditRecord {
    /// Bucket id, then the event id, so one bucket's trail scans in time order.
    pub fn key(&self) -> Vec<u8> {
        [&self.bucket_id.to_bytes()[..], &self.event_id.to_bytes()].concat()
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::identity::realm::RealmId;

    #[test]
    fn trail_keys_ordered() {
        let record = |event: u64| BucketAuditRecord {
            event_id: Ulid::from_parts(event, 1),
            bucket_id: Ulid::from_bytes([2; 16]),
            at_ms: event,
            action: AuditAction::Unlock,
            actor: Some(UserId::new(
                Ulid::from_bytes([3; 16]),
                RealmId::from_bytes([1; 32]),
            )),
            node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
            generation: Some(1),
            session_id: None,
            intent_id: None,
            deadline_ms: None,
            reason: None,
            outcome: AuditOutcome::Intent,
        };
        let (early, late) = (record(5), record(6));
        assert!(early.key() < late.key());
        assert!(early.key().starts_with(&early.bucket_id.to_bytes()));
        assert_eq!(
            BucketAuditRecord::from_bytes(&late.to_bytes().unwrap()).unwrap(),
            late
        );
        let names = serde_json::to_value((AuditAction::TimedLock, AuditOutcome::Applied)).unwrap();
        assert_eq!(names, serde_json::json!(["timed_lock", "applied"]));
    }

    #[test]
    fn event_ids_monotonic() {
        // Ids issued within one millisecond, or with an older time, still sort in issue order.
        let first = next_event_id(1_000);
        let same = next_event_id(1_000);
        let older = next_event_id(999);
        assert!(first < same && same < older);
        assert!(older < next_event_id(2_000));
    }
}
