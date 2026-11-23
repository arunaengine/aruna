//! Leaves of each job family the migration deletes what a finished prune leaves: its tombstone
//! and its aliases until they expire, so the submitter reads "pruned" instead of "not found".
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::keyspaces::FAMILY_TOMBSTONE_KEYSPACE;
use aruna_core::structs::execution::job::family::{
    JobFamilyId, JobFamilyRecord, JobRecordEnvelope,
};
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::execution::job::prune::{FamilyTombstone, record_time};
use aruna_core::structs::execution::job::record_keys::{family_prefix, prune_rows_at};
use aruna_core::structs::execution::job::record_rows::to_bytes;
use aruna_core::types::{Key, Value};
use aruna_core::user::id::UserId;

use super::compute;
use super::families::{self, exact};
use crate::explorer::ExplorerError;

/// The time a stored record states about itself, and the submitter a spec names.
pub(super) type Stated = (Option<u64>, Option<UserId>);

/// What one deleted family with records leaves: its submitter, last record time and aliases.
#[derive(Default)]
pub(super) struct Remains {
    created_by: Option<UserId>,
    last_record_ms: u64,
    aliases: BTreeSet<JobId>,
}

impl Remains {
    /// Notes what one stored record states, read in the current, alpha.206 or alpha.134 layout.
    pub(super) fn record(&mut self, value: &[u8]) {
        let current = exact::<JobRecordEnvelope>(value).map(|envelope| stated(&envelope.record));
        let read = current.or_else(|| compute::stated(value));
        let Some((time, created_by)) = read.or_else(|| families::old_stated(value)) else {
            return;
        };
        self.last_record_ms = self.last_record_ms.max(time.unwrap_or(0));
        self.created_by = self.created_by.or(created_by);
    }

    pub(super) fn alias(&mut self, job_id: JobId) {
        self.aliases.insert(job_id);
    }

    /// The finished tombstone a prune at `now_ms` leaves, after the index rows of its expiry.
    pub(super) fn rows(
        &self,
        family: &JobFamilyId,
        now_ms: u64,
    ) -> Result<[(String, Key, Value); 3], ExplorerError> {
        let tombstone = FamilyTombstone {
            pruned_at_ms: now_ms,
            last_record_ms: self.last_record_ms,
            finished: true,
            created_by: self.created_by,
            aliases: self.aliases.iter().copied().collect(),
        };
        let bytes =
            to_bytes(&tombstone).map_err(|error| ExplorerError::Decode(error.to_string()))?;
        let [due, slot] = prune_rows_at(family, tombstone.expires_at_ms());
        let value = Value::from(bytes.as_slice());
        let row = (
            FAMILY_TOMBSTONE_KEYSPACE.to_string(),
            family_prefix(family),
            value,
        );
        Ok([due, slot, row])
    }
}

fn stated(record: &JobFamilyRecord) -> Stated {
    let created_by = match record {
        JobFamilyRecord::Spec(spec) => Some(spec.created_by),
        _ => None,
    };
    (record_time(record), created_by)
}
