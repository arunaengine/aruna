//! Checks authoritative local resource rows before confirming that a group is empty.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::*;
use aruna_core::metadata::MetadataEventRecord;
use aruna_core::structs::execution::job::{JobFamilyRecord, JobRecordEnvelope};
use aruna_core::structs::identity::group_delete::GroupDeletionError;
use aruna_core::structs::placement::policy::document::PlacementPolicyDocument;
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess};
use aruna_core::structs::storage::cleanup::CleanupStrategy;
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::structs::storage::routing::GroupStorageRouting;
use aruna_core::types::{Effects, GroupId, Key, TxnId};
use smallvec::smallvec;

const MAX_NAMED: usize = 5;

const TABLES: &[(&str, &str, bool)] = &[
    (METADATA_INDEX_KEYSPACE, "metadata documents", true),
    (SOURCE_INDEX_KEYSPACE, "source connectors", true),
    (CONNECTOR_INDEX_KEYSPACE, "repository connectors", true),
    (HARVEST_SOURCE_KEYSPACE, "harvest sources", true),
    (S3_BUCKET_KEYSPACE, "S3 buckets", false),
    (STORAGE_BACKEND_KEYSPACE, "storage backends", false),
    (USER_ACCESS_KEYSPACE, "active S3 access keys", false),
    (PLACEMENT_POLICY_KEYSPACE, "owned placement policies", false),
    (STORAGE_ROUTING_KEYSPACE, "default storage routing", true),
    (BACKEND_IMPORT_KEYSPACE, "backend imports", true),
    (JOB_KEYSPACE, "job records", false),
    (FAMILY_RECORD_KEYSPACE, "job families", false),
    (FAMILY_PENDING_KEYSPACE, "pending job families", false),
    (
        PENDING_PROJECTION_KEYSPACE,
        "pending metadata creation",
        false,
    ),
];

#[derive(Debug, PartialEq)]
pub(super) struct EmptyCheck {
    group_id: GroupId,
    now: SystemTime,
    table: usize,
    pending: bool,
    next: Option<Key>,
    /// Every kind of resource found so far; the check names them all.
    found: Vec<&'static str>,
    /// Names of the first blocking buckets, and whether more exist.
    buckets: Vec<String>,
    more: bool,
}

impl EmptyCheck {
    pub fn new(group_id: GroupId, now: SystemTime) -> Self {
        Self {
            group_id,
            now,
            table: 0,
            pending: false,
            next: None,
            found: Vec::new(),
            buckets: Vec::new(),
            more: false,
        }
    }

    pub fn start(&self, txn_id: TxnId) -> Effects {
        let (key_space, _, prefix) = TABLES[self.table];
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: prefix.then(|| self.group_id.to_bytes().into()),
            start: self.next.clone().map(aruna_core::effects::IterStart::After),
            limit: 128,
            txn_id: Some(txn_id),
        })]
    }

    pub fn step(
        &mut self,
        event: Event,
        txn_id: TxnId,
    ) -> Result<Option<Effects>, GroupDeletionError> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(error.into());
        }
        let (space, reason, prefix) = TABLES[self.table];
        if self.pending {
            let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
                return Err(GroupDeletionError::Unavailable(
                    "invalid pending metadata read".into(),
                ));
            };
            self.pending = false;
            for (_, value) in values {
                let value = value.ok_or_else(|| {
                    GroupDeletionError::Unavailable("pending metadata event is missing".into())
                })?;
                let record: MetadataEventRecord = postcard::from_bytes(&value)
                    .map_err(|error| GroupDeletionError::Unavailable(error.to_string()))?;
                if record.record.group_id == self.group_id {
                    self.found.push(reason);
                    self.next = None;
                    break;
                }
            }
        } else {
            let Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) = event
            else {
                return Err(GroupDeletionError::Unavailable(
                    "invalid resource scan".into(),
                ));
            };
            self.next = next_start_after;
            if space == PENDING_PROJECTION_KEYSPACE && !values.is_empty() {
                self.pending = true;
                return Ok(Some(smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: values
                        .into_iter()
                        .map(|(key, _)| (EVENT_LOG_KEYSPACE.to_string(), key))
                        .collect(),
                    txn_id: Some(txn_id),
                })]));
            }
            for (key, value) in values {
                let owned = match space {
                    // A finished job only waits for its retention purge, so it does not block.
                    JOB_KEYSPACE => {
                        let record = crate::jobs::store::decode_job_record(&value)?;
                        record.payload.owner_group() == Some(self.group_id)
                            && !record.state.is_terminal()
                    }
                    FAMILY_RECORD_KEYSPACE => {
                        let envelope: JobRecordEnvelope =
                            crate::jobs::records::rows::from_bytes(&value)?;
                        matches!(envelope.record, JobFamilyRecord::Spec(spec) if spec.group_id == self.group_id)
                    }
                    FAMILY_PENDING_KEYSPACE => {
                        let pending: crate::jobs::records::rows::PendingRecord =
                            crate::jobs::records::rows::from_bytes(&value)?;
                        matches!(pending.envelope.record, JobFamilyRecord::Spec(spec) if spec.group_id == self.group_id)
                    }
                    S3_BUCKET_KEYSPACE => BucketInfo::from_bytes(&value)?.group_id == self.group_id,
                    // A disabled `retain` backend keeps its bytes in tenant storage; the commit drops it.
                    STORAGE_BACKEND_KEYSPACE => {
                        let record = GroupStorage::from_bytes(&value)?;
                        record.group_id == self.group_id
                            && !(record.disabled && record.cleanup == CleanupStrategy::Retain)
                    }
                    USER_ACCESS_KEYSPACE => {
                        let record = UserAccess::from_bytes(&value)?;
                        record.group_id == self.group_id
                            && record.revoked_at.is_none()
                            && record.expiry > self.now
                    }
                    PLACEMENT_POLICY_KEYSPACE => {
                        PlacementPolicyDocument::from_bytes(&value)?
                            .policy
                            .owner_group_id
                            == Some(self.group_id)
                    }
                    STORAGE_ROUTING_KEYSPACE => GroupStorageRouting::from_bytes(&value)?
                        .default_target
                        .is_some(),
                    _ => prefix,
                };
                if owned && space == S3_BUCKET_KEYSPACE {
                    if self.buckets.is_empty() {
                        self.found.push(reason);
                    }
                    if self.buckets.len() == MAX_NAMED {
                        self.more = true;
                        self.next = None;
                        break;
                    }
                    self.buckets
                        .push(String::from_utf8_lossy(&key).into_owned());
                } else if owned {
                    self.found.push(reason);
                    self.next = None;
                    break;
                }
            }
        }
        if self.next.is_none() {
            self.table += 1;
        }
        if self.table < TABLES.len() {
            return Ok(Some(self.start(txn_id)));
        }
        if self.found.is_empty() {
            return Ok(None);
        }
        Err(GroupDeletionError::NotEmpty {
            kinds: self.found.iter().map(|kind| kind.to_string()).collect(),
            buckets: self.buckets.clone(),
            more: self.more,
            hidden: 0,
        })
    }
}

/// Joins the kinds and puts the visible bucket names and the hidden count after `S3 buckets`.
pub(super) fn render(kinds: &[String], names: &[String], hidden: u64, more: bool) -> String {
    let kinds = match kinds.split_last() {
        None => String::new(),
        Some((last, [])) => last.clone(),
        Some((last, rest)) => format!("{} and {last}", rest.join(", ")),
    };
    let mut shown = names.to_vec();
    if hidden > 0 {
        shown.push(format!("{hidden} you cannot read"));
    }
    if shown.is_empty() {
        return kinds;
    }
    let more = if more { ", and more" } else { "" };
    kinds.replacen(
        "S3 buckets",
        &format!("S3 buckets ({}{more})", shown.join(", ")),
        1,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_every_kind() {
        let group_id = ulid::Ulid::from_bytes([1u8; 16]);
        let txn_id = TxnId::default();
        let mut check = EmptyCheck::new(group_id, SystemTime::UNIX_EPOCH);
        let mut result = Ok(None);
        for table in 0..TABLES.len() {
            // The first two tables are scoped by group, so any row there is owned.
            let values = match table < 2 {
                true => vec![(Key::from(vec![1]), vec![0].into())],
                false => Vec::new(),
            };
            let event = Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after: None,
            });
            result = check.step(event, txn_id);
        }
        let Err(GroupDeletionError::NotEmpty { kinds, .. }) = result else {
            panic!("expected a non-empty group, got {result:?}");
        };
        assert_eq!(kinds, ["metadata documents", "source connectors"]);
    }

    fn bucket(group_id: GroupId) -> aruna_core::types::Value {
        BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: aruna_core::structs::storage::format::Compression::Off,
            distribution: None,
        }
        .to_bytes()
        .unwrap()
        .into()
    }

    fn page(rows: &[(&str, GroupId)], next: Option<&str>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: rows
                .iter()
                .map(|(name, group)| (Key::from(name.as_bytes().to_vec()), bucket(*group)))
                .collect(),
            next_start_after: next.map(|next| Key::from(next.as_bytes().to_vec())),
        })
    }

    fn run(
        first: &[(&str, GroupId)],
        second: &[(&str, GroupId)],
        paged: bool,
    ) -> (Vec<String>, bool) {
        let group_id = ulid::Ulid::from_bytes([1u8; 16]);
        let txn_id = TxnId::default();
        let mut check = EmptyCheck::new(group_id, SystemTime::UNIX_EPOCH);
        let empty = || page(&[], None);
        let mut events = vec![empty(), empty(), empty(), empty()];
        events.push(page(first, paged.then_some("p1")));
        if paged {
            events.push(page(second, None));
        }
        events.extend((5..TABLES.len()).map(|_| empty()));
        let mut result = Ok(None);
        for event in events {
            result = check.step(event, txn_id);
        }
        let Err(GroupDeletionError::NotEmpty { buckets, more, .. }) = result else {
            panic!("expected a non-empty group, got {result:?}");
        };
        (buckets, more)
    }

    #[test]
    fn names_buckets() {
        let mine = ulid::Ulid::from_bytes([1u8; 16]);
        let other = ulid::Ulid::from_bytes([2u8; 16]);
        let first = [("a)x", mine), ("x", other), ("b", mine), ("c", mine)];
        let second = [
            ("d", mine),
            ("y", other),
            ("e", mine),
            ("f", mine),
            ("g", mine),
        ];
        let names = ["a)x", "b", "c", "d", "e"].map(String::from);
        assert_eq!(run(&first, &second, true), (names.to_vec(), true));
        assert_eq!(run(&first, &[], false), (names[..3].to_vec(), false));
    }

    #[test]
    fn renders_names() {
        let names = ["a)x".to_string(), "b".to_string()];
        let kinds = ["metadata documents", "job records", "S3 buckets"].map(String::from);
        assert_eq!(
            render(&kinds, &names, 0, true),
            "metadata documents, job records and S3 buckets (a)x, b, and more)"
        );
        assert_eq!(render(&kinds[2..], &[], 0, true), "S3 buckets");
        assert_eq!(
            render(&kinds[2..], &names[..1], 2, false),
            "S3 buckets (a)x, 2 you cannot read)"
        );
    }
}
