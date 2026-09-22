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
use aruna_core::structs::identity::s3_session::S3Session;
use aruna_core::structs::placement::policy::document::PlacementPolicyDocument;
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::structs::storage::routing::GroupStorageRouting;
use aruna_core::types::{Effects, GroupId, Key, TxnId};
use smallvec::smallvec;

const TABLES: &[(&str, &str, bool)] = &[
    (METADATA_INDEX_KEYSPACE, "metadata documents", true),
    (SOURCE_INDEX_KEYSPACE, "source connectors", true),
    (CONNECTOR_INDEX_KEYSPACE, "repository connectors", true),
    (HARVEST_SOURCE_KEYSPACE, "harvest sources", true),
    (S3_BUCKET_KEYSPACE, "S3 buckets", false),
    (STORAGE_BACKEND_KEYSPACE, "storage backends", false),
    (USER_ACCESS_KEYSPACE, "active S3 access keys", false),
    (S3_SESSION_KEYSPACE, "active S3 sessions", false),
    (PLACEMENT_POLICY_KEYSPACE, "owned placement policies", false),
    (STORAGE_ROUTING_KEYSPACE, "default storage routing", true),
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
}

impl EmptyCheck {
    pub fn new(group_id: GroupId, now: SystemTime) -> Self {
        Self {
            group_id,
            now,
            table: 0,
            pending: false,
            next: None,
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
                    return Err(GroupDeletionError::NotEmpty(reason.into()));
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
            for (_, value) in values {
                let owned = match space {
                    JOB_KEYSPACE => {
                        crate::jobs::store::decode_job_record(&value)?
                            .payload
                            .owner_group()
                            == Some(self.group_id)
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
                    STORAGE_BACKEND_KEYSPACE => {
                        GroupStorage::from_bytes(&value)?.group_id == self.group_id
                    }
                    USER_ACCESS_KEYSPACE => {
                        let record = UserAccess::from_bytes(&value)?;
                        record.group_id == self.group_id
                            && record.revoked_at.is_none()
                            && record.expiry > self.now
                    }
                    S3_SESSION_KEYSPACE => {
                        let record = S3Session::from_bytes(&value)?;
                        record.group_id == self.group_id && record.expiry > self.now
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
                if owned {
                    return Err(GroupDeletionError::NotEmpty(reason.into()));
                }
            }
        }
        if self.next.is_none() {
            self.table += 1;
        }
        Ok((self.table < TABLES.len()).then(|| self.start(txn_id)))
    }
}
