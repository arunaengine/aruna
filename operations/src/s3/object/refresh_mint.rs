//! Records a source change a refresh confirmed as a successor of the current reference version.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, PURGE_FENCE_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::placement::policy::PlacementPolicyError;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::storage_purge::StoragePurgeFence;
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;
use thiserror::Error;

use crate::connectors::repository::connector_reference_write;
use crate::distribution::guard::{GuardError, authority_read, record_epoch};
use crate::distribution::inventory::{
    AddedVersion, entry_of, stamp_head, stamp_version, version_written,
};
use crate::s3::object::get::MAX_AUTO_ADVANCES;
use crate::s3::object::metadata::ReferenceRefresh;
use crate::s3::purge_fence::{PurgeFenceError, check_decoded_fence, fence_key};
use crate::s3::usage::{RowChange, UsageCounterUpdate, UsageUpdateError};
use aruna_core::structs::storage::distribution::hint::mint_version;
use aruna_core::structs::storage::distribution::{DistributionBinding, OriginalEpoch};
use aruna_core::structs::storage::replication_queue::ReferenceAdvance;
use aruna_core::structs::storage::replication_queue::{
    LiveObligationRecord, live_obligation_entry,
};

/// One storage row a successor writes.
type Row = (KeySpace, Key, Value);

#[derive(Debug, Error, PartialEq)]
pub enum MintError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    #[error(transparent)]
    Usage(#[from] UsageUpdateError),
    #[error(transparent)]
    Purge(#[from] PurgeFenceError),
    #[error(transparent)]
    Distribution(#[from] GuardError),
}

impl From<IoError> for MintError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

type MintTurn = Result<Next<bool>, MintError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MintState {
    ReadRows,
    /// Reads a distributed bucket's record: only the key's coordinator advances it.
    ReadRecord,
    WriteRows,
    UpdateUsage,
}

/// Writes the successor carrying the refresh's observation while the refreshed version is the
/// head, and outputs whether it did. A caller folds these calls into its own transaction.
#[derive(Debug, PartialEq)]
pub(crate) struct MintSuccessorOperation {
    refresh: ReferenceRefresh,
    observation: SourceMetadata,
    predecessor: BlobVersion,
    state: MintState,
    /// The rows read and the binding whose record decides the successor's epoch.
    pending: Option<(Vec<Option<Value>>, DistributionBinding)>,
    usage: Option<UsageCounterUpdate>,
}

impl MintSuccessorOperation {
    pub(crate) fn new(
        refresh: ReferenceRefresh,
        observation: SourceMetadata,
        predecessor: BlobVersion,
    ) -> Self {
        Self {
            refresh,
            observation,
            predecessor,
            state: MintState::ReadRows,
            pending: None,
            usage: None,
        }
    }

    fn write_successor(&mut self, values: Vec<Option<Value>>, epoch: OriginalEpoch) -> MintTurn {
        let Some(writes) = self.successor_rows(values, epoch)? else {
            return Ok(Next::Done(false));
        };
        self.state = MintState::WriteRows;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn head_key(&self) -> Result<Vec<u8>, ConversionError> {
        BlobHeadKey::new(&self.refresh.bucket, &self.refresh.key).to_bytes()
    }

    fn read_rows(&mut self) -> MintTurn {
        let bucket = ByteView::from(self.refresh.bucket.as_bytes().to_vec());
        let reads = vec![
            (BLOB_HEAD_KEYSPACE.to_string(), self.head_key()?.into()),
            (S3_BUCKET_KEYSPACE.to_string(), bucket),
            (
                PURGE_FENCE_KEYSPACE.to_string(),
                fence_key(&self.refresh.bucket),
            ),
        ];
        self.state = MintState::ReadRows;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// The successor's rows once the head is still the refreshed version; `None` otherwise.
    fn successor_rows(
        &mut self,
        values: Vec<Option<Value>>,
        epoch: OriginalEpoch,
    ) -> Result<Option<Vec<Row>>, MintError> {
        let mut values = values.into_iter();
        let pointer = values
            .next()
            .flatten()
            .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
            .transpose()?;
        let bucket = values
            .next()
            .flatten()
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()?;
        // A suspended key fails the refresh, so its queue retries it after the purge.
        let fence = values
            .next()
            .flatten()
            .map(|value| StoragePurgeFence::from_bytes(value.as_ref()))
            .transpose()
            .map_err(|_| PurgeFenceError::Invalid)?;
        check_decoded_fence(fence.as_ref(), &self.refresh.bucket, &self.refresh.key)?;
        let (Some(pointer), Some(bucket)) = (pointer, bucket) else {
            return Ok(None);
        };
        let BlobVersionState::Reference {
            source,
            advance_count,
            ..
        } = &self.predecessor.state
        else {
            return Ok(None);
        };
        if pointer.version_id != self.refresh.version_id {
            return Ok(None);
        }
        let refresh = &self.refresh;
        let now = SystemTime::now();
        let hint = bucket.distribution.as_ref().map(DistributionBinding::hint);
        let version_id = mint_version(hint);
        let successor = BlobVersion::reference(
            source.clone(),
            self.observation.clone(),
            now,
            self.predecessor.created_by,
            now,
        )
        .with_metadata(self.predecessor.metadata.clone())
        .with_origin(self.predecessor.origin.clone())
        .with_hop(self.predecessor.hop.clone())
        .with_advance_count(advance_count + 1)
        .with_policies(self.predecessor.placement_policies.clone())?;
        let version_key = VersionKey::new(&refresh.bucket, &refresh.key, version_id);
        let next = CurrentVersionPointer::next_for(Some(&pointer), version_id)?;
        let binding = bucket.distribution.as_ref();
        let next = stamp_head(next, binding, epoch);
        let successor = stamp_version(successor, binding, next.generation, epoch);
        let obligation = LiveObligationRecord::new(
            refresh.node_id,
            refresh.auth_context.clone(),
            refresh.bucket.clone(),
            refresh.key.clone(),
            version_id,
            false,
        )
        .with_reference_advance(ReferenceAdvance {
            generation: next.generation,
            predecessor: pointer.version_id,
        });
        let mut writes = vec![
            (
                BLOB_VERSIONS_KEYSPACE.to_string(),
                version_key.to_bytes()?.into(),
                successor.to_bytes()?.into(),
            ),
            (
                BLOB_HEAD_KEYSPACE.to_string(),
                self.head_key()?.into(),
                next.to_bytes()?.into(),
            ),
            live_obligation_entry(&obligation)?,
        ];
        writes.extend(connector_reference_write(&successor, &version_key)?);
        // The successor books its own size; an offered directory observation is released.
        let released = self
            .predecessor
            .offered_size(&refresh.bucket, &refresh.key)
            .unwrap_or(0);
        let delta = UsageDelta {
            referenced_bytes: i128::from(self.observation.content_length) - i128::from(released),
            ..Default::default()
        };
        let added = AddedVersion {
            version_id,
            epoch: successor.original_epoch,
            generation: next.generation,
            entry: entry_of(&successor, self.observation.content_length),
        };
        let key = &refresh.key;
        let inventory = version_written(bucket.distribution.as_ref(), key, added, true);
        // The successor is one more version row of the bucket, booked like any other write.
        let usage = UsageCounterUpdate::for_group(bucket.group_id, delta)
            .in_bucket(&refresh.bucket, RowChange::AddedVersion);
        self.usage = Some(usage.with_inventory(inventory));
        Ok(Some(writes))
    }

    fn rows_read(&mut self, values: Vec<Option<Value>>) -> MintTurn {
        let bucket = (values.get(1).and_then(Option::as_ref))
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()?;
        let Some(binding) = bucket.and_then(|bucket| bucket.distribution) else {
            return self.write_successor(values, OriginalEpoch::default());
        };
        // Read in the mint transaction, so a fence set meanwhile conflicts it.
        self.state = MintState::ReadRecord;
        let read = authority_read(binding.set_id, None);
        self.pending = Some((values, binding));
        Ok(Next::Call(read.into()))
    }

    fn rows_written(&mut self) -> MintTurn {
        match self.usage.as_mut().filter(|usage| !usage.is_noop()) {
            Some(usage) => {
                let call = usage.start_calls().into_call();
                self.state = MintState::UpdateUsage;
                Ok(Next::Call(call))
            }
            None => Ok(Next::Done(true)),
        }
    }
}

impl Operation for MintSuccessorOperation {
    type Output = bool;
    type Error = MintError;

    fn first(&mut self) -> MintTurn {
        let exhausted = match &self.predecessor.state {
            BlobVersionState::Reference { advance_count, .. } => {
                *advance_count >= MAX_AUTO_ADVANCES
            }
            _ => true,
        };
        if exhausted {
            return Ok(Next::Done(false));
        }
        self.read_rows()
    }

    fn next(&mut self, result: IoResult) -> MintTurn {
        match self.state {
            MintState::ReadRows => self.rows_read(result.values()?),
            MintState::ReadRecord => {
                let event = result.event()?;
                let (values, binding) = self.pending.take().ok_or(IoError::Unfinished)?;
                let epoch = record_epoch(&binding, &self.refresh.key, event)?;
                self.write_successor(values, epoch)
            }
            MintState::WriteRows => {
                result.done()?;
                self.rows_written()
            }
            MintState::UpdateUsage => {
                let usage = self.usage.as_mut().ok_or(IoError::Unfinished)?;
                Ok(match usage.step_calls(result)? {
                    Some(call) => Next::Call(call.into_call()),
                    None => Next::Done(true),
                })
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::execution::source_access::SourceMetadata;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::{
        PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
    };
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use std::collections::HashMap;
    use ulid::Ulid;

    fn observation(content_length: u64) -> SourceMetadata {
        SourceMetadata {
            content_length,
            content_type: None,
            etag: Some(format!("etag-{content_length}")),
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        }
    }

    fn mint(version_id: Ulid) -> MintSuccessorOperation {
        let binding = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::new(),
                source_path: "a".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
                root: None,
            },
            connector_id: None,
            mirrors: Vec::new(),
        };
        let now = SystemTime::UNIX_EPOCH;
        let predecessor =
            BlobVersion::reference(binding, observation(1), now, Default::default(), now);
        let refresh = ReferenceRefresh {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id,
            metadata: Some(observation(2)),
            refreshed_at: now,
            node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
            auth_context: AuthContext {
                user_id: Default::default(),
                realm_id: RealmId([9; 32]),
                path_restrictions: None,
                session: None,
            },
        };
        MintSuccessorOperation::new(refresh, observation(2), predecessor)
    }

    fn bucket() -> BucketInfo {
        BucketInfo {
            group_id: Ulid::from_bytes([5; 16]),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: aruna_core::structs::storage::format::Compression::Off,
            distribution: None,
        }
    }

    // A key under a permanent purge gets no successor; the refresh fails and is retried.
    #[test]
    fn purge_fence_suspends() {
        use aruna_core::structs::execution::job::JobId;
        use aruna_core::structs::storage::storage_purge::StoragePurgeScope;
        let version_id = Ulid::from_bytes([2; 16]);
        let mut operation = mint(version_id);
        operation.first().unwrap();
        let fence = StoragePurgeFence {
            job_id: JobId::from_bytes([6; 16]),
            scope: StoragePurgeScope::Bucket {
                bucket: "bucket".to_string(),
            },
        };
        let head = CurrentVersionPointer::new(version_id);
        let values = IoResult::Values(vec![
            Some(head.to_bytes().unwrap().into()),
            Some(bucket().to_bytes().unwrap().into()),
            Some(fence.to_bytes().unwrap().into()),
        ]);

        assert_eq!(
            operation.next(values),
            Err(MintError::Purge(PurgeFenceError::Suspended))
        );
    }

    // A successor is booked on its bucket's row as well as on its group.
    #[test]
    fn successor_books_bucket() {
        let version_id = Ulid::from_bytes([2; 16]);
        let mut operation = mint(version_id);
        assert_eq!(operation.transaction(), TransactionMode::Write);
        operation.first().unwrap();
        let head = CurrentVersionPointer::new(version_id);
        let values = IoResult::Values(vec![
            Some(head.to_bytes().unwrap().into()),
            Some(bucket().to_bytes().unwrap().into()),
            None,
        ]);
        let turn = operation.next(values);
        assert!(matches!(turn, Ok(Next::Call(IoCall::Write(_)))));
        let turn = operation.next(IoResult::Done);

        let Ok(Next::Call(IoCall::AddUsage { buckets, .. })) = &turn else {
            panic!("expected the usage update, got {turn:?}");
        };
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].1.versions, 1);
    }

    // A refreshed version that is no longer the head gets no successor.
    #[test]
    fn stale_head_skipped() {
        let mut operation = mint(Ulid::from_bytes([2; 16]));
        assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
        let newer = CurrentVersionPointer::new(Ulid::from_bytes([3; 16]));
        let values = IoResult::Values(vec![
            Some(newer.to_bytes().unwrap().into()),
            Some(bucket().to_bytes().unwrap().into()),
            None,
        ]);

        assert_eq!(operation.next(values), Ok(Next::Done(false)));
    }
}
