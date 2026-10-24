//! Records a source change a refresh confirmed as a successor of the current reference version.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, PURGE_FENCE_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::placement::policy::PlacementPolicyError;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::storage_purge::StoragePurgeFence;
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

use crate::connectors::repository::connector_reference_write;
use crate::distribution::guard::{GuardError, record_epoch, record_read};
use crate::distribution::inventory::{
    AddedVersion, entry_of, stamp_head, stamp_version, version_written,
};
use crate::node::usage_stats::{UsageCounterUpdate, UsageUpdateError};
use crate::replication::protocol::ReferenceAdvance;
use crate::replication::queue::{LiveObligationRecord, live_obligation_entry};
use crate::s3::object::get::MAX_AUTO_ADVANCES;
use crate::s3::object::metadata::ReferenceRefresh;
use crate::s3::purge_fence::{PurgeFenceError, check_decoded_fence, fence_key};
use aruna_core::structs::storage::distribution::hint::mint_version;
use aruna_core::structs::storage::distribution::{DistributionBinding, OriginalEpoch};

/// One storage row a successor writes.
type Row = (KeySpace, Key, Value);

/// The rows read for a successor, in read order.
type ReadRows = Vec<(Key, Option<Value>)>;

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
    #[error("unexpected event in state {state:?}: {received}")]
    UnexpectedEvent { state: MintState, received: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MintState {
    Init,
    ReadRows,
    /// Reads a distributed bucket's record: only the key's coordinator advances it.
    ReadRecord,
    WriteRows,
    UpdateUsage,
    Finish,
    Error,
}

/// Writes, in the caller's transaction, the successor carrying the refresh's observation while
/// the refreshed version is the head. Outputs whether a successor was written.
#[derive(Debug, PartialEq)]
pub(crate) struct MintSuccessorOperation {
    txn_id: TxnId,
    refresh: ReferenceRefresh,
    observation: SourceMetadata,
    predecessor: BlobVersion,
    state: MintState,
    /// The rows read and the binding whose record decides the successor's epoch.
    pending: Option<(ReadRows, DistributionBinding)>,
    usage: Option<UsageCounterUpdate>,
    output: Option<Result<bool, MintError>>,
}

impl MintSuccessorOperation {
    pub(crate) fn new(
        txn_id: TxnId,
        refresh: ReferenceRefresh,
        observation: SourceMetadata,
        predecessor: BlobVersion,
    ) -> Self {
        Self {
            txn_id,
            refresh,
            observation,
            predecessor,
            state: MintState::Init,
            pending: None,
            usage: None,
            output: None,
        }
    }

    fn finish(&mut self, written: bool) -> Effects {
        self.state = MintState::Finish;
        self.output = Some(Ok(written));
        smallvec![]
    }

    fn fail(&mut self, error: MintError) -> Effects {
        self.state = MintState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn write_successor(
        &mut self,
        values: Vec<(Key, Option<Value>)>,
        epoch: OriginalEpoch,
    ) -> Result<Effects, MintError> {
        let Some(writes) = self.successor_rows(values, epoch)? else {
            return Ok(self.finish(false));
        };
        self.state = MintState::WriteRows;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(self.txn_id),
        })])
    }

    fn head_key(&self) -> Result<Vec<u8>, ConversionError> {
        BlobHeadKey::new(&self.refresh.bucket, &self.refresh.key).to_bytes()
    }

    fn read_rows(&mut self) -> Result<Effects, MintError> {
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
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(self.txn_id),
        })])
    }

    /// The successor's rows once the head is still the refreshed version; `None` otherwise.
    fn successor_rows(
        &mut self,
        values: Vec<(Key, Option<Value>)>,
        epoch: OriginalEpoch,
    ) -> Result<Option<Vec<Row>>, MintError> {
        let mut values = values.into_iter().map(|(_, value)| value);
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
        let usage = UsageCounterUpdate::for_group(bucket.group_id, delta);
        self.usage = Some(usage.with_inventory(inventory));
        Ok(Some(writes))
    }

    fn step_inner(&mut self, event: Event) -> Result<Effects, MintError> {
        match (self.state, event) {
            (MintState::ReadRows, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let bucket = values
                    .get(1)
                    .and_then(|(_, value)| value.as_ref())
                    .map(|value| BucketInfo::from_bytes(value.as_ref()))
                    .transpose()?;
                match bucket.and_then(|bucket| bucket.distribution) {
                    Some(binding) => {
                        self.state = MintState::ReadRecord;
                        let read = record_read(&binding);
                        self.pending = Some((values, binding));
                        Ok(smallvec![read])
                    }
                    None => self.write_successor(values, OriginalEpoch::default()),
                }
            }
            (MintState::ReadRecord, event @ Event::Storage(StorageEvent::ReadResult { .. })) => {
                let (values, binding) = self.pending.take().ok_or(StorageError::InvalidEffect)?;
                let epoch = record_epoch(&binding, &self.refresh.key, event)?;
                self.write_successor(values, epoch)
            }
            (MintState::WriteRows, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                match self.usage.as_mut().filter(|usage| !usage.is_noop()) {
                    Some(usage) => {
                        self.state = MintState::UpdateUsage;
                        Ok(usage.start(self.txn_id))
                    }
                    None => Ok(self.finish(true)),
                }
            }
            (MintState::UpdateUsage, event @ Event::Storage(_)) => {
                let txn_id = self.txn_id;
                let usage = self.usage.as_mut().ok_or(StorageError::InvalidEffect)?;
                match usage.step(event, txn_id)? {
                    Some(effects) => Ok(effects),
                    None => Ok(self.finish(true)),
                }
            }
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (state, event) => Err(MintError::UnexpectedEvent {
                state,
                received: format!("{event:?}"),
            }),
        }
    }
}

impl Operation for MintSuccessorOperation {
    type Output = bool;
    type Error = MintError;

    fn start(&mut self) -> Effects {
        let exhausted = match &self.predecessor.state {
            BlobVersionState::Reference { advance_count, .. } => {
                *advance_count >= MAX_AUTO_ADVANCES
            }
            _ => true,
        };
        if exhausted {
            return self.finish(false);
        }
        match self.read_rows() {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        if matches!(self.state, MintState::Finish | MintState::Error) {
            return smallvec![];
        }
        match self.step_inner(event) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, MintState::Finish | MintState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(MintError::UnexpectedEvent {
            state: self.state,
            received: "no output".to_string(),
        }))
    }

    /// The transaction belongs to the caller, which aborts it.
    fn abort(&mut self) -> Effects {
        smallvec![]
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
            },
            connector_id: None,
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
        MintSuccessorOperation::new(
            Ulid::from_bytes([1; 16]),
            refresh,
            observation(2),
            predecessor,
        )
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
        operation.start();
        let fence = StoragePurgeFence {
            job_id: JobId::from_bytes([6; 16]),
            scope: StoragePurgeScope::Bucket {
                bucket: "bucket".to_string(),
            },
        };
        let head = CurrentVersionPointer::new(version_id);
        let values = vec![
            (
                Key::from(b"head".to_vec()),
                Some(head.to_bytes().unwrap().into()),
            ),
            (
                Key::from(b"bucket".to_vec()),
                Some(bucket().to_bytes().unwrap().into()),
            ),
            (
                Key::from(b"fence".to_vec()),
                Some(fence.to_bytes().unwrap().into()),
            ),
        ];

        operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));

        assert_eq!(
            operation.finalize(),
            Err(MintError::Purge(PurgeFenceError::Suspended))
        );
    }

    // A refreshed version that is no longer the head gets no successor.
    #[test]
    fn stale_head_skipped() {
        let mut operation = mint(Ulid::from_bytes([2; 16]));
        assert!(matches!(
            operation.start().as_slice(),
            [Effect::Storage(StorageEffect::BatchRead { .. })]
        ));
        let newer = CurrentVersionPointer::new(Ulid::from_bytes([3; 16]));
        let values = vec![
            (
                Key::from(b"head".to_vec()),
                Some(newer.to_bytes().unwrap().into()),
            ),
            (
                Key::from(b"bucket".to_vec()),
                Some(bucket().to_bytes().unwrap().into()),
            ),
        ];

        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));

        assert!(effects.is_empty());
        assert_eq!(operation.finalize(), Ok(false));
    }
}
