//! Promotes pending archives after a key unlock and drops received archives whose hash was rejected.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::BlobError;
use aruna_core::keyspaces::{
    BLOB_QUARANTINE_KEYSPACE, BLOB_VERSIONS_KEYSPACE, PENDING_CLAIM_KEYSPACE,
    PENDING_LOCATION_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, read_row, values};
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BlobQuarantineRecord, BlobVersion, BucketInfo, VersionKey,
};
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::types::{Key, Value};
use std::collections::VecDeque;

use super::promote::{
    PROMOTE_PAGE, PromoteError, PromotePendingOperation, Promotion, RejectOwnersOperation,
};
use crate::s3::object::delete::{
    DeleteObjectError, DeleteObjectInput, DeleteObjectOperation, DeleteObjectResult,
};

#[derive(Debug, PartialEq)]
enum PromoteStep {
    Page,
    Promote(ArchiveKey),
    Drop,
}

type PromoteTurn = Result<Next<usize>, PromoteError>;

/// Promotes every pending archive sealed with `key`, one page of pending rows at a time. Run it
/// once `key` is installed; archives of other keys are skipped. Outputs how many were promoted.
#[derive(Debug, PartialEq)]
pub struct PromoteUnlockedOperation {
    key: BucketKeyRef,
    origin: (RealmId, NodeId),
    limits: RoCrateLimits,
    pages: PrefixPages,
    rows: VecDeque<(Key, Value)>,
    step: PromoteStep,
    promoted: usize,
}

impl PromoteUnlockedOperation {
    pub fn new(key: BucketKeyRef, origin: (RealmId, NodeId), limits: &RoCrateLimits) -> Self {
        Self {
            key,
            origin,
            limits: limits.clone(),
            pages: PrefixPages::new(
                PENDING_LOCATION_KEYSPACE,
                Key::from(Vec::new()),
                PROMOTE_PAGE,
            ),
            rows: VecDeque::new(),
            step: PromoteStep::Page,
            promoted: 0,
        }
    }

    fn call(&mut self, step: PromoteStep, call: IoCall) -> PromoteTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn next_row(&mut self) -> PromoteTurn {
        while let Some((row, value)) = self.rows.pop_front() {
            let location = BackendLocation::from_bytes(&value)?;
            if location.format.bucket_key() != Some(self.key) {
                continue;
            }
            let archive = ArchiveKey::from_bytes(&row)?;
            let (realm_id, node_id) = self.origin;
            let promote = PromotePendingOperation::new(
                archive.clone(),
                realm_id,
                node_id,
                self.limits.clone(),
            );
            return self.call(PromoteStep::Promote(archive), IoCall::child(promote));
        }
        if self.pages.done() {
            return Ok(Next::Done(self.promoted));
        }
        let page = self.pages.call();
        self.call(PromoteStep::Page, page)
    }

    fn promoted(
        &mut self,
        archive: ArchiveKey,
        outcome: Result<Promotion, PromoteError>,
    ) -> PromoteTurn {
        let drop = match outcome {
            Err(PromoteError::Blob(BlobError::IntegrityCheckFailed(reason))) => {
                DropReceivedOperation::rejected(archive, self.origin, reason)
            }
            Err(error) => return Err(error),
            Ok(Promotion::Promoted { .. }) => {
                self.promoted += 1;
                return self.next_row();
            }
            // A lock in between leaves the rest pending until the next unlock.
            Ok(Promotion::AwaitingKey(_)) => return Ok(Next::Done(self.promoted)),
            Ok(Promotion::Gone) => return self.next_row(),
            Ok(Promotion::Mismatch { claimed }) => {
                DropReceivedOperation::mismatched(archive, claimed, self.origin)
            }
        };
        self.call(PromoteStep::Drop, IoCall::child(drop))
    }
}

impl Operation for PromoteUnlockedOperation {
    type Output = usize;
    type Error = PromoteError;

    fn first(&mut self) -> PromoteTurn {
        let page = self.pages.call();
        self.call(PromoteStep::Page, page)
    }

    fn next(&mut self, result: IoResult) -> PromoteTurn {
        match std::mem::replace(&mut self.step, PromoteStep::Page) {
            PromoteStep::Page => {
                self.rows = self.pages.take(result)?.into();
                self.next_row()
            }
            PromoteStep::Promote(archive) => {
                self.promoted(archive, result.child::<Promotion, PromoteError>()?)
            }
            PromoteStep::Drop => {
                result.child::<usize, PromoteError>()??;
                self.next_row()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum DropStep {
    Claim,
    Record,
    Owners,
    Version(VersionKey),
    Delete,
}

type DropTurn = Result<Next<usize>, PromoteError>;

/// Quarantines a received archive whose content was rejected and deletes the versions that use
/// it; none of them was registered. Outputs how many versions were deleted.
#[derive(Debug, PartialEq)]
pub struct DropReceivedOperation {
    archive: ArchiveKey,
    claimed: Option<[u8; 32]>,
    origin: (RealmId, NodeId),
    reason: String,
    step: DropStep,
    fence: Option<RejectOwnersOperation>,
    attempts: u32,
    owners: VecDeque<VersionKey>,
    dropped: usize,
    /// What the previous pass over the owners had dropped.
    before: usize,
}

impl DropReceivedOperation {
    /// The archive's content differs from the hash its sender claimed.
    pub fn mismatched(archive: ArchiveKey, claimed: [u8; 32], origin: (RealmId, NodeId)) -> Self {
        let reason = "replicated content hash differs from the claimed hash".to_string();
        Self::new(archive, Some(claimed), origin, reason)
    }

    /// The archive failed its integrity check; the hash its sender claimed is read first.
    pub fn rejected(archive: ArchiveKey, origin: (RealmId, NodeId), reason: String) -> Self {
        Self::new(archive, None, origin, reason)
    }

    fn new(
        archive: ArchiveKey,
        claimed: Option<[u8; 32]>,
        origin: (RealmId, NodeId),
        reason: String,
    ) -> Self {
        Self {
            archive,
            claimed,
            origin,
            reason,
            step: DropStep::Claim,
            fence: None,
            attempts: 0,
            owners: VecDeque::new(),
            dropped: 0,
            before: 0,
        }
    }

    fn call(&mut self, step: DropStep, call: IoCall) -> DropTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn claim_read(&mut self, claim: Option<Value>) -> DropTurn {
        let claimed = claim.and_then(|claim| <[u8; 32]>::try_from(claim.as_ref()).ok());
        let Some(claimed) = claimed else {
            let reason = std::mem::take(&mut self.reason);
            return Err(BlobError::IntegrityCheckFailed(reason).into());
        };
        self.write_record(claimed)
    }

    fn write_record(&mut self, claimed: [u8; 32]) -> DropTurn {
        let now_ms = aruna_core::time::unix_timestamp_millis();
        let backend = self.archive.backend.clone();
        let reason = std::mem::take(&mut self.reason);
        let record = BlobQuarantineRecord::new(claimed, backend, reason, now_ms);
        let row = (
            BLOB_QUARANTINE_KEYSPACE.to_string(),
            record.key().into(),
            record.to_bytes()?.into(),
        );
        self.fence = Some(RejectOwnersOperation::new(self.archive.clone(), &record)?);
        self.call(DropStep::Record, IoCall::Write(vec![row]))
    }

    fn list_owners(&mut self) -> DropTurn {
        let Some(fence) = self.fence.clone() else {
            return Err(PromoteError::InvalidStateEvent {
                state: "Drop",
                expected: "quarantine record",
                received: "no record",
            });
        };
        self.attempts += 1;
        self.call(DropStep::Owners, IoCall::child(fence))
    }

    fn owners_listed(&mut self, owners: Result<Vec<VersionKey>, PromoteError>) -> DropTurn {
        let owners = match owners {
            Ok(owners) => owners,
            Err(error)
                if RejectOwnersOperation::is_conflict(&error)
                    && self.attempts <= RejectOwnersOperation::conflict_retries() =>
            {
                return self.list_owners();
            }
            Err(error) => return Err(error),
        };
        self.attempts = 0;
        if owners.is_empty() {
            return Ok(Next::Done(self.dropped));
        }
        self.owners = owners.into();
        self.before = self.dropped;
        self.next_owner()
    }

    fn next_owner(&mut self) -> DropTurn {
        let Some(version) = self.owners.pop_front() else {
            if self.dropped == self.before {
                return Err(PromoteError::Dropped(
                    "pending owners could not be removed".into(),
                ));
            }
            return self.list_owners();
        };
        let reads = vec![
            (
                BLOB_VERSIONS_KEYSPACE.to_string(),
                version.to_bytes()?.into(),
            ),
            (
                S3_BUCKET_KEYSPACE.to_string(),
                version.bucket.as_bytes().to_vec().into(),
            ),
        ];
        self.call(DropStep::Version(version), IoCall::Read(reads))
    }

    fn version_read(
        &mut self,
        version: VersionKey,
        [stored, info]: [Option<Value>; 2],
    ) -> DropTurn {
        let Some(stored) = stored else {
            return self.next_owner();
        };
        let stored = BlobVersion::from_bytes(&stored)?;
        let Some(info) = info else {
            return self.next_owner();
        };
        if stored.state.pending_archive() != Some(&self.archive) {
            return self.next_owner();
        }
        let (realm_id, node_id) = self.origin;
        let delete = DeleteObjectOperation::new(DeleteObjectInput {
            bucket: version.bucket.clone(),
            key: version.key.clone(),
            version_id: Some(version.version_id),
            group_id: BucketInfo::from_bytes(&info)?.group_id,
            realm_id,
            node_id,
            deleted_by: stored.created_by,
        });
        self.call(DropStep::Delete, IoCall::child(delete))
    }
}

impl Operation for DropReceivedOperation {
    type Output = usize;
    type Error = PromoteError;

    fn first(&mut self) -> DropTurn {
        if let Some(claimed) = self.claimed {
            return self.write_record(claimed);
        }
        let read = read_row(PENDING_CLAIM_KEYSPACE, self.archive.to_bytes());
        self.call(DropStep::Claim, read)
    }

    fn next(&mut self, result: IoResult) -> DropTurn {
        match std::mem::replace(&mut self.step, DropStep::Owners) {
            DropStep::Claim => self.claim_read(result.value()?),
            DropStep::Record => {
                result.done()?;
                self.list_owners()
            }
            DropStep::Owners => {
                self.owners_listed(result.child::<Vec<VersionKey>, PromoteError>()?)
            }
            DropStep::Version(version) => self.version_read(version, values::<2>(result)?),
            DropStep::Delete => {
                result
                    .child::<DeleteObjectResult, DeleteObjectError>()?
                    .map_err(|error| PromoteError::Dropped(error.to_string()))?;
                self.dropped += 1;
                self.next_owner()
            }
        }
    }
}

pub(super) async fn read_value(
    storage: &aruna_storage::StorageHandle,
    key_space: &str,
    key: Vec<u8>,
) -> Result<Option<Value>, PromoteError> {
    let read = StorageEffect::Read {
        key_space: key_space.to_string(),
        key: key.into(),
        txn_id: None,
    };
    match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(PromoteError::InvalidStateEvent {
            state: "Drop",
            expected: "ReadResult",
            received: (other).label(),
        }),
    }
}
