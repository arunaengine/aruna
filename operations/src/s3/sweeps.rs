//! Checks vault-locked buckets for a recovery path that weakened.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, VecDeque};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::StorageError;
use aruna_core::keyspaces::{BUCKET_ENCRYPTION_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, read_row};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{BucketEncryption, EncryptionMode};
use aruna_core::types::{GroupId, Key, Value};
use aruna_core::{NodeId, UserId};
use tracing::warn;

use crate::s3::bucket::holders::{LookupError, LookupKeysOperation, Lookups};
use crate::s3::bucket::key::recovery::{
    RecoveryInput, RecoveryNoticeError, RecoveryNoticeOperation,
};
use crate::s3::key_status::{KeySnapshot, KeyStatusError, KeyStatusOperation};

/// Encryption rows one sweep page reads.
const SWEEP_PAGE: usize = 1_000;

#[derive(Clone, Copy, Debug, PartialEq)]
enum RecoveryStep {
    Status,
    Lookup,
    Notice,
}

type RecoveryTurn = Result<Next<Vec<UserId>>, RecoveryNoticeError>;

/// Checks one bucket: reads its holders, asks the key directory and tells the holders if the
/// recovery path weakened.
#[derive(Debug, PartialEq)]
pub struct RecoveryCheckOperation {
    origin: (RealmId, NodeId),
    bucket: String,
    group_id: GroupId,
    now_ms: u64,
    step: RecoveryStep,
}

impl RecoveryCheckOperation {
    pub fn new(origin: (RealmId, NodeId), bucket: &str, group_id: GroupId, now_ms: u64) -> Self {
        Self {
            origin,
            bucket: bucket.to_string(),
            group_id,
            now_ms,
            step: RecoveryStep::Status,
        }
    }

    fn status_read(&mut self, snapshot: KeySnapshot) -> RecoveryTurn {
        let Some(info) = snapshot.info else {
            return Ok(Next::Done(Vec::new()));
        };
        let users: BTreeSet<_> = std::iter::once(info.created_by)
            .chain(snapshot.admins.iter().copied())
            .chain(snapshot.grants.iter().map(|grant| grant.user_id))
            .collect();
        let (realm_id, node_id) = self.origin;
        self.step = RecoveryStep::Lookup;
        let lookup = LookupKeysOperation::new(realm_id, node_id, users);
        Ok(Next::Call(IoCall::child(lookup)))
    }

    /// A failed lookup counts as unavailable, as a missing answer does.
    fn looked_up(&mut self, lookups: Result<Lookups, LookupError>) -> RecoveryTurn {
        let (realm_id, node_id) = self.origin;
        self.step = RecoveryStep::Notice;
        let notice = RecoveryNoticeOperation::new(RecoveryInput {
            bucket: self.bucket.clone(),
            group_id: self.group_id,
            realm_id,
            node_id,
            lookups: lookups.unwrap_or_default(),
            now_ms: self.now_ms,
        });
        Ok(Next::Call(IoCall::child(notice)))
    }
}

impl Operation for RecoveryCheckOperation {
    type Output = Vec<UserId>;
    type Error = RecoveryNoticeError;

    fn first(&mut self) -> RecoveryTurn {
        let status = KeyStatusOperation::new(self.bucket.clone(), self.origin.0, self.group_id);
        Ok(Next::Call(IoCall::child(status)))
    }

    fn next(&mut self, result: IoResult) -> RecoveryTurn {
        match self.step {
            RecoveryStep::Status => {
                let snapshot = result
                    .child::<KeySnapshot, KeyStatusError>()?
                    .map_err(|error| StorageError::ReadError(error.to_string()))?;
                self.status_read(snapshot)
            }
            RecoveryStep::Lookup => self.looked_up(result.child::<Lookups, LookupError>()?),
            RecoveryStep::Notice => {
                let told = result.child::<Vec<UserId>, RecoveryNoticeError>()??;
                Ok(Next::Done(told))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum SweepStep {
    Page,
    Group(String),
    Check(String),
}

type SweepTurn = Result<Next<()>, IoError>;

/// Checks every vault-locked bucket of this node once. A bucket that cannot be checked only
/// warns, and the next sweep tries it again.
#[derive(Debug, PartialEq)]
pub struct RecoverySweepOperation {
    origin: (RealmId, NodeId),
    now_ms: u64,
    pages: PrefixPages,
    locked: VecDeque<String>,
    step: SweepStep,
}

impl RecoverySweepOperation {
    pub fn new(origin: (RealmId, NodeId), now_ms: u64) -> Self {
        Self {
            origin,
            now_ms,
            pages: PrefixPages::new(
                BUCKET_ENCRYPTION_KEYSPACE,
                Key::from(Vec::new()),
                SWEEP_PAGE,
            ),
            locked: VecDeque::new(),
            step: SweepStep::Page,
        }
    }

    fn call(&mut self, step: SweepStep, call: IoCall) -> SweepTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn page_read(&mut self, rows: Vec<(Key, Value)>) -> SweepTurn {
        self.locked = (rows.into_iter())
            .filter(|(_, value)| {
                BucketEncryption::from_bytes(value.as_ref())
                    .is_ok_and(|settings| settings.mode == EncryptionMode::VaultLocked)
            })
            .map(|(key, _)| String::from_utf8_lossy(key.as_ref()).into_owned())
            .collect();
        self.next_bucket()
    }

    fn next_bucket(&mut self) -> SweepTurn {
        if let Some(bucket) = self.locked.pop_front() {
            let read = read_row(S3_BUCKET_KEYSPACE, bucket.as_bytes().to_vec());
            return self.call(SweepStep::Group(bucket), read);
        }
        if self.pages.done() {
            return Ok(Next::Done(()));
        }
        let page = self.pages.call();
        self.call(SweepStep::Page, page)
    }

    fn group_read(&mut self, bucket: String, value: Option<Value>) -> SweepTurn {
        let info = value.and_then(|value| BucketInfo::from_bytes(value.as_ref()).ok());
        let Some(info) = info else {
            return self.next_bucket();
        };
        let check = RecoveryCheckOperation::new(self.origin, &bucket, info.group_id, self.now_ms);
        self.call(SweepStep::Check(bucket), IoCall::child(check))
    }
}

impl Operation for RecoverySweepOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> SweepTurn {
        let page = self.pages.call();
        self.call(SweepStep::Page, page)
    }

    fn next(&mut self, result: IoResult) -> SweepTurn {
        match std::mem::replace(&mut self.step, SweepStep::Page) {
            SweepStep::Page => {
                let rows = self.pages.take(result)?;
                self.page_read(rows)
            }
            SweepStep::Group(bucket) => self.group_read(bucket, result.value()?),
            SweepStep::Check(bucket) => {
                if let Err(error) = result.child::<Vec<UserId>, RecoveryNoticeError>()? {
                    warn!(%bucket, error = %error, "Bucket recovery check failed");
                }
                self.next_bucket()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::storage::encryption::BucketEncryption;
    use ulid::Ulid;

    fn origin() -> (RealmId, NodeId) {
        let node = iroh::SecretKey::from_bytes(&[2; 32]).public();
        (RealmId::from_bytes([1; 32]), node)
    }

    fn settings(mode: EncryptionMode) -> Value {
        let settings = BucketEncryption {
            mode,
            bucket_id: Some(Ulid::from_bytes([4; 16])),
            key_generation: 1,
            ..Default::default()
        };
        settings.to_bytes().unwrap().into()
    }

    fn page(rows: Vec<(&str, Value)>) -> IoResult {
        IoResult::Page {
            rows: (rows.into_iter())
                .map(|(key, value)| (Key::from(key.as_bytes().to_vec()), value))
                .collect(),
            next: None,
        }
    }

    #[test]
    fn only_locked_checked() {
        let mut sweep = RecoverySweepOperation::new(origin(), 1);
        assert!(matches!(sweep.first(), Ok(Next::Call(IoCall::Page { .. }))));
        let rows = vec![
            ("open", settings(EncryptionMode::NodeManaged)),
            ("sealed", settings(EncryptionMode::VaultLocked)),
        ];
        assert!(matches!(
            sweep.next(page(rows)),
            Ok(Next::Call(IoCall::Read(_)))
        ));
        assert_eq!(sweep.step, SweepStep::Group("sealed".to_string()));
    }

    #[test]
    fn missing_bucket_skipped() {
        let mut sweep = RecoverySweepOperation::new(origin(), 1);
        sweep.first().unwrap();
        let rows = vec![("sealed", settings(EncryptionMode::VaultLocked))];
        sweep.next(page(rows)).unwrap();
        assert_eq!(sweep.next(IoResult::Values(vec![None])), Ok(Next::Done(())));
    }

    #[test]
    fn failed_check_continues() {
        let mut sweep = RecoverySweepOperation::new(origin(), 1);
        sweep.step = SweepStep::Check("sealed".to_string());
        sweep.pages.take(page(Vec::new())).unwrap();
        let failed = Err::<Vec<UserId>, _>(RecoveryNoticeError::NotFinished);
        let turn = sweep.next(IoResult::Child(ChildOutcome::new(failed)));
        assert_eq!(turn, Ok(Next::Done(())));
    }

    #[test]
    fn unknown_bucket_ends() {
        let group = Ulid::from_bytes([3; 16]);
        let mut check = RecoveryCheckOperation::new(origin(), "b", group, 1);
        assert!(matches!(check.first(), Ok(Next::Call(IoCall::Child(_)))));
        let status = Ok::<_, KeyStatusError>(KeySnapshot::default());
        let turn = check.next(IoResult::Child(ChildOutcome::new(status)));
        assert_eq!(turn, Ok(Next::Done(Vec::new())));
    }
}
