//! Lists the token copies of one encrypted bucket with the state of their credentials.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{KEY_COPY_KEYSPACE, USER_ACCESS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::structs::storage::encryption::TokenCopy;
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use std::collections::BTreeSet;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

const SCAN_PAGE: usize = 256;

#[derive(Debug, Error, PartialEq)]
pub enum TokenListError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event while listing tokens: {0:?}")]
    InvalidStateEvent(Event),
    #[error("the token listing did not finish")]
    NotFinished,
}

/// One token copy and whether its credential still authenticates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenEntry {
    pub copy: TokenCopy,
    pub credential_active: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Step {
    ScanCopies,
    ReadCredentials,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct ListTokensOperation {
    bucket_id: Ulid,
    now: SystemTime,
    step: Step,
    copies: Vec<TokenCopy>,
    output: Option<Result<Vec<TokenEntry>, TokenListError>>,
}

impl ListTokensOperation {
    /// Lists every generation's token copies of the bucket with stable id `bucket_id`.
    pub fn new(bucket_id: Ulid, now: SystemTime) -> Self {
        Self {
            bucket_id,
            now,
            step: Step::ScanCopies,
            copies: Vec::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<TokenListError>) -> Effects {
        self.step = Step::Error;
        self.output = Some(Err(error.into()));
        smallvec![]
    }

    fn scan(&mut self, start: Option<Key>) -> Effects {
        self.step = Step::ScanCopies;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: KEY_COPY_KEYSPACE.to_string(),
            prefix: Some(self.bucket_id.to_bytes().to_vec().into()),
            start: start.map(IterStart::After),
            limit: SCAN_PAGE,
            txn_id: None,
        })]
    }

    fn page(&mut self, values: Vec<(Key, Key)>, next: Option<Key>) -> Effects {
        for (key, value) in values {
            if TokenCopy::parse_key(&key).is_err() {
                continue;
            }
            match TokenCopy::from_bytes(&value) {
                Ok(copy) => self.copies.push(copy),
                Err(error) => return self.fail(error),
            }
        }
        if next.is_some() {
            return self.scan(next);
        }
        let access_keys: BTreeSet<_> = self.copies.iter().map(|copy| &copy.access_key).collect();
        if access_keys.is_empty() {
            return self.finish(Vec::new());
        }
        let reads = access_keys
            .into_iter()
            .map(|access_key| {
                let key = access_key.as_bytes().to_vec().into();
                (USER_ACCESS_KEYSPACE.to_string(), key)
            })
            .collect();
        self.step = Step::ReadCredentials;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn credentials(&mut self, values: Vec<(Key, Option<Key>)>) -> Effects {
        let mut active = BTreeSet::new();
        for (key, value) in values {
            let Some(value) = value else { continue };
            match UserAccess::from_bytes(&value) {
                Ok(access) if !access.is_revoked() && !access.is_expired(self.now) => {
                    active.insert(key.to_vec());
                }
                Ok(_) => {}
                Err(error) => return self.fail(error),
            }
        }
        let entries = std::mem::take(&mut self.copies)
            .into_iter()
            .map(|copy| TokenEntry {
                credential_active: active.contains(copy.access_key.as_bytes()),
                copy,
            })
            .collect();
        self.finish(entries)
    }

    fn finish(&mut self, entries: Vec<TokenEntry>) -> Effects {
        self.step = Step::Finish;
        self.output = Some(Ok(entries));
        smallvec![]
    }
}

impl Operation for ListTokensOperation {
    type Output = Vec<TokenEntry>;
    type Error = TokenListError;

    fn start(&mut self) -> Effects {
        self.scan(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.step, event) {
            (Step::Finish | Step::Error, _) => smallvec![],
            (_, Event::Storage(StorageEvent::Error { error })) => self.fail(error),
            (
                Step::ScanCopies,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.page(values, next_start_after),
            (Step::ReadCredentials, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.credentials(values)
            }
            (_, received) => self.fail(TokenListError::InvalidStateEvent(received)),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.step, Step::Finish | Step::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(TokenListError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use crate::tests::s3::{test_context, test_storage};
    use aruna_core::UserId;
    use aruna_core::credential_encryption::EncryptedS3Secret;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::encryption::{BucketKeyRef, SealedCopy};
    use std::time::Duration;

    #[tokio::test]
    async fn lists_bucket_tokens() {
        let (_dir, storage) = test_storage();
        let context = test_context(storage.clone());
        let user = UserId::new(Ulid::from_bytes([5; 16]), RealmId::from_bytes([1; 32]));
        let bucket_id = Ulid::from_bytes([4; 16]);
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let copy = |access_key: &str, bucket_id: Ulid, generation: u64| TokenCopy {
            key: BucketKeyRef::new(bucket_id, generation),
            access_key: access_key.to_string(),
            created_by: user,
            nonce: [0; 12],
            ciphertext: vec![0; 48],
            created_at_ms: 1,
        };
        let listed = [
            copy("ACTIVE", bucket_id, 1),
            copy("ACTIVE", bucket_id, 2),
            copy("REVOKED", bucket_id, 2),
        ];
        let elsewhere = copy("ACTIVE", Ulid::from_bytes([6; 16]), 1);
        let holder = SealedCopy {
            key: BucketKeyRef::new(bucket_id, 2),
            user_id: user,
            key_record: Ulid::from_bytes([7; 16]),
            key_id: "slot".to_string(),
            enc: [0; 32],
            ciphertext: vec![0; 48],
            created_at_ms: 1,
        };
        let mut writes: Vec<_> = listed
            .iter()
            .chain([&elsewhere])
            .map(|copy| (copy.key(), copy.to_bytes().unwrap()))
            .collect();
        writes.push((holder.key(), holder.to_bytes().unwrap()));
        let access = |access_key: &str, revoked_at| UserAccess {
            access_key: access_key.to_string(),
            user_identity: user,
            group_id: Ulid::from_bytes([3; 16]),
            secret: EncryptedS3Secret::empty(),
            expiry: now + Duration::from_secs(60),
            path_restrictions: None,
            issued_by: [0; 32],
            revoked_at,
        };
        let credentials = [access("ACTIVE", None), access("REVOKED", Some(now))];
        for (key_space, key, value) in writes
            .into_iter()
            .map(|(key, value)| (KEY_COPY_KEYSPACE, key, value))
            .chain(credentials.iter().map(|access| {
                let key = access.access_key.as_bytes().to_vec();
                (USER_ACCESS_KEYSPACE, key, access.to_bytes().unwrap())
            }))
        {
            let write = StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            };
            storage.send_storage_effect(write).await;
        }

        let entries = drive(ListTokensOperation::new(bucket_id, now), &context)
            .await
            .unwrap();
        // Every generation's copy of this bucket, never a holder copy or another bucket's.
        let found: Vec<_> = entries
            .iter()
            .map(|entry| (entry.copy.clone(), entry.credential_active))
            .collect();
        assert_eq!(
            found,
            [
                (listed[0].clone(), true),
                (listed[1].clone(), true),
                (listed[2].clone(), false),
            ]
        );
    }
}
