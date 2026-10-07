//! Lists the token grants of one encrypted bucket with the state of their credentials.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{ABE_GRANT_KEYSPACE, USER_ACCESS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::abe::AbeError;
use aruna_core::structs::storage::abe_access::KeyGrant;
use aruna_core::structs::storage::blob::UserAccess;
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
    #[error(transparent)]
    Abe(#[from] AbeError),
    #[error("unexpected event while listing tokens: {0:?}")]
    InvalidStateEvent(Event),
    #[error("the token listing did not finish")]
    NotFinished,
}

/// One token grant and whether its credential still authenticates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenEntry {
    pub grant: KeyGrant,
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
    grants: Vec<KeyGrant>,
    output: Option<Result<Vec<TokenEntry>, TokenListError>>,
}

impl ListTokensOperation {
    /// Lists every generation's token grants of the bucket with stable id `bucket_id`.
    pub fn new(bucket_id: Ulid, now: SystemTime) -> Self {
        Self {
            bucket_id,
            now,
            step: Step::ScanCopies,
            grants: Vec::new(),
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
            key_space: ABE_GRANT_KEYSPACE.to_string(),
            prefix: Some(self.bucket_id.to_bytes().to_vec().into()),
            start: start.map(IterStart::After),
            limit: SCAN_PAGE,
            txn_id: None,
        })]
    }

    fn page(&mut self, values: Vec<(Key, Key)>, next: Option<Key>) -> Effects {
        for (_, value) in values {
            match KeyGrant::from_bytes(&value) {
                Ok(grant) if grant.context.request.credential_id.is_some() => {
                    self.grants.push(grant)
                }
                Ok(_) => {}
                Err(error) => return self.fail(error),
            }
        }
        if next.is_some() {
            return self.scan(next);
        }
        let access_keys: BTreeSet<_> = self
            .grants
            .iter()
            .filter_map(|grant| grant.context.request.credential_id.as_ref())
            .collect();
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
        let entries = std::mem::take(&mut self.grants)
            .into_iter()
            .map(|grant| TokenEntry {
                credential_active: grant
                    .context
                    .request
                    .credential_id
                    .as_ref()
                    .is_some_and(|access_key| active.contains(access_key.as_bytes())),
                grant,
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
    use aruna_core::structs::storage::abe::AbeParameters;
    use aruna_core::structs::storage::abe_access::{GrantContext, KeyIssuer, KeyRequest, KeyScope};
    use aruna_core::structs::storage::encryption::BucketKeyRef;
    use std::time::Duration;

    #[tokio::test]
    async fn lists_bucket_tokens() {
        let (_dir, storage) = test_storage();
        let context = test_context(storage.clone());
        let realm = RealmId::from_bytes([1; 32]);
        let user = UserId::new(Ulid::from_bytes([5; 16]), realm);
        let node = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let bucket_id = Ulid::from_bytes([4; 16]);
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let grant = |access_key: Option<&str>, bucket_id, generation, id: u8| KeyGrant {
            context: GrantContext {
                request: KeyRequest {
                    request_id: Ulid::from_bytes([id; 16]),
                    requesting_user: user,
                    recipient_user: user,
                    recipient_record: Some(Ulid::from_bytes([7; 16])),
                    recipient_public: Some([5; 32]),
                    recipient_fingerprint: Some([6; 32]),
                    bucket: "reef".to_string(),
                    parameters: AbeParameters {
                        realm_id: realm,
                        node_id: node,
                        key: BucketKeyRef::new(bucket_id, generation),
                        fingerprint: [7; 32],
                        parameters: vec![9; 3],
                    },
                    scope: KeyScope::Subtree("data/".to_string()),
                    epochs: vec![1],
                    credential_id: access_key.map(str::to_string),
                    restrictions: None,
                    revisions: vec![[8; 32]],
                    created_at_ms: 1,
                },
                issuer: KeyIssuer::Node(node),
            },
            enc: [0; 32],
            ciphertext: vec![0; 48],
        };
        let listed = [
            grant(Some("ACTIVE"), bucket_id, 1, 1),
            grant(Some("ACTIVE"), bucket_id, 2, 2),
            grant(Some("REVOKED"), bucket_id, 2, 3),
        ];
        let others = [
            grant(Some("ACTIVE"), Ulid::from_bytes([6; 16]), 1, 4),
            grant(None, bucket_id, 2, 5),
        ];
        let writes: Vec<_> = listed
            .iter()
            .chain(&others)
            .map(|grant| (grant.context.request.key(), grant.to_bytes().unwrap()))
            .collect();
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
            .map(|(key, value)| (ABE_GRANT_KEYSPACE, key, value))
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
        // Every generation's token grant of this bucket, never a user grant or another bucket's.
        let found: Vec<_> = entries
            .iter()
            .map(|entry| (entry.grant.clone(), entry.credential_active))
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
