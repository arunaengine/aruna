//! Lists the token grants of one encrypted bucket with the state of their credentials.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{ABE_GRANT_KEYSPACE, USER_ACCESS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::PrefixPages;
use aruna_core::structs::storage::abe::AbeError;
use aruna_core::structs::storage::abe_access::KeyGrant;
use aruna_core::structs::storage::blob::UserAccess;
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
    #[error("could not list tokens: {0}")]
    Io(IoError),
}

impl From<IoError> for TokenListError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

/// One token grant and whether its credential still authenticates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenEntry {
    pub grant: KeyGrant,
    pub credential_active: bool,
}

type Step = Result<Next<Vec<TokenEntry>>, TokenListError>;

#[derive(Debug, PartialEq)]
pub struct ListTokensOperation {
    now: SystemTime,
    pages: PrefixPages,
    grants: Vec<KeyGrant>,
    /// The access keys read, in read order, once every grant page was taken.
    reads: Option<Vec<String>>,
}

impl ListTokensOperation {
    /// Lists every generation's token grants of the bucket with stable id `bucket_id`.
    pub fn new(bucket_id: Ulid, now: SystemTime) -> Self {
        let prefix = bucket_id.to_bytes().to_vec();
        Self {
            now,
            pages: PrefixPages::new(ABE_GRANT_KEYSPACE, prefix, SCAN_PAGE),
            grants: Vec::new(),
            reads: None,
        }
    }

    fn page(&mut self, result: IoResult) -> Step {
        for (_, value) in self.pages.take(result)? {
            let grant = KeyGrant::from_bytes(&value)?;
            if grant.context.request.credential_id.is_some() {
                self.grants.push(grant);
            }
        }
        if !self.pages.done() {
            return Ok(Next::Call(self.pages.call()));
        }
        let access_keys: BTreeSet<_> = (self.grants.iter())
            .filter_map(|grant| grant.context.request.credential_id.clone())
            .collect();
        if access_keys.is_empty() {
            return Ok(Next::Done(Vec::new()));
        }
        let access_keys: Vec<_> = access_keys.into_iter().collect();
        let reads = (access_keys.iter())
            .map(|access_key| {
                let key = access_key.as_bytes().to_vec().into();
                (USER_ACCESS_KEYSPACE.to_string(), key)
            })
            .collect();
        self.reads = Some(access_keys);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn credentials(&mut self, keys: Vec<String>, result: IoResult) -> Step {
        let mut active = BTreeSet::new();
        for (key, value) in keys.into_iter().zip(result.values()?) {
            let Some(value) = value else { continue };
            let access = UserAccess::from_bytes(&value)?;
            if !access.is_revoked() && !access.is_expired(self.now) {
                active.insert(key);
            }
        }
        let entries = std::mem::take(&mut self.grants)
            .into_iter()
            .map(|grant| TokenEntry {
                credential_active: (grant.context.request.credential_id.as_ref())
                    .is_some_and(|access_key| active.contains(access_key)),
                grant,
            })
            .collect();
        Ok(Next::Done(entries))
    }
}

impl Operation for ListTokensOperation {
    type Output = Vec<TokenEntry>;
    type Error = TokenListError;

    fn first(&mut self) -> Step {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.reads.take() {
            Some(keys) => self.credentials(keys, result),
            None => self.page(result),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use crate::testing::test_context;
    use crate::tests::s3::test_storage;
    use aruna_core::UserId;
    use aruna_core::credential_encryption::EncryptedS3Secret;
    use aruna_core::effects::StorageEffect;
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
