//! Plaintext copy requests of encrypted sources and the key holder check they need. A request
//! counts only while its requester is a current key holder of the source bucket (D30).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{
    BUCKET_ENCRYPTION_KEYSPACE, BUCKET_HOLDER_KEYSPACE, BUCKET_KEY_KEYSPACE,
    PLAINTEXT_COPY_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketHolder, BucketKeyRecord, HolderOrigin, KeyState,
};
use aruna_core::types::{Key, KeySpace, Value};
use thiserror::Error;
use ulid::Ulid;

use crate::s3::bucket::key::rows::{SettingsError, authority_keys, parse_authority};
use crate::s3::object::put::abe::keyed;

/// Key rows one page of the retained key scan reads.
const KEY_PAGE: usize = 64;

#[derive(Debug, Error, PartialEq)]
pub enum PlaintextError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("the plaintext request row does not decode: {0}")]
    Request(String),
    #[error("Bucket not found")]
    NoBucket,
    #[error(transparent)]
    Authority(#[from] SettingsError),
}

impl From<IoError> for PlaintextError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

type Turn<T> = Result<Next<T>, PlaintextError>;

/// Row of the plaintext request of an explicit copy job.
pub fn job_consent(job_key: &[u8]) -> Vec<u8> {
    [b"j", job_key].concat()
}

/// Row of the plaintext request of a sync relationship.
pub fn relationship_consent(id: Ulid) -> Vec<u8> {
    [&b"r"[..], &id.to_bytes()].concat()
}

/// The stored row of a plaintext request by `requester`.
pub fn consent_row(
    row: Vec<u8>,
    requester: UserId,
) -> Result<(KeySpace, Key, Value), PlaintextError> {
    let value = postcard::to_allocvec(&requester)
        .map_err(|error| PlaintextError::Request(error.to_string()))?;
    Ok((
        PLAINTEXT_COPY_KEYSPACE.to_string(),
        Key::from(row),
        value.into(),
    ))
}

/// The write of a plaintext request by `requester`.
pub fn consent_write(row: Vec<u8>, requester: UserId) -> Result<IoCall, PlaintextError> {
    Ok(IoCall::Write(vec![consent_row(row, requester)?]))
}

pub fn consent_delete(row: Vec<u8>) -> IoCall {
    IoCall::Delete(vec![(PLAINTEXT_COPY_KEYSPACE.to_string(), Key::from(row))])
}

/// Writes or deletes one plaintext request row, from [`consent_write`] or [`consent_delete`].
#[derive(Debug, PartialEq)]
pub struct ConsentStoreOperation {
    call: Option<IoCall>,
}

impl ConsentStoreOperation {
    pub fn new(call: IoCall) -> Self {
        Self { call: Some(call) }
    }
}

impl Operation for ConsentStoreOperation {
    type Output = ();
    type Error = PlaintextError;

    fn first(&mut self) -> Turn<()> {
        let call = self.call.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(call))
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        result.done()?;
        Ok(Next::Done(()))
    }
}

#[derive(Debug, PartialEq)]
enum HolderState {
    ReadConsent,
    ReadBucket,
    ReadAuthority,
    ReadGrant,
}

/// Whether `user` holds the key of `bucket` in realm `realm_id` now: its creator, a current group
/// admin, or an explicit holder, of any home realm. Losing the admin role ends it at once (D30).
/// With [`Self::with_consent`] the plaintext request row must also be the user's own.
#[derive(Debug, PartialEq)]
pub struct HolderCheckOperation {
    realm_id: RealmId,
    bucket: String,
    user: UserId,
    consent: Option<Vec<u8>>,
    group_id: Option<Ulid>,
    state: HolderState,
}

impl HolderCheckOperation {
    pub fn new(realm_id: RealmId, bucket: &str, user: UserId) -> Self {
        Self {
            realm_id,
            bucket: bucket.to_string(),
            user,
            consent: None,
            group_id: None,
            state: HolderState::ReadBucket,
        }
    }

    /// Also requires the plaintext request in `row` to be the user's.
    pub fn with_consent(mut self, row: Vec<u8>) -> Self {
        self.consent = Some(row);
        self
    }

    fn call(&mut self, state: HolderState, call: IoCall) -> Turn<bool> {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn read_bucket(&mut self) -> Turn<bool> {
        let read = read_row(S3_BUCKET_KEYSPACE, self.bucket.as_bytes().to_vec());
        self.call(HolderState::ReadBucket, read)
    }

    fn consent_read(&mut self, value: Option<Value>) -> Turn<bool> {
        match decode_requester(value.as_deref())? {
            Some(requester) if requester == self.user => self.read_bucket(),
            _ => Ok(Next::Done(false)),
        }
    }

    fn bucket_read(&mut self, value: Option<Value>) -> Turn<bool> {
        let value = value.ok_or(PlaintextError::NoBucket)?;
        let group_id = BucketInfo::from_bytes(&value)?.group_id;
        self.group_id = Some(group_id);
        let reads = authority_keys(&self.bucket, self.realm_id, group_id);
        self.call(HolderState::ReadAuthority, IoCall::Read(reads))
    }

    fn authority_read(&mut self, values: Vec<Option<Value>>) -> Turn<bool> {
        let group_id = self.group_id.ok_or(IoError::Unfinished)?;
        let state = parse_authority(keyed(&values), self.realm_id, group_id)?;
        if state.info.created_by == self.user || state.admins.contains(&self.user) {
            return Ok(Next::Done(true));
        }
        let Some(bucket_id) = state.settings.bucket_id else {
            return Ok(Next::Done(false));
        };
        let row = [&bucket_id.to_bytes()[..], &self.user.to_storage_key()].concat();
        let read = read_row(BUCKET_HOLDER_KEYSPACE, row);
        self.call(HolderState::ReadGrant, read)
    }

    fn grant_read(&mut self, value: Option<Value>) -> Turn<bool> {
        let grant = value
            .map(|value| BucketHolder::from_bytes(&value))
            .transpose()?;
        let explicit = grant.is_some_and(|grant| grant.origin == HolderOrigin::Explicit);
        Ok(Next::Done(explicit))
    }
}

impl Operation for HolderCheckOperation {
    type Output = bool;
    type Error = PlaintextError;

    fn first(&mut self) -> Turn<bool> {
        match self.consent.clone() {
            Some(row) => {
                let read = read_row(PLAINTEXT_COPY_KEYSPACE, row);
                self.call(HolderState::ReadConsent, read)
            }
            None => self.read_bucket(),
        }
    }

    fn next(&mut self, result: IoResult) -> Turn<bool> {
        match self.state {
            HolderState::ReadConsent => self.consent_read(result.value()?),
            HolderState::ReadBucket => self.bucket_read(result.value()?),
            HolderState::ReadAuthority => self.authority_read(result.values()?),
            HolderState::ReadGrant => self.grant_read(result.value()?),
        }
    }
}

/// Who asked for a plaintext copy in `row`, if anyone did.
#[derive(Debug, PartialEq)]
pub struct ConsentReadOperation {
    row: Vec<u8>,
}

impl ConsentReadOperation {
    pub fn new(row: Vec<u8>) -> Self {
        Self { row }
    }
}

impl Operation for ConsentReadOperation {
    type Output = Option<UserId>;
    type Error = PlaintextError;

    fn first(&mut self) -> Turn<Self::Output> {
        let read = read_row(PLAINTEXT_COPY_KEYSPACE, self.row.clone());
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Turn<Self::Output> {
        let value = result.value()?;
        Ok(Next::Done(decode_requester(value.as_deref())?))
    }
}

/// Whether `bucket` on this node encrypts its objects. With `retained` it also answers true
/// while a key that is not retired remains, since retained objects still need consent.
#[derive(Debug, PartialEq)]
pub struct EncryptionCheckOperation {
    bucket: String,
    retained: bool,
    bucket_id: Option<Ulid>,
    reading_keys: bool,
}

impl EncryptionCheckOperation {
    pub fn new(bucket: &str, retained: bool) -> Self {
        Self {
            bucket: bucket.to_string(),
            retained,
            bucket_id: None,
            reading_keys: false,
        }
    }

    fn read_keys(&mut self, after: Option<Key>) -> Turn<bool> {
        let Some(bucket_id) = self.bucket_id else {
            return Ok(Next::Done(false));
        };
        self.reading_keys = true;
        Ok(Next::Call(IoCall::Page {
            space: BUCKET_KEY_KEYSPACE.to_string(),
            prefix: bucket_id.to_bytes().to_vec().into(),
            after,
            limit: KEY_PAGE,
        }))
    }

    fn settings_read(&mut self, value: Option<Value>) -> Turn<bool> {
        let settings = BucketEncryption::from_row(value.as_deref())?;
        if settings.is_encrypted() || !self.retained {
            return Ok(Next::Done(settings.is_encrypted()));
        }
        self.bucket_id = settings.bucket_id;
        self.read_keys(None)
    }

    fn keys_read(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Turn<bool> {
        for (_, value) in values {
            if BucketKeyRecord::from_bytes(&value)?.state != KeyState::Retired {
                return Ok(Next::Done(true));
            }
        }
        match next {
            Some(next) => self.read_keys(Some(next)),
            None => Ok(Next::Done(false)),
        }
    }
}

impl Operation for EncryptionCheckOperation {
    type Output = bool;
    type Error = PlaintextError;

    fn first(&mut self) -> Turn<bool> {
        let key = self.bucket.as_bytes().to_vec();
        Ok(Next::Call(read_row(BUCKET_ENCRYPTION_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Turn<bool> {
        if self.reading_keys {
            let (values, next) = result.page()?;
            return self.keys_read(values, next);
        }
        self.settings_read(result.value()?)
    }
}

fn decode_requester(value: Option<&[u8]>) -> Result<Option<UserId>, PlaintextError> {
    value
        .map(|value| {
            postcard::from_bytes(value).map_err(|error| PlaintextError::Request(error.to_string()))
        })
        .transpose()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::group::GroupAuthorizationDocument;
    use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
    use aruna_core::structs::storage::encryption::{EncryptionMode, GrantState};
    use std::time::SystemTime;

    pub(crate) fn user(seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([3; 32]))
    }

    /// An encrypting bucket created by user 1, whose group user 2 administers, with an explicit
    /// grant to user 3.
    pub(crate) async fn bucket(context: &DriverContext) {
        let realm_id = RealmId::from_bytes([3; 32]);
        let group_id = Ulid::from_bytes([4; 16]);
        let bucket_id = Ulid::from_bytes([5; 16]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
            user_id: user(2),
            realm_id,
        };
        let realm = RealmAuthorizationDocument {
            realm_id,
            roles: Default::default(),
            operation_restrictions: Default::default(),
        };
        let group = GroupAuthorizationDocument::default_group_doc(user(2), realm_id, group_id);
        let info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: user(1),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
            distribution: None,
        };
        let settings = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(bucket_id),
            key_generation: 1,
            ..Default::default()
        };
        let grant = BucketHolder {
            bucket_id,
            user_id: user(3),
            origin: HolderOrigin::Explicit,
            state: GrantState::Ready,
            granted_by: user(1),
            granted_at_ms: 0,
        };
        let rows = [
            (
                AUTH_KEYSPACE,
                realm_id.as_bytes().to_vec(),
                realm.to_bytes(&actor).unwrap(),
            ),
            (
                AUTH_KEYSPACE,
                group_id.to_bytes().to_vec(),
                group.to_bytes(&actor).unwrap(),
            ),
            (
                S3_BUCKET_KEYSPACE,
                b"sealed".to_vec(),
                info.to_bytes().unwrap(),
            ),
            (
                BUCKET_ENCRYPTION_KEYSPACE,
                b"sealed".to_vec(),
                settings.to_bytes().unwrap(),
            ),
            (
                BUCKET_HOLDER_KEYSPACE,
                grant.key(),
                grant.to_bytes().unwrap(),
            ),
        ];
        for (key_space, key, value) in rows {
            let write = StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            };
            context.storage_handle.send_storage_effect(write).await;
        }
    }

    #[tokio::test]
    async fn plaintext_needs_holder() {
        let (_dir, storage) = crate::tests::s3::test_storage();
        let context = crate::testing::test_context(storage);
        bucket(&context).await;
        let realm = RealmId::from_bytes([3; 32]);
        let encrypted = EncryptionCheckOperation::new("sealed", false);
        assert!(drive(encrypted, &context).await.unwrap());
        for (seed, holder) in [(1, true), (2, true), (3, true), (4, false)] {
            assert_eq!(
                drive(
                    HolderCheckOperation::new(realm, "sealed", user(seed)),
                    &context
                )
                .await,
                Ok(holder)
            );
        }

        let row = relationship_consent(Ulid::from_bytes([6; 16]));
        // No request, or a request of another user, never permits plaintext.
        assert!(
            !drive(
                HolderCheckOperation::new(realm, "sealed", user(1)).with_consent(row.clone()),
                &context,
            )
            .await
            .unwrap()
        );
        drive(
            ConsentStoreOperation::new(consent_write(row.clone(), user(4)).unwrap()),
            &context,
        )
        .await
        .unwrap();
        assert!(
            !drive(
                HolderCheckOperation::new(realm, "sealed", user(4)).with_consent(row.clone()),
                &context,
            )
            .await
            .unwrap()
        );
        assert!(
            !drive(
                HolderCheckOperation::new(realm, "sealed", user(1)).with_consent(row.clone()),
                &context,
            )
            .await
            .unwrap()
        );
        drive(
            ConsentStoreOperation::new(consent_write(row.clone(), user(1)).unwrap()),
            &context,
        )
        .await
        .unwrap();
        assert!(
            drive(
                HolderCheckOperation::new(realm, "sealed", user(1)).with_consent(row.clone()),
                &context,
            )
            .await
            .unwrap()
        );
        drive(
            ConsentStoreOperation::new(consent_delete(row.clone())),
            &context,
        )
        .await
        .unwrap();
        assert!(
            !drive(
                HolderCheckOperation::new(realm, "sealed", user(1)).with_consent(row),
                &context,
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn foreign_explicit_holds() {
        // An explicit grant makes a user of another realm a holder; the creator's ULID does not.
        let (_dir, storage) = crate::tests::s3::test_storage();
        let context = crate::testing::test_context(storage);
        bucket(&context).await;
        let local = RealmId::from_bytes([3; 32]);
        let foreign =
            |seed| UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([9; 32]));
        let grant = BucketHolder {
            bucket_id: Ulid::from_bytes([5; 16]),
            user_id: foreign(7),
            origin: HolderOrigin::Explicit,
            state: GrantState::Ready,
            granted_by: user(1),
            granted_at_ms: 0,
        };
        let write = StorageEffect::Write {
            key_space: BUCKET_HOLDER_KEYSPACE.to_string(),
            key: grant.key().into(),
            value: grant.to_bytes().unwrap().into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(write).await;
        for (seed, holder) in [(1, false), (7, true)] {
            assert_eq!(
                drive(
                    HolderCheckOperation::new(local, "sealed", foreign(seed)),
                    &context
                )
                .await,
                Ok(holder)
            );
        }

        // Plaintext still needs the consent of that holder; a non-holder's consent never counts.
        let row = relationship_consent(Ulid::from_bytes([6; 16]));
        let allowed = async |user| {
            drive(
                HolderCheckOperation::new(local, "sealed", user).with_consent(row.clone()),
                &context,
            )
            .await
            .unwrap()
        };
        assert!(!allowed(foreign(7)).await);
        drive(
            ConsentStoreOperation::new(consent_write(row.clone(), foreign(1)).unwrap()),
            &context,
        )
        .await
        .unwrap();
        assert!(!allowed(foreign(1)).await && !allowed(foreign(7)).await);
        drive(
            ConsentStoreOperation::new(consent_write(row.clone(), foreign(7)).unwrap()),
            &context,
        )
        .await
        .unwrap();
        assert!(allowed(foreign(7)).await);
    }

    fn read_result(value: Option<Vec<u8>>) -> IoResult {
        IoResult::Values(vec![value.map(Into::into)])
    }

    fn info(created_by: UserId) -> Vec<u8> {
        BucketInfo {
            group_id: Ulid::from_bytes([4; 16]),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
            distribution: None,
        }
        .to_bytes()
        .unwrap()
    }

    #[test]
    fn foreign_request_stops() {
        // A request of another user answers false before the bucket is read.
        let realm = RealmId::from_bytes([3; 32]);
        let row = relationship_consent(Ulid::from_bytes([6; 16]));
        let mut operation =
            HolderCheckOperation::new(realm, "sealed", user(1)).with_consent(row.clone());
        let theirs = postcard::to_allocvec(&user(2)).unwrap();
        assert_eq!(
            operation.first(),
            Ok(Next::Call(read_row(PLAINTEXT_COPY_KEYSPACE, row)))
        );
        assert_eq!(
            operation.next(read_result(Some(theirs))),
            Ok(Next::Done(false))
        );
    }

    #[test]
    fn holder_read_order() {
        let realm = RealmId::from_bytes([3; 32]);
        let row = relationship_consent(Ulid::from_bytes([6; 16]));
        let mut operation =
            HolderCheckOperation::new(realm, "sealed", user(1)).with_consent(row.clone());
        let mine = postcard::to_allocvec(&user(1)).unwrap();
        let group_id = Ulid::from_bytes([4; 16]);
        operation.first().unwrap();
        assert_eq!(
            operation.next(read_result(Some(mine))),
            Ok(Next::Call(read_row(S3_BUCKET_KEYSPACE, b"sealed".to_vec())))
        );
        assert_eq!(
            operation.next(read_result(Some(info(user(1))))),
            Ok(Next::Call(IoCall::Read(authority_keys(
                "sealed", realm, group_id
            ))))
        );
        // A missing bucket fails instead of answering that nobody holds its key.
        let mut missing = HolderCheckOperation::new(realm, "gone", user(1));
        missing.first().unwrap();
        assert_eq!(
            missing.next(read_result(None)),
            Err(PlaintextError::NoBucket)
        );
    }

    #[test]
    fn errors_fail_checks() {
        assert_eq!(
            PlaintextError::from(IoError::Storage(StorageError::Timeout)),
            PlaintextError::Storage(StorageError::Timeout)
        );
        let mut consent = ConsentReadOperation::new(b"row".to_vec());
        consent.first().unwrap();
        assert!(matches!(
            consent.next(read_result(Some(vec![0xff; 40]))),
            Err(PlaintextError::Request(_))
        ));
        let mut keys = EncryptionCheckOperation::new("plain", true);
        keys.first().unwrap();
        let retained = BucketEncryption {
            bucket_id: Some(Ulid::from_bytes([5; 16])),
            ..Default::default()
        };
        let turn = keys.next(read_result(Some(retained.to_bytes().unwrap())));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Page { .. }))));
        let unreadable = IoResult::Page {
            rows: vec![(b"key".to_vec().into(), vec![0xff].into())],
            next: None,
        };
        assert!(keys.next(unreadable).is_err());
    }

    #[test]
    fn consent_store_calls() {
        // The request row is written or deleted by the one call the operation was given.
        let row = b"row".to_vec();
        let write = consent_write(row.clone(), user(1)).unwrap();
        let mut store = ConsentStoreOperation::new(write);
        assert_eq!(
            store.first(),
            Ok(Next::Call(IoCall::Write(vec![
                consent_row(row.clone(), user(1)).unwrap()
            ])))
        );
        assert_eq!(store.next(IoResult::Done), Ok(Next::Done(())));
        let mut delete = ConsentStoreOperation::new(consent_delete(row.clone()));
        assert_eq!(
            delete.first(),
            Ok(Next::Call(IoCall::Delete(vec![(
                PLAINTEXT_COPY_KEYSPACE.to_string(),
                row.into()
            )])))
        );
    }

    #[test]
    fn encryption_check_modes() {
        // Without `retained` the settings decide alone; an unencrypted bucket without keys is plain.
        let mut settings = EncryptionCheckOperation::new("plain", false);
        settings.first().unwrap();
        assert_eq!(settings.next(read_result(None)), Ok(Next::Done(false)));
        let mut retained = EncryptionCheckOperation::new("plain", true);
        retained.first().unwrap();
        assert_eq!(retained.next(read_result(None)), Ok(Next::Done(false)));
    }

    #[test]
    fn wrong_results_fail() {
        // Every check only reads, so a wrong answer fails it and leaves nothing to clean up.
        let realm = RealmId::from_bytes([3; 32]);
        let mut holder = HolderCheckOperation::new(realm, "sealed", user(1));
        holder.first().unwrap();
        assert!(holder.next(IoResult::Done).is_err());
        assert!(holder.abort().is_empty());
        let mut consent = ConsentReadOperation::new(b"row".to_vec());
        consent.first().unwrap();
        assert!(consent.next(IoResult::Done).is_err());
        assert!(consent.abort().is_empty());
        let mut encryption = EncryptionCheckOperation::new("b", true);
        encryption.first().unwrap();
        assert!(encryption.next(IoResult::Done).is_err());
        assert!(encryption.abort().is_empty());
        let mut store = ConsentStoreOperation::new(consent_delete(b"row".to_vec()));
        store.first().unwrap();
        assert!(store.next(read_result(None)).is_err());
        assert!(store.abort().is_empty());
    }
}
