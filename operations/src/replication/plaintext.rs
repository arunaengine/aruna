//! Plaintext copy requests of encrypted sources and the key holder check they need. A request
//! counts only while its requester is a current key holder of the source bucket (D30).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BUCKET_ENCRYPTION_KEYSPACE, BUCKET_HOLDER_KEYSPACE, BUCKET_KEY_KEYSPACE,
    PLAINTEXT_COPY_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketHolder, BucketKeyRecord, HolderOrigin, KeyState,
};
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::s3::bucket::key::rows::{SettingsError, authority_read, parse_authority};

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
    #[error("unexpected event in a plaintext check: {0}")]
    UnexpectedEvent(String),
    #[error("the plaintext check did not finish")]
    NotFinished,
}

/// Row of the plaintext request of an explicit copy job.
pub fn job_consent(job_key: &[u8]) -> Vec<u8> {
    [b"j", job_key].concat()
}

/// Row of the plaintext request of a sync relationship.
pub fn relationship_consent(id: Ulid) -> Vec<u8> {
    [&b"r"[..], &id.to_bytes()].concat()
}

/// The write of a plaintext request by `requester`.
pub fn consent_write(row: Vec<u8>, requester: UserId) -> Result<Effect, PlaintextError> {
    let value = postcard::to_allocvec(&requester)
        .map_err(|error| PlaintextError::Request(error.to_string()))?;
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: PLAINTEXT_COPY_KEYSPACE.to_string(),
        key: Key::from(row),
        value: value.into(),
        txn_id: None,
    }))
}

pub fn consent_delete(row: Vec<u8>) -> Effect {
    Effect::Storage(StorageEffect::Delete {
        key_space: PLAINTEXT_COPY_KEYSPACE.to_string(),
        key: Key::from(row),
        txn_id: None,
    })
}

/// Writes or deletes one plaintext request row, from [`consent_write`] or [`consent_delete`].
#[derive(Debug, PartialEq)]
pub struct ConsentStoreOperation {
    effect: Option<Effect>,
    output: Option<Result<(), PlaintextError>>,
}

impl ConsentStoreOperation {
    pub fn new(effect: Effect) -> Self {
        Self {
            effect: Some(effect),
            output: None,
        }
    }
}

impl Operation for ConsentStoreOperation {
    type Output = ();
    type Error = PlaintextError;

    fn start(&mut self) -> Effects {
        self.effect.take().into_iter().collect()
    }

    fn step(&mut self, event: Event) -> Effects {
        self.output = Some(match (&self.output, event) {
            (
                None,
                Event::Storage(
                    StorageEvent::WriteResult { .. } | StorageEvent::DeleteResult { .. },
                ),
            ) => Ok(()),
            (None, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (_, event) => Err(PlaintextError::UnexpectedEvent(event.label().to_string())),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<(), PlaintextError> {
        self.output.unwrap_or(Err(PlaintextError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Debug, PartialEq)]
enum HolderState {
    Init,
    ReadConsent,
    ReadBucket,
    ReadAuthority,
    ReadGrant,
    Finish,
    Error,
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
    output: Option<Result<bool, PlaintextError>>,
}

impl HolderCheckOperation {
    pub fn new(realm_id: RealmId, bucket: &str, user: UserId) -> Self {
        Self {
            realm_id,
            bucket: bucket.to_string(),
            user,
            consent: None,
            group_id: None,
            state: HolderState::Init,
            output: None,
        }
    }

    /// Also requires the plaintext request in `row` to be the user's.
    pub fn with_consent(mut self, row: Vec<u8>) -> Self {
        self.consent = Some(row);
        self
    }

    fn done(&mut self, result: Result<bool, PlaintextError>) -> Effects {
        self.state = match result {
            Ok(_) => HolderState::Finish,
            Err(_) => HolderState::Error,
        };
        self.output = Some(result);
        smallvec![]
    }

    fn read_bucket(&mut self) -> Effects {
        self.state = HolderState::ReadBucket;
        smallvec![read(S3_BUCKET_KEYSPACE, self.bucket.as_bytes().to_vec())]
    }

    fn consent_read(&mut self, value: Option<Value>) -> Effects {
        match decode_requester(value.as_deref()) {
            Ok(Some(requester)) if requester == self.user => self.read_bucket(),
            Ok(_) => self.done(Ok(false)),
            Err(error) => self.done(Err(error)),
        }
    }

    fn bucket_read(&mut self, value: Option<Value>) -> Effects {
        let Some(value) = value else {
            return self.done(Err(PlaintextError::NoBucket));
        };
        let group_id = match BucketInfo::from_bytes(&value) {
            Ok(info) => info.group_id,
            Err(error) => return self.done(Err(error.into())),
        };
        self.group_id = Some(group_id);
        self.state = HolderState::ReadAuthority;
        smallvec![authority_read(&self.bucket, self.realm_id, group_id, None)]
    }

    fn authority_read(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let Some(group_id) = self.group_id else {
            return self.done(Err(PlaintextError::NotFinished));
        };
        let state = match parse_authority(values, self.realm_id, group_id) {
            Ok(state) => state,
            Err(error) => return self.done(Err(error.into())),
        };
        if state.info.created_by == self.user || state.admins.contains(&self.user) {
            return self.done(Ok(true));
        }
        let Some(bucket_id) = state.settings.bucket_id else {
            return self.done(Ok(false));
        };
        self.state = HolderState::ReadGrant;
        let row = [&bucket_id.to_bytes()[..], &self.user.to_storage_key()].concat();
        smallvec![read(BUCKET_HOLDER_KEYSPACE, row)]
    }

    fn grant_read(&mut self, value: Option<Value>) -> Effects {
        let grant = value
            .map(|value| BucketHolder::from_bytes(&value))
            .transpose();
        match grant {
            Ok(grant) => self.done(Ok(
                grant.is_some_and(|grant| grant.origin == HolderOrigin::Explicit)
            )),
            Err(error) => self.done(Err(error.into())),
        }
    }
}

impl Operation for HolderCheckOperation {
    type Output = bool;
    type Error = PlaintextError;

    fn start(&mut self) -> Effects {
        match &self.consent {
            Some(row) => {
                self.state = HolderState::ReadConsent;
                smallvec![read(PLAINTEXT_COPY_KEYSPACE, row.clone())]
            }
            None => self.read_bucket(),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (
                HolderState::ReadConsent
                | HolderState::ReadBucket
                | HolderState::ReadAuthority
                | HolderState::ReadGrant,
                Event::Storage(StorageEvent::Error { error }),
            ) => self.done(Err(error.into())),
            (HolderState::ReadConsent, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.consent_read(value)
            }
            (HolderState::ReadBucket, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.bucket_read(value)
            }
            (
                HolderState::ReadAuthority,
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => self.authority_read(values),
            (HolderState::ReadGrant, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.grant_read(value)
            }
            (_, event) => self.done(Err(PlaintextError::UnexpectedEvent(
                event.label().to_string(),
            ))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, HolderState::Finish | HolderState::Error)
    }

    fn finalize(self) -> Result<bool, PlaintextError> {
        self.output.unwrap_or(Err(PlaintextError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Who asked for a plaintext copy in `row`, if anyone did.
#[derive(Debug, PartialEq)]
pub struct ConsentReadOperation {
    row: Vec<u8>,
    output: Option<Result<Option<UserId>, PlaintextError>>,
}

impl ConsentReadOperation {
    pub fn new(row: Vec<u8>) -> Self {
        Self { row, output: None }
    }
}

impl Operation for ConsentReadOperation {
    type Output = Option<UserId>;
    type Error = PlaintextError;

    fn start(&mut self) -> Effects {
        smallvec![read(PLAINTEXT_COPY_KEYSPACE, self.row.clone())]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.output = Some(match (&self.output, event) {
            (None, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                decode_requester(value.as_deref())
            }
            (None, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (_, event) => Err(PlaintextError::UnexpectedEvent(event.label().to_string())),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Option<UserId>, PlaintextError> {
        self.output.unwrap_or(Err(PlaintextError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Debug, PartialEq)]
enum EncryptionState {
    Init,
    ReadSettings,
    ReadKeys,
    Finish,
    Error,
}

/// Whether `bucket` on this node encrypts its objects. With `retained` it also answers true
/// while a key that is not retired remains, since retained objects still need consent.
#[derive(Debug, PartialEq)]
pub struct EncryptionCheckOperation {
    bucket: String,
    retained: bool,
    bucket_id: Option<Ulid>,
    state: EncryptionState,
    output: Option<Result<bool, PlaintextError>>,
}

impl EncryptionCheckOperation {
    pub fn new(bucket: &str, retained: bool) -> Self {
        Self {
            bucket: bucket.to_string(),
            retained,
            bucket_id: None,
            state: EncryptionState::Init,
            output: None,
        }
    }

    fn done(&mut self, result: Result<bool, PlaintextError>) -> Effects {
        self.state = match result {
            Ok(_) => EncryptionState::Finish,
            Err(_) => EncryptionState::Error,
        };
        self.output = Some(result);
        smallvec![]
    }

    fn read_keys(&mut self, after: Option<Key>) -> Effects {
        let Some(bucket_id) = self.bucket_id else {
            return self.done(Ok(false));
        };
        self.state = EncryptionState::ReadKeys;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BUCKET_KEY_KEYSPACE.to_string(),
            prefix: Some(bucket_id.to_bytes().to_vec().into()),
            start: after.map(IterStart::After),
            limit: KEY_PAGE,
            txn_id: None,
        })]
    }

    fn settings_read(&mut self, value: Option<Value>) -> Effects {
        let settings = match BucketEncryption::from_row(value.as_deref()) {
            Ok(settings) => settings,
            Err(error) => return self.done(Err(error.into())),
        };
        if settings.is_encrypted() || !self.retained {
            return self.done(Ok(settings.is_encrypted()));
        }
        self.bucket_id = settings.bucket_id;
        self.read_keys(None)
    }

    fn keys_read(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        for (_, value) in values {
            match BucketKeyRecord::from_bytes(&value) {
                Ok(record) if record.state != KeyState::Retired => return self.done(Ok(true)),
                Ok(_) => {}
                Err(error) => return self.done(Err(error.into())),
            }
        }
        match next {
            Some(next) => self.read_keys(Some(next)),
            None => self.done(Ok(false)),
        }
    }
}

impl Operation for EncryptionCheckOperation {
    type Output = bool;
    type Error = PlaintextError;

    fn start(&mut self) -> Effects {
        self.state = EncryptionState::ReadSettings;
        smallvec![read(
            BUCKET_ENCRYPTION_KEYSPACE,
            self.bucket.as_bytes().to_vec()
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (
                EncryptionState::ReadSettings | EncryptionState::ReadKeys,
                Event::Storage(StorageEvent::Error { error }),
            ) => self.done(Err(error.into())),
            (
                EncryptionState::ReadSettings,
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => self.settings_read(value),
            (
                EncryptionState::ReadKeys,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.keys_read(values, next_start_after),
            (_, event) => self.done(Err(PlaintextError::UnexpectedEvent(
                event.label().to_string(),
            ))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, EncryptionState::Finish | EncryptionState::Error)
    }

    fn finalize(self) -> Result<bool, PlaintextError> {
        self.output.unwrap_or(Err(PlaintextError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

fn read(key_space: &str, key: Vec<u8>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: key_space.to_string(),
        key: key.into(),
        txn_id: None,
    })
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
    use aruna_core::keyspaces::{AUTH_KEYSPACE, S3_BUCKET_KEYSPACE};
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::group::GroupAuthorizationDocument;
    use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
    use aruna_core::structs::storage::blob::BucketInfo;
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
        let context = crate::tests::s3::test_context(storage);
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
        let context = crate::tests::s3::test_context(storage);
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

    fn read_result(value: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: value.map(Into::into),
        })
    }

    fn timeout() -> Event {
        Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        })
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
        let start = [read(PLAINTEXT_COPY_KEYSPACE, row)];
        check_trace(
            &mut operation,
            &start,
            vec![(read_result(Some(theirs)), vec![])],
        );
        assert_eq!(operation.finalize(), Ok(false));
    }

    #[test]
    fn holder_read_order() {
        let realm = RealmId::from_bytes([3; 32]);
        let row = relationship_consent(Ulid::from_bytes([6; 16]));
        let mut operation =
            HolderCheckOperation::new(realm, "sealed", user(1)).with_consent(row.clone());
        let mine = postcard::to_allocvec(&user(1)).unwrap();
        let group_id = Ulid::from_bytes([4; 16]);
        let steps = vec![
            (
                read_result(Some(mine)),
                vec![read(S3_BUCKET_KEYSPACE, b"sealed".to_vec())],
            ),
            (
                read_result(Some(info(user(1)))),
                vec![authority_read("sealed", realm, group_id, None)],
            ),
        ];
        check_trace(&mut operation, &[read(PLAINTEXT_COPY_KEYSPACE, row)], steps);
        // A missing bucket fails instead of answering that nobody holds its key.
        let mut missing = HolderCheckOperation::new(realm, "gone", user(1));
        missing.start();
        missing.step(read_result(None));
        assert_eq!(missing.finalize(), Err(PlaintextError::NoBucket));
    }

    #[test]
    fn errors_fail_checks() {
        let realm = RealmId::from_bytes([3; 32]);
        let mut holder = HolderCheckOperation::new(realm, "sealed", user(1));
        holder.start();
        holder.step(timeout());
        assert_eq!(
            holder.finalize(),
            Err(PlaintextError::Storage(StorageError::Timeout))
        );
        let mut consent = ConsentReadOperation::new(b"row".to_vec());
        consent.start();
        consent.step(read_result(Some(vec![0xff; 40])));
        assert!(matches!(
            consent.finalize(),
            Err(PlaintextError::Request(_))
        ));
        let mut store = ConsentStoreOperation::new(consent_delete(b"row".to_vec()));
        store.start();
        store.step(timeout());
        assert_eq!(
            store.finalize(),
            Err(PlaintextError::Storage(StorageError::Timeout))
        );
        let mut keys = EncryptionCheckOperation::new("plain", true);
        keys.start();
        let retained = BucketEncryption {
            bucket_id: Some(Ulid::from_bytes([5; 16])),
            ..Default::default()
        };
        let effects = keys.step(read_result(Some(retained.to_bytes().unwrap())));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { .. })]
        ));
        keys.step(timeout());
        assert_eq!(
            keys.finalize(),
            Err(PlaintextError::Storage(StorageError::Timeout))
        );
    }

    #[test]
    fn encryption_check_modes() {
        // Without `retained` the settings decide alone; an unencrypted bucket without keys is plain.
        let mut settings = EncryptionCheckOperation::new("plain", false);
        settings.start();
        assert!(settings.step(read_result(None)).is_empty());
        assert_eq!(settings.finalize(), Ok(false));
        let mut retained = EncryptionCheckOperation::new("plain", true);
        retained.start();
        assert!(retained.step(read_result(None)).is_empty());
        assert_eq!(retained.finalize(), Ok(false));
    }

    #[test]
    fn operations_reject_events() {
        let realm = RealmId::from_bytes([3; 32]);
        let read_or_error = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. })
            )
        };
        let holder = || {
            let mut operation = HolderCheckOperation::new(realm, "sealed", user(1));
            operation.start();
            operation
        };
        assert!(!rejects_events(holder, sample_events, read_or_error).is_empty());
        let consent = || {
            let mut operation = ConsentReadOperation::new(b"row".to_vec());
            operation.start();
            operation
        };
        assert!(!rejects_events(consent, sample_events, read_or_error).is_empty());
        let encryption = || {
            let mut operation = EncryptionCheckOperation::new("b", true);
            operation.start();
            operation
        };
        assert!(!rejects_events(encryption, sample_events, read_or_error).is_empty());
        let store = || {
            let mut operation = ConsentStoreOperation::new(consent_delete(b"row".to_vec()));
            operation.start();
            operation
        };
        let refused = rejects_events(store, sample_events, |event| {
            matches!(
                event,
                Event::Storage(
                    StorageEvent::WriteResult { .. }
                        | StorageEvent::DeleteResult { .. }
                        | StorageEvent::Error { .. }
                )
            )
        });
        assert!(!refused.is_empty());
    }

    #[test]
    fn abort_and_finalize() {
        // Every check only reads, so no phase has cleanup.
        let realm = RealmId::from_bytes([3; 32]);
        let mut holder = HolderCheckOperation::new(realm, "sealed", user(1));
        holder.start();
        check_abort(&mut holder, &[]);
        rejects_finalize(holder);
        let mut consent = ConsentReadOperation::new(b"row".to_vec());
        check_abort(&mut consent, &[]);
        rejects_finalize(consent);
        let mut encryption = EncryptionCheckOperation::new("b", false);
        encryption.start();
        check_abort(&mut encryption, &[]);
        rejects_finalize(encryption);
        let mut store = ConsentStoreOperation::new(consent_delete(b"row".to_vec()));
        store.start();
        check_abort(&mut store, &[]);
        rejects_finalize(store);
    }
}
