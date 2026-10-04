//! Installs a checked bucket key in this node's unlock registry: prepare, then activate. A key
//! that cannot be activated is discarded, so none stays prepared.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::SharedSecret;
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::encryption::{BucketKeyRef, KeyTicket, UnlockStatus};
use aruna_core::types::Effects;
use smallvec::smallvec;
use std::time::Duration;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InstallState {
    Init,
    PrepareKey,
    ActivateKey,
    DiscardKey,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum InstallError {
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
    #[error("installing the key did not finish")]
    NotFinished,
}

/// The key and its unlock bounds: without a duration it lasts until `max`, or until lock or
/// restart when there is no maximum.
#[derive(Debug, PartialEq)]
pub struct InstallInput {
    pub key: BucketKeyRef,
    pub public_key: [u8; 32],
    pub private_key: SharedSecret,
    pub duration: Option<Duration>,
    pub max: Option<Duration>,
}

#[derive(Debug, PartialEq)]
pub struct InstallKeyOperation {
    key: BucketKeyRef,
    input: Option<InstallInput>,
    state: InstallState,
    ticket: Option<KeyTicket>,
    discarded: Option<KeyTicket>,
    failure: Option<BlobError>,
    output: Option<Result<UnlockStatus, InstallError>>,
}

impl InstallKeyOperation {
    pub fn new(input: InstallInput) -> Self {
        Self {
            key: input.key,
            input: Some(input),
            state: InstallState::Init,
            ticket: None,
            discarded: None,
            failure: None,
            output: None,
        }
    }

    /// A failure discards the prepared key this operation still owns.
    fn fail(&mut self, error: impl Into<InstallError>) -> Effects {
        self.output = Some(Err(error.into()));
        self.state = InstallState::Error;
        self.abort()
    }

    fn discard(&mut self, error: BlobError) -> Effects {
        let Some(ticket) = self.ticket.take() else {
            return self.fail(error);
        };
        self.failure = Some(error);
        self.state = InstallState::DiscardKey;
        self.discarded = Some(ticket);
        smallvec![Effect::Blob(BlobEffect::DiscardKey { ticket })]
    }
}

impl Operation for InstallKeyOperation {
    type Output = UnlockStatus;
    type Error = InstallError;

    /// The operation hands its only key handle to the adapter and keeps none.
    fn start(&mut self) -> Effects {
        let Some(input) = self.input.take() else {
            return self.fail(InstallError::NotFinished);
        };
        self.state = InstallState::PrepareKey;
        smallvec![Effect::Blob(BlobEffect::PrepareKey {
            key: input.key,
            public_key: input.public_key,
            private_key: input.private_key,
            duration: input.duration,
            max: input.max,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let ticket = self.ticket;
        let discarded = self.discarded;
        match (self.state, event) {
            (InstallState::PrepareKey, Event::Blob(BlobEvent::KeyPrepared { ticket }))
                if ticket.key == self.key =>
            {
                self.ticket = Some(ticket);
                self.state = InstallState::ActivateKey;
                smallvec![Effect::Blob(BlobEffect::ActivateKey { ticket })]
            }
            (InstallState::ActivateKey, Event::Blob(BlobEvent::KeyActivated { status }))
                if ticket.is_some_and(|ticket| {
                    (ticket.key, ticket.session_id) == (status.key, status.session_id)
                }) =>
            {
                self.ticket = None;
                self.state = InstallState::Finish;
                self.output = Some(Ok(status));
                smallvec![]
            }
            (InstallState::ActivateKey, Event::Blob(BlobEvent::Error(error))) => {
                self.discard(error)
            }
            (InstallState::DiscardKey, Event::Blob(BlobEvent::KeyDiscarded { ticket }))
                if discarded == Some(ticket) =>
            {
                let error = self.failure.take().unwrap_or(BlobError::InvalidEffect);
                self.fail(error)
            }
            (InstallState::DiscardKey, Event::Blob(BlobEvent::Error(_))) => {
                let error = self.failure.take().unwrap_or(BlobError::InvalidEffect);
                self.fail(error)
            }
            (_, Event::Blob(BlobEvent::Error(error))) => self.fail(error),
            (state, received) => self.fail(InstallError::InvalidStateEvent {
                state: format!("{state:?}"),
                expected: "the event of the current step",
                received,
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, InstallState::Finish | InstallState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(InstallError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        self.input = None;
        match self.ticket.take() {
            Some(ticket) => smallvec![Effect::Blob(BlobEffect::DiscardKey { ticket })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::compute::SecretBytes;
    use aruna_core::structs::storage::encryption::{BucketKeyError, public_key_of};
    use std::time::SystemTime;
    use ulid::Ulid;

    fn prepared() -> (InstallKeyOperation, KeyTicket) {
        let private = SecretBytes::new(vec![4; 32]);
        let mut operation = InstallKeyOperation::new(InstallInput {
            key: BucketKeyRef::new(Ulid::from_bytes([1; 16]), 1),
            public_key: public_key_of(&private).unwrap(),
            private_key: SharedSecret::new(private),
            duration: None,
            max: Some(Duration::from_secs(60)),
        });
        let effects = operation.start();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::PrepareKey { .. })]
        ));
        assert!(operation.input.is_none(), "the key handle is handed on");
        let ticket = KeyTicket {
            key: operation.key,
            session_id: Ulid::from_bytes([2; 16]),
        };
        let effects = operation.step(Event::Blob(BlobEvent::KeyPrepared { ticket }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ActivateKey { ticket })]
        );
        (operation, ticket)
    }

    fn status(ticket: KeyTicket) -> UnlockStatus {
        UnlockStatus {
            key: ticket.key,
            session_id: ticket.session_id,
            active: true,
            unlocked_at: SystemTime::UNIX_EPOCH,
            remaining: Some(Duration::from_secs(60)),
            max_remaining: Some(Duration::from_secs(60)),
        }
    }

    #[test]
    fn installs_prepared_key() {
        let (mut operation, ticket) = prepared();
        operation.step(Event::Blob(BlobEvent::KeyActivated {
            status: status(ticket),
        }));
        assert_eq!(operation.finalize(), Ok(status(ticket)));

        // A status of another session is no answer to this activation.
        let (mut operation, mut ticket) = prepared();
        ticket.session_id = Ulid::from_bytes([3; 16]);
        operation.step(Event::Blob(BlobEvent::KeyActivated {
            status: status(ticket),
        }));
        assert!(matches!(
            operation.finalize(),
            Err(InstallError::InvalidStateEvent { .. })
        ));
    }

    #[test]
    fn failed_activation_discards() {
        let refused = || BlobError::BucketKey(BucketKeyError::Capacity);
        let (mut operation, ticket) = prepared();
        let effects = operation.step(Event::Blob(BlobEvent::Error(refused())));
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::DiscardKey { ticket })]
        );
        operation.step(Event::Blob(BlobEvent::KeyDiscarded { ticket }));
        assert_eq!(operation.finalize(), Err(InstallError::Blob(refused())));

        // An unexpected answer discards the prepared key this operation owns.
        let (mut stray, ticket) = prepared();
        let effects = stray.step(Event::Blob(BlobEvent::KeyDiscarded { ticket }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::DiscardKey { ticket })]
        );
        assert!(matches!(
            stray.finalize(),
            Err(InstallError::InvalidStateEvent { .. })
        ));

        // A run stopped before activation discards the prepared key too.
        let (mut stopped, ticket) = prepared();
        assert_eq!(
            stopped.abort().as_slice(),
            [Effect::Blob(BlobEffect::DiscardKey { ticket })]
        );
    }

    #[tokio::test]
    async fn enable_then_install() {
        use crate::driver::{DriverContext, drive};
        use crate::s3::bucket::create::CreateBucketOperation;
        use crate::s3::bucket::encryption::{EnableEncryptionOperation, EnableInput};
        use aruna_blob::blob::BlobHandler;
        use aruna_core::UserId;
        use aruna_core::effects::StorageEffect;
        use aruna_core::events::StorageEvent;
        use aruna_core::keyspaces::KEY_COPY_KEYSPACE;
        use aruna_core::node_vault::{NodeVaultKey, VaultEntry, VaultPurpose};
        use aruna_core::structs::identity::realm::RealmId;
        use aruna_core::structs::storage::blob::{Backend, BackendConfig, BucketInfo};
        use aruna_core::structs::storage::encryption::{BlockCipher, BlockKeys, EncryptionMode};
        use aruna_core::structs::storage::format::Compression;
        use aruna_core::structs::storage::holders::KeyLookup;
        use std::collections::{BTreeMap, HashMap};

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let storage = aruna_storage::FjallStorage::open(root).unwrap();
        storage.open_vault(NodeVaultKey::random());
        let net = aruna_net::NetHandle::new(aruna_net::NetConfig::default(), storage.clone())
            .await
            .unwrap();
        let config = BackendConfig {
            backend_type: Backend::FileSystem,
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100_000),
            multipart_bucket: Some("multipart".to_string()),
            root: format!("{root}/blobs"),
            service_config: HashMap::new(),
            timeouts: Default::default(),
        };
        let blob = BlobHandler::new(config, storage.clone(), net.clone())
            .await
            .unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net.clone()),
            blob_handle: Some(blob.clone()),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let realm_id = RealmId::from_bytes([1; 32]);
        let creator = UserId::new(Ulid::from_bytes([5; 16]), realm_id);
        let group_id = Ulid::from_bytes([3; 16]);
        let info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: creator,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
        };
        // The authorization documents the enable reads its admins from.
        let rows = crate::s3::bucket::key_rows::authority_rows(&info, None, &[]);
        let documents = [realm_id.as_bytes().to_vec(), group_id.to_bytes().to_vec()];
        for (key, (_, value)) in documents.into_iter().zip(rows.into_iter().skip(2)) {
            let key_space = aruna_core::keyspaces::AUTH_KEYSPACE.to_string();
            let value = value.unwrap();
            let write = StorageEffect::Write {
                key_space,
                key: key.into(),
                value,
                txn_id: None,
            };
            storage.send_storage_effect(write).await;
        }
        drive(
            CreateBucketOperation::new("sealed".to_string(), info),
            &context,
        )
        .await
        .unwrap();
        let holder = SecretBytes::new(vec![6; 32]);
        let record = aruna_core::structs::identity::user::vault::UserKeyRecord {
            user_id: creator,
            record_id: Ulid::from_bytes([7; 16]),
            key_id: "slot".to_string(),
            public_key: public_key_of(&holder).unwrap(),
            fingerprint: aruna_core::vault_format::key_fingerprint(
                &public_key_of(&holder).unwrap(),
            ),
            has_recovery: true,
            node_id: net.node_id(),
            placement: aruna_core::structs::placement::record::PlacementRef::NIL,
            created_at_ms: 1,
        };
        let enabled = drive(
            EnableEncryptionOperation::new(EnableInput {
                bucket: "sealed".to_string(),
                group_id,
                realm_id,
                node_id: net.node_id(),
                mode: EncryptionMode::NodeManaged,
                cipher: BlockCipher::ChaCha20Poly1305,
                block_keys: BlockKeys::ContentDerived,
                max_unlock_ms: None,
                expected_generation: 0,
                lookups: BTreeMap::from([(creator, KeyLookup::Keys(vec![record]))]),
                now_ms: 1,
            }),
            &context,
        )
        .await
        .unwrap();
        let key = enabled.key.clone();
        let install = InstallKeyOperation::new(InstallInput {
            key: key.key,
            public_key: key.public_key,
            private_key: enabled.private_key,
            duration: None,
            max: None,
        });
        let status = drive(install, &context).await.unwrap();
        assert!(status.active && status.remaining.is_none());

        // The node copy opens to the installed key; the creator's copy is stored.
        let entry = VaultEntry::new(VaultPurpose::BucketKey, key.vault_entry.unwrap());
        let read = StorageEffect::VaultRead {
            entry,
            txn_id: None,
        };
        let Event::Storage(StorageEvent::VaultResult {
            secret: Some(secret),
            ..
        }) = storage.send_storage_effect(read).await
        else {
            panic!("no node copy");
        };
        assert_eq!(public_key_of(&secret), Some(key.public_key));
        let copy = enabled.holders.holder(creator).unwrap();
        assert_eq!(
            copy.state,
            aruna_core::structs::storage::holders::HolderState::Ready
        );
        let rows = StorageEffect::Iter {
            key_space: KEY_COPY_KEYSPACE.to_string(),
            prefix: Some(key.key.key().into()),
            start: None,
            limit: 8,
            txn_id: None,
        };
        let Event::Storage(StorageEvent::IterResult { values, .. }) =
            storage.send_storage_effect(rows).await
        else {
            panic!("no copies");
        };
        assert_eq!(values.len(), 1);
    }
}
