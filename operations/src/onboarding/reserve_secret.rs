//! Reserves an onboarding secret for a node and enforces the realm cap on pending devices.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{ONBOARDING_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::onboarding::{OnboardingSecretRecord, OnboardingSecretState};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{PrefixPages, done, values};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::types::Value;
use byteview::ByteView;
use thiserror::Error;
use ulid::Ulid;

use crate::onboarding::create_secret::{
    enrolled_devices, pending_devices, secret_pages, secret_record_key,
};
use crate::onboarding::secret_state::{resolve_secret_state, secret_state_entry, secret_state_key};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReserveSecretInput {
    pub enrollment_id: Ulid,
    pub secret_hash: String,
    pub node_id: String,
    pub now: u64,
    pub reservation_expires_at: u64,
    pub finalizing: bool,
}

/// What the reservation transaction waits for.
#[derive(Clone, Debug, PartialEq)]
enum Stage {
    Records,
    Config {
        owner: UserId,
    },
    /// Counts the owner's outstanding secrets page by page against `cap`.
    Count {
        cap: u32,
        enrolled: Vec<String>,
        /// Secrets counted by the pages already read.
        pending: u32,
    },
    Write,
}

/// Reserves the secret for one node in one write transaction, re-checking the owner's device
/// cap for a device secret.
#[derive(Debug, PartialEq)]
pub struct ReserveSecretOperation {
    input: ReserveSecretInput,
    stage: Stage,
    record: Option<OnboardingSecretRecord>,
    pages: PrefixPages,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReserveSecretError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("onboarding secret not found")]
    NotFound,
    #[error("onboarding secret expired")]
    Expired,
    #[error("onboarding secret already claimed")]
    AlreadyClaimed,
    #[error("onboarding secret does not match")]
    InvalidSecret,
    #[error("user already holds the maximum of {limit} devices")]
    DeviceCapExceeded { limit: u32 },
    #[error("reserving onboarding secret did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for ReserveSecretError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "ReserveSecret".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

type ReserveTurn = Result<Next<OnboardingSecretRecord>, ReserveSecretError>;

impl ReserveSecretOperation {
    pub fn new(input: ReserveSecretInput) -> Self {
        Self {
            input,
            stage: Stage::Records,
            record: None,
            pages: secret_pages(),
        }
    }

    fn validate_record(
        &self,
        record_value: Option<Value>,
        state_value: Option<Value>,
    ) -> Result<(OnboardingSecretRecord, OnboardingSecretState), ReserveSecretError> {
        let Some(value) = record_value else {
            return Err(ReserveSecretError::NotFound);
        };
        let record: OnboardingSecretRecord =
            postcard::from_bytes(&value).map_err(ConversionError::from)?;
        if record.secret_hash != self.input.secret_hash {
            return Err(ReserveSecretError::InvalidSecret);
        }
        let state = resolve_secret_state(&record, state_value.as_ref())?;
        Ok((record, state))
    }

    fn reserve_state(
        &self,
        mut record: OnboardingSecretRecord,
        state: OnboardingSecretState,
    ) -> Result<Option<OnboardingSecretRecord>, ReserveSecretError> {
        match state {
            OnboardingSecretState::Available => {
                if record.expires_at < self.input.now {
                    return Err(ReserveSecretError::Expired);
                }
                record.claimed_node_id = Some(self.input.node_id.clone());
                Ok(Some(record))
            }
            OnboardingSecretState::Reserved { node_id, .. } if node_id == self.input.node_id => {
                record.claimed_node_id = Some(self.input.node_id.clone());
                Ok(Some(record))
            }
            OnboardingSecretState::Reserved { expires_at, .. } => {
                if expires_at >= self.input.now {
                    return Err(ReserveSecretError::AlreadyClaimed);
                }
                if record.expires_at < self.input.now {
                    return Err(ReserveSecretError::Expired);
                }
                record.claimed_node_id = Some(self.input.node_id.clone());
                Ok(Some(record))
            }
            OnboardingSecretState::Finalizing { node_id } if node_id == self.input.node_id => {
                record.claimed_node_id = Some(self.input.node_id.clone());
                Ok(None)
            }
            OnboardingSecretState::Finalizing { .. } => Err(ReserveSecretError::AlreadyClaimed),
            OnboardingSecretState::Consumed { node_id } if node_id == self.input.node_id => {
                record.claimed_node_id = Some(self.input.node_id.clone());
                Ok(None)
            }
            OnboardingSecretState::Consumed { .. } => Err(ReserveSecretError::AlreadyClaimed),
        }
    }

    fn reserve_write(&mut self) -> ReserveTurn {
        let record = self
            .record
            .as_ref()
            .ok_or(ReserveSecretError::NotFinished)?;
        let record_value = postcard::to_allocvec(record).map_err(ConversionError::from)?;
        let state_entry = secret_state_entry(
            self.input.enrollment_id,
            if self.input.finalizing {
                OnboardingSecretState::Finalizing {
                    node_id: self.input.node_id.clone(),
                }
            } else {
                OnboardingSecretState::Reserved {
                    node_id: self.input.node_id.clone(),
                    expires_at: self.input.reservation_expires_at,
                }
            },
        )?;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(vec![
            (
                ONBOARDING_KEYSPACE.to_string(),
                secret_record_key(self.input.enrollment_id),
                ByteView::from(record_value),
            ),
            state_entry,
        ])))
    }

    fn handle_records(
        &mut self,
        record_value: Option<Value>,
        state_value: Option<Value>,
    ) -> ReserveTurn {
        let (stored, state) = self.validate_record(record_value, state_value)?;
        // A repeat by the node that already finalized or consumed writes nothing.
        let Some(record) = self.reserve_state(stored.clone(), state)? else {
            return Ok(Next::Done(stored));
        };
        let owner = record.mode.owner();
        self.record = Some(record);
        let Some(owner) = owner else {
            return self.reserve_write();
        };
        // The redeeming node re-checks the owner's device cap: the mint that issued
        // this secret only saw its own node's outstanding secrets, so a second
        // management node may have minted under the same cap.
        self.stage = Stage::Config { owner };
        Ok(Next::Call(IoCall::Read(vec![(
            REALM_CONFIG_KEYSPACE.to_string(),
            ByteView::from(*owner.realm_id.as_bytes()),
        )])))
    }

    fn handle_config(&mut self, owner: UserId, value: Option<Value>) -> ReserveTurn {
        let config = value
            .map(|bytes| RealmConfigDocument::from_bytes(&bytes))
            .transpose()?;
        let Some(cap) = config
            .as_ref()
            .and_then(|config| config.quota.devices_per_user)
        else {
            return self.reserve_write();
        };
        // The joiner's own membership is the slot this redemption takes,
        // so a retry after it joined must not charge it twice.
        let enrolled = enrolled_devices(config.as_ref(), owner)
            .into_iter()
            .filter(|node_id| node_id != &self.input.node_id)
            .collect::<Vec<_>>();
        if enrolled.len() as u32 >= cap {
            return Err(ReserveSecretError::DeviceCapExceeded { limit: cap });
        }
        self.stage = Stage::Count {
            cap,
            enrolled,
            pending: 0,
        };
        Ok(Next::Call(self.pages.call()))
    }

    fn handle_page(&mut self, result: IoResult) -> ReserveTurn {
        let owner = self.record.as_ref().and_then(|record| record.mode.owner());
        let Stage::Count {
            cap,
            enrolled,
            pending,
        } = &mut self.stage
        else {
            return Err(ReserveSecretError::NotFinished);
        };
        let rows = self.pages.take(result)?;
        *pending = pending.saturating_add(pending_devices(
            &rows,
            owner,
            self.input.enrollment_id,
            enrolled,
        ));
        if enrolled.len() as u32 + *pending >= *cap {
            return Err(ReserveSecretError::DeviceCapExceeded { limit: *cap });
        }
        // The range is followed to its end, so a slot this owner holds
        // cannot hide behind another owner's records.
        match self.pages.done() {
            true => self.reserve_write(),
            false => Ok(Next::Call(self.pages.call())),
        }
    }
}

impl Operation for ReserveSecretOperation {
    type Output = OnboardingSecretRecord;
    type Error = ReserveSecretError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> ReserveTurn {
        Ok(Next::Call(IoCall::Read(vec![
            (
                ONBOARDING_KEYSPACE.to_string(),
                secret_record_key(self.input.enrollment_id),
            ),
            (
                ONBOARDING_KEYSPACE.to_string(),
                secret_state_key(self.input.enrollment_id),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> ReserveTurn {
        match self.stage.clone() {
            Stage::Records => {
                let [record, state] = values::<2>(result)?;
                self.handle_records(record, state)
            }
            Stage::Config { owner } => {
                let [value] = values::<1>(result)?;
                self.handle_config(owner, value)
            }
            Stage::Count { .. } => self.handle_page(result),
            Stage::Write => {
                done(result)?;
                self.record
                    .take()
                    .map(Next::Done)
                    .ok_or(ReserveSecretError::NotFinished)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ReserveSecretError, ReserveSecretInput, ReserveSecretOperation};
    use crate::driver::{DriverContext, drive};
    use crate::onboarding::create_secret::{CreateSecretInput, CreateSecretOperation};
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
    use aruna_core::onboarding::{OnboardingMode, OnboardingPurpose, OnboardingSecretRecord};
    use aruna_core::structs::identity::realm::{
        QuotaConfig, RealmConfigDocument, RealmId, RealmNodeKind,
    };
    use aruna_storage::storage;
    use byteview::ByteView;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn realm() -> RealmId {
        RealmId::from_bytes([7u8; 32])
    }

    fn device_node(index: usize) -> String {
        iroh::SecretKey::from_bytes(&[index as u8 + 1; 32])
            .public()
            .to_string()
    }

    fn open_context(tempdir: &tempfile::TempDir) -> DriverContext {
        DriverContext {
            storage_handle: storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        }
    }

    /// Replaces the realm config, which is how a device enrolled elsewhere
    /// reaches the redeeming node.
    async fn write_config(context: &DriverContext, cap: Option<u32>, devices: &[UserId]) {
        let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
        config.quota = QuotaConfig {
            devices_per_user: cap,
            ..QuotaConfig::default()
        };
        for (index, owner) in devices.iter().enumerate() {
            config.ensure_node(
                iroh::SecretKey::from_bytes(&[index as u8 + 1; 32]).public(),
                RealmNodeKind::User { owner: *owner },
            );
        }
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: ByteView::from(*realm().as_bytes()),
                value: ByteView::from(postcard::to_allocvec(&config).unwrap()),
                txn_id: None,
            })
            .await;
    }

    async fn mint_device(context: &DriverContext, owner: UserId, enrollment_id: Ulid) {
        drive(
            CreateSecretOperation::new(CreateSecretInput {
                record: OnboardingSecretRecord {
                    enrollment_id,
                    secret_hash: "abc".to_string(),
                    mode: OnboardingMode::User { owner },
                    purpose: OnboardingPurpose::NodeEnrollment,
                    expires_at: 100,
                    claimed_node_id: None,
                },
            }),
            context,
        )
        .await
        .expect("device secret mints");
    }

    async fn reserve(
        context: &DriverContext,
        enrollment_id: Ulid,
        node_id: &str,
    ) -> Result<OnboardingSecretRecord, ReserveSecretError> {
        drive(
            ReserveSecretOperation::new(ReserveSecretInput {
                enrollment_id,
                secret_hash: "abc".to_string(),
                node_id: node_id.to_string(),
                now: 10,
                reservation_expires_at: 20,
                finalizing: false,
            }),
            context,
        )
        .await
    }

    #[tokio::test]
    async fn redemption_rechecks_cap() {
        // The other management node's device landed in the config after this
        // secret was minted, so the redemption must refuse it.
        let tempdir = tempdir().unwrap();
        let context = open_context(&tempdir);
        let owner = UserId::local(Ulid::generate(), realm());
        let enrollment_id = Ulid::generate();
        write_config(&context, Some(1), &[]).await;
        mint_device(&context, owner, enrollment_id).await;
        write_config(&context, Some(1), &[owner]).await;

        assert_eq!(
            reserve(&context, enrollment_id, &device_node(4)).await,
            Err(ReserveSecretError::DeviceCapExceeded { limit: 1 })
        );
    }

    #[tokio::test]
    async fn retry_keeps_slot() {
        // Once the joiner itself is in the config, its own retry must not be
        // charged a second device.
        let tempdir = tempdir().unwrap();
        let context = open_context(&tempdir);
        let owner = UserId::local(Ulid::generate(), realm());
        let enrollment_id = Ulid::generate();
        write_config(&context, Some(1), &[]).await;
        mint_device(&context, owner, enrollment_id).await;

        let node_id = device_node(0);
        reserve(&context, enrollment_id, &node_id)
            .await
            .expect("first redemption reserves");
        write_config(&context, Some(1), &[owner]).await;
        reserve(&context, enrollment_id, &node_id)
            .await
            .expect("the joiner's own membership is its own slot");
    }

    #[tokio::test]
    async fn reserves_same_node() {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let enrollment_id = Ulid::generate();
        drive(
            CreateSecretOperation::new(CreateSecretInput {
                record: OnboardingSecretRecord {
                    enrollment_id,
                    secret_hash: "abc".to_string(),
                    mode: OnboardingMode::Server,
                    purpose: OnboardingPurpose::NodeEnrollment,
                    expires_at: 100,
                    claimed_node_id: None,
                },
            }),
            &context,
        )
        .await
        .unwrap();

        let first = drive(
            ReserveSecretOperation::new(ReserveSecretInput {
                enrollment_id,
                secret_hash: "abc".to_string(),
                node_id: "node-a".to_string(),
                now: 10,
                reservation_expires_at: 20,
                finalizing: false,
            }),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(first.claimed_node_id.as_deref(), Some("node-a"));

        let second = drive(
            ReserveSecretOperation::new(ReserveSecretInput {
                enrollment_id,
                secret_hash: "abc".to_string(),
                node_id: "node-a".to_string(),
                now: 15,
                reservation_expires_at: 25,
                finalizing: false,
            }),
            &context,
        )
        .await;
        assert!(second.is_ok());

        let different_node = drive(
            ReserveSecretOperation::new(ReserveSecretInput {
                enrollment_id,
                secret_hash: "abc".to_string(),
                node_id: "node-b".to_string(),
                now: 15,
                reservation_expires_at: 25,
                finalizing: false,
            }),
            &context,
        )
        .await;
        assert_eq!(different_node, Err(ReserveSecretError::AlreadyClaimed));
    }

    #[tokio::test]
    async fn expired_reservation_reclaimed() {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let enrollment_id = Ulid::generate();
        drive(
            CreateSecretOperation::new(CreateSecretInput {
                record: OnboardingSecretRecord {
                    enrollment_id,
                    secret_hash: "abc".to_string(),
                    mode: OnboardingMode::Server,
                    purpose: OnboardingPurpose::NodeEnrollment,
                    expires_at: 100,
                    claimed_node_id: None,
                },
            }),
            &context,
        )
        .await
        .unwrap();

        drive(
            ReserveSecretOperation::new(ReserveSecretInput {
                enrollment_id,
                secret_hash: "abc".to_string(),
                node_id: "node-a".to_string(),
                now: 10,
                reservation_expires_at: 20,
                finalizing: false,
            }),
            &context,
        )
        .await
        .unwrap();

        let reclaimed = drive(
            ReserveSecretOperation::new(ReserveSecretInput {
                enrollment_id,
                secret_hash: "abc".to_string(),
                node_id: "node-b".to_string(),
                now: 21,
                reservation_expires_at: 30,
                finalizing: false,
            }),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(reclaimed.claimed_node_id.as_deref(), Some("node-b"));
    }
}
