use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{ONBOARDING_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::onboarding::{OnboardingSecretRecord, OnboardingSecretState};
use aruna_core::operation::Operation;
use aruna_core::structs::RealmConfigDocument;
use aruna_core::types::{Effects, TxnId, UserId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::create_onboarding_secret::{
    enrolled_devices, pending_devices, scan_secrets, secret_record_key,
};
use crate::onboarding_secret_state::{
    resolve_secret_state, secret_state_key, secret_state_write_entry,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReserveOnboardingSecretInput {
    pub enrollment_id: Ulid,
    pub secret_hash: String,
    pub node_id: String,
    pub now: u64,
    pub reservation_expires_at: u64,
    pub finalizing: bool,
}

#[derive(Debug, PartialEq)]
pub struct ReserveOnboardingSecretOperation {
    input: ReserveOnboardingSecretInput,
    state: ReserveOnboardingSecretState,
    output: Option<Result<OnboardingSecretRecord, ReserveOnboardingSecretError>>,
}

#[derive(Clone, Debug, PartialEq)]
enum ReserveOnboardingSecretState {
    Init,
    StartTransaction,
    ReadRecords {
        txn_id: TxnId,
    },
    ReadRealmConfig {
        txn_id: TxnId,
        record: OnboardingSecretRecord,
        owner: UserId,
    },
    CountPending {
        txn_id: TxnId,
        record: OnboardingSecretRecord,
        cap: u32,
        enrolled: Vec<String>,
        /// Secrets counted by the pages already read.
        pending: u32,
    },
    WriteReserved {
        txn_id: TxnId,
        record: OnboardingSecretRecord,
    },
    CommitTransaction {
        record: OnboardingSecretRecord,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReserveOnboardingSecretError {
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
        got: String,
    },
}

impl ReserveOnboardingSecretOperation {
    pub fn new(input: ReserveOnboardingSecretInput) -> Self {
        Self {
            input,
            state: ReserveOnboardingSecretState::Init,
            output: None,
        }
    }

    fn validate_record(
        &self,
        record_value: Option<Value>,
        state_value: Option<Value>,
    ) -> Result<(OnboardingSecretRecord, OnboardingSecretState), ReserveOnboardingSecretError> {
        let Some(value) = record_value else {
            return Err(ReserveOnboardingSecretError::NotFound);
        };
        let record: OnboardingSecretRecord =
            postcard::from_bytes(&value).map_err(ConversionError::from)?;
        if record.secret_hash != self.input.secret_hash {
            return Err(ReserveOnboardingSecretError::InvalidSecret);
        }
        let state = resolve_secret_state(&record, state_value.as_ref())?;
        Ok((record, state))
    }

    fn reserve_state(
        &self,
        mut record: OnboardingSecretRecord,
        state: OnboardingSecretState,
    ) -> Result<Option<OnboardingSecretRecord>, ReserveOnboardingSecretError> {
        match state {
            OnboardingSecretState::Available => {
                if record.expires_at < self.input.now {
                    return Err(ReserveOnboardingSecretError::Expired);
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
                    return Err(ReserveOnboardingSecretError::AlreadyClaimed);
                }
                if record.expires_at < self.input.now {
                    return Err(ReserveOnboardingSecretError::Expired);
                }
                record.claimed_node_id = Some(self.input.node_id.clone());
                Ok(Some(record))
            }
            OnboardingSecretState::Finalizing { node_id } if node_id == self.input.node_id => {
                record.claimed_node_id = Some(self.input.node_id.clone());
                Ok(None)
            }
            OnboardingSecretState::Finalizing { .. } => {
                Err(ReserveOnboardingSecretError::AlreadyClaimed)
            }
            OnboardingSecretState::Consumed { node_id } if node_id == self.input.node_id => {
                record.claimed_node_id = Some(self.input.node_id.clone());
                Ok(None)
            }
            OnboardingSecretState::Consumed { .. } => {
                Err(ReserveOnboardingSecretError::AlreadyClaimed)
            }
        }
    }

    fn emit_reserve_write(&mut self, txn_id: TxnId, record: OnboardingSecretRecord) -> Effects {
        let record_value = match postcard::to_allocvec(&record) {
            Ok(value) => value,
            Err(error) => {
                return fail(
                    self,
                    ReserveOnboardingSecretError::ConversionError(error.into()),
                );
            }
        };
        let state_entry = match secret_state_write_entry(
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
        ) {
            Ok(entry) => entry,
            Err(error) => return fail(self, ReserveOnboardingSecretError::ConversionError(error)),
        };
        self.state = ReserveOnboardingSecretState::WriteReserved { txn_id, record };
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (
                    ONBOARDING_KEYSPACE.to_string(),
                    secret_record_key(self.input.enrollment_id),
                    ByteView::from(record_value),
                ),
                state_entry,
            ],
            txn_id: Some(txn_id),
        })]
    }

    /// The redeeming node re-checks the owner's device cap: the mint that issued
    /// this secret only saw its own node's outstanding secrets, so a second
    /// management node may have minted under the same cap.
    fn emit_cap_check(
        &mut self,
        txn_id: TxnId,
        record: OnboardingSecretRecord,
        owner: UserId,
    ) -> Effects {
        self.state = ReserveOnboardingSecretState::ReadRealmConfig {
            txn_id,
            record,
            owner,
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(*owner.realm_id.as_bytes()),
            txn_id: Some(txn_id),
        })]
    }
}

impl Operation for ReserveOnboardingSecretOperation {
    type Output = OnboardingSecretRecord;
    type Error = ReserveOnboardingSecretError;

    fn start(&mut self) -> Effects {
        self.state = ReserveOnboardingSecretState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error }) => {
                return fail(self, ReserveOnboardingSecretError::StorageError(error));
            }
            other => other,
        };

        match self.state.clone() {
            ReserveOnboardingSecretState::StartTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return fail(
                        self,
                        ReserveOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "transaction started",
                            got,
                        },
                    );
                };

                self.state = ReserveOnboardingSecretState::ReadRecords { txn_id };
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: vec![
                        (
                            ONBOARDING_KEYSPACE.to_string(),
                            secret_record_key(self.input.enrollment_id),
                        ),
                        (
                            ONBOARDING_KEYSPACE.to_string(),
                            secret_state_key(self.input.enrollment_id),
                        ),
                    ],
                    txn_id: Some(txn_id),
                })]
            }
            ReserveOnboardingSecretState::ReadRecords { txn_id } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
                    return fail(
                        self,
                        ReserveOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "batch read result",
                            got,
                        },
                    );
                };
                let [(_, record_value), (_, state_value)] = values.as_slice() else {
                    return fail(
                        self,
                        ReserveOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "record and state batch read result",
                            got: format!("{values:?}"),
                        },
                    );
                };

                let (record, state) =
                    match self.validate_record(record_value.clone(), state_value.clone()) {
                        Ok(result) => result,
                        Err(error) => return fail(self, error),
                    };
                let Some(record) = (match self.reserve_state(record, state) {
                    Ok(record) => record,
                    Err(error) => return fail(self, error),
                }) else {
                    self.state = ReserveOnboardingSecretState::CommitTransaction {
                        record: record_value
                            .as_ref()
                            .and_then(|value| postcard::from_bytes(value).ok())
                            .expect("record was decoded earlier"),
                    };
                    return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
                };

                match record.mode.owner() {
                    Some(owner) => self.emit_cap_check(txn_id, record, owner),
                    None => self.emit_reserve_write(txn_id, record),
                }
            }
            ReserveOnboardingSecretState::ReadRealmConfig {
                txn_id,
                record,
                owner,
            } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return fail(
                        self,
                        ReserveOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "read result",
                            got,
                        },
                    );
                };
                let config = match value
                    .map(|bytes| RealmConfigDocument::from_bytes(&bytes))
                    .transpose()
                {
                    Ok(config) => config,
                    Err(error) => {
                        return fail(self, ReserveOnboardingSecretError::ConversionError(error));
                    }
                };
                let Some(cap) = config
                    .as_ref()
                    .and_then(|config| config.quota.max_devices_per_user)
                else {
                    return self.emit_reserve_write(txn_id, record);
                };
                // The joiner's own membership is the slot this redemption takes,
                // so a retry after it joined must not charge it twice.
                let enrolled = enrolled_devices(config.as_ref(), owner)
                    .into_iter()
                    .filter(|node_id| node_id != &self.input.node_id)
                    .collect::<Vec<_>>();
                if enrolled.len() as u32 >= cap {
                    return fail(
                        self,
                        ReserveOnboardingSecretError::DeviceCapExceeded { limit: cap },
                    );
                }
                self.state = ReserveOnboardingSecretState::CountPending {
                    txn_id,
                    record,
                    cap,
                    enrolled,
                    pending: 0,
                };
                smallvec![scan_secrets(txn_id, None)]
            }
            ReserveOnboardingSecretState::CountPending {
                txn_id,
                record,
                cap,
                enrolled,
                pending,
            } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) = event
                else {
                    return fail(
                        self,
                        ReserveOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "iter result",
                            got,
                        },
                    );
                };
                let pending = pending.saturating_add(pending_devices(
                    &values,
                    record.mode.owner(),
                    self.input.enrollment_id,
                    &enrolled,
                ));
                if enrolled.len() as u32 + pending >= cap {
                    return fail(
                        self,
                        ReserveOnboardingSecretError::DeviceCapExceeded { limit: cap },
                    );
                }
                // The range is followed to its end, so a slot this owner holds
                // cannot hide behind another owner's records.
                match next_start_after {
                    Some(next) => {
                        self.state = ReserveOnboardingSecretState::CountPending {
                            txn_id,
                            record,
                            cap,
                            enrolled,
                            pending,
                        };
                        smallvec![scan_secrets(txn_id, Some(next))]
                    }
                    None => self.emit_reserve_write(txn_id, record),
                }
            }
            ReserveOnboardingSecretState::WriteReserved { txn_id, record } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
                    return fail(
                        self,
                        ReserveOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "batch write result",
                            got,
                        },
                    );
                };

                self.state = ReserveOnboardingSecretState::CommitTransaction {
                    record: record.clone(),
                };
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            ReserveOnboardingSecretState::CommitTransaction { record } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return fail(
                        self,
                        ReserveOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "transaction committed",
                            got,
                        },
                    );
                };

                self.state = ReserveOnboardingSecretState::Finish;
                self.output = Some(Ok(record));
                smallvec![]
            }
            ReserveOnboardingSecretState::Init
            | ReserveOnboardingSecretState::Finish
            | ReserveOnboardingSecretState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ReserveOnboardingSecretState::Finish | ReserveOnboardingSecretState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or(ReserveOnboardingSecretError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            ReserveOnboardingSecretState::ReadRecords { txn_id }
            | ReserveOnboardingSecretState::ReadRealmConfig { txn_id, .. }
            | ReserveOnboardingSecretState::CountPending { txn_id, .. }
            | ReserveOnboardingSecretState::WriteReserved { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

fn fail(
    operation: &mut ReserveOnboardingSecretOperation,
    error: ReserveOnboardingSecretError,
) -> Effects {
    let cleanup = operation.abort();
    operation.state = ReserveOnboardingSecretState::Error;
    operation.output = Some(Err(error));
    cleanup
}

#[cfg(test)]
mod tests {
    use super::{
        ReserveOnboardingSecretError, ReserveOnboardingSecretInput,
        ReserveOnboardingSecretOperation,
    };
    use crate::create_onboarding_secret::{
        CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
    };
    use crate::driver::{DriverContext, drive};
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
    use aruna_core::onboarding::{OnboardingMode, OnboardingPurpose, OnboardingSecretRecord};
    use aruna_core::structs::{QuotaConfig, RealmConfigDocument, RealmId, RealmNodeKind};
    use aruna_core::types::UserId;
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
            task_handle: None,
            compute_handle: None,
        }
    }

    /// Replaces the realm config, which is how a device enrolled elsewhere
    /// reaches the redeeming node.
    async fn write_config(context: &DriverContext, cap: Option<u32>, devices: &[UserId]) {
        let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
        config.quota = QuotaConfig {
            max_devices_per_user: cap,
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
            CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
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
    ) -> Result<OnboardingSecretRecord, ReserveOnboardingSecretError> {
        drive(
            ReserveOnboardingSecretOperation::new(ReserveOnboardingSecretInput {
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
            Err(ReserveOnboardingSecretError::DeviceCapExceeded { limit: 1 })
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
    async fn reserves_secret_idempotently_for_same_node() {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let enrollment_id = Ulid::generate();
        drive(
            CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
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
            ReserveOnboardingSecretOperation::new(ReserveOnboardingSecretInput {
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
            ReserveOnboardingSecretOperation::new(ReserveOnboardingSecretInput {
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
            ReserveOnboardingSecretOperation::new(ReserveOnboardingSecretInput {
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
        assert_eq!(
            different_node,
            Err(ReserveOnboardingSecretError::AlreadyClaimed)
        );
    }

    #[tokio::test]
    async fn expired_reservation_can_be_reclaimed_before_secret_expiry() {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let enrollment_id = Ulid::generate();
        drive(
            CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
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
            ReserveOnboardingSecretOperation::new(ReserveOnboardingSecretInput {
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
            ReserveOnboardingSecretOperation::new(ReserveOnboardingSecretInput {
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
