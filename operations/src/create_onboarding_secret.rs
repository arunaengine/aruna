use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{ONBOARDING_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::onboarding::{OnboardingSecretRecord, OnboardingSecretState};
use aruna_core::operation::Operation;
use aruna_core::structs::RealmConfigDocument;
use aruna_core::types::{Effects, Key, TxnId, UserId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::onboarding_secret_state::secret_state_write_entry;

pub(crate) const SECRET_RECORD_PREFIX: &str = "secret:";
/// Caps the outstanding-secret scan: expired secrets are pruned on every mint,
/// so a realm never carries anywhere near this many live enrollments.
pub(crate) const MAX_SCANNED_SECRETS: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateOnboardingSecretInput {
    pub record: OnboardingSecretRecord,
}

#[derive(Debug, PartialEq)]
pub struct CreateOnboardingSecretOperation {
    input: CreateOnboardingSecretInput,
    state: CreateOnboardingSecretState,
    output: Option<Result<OnboardingSecretRecord, CreateOnboardingSecretError>>,
}

#[derive(Clone, Debug, PartialEq)]
enum CreateOnboardingSecretState {
    Init,
    StartTransaction,
    ReadRealmConfig {
        txn_id: TxnId,
    },
    CountPending {
        txn_id: TxnId,
        cap: u32,
        enrolled: Vec<String>,
    },
    WriteRecord {
        txn_id: TxnId,
    },
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateOnboardingSecretError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("creating onboarding secret did not finish")]
    NotFinished,
    #[error("user already holds the maximum of {limit} devices")]
    DeviceCapExceeded { limit: u32 },
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl CreateOnboardingSecretOperation {
    pub fn new(input: CreateOnboardingSecretInput) -> Self {
        Self {
            input,
            state: CreateOnboardingSecretState::Init,
            output: None,
        }
    }

    fn emit_write_record(&mut self, txn_id: TxnId) -> Effects {
        self.state = CreateOnboardingSecretState::WriteRecord { txn_id };
        let key = secret_record_key(self.input.record.enrollment_id);
        let value = match postcard::to_allocvec(&self.input.record) {
            Ok(value) => value,
            Err(error) => {
                return fail(
                    self,
                    CreateOnboardingSecretError::ConversionError(error.into()),
                );
            }
        };
        let state_entry = match secret_state_write_entry(
            self.input.record.enrollment_id,
            OnboardingSecretState::Available,
        ) {
            Ok(entry) => entry,
            Err(error) => return fail(self, CreateOnboardingSecretError::ConversionError(error)),
        };
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (ONBOARDING_KEYSPACE.to_string(), key, ByteView::from(value)),
                state_entry
            ],
            txn_id: Some(txn_id),
        })]
    }
}

/// Node ids already enrolled for `owner`, read from the signed realm config: a
/// device's owner is part of its membership kind, never a label.
pub(crate) fn enrolled_devices(config: Option<&RealmConfigDocument>, owner: UserId) -> Vec<String> {
    config
        .map(|config| {
            config
                .nodes
                .iter()
                .filter(|node| node.kind.owner() == Some(owner))
                .map(|node| node.node_id.clone())
                .collect()
        })
        .unwrap_or_default()
}

/// Secrets that still occupy a device slot for `owner`, skipping `skip` and any
/// secret whose claiming node already joined: those are counted through the
/// realm config instead of a second time here.
pub(crate) fn pending_devices(
    values: &[(Key, Value)],
    owner: Option<UserId>,
    skip: Ulid,
    enrolled: &[String],
) -> u32 {
    values
        .iter()
        .filter_map(|(_, bytes)| postcard::from_bytes::<OnboardingSecretRecord>(bytes).ok())
        .filter(|record| record.enrollment_id != skip && record.mode.owner() == owner)
        .filter(|record| {
            !record
                .claimed_node_id
                .as_ref()
                .is_some_and(|node_id| enrolled.iter().any(|enrolled| enrolled == node_id))
        })
        .count() as u32
}

impl Operation for CreateOnboardingSecretOperation {
    type Output = OnboardingSecretRecord;
    type Error = CreateOnboardingSecretError;

    fn start(&mut self) -> Effects {
        self.state = CreateOnboardingSecretState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error }) => {
                return fail(self, CreateOnboardingSecretError::StorageError(error));
            }
            other => other,
        };

        match self.state.clone() {
            CreateOnboardingSecretState::StartTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return fail(
                        self,
                        CreateOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "transaction started",
                            got,
                        },
                    );
                };

                // Only a device secret is capped, and the owner it is bound to
                // names the realm whose quota decides the cap.
                let Some(owner) = self.input.record.mode.owner() else {
                    return self.emit_write_record(txn_id);
                };
                self.state = CreateOnboardingSecretState::ReadRealmConfig { txn_id };
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: REALM_CONFIG_KEYSPACE.to_string(),
                    key: ByteView::from(*owner.realm_id.as_bytes()),
                    txn_id: Some(txn_id),
                })]
            }
            CreateOnboardingSecretState::ReadRealmConfig { txn_id } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return fail(
                        self,
                        CreateOnboardingSecretError::UnexpectedEvent {
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
                        return fail(self, CreateOnboardingSecretError::ConversionError(error));
                    }
                };
                let cap = config
                    .as_ref()
                    .and_then(|config| config.quota.max_devices_per_user);
                let (Some(cap), Some(owner)) = (cap, self.input.record.mode.owner()) else {
                    return self.emit_write_record(txn_id);
                };
                let enrolled = enrolled_devices(config.as_ref(), owner);
                if enrolled.len() as u32 >= cap {
                    return fail(
                        self,
                        CreateOnboardingSecretError::DeviceCapExceeded { limit: cap },
                    );
                }
                // Unclaimed secrets already occupy a slot: two mints followed by
                // two enrollments would otherwise cross the cap.
                self.state = CreateOnboardingSecretState::CountPending {
                    txn_id,
                    cap,
                    enrolled,
                };
                smallvec![Effect::Storage(StorageEffect::Iter {
                    key_space: ONBOARDING_KEYSPACE.to_string(),
                    prefix: Some(ByteView::from(SECRET_RECORD_PREFIX.as_bytes().to_vec())),
                    start: None,
                    limit: MAX_SCANNED_SECRETS,
                    txn_id: Some(txn_id),
                })]
            }
            CreateOnboardingSecretState::CountPending {
                txn_id,
                cap,
                enrolled,
            } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
                    return fail(
                        self,
                        CreateOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "iter result",
                            got,
                        },
                    );
                };
                let pending = pending_devices(
                    &values,
                    self.input.record.mode.owner(),
                    self.input.record.enrollment_id,
                    &enrolled,
                );
                if enrolled.len() as u32 + pending >= cap {
                    return fail(
                        self,
                        CreateOnboardingSecretError::DeviceCapExceeded { limit: cap },
                    );
                }
                self.emit_write_record(txn_id)
            }
            CreateOnboardingSecretState::WriteRecord { txn_id } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
                    return fail(
                        self,
                        CreateOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "batch write result",
                            got,
                        },
                    );
                };
                self.state = CreateOnboardingSecretState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            CreateOnboardingSecretState::CommitTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return fail(
                        self,
                        CreateOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "transaction committed",
                            got,
                        },
                    );
                };
                self.state = CreateOnboardingSecretState::Finish;
                self.output = Some(Ok(self.input.record.clone()));
                smallvec![]
            }
            CreateOnboardingSecretState::Init
            | CreateOnboardingSecretState::Finish
            | CreateOnboardingSecretState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateOnboardingSecretState::Finish | CreateOnboardingSecretState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or(CreateOnboardingSecretError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            CreateOnboardingSecretState::ReadRealmConfig { txn_id }
            | CreateOnboardingSecretState::CountPending { txn_id, .. }
            | CreateOnboardingSecretState::WriteRecord { txn_id } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

fn fail(
    operation: &mut CreateOnboardingSecretOperation,
    error: CreateOnboardingSecretError,
) -> Effects {
    let cleanup = operation.abort();
    operation.state = CreateOnboardingSecretState::Error;
    operation.output = Some(Err(error));
    cleanup
}

pub fn secret_record_key(enrollment_id: Ulid) -> ByteView {
    ByteView::from(format!("{SECRET_RECORD_PREFIX}{enrollment_id}").into_bytes())
}

#[cfg(test)]
mod tests {
    use super::{
        CreateOnboardingSecretError, CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
    };
    use crate::driver::{DriverContext, drive};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::errors::StorageError;
    use aruna_core::keyspaces::{ONBOARDING_KEYSPACE, REALM_CONFIG_KEYSPACE};
    use aruna_core::onboarding::{OnboardingMode, OnboardingPurpose, OnboardingSecretRecord};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{QuotaConfig, RealmConfigDocument, RealmId, RealmNodeKind};
    use aruna_core::types::UserId;
    use aruna_storage::storage;
    use byteview::ByteView;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn realm() -> RealmId {
        RealmId::from_bytes([4u8; 32])
    }

    fn device_node(index: usize) -> iroh::PublicKey {
        iroh::SecretKey::from_bytes(&[index as u8 + 1; 32]).public()
    }

    fn device_record(owner: UserId) -> OnboardingSecretRecord {
        OnboardingSecretRecord {
            enrollment_id: Ulid::generate(),
            secret_hash: Ulid::generate().to_string(),
            mode: OnboardingMode::User { owner },
            purpose: OnboardingPurpose::NodeEnrollment,
            expires_at: u64::MAX,
            claimed_node_id: None,
        }
    }

    async fn context_with_cap(
        cap: Option<u32>,
        enrolled: &[UserId],
    ) -> (tempfile::TempDir, DriverContext) {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
        config.quota = QuotaConfig {
            max_devices_per_user: cap,
            ..QuotaConfig::default()
        };
        for (index, owner) in enrolled.iter().enumerate() {
            config.ensure_node(device_node(index), RealmNodeKind::User { owner: *owner });
        }
        storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: ByteView::from(*realm().as_bytes()),
                value: ByteView::from(postcard::to_allocvec(&config).unwrap()),
                txn_id: None,
            })
            .await;
        (
            tempdir,
            DriverContext {
                storage_handle,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
                compute_handle: None,
            },
        )
    }

    /// Runs a mint's effects against storage up to its commit and hands that
    /// commit back, so two mints can be interleaved deliberately.
    async fn run_to_commit(
        operation: &mut CreateOnboardingSecretOperation,
        context: &DriverContext,
    ) -> StorageEffect {
        let mut queue: std::collections::VecDeque<Effect> = operation.start().into_iter().collect();
        while let Some(Effect::Storage(effect)) = queue.pop_front() {
            if matches!(effect, StorageEffect::CommitTransaction { .. }) {
                return effect;
            }
            let event = context.storage_handle.send_storage_effect(effect).await;
            queue.extend(operation.step(event));
        }
        panic!("the mint never reached its commit");
    }

    async fn write_claimed(context: &DriverContext, owner: UserId, node_id: &str) {
        let record = OnboardingSecretRecord {
            claimed_node_id: Some(node_id.to_string()),
            ..device_record(owner)
        };
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: ONBOARDING_KEYSPACE.to_string(),
                key: super::secret_record_key(record.enrollment_id),
                value: ByteView::from(postcard::to_allocvec(&record).unwrap()),
                txn_id: None,
            })
            .await;
    }

    async fn mint(
        context: &DriverContext,
        owner: UserId,
    ) -> Result<(), CreateOnboardingSecretError> {
        drive(
            CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
                record: device_record(owner),
            }),
            context,
        )
        .await
        .map(|_| ())
    }

    #[tokio::test]
    async fn cap_counts_enrolled() {
        let owner = UserId::local(Ulid::generate(), realm());
        let (_dir, context) = context_with_cap(Some(1), &[owner]).await;

        assert!(matches!(
            mint(&context, owner).await,
            Err(CreateOnboardingSecretError::DeviceCapExceeded { limit: 1 })
        ));
    }

    #[tokio::test]
    async fn cap_counts_pending() {
        // An unclaimed secret already occupies a device slot.
        let owner = UserId::local(Ulid::generate(), realm());
        let (_dir, context) = context_with_cap(Some(1), &[]).await;

        mint(&context, owner).await.expect("first device mints");
        assert!(matches!(
            mint(&context, owner).await,
            Err(CreateOnboardingSecretError::DeviceCapExceeded { limit: 1 })
        ));
    }

    #[tokio::test]
    async fn cap_per_owner() {
        // Another owner's enrolled device does not charge this owner's cap.
        let owner = UserId::local(Ulid::generate(), realm());
        let other = UserId::local(Ulid::generate(), realm());
        let (_dir, context) = context_with_cap(Some(1), &[other]).await;

        mint(&context, owner)
            .await
            .expect("another owner's device does not count");
    }

    #[tokio::test]
    async fn mints_conflict_locally() {
        // Two mints on one node under cap 1: the loser's pending-secret range
        // read conflicts with the winner's insert, so only one commits.
        let owner = UserId::local(Ulid::generate(), realm());
        let (_dir, context) = context_with_cap(Some(1), &[]).await;
        let mut first = CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
            record: device_record(owner),
        });
        let mut second = CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
            record: device_record(owner),
        });

        let first_commit = run_to_commit(&mut first, &context).await;
        let second_commit = run_to_commit(&mut second, &context).await;

        let event = context
            .storage_handle
            .send_storage_effect(first_commit)
            .await;
        first.step(event);
        first.finalize().expect("the first mint commits");

        let event = context
            .storage_handle
            .send_storage_effect(second_commit)
            .await;
        second.step(event);
        assert!(matches!(
            second.finalize(),
            Err(CreateOnboardingSecretError::StorageError(
                StorageError::TransactionConflict
            ))
        ));
    }

    #[tokio::test]
    async fn claimed_member_uncounted() {
        // The device this secret was claimed by already joined, so the realm
        // config alone charges its slot.
        let owner = UserId::local(Ulid::generate(), realm());
        let (_dir, context) = context_with_cap(Some(2), &[owner]).await;
        write_claimed(&context, owner, &device_node(0).to_string()).await;

        mint(&context, owner).await.expect("one slot is still free");
    }

    #[tokio::test]
    async fn claimed_stranger_counts() {
        // A claim by a node that never joined still occupies its slot.
        let owner = UserId::local(Ulid::generate(), realm());
        let (_dir, context) = context_with_cap(Some(2), &[owner]).await;
        write_claimed(&context, owner, &device_node(7).to_string()).await;

        assert!(matches!(
            mint(&context, owner).await,
            Err(CreateOnboardingSecretError::DeviceCapExceeded { limit: 2 })
        ));
    }

    #[tokio::test]
    async fn unlimited_without_cap() {
        let owner = UserId::local(Ulid::generate(), realm());
        let (_dir, context) = context_with_cap(None, &[owner, owner]).await;

        mint(&context, owner).await.expect("no cap configured");
    }

    #[tokio::test]
    async fn server_skips_cap() {
        // An infrastructure secret is never charged against a device cap.
        let owner = UserId::local(Ulid::generate(), realm());
        let (_dir, context) = context_with_cap(Some(1), &[owner]).await;

        drive(
            CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
                record: OnboardingSecretRecord {
                    mode: OnboardingMode::Server,
                    ..device_record(owner)
                },
            }),
            &context,
        )
        .await
        .expect("server secret mints");
    }
}
