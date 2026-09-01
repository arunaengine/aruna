//! Queues one offline authoring intent on the device.

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DEVICE_INTAKE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, TxnId};
use smallvec::smallvec;
use thiserror::Error;

use super::repository::{IntakeEntry, IntakeKind, MAX_INTAKE_ENTRIES, intake_entry};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnqueueDraftInput {
    pub entry: IntakeEntry,
}

#[derive(Debug, PartialEq)]
pub struct EnqueueDraftOperation {
    input: EnqueueDraftInput,
    state: EnqueueDraftState,
    output: Option<Result<IntakeEntry, EnqueueDraftError>>,
}

#[derive(Clone, Debug, PartialEq)]
enum EnqueueDraftState {
    Init,
    StartTransaction,
    CountEntries { txn_id: TxnId },
    WriteEntry { txn_id: TxnId },
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum EnqueueDraftError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("a queued draft needs a document path")]
    MissingPath,
    #[error("a queued draft needs an RO-Crate payload")]
    MissingPayload,
    #[error("the authoring queue already holds the maximum of {limit} drafts")]
    QueueFull { limit: usize },
    #[error("queueing the draft did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl EnqueueDraftOperation {
    pub fn new(input: EnqueueDraftInput) -> Self {
        Self {
            input,
            state: EnqueueDraftState::Init,
            output: None,
        }
    }

    fn emit_write(&mut self, txn_id: TxnId) -> Effects {
        self.state = EnqueueDraftState::WriteEntry { txn_id };
        let entry = match intake_entry(&self.input.entry) {
            Ok(entry) => entry,
            Err(error) => return fail(self, EnqueueDraftError::ConversionError(error)),
        };
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![entry],
            txn_id: Some(txn_id),
        })]
    }
}

impl Operation for EnqueueDraftOperation {
    type Output = IntakeEntry;
    type Error = EnqueueDraftError;

    fn start(&mut self) -> Effects {
        if self.input.entry.document_path.trim().is_empty() {
            return fail(self, EnqueueDraftError::MissingPath);
        }
        // An edit carries its submission in the kind; only a create authors the
        // crate text this field holds.
        if matches!(self.input.entry.kind, IntakeKind::Create)
            && self.input.entry.jsonld.trim().is_empty()
        {
            return fail(self, EnqueueDraftError::MissingPayload);
        }
        self.state = EnqueueDraftState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error }) => {
                return fail(self, EnqueueDraftError::StorageError(error));
            }
            other => other,
        };

        match self.state.clone() {
            EnqueueDraftState::StartTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return fail(
                        self,
                        EnqueueDraftError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "transaction started",
                            got,
                        },
                    );
                };
                self.state = EnqueueDraftState::CountEntries { txn_id };
                smallvec![Effect::Storage(StorageEffect::Iter {
                    key_space: DEVICE_INTAKE_KEYSPACE.to_string(),
                    prefix: None,
                    start: None,
                    limit: MAX_INTAKE_ENTRIES,
                    txn_id: Some(txn_id),
                })]
            }
            EnqueueDraftState::CountEntries { txn_id } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
                    return fail(
                        self,
                        EnqueueDraftError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "iter result",
                            got,
                        },
                    );
                };
                if values.len() >= MAX_INTAKE_ENTRIES {
                    return fail(
                        self,
                        EnqueueDraftError::QueueFull {
                            limit: MAX_INTAKE_ENTRIES,
                        },
                    );
                }
                self.emit_write(txn_id)
            }
            EnqueueDraftState::WriteEntry { txn_id } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
                    return fail(
                        self,
                        EnqueueDraftError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "batch write result",
                            got,
                        },
                    );
                };
                self.state = EnqueueDraftState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            EnqueueDraftState::CommitTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return fail(
                        self,
                        EnqueueDraftError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "transaction committed",
                            got,
                        },
                    );
                };
                self.state = EnqueueDraftState::Finish;
                self.output = Some(Ok(self.input.entry.clone()));
                smallvec![]
            }
            EnqueueDraftState::Init | EnqueueDraftState::Finish | EnqueueDraftState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            EnqueueDraftState::Finish | EnqueueDraftState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(EnqueueDraftError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            EnqueueDraftState::CountEntries { txn_id }
            | EnqueueDraftState::WriteEntry { txn_id } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            EnqueueDraftError::MissingPath
                | EnqueueDraftError::MissingPayload
                | EnqueueDraftError::QueueFull { .. }
        )
    }
}

fn fail(operation: &mut EnqueueDraftOperation, error: EnqueueDraftError) -> Effects {
    let cleanup = operation.abort();
    operation.state = EnqueueDraftState::Error;
    operation.output = Some(Err(error));
    cleanup
}

#[cfg(test)]
mod tests {
    use super::{EnqueueDraftError, EnqueueDraftInput, EnqueueDraftOperation};
    use crate::device::repository::{IntakeEntry, MAX_INTAKE_ENTRIES, intake_entry};
    use crate::driver::{DriverContext, drive};
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::RealmId;
    use aruna_core::types::UserId;
    use aruna_storage::storage;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn owner() -> UserId {
        UserId::local(Ulid::generate(), RealmId::from_bytes([9u8; 32]))
    }

    fn entry(owner: UserId) -> IntakeEntry {
        IntakeEntry::new(
            Ulid::generate(),
            owner,
            Ulid::generate(),
            "/reports/january".to_string(),
            false,
            r#"{"@graph":[]}"#.to_string(),
        )
    }

    async fn context() -> (tempfile::TempDir, DriverContext) {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
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

    async fn enqueue(
        context: &DriverContext,
        entry: IntakeEntry,
    ) -> Result<IntakeEntry, EnqueueDraftError> {
        drive(
            EnqueueDraftOperation::new(EnqueueDraftInput { entry }),
            context,
        )
        .await
    }

    #[tokio::test]
    async fn queues_offline_draft() {
        let (_tempdir, context) = context().await;
        let queued = enqueue(&context, entry(owner())).await.unwrap();
        assert!(queued.is_due(queued.created_at_ms));
    }

    #[tokio::test]
    async fn rejects_empty_draft() {
        let (_tempdir, context) = context().await;
        let mut blank = entry(owner());
        blank.document_path = "  ".to_string();
        assert_eq!(
            enqueue(&context, blank).await,
            Err(EnqueueDraftError::MissingPath)
        );
        let mut empty = entry(owner());
        empty.jsonld = String::new();
        assert_eq!(
            enqueue(&context, empty).await,
            Err(EnqueueDraftError::MissingPayload)
        );
    }

    #[tokio::test]
    async fn refuses_full_queue() {
        // The cap is a device-local backlog bound, not a realm quota.
        let (_tempdir, context) = context().await;
        let owner = owner();
        for _ in 0..MAX_INTAKE_ENTRIES {
            let (key_space, key, value) = intake_entry(&entry(owner)).unwrap();
            context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space,
                    key,
                    value,
                    txn_id: None,
                })
                .await;
        }
        assert_eq!(
            enqueue(&context, entry(owner)).await,
            Err(EnqueueDraftError::QueueFull {
                limit: MAX_INTAKE_ENTRIES
            })
        );
    }
}
