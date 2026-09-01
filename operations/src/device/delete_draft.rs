//! Removes one queued authoring intent.

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DEVICE_INTAKE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use super::repository::{IntakeEntry, IntakeState, intake_key, read_intake};

#[derive(Debug, PartialEq)]
pub struct DeleteDraftOperation {
    draft_id: Ulid,
    state: DeleteDraftState,
    output: Option<Result<IntakeEntry, DeleteDraftError>>,
}

#[derive(Clone, Debug, PartialEq)]
enum DeleteDraftState {
    Init,
    StartTransaction,
    ReadEntry {
        txn_id: TxnId,
    },
    DeleteEntry {
        txn_id: TxnId,
        entry: Box<IntakeEntry>,
    },
    CommitTransaction {
        entry: Box<IntakeEntry>,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteDraftError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("queued draft not found")]
    NotFound,
    #[error("the draft is being published; retry once it settles")]
    PublishInFlight,
    #[error("deleting the queued draft did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl DeleteDraftOperation {
    pub fn new(draft_id: Ulid) -> Self {
        Self {
            draft_id,
            state: DeleteDraftState::Init,
            output: None,
        }
    }
}

impl Operation for DeleteDraftOperation {
    type Output = IntakeEntry;
    type Error = DeleteDraftError;

    fn start(&mut self) -> Effects {
        self.state = DeleteDraftState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error }) => {
                return fail(self, DeleteDraftError::StorageError(error));
            }
            other => other,
        };

        match self.state.clone() {
            DeleteDraftState::StartTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return fail(
                        self,
                        DeleteDraftError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "transaction started",
                            got,
                        },
                    );
                };
                self.state = DeleteDraftState::ReadEntry { txn_id };
                smallvec![read_intake(self.draft_id, Some(txn_id))]
            }
            DeleteDraftState::ReadEntry { txn_id } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return fail(
                        self,
                        DeleteDraftError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "read result",
                            got,
                        },
                    );
                };
                let Some(bytes) = value else {
                    return fail(self, DeleteDraftError::NotFound);
                };
                let entry = match IntakeEntry::from_bytes(&bytes) {
                    Ok(entry) => entry,
                    Err(error) => return fail(self, DeleteDraftError::ConversionError(error)),
                };
                // A forward may already have reached a holder, so the outcome
                // must be recorded before the owner can drop the entry.
                if matches!(entry.state, IntakeState::Publishing { .. }) {
                    return fail(self, DeleteDraftError::PublishInFlight);
                }
                self.state = DeleteDraftState::DeleteEntry {
                    txn_id,
                    entry: Box::new(entry),
                };
                smallvec![Effect::Storage(StorageEffect::BatchDelete {
                    deletes: vec![(
                        DEVICE_INTAKE_KEYSPACE.to_string(),
                        intake_key(self.draft_id)
                    )],
                    txn_id: Some(txn_id),
                })]
            }
            DeleteDraftState::DeleteEntry { txn_id, entry } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
                    return fail(
                        self,
                        DeleteDraftError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "batch delete result",
                            got,
                        },
                    );
                };
                self.state = DeleteDraftState::CommitTransaction { entry };
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            DeleteDraftState::CommitTransaction { entry } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return fail(
                        self,
                        DeleteDraftError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "transaction committed",
                            got,
                        },
                    );
                };
                self.state = DeleteDraftState::Finish;
                self.output = Some(Ok(*entry));
                smallvec![]
            }
            DeleteDraftState::Init | DeleteDraftState::Finish | DeleteDraftState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DeleteDraftState::Finish | DeleteDraftState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(DeleteDraftError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match &self.state {
            DeleteDraftState::ReadEntry { txn_id }
            | DeleteDraftState::DeleteEntry { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction {
                    txn_id: *txn_id
                })]
            }
            _ => smallvec![],
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            DeleteDraftError::NotFound | DeleteDraftError::PublishInFlight
        )
    }
}

fn fail(operation: &mut DeleteDraftOperation, error: DeleteDraftError) -> Effects {
    let cleanup = operation.abort();
    operation.state = DeleteDraftState::Error;
    operation.output = Some(Err(error));
    cleanup
}

#[cfg(test)]
mod tests {
    use super::{DeleteDraftError, DeleteDraftOperation};
    use crate::device::repository::{IntakeEntry, IntakeState, intake_entry};
    use crate::driver::{DriverContext, drive};
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::RealmId;
    use aruna_core::types::UserId;
    use aruna_storage::storage;
    use tempfile::tempdir;
    use ulid::Ulid;

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

    fn entry() -> IntakeEntry {
        IntakeEntry::new(
            Ulid::generate(),
            UserId::local(Ulid::generate(), RealmId::from_bytes([5u8; 32])),
            Ulid::generate(),
            "/notes".to_string(),
            false,
            "{}".to_string(),
        )
    }

    async fn store(context: &DriverContext, entry: &IntakeEntry) {
        let (key_space, key, value) = intake_entry(entry).unwrap();
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

    #[tokio::test]
    async fn deletes_pending_draft() {
        let (_tempdir, context) = context().await;
        let entry = entry();
        store(&context, &entry).await;
        let deleted = drive(DeleteDraftOperation::new(entry.draft_id), &context)
            .await
            .unwrap();
        assert_eq!(deleted.draft_id, entry.draft_id);
        assert_eq!(
            drive(DeleteDraftOperation::new(entry.draft_id), &context).await,
            Err(DeleteDraftError::NotFound)
        );
    }

    #[tokio::test]
    async fn refuses_publishing_draft() {
        // The forward may already have applied, so the outcome must land first.
        let (_tempdir, context) = context().await;
        let mut entry = entry();
        entry.state = IntakeState::Publishing {
            document_id: Ulid::generate(),
            due_at_ms: 0,
            attempts: 1,
        };
        store(&context, &entry).await;
        assert_eq!(
            drive(DeleteDraftOperation::new(entry.draft_id), &context).await,
            Err(DeleteDraftError::PublishInFlight)
        );
    }

    #[tokio::test]
    async fn deletes_failed_draft() {
        let (_tempdir, context) = context().await;
        let mut entry = entry();
        entry.state = IntakeState::Failed {
            reason: "group is gone".to_string(),
            retryable: false,
            document_id: None,
        };
        store(&context, &entry).await;
        assert!(
            drive(DeleteDraftOperation::new(entry.draft_id), &context)
                .await
                .is_ok()
        );
    }
}
