//! Reads one queued authoring intent.

use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use super::repository::{IntakeEntry, read_intake};

#[derive(Debug, PartialEq)]
pub struct InspectDraftOperation {
    draft_id: Ulid,
    state: InspectDraftState,
    output: Option<Result<IntakeEntry, InspectDraftError>>,
}

#[derive(Clone, Debug, PartialEq)]
enum InspectDraftState {
    Init,
    ReadEntry,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum InspectDraftError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("queued draft not found")]
    NotFound,
    #[error("inspecting the queued draft did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl InspectDraftOperation {
    pub fn new(draft_id: Ulid) -> Self {
        Self {
            draft_id,
            state: InspectDraftState::Init,
            output: None,
        }
    }
}

impl Operation for InspectDraftOperation {
    type Output = IntakeEntry;
    type Error = InspectDraftError;

    fn start(&mut self) -> Effects {
        self.state = InspectDraftState::ReadEntry;
        smallvec![read_intake(self.draft_id, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error }) => {
                return fail(self, InspectDraftError::StorageError(error));
            }
            other => other,
        };

        match self.state.clone() {
            InspectDraftState::ReadEntry => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return fail(
                        self,
                        InspectDraftError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "read result",
                            got,
                        },
                    );
                };
                let Some(bytes) = value else {
                    return fail(self, InspectDraftError::NotFound);
                };
                match IntakeEntry::from_bytes(&bytes) {
                    Ok(entry) => {
                        self.state = InspectDraftState::Finish;
                        self.output = Some(Ok(entry));
                        smallvec![]
                    }
                    Err(error) => fail(self, InspectDraftError::ConversionError(error)),
                }
            }
            InspectDraftState::Init | InspectDraftState::Finish | InspectDraftState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            InspectDraftState::Finish | InspectDraftState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(InspectDraftError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(error, InspectDraftError::NotFound)
    }
}

fn fail(operation: &mut InspectDraftOperation, error: InspectDraftError) -> Effects {
    operation.state = InspectDraftState::Error;
    operation.output = Some(Err(error));
    smallvec![]
}

#[cfg(test)]
mod tests {
    use super::{InspectDraftError, InspectDraftOperation};
    use crate::device::enqueue_draft::{EnqueueDraftInput, EnqueueDraftOperation};
    use crate::device::repository::IntakeEntry;
    use crate::driver::{DriverContext, drive};
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

    #[tokio::test]
    async fn reads_queued_draft() {
        let (_tempdir, context) = context().await;
        let entry = IntakeEntry::new(
            Ulid::generate(),
            UserId::local(Ulid::generate(), RealmId::from_bytes([2u8; 32])),
            Ulid::generate(),
            "/notes".to_string(),
            true,
            "{}".to_string(),
        );
        let draft_id = entry.draft_id;
        drive(
            EnqueueDraftOperation::new(EnqueueDraftInput { entry }),
            &context,
        )
        .await
        .unwrap();
        let read = drive(InspectDraftOperation::new(draft_id), &context)
            .await
            .unwrap();
        assert_eq!(read.draft_id, draft_id);
        assert!(read.public);
    }

    #[tokio::test]
    async fn reports_missing_draft() {
        let (_tempdir, context) = context().await;
        assert_eq!(
            drive(InspectDraftOperation::new(Ulid::generate()), &context).await,
            Err(InspectDraftError::NotFound)
        );
    }
}
