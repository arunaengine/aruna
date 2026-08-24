//! Reads the device-local authoring queue in creation order.

use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

use super::repository::{IntakeEntry, MAX_INTAKE_ENTRIES, scan_intake};

#[derive(Debug, PartialEq)]
pub struct ListDraftsOperation {
    entries: Vec<IntakeEntry>,
    state: ListDraftsState,
    output: Option<Result<Vec<IntakeEntry>, ListDraftsError>>,
}

#[derive(Clone, Debug, PartialEq)]
enum ListDraftsState {
    Init,
    Scan,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListDraftsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("listing queued drafts did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl Default for ListDraftsOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl ListDraftsOperation {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            state: ListDraftsState::Init,
            output: None,
        }
    }
}

impl Operation for ListDraftsOperation {
    type Output = Vec<IntakeEntry>;
    type Error = ListDraftsError;

    fn start(&mut self) -> Effects {
        self.state = ListDraftsState::Scan;
        smallvec![scan_intake(None, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error }) => {
                return fail(self, ListDraftsError::StorageError(error));
            }
            other => other,
        };

        match self.state.clone() {
            ListDraftsState::Scan => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) = event
                else {
                    return fail(
                        self,
                        ListDraftsError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "iter result",
                            got,
                        },
                    );
                };
                for (_, bytes) in values {
                    match IntakeEntry::from_bytes(&bytes) {
                        Ok(entry) => self.entries.push(entry),
                        Err(error) => return fail(self, ListDraftsError::ConversionError(error)),
                    }
                }
                match next_start_after {
                    Some(cursor) if self.entries.len() < MAX_INTAKE_ENTRIES => {
                        smallvec![scan_intake(Some(cursor), None)]
                    }
                    _ => {
                        self.state = ListDraftsState::Finish;
                        self.output = Some(Ok(std::mem::take(&mut self.entries)));
                        smallvec![]
                    }
                }
            }
            ListDraftsState::Init | ListDraftsState::Finish | ListDraftsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ListDraftsState::Finish | ListDraftsState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ListDraftsError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

fn fail(operation: &mut ListDraftsOperation, error: ListDraftsError) -> Effects {
    operation.state = ListDraftsState::Error;
    operation.output = Some(Err(error));
    smallvec![]
}

#[cfg(test)]
mod tests {
    use super::ListDraftsOperation;
    use crate::device::enqueue_draft::{EnqueueDraftInput, EnqueueDraftOperation};
    use crate::device::repository::{INTAKE_PAGE_SIZE, IntakeEntry};
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
    async fn lists_queued_drafts() {
        // More than one page, so the cursor loop is exercised.
        let (_tempdir, context) = context().await;
        let owner = UserId::local(Ulid::generate(), RealmId::from_bytes([3u8; 32]));
        let mut queued = Vec::new();
        for index in 0..INTAKE_PAGE_SIZE + 3 {
            let entry = IntakeEntry::new(
                Ulid::generate(),
                owner,
                Ulid::generate(),
                format!("/notes/{index}"),
                false,
                "{}".to_string(),
            );
            queued.push(entry.draft_id);
            drive(
                EnqueueDraftOperation::new(EnqueueDraftInput { entry }),
                &context,
            )
            .await
            .unwrap();
        }
        let listed = drive(ListDraftsOperation::new(), &context).await.unwrap();
        queued.sort();
        assert_eq!(
            listed
                .iter()
                .map(|entry| entry.draft_id)
                .collect::<Vec<_>>(),
            queued
        );
    }

    #[tokio::test]
    async fn lists_empty_queue() {
        let (_tempdir, context) = context().await;
        assert!(
            drive(ListDraftsOperation::new(), &context)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
