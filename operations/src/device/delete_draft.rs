//! Removes one queued authoring draft from the device publish queue.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{DEVICE_FENCE_KEYSPACE, DEVICE_INTAKE_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::{done, values};
use thiserror::Error;
use ulid::Ulid;

use super::publish_queue::{PublishEntry, PublishState, publish_key, read_entry};

/// Deletes the queued draft and its fence row in one transaction and outputs the deleted entry.
#[derive(Debug, PartialEq)]
pub struct DeleteDraftOperation {
    draft_id: Ulid,
    /// The deleted entry, once the delete is asked for.
    entry: Option<Box<PublishEntry>>,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteDraftError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("queued draft not found")]
    NotFound,
    #[error("the draft is being published; retry once it settles")]
    PublishInFlight,
}

impl DeleteDraftOperation {
    pub fn new(draft_id: Ulid) -> Self {
        Self {
            draft_id,
            entry: None,
        }
    }

    fn delete(&mut self, result: IoResult) -> Result<IoCall, DeleteDraftError> {
        let [value] = values::<1>(result)?;
        let bytes = value.ok_or(DeleteDraftError::NotFound)?;
        let entry = PublishEntry::from_bytes(&bytes)?;
        // A forward may already have reached a holder, so the outcome
        // must be recorded before the owner can drop the entry.
        if matches!(entry.state, PublishState::Publishing { .. }) {
            return Err(DeleteDraftError::PublishInFlight);
        }
        self.entry = Some(Box::new(entry));
        let key = publish_key(self.draft_id);
        Ok(IoCall::Delete(vec![
            (DEVICE_INTAKE_KEYSPACE.to_string(), key.clone()),
            (DEVICE_FENCE_KEYSPACE.to_string(), key),
        ]))
    }
}

impl Operation for DeleteDraftOperation {
    type Output = PublishEntry;
    type Error = DeleteDraftError;

    fn first(&mut self) -> Result<Next<PublishEntry>, DeleteDraftError> {
        Ok(Next::Call(read_entry(self.draft_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PublishEntry>, DeleteDraftError> {
        match self.entry.take() {
            None => Ok(Next::Call(self.delete(result)?)),
            Some(entry) => {
                done(result)?;
                Ok(Next::Done(*entry))
            }
        }
    }

    fn transaction(&self) -> bool {
        true
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            DeleteDraftError::NotFound | DeleteDraftError::PublishInFlight
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{DeleteDraftError, DeleteDraftOperation};
    use crate::device::publish_queue::{PublishEntry, PublishState, publish_entry};
    use crate::driver::{DriverContext, drive};
    use crate::tests::device::context;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    fn entry() -> PublishEntry {
        PublishEntry::new(
            Ulid::generate(),
            UserId::local(Ulid::generate(), RealmId::from_bytes([5u8; 32])),
            Ulid::generate(),
            "/notes".to_string(),
            false,
            "{}".to_string(),
        )
    }

    async fn store(context: &DriverContext, entry: &PublishEntry) {
        let (key_space, key, value) = publish_entry(entry).unwrap();
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

    // The entry and its fence row are deleted in the transaction; a draft in flight stays.
    #[test]
    fn deletes_both_rows() {
        use super::{DEVICE_FENCE_KEYSPACE, DEVICE_INTAKE_KEYSPACE, publish_key};
        use aruna_core::calls::{IoCall, IoResult, Next};
        use aruna_core::operation::Operation;
        let entry = entry();
        let mut delete = DeleteDraftOperation::new(entry.draft_id);
        assert!(delete.transaction());
        delete.first().unwrap();
        let (_, _, value) = publish_entry(&entry).unwrap();
        let key = publish_key(entry.draft_id);
        let rows = IoCall::Delete(vec![
            (DEVICE_INTAKE_KEYSPACE.to_string(), key.clone()),
            (DEVICE_FENCE_KEYSPACE.to_string(), key),
        ]);
        let read = IoResult::Values(vec![Some(value)]);
        assert_eq!(delete.next(read), Ok(Next::Call(rows)));
        assert_eq!(delete.next(IoResult::Done), Ok(Next::Done(entry)));
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
        entry.state = PublishState::Publishing {
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
        entry.state = PublishState::Failed {
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
