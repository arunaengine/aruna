//! Reads one queued authoring draft from the device queue.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use thiserror::Error;
use ulid::Ulid;

use super::publish_queue::{PublishEntry, read_entry};

#[derive(Debug, PartialEq)]
pub struct InspectDraftOperation {
    draft_id: Ulid,
}

#[derive(Debug, Error, PartialEq)]
pub enum InspectDraftError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("queued draft not found")]
    NotFound,
}

impl InspectDraftOperation {
    pub fn new(draft_id: Ulid) -> Self {
        Self { draft_id }
    }
}

impl Operation for InspectDraftOperation {
    type Output = PublishEntry;
    type Error = InspectDraftError;

    fn first(&mut self) -> Result<Next<PublishEntry>, InspectDraftError> {
        Ok(Next::Call(read_entry(self.draft_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PublishEntry>, InspectDraftError> {
        let [value] = values::<1>(result)?;
        let bytes = value.ok_or(InspectDraftError::NotFound)?;
        Ok(Next::Done(PublishEntry::from_bytes(&bytes)?))
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(error, InspectDraftError::NotFound)
    }
}

#[cfg(test)]
mod tests {
    use super::{InspectDraftError, InspectDraftOperation};
    use crate::device::enqueue_draft::{EnqueueDraftInput, EnqueueDraftOperation};
    use crate::device::publish_queue::PublishEntry;
    use crate::driver::drive;
    use crate::tests::device::context;
    use aruna_core::UserId;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    // A missing row is not found, a row that does not decode fails.
    #[test]
    fn bad_rows_fail() {
        use aruna_core::calls::{IoResult, Next};
        use aruna_core::operation::Operation;
        let mut inspect = InspectDraftOperation::new(Ulid::nil());
        assert_eq!(
            inspect.first(),
            Ok(Next::Call(crate::device::publish_queue::read_entry(Ulid::nil())))
        );
        let missing = inspect.next(IoResult::Values(vec![None]));
        assert_eq!(missing, Err(InspectDraftError::NotFound));
        let garbage = IoResult::Values(vec![Some(vec![0xFF].into())]);
        assert!(matches!(
            inspect.next(garbage),
            Err(InspectDraftError::ConversionError(_))
        ));
    }

    #[tokio::test]
    async fn reads_queued_draft() {
        let (_tempdir, context) = context().await;
        let entry = PublishEntry::new(
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
