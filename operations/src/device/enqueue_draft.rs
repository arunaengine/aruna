//! Queues one offline authoring draft on the device while the queue limit allows it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::DEVICE_INTAKE_KEYSPACE;
use aruna_core::metadata::MetadataActor;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{PrefixPages, done};
use aruna_core::storage_entries::metadata_actor_entry;
use thiserror::Error;

use super::publish_queue::{
    MAX_PUBLISH_ENTRIES, PublishEntry, PublishKind, fence_entry, publish_entry,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnqueueDraftInput {
    pub entry: PublishEntry,
}

/// Counts the queue and writes the entry, its fence row and the advanced actor in one
/// transaction while the queue holds fewer than [`MAX_PUBLISH_ENTRIES`].
#[derive(Debug, PartialEq)]
pub struct EnqueueDraftOperation {
    input: EnqueueDraftInput,
    /// The edit's advanced CRDT actor, stored atomically with the entry.
    actor: Option<MetadataActor>,
    /// Issue time of the token that authored the entry, stored atomically with it.
    issued_at: u64,
    written: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum EnqueueDraftError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("a queued draft needs a document path")]
    MissingPath,
    #[error("a queued draft needs an RO-Crate payload")]
    MissingPayload,
    #[error("the authoring queue already holds the maximum of {limit} drafts")]
    QueueFull { limit: usize },
}

impl EnqueueDraftOperation {
    pub fn new(input: EnqueueDraftInput) -> Self {
        Self {
            input,
            actor: None,
            issued_at: 0,
            written: false,
        }
    }

    pub fn with_actor(mut self, actor: MetadataActor) -> Self {
        self.actor = Some(actor);
        self
    }

    pub fn with_fence(mut self, issued_at: u64) -> Self {
        self.issued_at = issued_at;
        self
    }

    fn write(&mut self) -> Result<IoCall, EnqueueDraftError> {
        let mut writes = vec![
            publish_entry(&self.input.entry)?,
            fence_entry(self.input.entry.draft_id, self.issued_at),
        ];
        if let Some(actor) = &self.actor {
            writes.push(metadata_actor_entry(actor)?);
        }
        self.written = true;
        Ok(IoCall::Write(writes))
    }
}

/// The page that counts the queued entries up to the limit.
fn count_call() -> IoCall {
    PrefixPages::new(DEVICE_INTAKE_KEYSPACE, Vec::new(), MAX_PUBLISH_ENTRIES).call()
}

impl Operation for EnqueueDraftOperation {
    type Output = PublishEntry;
    type Error = EnqueueDraftError;

    fn first(&mut self) -> Result<Next<PublishEntry>, EnqueueDraftError> {
        if self.input.entry.document_path.trim().is_empty() {
            return Err(EnqueueDraftError::MissingPath);
        }
        // An edit carries its submission in the kind; only a create authors the
        // crate text this field holds.
        if matches!(self.input.entry.kind, PublishKind::Create)
            && self.input.entry.jsonld.trim().is_empty()
        {
            return Err(EnqueueDraftError::MissingPayload);
        }
        Ok(Next::Call(count_call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PublishEntry>, EnqueueDraftError> {
        if self.written {
            done(result)?;
            return Ok(Next::Done(self.input.entry.clone()));
        }
        let (rows, _) = result.page()?;
        if rows.len() >= MAX_PUBLISH_ENTRIES {
            return Err(EnqueueDraftError::QueueFull {
                limit: MAX_PUBLISH_ENTRIES,
            });
        }
        Ok(Next::Call(self.write()?))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
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

#[cfg(test)]
mod tests {
    use super::{EnqueueDraftError, EnqueueDraftInput, EnqueueDraftOperation};
    use crate::device::publish_queue::{MAX_PUBLISH_ENTRIES, PublishEntry, publish_entry};
    use crate::driver::{DriverContext, drive};
    use crate::tests::device::context;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    fn owner() -> UserId {
        UserId::local(Ulid::generate(), RealmId::from_bytes([9u8; 32]))
    }

    fn entry(owner: UserId) -> PublishEntry {
        PublishEntry::new(
            Ulid::generate(),
            owner,
            Ulid::generate(),
            "/reports/january".to_string(),
            false,
            r#"{"@graph":[]}"#.to_string(),
        )
    }

    async fn enqueue(
        context: &DriverContext,
        entry: PublishEntry,
    ) -> Result<PublishEntry, EnqueueDraftError> {
        drive(
            EnqueueDraftOperation::new(EnqueueDraftInput { entry }),
            context,
        )
        .await
    }

    // The queue is counted, then the entry and its fence row are written in the transaction.
    #[test]
    fn counts_then_writes() {
        use super::count_call;
        use crate::device::publish_queue::fence_entry;
        use aruna_core::calls::{IoCall, IoResult, Next};
        use aruna_core::operation::Operation;
        let queued = entry(owner());
        let mut enqueue = EnqueueDraftOperation::new(EnqueueDraftInput {
            entry: queued.clone(),
        })
        .with_fence(7);
        assert_eq!(
            enqueue.transaction(),
            aruna_core::operation::TransactionMode::Write
        );
        assert_eq!(enqueue.first(), Ok(Next::Call(count_call())));
        let rows = IoCall::Write(vec![
            publish_entry(&queued).unwrap(),
            fence_entry(queued.draft_id, 7),
        ]);
        let empty = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(enqueue.next(empty), Ok(Next::Call(rows)));
        assert_eq!(enqueue.next(IoResult::Done), Ok(Next::Done(queued)));
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
        for _ in 0..MAX_PUBLISH_ENTRIES {
            let (key_space, key, value) = publish_entry(&entry(owner)).unwrap();
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
                limit: MAX_PUBLISH_ENTRIES
            })
        );
    }
}
