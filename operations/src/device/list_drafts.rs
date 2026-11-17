//! Reads the device-local authoring queue in creation order.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::DEVICE_INTAKE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::PrefixPages;
use thiserror::Error;

use super::publish_queue::{MAX_PUBLISH_ENTRIES, PUBLISH_PAGE_SIZE, PublishEntry};

/// Outputs the queued drafts in creation order, stopping after the page that reaches
/// [`MAX_PUBLISH_ENTRIES`].
#[derive(Debug, PartialEq)]
pub struct ListDraftsOperation {
    pages: PrefixPages,
    entries: Vec<PublishEntry>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListDraftsError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
}

impl Default for ListDraftsOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl ListDraftsOperation {
    pub fn new() -> Self {
        Self {
            pages: PrefixPages::new(DEVICE_INTAKE_KEYSPACE, Vec::new(), PUBLISH_PAGE_SIZE),
            entries: Vec::new(),
        }
    }
}

impl Operation for ListDraftsOperation {
    type Output = Vec<PublishEntry>;
    type Error = ListDraftsError;

    fn first(&mut self) -> Result<Next<Vec<PublishEntry>>, ListDraftsError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<PublishEntry>>, ListDraftsError> {
        for (_, bytes) in self.pages.take(result)? {
            self.entries.push(PublishEntry::from_bytes(&bytes)?);
        }
        if self.pages.done() || self.entries.len() >= MAX_PUBLISH_ENTRIES {
            return Ok(Next::Done(std::mem::take(&mut self.entries)));
        }
        Ok(Next::Call(self.pages.call()))
    }
}

#[cfg(test)]
mod tests {
    use super::ListDraftsOperation;
    use crate::device::enqueue_draft::{EnqueueDraftInput, EnqueueDraftOperation};
    use crate::device::publish_queue::{PUBLISH_PAGE_SIZE, PublishEntry};
    use crate::driver::drive;
    use crate::tests::device::context;
    use aruna_core::UserId;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    // A full queue stops after the page that fills it, and a bad row fails.
    #[test]
    fn stops_when_full() {
        use super::{DEVICE_INTAKE_KEYSPACE, ListDraftsError, MAX_PUBLISH_ENTRIES};
        use crate::device::publish_queue::publish_entry;
        use aruna_core::calls::{IoCall, IoResult, Next};
        use aruna_core::operation::Operation;
        let owner = UserId::local(Ulid::nil(), RealmId::from_bytes([3u8; 32]));
        let entry = PublishEntry::new(
            Ulid::nil(),
            owner,
            Ulid::nil(),
            "/notes".to_string(),
            false,
            "{}".to_string(),
        );
        let (_, key, value) = publish_entry(&entry).unwrap();
        let mut list = ListDraftsOperation::new();
        let Ok(Next::Call(IoCall::Page { space, limit, .. })) = list.first() else {
            panic!("expected a page call");
        };
        assert_eq!((space.as_str(), limit), (DEVICE_INTAKE_KEYSPACE, PUBLISH_PAGE_SIZE));
        let full = IoResult::Page {
            rows: vec![(key.clone(), value); MAX_PUBLISH_ENTRIES],
            next: Some(key.clone()),
        };
        let Ok(Next::Done(listed)) = list.next(full) else {
            panic!("expected the full queue");
        };
        assert_eq!(listed.len(), MAX_PUBLISH_ENTRIES);
        let mut list = ListDraftsOperation::new();
        list.first().unwrap();
        let garbage = IoResult::Page {
            rows: vec![(key, vec![0xFF].into())],
            next: None,
        };
        assert!(matches!(
            list.next(garbage),
            Err(ListDraftsError::ConversionError(_))
        ));
    }

    #[tokio::test]
    async fn lists_queued_drafts() {
        // More than one page, so the cursor loop is exercised.
        let (_tempdir, context) = context().await;
        let owner = UserId::local(Ulid::generate(), RealmId::from_bytes([3u8; 32]));
        let mut queued = Vec::new();
        for index in 0..PUBLISH_PAGE_SIZE + 3 {
            let entry = PublishEntry::new(
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
