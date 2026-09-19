//! Sums one bucket's objects, uploads and bytes for the portal bucket overview.
//! There is no per-bucket counter, so this scans with a budget and may report lower bounds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

use crate::s3::multipart::uploads::{ListUploadsError, ListUploadsInput, ListUploadsOperation};
use crate::s3::object::versions::{
    ListVersionsError, ListVersionsInput, ListVersionsItem, ListVersionsOperation,
};

#[derive(Debug, Error, PartialEq)]
pub enum BucketUsageError {
    #[error(transparent)]
    Versions(#[from] ListVersionsError),
    #[error(transparent)]
    Uploads(#[from] ListUploadsError),
    #[error("bucket usage received an event in state {state:?}: {event:?}")]
    UnexpectedEvent {
        state: BucketUsageState,
        event: Event,
    },
    #[error("BucketUsage failed")]
    BucketUsageFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BucketUsageState {
    Init,
    ScanVersions,
    ScanUploads,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct BucketUsageInput {
    pub bucket: String,
    /// Page bound for both scans. A scan that hits it leaves `complete` false.
    pub limit: usize,
}

/// Node-local totals. Every number is a lower bound once `complete` is false.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BucketUsageOutput {
    pub objects: u64,
    pub versions: u64,
    pub delete_markers: u64,
    pub open_multipart_uploads: u64,
    pub logical_bytes: u64,
    pub complete: bool,
}

#[derive(Debug, PartialEq)]
pub struct BucketUsageOperation {
    input: BucketUsageInput,
    state: BucketUsageState,
    versions: Option<ListVersionsOperation>,
    uploads: Option<ListUploadsOperation>,
    totals: BucketUsageOutput,
    truncated: bool,
    output: Option<Result<BucketUsageOutput, BucketUsageError>>,
}

impl BucketUsageOperation {
    pub fn new(input: BucketUsageInput) -> Self {
        Self {
            input,
            state: BucketUsageState::Init,
            versions: None,
            uploads: None,
            totals: BucketUsageOutput::default(),
            truncated: false,
            output: None,
        }
    }

    fn fail(&mut self, error: BucketUsageError) -> Effects {
        self.state = BucketUsageState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn scan_uploads(&mut self) -> Effects {
        let mut uploads = ListUploadsOperation::new(ListUploadsInput {
            bucket: self.input.bucket.clone(),
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: self.input.limit,
        });
        self.state = BucketUsageState::ScanUploads;
        let effects = uploads.start();
        self.uploads = Some(uploads);
        effects
    }

    fn finish(&mut self) -> Effects {
        self.state = BucketUsageState::Finish;
        self.totals.complete = !self.truncated;
        self.output = Some(Ok(self.totals));
        smallvec![]
    }

    fn step_versions(&mut self, event: Event) -> Effects {
        let Some(versions) = self.versions.as_mut() else {
            return self.fail(BucketUsageError::BucketUsageFailed);
        };
        let effects = versions.step(event);
        if !versions.is_complete() {
            return effects;
        }
        let Some(versions) = self.versions.take() else {
            return self.fail(BucketUsageError::BucketUsageFailed);
        };
        let result = match versions.finalize() {
            Ok(result) => result,
            Err(error) => return self.fail(error.into()),
        };
        self.truncated |= result.is_truncated;
        for item in result.items {
            match item {
                ListVersionsItem::Version {
                    is_latest,
                    location,
                    source_metadata,
                    ..
                } => {
                    self.totals.versions += 1;
                    self.totals.objects += u64::from(is_latest);
                    let bytes = location
                        .map(|location| location.blob_size)
                        .or_else(|| source_metadata.map(|metadata| metadata.content_length))
                        .unwrap_or_default();
                    self.totals.logical_bytes = self.totals.logical_bytes.saturating_add(bytes);
                }
                ListVersionsItem::DeleteMarker { .. } => self.totals.delete_markers += 1,
            }
        }
        self.scan_uploads()
    }

    fn step_uploads(&mut self, event: Event) -> Effects {
        let Some(uploads) = self.uploads.as_mut() else {
            return self.fail(BucketUsageError::BucketUsageFailed);
        };
        let effects = uploads.step(event);
        if !uploads.is_complete() {
            return effects;
        }
        let Some(uploads) = self.uploads.take() else {
            return self.fail(BucketUsageError::BucketUsageFailed);
        };
        let result = match uploads.finalize() {
            Ok(result) => result,
            // The scan keeps its row budget, so a reader cannot walk every
            // multipart row on the node; an exhausted budget is a lower bound.
            Err(ListUploadsError::ScanBudgetExceeded) => {
                self.truncated = true;
                return self.finish();
            }
            Err(error) => return self.fail(error.into()),
        };
        self.truncated |= result.is_truncated;
        self.totals.open_multipart_uploads = result.uploads.len() as u64;
        self.finish()
    }
}

impl Operation for BucketUsageOperation {
    type Output = BucketUsageOutput;
    type Error = BucketUsageError;

    fn start(&mut self) -> Effects {
        let mut versions = ListVersionsOperation::new(ListVersionsInput {
            bucket: self.input.bucket.clone(),
            prefix: None,
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: Some(self.input.limit),
        });
        self.state = BucketUsageState::ScanVersions;
        let effects = versions.start();
        self.versions = Some(versions);
        effects
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            BucketUsageState::ScanVersions => self.step_versions(event),
            BucketUsageState::ScanUploads => self.step_uploads(event),
            state => self.fail(BucketUsageError::UnexpectedEvent { state, event }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            BucketUsageState::Finish | BucketUsageState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(BucketUsageError::BucketUsageFailed)?
    }

    fn abort(&mut self) -> Effects {
        match (self.versions.as_mut(), self.uploads.as_mut()) {
            (Some(versions), _) => versions.abort(),
            (None, Some(uploads)) => uploads.abort(),
            (None, None) => smallvec![],
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::errors::StorageError;
    use aruna_core::events::StorageEvent;
    use ulid::Ulid;

    #[test]
    fn budget_bounds_uploads() {
        // A bucket reader must not be able to scan every multipart row on the
        // node, so an exhausted budget reports a partial result instead.
        let mut operation = BucketUsageOperation::new(BucketUsageInput {
            bucket: "data".to_string(),
            limit: 10,
        });
        let mut uploads = ListUploadsOperation::new(ListUploadsInput {
            bucket: "data".to_string(),
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 10,
        })
        .with_scan_budget(0);
        uploads.start();
        operation.state = BucketUsageState::ScanUploads;
        operation.uploads = Some(uploads);

        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_parts(1, 1),
        }));

        let usage = operation.finalize().expect("the inventory settles");
        assert!(!usage.complete);
    }

    #[test]
    fn child_storage_propagates() {
        // The active list child sees the error; the parent must surface its
        // typed error and not abort anything a second time.
        let mut operation = BucketUsageOperation::new(BucketUsageInput {
            bucket: "data".to_string(),
            limit: 10,
        });
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_parts(2, 2),
        }));

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::CommitFailed,
        }));

        assert!(effects.is_empty());
        assert!(operation.abort().is_empty());
        assert_eq!(
            operation.finalize(),
            Err(BucketUsageError::Versions(ListVersionsError::StorageError(
                StorageError::CommitFailed
            )))
        );
    }
}
