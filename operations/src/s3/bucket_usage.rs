//! Per-bucket inventory for the portal's bucket overview.
//!
//! There is no per-bucket usage counter: the maintained counters are keyed
//! globally, per group and per backend. This bounded scan reuses the version and
//! multipart listings the deletion preflight already runs and adds the byte sum.

use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

use crate::s3::list_multipart_uploads::{
    ListMultipartUploadsError, ListMultipartUploadsInput, ListMultipartUploadsOperation,
};
use crate::s3::list_object_versions::{
    ListObjectVersionsError, ListObjectVersionsInput, ListObjectVersionsItem,
    ListObjectVersionsOperation,
};

#[derive(Debug, Error, PartialEq)]
pub enum BucketUsageError {
    #[error(transparent)]
    Versions(#[from] ListObjectVersionsError),
    #[error(transparent)]
    Uploads(#[from] ListMultipartUploadsError),
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
    versions: Option<ListObjectVersionsOperation>,
    uploads: Option<ListMultipartUploadsOperation>,
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
        let mut uploads = ListMultipartUploadsOperation::new(ListMultipartUploadsInput {
            bucket: self.input.bucket.clone(),
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: self.input.limit,
        })
        .complete_scan();
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
        let result = match versions.finalize().and_then(|result| {
            result.unwrap_or(Err(ListObjectVersionsError::ListObjectVersionsFailed))
        }) {
            Ok(result) => result,
            Err(error) => return self.fail(error.into()),
        };
        self.truncated |= result.is_truncated;
        for item in result.items {
            match item {
                ListObjectVersionsItem::Version {
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
                ListObjectVersionsItem::DeleteMarker { .. } => self.totals.delete_markers += 1,
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
        let result = match uploads.finalize().and_then(|result| {
            result.unwrap_or(Err(ListMultipartUploadsError::ListMultipartUploadsFailed))
        }) {
            Ok(result) => result,
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
        let mut versions = ListObjectVersionsOperation::new(ListObjectVersionsInput {
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
