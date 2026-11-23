//! Reads one imported object row or one bucket counter row for the S3 and storage routes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoError, IoResult, Next};
use aruna_core::keyspaces::{IMPORTED_OBJECT_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::{read_row, values};
use aruna_core::structs::storage::backend_import::{ImportedObject, object_key};
use aruna_core::structs::storage::usage::{BucketCounters, usage_bucket_key};
use ulid::Ulid;

/// The imported object row of `path` under the import rule `import_id`; `None` when absent.
#[derive(Debug, PartialEq)]
pub struct ImportedRowOperation {
    import_id: Ulid,
    path: String,
}

impl ImportedRowOperation {
    pub fn new(import_id: Ulid, path: &str) -> Self {
        Self {
            import_id,
            path: path.to_string(),
        }
    }
}

impl Operation for ImportedRowOperation {
    type Output = Option<ImportedObject>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, IoError> {
        let key = object_key(self.import_id, &self.path);
        Ok(Next::Call(read_row(IMPORTED_OBJECT_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, IoError> {
        let [value] = values::<1>(result)?;
        let row = value.as_deref().map(ImportedObject::from_bytes).transpose();
        let row = row.map_err(|error| IoError::Codec(error.to_string()))?;
        Ok(Next::Done(row))
    }
}

/// The counter row of `bucket`; a bucket without one holds nothing yet.
#[derive(Debug, PartialEq)]
pub struct BucketCountersOperation {
    bucket: String,
}

impl BucketCountersOperation {
    pub fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
        }
    }
}

impl Operation for BucketCountersOperation {
    type Output = BucketCounters;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, IoError> {
        let key = usage_bucket_key(&self.bucket);
        Ok(Next::Call(read_row(USAGE_STATS_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, IoError> {
        let [value] = values::<1>(result)?;
        let counters = value.as_deref().map(BucketCounters::from_bytes).transpose();
        let counters = counters.map_err(|error| IoError::Codec(error.to_string()))?;
        Ok(Next::Done(counters.unwrap_or_default()))
    }
}
