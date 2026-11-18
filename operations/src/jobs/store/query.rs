//! Lists and pages stored job rows and runs the shared storage transaction helpers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::row_calls;
use super::store_call::RowBatch;
use super::*;
use crate::driver::drive;

use super::list_jobs::ListJobsOperation;

/// Owner-scoped listing, newest first, with an opaque cursor and record filter; see
/// [`ListJobsOperation`].
pub async fn list_user_jobs(
    context: &DriverContext,
    user_id: UserId,
    cursor: Option<Vec<u8>>,
    limit: usize,
    filter: impl Fn(&JobRecord) -> bool + Send,
) -> Result<(Vec<JobRecord>, Option<Vec<u8>>), String> {
    drive(
        ListJobsOperation::new(user_id, cursor, limit, filter),
        context,
    )
    .await
    .map_err(|error| error.to_string())
}

pub async fn list_job_entries(
    context: &DriverContext,
    job_id: JobId,
    last_key: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Vec<(Vec<u8>, Value)>, Option<Vec<u8>>), String> {
    drive(
        row_calls::list_job_entries(job_id, last_key, limit),
        context,
    )
    .await
    .map_err(|error| error.to_string())
}

pub(crate) async fn job_entry_deletes(
    context: &DriverContext,
    job_id: JobId,
    limit: usize,
) -> Result<(JobDeletes, bool), String> {
    drive(row_calls::job_entry_deletes(job_id, limit), context)
        .await
        .map_err(|error| error.to_string())
}

/// One bounded page of a staging job's queue, manifest and lane rows; `true` while more remain.
pub(crate) async fn staging_row_deletes(
    context: &DriverContext,
    job_id: JobId,
    limit: usize,
) -> Result<(JobDeletes, bool), String> {
    drive(row_calls::staging_row_deletes(job_id, limit), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn find_dedup_job(
    context: &DriverContext,
    created_by: UserId,
    dedup_key: &[u8],
    _txn_id: Option<TxnId>,
) -> Result<Option<JobId>, String> {
    drive(row_calls::find_dedup_job(created_by, dedup_key), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn find_dedup_plan(
    context: &DriverContext,
    created_by: UserId,
    dedup_key: &[u8],
    _txn_id: Option<TxnId>,
) -> Result<Option<(JobId, [u8; 32])>, String> {
    drive(row_calls::find_dedup_plan(created_by, dedup_key), context)
        .await
        .map_err(|error| error.to_string())
}

/// Earliest `(timestamp, job_id)` under a schedule-index prefix.
pub async fn first_schedule_entry(
    context: &DriverContext,
    prefix: &[u8],
) -> Result<Option<(u64, JobId)>, String> {
    drive(row_calls::first_schedule_entry(prefix), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn insert_job(context: &DriverContext, record: &JobRecord) -> Result<(), String> {
    drive(row_calls::insert_job(record), context)
        .await
        .map_err(|error| error.to_string())
}

pub(super) async fn batch_write(
    context: &DriverContext,
    writes: JobWrites,
    _txn_id: Option<TxnId>,
) -> Result<(), String> {
    drive(RowBatch::write(Ok(writes)), context)
        .await
        .map_err(|error| error.to_string())
}

pub(crate) async fn batch_delete(
    context: &DriverContext,
    deletes: JobDeletes,
    _txn_id: Option<TxnId>,
) -> Result<(), String> {
    drive(row_calls::delete_rows(deletes), context)
        .await
        .map_err(|error| error.to_string())
}
