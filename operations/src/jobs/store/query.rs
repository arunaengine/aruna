//! Lists and pages stored job rows and runs the shared storage transaction helpers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::row_calls;
use super::*;
use crate::driver::drive;


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

pub async fn insert_job(context: &DriverContext, record: &JobRecord) -> Result<(), String> {
    drive(row_calls::insert_job(record), context)
        .await
        .map_err(|error| error.to_string())
}
