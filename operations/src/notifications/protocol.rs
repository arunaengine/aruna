//! Bounds the watch event batches, scans and retries of the notification transport.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub const EVENT_BATCH_SIZE: usize = 128;
pub const SUBSCRIPTION_SCAN_CAP: usize = 1024;
pub const DIRTY_REALM_CAP: usize = 4;
pub const RETRY_BATCH_CAP: usize = 4;
pub const RETRY_BYTES_CAP: usize = 1024 * 1024;
pub const EXPANSION_WORK_CAP: usize = 4096;
pub const EXPANSION_CANDIDATE_CAP: usize = 1024;
pub const EXPANSION_RECORD_CAP: usize = 1024;
