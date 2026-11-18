//! Names the page size of waking key waits, and holds the key wait tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub use super::row_calls::WAKE_PAGE;

#[cfg(test)]
#[path = "key_wait_tests.rs"]
mod tests;
