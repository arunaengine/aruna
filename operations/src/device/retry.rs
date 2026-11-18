//! Shared retry math for the two device queues; their stored state enums stay distinct.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::time::unix_timestamp_millis;

use super::publish_queue::{PublishEntry, PublishState};
use super::sync::repository::{SyncUpload, UploadState};
use crate::tasks::queue_backoff::retry_delay_ms;

/// The retry view both device queue states expose.
pub(crate) trait RetryView {
    /// When the entry may be forwarded again; `None` while it is terminal.
    fn due_at_ms(&self) -> Option<u64>;
    /// Forwards already spent; zero while the entry is terminal.
    fn attempts(&self) -> u32;

    /// Whether the drain may pick the entry up now.
    fn is_due(&self, now_ms: u64) -> bool {
        self.due_at_ms()
            .is_some_and(|due_at_ms| due_at_ms <= now_ms)
    }
}

impl RetryView for PublishState {
    fn due_at_ms(&self) -> Option<u64> {
        match self {
            Self::Pending { due_at_ms, .. } | Self::Publishing { due_at_ms, .. } => {
                Some(*due_at_ms)
            }
            Self::Published { .. } | Self::Failed { .. } => None,
        }
    }

    fn attempts(&self) -> u32 {
        match self {
            Self::Pending { attempts, .. } | Self::Publishing { attempts, .. } => *attempts,
            Self::Published { .. } | Self::Failed { .. } => 0,
        }
    }
}

impl RetryView for UploadState {
    fn due_at_ms(&self) -> Option<u64> {
        match self {
            Self::Pending { due_at_ms, .. } => Some(*due_at_ms),
            Self::Transferring { .. } => Some(0),
            Self::Failed { .. } => None,
        }
    }

    fn attempts(&self) -> u32 {
        match self {
            Self::Pending { attempts, .. } | Self::Transferring { attempts, .. } => *attempts,
            Self::Failed { .. } => 0,
        }
    }
}

impl RetryView for PublishEntry {
    fn due_at_ms(&self) -> Option<u64> {
        self.state.due_at_ms()
    }

    fn attempts(&self) -> u32 {
        self.state.attempts()
    }
}

impl RetryView for SyncUpload {
    fn due_at_ms(&self) -> Option<u64> {
        self.state.due_at_ms()
    }

    fn attempts(&self) -> u32 {
        self.state.attempts()
    }
}

/// Due time for the next attempt after `attempts` failures.
pub(crate) fn retry_due_ms(attempts: u32) -> u64 {
    retry_due_at(unix_timestamp_millis(), attempts)
}

/// Due time for the next attempt after `attempts` failures, counted from `now_ms`.
pub(crate) fn retry_due_at(now_ms: u64, attempts: u32) -> u64 {
    now_ms.saturating_add(retry_delay_ms(attempts))
}

/// Whether `attempts` spent exhausts `max_attempts` and parks the row.
pub(crate) fn exhausted(attempts: u32, max_attempts: u32) -> bool {
    attempts >= max_attempts
}
