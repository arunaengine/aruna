//! Declares trigger kinds, what each one needs, and the fields a trigger may carry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};

use crate::permission::Permission;

/// Timers are coarse schedules, not sleep loops.
pub const MIN_INTERVAL: u64 = 60;
pub const MAX_INTERVAL: u64 = 366 * 24 * 3600;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerKind {
    Manual,
    Every,
    Webhook,
    DatasetCreated,
    DataUploaded,
    SyncCompleted,
    SyncFailed,
    DatasetUpdated,
}

impl TriggerKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Every => "every",
            Self::Webhook => "webhook",
            Self::DatasetCreated => "dataset_created",
            Self::DataUploaded => "data_uploaded",
            Self::SyncCompleted => "sync_completed",
            Self::SyncFailed => "sync_failed",
            Self::DatasetUpdated => "dataset_updated",
        }
    }

    /// The read permission an event trigger needs before any event is delivered.
    pub fn needs(self) -> Option<Permission> {
        match self {
            Self::DatasetCreated | Self::DatasetUpdated => Some(Permission::MetadataRead),
            Self::DataUploaded | Self::SyncCompleted | Self::SyncFailed => {
                Some(Permission::ObjectsRead)
            }
            Self::Manual | Self::Every | Self::Webhook => None,
        }
    }
}

/// What a manual run asks the person for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ManualInput {
    Dataset,
}

/// One trigger. Which fields apply depends on its kind.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Trigger {
    /// Defaults to the handler named like the trigger, else the only handler.
    #[serde(default)]
    pub handler: Option<String>,
    #[serde(default)]
    pub input: Option<ManualInput>,
    /// `every` only: a whole number followed by `m`, `h` or `d`.
    #[serde(default)]
    pub interval: Option<String>,
    /// `webhook` only: the secret slot that checks each request.
    #[serde(default)]
    pub verify: Option<String>,
    /// `webhook` only: the lowercase header carrying the provider's delivery id.
    #[serde(default)]
    pub delivery_id: Option<String>,
}

impl Trigger {
    /// The timer interval in seconds, when it is a valid interval.
    pub fn interval_secs(&self) -> Option<u64> {
        let text = self.interval.as_deref()?;
        let unit = match text.chars().last()? {
            'm' => 60,
            'h' => 3600,
            'd' => 86_400,
            _ => return None,
        };
        let count: u64 = text[..text.len() - 1].parse().ok()?;
        let secs = count.checked_mul(unit)?;
        (MIN_INTERVAL..=MAX_INTERVAL)
            .contains(&secs)
            .then_some(secs)
    }
}
