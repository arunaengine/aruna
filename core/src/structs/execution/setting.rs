//! Defines the replicated notification settings of a user and how the current one is chosen.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};

/// The channel a notification setting turns on or off.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SettingChannel {
    Email,
    Webhook,
}

impl SettingChannel {
    /// Bound into sealed secrets and tentative row keys, so the channels never mix.
    pub const fn tag(self) -> u8 {
        match self {
            Self::Email => 1,
            Self::Webhook => 2,
        }
    }
}
