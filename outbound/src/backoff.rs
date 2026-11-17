//! Holds the in-memory delays of deliveries while no management node opens a setting secret.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use aruna_core::UserId;
use aruna_core::structs::execution::setting::SettingChannel;

/// Delays per owner and channel. They start empty and are lost on restart, as before.
#[derive(Debug, Default)]
pub struct Backoffs(Mutex<HashMap<(UserId, SettingChannel), Backoff>>);

#[derive(Clone, Copy, Debug)]
struct Backoff {
    until_ms: u64,
    delay_ms: u64,
}

impl Backoffs {
    /// The end of a running backoff.
    pub fn until(&self, owner: UserId, channel: SettingChannel, now_ms: u64) -> Option<u64> {
        let map = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let until = map.get(&(owner, channel)).map(|backoff| backoff.until_ms);
        until.filter(|until| *until > now_ms)
    }

    /// Records a failed attempt with the delay `next(previous delay)` and returns its end.
    pub fn fail(
        &self,
        owner: UserId,
        channel: SettingChannel,
        now_ms: u64,
        next: impl FnOnce(Option<u64>) -> u64,
    ) -> u64 {
        let mut map = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let delay_ms = next(map.get(&(owner, channel)).map(|backoff| backoff.delay_ms));
        let until_ms = now_ms.saturating_add(delay_ms);
        map.insert((owner, channel), Backoff { until_ms, delay_ms });
        until_ms
    }

    pub fn clear(&self, owner: UserId, channel: SettingChannel) {
        let mut map = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        map.remove(&(owner, channel));
    }
}
