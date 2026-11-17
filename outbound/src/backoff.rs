//! Holds the in-memory delays of deliveries while no management node opens a setting secret.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use aruna_core::UserId;
use aruna_core::outbound::{BackoffChange, BackoffRequest, StoredBackoff};
use aruna_core::structs::execution::setting::SettingChannel;

/// Delays per owner and channel. They start empty and are lost on restart, as before.
#[derive(Debug, Default)]
pub struct Backoffs(Mutex<HashMap<(UserId, SettingChannel), StoredBackoff>>);

impl Backoffs {
    /// Applies `request` and returns the delay stored after it.
    pub(crate) fn apply(&self, request: BackoffRequest) -> Option<StoredBackoff> {
        let key = (request.owner, request.channel);
        let mut map = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        match request.change {
            BackoffChange::Read => {}
            BackoffChange::Set { until_ms, delay_ms } => {
                map.insert(key, StoredBackoff { until_ms, delay_ms });
            }
            BackoffChange::Clear => {
                map.remove(&key);
            }
        }
        map.get(&key).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;

    fn user(seed: u8) -> UserId {
        UserId::local(
            ulid::Ulid::from_bytes([seed; 16]),
            RealmId::from_bytes([1; 32]),
        )
    }

    fn request(owner: UserId, channel: SettingChannel, change: BackoffChange) -> BackoffRequest {
        BackoffRequest {
            owner,
            channel,
            change,
        }
    }

    /// Every notice of one owner and channel shares the delay; others keep their own.
    #[test]
    fn delay_per_owner() {
        let backoffs = Backoffs::default();
        let set = BackoffChange::Set {
            until_ms: 31_000,
            delay_ms: 30_000,
        };
        let stored = StoredBackoff {
            until_ms: 31_000,
            delay_ms: 30_000,
        };
        let email = SettingChannel::Email;
        assert_eq!(backoffs.apply(request(user(6), email, set)), Some(stored));
        let read = BackoffChange::Read;
        assert_eq!(backoffs.apply(request(user(6), email, read)), Some(stored));
        assert_eq!(backoffs.apply(request(user(7), email, read)), None);
        let webhook = SettingChannel::Webhook;
        assert_eq!(backoffs.apply(request(user(6), webhook, read)), None);
        let clear = BackoffChange::Clear;
        assert_eq!(backoffs.apply(request(user(6), email, clear)), None);
        assert_eq!(backoffs.apply(request(user(6), email, read)), None);
    }
}
