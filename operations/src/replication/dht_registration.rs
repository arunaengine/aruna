//! Builds the DHT put effect that announces this node as a holder of a blob hash.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{DhtEffect, Effect, NetEffect};
use aruna_core::errors::ConversionError;
use aruna_core::id::DhtKeyId;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::realm::RealmId;
use std::time::Duration;

pub(crate) fn dht_registration_effect(
    blake3: &[u8],
    local_realm_id: RealmId,
    limits: &RoCrateLimits,
) -> Result<Effect, ConversionError> {
    let key = DhtKeyId::from_bytes(blake3.try_into()?);

    Ok(Effect::Net(NetEffect::Dht(DhtEffect::Put {
        key,
        realm_id: local_realm_id,
        value: Vec::new(),
        ttl: Duration::from_millis(limits.holder_ttl_ms),
    })))
}
