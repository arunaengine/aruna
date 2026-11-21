//! Fixes upper limits on the sizes of the effect and event types that every step moves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::mem::size_of;

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::types::Effects;

// Measured on 1feeab85c with the workspace toolchain. A change may lower a limit;
// raising one needs the owner's approval.
const EFFECT_LIMIT: usize = 416;
const EVENT_LIMIT: usize = 384;
const EFFECTS_LIMIT: usize = 1696;

#[test]
fn sizes_stay_bounded() {
    let sizes = [
        ("Effect", size_of::<Effect>(), EFFECT_LIMIT),
        ("Event", size_of::<Event>(), EVENT_LIMIT),
        ("Effects", size_of::<Effects>(), EFFECTS_LIMIT),
    ];
    for (name, size, limit) in sizes {
        assert!(
            size <= limit,
            "{name} is {size} bytes, above its limit of {limit}"
        );
    }
}
