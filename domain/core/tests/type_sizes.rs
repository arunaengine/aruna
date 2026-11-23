//! Fixes upper limits on the sizes of the effect and event types that every step moves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::mem::size_of;

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::types::Effects;

// Tightened after boxing large Blob/StagingSource and metadata outlier variants
// and shrinking Effects inline capacity from 4 to 2. Raising a limit needs approval.
const EFFECT_LIMIT: usize = 160;
const EVENT_LIMIT: usize = 160;
const EFFECTS_LIMIT: usize = 352;

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
