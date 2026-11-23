//! Defines the clock skew bound, the clock health errors and the time and entropy source trait
//! of id minting; the host clocks that implement it live in `aruna-clock`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use thiserror::Error;

/// Realm skew bound default of five minutes (REQ-META-ID-TIME-001).
pub const MAX_ID_SKEW: u64 = 300_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ClockHealthError {
    #[error("wall clock jumped forward {jump_ms} ms, beyond max_id_clock_skew_ms {max_skew_ms}")]
    ForwardJump { jump_ms: u64, max_skew_ms: u64 },
    #[error("timestamp {timestamp_ms} ms exceeds the 48-bit id timestamp range")]
    TimestampOverflow { timestamp_ms: u64 },
}

/// Injectable time and entropy source. `aruna_clock::SystemEnvironment` reads the host clocks
/// and the OS random source; tests supply deterministic values.
pub trait IdEnvironment {
    /// Validated wall-clock time as Unix milliseconds.
    fn now_ms(&self) -> u64;
    /// Milliseconds from a suspend-aware monotonic clock with an arbitrary but
    /// fixed origin.
    fn monotonic_ms(&self) -> u64;
    /// A fresh 48-bit nonce drawn from a cryptographically secure source.
    fn random_nonce(&self) -> u64;
}
