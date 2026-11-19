//! Reads the host clocks and the OS random source for the runner, the backends and the inputs.
//! Core and operations never depend on this crate, so they cannot read a clock.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[cfg(any(target_os = "linux", target_os = "macos"))]
use nix::{sys::time::TimeValLike, time::ClockId};
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

use aruna_core::structured_id::clock_health::IdEnvironment;
use ulid::Ulid;

#[cfg(feature = "test-hooks")]
pub mod testing;

/// The 48 bits of a structured id's nonce.
const NONCE_MASK: u64 = (1 << 48) - 1;

/// The Unix time in seconds; 0 if the system time is before the Unix epoch.
#[inline]
pub fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The Unix time in milliseconds; 0 if the system time is before the Unix epoch.
#[inline]
pub fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The host wall clock, its suspend-aware monotonic clock and the OS random source.
#[derive(Debug, Clone, Copy)]
pub struct SystemEnvironment {
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    origin: Instant,
}

impl SystemEnvironment {
    pub fn new() -> Self {
        Self {
            #[cfg(not(any(target_os = "linux", target_os = "macos")))]
            origin: Instant::now(),
        }
    }
}

impl Default for SystemEnvironment {
    fn default() -> Self {
        Self::new()
    }
}

impl IdEnvironment for SystemEnvironment {
    fn now_ms(&self) -> u64 {
        unix_timestamp_millis()
    }

    fn monotonic_ms(&self) -> u64 {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            #[cfg(target_os = "linux")]
            let clock = ClockId::CLOCK_BOOTTIME;
            #[cfg(target_os = "macos")]
            let clock = ClockId::from_raw(nix::libc::CLOCK_MONOTONIC_RAW);

            clock
                .now()
                .expect("suspend-aware monotonic clock is available")
                .num_milliseconds() as u64
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            self.origin.elapsed().as_millis() as u64
        }
    }

    fn random_nonce(&self) -> u64 {
        (Ulid::generate().random() as u64) & NONCE_MASK
    }
}
