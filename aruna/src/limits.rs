//! Raises the open file soft limit to the hard limit at process start.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

/// Containers often start with a soft limit of 1024, which fjall, tantivy and the
/// listeners exhaust. Raises it to the hard limit, never lowers it, and never fails.
#[cfg(unix)]
pub fn raise_open_files() {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};

    let limit = getrlimit(Resource::Nofile);
    let target = soft_target(limit.maximum);
    // `None` is unlimited.
    if target.unwrap_or(u64::MAX) > limit.current.unwrap_or(u64::MAX) {
        let raised = Rlimit {
            current: target,
            maximum: limit.maximum,
        };
        if let Err(error) = setrlimit(Resource::Nofile, raised) {
            tracing::warn!(old = ?limit.current, ?target, %error, "Could not raise the open file limit");
            return;
        }
    }
    let new = getrlimit(Resource::Nofile).current;
    tracing::info!(old = ?limit.current, ?new, "Open file soft limit");
}

/// Windows has no `RLIMIT_NOFILE`.
#[cfg(not(unix))]
pub fn raise_open_files() {}

/// macOS setrlimit(2) rejects a soft limit above `OPEN_MAX` and advises
/// `min(OPEN_MAX, rlim_max)`.
#[cfg(unix)]
fn soft_target(maximum: Option<u64>) -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        const OPEN_MAX: u64 = 10_240;
        Some(maximum.map_or(OPEN_MAX, |hard| hard.min(OPEN_MAX)))
    }
    #[cfg(not(target_os = "macos"))]
    {
        maximum
    }
}
