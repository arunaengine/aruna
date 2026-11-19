//! Counts the reads that hold kept copies, so eviction leaves a copy in use to the hidden sweep.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use aruna_core::stream::{BackendStream, StreamError};
use bytes::Bytes;
use futures::StreamExt;

type Blob = BackendStream<Result<Bytes, StreamError>>;
type Pins = Arc<Mutex<HashMap<String, usize>>>;

/// Kept copies being read on this node, by backend path, with their reader counts.
#[derive(Clone, Debug, Default)]
pub(super) struct ReadPins(Pins);

impl ReadPins {
    /// `blob` of the kept copy at `backend_path`, held until the stream ends or drops.
    pub(super) fn pin(&self, backend_path: String, blob: Blob) -> Blob {
        let mut pins = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *pins.entry(backend_path.clone()).or_default() += 1;
        drop(pins);
        let pin = ReadPin {
            pins: self.0.clone(),
            backend_path,
        };
        let held = blob.0.inspect(move |_| {
            let _ = &pin;
        });
        BackendStream(Box::pin(held))
    }

    /// Whether a read holds the kept copy at `backend_path`.
    pub(super) fn held(&self, backend_path: &str) -> bool {
        let pins = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pins.contains_key(backend_path)
    }
}

/// One read's hold on a kept copy.
struct ReadPin {
    pins: Pins,
    backend_path: String,
}

impl Drop for ReadPin {
    fn drop(&mut self) {
        let mut pins = self
            .pins
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(count) = pins.get_mut(&self.backend_path) {
            *count -= 1;
            if *count == 0 {
                pins.remove(&self.backend_path);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob() -> Blob {
        BackendStream(Box::pin(futures::stream::iter([Ok(Bytes::from_static(
            b"bytes",
        ))])))
    }

    // A copy is held while any of its reads streams, and free once the last one is dropped.
    #[test]
    fn held_until_dropped() {
        let pins = ReadPins::default();
        let first = pins.pin("copy".to_string(), blob());
        let second = pins.pin("copy".to_string(), blob());
        assert!(pins.held("copy"));
        assert!(!pins.held("other"));
        drop(first);
        assert!(pins.held("copy"));
        drop(second);
        assert!(!pins.held("copy"));
    }
}
