//! Holds the traceparent and tracestate strings that carry a trace between nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DistributedTraceContext {
    pub traceparent: String,
    pub tracestate: Option<String>,
}

impl DistributedTraceContext {
    pub fn new(traceparent: String, tracestate: Option<String>) -> Self {
        Self {
            traceparent,
            tracestate,
        }
    }
}
