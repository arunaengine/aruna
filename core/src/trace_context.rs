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
