//! Lifecycle state behind the operational probes.
//!
//! Kubernetes only stops routing new requests to a pod once its readiness probe
//! fails, so shutdown has to flip this gate before it closes any listener. The
//! liveness probe keeps answering for the whole drain: it dies with the process.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Startup and drain gate for readiness.
#[derive(Clone, Debug, Default)]
pub struct Readiness {
    started: Arc<AtomicBool>,
    draining: Arc<AtomicBool>,
}

impl Readiness {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bootstrap finished and the request listeners are bound.
    pub fn set_ready(&self) {
        self.started.store(true, Ordering::SeqCst);
    }

    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// Shutdown started: fail readiness so the load balancer drops this node
    /// before in-flight work is drained.
    pub fn begin_drain(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    /// What `/readyz` reports for the node's own lifecycle.
    pub fn is_ready(&self) -> bool {
        self.is_started() && !self.is_draining()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_only_after_startup() {
        let readiness = Readiness::new();
        assert!(!readiness.is_ready());

        readiness.set_ready();
        assert!(readiness.is_ready());
    }

    #[test]
    fn draining_fails_readiness() {
        let readiness = Readiness::new();
        readiness.set_ready();

        readiness.begin_drain();

        assert!(!readiness.is_ready());
        assert!(readiness.is_started());
        assert!(readiness.is_draining());
    }
}
