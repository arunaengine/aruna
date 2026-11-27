//! Answers workflow coordinator calls with the workflow runtime the executor registry holds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::workflow::{WorkflowAnswer, WorkflowEffect};

use crate::driver::DriverContext;

/// Runs `effect` on the node's workflow runtime; a node without one answers `Unavailable`.
pub(super) async fn dispatch_workflow(
    effect: WorkflowEffect,
    context: &DriverContext,
) -> WorkflowAnswer {
    let host = (context.compute_handle.as_ref()).and_then(|registry| registry.workflow());
    match host {
        Some(host) => host.serve(effect).await,
        None => effect.unavailable(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use aruna_compute::registry::ExecutorRegistry;
    use aruna_core::workflow::{WorkflowError, WorkflowHost};
    use aruna_storage::storage::memory::MemoryStorage;

    use super::*;

    struct Ended;

    #[async_trait::async_trait]
    impl WorkflowHost for Ended {
        async fn serve(&self, _effect: WorkflowEffect) -> WorkflowAnswer {
            WorkflowAnswer::Received(Ok(None))
        }
    }

    fn receive() -> WorkflowEffect {
        WorkflowEffect::Receive {
            session: "run".into(),
        }
    }

    #[tokio::test]
    async fn answers_without_runtime() {
        let mut context = crate::testing::test_context(MemoryStorage::open());
        let answer = dispatch_workflow(receive(), &context).await;
        let refused = WorkflowAnswer::Received(Err(WorkflowError::Unavailable));
        assert_eq!(answer, refused);
        let registry = ExecutorRegistry::new().with_workflow(Arc::new(Ended));
        context.compute_handle = Some(Arc::new(registry));
        let answer = dispatch_workflow(receive(), &context).await;
        assert_eq!(answer, WorkflowAnswer::Received(Ok(None)));
    }
}
