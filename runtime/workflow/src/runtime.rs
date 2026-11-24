//! The workflow runtime of a node: preflight requests go to the warm pool, run and report
//! sessions get a coordinator attempt of their own, addressed by the session name.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use aruna_compute::executor::ExecutorBackend;
use aruna_core::workflow::{WorkflowAnswer, WorkflowEffect, WorkflowError, WorkflowHost};
use aruna_workflow_contract::{CoordinatorMode, HostToCoordinator};
use async_trait::async_trait;

use crate::channel::{Connection, FrameReader, FrameWriter};
use crate::pool::PreflightPool;
use crate::settings::WorkflowSettings;
use crate::transport::{ContainerTransport, CoordinatorTransport, LocalBinary};

/// An open run or report session: each half of its channel locks on its own, so a send never
/// waits for a pending receive. The guard ends the attempt with the last handle.
struct Session {
    reader: tokio::sync::Mutex<FrameReader>,
    writer: tokio::sync::Mutex<FrameWriter>,
    _guard: Mutex<Box<dyn Send>>,
}

/// Answers `Effect::Workflow` for the runner.
pub struct WorkflowRuntime {
    pool: PreflightPool,
    transport: Arc<dyn CoordinatorTransport>,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    timeout: Duration,
}

impl WorkflowRuntime {
    pub fn new(transport: Arc<dyn CoordinatorTransport>, settings: &WorkflowSettings) -> Self {
        let pool = PreflightPool::new(
            transport.clone(),
            settings.preflight_workers,
            settings.request_timeout,
            settings.recycle_after,
        );
        Self {
            pool,
            transport,
            sessions: Mutex::new(HashMap::new()),
            timeout: settings.request_timeout,
        }
    }

    /// The runtime the settings name: a local binary when one is set, else the image on
    /// `backend`. `None` when the settings name neither or no backend runs the image.
    pub fn configured(
        settings: &WorkflowSettings,
        backend: Option<Arc<dyn ExecutorBackend>>,
    ) -> Option<Self> {
        let transport: Arc<dyn CoordinatorTransport> = match (&settings.binary, &settings.image) {
            (Some(path), _) => Arc::new(LocalBinary::new(path.clone())),
            (None, Some(image)) => Arc::new(ContainerTransport::new(
                backend?,
                image.clone(),
                settings.limits,
            )),
            (None, None) => return None,
        };
        Some(Self::new(transport, settings))
    }

    pub fn pool(&self) -> &PreflightPool {
        &self.pool
    }

    async fn open(
        &self,
        session: String,
        mode: CoordinatorMode,
        (run_id, digest): (String, String),
    ) -> Result<(), WorkflowError> {
        let started = async {
            let mut connection = Connection::new(self.transport.start().await?);
            connection.greet(mode, &run_id, &digest).await?;
            Ok(connection)
        };
        let connection = tokio::time::timeout(self.timeout, started)
            .await
            .map_err(|_| WorkflowError::Timeout {
                seconds: self.timeout.as_secs(),
            })??;
        let (reader, writer, guard) = connection.into_parts();
        let open = Session {
            reader: tokio::sync::Mutex::new(reader),
            writer: tokio::sync::Mutex::new(writer),
            _guard: Mutex::new(guard),
        };
        let replaced = self.sessions().insert(session.clone(), Arc::new(open));
        if replaced.is_some() {
            tracing::warn!(%session, "a coordinator session was opened again; the old one ends");
        }
        Ok(())
    }

    fn session(&self, session: &str) -> Result<Arc<Session>, WorkflowError> {
        (self.sessions().get(session).cloned())
            .ok_or_else(|| WorkflowError::UnknownSession(session.to_owned()))
    }

    fn sessions(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<Session>>> {
        self.sessions.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[async_trait]
impl WorkflowHost for WorkflowRuntime {
    async fn serve(&self, effect: WorkflowEffect) -> WorkflowAnswer {
        match effect {
            WorkflowEffect::Preflight(kind) => {
                WorkflowAnswer::Preflight(self.pool.request(kind).await)
            }
            WorkflowEffect::Open {
                session,
                mode,
                run_id,
                claim_token_digest,
            } => {
                WorkflowAnswer::Opened(self.open(session, mode, (run_id, claim_token_digest)).await)
            }
            WorkflowEffect::Send { session, message } => {
                let sent = match self.session(&session) {
                    Ok(open) => open.writer.lock().await.send(&message).await,
                    Err(error) => Err(error),
                };
                WorkflowAnswer::Sent(sent)
            }
            WorkflowEffect::Receive { session } => {
                let received = match self.session(&session) {
                    Ok(open) => open.reader.lock().await.receive().await,
                    Err(error) => Err(error),
                };
                WorkflowAnswer::Received(received)
            }
            WorkflowEffect::Close { session } => {
                let Some(open) = self.sessions().remove(&session) else {
                    return WorkflowAnswer::Closed(Err(WorkflowError::UnknownSession(session)));
                };
                // The attempt ends when its last handle drops, after the shutdown frame.
                let closed = open
                    .writer
                    .lock()
                    .await
                    .send(&HostToCoordinator::Shutdown)
                    .await;
                WorkflowAnswer::Closed(closed.or(Ok(())))
            }
        }
    }
}
