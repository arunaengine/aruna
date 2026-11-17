//! Carries out one session request on the live session of a job, held by the compute registry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::session::{SessionAnswer, SessionEffect, SessionEvent, SessionRequest};
use aruna_core::stream::BackendStream;
use futures_util::stream;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::RecvError;

use crate::driver::DriverContext;

pub(super) async fn dispatch_session(
    effect: SessionEffect,
    context: &DriverContext,
) -> SessionAnswer {
    let session = (context.compute_handle.as_ref())
        .and_then(|registry| registry.sessions().get(&effect.job_id));
    let Some(session) = session else {
        return SessionAnswer::Missing;
    };
    let done = |result: Result<(), _>| match result {
        Ok(()) => SessionAnswer::Done,
        Err(error) => SessionAnswer::Refused(error),
    };
    let body = |result: Result<_, _>| match result {
        Ok(body) => SessionAnswer::Body(body),
        Err(error) => SessionAnswer::Refused(error),
    };
    match effect.request {
        SessionRequest::Snapshot => SessionAnswer::Snapshot(Box::new(session.snapshot())),
        SessionRequest::Subscribe { after: Some(after) } => match session.subscribe(after) {
            Ok((backlog, receiver)) => streamed(backlog, receiver),
            Err(_) => SessionAnswer::Gap,
        },
        SessionRequest::Subscribe { after: None } => {
            let (backlog, receiver) = session.subscribe_all();
            streamed(backlog, receiver)
        }
        SessionRequest::SubmitCell { cell_id, code } => {
            match session.submit_cell(&cell_id, &code) {
                Ok(position) => SessionAnswer::Position(position),
                Err(error) => SessionAnswer::Refused(error),
            }
        }
        SessionRequest::Interrupt => done(session.interrupt()),
        SessionRequest::End(reason) => {
            session.end(reason);
            SessionAnswer::Done
        }
        SessionRequest::RecordInput(input) => {
            session.record_input(input);
            SessionAnswer::Done
        }
        SessionRequest::RecordPending(input) => {
            session.record_pending(input);
            SessionAnswer::Done
        }
        SessionRequest::RecordTouched(object) => {
            session.record_touched(object);
            SessionAnswer::Done
        }
        SessionRequest::Touch => {
            session.touch();
            SessionAnswer::Done
        }
        SessionRequest::CellOutputs { cell_id, after } => {
            let (last_event_id, outputs) = session.cell_outputs(&cell_id, after);
            SessionAnswer::Outputs {
                last_event_id,
                outputs,
            }
        }
        SessionRequest::ListScratch { path } => body(session.list_scratch(&path).await),
        SessionRequest::ReadScratch { path, limit } => {
            body(session.read_scratch(&path, 0, limit).await)
        }
    }
}

/// The replayed frames and the live tail. A reader that fell behind skips what it lost;
/// the frame ids tell it the range. The tail ends when the session closes.
fn streamed(backlog: Vec<SessionEvent>, receiver: Receiver<SessionEvent>) -> SessionAnswer {
    let live = stream::unfold(receiver, |mut receiver| async move {
        loop {
            match receiver.recv().await {
                Ok(event) => return Some((event, receiver)),
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    });
    SessionAnswer::Stream {
        backlog,
        live: BackendStream(Box::pin(live)),
    }
}
