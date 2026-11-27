//! Streams the events of a workflow run as server-sent events: the stored events after the
//! client's last one, then each new one as the run stores it, until the run ended.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::structs::execution::workflow_run::RunEventRecord;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_ops_work::workflows::runs::events::{EventBatch, ReadEventsOperation};
use aruna_runner::driver::{DriverContext, drive};
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_core::Stream;
use futures_util::stream;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::RecvError;
use ulid::Ulid;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::catalog_error;
use super::run_wire::RunEventsQuery;
use super::runs::run_id;
use crate::auth::RealmAuth;
use crate::error::{ErrorResponse, ServerResult};
use crate::server::state::ServerState;

/// Events one read of the stream takes.
const READ_EVENTS: usize = 256;
/// How long a stream waits for a wake before it reads again anyway.
const WAKE_FALLBACK: Duration = Duration::from_secs(5);
/// How often an idle stream sends a comment, so proxies keep it open.
const KEEP_ALIVE: Duration = Duration::from_secs(20);

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new().routes(routes!(run_events))
}

/// The newest sequence the client saw: the larger of `?after=` and `Last-Event-ID`.
fn last_seen(query: &RunEventsQuery, headers: &HeaderMap) -> u64 {
    let header = (headers.get("last-event-id"))
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse().ok());
    query.after.unwrap_or(0).max(header.unwrap_or(0))
}

fn sse_event(row: &RunEventRecord) -> Event {
    Event::default()
        .id(row.seq.to_string())
        .event(row.event_type.as_str())
        .data(row.payload_json.as_str())
}

/// One open stream: the rows read and not yet sent, and whether the run's events are all read.
struct Follow {
    context: Arc<DriverContext>,
    auth: AuthContext,
    run_id: Ulid,
    after: u64,
    pending: VecDeque<RunEventRecord>,
    more: bool,
    done: bool,
    wakes: Option<Receiver<String>>,
}

impl Follow {
    fn take(&mut self, batch: EventBatch) {
        self.more = batch.more;
        self.done = batch.record.state.is_terminal() && !batch.more;
        if let Some(last) = batch.events.last() {
            self.after = last.seq;
        }
        self.pending.extend(batch.events);
    }

    /// Waits until the run stored new events, or the fallback passed.
    async fn wait(&mut self) {
        let run = self.run_id.to_string();
        let Some(wakes) = self.wakes.as_mut() else {
            tokio::time::sleep(WAKE_FALLBACK).await;
            return;
        };
        let deadline = tokio::time::sleep(WAKE_FALLBACK);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                () = &mut deadline => return,
                wake = wakes.recv() => match wake {
                    Ok(woken) if woken == run => return,
                    Ok(_) => {}
                    Err(RecvError::Lagged(_)) => return,
                    Err(RecvError::Closed) => {
                        self.wakes = None;
                        return;
                    }
                },
            }
        }
    }

    async fn next(&mut self) -> Option<Event> {
        loop {
            if let Some(row) = self.pending.pop_front() {
                return Some(sse_event(&row));
            }
            if self.done {
                return None;
            }
            if !self.more {
                self.wait().await;
            }
            let read = ReadEventsOperation::new(&self.auth, self.run_id, self.after, READ_EVENTS);
            // A run that was deleted, or that the caller may no longer read, ends the stream.
            let batch = drive(read, &self.context).await.ok()?;
            self.take(batch);
        }
    }
}

#[utoipa::path(
    get,
    path = "/compute/workflow-runs/{run_id}/events",
    tag = "compute/workflow-runs",
    summary = "Follow the events of a workflow run",
    description = r#"Streams the events of a run as `text/event-stream`.

**Authentication**: realm bearer token.

**Behavior**
- Each frame has `id: <seq>`, `event: run|state|step|task` and one `data:` line of JSON that
  always names the `run_id`. `run` and `state` frames carry fields of the run, `step` frames a
  step summary and `task` frames a task, as the other run routes return them.
- `?after=<seq>` or the `Last-Event-ID` header continue after that event; the larger wins. The
  stored events after it are sent first, then each new one as the run stores it.
- The stream ends cleanly once the run ended and every event was sent; a client reconnects only
  while the run is active.
- An idle stream sends a comment every 20 s."#,
    params(
        ("run_id" = String, Path, description = "Run id, as a 26-character ULID"),
        ("Last-Event-ID" = Option<String>, Header, description = "The last sequence seen"),
        RunEventsQuery
    ),
    responses(
        (status = 200, description = "The event stream of the run", body = String, content_type = "text/event-stream"),
        (status = 404, description = "Unknown, or not visible to the caller", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn run_events(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(run): Path<String>,
    Query(query): Query<RunEventsQuery>,
    headers: HeaderMap,
) -> ServerResult<Sse<impl Stream<Item = Result<Event, Infallible>> + Send>> {
    let run_id = run_id(&run)?;
    let after = last_seen(&query, &headers);
    let context = state.get_ctx();
    let wakes = (context.compute_handle.as_ref()).map(|registry| registry.run_wakes());
    let read = ReadEventsOperation::new(&auth, run_id, after, READ_EVENTS);
    let batch = drive(read, &context).await.map_err(catalog_error)?;
    let mut follow = Follow {
        context,
        auth,
        run_id,
        after,
        pending: VecDeque::new(),
        more: false,
        done: false,
        wakes,
    };
    follow.take(batch);
    let events = stream::unfold(follow, |mut follow| async move {
        let event = follow.next().await?;
        Some((Ok(event), follow))
    });
    Ok(Sse::new(events).keep_alive(KeepAlive::new().interval(KEEP_ALIVE)))
}
