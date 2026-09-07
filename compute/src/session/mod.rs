//! Interactive sessions this node runs, in memory only. A session owns the
//! channel to its helper, the event log a client resumes from, and its idle
//! timer.

pub mod events;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io;
use std::sync::{Arc, Mutex};

use aruna_core::compute::FenceContext;
use aruna_core::compute::session::{MAX_TOUCHED_OBJECTS, MAX_TRACKED_INPUTS, TRUNCATED_NOTICE};
use serde::Serialize;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Notify, broadcast, mpsc, oneshot};
use tokio::time::{Duration, Instant, timeout};
use tokio_util::sync::CancellationToken;

use crate::executor::{ExecutorBackend, now_ms};
use events::{BudgetVerdict, CellBudget, EventRing, SessionEvent};

pub use aruna_core::compute::session::{
    CellPhase, EndReason, EventKind, HelperEvent, HelperRequest, MAX_CELL_CODE_BYTES,
    MAX_CELL_ID_LEN, MAX_QUEUED_CELLS, MAX_RING_EVENTS, MAX_SCRATCH_READ_BYTES, MAX_SUBMITS,
    MAX_TRACKED_CELLS, SUBMIT_WINDOW, SessionError, SessionPhase,
};

/// Bytes one helper line may carry before the session is torn down.
const MAX_HELPER_LINE_BYTES: usize = 16 * 1024 * 1024;
/// How long the node waits for a helper reply to a scratch or status request.
const REPLY_TIMEOUT: Duration = Duration::from_secs(30);
/// How long the node keeps retrying to reach the helper of a starting attempt.
const OPEN_DEADLINE: Duration = Duration::from_secs(600);
/// Wait between two attempts to open the channel.
const OPEN_RETRY: Duration = Duration::from_secs(2);
/// Frames one slow reader may fall behind before it is told about a gap.
const BROADCAST_DEPTH: usize = 512;

/// One `cell` frame. Absent optionals are omitted, never sent as null.
#[derive(Serialize)]
struct CellFrame<'a> {
    cell_id: &'a str,
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    started_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    finished_at_ms: Option<u64>,
}

/// One `session` frame: the state object without its cells.
#[derive(Serialize)]
struct SessionFrame<'a> {
    job_id: &'a str,
    state: &'static str,
    runtime: &'a str,
    workspace_bucket: &'a str,
    executor_node_id: &'a str,
    started_at_ms: u64,
    idle_after_ms: u64,
    idle_deadline_ms: u64,
    credential_expires_at_ms: u64,
    last_event_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    ended: Option<EndedFrame>,
}

#[derive(Serialize)]
struct EndedFrame {
    reason: &'static str,
}

/// One object staged into the workspace bucket after the session started.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StagedInput {
    pub dest_key: String,
    pub bytes: u64,
    pub blake3: String,
    pub source_node_id: String,
    pub version_id: String,
}

/// One object the session's own credential read or wrote, as the S3 plane
/// attributed it to this job.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TouchedObject {
    pub bucket: String,
    pub key: String,
    /// `read` or `write`.
    pub operation: String,
}

/// One cell as the client sees it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CellSnapshot {
    pub cell_id: String,
    pub state: CellPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
}

/// The whole session as the client sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionSnapshot {
    pub job_id: String,
    pub state: SessionPhase,
    pub runtime: String,
    pub workspace_bucket: String,
    pub executor_node_id: String,
    pub started_at_ms: u64,
    pub idle_after_ms: u64,
    pub idle_deadline_ms: u64,
    pub credential_expires_at_ms: u64,
    pub last_event_id: u64,
    pub cells: Vec<CellSnapshot>,
    pub ended: Option<EndReason>,
}

/// What one session is opened with.
#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub job_id: String,
    pub public_job_id: String,
    pub runtime: String,
    pub workspace_bucket: String,
    pub executor_node_id: String,
    pub idle_after_ms: u64,
    pub credential_expires_at_ms: u64,
}

struct CellRecord {
    state: CellPhase,
    execution_count: Option<u32>,
    started_at_ms: Option<u64>,
    finished_at_ms: Option<u64>,
    budget: CellBudget,
}

struct Inner {
    phase: SessionPhase,
    ended: Option<EndReason>,
    ring: EventRing,
    order: VecDeque<String>,
    cells: BTreeMap<String, CellRecord>,
    queue: VecDeque<String>,
    submits: VecDeque<Instant>,
    idle_deadline: Instant,
    credential_expires_at_ms: u64,
    next_request: u64,
    inventory: Vec<StagedInput>,
    touched: Vec<TouchedObject>,
}

/// One live session. Everything about it is node local and lost on restart.
pub struct Session {
    config: SessionConfig,
    started_at_ms: u64,
    inner: Mutex<Inner>,
    events: broadcast::Sender<SessionEvent>,
    requests: mpsc::Sender<HelperRequest>,
    replies: Mutex<HashMap<u64, oneshot::Sender<Map<String, Value>>>>,
    bumped: Notify,
    done: CancellationToken,
}

impl Session {
    pub fn job_id(&self) -> &str {
        &self.config.job_id
    }

    /// Resolves once the session has stopped, with the reason it stopped for.
    pub async fn finished(&self) -> EndReason {
        self.done.cancelled().await;
        self.inner
            .lock()
            .map(|inner| inner.ended)
            .ok()
            .flatten()
            .unwrap_or(EndReason::Ended)
    }

    pub fn snapshot(&self) -> SessionSnapshot {
        let inner = self.lock();
        let cells = inner
            .order
            .iter()
            .filter_map(|cell_id| {
                let record = inner.cells.get(cell_id)?;
                Some(CellSnapshot {
                    cell_id: cell_id.clone(),
                    state: record.state,
                    execution_count: record.execution_count,
                    started_at_ms: record.started_at_ms,
                    finished_at_ms: record.finished_at_ms,
                })
            })
            .collect();
        SessionSnapshot {
            job_id: self.config.public_job_id.clone(),
            state: inner.phase,
            runtime: self.config.runtime.clone(),
            workspace_bucket: self.config.workspace_bucket.clone(),
            executor_node_id: self.config.executor_node_id.clone(),
            started_at_ms: self.started_at_ms,
            idle_after_ms: self.config.idle_after_ms,
            idle_deadline_ms: deadline_ms(inner.idle_deadline),
            credential_expires_at_ms: inner.credential_expires_at_ms,
            last_event_id: inner.ring.last_id(),
            cells,
            ended: inner.ended,
        }
    }

    /// Frames after `after`, plus a live receiver. `Err(from)` means the ring
    /// no longer reaches that point.
    pub fn subscribe(
        &self,
        after: u64,
    ) -> Result<(Vec<SessionEvent>, broadcast::Receiver<SessionEvent>), u64> {
        let inner = self.lock();
        let receiver = self.events.subscribe();
        inner.ring.since(after).map(|backlog| (backlog, receiver))
    }

    /// Everything the ring still holds, plus a live receiver. Used after a gap,
    /// when the client's resume point is already gone.
    pub fn subscribe_all(&self) -> (Vec<SessionEvent>, broadcast::Receiver<SessionEvent>) {
        let inner = self.lock();
        let receiver = self.events.subscribe();
        let backlog = inner.ring.since(inner.ring.first_id().saturating_sub(1));
        (backlog.unwrap_or_default(), receiver)
    }

    /// Records one object staged into the workspace bucket. The job report
    /// lists them at the end, so the run says where its data came from.
    pub fn record_input(&self, input: StagedInput) {
        let mut inner = self.lock();
        if inner.inventory.len() < MAX_TRACKED_INPUTS {
            inner.inventory.push(input);
        }
    }

    /// Everything staged into the workspace bucket while the session ran.
    pub fn inventory(&self) -> Vec<StagedInput> {
        self.lock().inventory.clone()
    }

    /// Marks the session as reconnecting after a lost channel, so the client
    /// sees it starting again rather than a stale ready.
    pub fn reopening(&self) {
        {
            let mut inner = self.lock();
            if inner.phase == SessionPhase::Ended {
                return;
            }
            inner.phase = SessionPhase::Starting;
        }
        self.announce();
    }

    /// Records one object the session's own credential touched. The job report
    /// lists them at the end, so a run says what data it used.
    pub fn record_touched(&self, object: TouchedObject) {
        let mut inner = self.lock();
        if inner.touched.len() >= MAX_TOUCHED_OBJECTS || inner.touched.contains(&object) {
            return;
        }
        inner.touched.push(object);
    }

    /// Everything the session's credential read or wrote.
    pub fn touched(&self) -> Vec<TouchedObject> {
        self.lock().touched.clone()
    }

    /// Re-sends the state object without its cells. The client's countdown and
    /// state badge read it, so it follows every state or deadline change.
    pub fn announce(&self) {
        let mut inner = self.lock();
        let frame = session_frame(&self.config, self.started_at_ms, &inner);
        let event = inner.ring.push(EventKind::Session, &frame);
        let _ = self.events.send(event);
    }

    /// Resets the idle wait and announces the new deadline. Reading scratch or
    /// staging inputs counts as use, exactly like a cell submit.
    pub fn touch(&self) {
        {
            let mut inner = self.lock();
            if inner.phase == SessionPhase::Ended {
                return;
            }
            inner.bump(self.config.idle_after_ms);
        }
        self.bumped.notify_waiters();
        self.announce();
    }

    /// Outputs of one cell the ring still holds, after `after`, with the id to
    /// continue from. Older outputs are gone, which the caller reports as a gap.
    pub fn cell_outputs(&self, cell_id: &str, after: u64) -> (u64, Vec<Value>) {
        let inner = self.lock();
        let mut outputs = Vec::new();
        let backlog = inner.ring.since(after).unwrap_or_default();
        for event in &backlog {
            if event.kind != EventKind::Output {
                continue;
            }
            let Ok(body) = serde_json::from_str::<Value>(&event.data) else {
                continue;
            };
            if body.get("cell_id").and_then(Value::as_str) == Some(cell_id)
                && let Some(output) = body.get("output")
            {
                outputs.push(output.clone());
            }
        }
        (inner.ring.last_id(), outputs)
    }

    /// Accepts one cell and returns its place in the queue, counted from one.
    pub fn submit_cell(&self, cell_id: &str, code: &str) -> Result<usize, SessionError> {
        if cell_id.is_empty()
            || cell_id.len() > MAX_CELL_ID_LEN
            || !cell_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
        {
            return Err(SessionError::CellId);
        }
        if code.len() > MAX_CELL_CODE_BYTES {
            return Err(SessionError::CodeTooLarge);
        }
        let position = {
            let mut inner = self.lock();
            match inner.phase {
                SessionPhase::Starting => return Err(SessionError::Starting),
                SessionPhase::Ended => return Err(SessionError::Ended),
                SessionPhase::Ready | SessionPhase::Busy => {}
            }
            if inner
                .cells
                .get(cell_id)
                .is_some_and(|record| record.state.is_open())
            {
                return Err(SessionError::CellBusy(cell_id.to_string()));
            }
            let now = Instant::now();
            while inner
                .submits
                .front()
                .is_some_and(|at| now.duration_since(*at) >= SUBMIT_WINDOW)
            {
                inner.submits.pop_front();
            }
            if inner.queue.len() >= MAX_QUEUED_CELLS || inner.submits.len() >= MAX_SUBMITS {
                return Err(SessionError::TooMany);
            }
            let permit = self
                .requests
                .try_reserve()
                .map_err(|_| SessionError::TooMany)?;
            inner.submits.push_back(now);
            inner.queue.push_back(cell_id.to_string());
            inner.track(cell_id);
            inner.set_cell(cell_id, CellPhase::Queued, None, now_ms());
            let event = inner.ring.push(
                EventKind::Cell,
                &CellFrame {
                    cell_id,
                    state: CellPhase::Queued.as_str(),
                    execution_count: None,
                    started_at_ms: None,
                    finished_at_ms: None,
                },
            );
            let _ = self.events.send(event);
            inner.bump(self.config.idle_after_ms);
            let id = inner.next_id();
            permit.send(HelperRequest::Execute {
                id,
                cell_id: cell_id.to_string(),
                code: code.to_string(),
            });
            inner.queue.len()
        };
        self.bumped.notify_waiters();
        self.announce();
        Ok(position)
    }

    /// Interrupts the running cell and drops the queue.
    pub fn interrupt(&self) -> Result<(), SessionError> {
        {
            let mut inner = self.lock();
            if inner.phase == SessionPhase::Ended {
                return Err(SessionError::Ended);
            }
            let permit = self
                .requests
                .try_reserve()
                .map_err(|_| SessionError::TooMany)?;
            // The cell the kernel is running keeps its state: its reply
            // decides whether it finished, errored or was interrupted.
            let queued: Vec<String> = inner.queue.drain(..).collect();
            let dropped: Vec<String> = queued
                .into_iter()
                .filter(|cell_id| {
                    inner
                        .cells
                        .get(cell_id)
                        .is_none_or(|record| record.state != CellPhase::Running)
                })
                .collect();
            for cell_id in dropped {
                inner.set_cell(&cell_id, CellPhase::Interrupted, None, now_ms());
                let event = inner.ring.push(
                    EventKind::Cell,
                    &CellFrame {
                        cell_id: &cell_id,
                        state: CellPhase::Interrupted.as_str(),
                        execution_count: None,
                        started_at_ms: None,
                        finished_at_ms: None,
                    },
                );
                let _ = self.events.send(event);
            }
            let id = inner.next_id();
            permit.send(HelperRequest::Interrupt { id });
        };
        self.announce();
        Ok(())
    }

    /// Ends the session. Repeating it keeps the first reason.
    pub fn end(&self, reason: EndReason) {
        {
            let mut inner = self.lock();
            if inner.phase == SessionPhase::Ended {
                return;
            }
            inner.phase = SessionPhase::Ended;
            inner.ended = Some(reason);
            inner.queue.clear();
            let frame = session_frame(&self.config, self.started_at_ms, &inner);
            let announced = inner.ring.push(EventKind::Session, &frame);
            let _ = self.events.send(announced);
            let event = inner
                .ring
                .push(EventKind::Ended, &json!({ "reason": reason.as_str() }));
            let _ = self.events.send(event);
        }
        self.done.cancel();
        self.bumped.notify_waiters();
    }

    /// Records a refreshed credential expiry and tells the client about it.
    pub fn credential_renewed(&self, expires_at_ms: u64) {
        {
            let mut inner = self.lock();
            inner.credential_expires_at_ms = expires_at_ms;
            let event = inner.ring.push(
                EventKind::Credential,
                &json!({ "expires_at_ms": expires_at_ms }),
            );
            let _ = self.events.send(event);
        }
        self.announce();
    }

    /// Lists one scratch directory through the helper.
    pub async fn list_scratch(&self, path: &str) -> Result<Map<String, Value>, SessionError> {
        let path = scratch_path(path)?;
        let id = self.lock().next_id();
        self.ask(HelperRequest::List { id, path }).await
    }

    /// Reads one scratch file through the helper.
    pub async fn read_scratch(
        &self,
        path: &str,
        offset: u64,
        limit: u64,
    ) -> Result<Map<String, Value>, SessionError> {
        let path = scratch_path(path)?;
        let id = self.lock().next_id();
        self.ask(HelperRequest::Read {
            id,
            path,
            offset,
            limit: limit.min(MAX_SCRATCH_READ_BYTES),
        })
        .await
    }

    async fn ask(&self, request: HelperRequest) -> Result<Map<String, Value>, SessionError> {
        if self.lock().phase == SessionPhase::Ended {
            return Err(SessionError::Ended);
        }
        let id = request.id();
        let (sender, receiver) = oneshot::channel();
        if let Ok(mut replies) = self.replies.lock() {
            replies.insert(id, sender);
        }
        self.requests
            .send(request)
            .await
            .map_err(|_| SessionError::Ended)?;
        let answered = timeout(REPLY_TIMEOUT, receiver).await;
        if answered.is_err()
            && let Ok(mut replies) = self.replies.lock()
        {
            replies.remove(&id);
        }
        let body = answered
            .map_err(|_| SessionError::NoReply)?
            .map_err(|_| SessionError::NoReply)?;
        match body.get("error").and_then(Value::as_str) {
            Some(message) => Err(SessionError::Helper(message.to_string())),
            None => Ok(body),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn apply(&self, event: HelperEvent) {
        let mut inner = self.lock();
        match event {
            HelperEvent::Output { cell_id, output } => {
                let bytes = output.to_string().len();
                let verdict = inner
                    .cells
                    .get_mut(&cell_id)
                    .map_or(BudgetVerdict::Keep, |record| record.budget.charge(bytes));
                let output = match verdict {
                    BudgetVerdict::Keep => output,
                    BudgetVerdict::Truncate => json!({
                        "output_type": "stream",
                        "name": "stderr",
                        "text": TRUNCATED_NOTICE,
                    }),
                    BudgetVerdict::Drop => return,
                };
                let seq = inner.ring.last_id().saturating_add(1);
                let frame = inner.ring.push(
                    EventKind::Output,
                    &json!({ "cell_id": cell_id, "seq": seq, "output": output }),
                );
                let _ = self.events.send(frame);
            }
            HelperEvent::Cell {
                cell_id,
                state,
                execution_count,
            } => {
                let Some(phase) = CellPhase::from_wire(&state) else {
                    return;
                };
                inner.track(&cell_id);
                inner.set_cell(&cell_id, phase, execution_count, now_ms());
                if !phase.is_open() {
                    inner.queue.retain(|queued| queued != &cell_id);
                }
                let started_at_ms = inner
                    .cells
                    .get(&cell_id)
                    .and_then(|record| record.started_at_ms);
                let finished_at_ms = inner
                    .cells
                    .get(&cell_id)
                    .and_then(|record| record.finished_at_ms);
                let frame = inner.ring.push(
                    EventKind::Cell,
                    &CellFrame {
                        cell_id: &cell_id,
                        state: phase.as_str(),
                        execution_count,
                        started_at_ms,
                        finished_at_ms,
                    },
                );
                let _ = self.events.send(frame);
            }
            HelperEvent::Kernel { state } => {
                if inner.phase != SessionPhase::Ended {
                    inner.phase = match state.as_str() {
                        "busy" => SessionPhase::Busy,
                        "starting" => SessionPhase::Starting,
                        _ => SessionPhase::Ready,
                    };
                }
                let frame = inner
                    .ring
                    .push(EventKind::Kernel, &json!({ "state": state }));
                let _ = self.events.send(frame);
                let announced = session_frame(&self.config, self.started_at_ms, &inner);
                let event = inner.ring.push(EventKind::Session, &announced);
                let _ = self.events.send(event);
                if state == "dead" {
                    drop(inner);
                    self.end(EndReason::KernelExit);
                }
            }
            HelperEvent::Reply { id, body } => {
                drop(inner);
                if let Ok(mut replies) = self.replies.lock()
                    && let Some(sender) = replies.remove(&id)
                {
                    let _ = sender.send(body);
                }
            }
        }
    }
}

impl Inner {
    fn next_id(&mut self) -> u64 {
        self.next_request = self.next_request.saturating_add(1);
        self.next_request
    }

    fn bump(&mut self, idle_after_ms: u64) {
        self.idle_deadline = Instant::now() + Duration::from_millis(idle_after_ms);
    }

    fn track(&mut self, cell_id: &str) {
        if self.cells.contains_key(cell_id) {
            return;
        }
        self.order.push_back(cell_id.to_string());
        while self.order.len() > MAX_TRACKED_CELLS {
            if let Some(dropped) = self.order.pop_front() {
                self.cells.remove(&dropped);
            }
        }
    }

    fn set_cell(
        &mut self,
        cell_id: &str,
        phase: CellPhase,
        execution_count: Option<u32>,
        now: u64,
    ) {
        let record = self.cells.entry(cell_id.to_string()).or_insert(CellRecord {
            state: phase,
            execution_count: None,
            started_at_ms: None,
            finished_at_ms: None,
            budget: CellBudget::default(),
        });
        record.state = phase;
        if execution_count.is_some() {
            record.execution_count = execution_count;
        }
        match phase {
            CellPhase::Running => record.started_at_ms = Some(now),
            CellPhase::Done | CellPhase::Error | CellPhase::Interrupted => {
                record.finished_at_ms = Some(now)
            }
            CellPhase::Queued => {
                record.execution_count = None;
                record.started_at_ms = None;
                record.finished_at_ms = None;
                record.budget = CellBudget::default();
            }
        }
    }
}

/// Every session this node runs, keyed by job id.
#[derive(Default)]
pub struct SessionRegistry {
    sessions: Mutex<HashMap<String, Arc<Session>>>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, job_id: &str) -> Option<Arc<Session>> {
        self.sessions
            .lock()
            .ok()
            .and_then(|sessions| sessions.get(job_id).cloned())
    }

    /// Registers a session and starts its channel and idle timer. A second
    /// open for the same job returns the session already running.
    pub fn open(
        self: &Arc<Self>,
        config: SessionConfig,
        backend: Arc<dyn ExecutorBackend>,
        fence: FenceContext,
    ) -> Arc<Session> {
        if let Some(existing) = self.get(&config.job_id) {
            return existing;
        }
        let (session, requests) = build_session(config);
        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.insert(session.config.job_id.clone(), session.clone());
        }
        tokio::spawn(pump(session.clone(), backend, fence, requests));
        tokio::spawn(idle_watch(session.clone()));
        tokio::spawn(forget(self.clone(), session.clone()));
        session
    }

    /// Registers a session without a channel. Only tests use it; production
    /// always has a backend to talk to.
    #[cfg(test)]
    fn open_detached(self: &Arc<Self>, config: SessionConfig) -> (Arc<Session>, TestChannel) {
        let (session, requests) = build_session(config);
        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.insert(session.config.job_id.clone(), session.clone());
        }
        tokio::spawn(idle_watch(session.clone()));
        (
            session,
            TestChannel {
                _requests: requests,
            },
        )
    }
}

/// Keeps the request channel of a detached test session open, so a submit is
/// not refused for a dropped receiver.
#[cfg(test)]
struct TestChannel {
    _requests: mpsc::Receiver<HelperRequest>,
}

fn build_session(config: SessionConfig) -> (Arc<Session>, mpsc::Receiver<HelperRequest>) {
    let (events, _) = broadcast::channel(BROADCAST_DEPTH);
    let (sender, receiver) = mpsc::channel(MAX_QUEUED_CELLS * 2);
    let idle_deadline = Instant::now() + Duration::from_millis(config.idle_after_ms);
    let credential_expires_at_ms = config.credential_expires_at_ms;
    let session = Arc::new(Session {
        config,
        started_at_ms: now_ms(),
        inner: Mutex::new(Inner {
            phase: SessionPhase::Starting,
            ended: None,
            ring: EventRing::default(),
            order: VecDeque::new(),
            cells: BTreeMap::new(),
            queue: VecDeque::new(),
            submits: VecDeque::new(),
            idle_deadline,
            credential_expires_at_ms,
            next_request: 0,
            inventory: Vec::new(),
            touched: Vec::new(),
        }),
        events,
        requests: sender,
        replies: Mutex::new(HashMap::new()),
        bumped: Notify::new(),
        done: CancellationToken::new(),
    });
    (session, receiver)
}

/// Drops the session from the registry once it has ended, so a later job of the
/// same id starts clean.
async fn forget(registry: Arc<SessionRegistry>, session: Arc<Session>) {
    session.finished().await;
    if let Ok(mut sessions) = registry.sessions.lock() {
        sessions.remove(session.job_id());
    }
}

/// Ends the session once it has gone without a submit for its idle wait.
async fn idle_watch(session: Arc<Session>) {
    loop {
        let deadline = session.lock().idle_deadline;
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                if session.lock().idle_deadline <= Instant::now() {
                    session.end(EndReason::Idle);
                    return;
                }
            }
            _ = session.bumped.notified() => {}
            _ = session.done.cancelled() => return,
        }
    }
}

/// Opens the channel, then forwards requests and helper events. A lost channel
/// is reopened: a reconnect is a new exec against the same container.
async fn pump(
    session: Arc<Session>,
    backend: Arc<dyn ExecutorBackend>,
    fence: FenceContext,
    requests: mpsc::Receiver<HelperRequest>,
) {
    let requests = Arc::new(tokio::sync::Mutex::new(requests));
    loop {
        let Some(channel) = connect(&session, backend.as_ref(), &fence).await else {
            session.end(EndReason::KernelExit);
            return;
        };
        // The helper reports the kernel's own state on attach, so the session
        // stays starting until it says otherwise.
        session.announce();
        let writer = tokio::spawn(write_requests(
            channel.input,
            requests.clone(),
            session.done.clone(),
        ));
        read_events(&session, channel.output).await;
        writer.abort();
        if session.done.is_cancelled() {
            return;
        }
        // An exec that exits at once must not become a reconnect storm.
        tokio::select! {
            _ = tokio::time::sleep(OPEN_RETRY) => {}
            _ = session.done.cancelled() => return,
        }
        session.reopening();
    }
}

/// Reads helper events until the channel ends or the session does.
async fn read_events(
    session: &Arc<Session>,
    output: std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>,
) {
    let mut reader = BufReader::new(output);
    let mut line = Vec::new();
    loop {
        let read = tokio::select! {
            read = read_line(&mut reader, &mut line) => read,
            _ = session.done.cancelled() => return,
        };
        match read {
            Ok(0) => return,
            Ok(_) => {
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                match serde_json::from_slice::<HelperEvent>(&line) {
                    Ok(event) => session.apply(event),
                    Err(error) => tracing::warn!(
                        job_id = session.job_id(),
                        error = %error,
                        "session helper sent an unreadable event"
                    ),
                }
            }
            Err(error) => {
                tracing::warn!(
                    job_id = session.job_id(),
                    error = %error,
                    "session channel failed"
                );
                return;
            }
        }
    }
}

/// Retries until the attempt's helper answers, the session ends, or the node
/// gives up waiting for a container that never became reachable.
async fn connect(
    session: &Arc<Session>,
    backend: &dyn ExecutorBackend,
    fence: &FenceContext,
) -> Option<crate::executor::SessionChannel> {
    let give_up = Instant::now() + OPEN_DEADLINE;
    loop {
        match backend.open_session(fence).await {
            Ok(channel) => return Some(channel),
            Err(error) => {
                if Instant::now() >= give_up {
                    tracing::warn!(
                        job_id = session.job_id(),
                        error = %error,
                        "session helper never became reachable"
                    );
                    return None;
                }
                tokio::select! {
                    _ = tokio::time::sleep(OPEN_RETRY) => {}
                    _ = session.done.cancelled() => return None,
                }
            }
        }
    }
}

async fn write_requests(
    mut input: std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>,
    requests: Arc<tokio::sync::Mutex<mpsc::Receiver<HelperRequest>>>,
    done: CancellationToken,
) {
    let mut requests = requests.lock().await;
    loop {
        let request = tokio::select! {
            request = requests.recv() => request,
            _ = done.cancelled() => return,
        };
        let Some(request) = request else { return };
        let Ok(mut line) = serde_json::to_vec(&request) else {
            continue;
        };
        line.push(b'\n');
        if input.write_all(&line).await.is_err() || input.flush().await.is_err() {
            return;
        }
    }
}

/// Reads one line, refusing an oversized one so a broken helper cannot grow the
/// node's memory without bound. `Ok(0)` is end of stream.
async fn read_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    line: &mut Vec<u8>,
) -> io::Result<usize> {
    line.clear();
    let read = reader
        .take((MAX_HELPER_LINE_BYTES + 1) as u64)
        .read_until(b'\n', line)
        .await?;
    if line.last() == Some(&b'\n') {
        line.pop();
        return Ok(line.len().max(1));
    }
    if line.len() > MAX_HELPER_LINE_BYTES {
        return Err(io::Error::other("session helper line is too long"));
    }
    Ok(read)
}

/// A scratch path is relative to the working directory and carries no `..`.
fn scratch_path(path: &str) -> Result<String, SessionError> {
    let trimmed = path.trim();
    if trimmed.starts_with('/') || trimmed.split('/').any(|part| part == "..") {
        return Err(SessionError::Path);
    }
    Ok(if trimmed.is_empty() {
        ".".to_string()
    } else {
        trimmed.to_string()
    })
}

/// The state object the `session` frame carries: everything the snapshot has
/// except its cells.
fn session_frame<'a>(
    config: &'a SessionConfig,
    started_at_ms: u64,
    inner: &Inner,
) -> SessionFrame<'a> {
    SessionFrame {
        job_id: &config.public_job_id,
        state: inner.phase.as_str(),
        runtime: &config.runtime,
        workspace_bucket: &config.workspace_bucket,
        executor_node_id: &config.executor_node_id,
        started_at_ms,
        idle_after_ms: config.idle_after_ms,
        idle_deadline_ms: deadline_ms(inner.idle_deadline),
        credential_expires_at_ms: inner.credential_expires_at_ms,
        last_event_id: inner.ring.last_id(),
        ended: inner.ended.map(|reason| EndedFrame {
            reason: reason.as_str(),
        }),
    }
}

/// Wall-clock milliseconds a monotonic deadline falls on.
fn deadline_ms(deadline: Instant) -> u64 {
    let now = Instant::now();
    let left = deadline.saturating_duration_since(now);
    now_ms().saturating_add(left.as_millis().try_into().unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests;
