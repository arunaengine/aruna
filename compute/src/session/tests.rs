use super::*;
use aruna_core::compute::session::{MAX_CELL_OUTPUTS, MAX_RING_EVENTS, TRUNCATED_NOTICE};
use serde_json::json;

fn config(idle_after_ms: u64) -> SessionConfig {
    SessionConfig {
        job_id: "01JJRSTVWXYZ0123456789ABCD".to_string(),
        runtime: "python-notebook".to_string(),
        workspace_bucket: "lab-data".to_string(),
        executor_node_id: "node-1".to_string(),
        idle_after_ms,
        credential_expires_at_ms: 42,
    }
}

/// A session the kernel already reported ready for, with its request channel
/// kept open so submits are accepted.
fn ready(idle_after_ms: u64) -> (Arc<Session>, TestChannel) {
    let registry = Arc::new(SessionRegistry::new());
    let (session, channel) = registry.open_detached(config(idle_after_ms));
    session.apply(HelperEvent::Kernel {
        state: "idle".to_string(),
    });
    (session, channel)
}

#[tokio::test(start_paused = true)]
async fn refuses_while_starting() {
    let registry = Arc::new(SessionRegistry::new());
    let (session, _channel) = registry.open_detached(config(600_000));
    assert_eq!(session.submit_cell("c1", "1"), Err(SessionError::Starting));
}

#[tokio::test(start_paused = true)]
async fn refuses_after_end() {
    let (session, _channel) = ready(600_000);
    session.end(EndReason::Ended);
    assert_eq!(session.submit_cell("c1", "1"), Err(SessionError::Ended));
    assert_eq!(session.snapshot().state, SessionPhase::Ended);
    assert_eq!(session.snapshot().ended, Some(EndReason::Ended));
}

#[tokio::test(start_paused = true)]
async fn refuses_bad_id() {
    // A cell id is 1 to 64 characters of A-Z, a-z, 0-9, _ and -.
    let (session, _channel) = ready(600_000);
    assert_eq!(session.submit_cell("", "1"), Err(SessionError::CellId));
    assert_eq!(session.submit_cell("a b", "1"), Err(SessionError::CellId));
    assert_eq!(
        session.submit_cell(&"c".repeat(MAX_CELL_ID_LEN + 1), "1"),
        Err(SessionError::CellId)
    );
}

#[tokio::test(start_paused = true)]
async fn refuses_large_code() {
    let (session, _channel) = ready(600_000);
    let code = "x".repeat(MAX_CELL_CODE_BYTES + 1);
    assert_eq!(
        session.submit_cell("c1", &code),
        Err(SessionError::CodeTooLarge)
    );
}

#[tokio::test(start_paused = true)]
async fn refuses_busy_cell() {
    let (session, _channel) = ready(600_000);
    session.submit_cell("c1", "1").expect("first submit");
    assert_eq!(
        session.submit_cell("c1", "2"),
        Err(SessionError::CellBusy("c1".to_string()))
    );
}

#[tokio::test(start_paused = true)]
async fn limits_submit_burst() {
    let (session, _channel) = ready(600_000);
    for index in 0..MAX_SUBMITS {
        session
            .submit_cell(&format!("c{index}"), "1")
            .expect("burst submit");
    }
    assert_eq!(session.submit_cell("late", "1"), Err(SessionError::TooMany));
    tokio::time::advance(SUBMIT_WINDOW).await;
    session.submit_cell("late", "1").expect("window moved on");
}

#[tokio::test(start_paused = true)]
async fn limits_queue_depth() {
    let (session, _channel) = ready(600_000);
    let mut accepted = 0;
    while accepted < MAX_QUEUED_CELLS {
        for index in 0..MAX_SUBMITS.min(MAX_QUEUED_CELLS - accepted) {
            session
                .submit_cell(&format!("c{}", accepted + index), "1")
                .expect("queued submit");
        }
        accepted += MAX_SUBMITS.min(MAX_QUEUED_CELLS - accepted);
        tokio::time::advance(SUBMIT_WINDOW).await;
    }
    assert_eq!(session.submit_cell("over", "1"), Err(SessionError::TooMany));
}

#[tokio::test(start_paused = true)]
async fn interrupt_drops_queue() {
    let (session, _channel) = ready(600_000);
    session.submit_cell("c1", "1").expect("submit");
    session.submit_cell("c2", "2").expect("submit");
    session.interrupt().expect("interrupt");
    let states: Vec<CellPhase> = session
        .snapshot()
        .cells
        .into_iter()
        .map(|cell| cell.state)
        .collect();
    assert_eq!(states, vec![CellPhase::Interrupted, CellPhase::Interrupted]);
}

#[tokio::test(start_paused = true)]
async fn truncates_cell_output() {
    // One runaway cell may fill its budget once and is then silent.
    let (session, _channel) = ready(600_000);
    session.submit_cell("c1", "1").expect("submit");
    for _ in 0..(MAX_CELL_OUTPUTS + 4) {
        session.apply(HelperEvent::Output {
            cell_id: "c1".to_string(),
            output: json!({"output_type": "stream", "name": "stdout", "text": "x"}),
        });
    }
    let (backlog, _receiver) = session.subscribe(0).expect("fresh ring resumes");
    let notices = backlog
        .iter()
        .filter(|event| event.data.contains(TRUNCATED_NOTICE))
        .count();
    let outputs = backlog
        .iter()
        .filter(|event| event.kind == EventKind::Output)
        .count();
    assert_eq!(notices, 1);
    assert_eq!(outputs, MAX_CELL_OUTPUTS + 1);
}

#[tokio::test(start_paused = true)]
async fn rerun_resets_output() {
    let (session, _channel) = ready(600_000);
    session.submit_cell("c1", "first").unwrap();
    session.apply(HelperEvent::Output {
        cell_id: "c1".to_string(),
        output: json!({"output_type": "stream", "name": "stdout", "text": "x".repeat(1024 * 1024)}),
    });
    session.apply(HelperEvent::Cell {
        cell_id: "c1".to_string(),
        state: "done".to_string(),
        execution_count: Some(1),
    });
    let after = session.snapshot().last_event_id;
    session.submit_cell("c1", "second").unwrap();
    let cell = &session.snapshot().cells[0];
    assert_eq!(cell.execution_count, None);
    assert_eq!(cell.started_at_ms, None);
    assert_eq!(cell.finished_at_ms, None);
    session.apply(HelperEvent::Output {
        cell_id: "c1".to_string(),
        output: json!({"output_type": "stream", "name": "stdout", "text": "second"}),
    });
    assert_eq!(session.cell_outputs("c1", after).1[0]["text"], "second");
}

#[tokio::test(start_paused = true)]
async fn resumes_and_gaps() {
    // A recent resume point replays; one the ring dropped reports a gap.
    let (session, _channel) = ready(600_000);
    session.submit_cell("c1", "1").expect("submit");
    let last = session.snapshot().last_event_id;
    session.apply(HelperEvent::Kernel {
        state: "busy".to_string(),
    });
    let (backlog, _receiver) = session.subscribe(last).expect("resume");
    assert_eq!(backlog[0].kind, EventKind::Kernel);
    assert_eq!(backlog[1].kind, EventKind::Session);

    for index in 0..MAX_RING_EVENTS {
        session.apply(HelperEvent::Kernel {
            state: format!("busy{index}"),
        });
    }
    assert!(session.subscribe(1).is_err());
}

#[tokio::test(start_paused = true)]
async fn kernel_death_ends() {
    let (session, _channel) = ready(600_000);
    session.apply(HelperEvent::Kernel {
        state: "dead".to_string(),
    });
    assert_eq!(session.snapshot().ended, Some(EndReason::KernelExit));
}

#[tokio::test(start_paused = true)]
async fn idle_timeout_ends() {
    // Virtual time only: the wait must be the configured one, not a sleep.
    let start = Instant::now();
    let (session, _channel) = ready(60_000);
    assert_eq!(session.finished().await, EndReason::Idle);
    assert!(Instant::now().duration_since(start) >= Duration::from_millis(60_000));
}

#[tokio::test(start_paused = true)]
async fn submit_resets_idle() {
    let (session, _channel) = ready(60_000);
    let before = session.lock().idle_deadline;
    tokio::time::advance(Duration::from_millis(30_000)).await;
    session.submit_cell("c1", "1").expect("submit");
    assert!(session.lock().idle_deadline > before);
}

#[tokio::test(start_paused = true)]
async fn refuses_escaping_path() {
    // A scratch path stays inside the working directory.
    assert_eq!(scratch_path("/etc/passwd"), Err(SessionError::Path));
    assert_eq!(scratch_path("a/../../b"), Err(SessionError::Path));
    assert_eq!(scratch_path(""), Ok(".".to_string()));
    assert_eq!(scratch_path("data/out"), Ok("data/out".to_string()));
}

#[tokio::test(start_paused = true)]
async fn announces_credential_refresh() {
    // A refreshed credential expiry reaches the client as its own frame.
    let (session, _channel) = ready(600_000);
    session.credential_renewed(99);
    assert_eq!(session.snapshot().credential_expires_at_ms, 99);
    let (backlog, _receiver) = session.subscribe(0).expect("resume");
    assert!(
        backlog
            .iter()
            .any(|event| event.kind == EventKind::Credential)
    );
}

#[tokio::test(start_paused = true)]
async fn records_touched_objects() {
    // The report lists what the session's own credential used, once each.
    let (session, _channel) = ready(600_000);
    let object = TouchedObject {
        bucket: "lab-data".to_string(),
        key: "data/reads.fastq".to_string(),
        operation: "read".to_string(),
    };
    session.record_touched(object.clone());
    session.record_touched(object.clone());
    assert_eq!(session.touched(), vec![object]);
}

#[tokio::test(start_paused = true)]
async fn reopening_starts_again() {
    // A lost channel puts the session back into starting, not a stale ready.
    let (session, _channel) = ready(600_000);
    assert_eq!(session.snapshot().state, SessionPhase::Ready);
    session.reopening();
    assert_eq!(session.snapshot().state, SessionPhase::Starting);
}
