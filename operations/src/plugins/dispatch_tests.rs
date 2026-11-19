//! Tests starting runs: a manual run, its saved invocation and refusals, timer occurrences queued
//! once each and skipped at the bound, and the stored trigger kind indexes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::drive;
use crate::plugins::calls::*;
use crate::plugins::install::tests::Fixture;
use crate::plugins::matcher::event_trigger;
use crate::plugins::rows::tests::{page_result, values_result};
use crate::plugins::rows::{StoreError, decode, read_call, values};
use crate::plugins::runs::{TimerRow, list_runs, read_run};
use crate::plugins::test_support::installed;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::PLUGIN_RUNS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::plugin_runs::SourceKind;
use aruna_core::plugin_runs::{RUNS_QUEUED, timer_key};

const HOUR: u64 = 3600 * 1000;

#[tokio::test]
async fn manual_runs() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let (ctx, realm, local) = (&fixture.ctx, fixture.actor.realm_id, fixture.actor.node_id);
    let start = |input: &'static str| {
        let auth = fixture.auth();
        async move { start_run(ctx, &auth, realm, local, install.install_id, input, 5).await }
    };
    // The fixture's manual trigger has no form, so only empty input fits it.
    let refused = start(r#"{"dataset":"d1"}"#).await;
    let unknown = SettingError::Unknown("dataset".into());
    assert!(matches!(refused, Err(StartError::Form(error)) if error == unknown));
    for input in ["[]", "1", "{"] {
        assert!(
            matches!(start(input).await, Err(StartError::Input)),
            "{input}"
        );
    }
    let run = start("{}").await.unwrap();
    let stored = drive(read_run(install.install_id, run.run_id), ctx)
        .await
        .unwrap();
    assert_eq!(stored, run);
    assert_eq!(
        (run.handler.as_str(), run.revision),
        ("manual", install.revision)
    );
    assert_eq!(run.settings, install.settings);
    let by = fixture.actor.user_id;
    let input = "{}".to_string();
    assert_eq!(run.trigger, RunTrigger::Manual { by, input });
    assert!(matches!(start("not json").await, Err(StartError::Input)));

    let other = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    let auth = fixture.auth();
    let elsewhere = start_run(ctx, &auth, realm, other, install.install_id, "{}", 5).await;
    assert!(matches!(elsewhere, Err(StartError::Elsewhere(node)) if node == local));
    let plain = fixture.install(&fixture.request(None)).await.unwrap();
    let none = start_run(ctx, &auth, realm, local, plain.install_id, "{}", 5).await;
    assert!(matches!(none, Err(StartError::NoTrigger("manual"))));
}

#[tokio::test]
async fn timer_occurrences() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let ctx = &fixture.ctx;
    let config = read_config(ctx, fixture.actor.realm_id).await.unwrap();
    let local = fixture.actor.node_id;
    let active = install.active_since_ms.unwrap();
    assert_eq!(
        enqueue_timers(ctx, &config, local, active + HOUR - 1)
            .await
            .unwrap(),
        0
    );
    // Three missed hours collapse into the latest occurrence, queued once.
    assert_eq!(
        enqueue_timers(ctx, &config, local, active + 3 * HOUR + 7)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        enqueue_timers(ctx, &config, local, active + 3 * HOUR + 9)
            .await
            .unwrap(),
        0
    );
    let (runs, _) = drive(list_runs(install.install_id, None, 10), ctx)
        .await
        .unwrap();
    let occurrence_ms = active + 3 * HOUR;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].trigger, RunTrigger::Timer { occurrence_ms });
    assert_eq!(runs[0].handler, "timer");
    let other = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    assert_eq!(
        enqueue_timers(ctx, &config, other, active + 9 * HOUR)
            .await
            .unwrap(),
        0
    );

    // At the queue bound an occurrence is skipped and counted, and the timer moves on.
    for n in 1..RUNS_QUEUED as u64 {
        let auth = fixture.auth();
        let realm = fixture.actor.realm_id;
        start_run(ctx, &auth, realm, local, install.install_id, "{}", n)
            .await
            .unwrap();
    }
    assert_eq!(
        enqueue_timers(ctx, &config, local, active + 4 * HOUR)
            .await
            .unwrap(),
        0
    );
    let row = drive(read_timer(install.install_id), ctx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row,
        TimerRow {
            last_due_ms: active + 4 * HOUR,
            skipped: 1
        }
    );
}

// Coverage rows store trigger kinds by index; the update kind was appended, so a row written
// before it keeps its meaning, and every event kind starts the trigger of its own name.
#[test]
fn trigger_indexes_kept() {
    let index = |kind: TriggerKind| postcard::to_allocvec(&kind).unwrap();
    assert_eq!(index(TriggerKind::SyncFailed), [6]);
    assert_eq!(index(TriggerKind::DatasetUpdated), [7]);
    for kind in [
        SourceKind::DatasetCreated,
        SourceKind::DataUploaded,
        SourceKind::DatasetUpdated,
    ] {
        assert_eq!(event_trigger(kind).as_str(), kind.name());
    }
}

#[test]
fn plans_timer_occurrences() {
    let install = crate::plugins::work::tests::sample_install(Ulid::from_parts(1, 1));
    let plan = |now_ms| enqueue_timer(&install, "every".into(), 10, now_ms);
    // Before the first interval since activation nothing is due.
    let mut early = plan(14);
    assert!(matches!(
        early.first().unwrap(),
        Next::Call(IoCall::Read(_))
    ));
    assert_eq!(
        early.next(values_result(vec![None])).unwrap(),
        Next::Done(false)
    );
    // Missed occurrences collapse into the latest; a full queue skips it.
    let mut due = plan(37);
    due.first().unwrap();
    assert!(matches!(
        due.next(values_result(vec![None])).unwrap(),
        Next::Call(IoCall::Page { .. })
    ));
    let full = (0..RUNS_QUEUED)
        .map(|seq| (vec![seq as u8], Vec::new()))
        .collect();
    let Next::Call(IoCall::Write(rows)) = due.next(page_result(full, None)).unwrap() else {
        panic!("the timer row is written");
    };
    let row: TimerRow = decode(&rows[0].2).unwrap();
    assert_eq!((row.last_due_ms, row.skipped), (35, 1));
    assert_eq!(due.next(IoResult::Done).unwrap(), Next::Done(false));
    let mut queued = plan(37);
    queued.first().unwrap();
    queued.next(values_result(vec![None])).unwrap();
    let Next::Call(IoCall::Write(rows)) = queued.next(page_result(Vec::new(), None)).unwrap()
    else {
        panic!("the run and the timer row are written");
    };
    assert_eq!(rows.len(), 3);
    assert_eq!(queued.next(IoResult::Done).unwrap(), Next::Done(true));
}

/// The timer row of an install, if its timer ran before.
fn read_timer(install_id: Ulid) -> ReadTimer {
    ReadTimer(timer_key(install_id))
}

#[derive(Debug, PartialEq)]
struct ReadTimer(Vec<u8>);

impl Operation for ReadTimer {
    type Output = Option<TimerRow>;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, StoreError> {
        Ok(Next::Call(read_call(vec![(
            PLUGIN_RUNS_KEYSPACE,
            self.0.clone(),
        )])))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, StoreError> {
        let [found] = values::<1>(answer)?;
        Ok(Next::Done(found.map(|bytes| decode(&bytes)).transpose()?))
    }
}

mod operations {
    use serde_json::{Value, json};

    use super::*;
    use crate::plugins::bound::NodeCheckFailure;
    use crate::plugins::fixtures::{self, config_result, is_child};
    use crate::plugins::rows::tests::child_result;
    use crate::users::service_account::check::ServiceCheckError;
    use aruna_core::plugin_installs::SecretBinding;
    use aruna_core::plugins::ReleaseError;
    use aruna_core::structs::identity::user::User;

    fn manual() -> Value {
        json!({ "triggers": { "manual": {} } })
    }

    fn webhook(kind: &str) -> Value {
        json!({
            "secrets": { "hook_key": { "title": "Hook key", "kind": kind } },
            "triggers": { "webhook": { "verify": "hook_key", "delivery_id": "x-delivery" } }
        })
    }

    fn start(input: &str, local: u8) -> StartRun {
        let place = (fixtures::realm(), fixtures::node(local));
        let id = fixtures::install().install_id;
        StartRun::new(&fixtures::caller().auth, place, id, input, 5_000)
    }

    fn admin_ok() -> IoResult {
        child_result(Ok::<Option<User>, ServiceCheckError>(None))
    }

    fn checked(start: &mut StartRun) -> Result<Next<PluginRun>, StartError> {
        assert!(is_child(&start.first()));
        let config = fixtures::config(&fixtures::install(), manual());
        assert!(is_child(&start.next(config_result(config))));
        start.next(admin_ok())
    }

    #[test]
    fn start_queues_run() {
        let mut run = start("{}", 3);
        assert_eq!(run.hold_scope(), Some(HoldScope::PluginAdmissions));
        assert!(is_child(&checked(&mut run)));
        let Ok(Next::Done(queued)) = run.next(child_result(Ok::<(), RunError>(()))) else {
            panic!("the admission ends the start");
        };
        assert_eq!(queued.install_id, fixtures::install().install_id);
    }

    #[test]
    fn start_refuses_requests() {
        let receiver = fixtures::node(3);
        let elsewhere = checked(&mut start("{}", 4));
        assert_eq!(elsewhere.unwrap_err(), StartError::Elsewhere(receiver));
        let invalid = checked(&mut start("[]", 3));
        assert_eq!(invalid.unwrap_err(), StartError::Input);
    }

    #[test]
    fn timers_queue_due() {
        let every = json!({ "triggers": { "every": { "interval": "1h" } } });
        let config = fixtures::config(&fixtures::install(), every);
        let mut timers = EnqueueTimers::new(&config, fixtures::node(3), 5_000);
        assert_eq!(timers.hold_scope(), Some(HoldScope::PluginAdmissions));
        assert!(is_child(&timers.first()));
        let queued = child_result(Ok::<bool, StoreError>(true));
        assert_eq!(timers.next(queued), Ok(Next::Done(1)));
        let mut elsewhere = EnqueueTimers::new(&config, fixtures::node(4), 5_000);
        assert_eq!(elsewhere.first(), Ok(Next::Done(0)));
    }

    fn hook_request() -> HookRequest {
        HookRequest {
            delivery_id: None,
            method: "POST".into(),
            query: String::new(),
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    fn verify(
        local: u8,
        install: &PluginInstall,
        extra: Value,
    ) -> Result<Next<VerifiedHook>, HookError> {
        let place = (fixtures::realm(), fixtures::node(local));
        let mut hook = VerifyHook::new(place, install.install_id, hook_request(), 5_000);
        assert!(is_child(&hook.first()));
        hook.next(config_result(fixtures::config(install, extra)))
    }

    #[test]
    fn hook_checks_install() {
        let install = fixtures::install();
        let elsewhere = verify(4, &install, webhook("header"));
        assert_eq!(
            elsewhere.unwrap_err(),
            HookError::Elsewhere(fixtures::node(3))
        );
        assert_eq!(
            verify(3, &install, json!({})).unwrap_err(),
            HookError::NotFound
        );
        let mut paused = install;
        paused.state = InstallState::Paused;
        let answer = verify(3, &paused, webhook("header"));
        assert!(matches!(answer, Err(HookError::Paused(_))));
    }

    #[test]
    fn hook_needs_secret() {
        let mut install = fixtures::install();
        install.install_id = Ulid::from_parts(7, 7);
        let place = (fixtures::realm(), fixtures::node(3));
        let mut hook = VerifyHook::new(place, install.install_id, hook_request(), 5_000);
        hook.first().unwrap();
        let config = fixtures::config(&install, webhook("header"));
        assert!(is_child(&hook.next(config_result(config))));
        let gone = ReleaseFailure(ReleaseError::Unavailable("no".into()));
        let answer = hook.next(child_result(Err::<Released, _>(gone)));
        assert!(matches!(answer, Err(HookError::Unavailable(_))));
    }

    fn authorization(
        scope: &str,
        kind: &str,
    ) -> (HookAuthorization, Result<Next<Option<bool>>, IoError>) {
        let mut install = fixtures::install();
        let binding = SecretBinding::parse(&format!("{scope}:key")).unwrap();
        install.secrets.insert("hook_key".into(), binding);
        let config = fixtures::config(&install, webhook(kind));
        let place = (fixtures::node(3), install.install_id);
        let mut check = HookAuthorization::new(config, place);
        let first = check.first();
        (check, first)
    }

    #[test]
    fn relay_reads_secret() {
        let (_, bearer) = authorization("group", "bearer");
        assert_eq!(bearer, Ok(Next::Done(Some(true))));
        let (mut node, first) = authorization("node", "header");
        assert!(is_child(&first));
        let header = Ok::<_, NodeCheckFailure>(Some("Authorization".to_string()));
        assert_eq!(node.next(child_result(header)), Ok(Next::Done(Some(true))));
        let (mut group, first) = authorization("group", "header");
        assert!(is_child(&first));
        let gone = ReleaseFailure(ReleaseError::Unavailable("no".into()));
        let failed = child_result(Err::<Option<Selected>, _>(gone));
        assert_eq!(group.next(failed), Ok(Next::Done(None)));
    }

    #[test]
    fn relay_without_check() {
        let install = fixtures::install();
        let place = (fixtures::node(3), install.install_id);
        let plain = fixtures::config(&install, json!({}));
        let mut check = HookAuthorization::new(plain.clone(), place);
        assert_eq!(check.first(), Ok(Next::Done(Some(false))));
        let unknown = (fixtures::node(3), Ulid::from_parts(9, 9));
        let mut check = HookAuthorization::new(plain, unknown);
        assert_eq!(check.first(), Ok(Next::Done(None)));
    }
}
