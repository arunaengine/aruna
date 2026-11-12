//! Tests sends through the real fixture guest with a fake sender: the URL stays below the
//! connection, answers are projected once, redirects show status only, no row holds the secret.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::{Arc, Mutex};

use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{RunState, RunTrigger};
use aruna_core::time::unix_timestamp_millis;
use ulid::Ulid;

use super::*;
use crate::plugins::install::tests::Fixture;
use crate::plugins::runs::tests::queued;
use crate::plugins::runs::{admit, read_run};
use crate::plugins::worker::tests::{connected, install_artifact, worker};

/// Answers every request with `answer` and records what it was asked.
pub(crate) struct Fake {
    pub answer: Answer,
    pub asked: Mutex<Vec<(OutgoingRequest, Vec<u8>)>>,
}

#[async_trait]
impl Outbound for Fake {
    async fn send(&self, request: OutgoingRequest, body: Vec<u8>) -> Result<Answer, SendError> {
        self.asked.lock().unwrap().push((request, body));
        Ok(self.answer.clone())
    }
}

pub(crate) fn fake(status: u16, body: &str) -> Arc<Fake> {
    Arc::new(Fake {
        answer: Answer {
            status,
            headers: vec![(
                "location".to_string(),
                "https://elsewhere.example.org/".to_string(),
            )],
            body: body.as_bytes().to_vec(),
        },
        asked: Mutex::new(Vec::new()),
    })
}

/// Runs the fixture's `probe` handler once with `input` and answers the finished run.
pub(crate) async fn probe(
    fixture: &Fixture,
    install: &PluginInstall,
    outbound: Arc<dyn Outbound>,
    input: serde_json::Value,
) -> aruna_core::plugin_runs::PluginRun {
    let mut run = queued(install.install_id, 1);
    run.run_id = Ulid::generate();
    (run.digest, run.settings) = (install.digest.clone(), install.settings.clone());
    (run.handler, run.due_ms) = ("probe".to_string(), 0);
    run.trigger = RunTrigger::Manual {
        by: fixture.actor.user_id,
        input: input.to_string(),
    };
    admit(&fixture.ctx, &run).await.unwrap();
    let worker = worker(fixture);
    worker.use_outbound(outbound);
    worker.claim(unix_timestamp_millis()).await.unwrap();
    worker.settle().await;
    read_run(&fixture.ctx, install.install_id, run.run_id)
        .await
        .unwrap()
}

#[tokio::test]
async fn sends_below_connection() {
    let fixture = Fixture::new().await;
    let install = install_artifact(&fixture, connected()).await;
    let answer = r#"{"id":7,"secret":"s","meta":{"name":"n","token":"t"}}"#;
    let sender = fake(200, answer);
    let input = serde_json::json!({
        "call": "send", "connection": "api", "method": "POST", "path": "records", "body": {"a": 1}
    });
    let done = probe(&fixture, &install, sender.clone(), input).await;
    assert_eq!(done.state, RunState::Succeeded, "{:?}", done.message);
    let output: serde_json::Value = serde_json::from_str(&done.output.unwrap()).unwrap();
    // Projected once: the listed pointers and nothing beside them.
    let fields = serde_json::json!({"/id": 7, "/meta/name": "n"});
    assert_eq!(output, serde_json::json!({"status": 200, "fields": fields}));
    let (request, body) = sender.asked.lock().unwrap()[0].clone();
    assert_eq!(request.url.as_str(), "https://api.example.org/v1/records");
    assert_eq!(
        (request.method.as_str(), body.as_slice()),
        ("POST", br#"{"a":1}"#.as_slice())
    );

    let redirect = fake(302, answer);
    let input = serde_json::json!({"call": "send", "connection": "api", "path": "x"});
    let moved = probe(&fixture, &install, redirect, input).await;
    let output: serde_json::Value = serde_json::from_str(&moved.output.unwrap()).unwrap();
    assert_eq!(output, serde_json::json!({"status": 302, "fields": {}}));

    let escape = serde_json::json!({"call": "send", "connection": "api", "path": "../admin"});
    let refused = probe(&fixture, &install, fake(200, answer), escape).await;
    assert_eq!(refused.state, RunState::Failed);
    let message = refused.message.unwrap_or_default();
    assert!(message.contains("below the connection"), "{message}");
}

#[tokio::test]
async fn sends_to_admitted() {
    use crate::plugins::bound::tests::edit_config;
    let fixture = Fixture::new().await;
    let install = install_artifact(&fixture, connected()).await;
    // The address setting changes after the run was admitted with the old one.
    edit_config(&fixture, |config| {
        config.installs[0].settings = r#"{"api_url":"https://moved.example.org"}"#.into();
    })
    .await;
    let sender = fake(200, "{}");
    let input = serde_json::json!({"call": "send", "connection": "api", "path": "records"});
    let done = probe(&fixture, &install, sender.clone(), input).await;
    assert_eq!(done.state, RunState::Succeeded, "{:?}", done.message);
    let (request, _) = sender.asked.lock().unwrap()[0].clone();
    assert_eq!(request.url.as_str(), "https://api.example.org/v1/records");
}

/// Answers every request as too large to read, and counts the sends.
struct Oversized(Mutex<usize>);

#[async_trait]
impl Outbound for Oversized {
    async fn send(&self, _: OutgoingRequest, _: Vec<u8>) -> Result<Answer, SendError> {
        *self.0.lock().unwrap() += 1;
        Err(SendError::TooLarge { status: 201 })
    }
}

#[tokio::test]
async fn oversized_answers_kept() {
    let fixture = Fixture::new().await;
    let install = install_artifact(&fixture, connected()).await;
    let sender = Arc::new(Oversized(Mutex::new(0)));
    let input = serde_json::json!({
        "call": "send", "connection": "api", "method": "POST", "path": "records", "body": {}
    });
    let done = probe(&fixture, &install, sender.clone(), input).await;
    let message = done.message.clone().unwrap_or_default();
    assert!(message.contains("larger than the call allows"), "{message}");
    assert_eq!(*sender.0.lock().unwrap(), 1);
    // The marker is stored with the intent, so a rerun answers it and never sends again.
    let kept = String::from_utf8_lossy(&stored(&fixture).await).into_owned();
    assert!(kept.contains("too_large"), "{kept}");
}

#[tokio::test]
async fn undeclared_method() {
    use crate::plugins::broker::RunBroker;
    use crate::plugins::check::RunScope;
    use aruna_plugins::broker::{Broker, CallContext, Method, SendRequest};
    let fixture = Fixture::new().await;
    let install = install_artifact(&fixture, connected()).await;
    let mut run = queued(install.install_id, 1);
    run.digest = install.digest.clone();
    let scope = RunScope {
        realm_id: fixture.actor.realm_id,
        local: fixture.actor.node_id,
        run,
    };
    let sender = fake(200, "{}");
    let broker = RunBroker::new(Arc::new(fixture.ctx.clone()), scope).with_outbound(sender.clone());
    let cx = CallContext {
        run: "run".to_string(),
        install: install.install_id.to_string(),
        max_bytes: 1 << 20,
    };
    let request = SendRequest {
        connection: "api".to_string(),
        method: Method::Put,
        path: "records".to_string(),
        query: Vec::new(),
        headers: Vec::new(),
        body: Vec::new(),
        intent: None,
    };
    let refused = broker.connection_send(&cx, request).await.unwrap_err();
    assert_eq!(refused.kind, aruna_plugins::broker::ErrorKind::Invalid);
    assert!(sender.asked.lock().unwrap().is_empty());
}

/// The secured fixture installed with its slot bound to the group secret `api-key`, whose
/// one rule lets this install inject it toward the fixture's address with `pointers`.
async fn secured_install(fixture: &Fixture, pointers: &[&str]) -> PluginInstall {
    use crate::plugins::bound::tests::{edit_config, secret_with};
    use crate::plugins::worker::tests::{publish_artifact, secured};
    install_artifact(fixture, connected()).await;
    let digest = publish_artifact(fixture, &secured()).await;
    let binding = aruna_core::plugin_installs::SecretBinding::parse("group:api-key").unwrap();
    edit_config(fixture, |config| {
        let install = &mut config.installs[0];
        (install.digest, install.version) = (digest, "0.2.1".to_string());
        install.secrets = [("api_key".to_string(), binding)].into();
    })
    .await;
    let config = crate::plugins::install::read_config(&fixture.ctx, fixture.actor.realm_id);
    let install = config.await.unwrap().installs.remove(0);
    secret_with(fixture, &install, exposure(pointers)).await;
    install
}

fn exposure(pointers: &[&str]) -> aruna_core::secrets::ResponseExposure {
    use aruna_core::secrets::ResponseExposure;
    match pointers.is_empty() {
        true => ResponseExposure::Status,
        false => ResponseExposure::Fields {
            pointers: pointers.iter().map(|pointer| pointer.to_string()).collect(),
            headers: Vec::new(),
        },
    }
}

/// Every stored byte of the run's record and intents, to look for a canary.
async fn stored(fixture: &Fixture) -> Vec<u8> {
    use aruna_core::keyspaces::{PLUGIN_INTENTS_KEYSPACE, PLUGIN_RUNS_KEYSPACE};
    let txn = crate::plugins::store::Txn::direct(&fixture.ctx);
    let mut bytes = Vec::new();
    for space in [PLUGIN_RUNS_KEYSPACE, PLUGIN_INTENTS_KEYSPACE] {
        for (_, value) in txn.scan(space, &[]).await.unwrap() {
            bytes.extend(value);
        }
    }
    bytes
}

#[tokio::test]
async fn injects_without_showing() {
    use crate::plugins::bound::tests::{TOKEN, keys};
    let fixture = Fixture::keyed().await;
    let install = secured_install(&fixture, &["/id", "/meta/name"]).await;
    // The destination echoes the token back in a field the code may read.
    let echo = format!(r#"{{"id":7,"meta":{{"name":"{TOKEN}"}},"items":[1,2]}}"#);
    let sender = fake(200, &echo);
    let input = serde_json::json!({
        "call": "send", "connection": "api", "method": "POST", "path": "records", "body": {"a": 1}
    });
    let worker = worker(&fixture);
    worker.use_keys(keys());
    let done = probe_with(&fixture, &install, (&worker, sender.clone()), input).await;
    assert_eq!(done.state, RunState::Succeeded, "{:?}", done.message);
    let (request, _) = sender.asked.lock().unwrap()[0].clone();
    let auth = (request.headers.iter()).find(|(name, _)| name == "authorization");
    assert_eq!(
        auth.map(|(_, value)| value.as_str()),
        Some(format!("Bearer {TOKEN}").as_str())
    );
    let output: serde_json::Value = serde_json::from_str(done.output.as_ref().unwrap()).unwrap();
    let fields = serde_json::json!({"/id": 7, "/meta/name": "[redacted]"});
    assert_eq!(output, serde_json::json!({"status": 200, "fields": fields}));
    let kept = stored(&fixture).await;
    assert!(
        !kept
            .windows(TOKEN.len())
            .any(|window| window == TOKEN.as_bytes())
    );
}

/// Runs `probe` once on `worker` and answers the finished run.
async fn probe_with(
    fixture: &Fixture,
    install: &PluginInstall,
    (worker, outbound): (&Arc<crate::plugins::worker::Worker>, Arc<Fake>),
    input: serde_json::Value,
) -> aruna_core::plugin_runs::PluginRun {
    let mut run = queued(install.install_id, 1);
    run.run_id = Ulid::generate();
    (run.digest, run.settings) = (install.digest.clone(), install.settings.clone());
    (run.handler, run.due_ms) = ("probe".to_string(), 0);
    run.trigger = RunTrigger::Manual {
        by: fixture.actor.user_id,
        input: input.to_string(),
    };
    admit(&fixture.ctx, &run).await.unwrap();
    worker.use_outbound(outbound);
    worker.claim(unix_timestamp_millis()).await.unwrap();
    worker.settle().await;
    read_run(&fixture.ctx, install.install_id, run.run_id)
        .await
        .unwrap()
}

#[tokio::test]
async fn replays_only_narrow() {
    use crate::plugins::bound::tests::keys;
    use crate::plugins::rules::{RuleGuard, consumer, edit_rules};
    let fixture = Fixture::keyed().await;
    let install = secured_install(&fixture, &["/id", "/meta/name"]).await;
    let sender = fake(200, r#"{"id":7,"meta":{"name":"n"}}"#);
    let input = serde_json::json!({
        "call": "send", "connection": "api", "method": "POST", "path": "records"
    });
    let worker = worker(&fixture);
    worker.use_keys(keys());
    let mut run = probe_with(&fixture, &install, (&worker, sender.clone()), input).await;
    let fields = |run: &aruna_core::plugin_runs::PluginRun| {
        let output: serde_json::Value = serde_json::from_str(run.output.as_ref().unwrap()).unwrap();
        output["fields"].clone()
    };
    assert_eq!(
        fields(&run),
        serde_json::json!({"/id": 7, "/meta/name": "n"})
    );
    let name = aruna_core::secrets::SecretName::try_from("api-key".to_string()).unwrap();
    let guard = RuleGuard::Revision {
        install_id: install.install_id,
        revision: install.revision,
    };
    // Each retry answers the settled call from its stored reply under the rule of that moment.
    for (pointers, expected) in [
        (
            vec!["/id", "/meta/name"],
            serde_json::json!({"/id": 7, "/meta/name": "n"}),
        ),
        (vec!["/id"], serde_json::json!({"/id": 7})),
        (vec![], serde_json::json!({})),
    ] {
        let mut rule = crate::plugins::bound::tests::rule_for(&install, exposure(&pointers));
        rule.paths = vec!["/v1".to_string()];
        let target = (install.group_id, &name);
        edit_rules(
            &fixture.ctx,
            &fixture.actor,
            target,
            (&consumer(&install), vec![rule]),
            guard,
        )
        .await
        .unwrap();
        (run.state, run.output, run.finished_ms) = (RunState::Queued, None, None);
        admit(&fixture.ctx, &run).await.unwrap();
        worker.claim(unix_timestamp_millis()).await.unwrap();
        worker.settle().await;
        run = read_run(&fixture.ctx, install.install_id, run.run_id)
            .await
            .unwrap();
        assert_eq!(fields(&run), expected, "{pointers:?}");
    }
    assert_eq!(
        sender.asked.lock().unwrap().len(),
        1,
        "a settled call is never sent again"
    );
}

#[tokio::test]
async fn whole_answers_gated() {
    use crate::plugins::bound::tests::{bound, edit_config, keys, secret_with};
    use crate::plugins::broker::RunBroker;
    use crate::plugins::check::RunScope;
    use aruna_core::secrets::ResponseExposure;
    use aruna_plugins::broker::{Broker, CallContext, ErrorKind, Method, SendRequest};
    let fixture = Fixture::keyed().await;
    // A connection that reads whole answers with the bearer slot `api_key`.
    let install = bound(&fixture).await;
    let whole = serde_json::json!({
        "secrets": { "api_key": { "title": "API key", "kind": "bearer" } },
        "connections": { "catalog": { "url": "{catalog_url}/api/", "auth": "api_key", "response": "full" } }
    });
    let digest = fixture.publish("0.3.0", "ef", whole).await;
    edit_config(&fixture, |config| {
        (config.installs[0].digest, config.installs[0].version) = (digest, "0.3.0".to_string());
    })
    .await;
    let config = crate::plugins::install::read_config(&fixture.ctx, fixture.actor.realm_id);
    let install = config
        .await
        .unwrap()
        .installs
        .into_iter()
        .find(|i| i.install_id == install.install_id)
        .unwrap();
    secret_with(&fixture, &install, ResponseExposure::Status).await;
    let send = || {
        let ctx = Arc::new(fixture.ctx.clone());
        let mut run = queued(install.install_id, 1);
        (run.digest, run.settings) = (install.digest.clone(), install.settings.clone());
        let scope = RunScope {
            realm_id: fixture.actor.realm_id,
            local: fixture.actor.node_id,
            run,
        };
        let sender = fake(200, r#"{"id":1}"#);
        let broker = RunBroker::new(ctx, scope)
            .with_outbound(sender)
            .with_keys(keys());
        let install = &install;
        async move {
            let cx = CallContext {
                run: "run".to_string(),
                install: install.install_id.to_string(),
                max_bytes: 1 << 20,
            };
            let request = SendRequest {
                connection: "catalog".to_string(),
                method: Method::Get,
                path: "records".to_string(),
                query: Vec::new(),
                headers: Vec::new(),
                body: Vec::new(),
                intent: None,
            };
            broker.connection_send_full(&cx, request).await
        }
    };
    use crate::plugins::rules::{RuleGuard, consumer, edit_rules};
    let name = aruna_core::secrets::SecretName::try_from("api-key".to_string()).unwrap();
    let guard = RuleGuard::Revision {
        install_id: install.install_id,
        revision: install.revision,
    };
    let set_rule = |response| {
        let mut rule = crate::plugins::bound::tests::rule_for(&install, response);
        (rule.origins, rule.paths) = (
            vec!["https://catalog.example.org".into()],
            vec!["/api".into()],
        );
        let (ctx, actor, install, name) = (&fixture.ctx, &fixture.actor, &install, &name);
        async move {
            let target = (install.group_id, name);
            edit_rules(ctx, actor, target, (&consumer(install), vec![rule]), guard)
                .await
                .unwrap();
        }
    };
    set_rule(ResponseExposure::Status).await;
    let narrower = send().await.unwrap_err();
    assert_eq!(narrower.kind, ErrorKind::Denied);
    assert!(
        narrower.message.contains("whole answer"),
        "{}",
        narrower.message
    );
    // Once the rule exposes the whole answer, it reaches the code.
    set_rule(ResponseExposure::Full).await;
    let answer = send().await.unwrap();
    assert_eq!(
        (answer.status, answer.body.as_slice()),
        (200, br#"{"id":1}"#.as_slice())
    );
}

/// The keyspaces with a row that holds `canary` in plain bytes.
pub(crate) async fn leaks(fixture: &Fixture, canary: &str) -> Vec<&'static str> {
    use aruna_core::keyspaces::KEYSPACE_CATALOG;
    let txn = crate::plugins::store::Txn::direct(&fixture.ctx);
    let held = |bytes: &Vec<u8>| {
        bytes
            .windows(canary.len())
            .any(|part| part == canary.as_bytes())
    };
    let mut found = Vec::new();
    for space in KEYSPACE_CATALOG {
        let Ok(rows) = txn.scan(space, &[]).await else {
            continue;
        };
        if rows.iter().any(|(key, value)| held(key) || held(value)) {
            found.push(*space);
        }
    }
    found
}

#[tokio::test]
async fn canaries_stay_hidden() {
    use crate::plugins::bound::tests::{TOKEN, keys};
    use aruna_core::keyspaces::ADMIN_AUDIT_KEYSPACE;
    use aruna_core::structs::identity::admin_audit::AdminAuditRecord;
    let fixture = Fixture::keyed().await;
    let install = secured_install(&fixture, &["/id", "/meta/name"]).await;
    // The destination echoes the token in a readable field and in a header.
    let echo = format!(r#"{{"id":7,"meta":{{"name":"Bearer {TOKEN}"}}}}"#);
    let sender = Arc::new(Fake {
        answer: Answer {
            status: 200,
            headers: vec![("x-echo".to_string(), format!("Bearer {TOKEN}"))],
            body: echo.into_bytes(),
        },
        asked: Mutex::new(Vec::new()),
    });
    let input = serde_json::json!({
        "call": "send", "connection": "api", "method": "POST", "path": "records", "body": {}
    });
    let worker = worker(&fixture);
    worker.use_keys(keys());
    let done = probe_with(&fixture, &install, (&worker, sender), input).await;
    assert_eq!(done.state, RunState::Succeeded, "{:?}", done.message);
    // Run records, guest log lines, intents and audit rows hold no form of the value; the scan
    // does find what is stored, such as the plugin's name.
    assert_eq!(leaks(&fixture, TOKEN).await, Vec::<&str>::new());
    assert!(!leaks(&fixture, &install.plugin).await.is_empty());
    let txn = crate::plugins::store::Txn::direct(&fixture.ctx);
    let audit = txn.scan(ADMIN_AUDIT_KEYSPACE, &[]).await.unwrap();
    let uses = (audit.iter())
        .filter_map(|(_, value)| AdminAuditRecord::from_bytes(value).ok())
        .filter(|record| record.secret_use.is_some());
    assert!(uses.count() > 0, "the use left no audit row");
}
