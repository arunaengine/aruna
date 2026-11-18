//! Tests native work started by a guest, through the plugin host.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_operations::driver::drive;
use aruna_operations::plugins::flow_support::submit::fetcher;
use aruna_operations::plugins::flow_support::submit::send;
use aruna_operations::plugins::flow_support::submit::stored_object;
use aruna_operations::plugins::test_support::Fixture;
use aruna_operations::plugins::test_support::edit_config;
use aruna_operations::plugins::test_support::path;
use aruna_operations::plugins::test_support::ready;
use aruna_operations::plugins::test_support::scope;
use aruna_operations::plugins::work::Identity;
use aruna_operations::plugins::work::WorkKind;
use aruna_operations::plugins::work::WorkRow;
use aruna_operations::plugins::work::read_work;
use aruna_plugins::broker::ErrorKind;
use ulid::Ulid;

#[tokio::test]
async fn broker_starts_copies() {
    // A run names objects by bucket path; the broker expands them, pins a given version, and a
    // rerun of the call gets the same job id.
    use std::sync::Arc;

    use aruna_plugins::broker::{Broker, CallContext, Intent, JobCopy};

    use crate::plugin_host::broker::RunBroker;
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let broker = RunBroker::new(Arc::new(fixture.ctx.clone()), scope(&fixture, &install));
    let cx = CallContext {
        run: "run".into(),
        install: install.install_id.to_string(),
        max_bytes: 1 << 20,
    };
    let copy = |version: Option<&str>| JobCopy {
        source: "in/a.txt".into(),
        version: version.map(str::to_string),
        target: "out/a.txt".into(),
        intent: Intent::new(3, &[b"copy"]),
    };
    let id = broker.jobs_copy(&cx, copy(None)).await.unwrap();
    assert_eq!(broker.jobs_copy(&cx, copy(None)).await.unwrap(), id);
    let row = drive(
        read_work(install.install_id, id.parse().unwrap()),
        &fixture.ctx,
    )
    .await;
    let row = row.unwrap().unwrap();
    assert_eq!(row.source, Some(path(&fixture, &install, "in/a.txt")));
    let bad = broker.jobs_copy(&cx, copy(Some("v1"))).await;
    assert_eq!(bad.map_err(|error| error.kind), Err(ErrorKind::Invalid));
}

#[tokio::test]
async fn shifted_copy_refused() {
    // A retried run that reaches its copy at the ordinal of an earlier state write changed: the
    // call is refused before any work is reserved, and the run fails as changed.
    use std::sync::Arc;

    use aruna_plugins::broker::{Broker, CallContext, Intent, JobCopy, StatePut};

    use crate::plugin_host::broker::RunBroker;
    use aruna_operations::plugins::work::list_work;
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let broker = RunBroker::new(Arc::new(fixture.ctx.clone()), scope(&fixture, &install));
    let cx = CallContext {
        run: "run".into(),
        install: install.install_id.to_string(),
        max_bytes: 1 << 20,
    };
    let put = StatePut {
        key: "init".into(),
        value: b"1".to_vec(),
        expected: None,
        intent: Intent::new(1, &[b"init"]),
    };
    broker.state_put(&cx, put).await.unwrap();
    let copy = JobCopy {
        source: "in/a.txt".into(),
        version: None,
        target: "out/a.txt".into(),
        intent: Intent::new(1, &[b"copy"]),
    };
    let refused = broker.jobs_copy(&cx, copy).await;
    assert_eq!(
        refused.map_err(|error| error.kind),
        Err(ErrorKind::Conflict)
    );
    assert!(broker.changed());
    let (rows, _) = drive(list_work(install.install_id, None, 10), &fixture.ctx)
        .await
        .unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn broker_starts_downloads() {
    // The broker keeps the request with the address from the run's settings, and a rerun of the
    // call gets the same job; a checksum it cannot check is invalid input.
    use std::sync::Arc;

    use aruna_plugins::broker::{Broker, CallContext, Field, Intent, JobDownload};

    use crate::plugin_host::broker::RunBroker;
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let mut scope = scope(&fixture, &install);
    scope.run.settings = r#"{"catalog_url":"https://catalog.example.org"}"#.into();
    let broker = RunBroker::new(Arc::new(fixture.ctx.clone()), scope);
    let cx = CallContext {
        run: "run".into(),
        install: install.install_id.to_string(),
        max_bytes: 1 << 20,
    };
    let download = |checksums: Vec<Field>| JobDownload {
        service: "catalog".into(),
        path: "reads.fq".into(),
        query: Vec::new(),
        headers: Vec::new(),
        target: "in/reads.fq".into(),
        size_limit: Some(9),
        checksums,
        intent: Intent::new(4, &[b"download"]),
    };
    let md5 = |value: &str| Field {
        name: "MD5".into(),
        value: value.into(),
    };
    let digest = "0123456789abcdef0123456789abcdef";
    let id = broker
        .jobs_download(&cx, download(vec![md5(digest)]))
        .await
        .unwrap();
    let again = broker.jobs_download(&cx, download(vec![md5(digest)])).await;
    assert_eq!(again, Ok(id.clone()));
    let row = drive(
        read_work(install.install_id, id.parse().unwrap()),
        &fixture.ctx,
    )
    .await;
    let row = row.unwrap().unwrap();
    assert_eq!(row.target, Some(path(&fixture, &install, "in/reads.fq")));
    let context = row.context.unwrap();
    assert_eq!(
        (context.base.as_str(), context.path.as_str()),
        ("https://catalog.example.org/api/", "reads.fq")
    );
    for wrong in [
        md5("00"),
        md5("not hex"),
        Field {
            name: "md4".into(),
            value: digest.into(),
        },
    ] {
        let refused = broker.jobs_download(&cx, download(vec![wrong])).await;
        assert_eq!(refused.map_err(|error| error.kind), Err(ErrorKind::Invalid));
    }
}

#[tokio::test]
async fn narrowed_download_refused() {
    // A rule that shows only the status refuses the download at the call; no work is recorded.
    use std::sync::Arc;

    use aruna_core::secrets::ResponseExposure;
    use aruna_plugins::broker::{Broker, CallContext, Intent, JobDownload};

    use crate::plugin_host::broker::RunBroker;
    use aruna_operations::plugins::test_support::{ORIGIN, hold_keys, secret_with};
    use aruna_operations::plugins::work::list_work;
    let fixture = Fixture::keyed().await;
    let install = fetcher(&fixture).await;
    secret_with(&fixture, &install, ResponseExposure::Status).await;
    let mut scope = scope(&fixture, &install);
    scope.run.settings = format!(r#"{{"catalog_url":"{ORIGIN}"}}"#);
    hold_keys(&fixture.ctx);
    let broker = RunBroker::new(Arc::new(fixture.ctx.clone()), scope);
    let cx = CallContext {
        run: "run".into(),
        install: install.install_id.to_string(),
        max_bytes: 1 << 20,
    };
    let download = JobDownload {
        service: "catalog".into(),
        path: "reads.fq".into(),
        query: Vec::new(),
        headers: Vec::new(),
        target: "in/reads.fq".into(),
        size_limit: None,
        checksums: Vec::new(),
        intent: Intent::new(4, &[b"download"]),
    };
    let refused = broker.jobs_download(&cx, download).await.unwrap_err();
    assert_eq!(refused.kind, ErrorKind::Denied);
    assert_eq!(
        refused.message,
        "this connection does not expose its whole answer"
    );
    let (works, _) = drive(list_work(install.install_id, None, 10), &fixture.ctx)
        .await
        .unwrap();
    assert!(works.is_empty());
}

#[tokio::test]
async fn reply_rechecked() {
    // A kept upload answer is shown while the secret's rule shows whole answers, and dropped,
    // with the state kept, once the rule shows the status only.
    use std::sync::Arc;

    use aruna_core::secrets::{ResponseExposure, SecretName};
    use aruna_plugins::broker::{Broker, CallContext};

    use crate::plugin_host::broker::RunBroker;
    use aruna_operations::plugins::rules::{RuleGuard, consumer};
    use aruna_operations::plugins::test_support::edit_rules;
    use aruna_operations::plugins::test_support::{ORIGIN, hold_keys, rule_for, secret_with};
    use aruna_operations::plugins::work::{WorkChange, reserve, update_work};
    let fixture = Fixture::keyed().await;
    fetcher(&fixture).await;
    let puts = serde_json::json!({
        "secrets": { "api_key": { "title": "API key", "kind": "bearer" } },
        "connections": { "catalog": { "url": "{catalog_url}/api/", "auth": "api_key",
            "methods": ["GET", "PUT"] } }
    });
    let digest = fixture.publish("0.3.0", "ef", puts).await;
    edit_config(&fixture, |config| {
        (config.installs[0].digest, config.installs[0].version) = (digest, "0.3.0".into());
    })
    .await;
    let config =
        aruna_operations::plugins::calls::read_config(&fixture.ctx, fixture.actor.realm_id);
    let install = config.await.unwrap().installs.remove(0);
    secret_with(&fixture, &install, ResponseExposure::Full).await;
    let mut row = WorkRow::reserved(&install, WorkKind::Upload, Ulid::from_parts(9, 9), 1);
    let mut context = send(&fixture, &install, None).context;
    context.base = format!("{ORIGIN}/api/");
    row.context = Some(context);
    let identity = Identity::Call {
        run_id: row.run_id,
        ordinal: 0,
    };
    let (row, _) = drive(
        reserve((&install, &identity), row, None).unwrap(),
        &fixture.ctx,
    )
    .await
    .unwrap();
    let reply = r#"{"status":201,"headers":[["etag","e1"]],"body":"eyJpZCI6N30="}"#;
    let ids = (install.install_id, row.work_id);
    let answer = (201, Some(reply.into()));
    let sent = WorkChange::Sent {
        status: answer.0,
        reply: answer.1,
        bytes: 8,
        version: Ulid::nil(),
    };
    drive(update_work(ids, sent), &fixture.ctx).await.unwrap();
    hold_keys(&fixture.ctx);
    let broker = RunBroker::new(Arc::new(fixture.ctx.clone()), scope(&fixture, &install));
    let cx = CallContext {
        run: "run".into(),
        install: install.install_id.to_string(),
        max_bytes: 1 << 20,
    };
    let id = row.work_id.to_string();
    let shown = broker.jobs_get(&cx, &id).await.unwrap();
    assert_eq!(shown.reply_status, Some(201));
    assert_eq!(shown.reply_body.as_deref(), Some(&br#"{"id":7}"#[..]));
    assert_eq!(shown.reply_headers[0].value, "e1");
    let name = SecretName::try_from("api-key".to_string()).unwrap();
    let guard = RuleGuard::Revision {
        install_id: install.install_id,
        revision: install.revision,
    };
    let rule = rule_for(&install, ResponseExposure::Status);
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
    let narrowed = broker.jobs_get(&cx, &id).await.unwrap();
    assert_eq!((narrowed.reply_status, narrowed.reply_body), (None, None));
    assert_eq!(narrowed.state, shown.state);
}

#[tokio::test]
async fn broker_starts_uploads() {
    // An upload is a PUT or a POST; a form names its file part after the source's file name.
    use std::sync::Arc;

    use aruna_plugins::broker::{Broker, CallContext, Field, Intent, JobUpload, Method};

    use crate::plugin_host::broker::RunBroker;
    use aruna_operations::plugins::work::read_work;
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let puts = serde_json::json!({
        "permissions": { "required": ["objects.copy"] },
        "connections": { "catalog": { "url": "{catalog_url}/api/", "methods": ["GET", "POST"] } }
    });
    let digest = fixture.publish("0.3.0", "ef", puts).await;
    edit_config(&fixture, |config| {
        (config.installs[0].digest, config.installs[0].version) = (digest, "0.3.0".into());
    })
    .await;
    let config =
        aruna_operations::plugins::calls::read_config(&fixture.ctx, fixture.actor.realm_id);
    let install = (config.await.unwrap().installs.into_iter())
        .find(|found| found.install_id == install.install_id)
        .unwrap();
    let (_root, blob) = stored_object(&fixture, &install).await;
    let mut scope = scope(&fixture, &install);
    scope.run.settings = r#"{"catalog_url":"https://catalog.example.org"}"#.into();
    let broker = RunBroker::new(Arc::new(blob), scope);
    let cx = CallContext {
        run: "run".into(),
        install: install.install_id.to_string(),
        max_bytes: 1 << 20,
    };
    let upload = |method| JobUpload {
        service: "catalog".into(),
        method,
        path: "deposit".into(),
        query: Vec::new(),
        headers: Vec::new(),
        source: "in/a.txt".into(),
        version: None,
        file_field: Some("file".into()),
        fields: vec![Field {
            name: "title".into(),
            value: "Run 7".into(),
        }],
        intent: Intent::new(5, &[b"upload"]),
    };
    let refused = broker
        .jobs_upload(&cx, upload(Method::Get))
        .await
        .unwrap_err();
    assert_eq!(refused.kind, ErrorKind::Invalid);
    let id = broker.jobs_upload(&cx, upload(Method::Post)).await.unwrap();
    let row = drive(
        read_work(install.install_id, id.parse().unwrap()),
        &fixture.ctx,
    )
    .await;
    let row = row.unwrap().unwrap();
    assert_eq!(row.source, Some(path(&fixture, &install, "in/a.txt")));
    assert_eq!(row.context.unwrap().method, "POST");
}
