//! Tests the worker with the runtime fixture: a run that succeeds, one left with an unknown
//! call, the per install bound, and a stop that leaves a run to start again from the top.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use super::*;
use crate::driver::drive;
use crate::plugins::copies::store_local;
use crate::plugins::install::InstallRequest;
use crate::plugins::install::tests::Fixture;
use crate::plugins::intents::mark_unknown;
use crate::plugins::publish::{PublishEntryConfig, PublishEntryOperation};
use crate::plugins::runs::admit;
use crate::plugins::runs::tests::queued;
use aruna_core::plugins::PublishedPlugin;
use aruna_plugins::artifact::{ArtifactDigest, attach_manifest, read_manifest};
use aruna_plugins::broker::Intent;

const FIXTURE: &[u8] = include_bytes!("../../../plugins/tests/fixture.wasm");

fn leb(mut value: usize, out: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        match value {
            0 => return out.push(byte),
            _ => out.push(byte | 0x80),
        }
    }
}

/// The fixture without its secret and connection, which installs refuse.
fn runnable() -> Vec<u8> {
    let manifest = read_manifest(FIXTURE).unwrap();
    let json = serde_json::to_vec(&manifest).unwrap();
    let mut content = Vec::new();
    leb("aruna-plugin".len(), &mut content);
    content.extend_from_slice(b"aruna-plugin");
    content.extend_from_slice(&json);
    let mut section = vec![0u8];
    leb(content.len(), &mut section);
    section.extend_from_slice(&content);
    let component = FIXTURE.strip_suffix(section.as_slice()).unwrap();
    let mut narrow = manifest;
    narrow.secrets.clear();
    narrow.connections.clear();
    attach_manifest(component, &narrow).unwrap()
}

/// An install of the runnable fixture whose artifact this node keeps.
pub(crate) async fn installed(fixture: &Fixture) -> PluginInstall {
    let bytes = runnable();
    let manifest = read_manifest(&bytes).unwrap();
    let digest = ArtifactDigest::of(&bytes).to_string();
    let plugin = PublishedPlugin {
        name: manifest.name.clone(),
        version: manifest.version.clone(),
        digest: digest.clone(),
        size: bytes.len() as u64,
        manifest: serde_json::to_string(&manifest).unwrap(),
        published_by: fixture.actor.user_id,
        published_at_ms: 1,
    };
    let publish = PublishEntryOperation::new(PublishEntryConfig {
        actor: fixture.actor.clone(),
        auth_context: fixture.auth(),
        plugin,
        copies: BTreeSet::from([fixture.actor.node_id]),
    });
    drive(publish, &fixture.ctx).await.unwrap();
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    store_local(&fixture.ctx, &config, &digest, &bytes, 1)
        .await
        .unwrap();
    let settings = serde_json::json!({ "api_url": "https://api.example.org" });
    let request = InstallRequest {
        plugin: manifest.name,
        group_id: fixture.group_id,
        version: Some(digest),
        settings: settings.as_object().cloned().unwrap_or_default(),
        grants: BTreeMap::new(),
        install_id: None,
    };
    fixture.install(&request).await.unwrap()
}

/// A run of `install` that calls `handler` and is due now.
async fn admitted(
    fixture: &Fixture,
    install: &PluginInstall,
    seq: u64,
    handler: &str,
) -> PluginRun {
    let mut run = queued(install.install_id, seq);
    run.digest = install.digest.clone();
    run.settings = install.settings.clone();
    run.handler = handler.into();
    run.due_ms = 0;
    admit(&fixture.ctx, &run).await.unwrap();
    run
}

fn worker(fixture: &Fixture) -> Arc<Worker> {
    let ctx = Arc::new(fixture.ctx.clone());
    Worker::new(ctx, fixture.actor.realm_id, fixture.actor.node_id).unwrap()
}

#[tokio::test]
async fn finishes_runs() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let ok = admitted(&fixture, &install, 1, "echo").await;
    let unsure = admitted(&fixture, &install, 2, "echo").await;
    let intent = Intent::new(0, None, &[b"patch"]);
    mark_unknown(&fixture.ctx, unsure.run_id, &intent, "timed out")
        .await
        .unwrap();
    let worker = worker(&fixture);
    assert_eq!(worker.claim(unix_timestamp_millis()).await.unwrap(), 2);
    worker.settle().await;
    let done = read_run(&fixture.ctx, install.install_id, ok.run_id)
        .await
        .unwrap();
    assert_eq!(done.state, RunState::Succeeded, "{:?}", done.message);
    assert!(
        done.output
            .unwrap_or_default()
            .starts_with(&ok.run_id.to_string())
    );
    assert_eq!(done.attempts, 1);
    let open = read_run(&fixture.ctx, install.install_id, unsure.run_id)
        .await
        .unwrap();
    assert_eq!(open.state, RunState::NeedsAttention);
    assert!(unfinished(&fixture.ctx).await.unwrap().is_empty());
}

#[tokio::test]
async fn bounded_and_restarted() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let run = admitted(&fixture, &install, 1, "spin").await;
    let first = worker(&fixture);
    for n in 0..RUNS_PARALLEL as u128 {
        let busy = (install.install_id, CancellationToken::new());
        first.running().insert(Ulid::from_parts(900, n), busy);
    }
    assert_eq!(first.claim(unix_timestamp_millis()).await.unwrap(), 0);
    first.running().clear();
    assert_eq!(first.claim(unix_timestamp_millis()).await.unwrap(), 1);
    first.stop().await;
    let cut = read_run(&fixture.ctx, install.install_id, run.run_id)
        .await
        .unwrap();
    assert_eq!((cut.state, cut.attempts), (RunState::Running, 1));

    // The next start runs it again from the top.
    let second = worker(&fixture);
    assert_eq!(second.claim(unix_timestamp_millis()).await.unwrap(), 1);
    second.stop().await;
    let again = read_run(&fixture.ctx, install.install_id, run.run_id)
        .await
        .unwrap();
    assert_eq!(again.attempts, 2);
}
