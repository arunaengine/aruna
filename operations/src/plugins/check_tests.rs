//! Tests the per call check: the receiver, the account, the record, the grants and the
//! authorization as the account, each refusing on its own.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::drive;
use crate::plugins::install::tests::Fixture;
use crate::plugins::lifecycle::pause_install;
use crate::plugins::runs::tests::queued;

fn kind(result: Result<PluginInstall, CallError>) -> Option<ErrorKind> {
    result.err().map(|error| error.0.kind)
}

#[test]
fn covered_paths() {
    assert!(covers("/r/g/a/meta/**", "/r/g/a/meta"));
    assert!(covers("/r/g/a/meta/**", "/r/g/a/meta/x/y"));
    assert!(!covers("/r/g/a/meta/**", "/r/g/a/metadata"));
    assert!(covers("/r/g/a/meta/x", "/r/g/a/meta/x"));
    assert!(!covers("/r/g/a/meta/x", "/r/g/a/meta/x/y"));
}

#[tokio::test]
async fn checks_each_step() {
    let fixture = Fixture::new().await;
    let (ctx, actor) = (&fixture.ctx, &fixture.actor);
    let mut request = fixture.request(None);
    request.grants.insert("write".into(), Vec::new());
    let installed = fixture.install(&request).await.unwrap();
    let mut run = queued(installed.install_id, 1);
    run.digest = installed.digest.clone();
    let scope = RunScope {
        realm_id: actor.realm_id,
        local: actor.node_id,
        run,
    };
    let meta = format!("/{}/g/{}/meta", actor.realm_id, fixture.group_id);
    let read = (Permission::MetadataRead, format!("{meta}/x"));
    let call = |scope: RunScope, target: (Permission, String)| async move {
        drive(CheckCall::new(&scope, Some((target.0, &target.1))), ctx).await
    };
    assert!(call(scope.clone(), read.clone()).await.is_ok());
    assert!(drive(CheckCall::new(&scope, None), ctx).await.is_ok());

    let write = (Permission::MetadataWrite, format!("{meta}/x"));
    assert_eq!(
        kind(call(scope.clone(), write).await),
        Some(ErrorKind::NotGranted)
    );
    let elsewhere = (
        Permission::MetadataRead,
        format!("/{}/g/x/meta/y", actor.realm_id),
    );
    let refused = call(scope.clone(), elsewhere).await;
    assert_eq!(kind(refused), Some(ErrorKind::NotGranted));

    let mut other = scope.clone();
    other.local = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    assert_eq!(
        kind(call(other, read.clone()).await),
        Some(ErrorKind::Denied)
    );
    let mut moved = scope.clone();
    moved.run.digest = format!("sha256:{}", "ef".repeat(32));
    assert_eq!(
        kind(call(moved, read.clone()).await),
        Some(ErrorKind::Denied)
    );

    fixture.deny(&["operation == 'plugin.metadata.read'"]).await;
    assert_eq!(
        kind(call(scope.clone(), read.clone()).await),
        Some(ErrorKind::Denied)
    );
    fixture.deny(&[]).await;
    assert!(call(scope.clone(), read.clone()).await.is_ok());

    let auth = fixture.auth();
    pause_install(ctx, actor, &auth, installed.install_id)
        .await
        .unwrap();
    assert_eq!(
        kind(drive(CheckCall::new(&scope, None), ctx).await),
        Some(ErrorKind::Denied)
    );
}
