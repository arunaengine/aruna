//! Tests the per call check: the receiver, the account, the record, the grants and the
//! authorization as the account, each refusing on its own.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::auth::install_access::covers;
use crate::plugins::calls::*;
use crate::plugins::test_support::{install_as, install_request};
use crate::tests::calls::{compiled, values_of};
use aruna_drivers::driver::drive;
use aruna_test_support::plugins::Fixture;
use aruna_test_support::plugins::queued;

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
    let mut request = install_request(&fixture, None);
    request.grants.insert("write".into(), Vec::new());
    let installed = install_as(&fixture, &request).await.unwrap();
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

/// The rows that let the account of `install` read below `pattern`: the realm and group
/// authorization, then the realm config, the group and its authorization without policies.
pub(crate) fn access_rows(install: &PluginInstall, pattern: &str) -> [IoResult; 2] {
    use aruna_core::structs::identity::auth::{Actor, Role};
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
    use std::collections::{HashMap, HashSet};
    let realm_id = install.account.realm_id;
    let actor = Actor {
        node_id: install.receiver,
        user_id: install.account,
        realm_id,
    };
    let realm = RealmAuthorizationDocument {
        realm_id,
        roles: HashMap::new(),
        operation_restrictions: HashMap::new(),
    };
    let role = Role {
        role_id: Ulid::from_parts(9, 9),
        name: "readers".to_string(),
        permissions: HashMap::from([(pattern.to_string(), Access::READ)]),
        assigned_users: HashSet::from([install.account]),
    };
    let auth = GroupAuthorizationDocument {
        group_id: install.group_id,
        roles: HashMap::from([(role.role_id, role)]),
        policies: Vec::new(),
    };
    let group = Group {
        display_name: "readers".to_string(),
        group_id: install.group_id,
        realm_id,
        roles: HashSet::new(),
        owner: install.account,
    };
    let config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    let auth = auth.to_bytes(&actor).unwrap();
    let rows = |rows: Vec<Vec<u8>>| values_of(rows.into_iter().map(Some).collect());
    [
        rows(vec![realm.to_bytes(&actor).unwrap(), auth.clone()]),
        rows(vec![
            config.to_bytes(&actor).unwrap(),
            group.to_bytes(&actor).unwrap(),
            auth,
        ]),
    ]
}

/// The test install with read on everything below `root`.
fn reader(root: &str) -> PluginInstall {
    let mut install = crate::plugins::work::tests::sample_install(Ulid::from_parts(1, 1));
    let pattern = vec![format!("{root}/**")];
    install.grants = std::collections::BTreeMap::from([("read".to_string(), pattern)]);
    install
}

fn read_check(install: &PluginInstall, path: &str) -> Authorize {
    let patterns = matching(install, Permission::MetadataRead, path).unwrap();
    Authorize::call(install, Permission::MetadataRead, path, &patterns)
}

fn meta_root() -> String {
    let install = crate::plugins::work::tests::sample_install(Ulid::from_parts(1, 1));
    format!("/{}/g/{}/meta", install.account.realm_id, install.group_id)
}

// The second check of a group decides from the rows and the policies the first one read.
#[test]
fn checks_share_reads() {
    let root = meta_root();
    let install = reader(&root);
    let [rules, policies] = access_rows(&install, &format!("{root}/**"));
    let mut checks = SharedChecks::default();
    let first = checks.start(read_check(&install, &format!("{root}/a")));
    assert!(matches!(first, Checked::Ask(IoCall::Read(_))));
    let Ok(Checked::Ask(IoCall::Read(_))) = checks.answer(rules) else {
        panic!("allowed rules read the policies next");
    };
    let Ok(Checked::Ask(compile)) = checks.answer(policies) else {
        panic!("read policies compile next");
    };
    assert_eq!(checks.answer(compiled(compile)), Ok(Checked::Done(Ok(()))));
    let second = checks.start(read_check(&install, &format!("{root}/b")));
    assert_eq!(second, Checked::Done(Ok(())));
}

// A refusal for missing rules refuses the next check of the group without a second read.
#[test]
fn refusals_share_reads() {
    let root = meta_root();
    let install = reader(&root);
    let mut checks = SharedChecks::default();
    checks.start(read_check(&install, &format!("{root}/a")));
    let Ok(Checked::Done(Err(CallError(first)))) = checks.answer(values_of(vec![None, None]))
    else {
        panic!("missing rules decide the check");
    };
    let second = checks.start(read_check(&install, &format!("{root}/b")));
    let Checked::Done(Err(CallError(second))) = second else {
        panic!("the second check reads nothing");
    };
    assert_eq!(
        (first.kind, second.kind),
        (ErrorKind::Denied, ErrorKind::Denied)
    );
}
