//! Tests changes of an active install: grants, updates, pause and resume, removal, recovery of
//! a failed change, and the rights an updating caller must hold.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::plugins::account::{ensure_role, guard_of};
use crate::plugins::grants::role_paths;
use crate::plugins::install::repair_install;
use crate::plugins::install::tests::Fixture;
use crate::users::admin_audit::{AuditPageInput, AuditPageOperation};
use aruna_core::structs::identity::admin_audit::AuditIndex;
use aruna_core::structs::identity::auth::{PathRestriction, Permission as Access};
use serde_json::json;

fn needs_objects() -> Value {
    json!({ "permissions": { "required": ["metadata.read", "objects.read"], "optional": ["metadata.write"] } })
}

#[tokio::test]
async fn later_changes() {
    let fixture = Fixture::new().await;
    let (ctx, actor, auth) = (&fixture.ctx, &fixture.actor, fixture.auth());
    let installed = fixture.install(&fixture.request(None)).await.unwrap();
    let id = installed.install_id;
    let meta = format!("/{}/g/{}/meta", actor.realm_id, fixture.group_id);
    let choices = BTreeMap::from([(
        "metadata.write".to_string(),
        vec![format!("{meta}/runs/**")],
    )]);
    let narrowed = set_grants(ctx, actor, &auth, id, &choices).await.unwrap();
    assert_eq!(narrowed.state, InstallState::Active);
    assert_eq!(
        narrowed.grants["metadata.write"],
        vec![format!("{meta}/runs/**")]
    );
    let role = fixture.role(&narrowed).await.unwrap();
    assert_eq!(role.permissions[&format!("{meta}/runs/**")], Access::WRITE);
    assert_eq!(role.permissions[&format!("{meta}/**")], Access::READ);

    let paused = pause_install(ctx, actor, &auth, id).await.unwrap();
    assert_eq!((paused.state, paused.paused), (InstallState::Paused, true));
    assert!(matches!(
        pause_install(ctx, actor, &auth, id).await,
        Err(InstallError::State(_))
    ));
    assert_eq!(
        resume_install(ctx, actor, &auth, id).await.unwrap().state,
        InstallState::Active
    );

    let digest = fixture.publish("0.2.0", "ef", needs_objects()).await;
    let (updated, missing) = update_install(ctx, actor, &auth, id, &digest, None)
        .await
        .unwrap();
    assert_eq!(
        (updated.state, missing),
        (InstallState::Blocked, vec!["objects.read".to_string()])
    );
    assert_eq!(
        updated.grants["metadata.write"],
        vec![format!("{meta}/runs/**")]
    );
    let approved = set_grants(ctx, actor, &auth, id, &BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(approved.state, InstallState::Active);

    remove_install(ctx, actor, &auth, id).await.unwrap();
    assert!(fixture.installs().await.is_empty());
    assert!(fixture.account(&approved).await.unwrap().is_deactivated());
    assert!(fixture.role(&approved).await.is_none());
    let page = AuditPageOperation::new(AuditPageInput {
        index: AuditIndex::Group(fixture.group_id),
        start_after: None,
        limit: 50,
    });
    let actions: Vec<_> = (drive(page, ctx).await.unwrap().records.into_iter())
        .filter(|record| record.plugin.is_some())
        .map(|record| record.action)
        .collect();
    let expected = [
        AdminAuditAction::PluginInstalled,
        AdminAuditAction::PluginGrantsChanged,
        AdminAuditAction::PluginPaused,
        AdminAuditAction::PluginResumed,
        AdminAuditAction::PluginUpdated,
        AdminAuditAction::PluginGrantsChanged,
        AdminAuditAction::PluginRemoved,
    ];
    assert_eq!(actions.len(), expected.len(), "{actions:?}");
    assert!(expected.iter().all(|action| actions.contains(action)));
}

#[tokio::test]
async fn keeps_pause() {
    // A paused install stays paused through an update and the approval of what it needs.
    let fixture = Fixture::new().await;
    let (ctx, actor, auth) = (&fixture.ctx, &fixture.actor, fixture.auth());
    let id = fixture
        .install(&fixture.request(None))
        .await
        .unwrap()
        .install_id;
    pause_install(ctx, actor, &auth, id).await.unwrap();
    let digest = fixture.publish("0.2.0", "ef", needs_objects()).await;
    let (updated, _) = update_install(ctx, actor, &auth, id, &digest, None)
        .await
        .unwrap();
    assert_eq!(
        (updated.state, updated.paused),
        (InstallState::Blocked, true)
    );
    let approved = set_grants(ctx, actor, &auth, id, &BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(
        (approved.state, approved.paused),
        (InstallState::Paused, true)
    );
    let resumed = resume_install(ctx, actor, &auth, id).await.unwrap();
    assert_eq!(
        (resumed.state, resumed.paused),
        (InstallState::Active, false)
    );
}

#[tokio::test]
async fn resume_needs_grants() {
    // Resuming a paused install that misses a required grant leaves it blocked.
    let fixture = Fixture::new().await;
    let (ctx, actor, auth) = (&fixture.ctx, &fixture.actor, fixture.auth());
    let id = fixture
        .install(&fixture.request(None))
        .await
        .unwrap()
        .install_id;
    pause_install(ctx, actor, &auth, id).await.unwrap();
    let digest = fixture.publish("0.2.0", "ef", needs_objects()).await;
    update_install(ctx, actor, &auth, id, &digest, None)
        .await
        .unwrap();
    let resumed = resume_install(ctx, actor, &auth, id).await.unwrap();
    assert_eq!(
        (resumed.state, resumed.paused),
        (InstallState::Blocked, false)
    );
}

#[tokio::test]
async fn checks_updater() {
    // A session restricted to the group's admin path may not carry whole-group grants over.
    let fixture = Fixture::new().await;
    let (ctx, actor) = (&fixture.ctx, &fixture.actor);
    let id = fixture
        .install(&fixture.request(None))
        .await
        .unwrap()
        .install_id;
    let digest = fixture.publish("0.1.1", "cd", json!({})).await;
    let mut restricted = fixture.auth();
    restricted.path_restrictions = Some(vec![PathRestriction {
        pattern: format!("/{}/g/{}/admin", actor.realm_id, fixture.group_id),
        permission: Access::WRITE,
    }]);
    let refused = update_install(ctx, actor, &restricted, id, &digest, None).await;
    assert!(
        matches!(refused, Err(InstallError::NotHeld(_))),
        "{refused:?}"
    );
    assert_eq!(
        fixture.installs().await[0].digest,
        format!("sha256:{}", "ab".repeat(32))
    );
}

#[tokio::test]
async fn recovers_changes() {
    // A crash after the audited record write leaves the install inactive, never active with a
    // role wider than its grants; the next change finishes it.
    let fixture = Fixture::new().await;
    let (ctx, actor, auth) = (&fixture.ctx, &fixture.actor, fixture.auth());
    let installed = fixture.install(&fixture.request(None)).await.unwrap();
    let id = installed.install_id;
    let meta = format!("/{}/g/{}/meta", actor.realm_id, fixture.group_id);
    let choices = BTreeMap::from([("metadata.write".to_string(), Vec::new())]);
    let scope = scope_of(actor.realm_id, &installed);
    let manifest_grants = narrow_grants(
        &read_manifest(&fixture_plugin(&fixture, &installed).await).unwrap(),
        &scope,
        &choices,
    )
    .unwrap();
    let crashed = preparing(&installed, &manifest_grants);
    let crashed = write_record(ctx, actor, &auth, Some(installed.revision), crashed, None).await;
    assert_eq!(crashed.unwrap().state, InstallState::Preparing);
    let finished = set_grants(ctx, actor, &auth, id, &choices).await.unwrap();
    assert_eq!(finished.state, InstallState::Active);
    let role = fixture.role(&finished).await.unwrap();
    assert_eq!(
        role.permissions[&format!("{meta}/**")],
        Access::READ,
        "write is gone"
    );
}

async fn fixture_plugin(
    fixture: &Fixture,
    install: &PluginInstall,
) -> aruna_core::plugins::PublishedPlugin {
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    pick_version(&config, &install.plugin, Some(&install.digest)).unwrap()
}

#[tokio::test]
async fn stale_node_changes() {
    // Node A stalls in a grant change while node B finishes another one; A's late role write and
    // cleanup must not break B's newer active install, on either node.
    let fixture = Fixture::new().await;
    let installed = fixture.install(&fixture.request(None)).await.unwrap();
    let id = installed.install_id;
    let (a, b) = fixture.split().await;
    let a_auth = a.auth();
    let scope = scope_of(a.actor.realm_id, &installed);
    let manifest = read_manifest(&fixture_plugin(&a, &installed).await).unwrap();
    let dropped = BTreeMap::from([("metadata.write".to_string(), Vec::new())]);
    let grants_a = narrow_grants(&manifest, &scope, &dropped).unwrap();
    let stale = preparing(&installed, &grants_a);
    let stale = write_record(
        &a.ctx,
        &a.actor,
        &a_auth,
        Some(installed.revision),
        stale,
        None,
    );
    let stale = stale.await.unwrap();
    a.deliver_config(&b).await;

    let meta = format!("/{}/g/{}/meta", b.actor.realm_id, b.group_id);
    let runs = BTreeMap::from([(
        "metadata.write".to_string(),
        vec![format!("{meta}/runs/**")],
    )]);
    let active = set_grants(&b.ctx, &b.actor, &b.auth(), id, &runs)
        .await
        .unwrap();
    assert_eq!(active.state, InstallState::Active);
    let wanted = role_paths(&parse_grants(&active.grants));

    // A writes its role before it learns of B's record; that role reaches B late, and B repairs.
    let identity = Identity::of(&stale);
    let paths_a = role_paths(&grants_a);
    let write = ensure_role(
        &a.ctx,
        &a.actor,
        &a_auth,
        &scope,
        identity,
        &paths_a,
        guard_of(&stale),
    );
    write.await.unwrap();
    a.deliver_group(&b).await;
    assert_ne!(b.role(&active).await.unwrap().permissions, wanted);
    let node = b.actor.node_id;
    assert!(
        repair_install(&b.ctx, node, b.actor.realm_id, &active)
            .await
            .unwrap()
    );
    assert_eq!(b.role(&active).await.unwrap().permissions, wanted);

    // Once A holds B's record, its stale role write and cleanup are refused, and A repairs too.
    b.deliver_config(&a).await;
    let again = ensure_role(
        &a.ctx,
        &a.actor,
        &a_auth,
        &scope,
        identity,
        &paths_a,
        guard_of(&stale),
    );
    assert!(again.await.is_err(), "a stale role write is refused");
    let undo = teardown(
        &a.ctx,
        &a.actor,
        &a_auth,
        &scope,
        identity,
        guard_of(&stale),
    )
    .await;
    assert!(undo.is_err(), "a stale cleanup is refused");
    assert!(!a.account(&active).await.unwrap().is_deactivated());
    assert!(
        repair_install(&a.ctx, node, a.actor.realm_id, &active)
            .await
            .unwrap()
    );
    assert_eq!(a.role(&active).await.unwrap().permissions, wanted);
}
