//! Tests prepared installs: activation, resuming after each step, refusals, held accounts,
//! foreign roles, grant checks against roles and policies, and overlapping retries.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use crate::groups::add_role::{AddRoleConfig, AddRoleOperation};
use crate::groups::create_group::{CreateGroupConfig, CreateGroupOperation};
use crate::plugins::account::{EnsureAccount, EnsureRole};
use crate::plugins::calls::*;
use crate::plugins::publish::{PublishEntryConfig, PublishEntryOperation};
use crate::plugins::rows::read_rows;
#[allow(unused_imports)]
pub(crate) use crate::plugins::test_support::{Fixture, REALM_KEY};
use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_core::admin_documents::AdminDocumentTarget;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::keyspaces::{AUTH_KEYSPACE, DOCUMENT_STATE_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::request_policy::{PolicyKind, RequestPolicy};
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::identity::auth::{Permission as Access, Role};
use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
use aruna_core::structs::identity::user::User;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use serde_json::json;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::{TempDir, tempdir};

#[tokio::test]
async fn installs_and_resumes() {
    let fixture = Fixture::new().await;
    let (auth, first) = (fixture.auth(), fixture.request(None));
    let preview = prepare_install(&fixture.ctx, &auth, fixture.actor.realm_id, &first).await;
    let id = preview.unwrap().scope.install_id;
    assert!(
        fixture.installs().await.is_empty(),
        "a preview writes nothing"
    );

    let request = fixture.request(Some(id));
    let installed = fixture.install(&request).await.unwrap();
    assert_eq!(
        (installed.state, installed.revision),
        (InstallState::Active, 2)
    );
    assert_eq!(installed.prerequisites.len(), 2);
    assert_ne!(
        installed.account.user_ulid, id,
        "the account id is not the caller's"
    );
    assert_ne!(installed.role_id, id, "the role id is not the caller's");
    assert_eq!(fixture.installs().await, vec![installed.clone()]);
    let account = fixture.account(&installed).await.unwrap();
    assert_eq!(account.service_group(), Some(fixture.group_id));
    assert_eq!(account.install(), Some(id));
    assert!(!account.is_deactivated());
    assert!(account.name.starts_with("plugin announcer (Lab A)"));
    let role = fixture.role(&installed).await.unwrap();
    assert_eq!(role.name, "plugin-write");
    assert_eq!(role.assigned_users, HashSet::from([installed.account]));

    assert_eq!(
        fixture.install(&request).await.unwrap(),
        installed,
        "a retry answers the same"
    );
    // A newer version published meanwhile does not change what a retry without version means.
    fixture.publish("0.1.1", "cd", json!({})).await;
    assert_eq!(fixture.install(&request).await.unwrap(), installed);
    let mut other = request.clone();
    other.grants.insert("write".into(), Vec::new());
    assert!(matches!(
        fixture.install(&other).await,
        Err(InstallError::Changed)
    ));
}

#[tokio::test]
async fn resumes_after_steps() {
    for stop in 1..=4 {
        let fixture = Fixture::new().await;
        let (ctx, actor, auth) = (&fixture.ctx, &fixture.actor, fixture.auth());
        let id = Ulid::from_parts(9, stop);
        let record = fixture.prepared_only(id).await;
        let change = AccountChange::of((actor, &auth), &record, 1);
        if stop >= 2 {
            let ensure = EnsureAccount::new(change.clone(), "x".into());
            drive(ensure, ctx).await.unwrap();
        }
        if stop >= 3 {
            let paths = role_paths(&parse_grants(&record.grants));
            drive(EnsureRole::new(change.clone(), paths), ctx)
                .await
                .unwrap();
        }
        if stop == 4 {
            drive(Teardown::new(change), ctx).await.unwrap();
            assert!(fixture.account(&record).await.unwrap().is_deactivated());
            assert!(fixture.role(&record).await.is_none());
        }
        assert_eq!(fixture.installs().await[0].state, InstallState::Preparing);
        let installed = fixture.install(&fixture.request(Some(id))).await.unwrap();
        assert_eq!(
            installed.state,
            InstallState::Active,
            "stop after step {stop}"
        );
        assert!(!fixture.account(&installed).await.unwrap().is_deactivated());
        assert!(fixture.role(&installed).await.is_some());
    }
}

#[tokio::test]
async fn refuses_requests() {
    let fixture = Fixture::new().await;
    let mut outsider = fixture.auth();
    outsider.user_id = UserId::local(Ulid::from_parts(4, 4), fixture.actor.realm_id);
    let realm_id = fixture.actor.realm_id;
    let refused = prepare_install(&fixture.ctx, &outsider, realm_id, &fixture.request(None)).await;
    assert!(
        matches!(refused, Err(InstallError::Unauthorized)),
        "{refused:?}"
    );

    let mut unknown = fixture.request(None);
    unknown.plugin = "other".into();
    assert!(matches!(
        fixture.install(&unknown).await,
        Err(InstallError::NotPublished)
    ));
    let mut outside = fixture.request(None);
    let other_group = format!("/{realm_id}/g/{}/meta/**", Ulid::from_parts(5, 5));
    outside.grants.insert("read".into(), vec![other_group]);
    assert!(matches!(
        fixture.install(&outside).await,
        Err(InstallError::Grant(_))
    ));
    let mut settings = fixture.request(None);
    settings.settings.clear();
    assert!(matches!(
        fixture.install(&settings).await,
        Err(InstallError::Setting(_))
    ));

    let secret = fixture
        .publish(
            "0.1.1",
            "cd",
            json!({ "secrets": { "key": { "title": "Key" } } }),
        )
        .await;
    let mut secrets = fixture.request(None);
    secrets.version = Some(secret);
    // A slot whose secret does not exist is refused before anything is written.
    assert!(matches!(
        fixture.install(&secrets).await,
        Err(InstallError::Binding(_))
    ));
    assert!(fixture.installs().await.is_empty());
}

#[tokio::test]
async fn refuses_held_accounts() {
    // An account id that already holds a role is never adopted: neither a realm role nor
    // another role of the group.
    let fixture = Fixture::new().await;
    let record = fixture.prepared_only(Ulid::from_parts(9, 1)).await;
    fixture.realm_roles(&[record.account]).await;
    let held = fixture
        .install(&fixture.request(Some(record.install_id)))
        .await;
    assert!(
        matches!(
            held,
            Err(InstallError::Step {
                step: "account",
                ..
            })
        ),
        "{held:?}"
    );
    assert!(
        fixture.account(&record).await.is_none(),
        "no account is created"
    );
    assert_eq!(
        fixture.installs().await[0].failed.as_deref(),
        Some("account")
    );

    let fixture = Fixture::new().await;
    let record = fixture.prepared_only(Ulid::from_parts(9, 2)).await;
    let extra = AddRoleOperation::new(AddRoleConfig {
        auth_context: fixture.auth(),
        actor: fixture.actor.clone(),
        realm_id: fixture.actor.realm_id,
        group_id: fixture.group_id,
        role: Role {
            role_id: Ulid::from_parts(8, 8),
            name: "extra".into(),
            permissions: HashMap::new(),
            assigned_users: HashSet::from([record.account]),
        },
    });
    drive(extra, &fixture.ctx).await.unwrap();
    let held = fixture
        .install(&fixture.request(Some(record.install_id)))
        .await;
    assert!(
        matches!(
            held,
            Err(InstallError::Step {
                step: "account",
                ..
            })
        ),
        "{held:?}"
    );
}

#[tokio::test]
async fn keeps_foreign_roles() {
    // An install id equal to an existing role's id leaves that role alone.
    let fixture = Fixture::new().await;
    let roles = fixture.roles().await;
    let (admin_id, admin) = (roles.iter())
        .find(|(_, role)| role.name == "admin")
        .unwrap();
    let installed = fixture
        .install(&fixture.request(Some(*admin_id)))
        .await
        .unwrap();
    assert_ne!(installed.role_id, *admin_id);
    assert_eq!(fixture.roles().await.get(admin_id), Some(admin));
}

#[tokio::test]
async fn checks_policies() {
    let fixture = Fixture::new().await;
    let meta = format!("/{}/g/{}/meta", fixture.actor.realm_id, fixture.group_id);
    // A policy that reads the path: a subtree cannot be proven, exact paths can.
    fixture
        .deny(&[&format!("path.startsWith('{meta}/private')")])
        .await;
    let refused = fixture.install(&fixture.request(None)).await;
    assert!(
        matches!(refused, Err(InstallError::NotHeld(_))),
        "{refused:?}"
    );
    let mut exact = fixture.request(None);
    exact
        .grants
        .insert("read".into(), vec![format!("{meta}/runs")]);
    exact.grants.insert("write".into(), Vec::new());
    assert!(fixture.install(&exact).await.is_ok());

    // A policy on one action refuses that action only, with its own access and operation.
    let fixture = Fixture::new().await;
    fixture
        .deny(&["operation == 'plugin.metadata.write' && permission == 'write'"])
        .await;
    let refused = fixture.install(&fixture.request(None)).await;
    assert!(
        matches!(refused, Err(InstallError::NotHeld(_))),
        "{refused:?}"
    );
    let mut read_only = fixture.request(None);
    read_only.grants.insert("write".into(), Vec::new());
    assert!(fixture.install(&read_only).await.is_ok());
}

#[tokio::test]
async fn serializes_retries() {
    let fixture = Fixture::new().await;
    let request = fixture.request(Some(Ulid::from_parts(9, 3)));
    let (first, second) = tokio::join!(fixture.install(&request), fixture.install(&request));
    let (first, second) = (first.unwrap(), second.unwrap());
    assert_eq!(first, second);
    assert_eq!(first.state, InstallState::Active);
    assert!(!fixture.account(&first).await.unwrap().is_deactivated());
    assert!(fixture.role(&first).await.is_some());
}

#[test]
fn accesses_per_action() {
    use crate::plugins::grants::accesses;
    use aruna_plugins::permission::Permission;
    assert_eq!(
        accesses(Permission::ObjectsCopy),
        [Access::READ, Access::WRITE]
    );
    assert_eq!(accesses(Permission::MetadataRead), [Access::READ]);
}

/// A storage that passes every effect to `real` and fails the commit numbered `fail_at`, counting
/// from 0; `seen` counts the commits so far.
fn failing_commit(
    real: StorageHandle,
    seen: Arc<AtomicUsize>,
    fail_at: Arc<AtomicUsize>,
) -> StorageHandle {
    let (handle, receivers) = StorageHandle::new();
    for receiver in [receivers.foreground, receivers.bulk] {
        let (real, seen, fail_at) = (real.clone(), seen.clone(), fail_at.clone());
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            while let Ok((effect, response, ..)) = receiver.recv() {
                let commit = matches!(effect, StorageEffect::CommitTransaction { .. });
                let failing =
                    commit && seen.fetch_add(1, Ordering::SeqCst) == fail_at.load(Ordering::SeqCst);
                let event = match failing {
                    true => aruna_core::events::StorageEvent::Error {
                        error: aruna_core::errors::StorageError::PersistError("injected".into()),
                    },
                    false => match runtime.block_on(real.send_storage_effect(effect)) {
                        aruna_core::events::Event::Storage(event) => event,
                        other => panic!("unexpected event: {other:?}"),
                    },
                };
                response.send(event);
            }
        });
    }
    handle
}

/// A fixture whose commits are counted in the first value and fail at the second.
async fn counted() -> (Fixture, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let (seen, fail_at) = (
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(usize::MAX)),
    );
    let (inner_seen, inner_fail) = (seen.clone(), fail_at.clone());
    let fixture = Fixture::on(move |storage| failing_commit(storage, inner_seen, inner_fail)).await;
    (fixture, seen, fail_at)
}

#[tokio::test]
async fn failed_activation() {
    // The activation is an install's last commit. Count a whole install's commits on one node,
    // then fail exactly that commit on another: the install stays preparing, a retry finishes.
    let id = Ulid::from_parts(9, 4);
    let (probe, seen, _) = counted().await;
    let start = seen.load(Ordering::SeqCst);
    probe.install(&probe.request(Some(id))).await.unwrap();
    let commits = seen.load(Ordering::SeqCst) - start;

    let (fixture, seen, fail_at) = counted().await;
    fail_at.store(seen.load(Ordering::SeqCst) + commits - 1, Ordering::SeqCst);
    let request = fixture.request(Some(id));
    let failed = fixture.install(&request).await;
    assert!(matches!(failed, Err(InstallError::Record(_))), "{failed:?}");
    let record = fixture.installs().await.remove(0);
    assert_eq!(record.state, InstallState::Preparing);
    assert!(fixture.account(&record).await.is_some());
    assert!(fixture.role(&record).await.is_some());
    let retried = fixture.install(&request).await.unwrap();
    assert_eq!(retried.state, InstallState::Active);
}

#[tokio::test]
async fn copy_needs_both() {
    // `objects.copy` needs both READ and WRITE: a policy denying either one refuses `write`.
    for denied in ["read", "write"] {
        let fixture = Fixture::new().await;
        let copy = json!({ "permissions": { "required": ["metadata.read"], "optional": ["objects.copy"] } });
        let digest = fixture.publish("0.1.1", "cd", copy).await;
        let mut request = fixture.request(None);
        request.version = Some(digest);
        let rule = format!("operation == 'plugin.objects.copy' && permission == '{denied}'");
        fixture.deny(&[&rule]).await;
        let refused = fixture.install(&request).await;
        assert!(
            matches!(refused, Err(InstallError::NotHeld(_))),
            "{denied}: {refused:?}"
        );
        request.grants.insert("write".into(), Vec::new());
        assert!(fixture.install(&request).await.is_ok(), "{denied}");
    }
}

#[tokio::test]
async fn keeps_receiver() {
    // The install records an eligible holder of the group's metadata; no write may move it.
    use crate::plugins::receiver::{eligible, pick_receiver};
    let fixture = Fixture::new().await;
    let (ctx, actor, auth) = (&fixture.ctx, &fixture.actor, fixture.auth());
    let installed = fixture.install(&fixture.request(None)).await.unwrap();
    assert_eq!(installed.receiver, actor.node_id);
    let mut moved = installed.clone();
    moved.receiver = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    moved.revision += 1;
    let refused = write_record(ctx, actor, &auth, Some(installed.revision), moved, None).await;
    let changed = matches!(
        refused,
        Err(InstallError::Record(RecordError::ReceiverChanged))
    );
    assert!(changed, "{refused:?}");

    let mut config = read_config(ctx, actor.realm_id).await.unwrap();
    assert!(!eligible(
        &config,
        iroh::SecretKey::from_bytes(&[9u8; 32]).public()
    ));
    let entry = config
        .placement_map
        .iter_mut()
        .find(|e| e.node_id == actor.node_id);
    entry.unwrap().draining = true;
    assert!(!eligible(&config, actor.node_id));
    assert_eq!(
        pick_receiver(&config, actor.realm_id, fixture.group_id),
        None
    );
}

#[tokio::test]
async fn picks_among_holders() {
    // With several holders the pick is the same whatever the entry order, and skips a draining one.
    use crate::plugins::receiver::{eligible, pick_receiver};
    use aruna_core::structs::identity::realm::RealmNodeKind;
    use aruna_core::structs::placement::record::NodePlacementEntry;
    let fixture = Fixture::new().await;
    let (realm, group) = (fixture.actor.realm_id, fixture.group_id);
    let mut config = read_config(&fixture.ctx, realm).await.unwrap();
    let (node, entry) = (config.nodes[0].clone(), config.placement_map[0].clone());
    for seed in [3u8, 4] {
        let node_id = iroh::SecretKey::from_bytes(&[seed; 32]).public();
        let mut added = node.clone();
        added.node_id = node_id.to_string();
        added.kind = RealmNodeKind::Server;
        config.nodes.push(added);
        config.placement_map.push(NodePlacementEntry {
            node_id,
            ..entry.clone()
        });
    }
    for strategy in &mut config.strategies {
        strategy.replica_count = Some(2);
    }
    config.candidate_maps.clear();
    config.placement_activations.clear();
    let mut reordered = config.clone();
    reordered.nodes.reverse();
    reordered.placement_map.reverse();
    config.snapshot_candidate_map();
    reordered.snapshot_candidate_map();
    let picked = pick_receiver(&config, realm, group).unwrap();
    assert!(eligible(&config, picked));
    assert_eq!(pick_receiver(&reordered, realm, group), Some(picked));

    let entry = (config.placement_map.iter_mut()).find(|entry| entry.node_id == picked);
    entry.unwrap().draining = true;
    let next = pick_receiver(&config, realm, group);
    assert!(
        next.is_some_and(|next| next != picked && eligible(&config, next)),
        "{next:?}"
    );
}

fn summary_scope() -> InstallScope {
    InstallScope {
        realm_id: aruna_core::structs::identity::realm::RealmId::from_bytes([1u8; 32]),
        group_id: Ulid::from_parts(1, 1),
        install_id: Ulid::from_parts(2, 2),
    }
}

#[test]
fn summary_warnings() {
    let warned = |triggers: Value, binding: Option<&str>| {
        let manifest = serde_json::json!({"name": "p", "version": "0.1.0", "triggers": triggers});
        let manifest: Manifest = serde_json::from_value(manifest).unwrap();
        let bound = (binding.into_iter())
            .map(|text| ("key".to_string(), SecretBinding::parse(text).unwrap()))
            .collect();
        summary_of(
            &manifest,
            "sha256:ab",
            &summary_scope(),
            (&BTreeMap::new(), &bound),
        )
        .warnings
    };
    // Sync triggers are delivered now and warn about nothing; a shared secret still warns.
    let sync = serde_json::json!({"sync_completed": {}, "sync_failed": {}});
    assert!(warned(sync.clone(), None).is_empty());
    assert!(warned(sync.clone(), Some("group:key")).is_empty());
    assert_eq!(
        warned(sync, Some("realm:key")),
        vec!["a realm admin adds the use rule of `realm:key` for `key`".to_string()]
    );
}

#[test]
fn summary_lists_methods() {
    let manifest = serde_json::json!({"name": "p", "version": "0.1.0", "connections": {
        "catalog": {"url": "https://c.example.org/", "methods": ["GET", "POST"]}}});
    let manifest: Manifest = serde_json::from_value(manifest).unwrap();
    let none = (&BTreeMap::new(), &BTreeMap::new());
    let lines = summary_of(&manifest, "sha256:ab", &summary_scope(), none).connections;
    assert_eq!(lines[0].methods, ["GET", "POST"]);
}

#[test]
fn summary_stored_answers() {
    // Only a download through a connection with a secret stores answers the rule screens.
    let stored = |required: Value| {
        let manifest = serde_json::json!({"name": "p", "version": "0.1.0",
            "permissions": {"required": required},
            "connections": {
                "files": {"url": "https://f.example.org/", "auth": "token"},
                "public": {"url": "https://p.example.org/"}}});
        let manifest: Manifest = serde_json::from_value(manifest).unwrap();
        let none = (&BTreeMap::new(), &BTreeMap::new());
        let lines = summary_of(&manifest, "sha256:ab", &summary_scope(), none).connections;
        lines.iter().map(|line| line.stored).collect::<Vec<_>>()
    };
    let download = serde_json::json!(["transfers.download"]);
    assert_eq!(stored(download), [true, false]);
    assert_eq!(stored(serde_json::json!([])), [false, false]);
}

mod operations {
    use serde_json::json;

    use super::*;
    use crate::plugins::fixtures::{self, config_result, is_child};
    use crate::plugins::rows::tests::{child_result, values_result};

    fn open_manifest() -> Value {
        json!({ "permissions": { "required": [] } })
    }

    fn request(install: &PluginInstall) -> InstallRequest {
        InstallRequest {
            plugin: install.plugin.clone(),
            group_id: install.group_id,
            version: None,
            settings: Map::new(),
            grants: BTreeMap::new(),
            install_id: Some(install.install_id),
            secrets: BTreeMap::new(),
        }
    }

    fn prepared(install: &PluginInstall) -> Prepared {
        let plugin = fixtures::plugin(open_manifest());
        Prepared {
            scope: scope_of(fixtures::realm(), install),
            manifest: read_manifest(&plugin).unwrap(),
            plugin,
            settings: "{}".into(),
            grants: Grants::new(),
            secrets: BTreeMap::new(),
            warnings: Vec::new(),
            request: install.request.clone(),
        }
    }

    fn write_done() -> IoResult {
        child_result(Ok::<Option<PluginInstall>, RecordError>(None))
    }

    fn admin_ok() -> IoResult {
        child_result(Ok::<Option<User>, ServiceCheckError>(None))
    }

    #[test]
    fn prepare_runs_steps() {
        let install = fixtures::install();
        let config = fixtures::config(&install, json!({}));
        let place = (fixtures::realm(), Some(fixtures::node(3)));
        let auth = fixtures::caller().auth;
        let mut prepare = PrepareInstall::new(&auth, place, &request(&install));
        assert!(is_child(&prepare.first()));
        assert!(is_child(&prepare.next(admin_ok())));
        assert!(is_child(&prepare.next(config_result(config))));
        let held = child_result(Ok::<(), InstallError>(()));
        assert!(is_child(&prepare.next(held)));
        let bound = (
            BTreeMap::<String, SecretBinding>::new(),
            vec!["note".to_string()],
        );
        let Ok(Next::Done(done)) = prepare.next(child_result(Ok::<_, BindError>(bound))) else {
            panic!("the bound slots end the preparation");
        };
        assert_eq!(done.scope.install_id, install.install_id);
        assert_eq!(done.warnings, ["note"]);
    }

    #[test]
    fn prepare_refuses_strangers() {
        let install = fixtures::install();
        let place = (fixtures::realm(), None);
        let auth = fixtures::caller().auth;
        let mut prepare = PrepareInstall::new(&auth, place, &request(&install));
        prepare.first().unwrap();
        let refused = child_result(Err::<Option<User>, _>(ServiceCheckError::Refused));
        assert_eq!(prepare.next(refused), Err(InstallError::Unauthorized));
        let mut prepare = PrepareInstall::new(&auth, place, &request(&install));
        prepare.first().unwrap();
        prepare.next(admin_ok()).unwrap();
        let mut empty = fixtures::config(&install, json!({}));
        empty.plugins.clear();
        let answer = prepare.next(config_result(empty));
        assert_eq!(answer, Err(InstallError::NotPublished));
    }

    fn started(
        install: &PluginInstall,
        stored: &PluginInstall,
    ) -> Result<Next<PluginInstall>, InstallError> {
        let mut run = InstallPrepared::new(fixtures::caller(), prepared(install));
        assert_eq!(
            run.hold_scope(),
            Some(HoldScope::PluginInstall(install.install_id))
        );
        assert!(is_child(&run.first()));
        run.next(config_result(fixtures::config(stored, json!({}))))
    }

    #[test]
    fn install_checks_existing() {
        let install = fixtures::install();
        let mut other = install.clone();
        other.request = "another".into();
        assert_eq!(started(&install, &other), Err(InstallError::Changed));
        let mut removing = install.clone();
        removing.state = InstallState::Removing;
        let refused = InstallError::State("being removed");
        assert_eq!(started(&install, &removing), Err(refused));

        let mut run = InstallPrepared::new(fixtures::caller(), prepared(&install));
        run.first().unwrap();
        let config = fixtures::config(&install, json!({}));
        assert!(is_child(&run.next(config_result(config))));
        let ready = child_result(Ok::<_, AccountError>(Vec::<Prerequisite>::new()));
        assert_eq!(run.next(ready), Ok(Next::Done(install)));
    }

    #[test]
    fn install_settles_preparing() {
        let mut install = fixtures::install();
        install.state = InstallState::Preparing;
        let mut run = InstallPrepared::new(fixtures::caller(), prepared(&install));
        run.first().unwrap();
        let config = fixtures::config(&install, json!({}));
        assert!(is_child(&run.next(config_result(config))));
        let settled = child_result(Ok::<_, InstallError>(install.clone()));
        assert_eq!(run.next(settled), Ok(Next::Done(install)));
    }

    #[test]
    fn install_needs_receiver() {
        let install = fixtures::install();
        let mut run = InstallPrepared::new(fixtures::caller(), prepared(&install));
        run.first().unwrap();
        let empty = RealmConfigDocument::new(fixtures::realm(), Vec::new(), 3);
        let answer = run.next(config_result(empty));
        assert!(matches!(answer, Err(InstallError::Unavailable(_))));
    }

    #[test]
    fn settle_writes_state() {
        let mut record = fixtures::install();
        record.state = InstallState::Preparing;
        record.active_since_ms = None;
        let mut settle = SettleInstall::new(fixtures::caller(), record.clone(), None);
        assert!(is_child(&settle.first()));
        let config = fixtures::config(&record, open_manifest());
        assert!(is_child(&settle.next(config_result(config))));
        let ready = child_result(Ok::<_, StepError>(Vec::<Prerequisite>::new()));
        assert!(is_child(&settle.next(ready)));
        let rules = child_result(Ok::<(), RulesFailure>(()));
        assert!(is_child(&settle.next(rules)));
        let Ok(Next::Done(next)) = settle.next(write_done()) else {
            panic!("the write ends the settling");
        };
        assert_eq!(next.state, InstallState::Active);
        assert_eq!(next.revision, record.revision + 1);
        assert_eq!(next.active_since_ms, Some(5_000));
        assert_eq!(next.activated_by, Some(fixtures::node(3)));
    }

    #[test]
    fn settle_abandons_failure() {
        let record = fixtures::install();
        let mut settle = SettleInstall::new(fixtures::caller(), record.clone(), None);
        settle.first().unwrap();
        settle
            .next(config_result(fixtures::config(&record, json!({}))))
            .unwrap();
        let failed = StepError {
            step: "role",
            error: "no".into(),
        };
        let failed = child_result(Err::<Vec<Prerequisite>, _>(failed));
        assert!(is_child(&settle.next(failed)));
        let answer = settle.next(child_result(Ok::<(), IoError>(())));
        let step = InstallError::Step {
            step: "role",
            error: "no".into(),
        };
        assert_eq!(answer, Err(step));
    }

    #[test]
    fn abandon_undoes_record() {
        let record = fixtures::install();
        let mut abandon = AbandonInstall::new(fixtures::caller(), &record, "role");
        assert!(is_child(&abandon.first()));
        let config = fixtures::config(&record, json!({}));
        assert!(is_child(&abandon.next(config_result(config))));
        let torn = child_result(Err::<(), _>(AccountError("no".into())));
        assert!(is_child(&abandon.next(torn)));
        assert!(is_child(
            &abandon.next(child_result(Ok::<(), RulesFailure>(())))
        ));
        assert_eq!(abandon.next(write_done()), Ok(Next::Done(())));
    }

    #[test]
    fn abandon_leaves_moved() {
        let record = fixtures::install();
        let mut moved = record.clone();
        moved.revision += 1;
        let mut abandon = AbandonInstall::new(fixtures::caller(), &record, "role");
        abandon.first().unwrap();
        let config = fixtures::config(&moved, json!({}));
        assert_eq!(abandon.next(config_result(config)), Ok(Next::Done(())));
    }

    fn holder(pattern: &str) -> CheckHolder {
        let install = fixtures::install();
        let scope = scope_of(fixtures::realm(), &install);
        let pattern = format!("{}/meta/{pattern}", scope.group());
        let grants = Grants::from([(Grant::Read, [pattern].into())]);
        let config = fixtures::config(&install, json!({}));
        CheckHolder::new(&fixtures::caller().auth, (&scope, &grants), &config)
    }

    #[test]
    fn holder_reads_rules() {
        let mut check = holder("a");
        assert!(matches!(check.first(), Ok(Next::Call(IoCall::Read(_)))));
        let missing = check.next(values_result(vec![None, None]));
        assert!(matches!(missing, Err(InstallError::Unavailable(_))));
        let mut check = holder("a");
        check.phase = HolderPhase::Group;
        let failed = child_result(Err::<Option<GroupAuthorizationDocument>, _>(AccountError(
            "no".into(),
        )));
        assert_eq!(
            check.next(failed),
            Err(InstallError::Unavailable("no".into()))
        );
    }

    #[test]
    fn holder_refuses_unheld() {
        let mut check = holder("a");
        check.rules = Some(PermissionRules::default());
        assert!(matches!(check.advance(), Err(InstallError::NotHeld(_))));
        let mut subtree = holder("**");
        subtree.rules = Some(PermissionRules::default());
        subtree.path_policies = true;
        let Err(InstallError::NotHeld(what)) = subtree.advance() else {
            panic!("a policy reading the path refuses a subtree");
        };
        assert!(what.contains("reads the path"));
        let mut none = holder("a");
        none.patterns.clear();
        none.rules = Some(PermissionRules::default());
        assert_eq!(none.advance(), Ok(Next::Done(())));
    }
}
