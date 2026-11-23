//! The plugin test helpers whose signatures hold operations types, over the shared fixture.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::plugins::account::ReadGroupAuth;
use crate::plugins::calls::install_plugin;
use crate::plugins::calls::prepare_install;
use crate::plugins::calls::write_record;
use crate::plugins::check::RunScope;
use crate::plugins::install::InstallError;
use crate::plugins::install::InstallRequest;
use crate::plugins::install::new_record;
use crate::plugins::rows::StoreError;
use crate::plugins::rows::decode;
use crate::plugins::rows::read_rows;
use crate::plugins::rows::write_rows;
use crate::plugins::rules::EditRules;
use crate::plugins::rules::RuleError;
use crate::plugins::rules::RuleGuard;
use crate::plugins::submit::CopyRequest;
use crate::plugins::work::row_of;
use aruna_core::install_work::WorkRow;
use aruna_core::install_work::index_key;
use aruna_core::keyspaces::PLUGIN_WORK_KEYSPACE;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::secrets::SecretConsumer;
use aruna_core::secrets::SecretName;
use aruna_core::secrets::UseRule;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::auth::Role;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::GroupId;
use aruna_runner::driver::DriverContext;
use aruna_runner::driver::drive;
use aruna_test_harness::plugins::Fixture;
use aruna_test_harness::plugins::install_run;
use aruna_test_harness::plugins::path;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::HashMap;
use ulid::Ulid;

/// The install of the published `announcer` that tests change before they install it.
pub fn install_request(fixture: &Fixture, install_id: Option<Ulid>) -> InstallRequest {
    let settings = json!({ "catalog_url": "https://catalog.example.org" });
    InstallRequest {
        plugin: "announcer".into(),
        group_id: fixture.group_id,
        version: None,
        settings: settings.as_object().cloned().unwrap_or_default(),
        grants: BTreeMap::new(),
        install_id,
        secrets: BTreeMap::new(),
    }
}

pub async fn install_as(
    fixture: &Fixture,
    request: &InstallRequest,
) -> Result<PluginInstall, InstallError> {
    install_plugin(&fixture.ctx, &fixture.actor, &fixture.auth(), request).await
}

pub async fn group_roles(fixture: &Fixture) -> HashMap<Ulid, Role> {
    drive(ReadGroupAuth(fixture.group_id), &fixture.ctx)
        .await
        .unwrap()
        .unwrap()
        .roles
}

pub async fn group_role(fixture: &Fixture, install: &PluginInstall) -> Option<Role> {
    group_roles(fixture).await.get(&install.role_id).cloned()
}

/// The first preparation step alone, as a crash right after it leaves the install.
pub async fn prepared_only(fixture: &Fixture, install_id: Ulid) -> PluginInstall {
    let auth = fixture.auth();
    let request = install_request(fixture, Some(install_id));
    let prepared = prepare_install(&fixture.ctx, &auth, fixture.actor.realm_id, &request).await;
    let record = new_record(
        &fixture.actor,
        &prepared.unwrap(),
        (fixture.actor.node_id, (Ulid::generate(), Ulid::generate())),
    );
    write_record(&fixture.ctx, &fixture.actor, &auth, None, record, None)
        .await
        .unwrap()
}

/// Sets the rules of one consumer on a group secret, now.
pub async fn edit_rules(
    ctx: &DriverContext,
    actor: &Actor,
    target: (GroupId, &SecretName),
    rules: (&SecretConsumer, Vec<UseRule>),
    guard: RuleGuard,
) -> Result<bool, RuleError> {
    let now_ms = unix_timestamp_millis();
    drive(EditRules::new(actor, target, rules, guard, now_ms), ctx).await
}

pub fn scope(fixture: &Fixture, install: &PluginInstall) -> RunScope {
    RunScope {
        realm_id: fixture.actor.realm_id,
        local: fixture.actor.node_id,
        run: install_run(install),
    }
}

pub fn request(fixture: &Fixture, install: &PluginInstall, target: &str) -> CopyRequest {
    CopyRequest {
        source: path(fixture, install, "in/a.txt"),
        source_version: None,
        target: path(fixture, install, target),
        ordinal: 0,
    }
}

/// Changes the row of work `ids` as a test needs; `None` when it is gone.
pub async fn edit_work(
    ctx: &DriverContext,
    (install_id, work_id): (Ulid, Ulid),
    change: impl FnOnce(&mut WorkRow),
) -> Result<Option<WorkRow>, StoreError> {
    let index = vec![(PLUGIN_WORK_KEYSPACE, index_key(install_id, work_id))];
    let [key] = <[_; 1]>::try_from(drive(read_rows(index), ctx).await?).unwrap();
    let Some(key) = key else {
        return Ok(None);
    };
    let read = vec![(PLUGIN_WORK_KEYSPACE, key.clone())];
    let [value] = <[_; 1]>::try_from(drive(read_rows(read), ctx).await?).unwrap();
    let Some(value) = value else {
        return Ok(None);
    };
    let mut row: WorkRow = decode(&value)?;
    change(&mut row);
    drive(write_rows(vec![row_of(key, &row)?]), ctx).await?;
    Ok(Some(row))
}
