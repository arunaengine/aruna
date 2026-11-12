//! Tests the prunes: old use audit rows go with their index rows while other rows stay, a prune
//! stopped after one page goes on from its cursor, and old or removing webhook rows go.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{hook_key, hook_time_key};
use aruna_core::secrets::{Delivery, SecretName, SecretScope};
use aruna_core::structs::identity::admin_audit::{AdminAuditAction, SecretAudit, SecretUse};

use super::*;
use crate::plugins::bound::tests::edit_config;
use crate::plugins::install::tests::Fixture;
use crate::plugins::store::encode;

const DAY: u64 = 24 * 3600 * 1000;
const NOW: u64 = 100 * DAY;

/// A use row of `install` written at `at_ms`.
fn used(install: &PluginInstall, at_ms: u64) -> AdminAuditRecord {
    let secret = SecretAudit {
        scope: SecretScope::Group(install.group_id),
        name: SecretName::try_from("api-key".to_string()).unwrap(),
        incarnation: None,
        write_id: None,
    };
    let used = SecretUse {
        delivery: Delivery::Inject,
        run_id: None,
        refused: None,
    };
    let mut record = AdminAuditRecord::for_use(install, secret, used);
    record.audit_id = Ulid::from_parts(at_ms, 1);
    record
}

async fn write(fixture: &Fixture, records: &[AdminAuditRecord]) {
    let rows = (records.iter())
        .flat_map(|record| record.entries().unwrap())
        .map(|(_, key, value)| (key.to_vec(), value.to_vec()))
        .collect();
    let txn = Txn::direct(&fixture.ctx);
    txn.write(ADMIN_AUDIT_KEYSPACE, rows).await.unwrap();
}

/// The times of the rows below `index` this test wrote; the fixture's own rows are newer.
async fn ids(fixture: &Fixture, index: AuditIndex) -> Vec<u64> {
    let txn = Txn::direct(&fixture.ctx);
    let rows = txn.scan(ADMIN_AUDIT_KEYSPACE, &audit_prefix(index)).await;
    let rows = rows.unwrap().into_iter();
    rows.map(|(_, value)| AdminAuditRecord::from_bytes(&value).unwrap())
        .map(|record| record.audit_id.timestamp_ms())
        .filter(|at| *at <= NOW)
        .collect()
}

#[tokio::test]
async fn prunes_old_uses() {
    let fixture = Fixture::new().await;
    let install = fixture.install(&fixture.request(None)).await.unwrap();
    let mut kept = AdminAuditRecord::for_install(
        AdminAuditAction::PluginInstalled,
        fixture.actor.user_id,
        &install,
    );
    kept.audit_id = Ulid::from_parts(DAY, 1);
    let uses = [2, 3, 4, 90].map(|day| used(&install, day * DAY));
    write(&fixture, &[kept]).await;
    write(&fixture, &uses).await;
    // One page checks the kept row and the first use; a restart goes on from its cursor.
    assert_eq!(prune_page(&fixture.ctx, NOW, 2).await, Ok((1, true)));
    assert_eq!(ids(&fixture, AuditIndex::Realm).await.len(), 4);
    assert_eq!(prune_uses(&fixture.ctx, NOW).await, Ok(2));
    assert_eq!(prune_uses(&fixture.ctx, NOW).await, Ok(0));
    let left = [DAY, 90 * DAY];
    assert_eq!(ids(&fixture, AuditIndex::Realm).await, left);
    assert_eq!(
        ids(&fixture, AuditIndex::Group(install.group_id)).await,
        left
    );
}

async fn config(fixture: &Fixture) -> RealmConfigDocument {
    let read = crate::plugins::install::read_config(&fixture.ctx, fixture.actor.realm_id);
    read.await.unwrap()
}

#[tokio::test]
async fn prunes_old_hooks() {
    let fixture = Fixture::new().await;
    let install = fixture.install(&fixture.request(None)).await.unwrap();
    let id = install.install_id;
    let rows = [("old", NOW - 8 * DAY), ("new", NOW - DAY)]
        .into_iter()
        .flat_map(|(delivery, at)| {
            [
                (hook_key(id, delivery), encode(&at).unwrap()),
                (hook_time_key(at, id, delivery), Vec::new()),
            ]
        })
        .collect();
    let txn = Txn::direct(&fixture.ctx);
    txn.write(PLUGIN_HOOKS_KEYSPACE, rows).await.unwrap();
    let held = config(&fixture).await;
    assert_eq!(prune_hooks(&fixture.ctx, &held, NOW).await, Ok(1));
    let left = txn.scan(PLUGIN_HOOKS_KEYSPACE, &[]).await.unwrap();
    let keys: Vec<Vec<u8>> = left.into_iter().map(|(key, _)| key).collect();
    assert_eq!(
        keys,
        [hook_key(id, "new"), hook_time_key(NOW - DAY, id, "new")]
    );
    // A removing install's rows go before their time.
    edit_config(&fixture, |config| {
        config.installs[0].state = InstallState::Removing;
    })
    .await;
    let removing = config(&fixture).await;
    assert_eq!(prune_hooks(&fixture.ctx, &removing, NOW).await, Ok(1));
    assert!(
        txn.scan(PLUGIN_HOOKS_KEYSPACE, &[])
            .await
            .unwrap()
            .is_empty()
    );
}
