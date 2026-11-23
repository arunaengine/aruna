//! The event and work scenarios plugin tests in operations and in the node share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod cleanup {
    use crate::plugins::Fixture;
    use crate::plugins::rows::Txn;

    pub async fn count(fixture: &Fixture, space: &str) -> usize {
        let txn = Txn::direct(&fixture.ctx);
        txn.scan(space, &[]).await.unwrap().len()
    }
}

pub mod decide {
    use crate::plugins::{Fixture, created};
    use aruna_core::plugin_installs::PluginInstall;
    use aruna_core::plugin_runs::{ForwardRow, PluginRun, SourceEvent};
    use aruna_ops_work::plugins::runs::list_runs;
    use aruna_runner::driver::drive;

    /// A created dataset journaled now, and its forward row for `install`.
    pub fn event(install: &PluginInstall, resource: &str, now: u64) -> (SourceEvent, ForwardRow) {
        let mut event = created(install, resource).0;
        (event.journaled_ms, event.anchor_ms) = (now, now);
        let row = ForwardRow {
            install_id: install.install_id,
            revision: install.revision,
            event: event.clone(),
            queued_ms: now,
        };
        (event, row)
    }

    pub async fn runs(fixture: &Fixture, install: &PluginInstall) -> Vec<PluginRun> {
        let listed = drive(list_runs(install.install_id, None, 1_000), &fixture.ctx).await;
        listed.unwrap().0
    }
}

pub mod events {
    use crate::plugins::{Fixture, created, synced};
    use aruna_core::plugin_installs::PluginInstall;
    use aruna_core::plugin_runs::{RunTrigger, SourceEvent};
    use aruna_ops_work::plugins::runs::list_runs;
    use aruna_runner::driver::drive;
    use ulid::Ulid;

    /// A created dataset journaled at `at`, with its origin anchor there too.
    pub fn dataset(install: &PluginInstall, resource: &str, at: u64) -> SourceEvent {
        let mut event = created(install, resource).0;
        (event.journaled_ms, event.anchor_ms) = (at, at);
        event
    }

    /// The resources of the install's runs, sorted.
    pub async fn ran(fixture: &Fixture, install: &PluginInstall) -> Vec<String> {
        let runs = drive(list_runs(install.install_id, None, 100), &fixture.ctx).await;
        let mut resources: Vec<String> = (runs.unwrap().0.into_iter())
            .filter_map(|run| match run.trigger {
                RunTrigger::Event(event) => Some(event.resource),
                _ => None,
            })
            .collect();
        resources.sort();
        resources
    }

    /// A sync event of `install` journaled at `at`, for the relationship numbered `relationship`.
    pub fn sync_at(install: &PluginInstall, relationship: u128, at: u64) -> SourceEvent {
        let mut event = synced(install, "raw/", Ulid::from_parts(relationship as u64, 1));
        (event.journaled_ms, event.anchor_ms) = (at, at);
        event
    }
}

pub mod submit {
    use crate::plugins::flows::matcher::config;
    use crate::plugins::{Fixture, bound, edit_config};
    use aruna_core::plugin_installs::PluginInstall;
    use aruna_ops_work::s3::object::put::PutObjectOperation;
    use aruna_ops_work::testing_objects::put_config;
    use aruna_runner::driver::{DriverContext, drive};
    use aruna_runner::testing::{seed_bucket, with_blob};

    /// An install granted the group's data whose `catalog` connection injects the slot `api_key`.
    pub async fn fetcher(fixture: &Fixture) -> PluginInstall {
        let install = bound(fixture).await;
        let data = format!("/{}/g/{}/data/**", fixture.actor.realm_id, fixture.group_id);
        edit_config(fixture, |config| {
            config.installs[0].grants.insert("write".into(), vec![data]);
        })
        .await;
        let install = (config(fixture).await.installs.into_iter())
            .find(|found| found.install_id == install.install_id)
            .unwrap();
        seed_bucket(
            &fixture.ctx,
            "in",
            install.group_id,
            install.account,
            Vec::new(),
        )
        .await;
        install
    }

    /// A blob context with the object `in/a.txt` of the fetcher's group.
    pub async fn stored_object(
        fixture: &Fixture,
        install: &PluginInstall,
    ) -> (tempfile::TempDir, DriverContext) {
        let root = tempfile::tempdir().unwrap();
        let blob = with_blob(&fixture.ctx, root.path()).await;
        let (realm, node) = (fixture.actor.realm_id, fixture.actor.node_id);
        let put = put_config(realm, install.group_id, node, "in", "a.txt", b"ACGT");
        drive(PutObjectOperation::new(put), &blob).await.unwrap();
        (root, blob)
    }
}

pub mod matcher {
    use crate::plugins::Fixture;
    use crate::plugins::rows::Txn;
    use aruna_core::keyspaces::{EVENT_JOURNAL_KEYSPACE, PLUGIN_OUTBOX_KEYSPACE};
    use aruna_core::plugin_installs::PluginInstall;
    use aruna_core::plugin_runs::{ForwardRow, SourceEvent, journal_entries};
    use aruna_core::structs::identity::realm::RealmConfigDocument;
    use aruna_ops_work::document_repository::GetConfigOperation;
    use aruna_ops_work::plugins::rows::decode;
    use aruna_runner::driver::drive;

    pub async fn config(fixture: &Fixture) -> RealmConfigDocument {
        let read = GetConfigOperation::new(fixture.actor.realm_id);
        drive(read, &fixture.ctx).await.unwrap()
    }

    /// The forward rows of `install` on this node, in journal order.
    pub async fn forwarded(fixture: &Fixture, install: &PluginInstall) -> Vec<String> {
        let txn = Txn::direct(&fixture.ctx);
        let prefix = install.install_id.to_bytes();
        let rows = txn.scan(PLUGIN_OUTBOX_KEYSPACE, &prefix).await.unwrap();
        (rows.into_iter())
            .map(|(_, value)| decode::<ForwardRow>(&value).unwrap().event.resource)
            .collect()
    }

    /// Journals `event` on this node as a committing transaction would.
    pub async fn journal(fixture: &Fixture, event: &SourceEvent) {
        let rows = (journal_entries(event).unwrap().into_iter())
            .map(|(_, key, value)| (key.to_vec(), value.to_vec()))
            .collect();
        (Txn::direct(&fixture.ctx))
            .write(EVENT_JOURNAL_KEYSPACE, rows)
            .await
            .unwrap();
    }
}
