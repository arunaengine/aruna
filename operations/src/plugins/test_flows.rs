//! The event and work scenarios plugin tests here and in the node share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod cleanup {
    use crate::plugins::store::Txn;
    use crate::plugins::test_support::Fixture;

    pub async fn count(fixture: &Fixture, space: &str) -> usize {
        let txn = Txn::direct(&fixture.ctx);
        txn.scan(space, &[]).await.unwrap().len()
    }
}

pub mod decide {
    use crate::driver::drive;
    use crate::plugins::runs::list_runs;
    use crate::plugins::test_support::Fixture;
    use crate::plugins::test_support::created;
    use aruna_core::plugin_installs::PluginInstall;
    use aruna_core::plugin_runs::ForwardRow;
    use aruna_core::plugin_runs::PluginRun;
    use aruna_core::plugin_runs::SourceEvent;

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
    use crate::driver::drive;
    use crate::plugins::calls::decide_batch;
    use crate::plugins::flow_support::matcher::config;
    use crate::plugins::runs::list_runs;
    use crate::plugins::test_support::Fixture;
    use crate::plugins::test_support::created;
    use crate::plugins::test_support::synced;
    use aruna_core::plugin_installs::PluginInstall;
    use aruna_core::plugin_runs::Decisions;
    use aruna_core::plugin_runs::EventBatch;
    use aruna_core::plugin_runs::ForwardRow;
    use aruna_core::plugin_runs::RunTrigger;
    use aruna_core::plugin_runs::SourceEvent;
    use ulid::Ulid;

    /// A created dataset journaled at `at`, with its origin anchor there too.
    pub fn dataset(install: &PluginInstall, resource: &str, at: u64) -> SourceEvent {
        let mut event = created(install, resource).0;
        (event.journaled_ms, event.anchor_ms) = (at, at);
        event
    }

    /// Decides `rows` on this node as their receiver.
    pub async fn decide(fixture: &Fixture, rows: Vec<ForwardRow>, now: u64) -> Decisions {
        let nodes = (fixture.actor.node_id, fixture.actor.node_id);
        let batch = EventBatch {
            events: rows,
            gaps: Vec::new(),
        };
        let config = config(fixture).await;
        decide_batch(&fixture.ctx, &config, nodes, batch, now)
            .await
            .unwrap()
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
    use crate::driver::DriverContext;
    use crate::driver::drive;
    use crate::plugins::calls::read_config;
    use crate::plugins::submit::UploadRequest;
    use crate::plugins::test_support::Fixture;
    use crate::plugins::test_support::bound;
    use crate::plugins::test_support::edit_config;
    use crate::plugins::test_support::path;
    use crate::plugins::work::RequestContext;
    use crate::testing_objects::seed_bucket;
    use aruna_core::plugin_installs::PluginInstall;
    use ulid::Ulid;

    /// An install granted the group's data whose `catalog` connection injects the slot `api_key`.
    pub async fn fetcher(fixture: &Fixture) -> PluginInstall {
        let install = crate::plugins::test_support::bound(fixture).await;
        let data = format!("/{}/g/{}/data/**", fixture.actor.realm_id, fixture.group_id);
        edit_config(fixture, |config| {
            config.installs[0].grants.insert("write".into(), vec![data]);
        })
        .await;
        let config = crate::plugins::calls::read_config(&fixture.ctx, fixture.actor.realm_id);
        let install = (config.await.unwrap().installs.into_iter())
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

    pub fn send(
        fixture: &Fixture,
        install: &PluginInstall,
        version: Option<Ulid>,
    ) -> UploadRequest {
        UploadRequest {
            context: RequestContext {
                connection: "catalog".into(),
                base: "https://api.example.org/api/".into(),
                method: "PUT".into(),
                path: "a.txt".into(),
                query: Vec::new(),
                headers: Vec::new(),
            },
            source: path(fixture, install, "in/a.txt"),
            version,
            form: None,
            ordinal: 0,
        }
    }

    /// A blob context with the object `in/a.txt` of the fetcher's group.
    pub async fn stored_object(
        fixture: &Fixture,
        install: &PluginInstall,
    ) -> (tempfile::TempDir, DriverContext) {
        use crate::driver::drive;
        use crate::s3::object::copy::test::{put_config, with_blob};
        use crate::s3::object::put::PutObjectOperation;
        let root = tempfile::tempdir().unwrap();
        let blob = with_blob(&fixture.ctx, root.path()).await;
        let (realm, node) = (fixture.actor.realm_id, fixture.actor.node_id);
        let put = put_config(realm, install.group_id, node, "in", "a.txt", b"ACGT");
        drive(PutObjectOperation::new(put), &blob).await.unwrap();
        (root, blob)
    }
}

pub mod matcher {
    use crate::plugins::calls::read_config;
    use crate::plugins::rows::decode;
    use crate::plugins::store::Txn;
    use crate::plugins::test_support::Fixture;
    use aruna_core::keyspaces::EVENT_JOURNAL_KEYSPACE;
    use aruna_core::keyspaces::PLUGIN_OUTBOX_KEYSPACE;
    use aruna_core::plugin_installs::PluginInstall;
    use aruna_core::plugin_runs::ForwardRow;
    use aruna_core::plugin_runs::SourceEvent;
    use aruna_core::plugin_runs::journal_entries;
    use aruna_core::structs::identity::realm::RealmConfigDocument;

    pub async fn config(fixture: &Fixture) -> RealmConfigDocument {
        read_config(&fixture.ctx, fixture.actor.realm_id)
            .await
            .unwrap()
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
