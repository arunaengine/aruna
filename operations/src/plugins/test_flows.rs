//! The event and work scenarios of the plugin tests whose signatures hold operations types.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod events {
    use crate::plugins::calls::decide_batch;
    use aruna_core::plugin_runs::Decisions;
    use aruna_core::plugin_runs::EventBatch;
    use aruna_core::plugin_runs::ForwardRow;
    use aruna_test_support::plugins::Fixture;
    use aruna_test_support::plugins::flows::matcher::config;

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
}

pub mod submit {
    use crate::plugins::submit::UploadRequest;
    use aruna_core::install_work::RequestContext;
    use aruna_core::plugin_installs::PluginInstall;
    use aruna_test_support::plugins::Fixture;
    use aruna_test_support::plugins::path;
    use ulid::Ulid;

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
}
