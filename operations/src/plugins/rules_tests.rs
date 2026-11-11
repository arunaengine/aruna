//! Tests rule edits: ensuring is idempotent, keeps other consumers' rules, writes nothing for a
//! stale revision, and removal only runs for a retired install.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::plugin_installs::PluginInstall;
use aruna_core::secrets::{Delivery, ResponseExposure};

use super::*;
use crate::plugins::bound::select;
use crate::plugins::bound::tests::{ORIGIN, bound, edit_config, secret};
use crate::plugins::install::tests::Fixture;

fn rule(consumer: &SecretConsumer, response: ResponseExposure) -> UseRule {
    UseRule {
        consumer: consumer.clone(),
        delivery: Delivery::Inject,
        origins: vec![ORIGIN.to_string()],
        paths: vec!["/api".to_string()],
        methods: vec!["POST".to_string()],
        response,
        groups: Vec::new(),
    }
}

async fn rules(fixture: &Fixture, install: &PluginInstall) -> Vec<UseRule> {
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    let binding = &install.secrets["api_key"];
    let selected = select(&fixture.ctx, &config, install, binding)
        .await
        .unwrap();
    selected.unwrap().rules
}

#[tokio::test]
async fn ensure_and_retire() {
    let fixture = Fixture::keyed().await;
    let install = bound(&fixture).await;
    secret(&fixture, &install).await;
    let name = SecretName::try_from("api-key".to_string()).unwrap();
    let target = (install.group_id, &name);
    let ours = consumer(&install);
    let other = SecretConsumer::Plugin {
        plugin_id: "other".to_string(),
        install_id: Ulid::from_parts(8, 8),
    };
    let current = RuleGuard::Revision {
        install_id: install.install_id,
        revision: install.revision,
    };
    let edit = |consumer: &SecretConsumer, wanted: Vec<UseRule>, guard| {
        let consumer = consumer.clone();
        let (ctx, actor) = (&fixture.ctx, &fixture.actor);
        async move { edit_rules(ctx, actor, target, (&consumer, wanted), guard).await }
    };
    let theirs = rule(&other, ResponseExposure::Status);
    assert_eq!(edit(&other, vec![theirs.clone()], current).await, Ok(true));
    let wanted = vec![rule(&ours, ResponseExposure::Full)];
    assert_eq!(edit(&ours, wanted.clone(), current).await, Ok(true));
    assert_eq!(edit(&ours, wanted.clone(), current).await, Ok(false));
    assert_eq!(
        rules(&fixture, &install).await,
        [theirs.clone(), wanted[0].clone()]
    );
    // A revision the record has moved past writes nothing.
    let stale = RuleGuard::Revision {
        install_id: install.install_id,
        revision: install.revision + 1,
    };
    assert_eq!(edit(&ours, Vec::new(), stale).await, Ok(false));
    let retired = RuleGuard::Retired {
        install_id: install.install_id,
    };
    assert_eq!(edit(&ours, Vec::new(), retired).await, Ok(false));
    edit_config(&fixture, |config| {
        config.installs[0].state = aruna_core::plugin_installs::InstallState::Removing;
    })
    .await;
    assert_eq!(edit(&ours, Vec::new(), retired).await, Ok(true));
    assert_eq!(rules(&fixture, &install).await, [theirs]);
    let missing = SecretName::try_from("absent".to_string()).unwrap();
    let absent = edit_rules(
        &fixture.ctx,
        &fixture.actor,
        (install.group_id, &missing),
        (&ours, Vec::new()),
        retired,
    );
    assert_eq!(absent.await, Err(RuleError::Missing));
}
