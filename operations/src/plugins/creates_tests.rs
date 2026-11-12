//! Tests keyed creates: the first call keeps its row and a later one adopts it, the next step
//! follows the row and the lookup, and a removed install's keys go with the sweep.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::plugin_installs::InstallState;
use aruna_core::time::unix_timestamp_millis;

use super::*;
use crate::metadata::create_document::CreateDocumentPayload;
use crate::plugins::cleanup::sweep_events;
use crate::plugins::install::tests::Fixture;
use crate::plugins::matcher::tests::config;
use crate::plugins::worker::tests::installed;

fn pinned(fixture: &Fixture, name: &str) -> PinnedCreate {
    PinnedCreate {
        group_id: fixture.group_id,
        path: format!("elabftw/{name}"),
        document_id: Ulid::generate(),
        target: fixture.actor.node_id,
        payload: CreateDocumentPayload::RoCrate {
            jsonld: format!(r#"{{"name":"{name}"}}"#),
        },
        message: None,
    }
}

#[tokio::test]
async fn later_call_adopts() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await.install_id;
    let first = pinned(&fixture, "a");
    let claimed = claim_key(&fixture.ctx, (install, "42"), first.clone()).await;
    assert_eq!(claimed, Ok((first.clone(), Claim::Fresh)));
    // An edited source sends other arguments; the stored ones stay.
    let edited = pinned(&fixture, "b");
    let again = claim_key(&fixture.ctx, (install, "42"), edited.clone()).await;
    assert_eq!(again, Ok((first, Claim::Earlier)));
    let other = claim_key(&fixture.ctx, (install, "43"), edited.clone()).await;
    assert_eq!(other, Ok((edited, Claim::Fresh)));
}

#[test]
fn steps_follow_lookup() {
    let found = Ulid::from_parts(1, 1);
    let cases = [
        (Claim::Fresh, None, Step::Dispatch),
        (Claim::Fresh, Some(Lookup::Unreachable), Step::Dispatch),
        (Claim::Earlier, None, Step::Lookup),
        (
            Claim::Earlier,
            Some(Lookup::Found(found)),
            Step::Adopt(found),
        ),
        (Claim::Earlier, Some(Lookup::Missing), Step::Dispatch),
        (Claim::Earlier, Some(Lookup::Unreachable), Step::Unknown),
    ];
    for (claim, lookup, step) in cases {
        assert_eq!(next_step(claim, lookup), step, "{claim:?} {lookup:?}");
    }
}

#[tokio::test]
async fn removal_drops_keys() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await.install_id;
    for key in ["1", "2"] {
        let minted = pinned(&fixture, key);
        claim_key(&fixture.ctx, (install, key), minted)
            .await
            .unwrap();
    }
    let mut config = config(&fixture).await;
    let now = unix_timestamp_millis();
    assert_eq!(sweep_events(&fixture.ctx, &config, now).await, Ok(0));
    config.installs[0].state = InstallState::Removing;
    assert_eq!(sweep_events(&fixture.ctx, &config, now).await, Ok(2));
    let left = Txn::direct(&fixture.ctx)
        .scan(PLUGIN_CREATES_KEYSPACE, &[])
        .await
        .unwrap();
    assert!(left.is_empty());
}
