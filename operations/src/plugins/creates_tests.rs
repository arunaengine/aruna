//! Tests keyed creates: a later call adopts the row, a refused one may be corrected, an unknown one
//! not, a reinstall adopts nothing, the next step follows the lookup, and removed keys are swept.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::drive;
use aruna_core::UserId;
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::time::unix_timestamp_millis;

use super::*;
use crate::metadata::create_document::CreateDocumentPayload;
use crate::plugins::cleanup::sweep_events;
use crate::plugins::install::tests::Fixture;
use crate::plugins::matcher::tests::config;
use crate::plugins::store::Txn;
use crate::plugins::test_support::installed;

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
    let install = installed(&fixture).await;
    let first = pinned(&fixture, "a");
    let claimed = drive(claim_key((&install, "42"), first.clone()), &fixture.ctx).await;
    assert_eq!(claimed, Ok((first.clone(), Claim::Fresh)));
    // An edited source sends other arguments; the stored ones stay.
    let edited = pinned(&fixture, "b");
    let again = drive(claim_key((&install, "42"), edited.clone()), &fixture.ctx).await;
    assert_eq!(again, Ok((first, Claim::Earlier)));
    let other = drive(claim_key((&install, "43"), edited.clone()), &fixture.ctx).await;
    assert_eq!(other, Ok((edited, Claim::Fresh)));
}

#[tokio::test]
async fn rejection_allows_correction() {
    // After a refusal of its arguments, a later call corrects them under the same document and
    // node; once that create committed, the corrected arguments stay.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let ctx = &fixture.ctx;
    let first = pinned(&fixture, "a");
    drive(claim_key((&install, "42"), first.clone()), ctx)
        .await
        .unwrap();
    drive(settle_key((&install, "42"), true), ctx)
        .await
        .unwrap();
    let edited = pinned(&fixture, "b");
    let (corrected, claim) = drive(claim_key((&install, "42"), edited.clone()), ctx)
        .await
        .unwrap();
    assert_eq!(claim, Claim::Fresh);
    assert_eq!(corrected.payload, edited.payload);
    assert_eq!(corrected.path, edited.path);
    assert_eq!(
        (corrected.document_id, corrected.target),
        (first.document_id, first.target)
    );
    drive(settle_key((&install, "42"), false), ctx)
        .await
        .unwrap();
    let later = drive(claim_key((&install, "42"), pinned(&fixture, "c")), ctx).await;
    assert_eq!(later, Ok((corrected, Claim::Earlier)));
}

#[tokio::test]
async fn unknown_keeps_arguments() {
    // While an attempt may still commit, a later refusal does not free the arguments.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let ctx = &fixture.ctx;
    let first = pinned(&fixture, "a");
    drive(claim_key((&install, "7"), first.clone()), ctx)
        .await
        .unwrap();
    let again = drive(claim_key((&install, "7"), pinned(&fixture, "b")), ctx).await;
    assert_eq!(again, Ok((first.clone(), Claim::Earlier)));
    drive(settle_key((&install, "7"), true), ctx).await.unwrap();
    let later = drive(claim_key((&install, "7"), pinned(&fixture, "c")), ctx).await;
    assert_eq!(later, Ok((first, Claim::Earlier)));
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
    let install = installed(&fixture).await;
    for key in ["1", "2"] {
        let minted = pinned(&fixture, key);
        drive(claim_key((&install, key), minted), &fixture.ctx)
            .await
            .unwrap();
    }
    let mut config = config(&fixture).await;
    let now = unix_timestamp_millis();
    assert_eq!(drive(sweep_events(&config, now), &fixture.ctx).await, Ok(0));
    config.installs[0].state = InstallState::Removing;
    assert_eq!(drive(sweep_events(&config, now), &fixture.ctx).await, Ok(2));
    let left = Txn::direct(&fixture.ctx)
        .scan(PLUGIN_CREATES_KEYSPACE, &[])
        .await
        .unwrap();
    assert!(left.is_empty());
}

#[tokio::test]
async fn reinstall_starts_fresh() {
    // A removal the receiver never saw as removing still drops the keys once the install is gone,
    // and a reinstall under the same id, with its new account, adopts no key left behind.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let first = pinned(&fixture, "a");
    drive(claim_key((&install, "9"), first.clone()), &fixture.ctx)
        .await
        .unwrap();
    let reinstalled = PluginInstall {
        account: UserId::local(Ulid::generate(), fixture.actor.realm_id),
        ..install.clone()
    };
    let fresh = pinned(&fixture, "b");
    let claimed = drive(claim_key((&reinstalled, "9"), fresh.clone()), &fixture.ctx).await;
    assert_eq!(claimed, Ok((fresh, Claim::Fresh)));

    let mut config = config(&fixture).await;
    config.installs.clear();
    let now = unix_timestamp_millis();
    assert_eq!(drive(sweep_events(&config, now), &fixture.ctx).await, Ok(1));
    let again = drive(claim_key((&reinstalled, "9"), first.clone()), &fixture.ctx).await;
    assert_eq!(again, Ok((first, Claim::Fresh)));
}

mod finish_create {
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::operation::Operation;

    use super::*;
    use crate::jobs::repository::test_support::registry_record;
    use crate::plugins::fixtures::{self, is_child, node};
    use crate::plugins::rows::tests::child_result;

    fn minted() -> PinnedCreate {
        PinnedCreate {
            group_id: fixtures::install().group_id,
            path: "a".to_string(),
            document_id: Ulid::from_parts(2, 2),
            target: node(3),
            payload: CreateDocumentPayload::RoCrate {
                jsonld: "{}".to_string(),
            },
            message: None,
        }
    }

    fn finish(minted: Result<Box<PinnedCreate>, String>) -> FinishCreate {
        let caller = fixtures::caller();
        let ready = ReadyCreate {
            actor: caller.actor,
            granted: Granted {
                install: fixtures::install(),
                auth: caller.auth,
            },
            minted,
        };
        let intent = CallIntent {
            ordinal: 0,
            digest: [1; 32],
        };
        FinishCreate::new(ready, None, "key", (Ulid::from_parts(3, 3), intent))
    }

    fn claimed(claim: Claim) -> FinishCreate {
        let mut create = finish(Ok(Box::new(minted())));
        assert!(is_child(&create.first()));
        let stored = child_result(Ok::<_, StoreError>((minted(), claim)));
        assert!(is_child(&create.next(stored)));
        create
    }

    fn settled() -> IoResult {
        child_result(Ok::<(), StoreError>(()))
    }

    fn recorded(create: &mut FinishCreate) -> Result<Next<String>, CallFailure> {
        create.next(child_result(Ok::<_, CallFailure>("reply".to_string())))
    }

    #[test]
    fn fresh_key_creates() {
        let mut create = claimed(Claim::Fresh);
        assert_eq!(create.phase, CreatePhase::Create);
        let done = child_result(Ok::<_, CreateAuthorizedError>(registry_record()));
        assert!(is_child(&create.next(done)));
        assert!(matches!(create.phase, CreatePhase::Settle(_)));
        assert!(is_child(&create.next(settled())));
        assert_eq!(recorded(&mut create), Ok(Next::Done("reply".to_string())));
    }

    #[test]
    fn earlier_key_adopts() {
        let mut create = claimed(Claim::Earlier);
        assert_eq!(create.phase, CreatePhase::Lookup);
        let found = child_result(Ok::<_, MetadataApiError>(registry_record()));
        assert!(is_child(&create.next(found)));
        assert!(matches!(create.phase, CreatePhase::Settle(_)));
    }

    #[test]
    fn missing_document_resent() {
        let mut create = claimed(Claim::Earlier);
        let gone = Err::<MetadataRegistryRecord, _>(MetadataApiError::NotFound);
        assert!(is_child(&create.next(child_result(gone))));
        assert_eq!(create.phase, CreatePhase::Create);
    }

    #[test]
    fn unreachable_stays_unknown() {
        let mut create = claimed(Claim::Earlier);
        let down = Err::<MetadataRegistryRecord, _>(MetadataApiError::ServiceUnavailable);
        assert!(is_child(&create.next(child_result(down))));
        assert_eq!(create.phase, CreatePhase::Record);
    }

    #[test]
    fn refusal_settles_key() {
        let mut create = claimed(Claim::Fresh);
        let denied = Err::<MetadataRegistryRecord, _>(CreateAuthorizedError::Forbidden);
        assert!(is_child(&create.next(child_result(denied))));
        assert!(matches!(create.phase, CreatePhase::Settle(_)));
    }

    #[test]
    fn failed_mint_recorded() {
        let mut create = finish(Err("no".to_string()));
        let first = create.first();
        assert!(matches!(first, Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(create.phase, CreatePhase::Record);
        let lost = create.next(IoResult::Done);
        assert!(lost.is_err());
    }
}
