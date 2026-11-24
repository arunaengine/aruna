//! Runs applet operations against in-memory storage: drafts and their room, the notice feed an
//! edge follows, and visit caps under concurrent counting.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::applet::notices::notice_prefix;
use aruna_core::applet::record::Draft;
use aruna_core::applet::usage::Caps;
use aruna_core::structs::identity::realm::RealmId;
use aruna_runner::driver::drive;
use ulid::Ulid;

use super::AppletError;
use super::change::{AppletChange, ChangeAppletOperation};
use super::create::{CreateAppletOperation, Hosting, NewApplet};
use super::feed::FeedOperation;
use super::visits::CountVisitOperation;

#[tokio::test]
async fn drafts_notices_caps() {
    let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
    let context =
        aruna_runner::testing::test_context(aruna_storage::storage::memory::MemoryStorage::open());
    let (realm, group) = (RealmId([5; 32]), Ulid::from_bytes([10; 16]));
    let owner = aruna_runner::testing::seed_access(&context, (realm, group, node), "/none").await;
    let hosting = Hosting {
        node,
        applet_domain: "apps.example.org".into(),
        visitor_salt: [1; 32],
    };
    let request = NewApplet {
        group_id: group,
        name: "Bakta".into(),
        slug: "bakta".into(),
        repository: None,
    };
    let created = CreateAppletOperation::new(&owner, request, hosting);
    let applet = drive(created, &context).await.unwrap();
    let change =
        |change| ChangeAppletOperation::new(&owner, (applet.applet_id, None), change, node);

    // One room: the second build is refused until the first draft is gone.
    let start = |seed: u8| AppletChange::StartBuild {
        draft: Box::new(Draft {
            build_id: Ulid::from_bytes([seed; 16]),
            job_id: Ulid::from_bytes([seed; 16]),
            commit: "abc".into(),
            started_by: owner.user_id,
            started_at_ms: 0,
            expires_at_ms: 0,
            manifest: None,
            failed: None,
        }),
        max: 1,
        ttl_ms: 60_000,
    };
    let first = drive(change(start(1)), &context).await.unwrap();
    assert!(first.drafts[0].expires_at_ms > first.drafts[0].started_at_ms);
    let full = drive(change(start(2)), &context).await;
    assert!(matches!(full, Err(AppletError::Conflict(_))), "{full:?}");
    let drop = AppletChange::DropDraft {
        build_id: Ulid::from_bytes([1; 16]),
    };
    drive(change(drop), &context).await.unwrap();
    drive(change(start(2)), &context).await.unwrap();

    // Drafts tell the edge nothing; an embedding change names the applet's hosts.
    let feed = || FeedOperation::new(notice_prefix(), 10, Duration::ZERO);
    assert!(drive(feed(), &context).await.unwrap().notices.is_empty());
    let embed = AppletChange::Embedding {
        enabled: true,
        origins: Vec::new(),
    };
    drive(change(embed), &context).await.unwrap();
    let page = drive(feed(), &context).await.unwrap();
    assert_eq!(page.notices.len(), 1);
    assert_eq!(
        page.notices[0].hosts,
        vec!["bakta.apps.example.org".to_string()]
    );
    let after = page.last.unwrap();
    let rest = FeedOperation::new(after, 10, Duration::ZERO);
    assert!(drive(rest, &context).await.unwrap().notices.is_empty());

    // A visitor gets one action this hour; another visitor still has its own.
    let caps = Caps {
        hourly: Some(1),
        daily: None,
        applet_daily: Some(2),
    };
    let count = |visitor: &str| CountVisitOperation::new(applet.applet_id, visitor, caps);
    drive(count("ab"), &context).await.unwrap();
    let limited = drive(count("ab"), &context).await;
    assert!(
        matches!(limited, Err(AppletError::Limited(_))),
        "{limited:?}"
    );
    drive(count("cd"), &context).await.unwrap();
    let applet_cap = drive(count("ef"), &context).await;
    assert!(
        matches!(applet_cap, Err(AppletError::Limited(_))),
        "{applet_cap:?}"
    );
}
