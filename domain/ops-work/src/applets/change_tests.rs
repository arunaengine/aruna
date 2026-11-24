//! Tests how one change moves an applet record and its host rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::applet::record::{Embedding, Lifecycle, PinnedObject};
use aruna_core::structs::identity::realm::RealmId;

fn user(seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), RealmId([2; 32]))
}

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[1; 32]).public()
}

pub(crate) fn version(number: u32) -> VersionRef {
    VersionRef {
        version: number,
        commit: Some("abc".into()),
        manifest: PinnedObject {
            node: node(),
            bucket: "applets".into(),
            key: format!("{number}/manifest.json"),
            version_id: "v".into(),
            blake3: "00".into(),
        },
        state: VersionState::Private,
        created_by: user(1),
        created_at_ms: 1,
        approved_by: None,
    }
}

pub(crate) fn applet() -> Applet {
    Applet {
        applet_id: Ulid::from_bytes([3; 16]),
        group_id: Ulid::from_bytes([4; 16]),
        name: "Bakta".into(),
        slug: "bakta".into(),
        host: "bakta.apps.example.org".into(),
        repository: None,
        host_node: node(),
        lifecycle: Lifecycle::Private,
        access: Access::Private,
        live: None,
        versions: Vec::new(),
        drafts: Vec::new(),
        grants: Grants::default(),
        embedding: Embedding::default(),
        guests: None,
        domains: Vec::new(),
        pending: None,
        account: None,
        role_id: None,
        visitor_salt: [7; 32],
        created_by: user(1),
        created_at_ms: 1,
        revision: 1,
    }
}

#[test]
fn versions_go_live() {
    let mut applet = applet();
    let stale = apply(
        &mut applet,
        AppletChange::AddVersion(Box::new(version(2))),
        user(1),
        5,
    );
    assert!(matches!(stale, Err(AppletError::Conflict(_))));
    apply(
        &mut applet,
        AppletChange::AddVersion(Box::new(version(1))),
        user(1),
        5,
    )
    .unwrap();
    let request = AppletChange::RequestApproval {
        version: 1,
        access: Access::Public,
        grants: Grants::default(),
        origins: vec!["https://lab.org".into()],
    };
    apply(&mut applet, request, user(1), 6).unwrap();
    apply(
        &mut applet,
        AppletChange::Approve { version: 1 },
        user(2),
        7,
    )
    .unwrap();
    assert_eq!(
        (applet.lifecycle, applet.access),
        (Lifecycle::Live, Access::Public)
    );
    assert_eq!(applet.embedding.origins, vec!["https://lab.org"]);
    assert_eq!(applet.revision, 4);
    assert_eq!(AppletChange::Approve { version: 1 }.need(), Need::Publish);
    assert_eq!(AppletChange::Pause.need(), Need::Write);
}

// Removing an origin or switching embedding applies at once; adding an origin needs approval.
#[test]
fn origins_need_approval() {
    let mut applet = applet();
    applet.embedding.origins = vec!["https://a.org".into(), "https://b.org".into()];
    let add = AppletChange::Embedding {
        enabled: true,
        origins: vec!["https://c.org".into()],
    };
    assert!(matches!(
        apply(&mut applet, add, user(1), 1),
        Err(AppletError::Invalid(_))
    ));
    let narrow = AppletChange::Embedding {
        enabled: true,
        origins: vec!["https://b.org".into()],
    };
    apply(&mut applet, narrow, user(1), 1).unwrap();
    assert!(applet.embedding.enabled);
    assert_eq!(applet.embedding.origins, vec!["https://b.org"]);
}

// A domain gets its host row once a check passes and loses it on suspension or removal.
#[test]
fn domains_move_hosts() {
    let mut applet = applet();
    let host = "bakta.lab.org".to_string();
    let add = AppletChange::AddDomain {
        host: host.clone(),
        nonce: "00".into(),
    };
    assert_eq!(
        apply(&mut applet, add.clone(), user(1), 1).unwrap(),
        HostMoves::default()
    );
    assert!(matches!(
        apply(&mut applet, add, user(1), 1),
        Err(AppletError::Conflict(_))
    ));
    let check = |passed| AppletChange::CheckDomain {
        host: host.clone(),
        passed,
    };
    let moves = apply(&mut applet, check(true), user(1), 2).unwrap();
    assert_eq!(moves.add, vec![host.clone()]);
    assert_eq!(
        apply(&mut applet, check(false), user(1), 3).unwrap(),
        HostMoves::default()
    );
    let moves = apply(&mut applet, check(false), user(1), 4).unwrap();
    assert_eq!(moves.remove, vec![host.clone()]);
    let remove = AppletChange::RemoveDomain { host: host.clone() };
    assert_eq!(
        apply(&mut applet, remove.clone(), user(1), 5)
            .unwrap()
            .remove,
        vec![host]
    );
    assert_eq!(
        apply(&mut applet, remove, user(1), 5),
        Err(AppletError::NotFound)
    );
}

#[test]
fn takedown_blocks_versions() {
    let mut applet = applet();
    apply(
        &mut applet,
        AppletChange::TakeDown { version: None },
        user(1),
        1,
    )
    .unwrap();
    let added = apply(
        &mut applet,
        AppletChange::AddVersion(Box::new(version(1))),
        user(1),
        2,
    );
    assert!(matches!(added, Err(AppletError::Conflict(_))));
    let resumed = apply(&mut applet, AppletChange::Resume, user(1), 2);
    assert!(matches!(resumed, Err(AppletError::Conflict(_))));
}

// Creation claims the host once and needs WRITE on the group's data; a stale revision is a
// conflict; the group list and the host lookup find the record.
#[tokio::test]
async fn create_claims_host() {
    use crate::applets::create::{CreateAppletOperation, Hosting, NewApplet};
    use crate::applets::reads::{HostLookupOperation, ListAppletsOperation};
    use aruna_runner::driver::drive;

    let context =
        aruna_runner::testing::test_context(aruna_storage::storage::memory::MemoryStorage::open());
    let (realm, group) = (RealmId([5; 32]), Ulid::from_bytes([10; 16]));
    let owner = aruna_runner::testing::seed_access(&context, (realm, group, node()), "/none").await;
    let hosting = Hosting {
        node: node(),
        applet_domain: "apps.example.org".into(),
        visitor_salt: [1; 32],
    };
    let request = |slug: &str| NewApplet {
        group_id: group,
        name: "Bakta".into(),
        slug: slug.into(),
        repository: None,
    };
    let create = |auth: &AuthContext, slug: &str| {
        CreateAppletOperation::new(auth, request(slug), hosting.clone())
    };
    let created = drive(create(&owner, "bakta"), &context).await.unwrap();
    assert_eq!(created.host, "bakta.apps.example.org");
    let taken = drive(create(&owner, "bakta"), &context).await;
    assert!(matches!(taken, Err(AppletError::Conflict(_))), "{taken:?}");
    let stranger = AuthContext::new(UserId::new(Ulid::from_bytes([9; 16]), realm), realm);
    let refused = drive(create(&stranger, "other"), &context).await;
    assert_eq!(refused, Err(AppletError::Forbidden));

    let key = (created.applet_id, Some(9));
    let stale = ChangeAppletOperation::new(&owner, key, AppletChange::Reject, node());
    let stale = drive(stale, &context).await;
    assert!(matches!(stale, Err(AppletError::Conflict(_))), "{stale:?}");
    let hidden = ChangeAppletOperation::new(
        &stranger,
        (created.applet_id, None),
        AppletChange::Pause,
        node(),
    );
    assert_eq!(drive(hidden, &context).await, Err(AppletError::NotFound));

    let list = ListAppletsOperation::new(&owner, (group, node()), None, 10);
    let page = drive(list, &context).await.unwrap();
    assert_eq!(page.applets, vec![created.clone()]);
    let found = drive(HostLookupOperation::new("bakta.apps.example.org"), &context).await;
    assert_eq!(
        found.unwrap().map(|(_, applet)| applet.applet_id),
        Some(created.applet_id)
    );
}
