//! Tests the applet lifecycle, serving state, pruning, domain checks and role paths.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[1; 32]).public()
}

fn user(seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), RealmId([2; 32]))
}

fn version(number: u32, state: VersionState) -> VersionRef {
    VersionRef {
        version: number,
        commit: None,
        manifest: PinnedObject {
            node: node(),
            bucket: "applets".into(),
            key: format!("v{number}/manifest.json"),
            version_id: "v".into(),
            blake3: "00".into(),
        },
        state,
        created_by: user(1),
        created_at_ms: u64::from(number),
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
        repository: Some(Ulid::from_bytes([5; 16])),
        host_node: node(),
        lifecycle: Lifecycle::Private,
        access: Access::Private,
        live: None,
        versions: vec![version(1, VersionState::Private)],
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
        automations: Vec::new(),
    }
}

pub(crate) fn request(version: u32) -> ApprovalRequest {
    ApprovalRequest {
        version,
        access: Access::Private,
        grants: Grants::default(),
        origins: Vec::new(),
        requested_by: user(1),
        requested_at_ms: 2,
    }
}

#[test]
fn approval_goes_live() {
    let mut applet = applet();
    assert_eq!(applet.serving(true, true), Serving::Preview { version: 1 });
    assert_eq!(applet.approve(1, user(2)), Err(TransitionError::NotPending));
    applet.request_approval(request(1)).unwrap();
    assert_eq!(applet.lifecycle, Lifecycle::WaitingForApproval);
    let mut pending = request(1);
    pending.access = Access::Public;
    applet.pending = Some(pending);
    applet.approve(1, user(2)).unwrap();
    assert_eq!(
        (applet.lifecycle, applet.access),
        (Lifecycle::Live, Access::Public)
    );
    assert_eq!(applet.version(1).unwrap().approved_by, Some(user(2)));
    assert_eq!(applet.serving(true, true), Serving::Live { version: 1 });

    // A second version waits while the first keeps serving; rejecting keeps the applet live.
    applet.versions.push(version(2, VersionState::Private));
    applet.request_approval(request(2)).unwrap();
    assert_eq!(applet.lifecycle, Lifecycle::Live);
    applet.reject().unwrap();
    assert_eq!((applet.lifecycle, applet.live), (Lifecycle::Live, Some(1)));
}

// The feature switch pauses only a live applet and never touches an owner's pause.
#[test]
fn feature_pause_separate() {
    let mut applet = applet();
    applet.request_approval(request(1)).unwrap();
    applet.approve(1, user(2)).unwrap();
    assert_eq!(applet.serving(false, true), Serving::FeaturePaused);
    assert_eq!(applet.lifecycle, Lifecycle::Live);
    assert_eq!(applet.serving(true, true), Serving::Live { version: 1 });
    applet.pause().unwrap();
    assert_eq!(applet.serving(false, true), Serving::Paused);
    assert_eq!(applet.serving(true, true), Serving::Paused);
    assert_eq!(applet.serving(true, false), Serving::Paused);
    applet.resume().unwrap();
    assert_eq!(applet.serving(true, false), Serving::Gone);
    assert_eq!(applet.resume(), Err(TransitionError::NotPaused));
}

#[test]
fn rollback_needs_approved() {
    let mut applet = applet();
    applet.versions.push(version(2, VersionState::Approved));
    applet.versions.push(version(3, VersionState::TakenDown));
    assert_eq!(applet.rollback(1), Err(TransitionError::NotApproved));
    assert_eq!(applet.rollback(3), Err(TransitionError::NotApproved));
    applet.rollback(2).unwrap();
    assert_eq!(applet.live, Some(2));
    applet.take_down(Some(2)).unwrap();
    assert_eq!((applet.live, applet.lifecycle), (None, Lifecycle::Paused));
    applet.take_down(None).unwrap();
    assert_eq!(applet.serving(true, true), Serving::Gone);
    assert_eq!(applet.rollback(2), Err(TransitionError::TakenDown));
    assert_eq!(
        applet.request_approval(request(1)),
        Err(TransitionError::TakenDown)
    );
}

#[test]
fn prune_keeps_rollback() {
    let mut applet = applet();
    applet.versions = (1..=12)
        .map(|number| match number % 2 {
            0 => version(number, VersionState::Approved),
            _ => version(number, VersionState::Private),
        })
        .collect();
    applet.live = Some(2);
    applet.pending = Some(request(1));
    let dropped = applet.prune();
    let kept: Vec<u32> = applet.versions.iter().map(|entry| entry.version).collect();
    // Live 2, approved 12, 10, 8, drafts 11, 9, 7, 5, 3 and the pending 1.
    assert_eq!(kept, vec![1, 2, 3, 5, 7, 8, 9, 10, 11, 12]);
    let dropped: Vec<u32> = dropped.iter().map(|entry| entry.version).collect();
    assert_eq!(dropped, vec![6, 4]);
    assert_eq!(applet.next_version(), 13);
}

#[test]
fn domain_suspends_twice() {
    let mut domain = CustomDomain {
        host: "bakta.lab.org".into(),
        nonce: "00".into(),
        state: DomainState::Pending,
        failures: 0,
        checked_at_ms: 0,
    };
    assert!(domain.record_check(false, 1));
    assert!(domain.record_check(false, 2));
    assert_eq!(domain.state, DomainState::Pending);
    assert!(domain.record_check(true, 3));
    assert_eq!(domain.state, DomainState::Active);
    assert!(domain.record_check(false, 4));
    assert_eq!(domain.state, DomainState::Active);
    assert!(domain.record_check(false, 5));
    assert_eq!(domain.state, DomainState::Suspended);
}

// The applet's role reads what was granted and writes below its submission folder only.
#[test]
fn role_paths_bounded() {
    let mut applet = applet();
    let realm = RealmId([2; 32]);
    applet.grants.read = vec!["/r/g/G/data/n/results/**".into()];
    applet.grants.submissions = Some(Folder {
        bucket: "uploads".into(),
        prefix: "bakta/".into(),
    });
    let paths = applet.role_paths(realm, node());
    assert_eq!(paths.len(), 2);
    assert_eq!(
        paths[0],
        ("/r/g/G/data/n/results/**".to_string(), Permission::READ)
    );
    assert!(paths[1].0.ends_with("/uploads/bakta/**"), "{:?}", paths[1]);
    assert_eq!(paths[1].1, Permission::WRITE);
    applet.grants.start = Some(StartGrant::Workflow {
        workflow_id: Ulid::from_bytes([9; 16]),
        version: 1,
    });
    let with_start = applet.role_paths(realm, node());
    assert_eq!(
        with_start[2],
        (
            crate::structs::execution::workflow::workflow_permission_path(realm, applet.group_id),
            Permission::WRITE
        )
    );
    let encoded = applet.to_bytes().unwrap();
    assert_eq!(Applet::from_bytes(&encoded).unwrap(), applet);

    // Bucket-root submission folders avoid a double slash, and glob chars in prefixes are escaped.
    applet.grants.submissions = Some(Folder {
        bucket: "uploads".into(),
        prefix: String::new(),
    });
    let root_paths = applet.role_paths(realm, node());
    assert!(
        root_paths[1].0.ends_with("/uploads/**") && !root_paths[1].0.ends_with("/uploads//**"),
        "{:?}",
        root_paths[1]
    );
    applet.grants.submissions = Some(Folder {
        bucket: "uploads".into(),
        prefix: "team [1]/".into(),
    });
    let escaped_paths = applet.role_paths(realm, node());
    assert!(
        escaped_paths[1].0.ends_with("/uploads/team [[]1[]]/**"),
        "{:?}",
        escaped_paths[1]
    );
}

#[test]
fn pseudonyms_differ() {
    let salt = [1; 32];
    assert_eq!(pseudonym(&salt, &user(1)), pseudonym(&salt, &user(1)));
    assert_ne!(pseudonym(&salt, &user(1)), pseudonym(&salt, &user(2)));
    assert_ne!(pseudonym(&salt, &user(1)), pseudonym(&[2; 32], &user(1)));
    assert_eq!(pseudonym(&salt, &user(1)).len(), 32);
}

#[test]
fn drafts_expire_counted() {
    let mut applet = applet();
    let draft = |seed: u8, expires_at_ms: u64| Draft {
        build_id: Ulid::from_bytes([seed; 16]),
        job_id: Ulid::from_bytes([seed; 16]),
        commit: "c".into(),
        started_by: user(1),
        started_at_ms: 0,
        expires_at_ms,
        manifest: None,
        failed: None,
    };
    applet.drafts = vec![draft(1, 10), draft(2, 20)];
    assert_eq!(applet.draft_room(6), 4);
    assert_eq!(applet.draft_room(1), 0);
    let expired = applet.expire_drafts(10);
    assert_eq!(expired.len(), 1);
    assert!(applet.draft(Ulid::from_bytes([1; 16])).is_none());
    assert!(applet.draft(Ulid::from_bytes([2; 16])).is_some());
    let guest = guest_pseudonym(&[1; 32], "203.0.113.7");
    assert_eq!(guest.len(), 32);
    assert_ne!(guest, guest_pseudonym(&[2; 32], "203.0.113.7"));
}
