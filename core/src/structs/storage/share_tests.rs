//! Tests the share link records: link tokens, secrets and their scope.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::structs::identity::realm::RealmId;

fn link(secret: u8) -> ShareLink {
    let realm = RealmId::from_bytes([1; 32]);
    ShareLink {
        id: Ulid::from_bytes([2; 16]),
        group_id: Ulid::from_bytes([3; 16]),
        target: ShareTarget::Folder {
            bucket: "data".into(),
            prefix: "shared/".into(),
        },
        mode: ShareMode::Read,
        created_by: UserId::new(Ulid::from_bytes([4; 16]), realm),
        created_at: 100,
        label: "Results".into(),
        expires_at: Some(1_000),
        password: None,
        secret: [secret; 32],
        signing_key: [!secret; 32],
        revoked_at: None,
    }
}

#[test]
fn token_opens_link() {
    let own = link(7);
    let token = ShareToken::parse(&own.link_token()).unwrap();
    assert!(own.admits(&token, 500));
    assert!(!link(8).admits(&token, 500));
    assert!(ShareToken::parse("not-a-token").is_none());
    let record = ShareLink::from_bytes(&own.to_bytes().unwrap()).unwrap();
    assert_eq!(record, own);
    assert!(!format!("{own:?}").contains("secret"));
}

#[test]
fn visit_token_bounded() {
    let own = link(7);
    let token = ShareToken::parse(&own.visit_token(600)).unwrap();
    assert!(own.admits(&token, 500));
    assert!(!own.admits(&token, 600));
    // A token signed for longer than a visit lasts is refused.
    let long = ShareToken::parse(&own.visit_token(500 + VISIT_SECS + 1)).unwrap();
    assert!(!own.admits(&long, 500));
    assert!(!link(8).admits(&token, 500));
    let ShareToken::Visit { id, mac, .. } = token else {
        panic!("visit token expected");
    };
    let moved = ShareToken::Visit {
        id,
        expires_at: 700,
        mac,
    };
    assert!(!own.admits(&moved, 500));
}

#[test]
fn secret_signs_nothing() {
    // Whoever holds the link URL must not mint a visit token that skips the password.
    let own = link(7);
    let forged = blake3::Hasher::new_keyed(&own.secret)
        .update(VISIT_DOMAIN)
        .update(&own.id.to_bytes())
        .update(&600u64.to_be_bytes())
        .finalize();
    let token = format!(
        "v.{}.600.{}",
        own.id,
        URL_SAFE_NO_PAD.encode(forged.as_bytes())
    );
    assert!(!own.admits(&ShareToken::parse(&token).unwrap(), 500));
    assert!(own.admits(&ShareToken::parse(&own.visit_token(600)).unwrap(), 500));
}

#[test]
fn ticket_signed_once() {
    let own = link(7);
    let body = TicketBody {
        share_id: own.id,
        bucket: "data".into(),
        key: "shared/a.txt".into(),
        version_id: None,
        expires_at: 530,
        nonce: [9; 16],
    };
    let ticket = own.ticket(&body).unwrap();
    assert_eq!(own.open_ticket(&ticket, 500), Some(body.clone()));
    assert_eq!(own.open_ticket(&ticket, 530), None);
    assert_eq!(link(8).open_ticket(&ticket, 500), None);
    let (encoded, _) = ticket.split_once('.').unwrap();
    let forged = format!("{encoded}.{}", URL_SAFE_NO_PAD.encode([0u8; 32]));
    assert_eq!(own.open_ticket(&forged, 500), None);
}

#[test]
fn ended_links_inactive() {
    let mut own = link(7);
    assert!(own.active(999));
    assert!(!own.active(1_000));
    own.expires_at = None;
    assert!(own.active(u64::MAX));
    own.revoked_at = Some(5);
    assert!(!own.active(6));
}

#[test]
fn targets_cover_scope() {
    let folder = link(7).target;
    assert!(folder.covers("data", "shared/a/b.txt"));
    assert!(!folder.covers("data", "sharedx/a.txt"));
    assert!(!folder.covers("other", "shared/a.txt"));
    let object = ShareTarget::Object {
        bucket: "data".into(),
        key: "a.txt".into(),
    };
    assert!(object.covers("data", "a.txt"));
    assert!(!object.covers("data", "a.txt.bak"));
}

#[test]
fn uses_count_accesses() {
    let event = |action, at| ShareEvent {
        at,
        action,
        key: None,
        bytes: None,
        client: None,
    };
    let mut uses = ShareUses::default();
    uses.record(&event(ShareAction::WrongPassword, 50));
    assert_eq!(uses, ShareUses::default());
    uses.record(&event(ShareAction::Download, 100));
    uses.record(&event(ShareAction::Upload, 90));
    uses.record(&event(ShareAction::Open, 80));
    assert_eq!(
        uses,
        ShareUses {
            downloads: 1,
            uploads: 1,
            last_access_at: Some(100),
        }
    );
}

#[test]
fn client_network_truncated() {
    assert_eq!(
        client_network("192.0.2.77".parse().unwrap()),
        "192.0.2.0/24"
    );
    assert_eq!(
        client_network("2001:db8:1:2:3::9".parse().unwrap()),
        "2001:db8:1::/48"
    );
    assert_eq!(
        client_network("::ffff:198.51.100.4".parse().unwrap()),
        "198.51.100.0/24"
    );
}
