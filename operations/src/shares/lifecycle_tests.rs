//! Tests creating, opening and revoking share links.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::errors::AuthorizationError;
use aruna_core::password::{Kdf, PasswordHash};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::share::{ShareMode, ShareTarget};

const CHEAP: Kdf = Kdf::Argon2id {
    memory_kib: 64,
    passes: 1,
    lanes: 1,
};

fn user(seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
}

fn link(mode: ShareMode) -> ShareLink {
    ShareLink {
        id: Ulid::from_bytes([2; 16]),
        group_id: Ulid::from_bytes([3; 16]),
        target: ShareTarget::Folder {
            bucket: "data".into(),
            prefix: "in/".into(),
        },
        mode,
        created_by: user(4),
        created_at: 100,
        label: String::new(),
        expires_at: Some(10_000),
        password: None,
        secret: [5; 32],
        signing_key: [6; 32],
        revoked_at: None,
    }
}

fn auth(seed: u8) -> AuthContext {
    AuthContext {
        user_id: user(seed),
        realm_id: RealmId::from_bytes([1; 32]),
        path_restrictions: None,
        session: None,
    }
}

fn create(link: ShareLink, caller: u8) -> CreateShareOperation {
    CreateShareOperation::new(CreateShareConfig {
        auth: auth(caller),
        link,
        path: "/realm/g/group/data/node/data/in/".into(),
        now: 100,
    })
}

fn read_result(link: &ShareLink) -> Event {
    Event::Storage(StorageEvent::ReadResult {
        key: link.id.to_bytes().to_vec().into(),
        value: Some(link.to_bytes().unwrap().into()),
    })
}

fn allowed(result: Result<bool, AuthorizationError>) -> Event {
    Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed: result })
}

fn batch_written() -> Event {
    Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    })
}

fn log_written() -> Event {
    Event::SubOperation(SubOperationEvent::ShareLogged { result: Ok(()) })
}

#[test]
fn create_checks_creator() {
    let mut denied = create(link(ShareMode::Upload), 4);
    assert!(matches!(denied.start()[0], Effect::SubOperation(_)));
    assert!(denied.step(allowed(Ok(false))).is_empty());
    assert_eq!(denied.finalize(), Err(ShareError::Denied));
    let mut allowed_op = create(link(ShareMode::Upload), 4);
    allowed_op.start();
    let save = allowed_op.step(allowed(Ok(true)));
    assert!(matches!(
        save[0],
        Effect::Storage(StorageEffect::BatchWrite { .. })
    ));
    allowed_op.step(batch_written());
    assert_eq!(allowed_op.finalize().unwrap(), link(ShareMode::Upload));
}

#[test]
fn refuses_before_io() {
    // A link for someone else, or a bad link, never reaches the permission check.
    let mut foreign = create(link(ShareMode::Read), 9);
    assert!(foreign.start().is_empty());
    assert_eq!(foreign.finalize(), Err(ShareError::Denied));
    let mut object = link(ShareMode::Upload);
    object.target = ShareTarget::Object {
        bucket: "data".into(),
        key: "a.txt".into(),
    };
    let mut bad = create(object, 4);
    assert!(bad.start().is_empty());
    assert_eq!(
        bad.finalize(),
        Err(ShareError::Rule(ShareRuleError::UploadTarget))
    );
    let mut unexpected = create(link(ShareMode::Read), 4);
    unexpected.start();
    unexpected.step(batch_written());
    assert_eq!(
        unexpected.finalize(),
        Err(ShareError::UnexpectedEvent("create"))
    );
}

fn open(link: &ShareLink, password: Option<&str>) -> OpenShareOperation {
    OpenShareOperation::new(OpenShareConfig {
        token: ShareToken::parse(&link.link_token()).unwrap(),
        password: password.map(str::to_string),
        now: 500,
        entry_id: Ulid::from_bytes([7; 16]),
        client: "192.0.2.0/24".into(),
    })
}

#[test]
fn open_checks_password() {
    let mut guarded = link(ShareMode::Read);
    guarded.password = Some(PasswordHash::new("secret", CHEAP, [8; 16]).unwrap());
    let mut missing = open(&guarded, None);
    missing.start();
    missing.step(read_result(&guarded));
    assert!(missing.step(creator(None)).is_empty());
    assert_eq!(missing.finalize(), Err(ShareError::PasswordRequired));
    let mut wrong = open(&guarded, Some("guess"));
    wrong.start();
    wrong.step(read_result(&guarded));
    let log = wrong.step(creator(None));
    assert!(matches!(log[0], Effect::SubOperation(_)));
    wrong.step(log_written());
    assert_eq!(wrong.finalize(), Err(ShareError::WrongPassword));
    let mut right = open(&guarded, Some("secret"));
    right.start();
    right.step(read_result(&guarded));
    right.step(creator(Some(false)));
    right.step(log_written());
    let opened = right.finalize().unwrap();
    assert_eq!(
        opened.expires_at,
        500 + aruna_core::structs::storage::share::VISIT_SECS
    );
    let visit = ShareToken::parse(&opened.visit_token).unwrap();
    assert!(guarded.admits(&visit, 500));
}

/// The creator's user document read: absent, active or deactivated.
fn creator(deactivated: Option<bool>) -> Event {
    let value = deactivated.map(|deactivated| {
        let attributes = match deactivated {
            true => [(
                aruna_core::user::validation::DEACTIVATED_ATTRIBUTE.to_string(),
                "true".to_string(),
            )]
            .into(),
            false => Default::default(),
        };
        let account = aruna_core::structs::identity::user::User {
            user_id: user(4),
            name: "creator".into(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes,
        };
        let actor = aruna_core::structs::identity::auth::Actor {
            node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
            user_id: user(4),
            realm_id: RealmId::from_bytes([1; 32]),
        };
        account.to_bytes(&actor).unwrap().into()
    });
    Event::Storage(StorageEvent::ReadResult {
        key: Vec::new().into(),
        value,
    })
}

#[test]
fn deactivation_ends_link() {
    // A deactivated creator's link answers like an ended one: no password step, no log entry.
    let mut guarded = link(ShareMode::Read);
    guarded.password = Some(PasswordHash::new("secret", CHEAP, [8; 16]).unwrap());
    for password in [None, Some("guess"), Some("secret")] {
        let mut operation = open(&guarded, password);
        operation.start();
        let read = operation.step(read_result(&guarded));
        assert!(matches!(
            read[0],
            Effect::Storage(StorageEffect::Read { .. })
        ));
        assert!(operation.step(creator(Some(true))).is_empty());
        assert_eq!(operation.finalize(), Err(ShareError::Unavailable));
    }
}

#[test]
fn open_hides_ended() {
    let mut revoked = link(ShareMode::Read);
    revoked.revoked_at = Some(400);
    let mut operation = open(&revoked, None);
    operation.start();
    operation.step(read_result(&revoked));
    assert_eq!(operation.finalize(), Err(ShareError::Unavailable));
    let mut unknown = open(&link(ShareMode::Read), None);
    unknown.start();
    unknown.step(Event::Storage(StorageEvent::ReadResult {
        key: Vec::new().into(),
        value: None,
    }));
    assert_eq!(unknown.finalize(), Err(ShareError::Unavailable));
    // A visit token cannot reopen a link to skip its password.
    let own = link(ShareMode::Read);
    let mut replay = OpenShareOperation::new(OpenShareConfig {
        token: ShareToken::parse(&own.visit_token(600)).unwrap(),
        ..open(&own, None).config
    });
    replay.start();
    replay.step(read_result(&own));
    assert_eq!(replay.finalize(), Err(ShareError::Unavailable));
}

#[test]
fn revoke_own_link() {
    let own = link(ShareMode::Read);
    let mut foreign = RevokeShareOperation::new(user(9), own.id, 600);
    foreign.start();
    assert!(foreign.step(read_result(&own)).is_empty());
    assert_eq!(foreign.finalize(), Err(ShareError::Unavailable));
    let mut mine = RevokeShareOperation::new(own.created_by, own.id, 600);
    mine.start();
    let save = mine.step(read_result(&own));
    let Effect::Storage(StorageEffect::BatchWrite { writes, .. }) = &save[0] else {
        panic!("revoke saves the link");
    };
    let stored = ShareLink::from_bytes(&writes[0].2).unwrap();
    assert_eq!(stored.revoked_at, Some(600));
    mine.step(batch_written());
    assert_eq!(mine.finalize(), Ok(()));
}
