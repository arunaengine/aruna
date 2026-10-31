// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::permission_path::permission_pattern_matches;
use ulid::Ulid;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[2; 32]).public()
}

fn link(target: ShareTarget, mode: ShareMode) -> ShareLink {
    ShareLink {
        id: Ulid::from_bytes([3; 16]),
        group_id: Ulid::from_bytes([4; 16]),
        target,
        mode,
        created_by: UserId::new(Ulid::from_bytes([5; 16]), realm()),
        created_at: 100,
        label: String::new(),
        expires_at: Some(1_000),
        password: None,
        secret: [6; 32],
        signing_key: [6; 32],
        revoked_at: None,
    }
}

fn folder(prefix: &str) -> ShareTarget {
    ShareTarget::Folder {
        bucket: "data".into(),
        prefix: prefix.into(),
    }
}

#[test]
fn uploads_need_folder() {
    let object = ShareTarget::Object {
        bucket: "data".into(),
        key: "a.txt".into(),
    };
    let upload = link(object.clone(), ShareMode::Upload);
    assert_eq!(
        validate_link(&upload, 100),
        Err(ShareRuleError::UploadTarget)
    );
    assert_eq!(validate_link(&link(object, ShareMode::Read), 100), Ok(()));
    let dataset = ShareTarget::Dataset {
        document_id: Ulid::from_bytes([7; 16]),
    };
    let upload = link(dataset, ShareMode::ReadUpload);
    assert_eq!(
        validate_link(&upload, 100),
        Err(ShareRuleError::UploadTarget)
    );
    assert_eq!(
        validate_link(&link(folder("in/"), ShareMode::Upload), 100),
        Ok(())
    );
}

#[test]
fn rejects_bad_links() {
    let read = |target| link(target, ShareMode::Read);
    assert_eq!(
        validate_link(&read(folder("in")), 100),
        Err(ShareRuleError::Prefix)
    );
    assert_eq!(
        validate_link(&read(folder("../in/")), 100),
        Err(ShareRuleError::Prefix)
    );
    assert_eq!(
        validate_link(&read(folder("in/")), 1_000),
        Err(ShareRuleError::Expiry)
    );
    let mut long = read(folder("in/"));
    long.label = "x".repeat(MAX_LABEL + 1);
    assert_eq!(validate_link(&long, 100), Err(ShareRuleError::Label));
}

#[test]
fn scope_confines_visit() {
    let share = link(folder("in [1]/"), ShareMode::Read);
    let pattern = scope_pattern(&share, realm(), node()).unwrap();
    let path = |key: &str| object_permission_path(realm(), share.group_id, node(), "data", key);
    assert!(permission_pattern_matches(&pattern, &path("in [1]/a.txt")));
    assert!(!permission_pattern_matches(&pattern, &path("in 1/a.txt")));
    assert!(!permission_pattern_matches(&pattern, &path("other/a.txt")));
    let context = visit_context(&share, realm(), pattern);
    assert_eq!(context.user_id, share.created_by);
    assert_eq!(context.session.unwrap().kind, SessionKind::Share);
    let upload = link(folder("in/"), ShareMode::Upload);
    let restriction = &visit_context(&upload, realm(), String::new())
        .path_restrictions
        .unwrap()[0];
    assert_eq!(restriction.permission, Permission::WRITE);
}

#[test]
fn visit_ends_with_link() {
    let mut share = link(folder("in/"), ShareMode::Read);
    assert_eq!(visit_expiry(&share, 500), 1_000);
    share.expires_at = None;
    assert_eq!(visit_expiry(&share, 500), 500 + VISIT_SECS);
}

#[test]
fn upload_names_checked() {
    let target = folder("in/");
    assert_eq!(upload_key(&target, "a/b.txt").unwrap(), "in/a/b.txt");
    assert_eq!(upload_key(&target, "../b.txt"), Err(ShareRuleError::Key));
    assert_eq!(upload_key(&target, "/b.txt"), Err(ShareRuleError::Key));
    assert_eq!(upload_key(&target, "a\nb"), Err(ShareRuleError::Key));
    let bucket = ShareTarget::Bucket {
        bucket: "data".into(),
    };
    assert_eq!(upload_key(&bucket, "b.txt").unwrap(), "b.txt");
}

#[test]
fn numbered_keys_keep_extension() {
    assert_eq!(numbered_key("in/report.pdf", 2), "in/report (2).pdf");
    assert_eq!(numbered_key("report", 1), "report (1)");
    assert_eq!(numbered_key(".env", 1), ".env (1)");
    assert_eq!(numbered_key("in/a.tar.gz", 3), "in/a.tar (3).gz");
}
