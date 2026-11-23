//! Tests the install grant match and the first call of an install-scoped authorization.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::plugin_installs::InstallState;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::user::id::UserId;
use ulid::Ulid;

use super::*;
use crate::tests::calls::record_node;

fn install() -> PluginInstall {
    PluginInstall {
        install_id: Ulid::from_bytes([1; 16]),
        group_id: Ulid::from_bytes([2; 16]),
        plugin: "map-reads".to_string(),
        version: "0.1.0".to_string(),
        digest: "sha256:aa".to_string(),
        state: InstallState::Active,
        paused: false,
        settings: "{}".to_string(),
        grants: Default::default(),
        account: UserId::local(Ulid::from_bytes([3; 16]), RealmId([3u8; 32])),
        role_id: Ulid::from_bytes([4; 16]),
        receiver: record_node(1),
        granted_by: UserId::local(Ulid::from_bytes([5; 16]), RealmId([3u8; 32])),
        request: "sha256:bb".to_string(),
        revision: 1,
        active_since_ms: Some(1),
        failed: None,
        prerequisites: Vec::new(),
        activated_by: None,
        widened: None,
        secrets: Default::default(),
    }
}

/// The test install with `read` on `read` and `write` on `write`.
fn granted_install(read: &str, write: &str) -> PluginInstall {
    let mut install = install();
    install.grants = BTreeMap::from([
        ("read".to_string(), vec![read.to_string()]),
        ("write".to_string(), vec![write.to_string()]),
    ]);
    install
}

// A read is covered by read and write grants, a task run only by write grants, and a pattern
// that does not cover the target is never one of them.
#[test]
fn granted_covers_target() {
    let install = granted_install("/r/g/a/data/**", "/r/g/a/data/out/**");
    let read = granted(&install, Permission::ObjectsRead, "/r/g/a/data/out/x");
    let both = BTreeSet::from([
        "/r/g/a/data/**".to_string(),
        "/r/g/a/data/out/**".to_string(),
    ]);
    assert_eq!(read, both);
    let run = granted(&install, Permission::JobsRun, "/r/g/a/data/in/x");
    assert!(run.is_empty());
    let unknown = PluginInstall {
        grants: BTreeMap::from([("later".to_string(), vec!["/r/**".to_string()])]),
        ..install
    };
    assert!(parse_grants(&unknown.grants).is_empty());
}

// Each access reads its rules first; a copy checks read and then write.
#[test]
fn authorize_reads_rules() {
    let install = install();
    let realm_id = install.account.realm_id;
    let target = object_permission_path(realm_id, install.group_id, install.receiver, "raw", "x");
    let patterns = BTreeSet::from([target.clone()]);
    let mut copy = Authorize::call(&install, Permission::ObjectsCopy, &target, &patterns);
    assert_eq!(copy.accesses.len(), 2);
    assert!(matches!(copy.first(), Ok(Next::Call(IoCall::Read(_)))));
    assert_eq!(copy.accesses, VecDeque::from([Access::WRITE]));
}
