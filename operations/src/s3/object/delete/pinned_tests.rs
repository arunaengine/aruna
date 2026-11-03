//! Tests the release pin refusal of permanent deletions: which answers refuse, which purge
//! scopes a release reaches, and that a refusal names no release.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::bucket_metadata::ReleaseRef;
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::origin::OriginVersion;
use aruna_core::structs::storage::release::ReleaseMembers;

use super::*;

fn query(key: &str) -> MembershipQuery {
    MembershipQuery {
        key: key.to_string(),
        version_id: Ulid::from_bytes([1; 16]),
    }
}

fn release(label: &str) -> ReleaseRef {
    ReleaseRef {
        crate_version: OriginVersion::Global(GlobalId::Version {
            realm_id: RealmId::from_bytes([1; 32]),
            version_id: Ulid::from_bytes([2; 16]),
        }),
        label: label.to_string(),
        upstream: Vec::new(),
    }
}

fn output(answers: Vec<Vec<ReleaseRef>>, complete: bool) -> MembersOutput {
    MembersOutput {
        answers: (answers.into_iter())
            .map(|releases| ReleaseMembers { releases, complete })
            .collect(),
        releases: Vec::new(),
        complete,
        refused: Vec::new(),
    }
}

#[test]
fn pins_follow_queries() {
    let queries = [query("db/a"), query("db/b")];
    let answers = vec![vec![release("6.0"), release("6.1")], Vec::new()];
    let pins = pinned(&queries, &output(answers, true)).unwrap();
    assert_eq!(pins, [true, false]);
}

#[test]
fn unchecked_answers_refuse() {
    // A release that could not be read may pin the version, so nothing is deleted.
    let queries = [query("db/a")];
    let incomplete = pinned(&queries, &output(vec![Vec::new()], false));
    assert!(matches!(incomplete, Err(PinnedError::Unchecked(_))));
    let short = pinned(&queries, &output(Vec::new(), true));
    assert!(matches!(short, Err(PinnedError::Unchecked(_))));
}

#[test]
fn scopes_reach_releases() {
    let file = StoragePurgeScope::File {
        bucket: "b".to_string(),
        key: "db/a.faa".to_string(),
    };
    let folder = StoragePurgeScope::Prefix {
        bucket: "b".to_string(),
        prefix: "db/".to_string(),
    };
    let bucket = StoragePurgeScope::Bucket {
        bucket: "b".to_string(),
    };
    assert_eq!(reach(&file, "db/"), Some("db/a.faa"));
    assert_eq!(reach(&file, ""), Some("db/a.faa"));
    assert_eq!(reach(&file, "other/"), None);
    // A folder inside a release's prefix, or one holding it, reaches it.
    assert_eq!(reach(&folder, "db/v6/"), Some("db/"));
    assert_eq!(reach(&folder, ""), Some("db/"));
    assert_eq!(reach(&folder, "dbx/"), None);
    // The whole bucket holds every release.
    assert_eq!(reach(&bucket, ""), Some(""));
    assert_eq!(reach(&bucket, "db/v6/"), Some(""));
}

#[test]
fn refusal_names_none() {
    // A release the caller cannot read also refuses, so no release label is shown.
    let error = PinnedError::Version {
        key: "db/a".to_string(),
        version_id: Ulid::from_bytes([1; 16]),
    };
    let message = error.to_string();
    assert!(
        message.contains("db/a") && message.contains("a release"),
        "{message}"
    );
    assert!(PinnedError::Scope.to_string().contains("a release"));
}
