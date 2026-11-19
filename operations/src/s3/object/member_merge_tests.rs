//! Tests how merged listings read member answers and page over ranges, call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::structs::storage::distribution::DistributionRevision;

use super::*;

fn caller() -> MergeCaller {
    MergeCaller {
        ids: None,
        caller: DelegatedCaller {
            user_id: aruna_core::UserId::nil(RealmId::from_bytes([1; 32])),
            group_id: Ulid::from_bytes([2; 16]),
            credential_id: String::new(),
            restrictions: None,
            credential_expiry: 0,
        },
        binding: DistributionBinding {
            set_id: Ulid::from_bytes([3; 16]),
            member_id: Ulid::from_bytes([4; 16]),
            incarnation: 0,
            revision: DistributionRevision::default(),
        },
    }
}

fn range(lower: &str, upper: Option<&str>) -> KeyRange {
    KeyRange {
        range_id: Ulid::from_bytes([5; 16]),
        lower: lower.to_string(),
        upper: upper.map(str::to_string),
        lineage: None,
        coordinator: Ulid::from_bytes([6; 16]),
        epoch: 1,
        replicas: Vec::new(),
    }
}

#[test]
fn bounds_in_query() {
    let bounded = range("a b", Some("m&n"));
    let query = range_query(&[("list-type", "2".to_string())], &bounded);
    assert_eq!(query, "list-type=2&aruna-lower=a+b&aruna-upper=m%26n");
}

#[test]
fn holder_page_parsed() {
    let body = "<ListBucketResult><Contents><Key>a&amp;b</Key><Size>3</Size></Contents>\
                <CommonPrefixes><Prefix>d/</Prefix></CommonPrefixes></ListBucketResult>";
    let part = remote_objects(body).ok().expect("the page reads");
    assert_eq!(part.objects[0].key(), "a&b");
    assert_eq!(part.prefixes, ["d/"]);
    assert!(part.token.is_none() && part.sealed.is_none());
}

#[test]
fn foreign_token_refused() {
    let body = "<R><NextContinuationToken>not base64!</NextContinuationToken></R>";
    assert!(matches!(remote_objects(body), Err(MergeError::Token)));
}

#[test]
fn silent_members_incomplete() {
    let mut ask = AskMembersOperation {
        from: caller(),
        range_id: Ulid::from_bytes([5; 16]),
        members: VecDeque::new(),
        operation: "ListObjectsV2",
        bucket: "b".to_string(),
        query: String::new(),
        asked: None,
    };
    let failure = ask.first().unwrap_err();
    assert!(matches!(failure, MergeError::Incomplete { .. }));
    assert!(failure.unreachable());
}

#[test]
fn marker_query_names() {
    let query = MarkerQuery {
        bucket: "b".to_string(),
        prefix: Some("p/".to_string()),
        delimiter: None,
        key_marker: None,
        id_marker: None,
    };
    let names = ("uploads", "max-uploads", "upload-id-marker");
    let markers = (Some("k".to_string()), Some("i".to_string()));
    let base = query.base(names, markers, 5);
    let found: Vec<&str> = base.iter().map(|(name, _)| *name).collect();
    assert_eq!(
        found,
        [
            "uploads",
            "max-uploads",
            "prefix",
            "key-marker",
            "upload-id-marker"
        ]
    );
}

#[test]
fn rest_markers_read() {
    let body = "<R><IsTruncated>true</IsTruncated><NextKeyMarker>k</NextKeyMarker>\
                <NextUploadIdMarker>i</NextUploadIdMarker>\
                <CommonPrefixes><Prefix>d/</Prefix></CommonPrefixes></R>";
    let (prefixes, next) = listed_rest(body, "NextUploadIdMarker");
    assert_eq!(prefixes, ["d/"]);
    assert_eq!(next, Some(("k".to_string(), Some("i".to_string()))));
}

#[test]
fn silent_coordinator_incomplete() {
    let mut remote = RemoteUploadsOperation {
        text: None,
        set_id: Ulid::from_bytes([3; 16]),
        range_id: Ulid::from_bytes([5; 16]),
    };
    let refused = MemberRefusal::Failed("no answer".to_string());
    let outcome = ChildOutcome::new(Err::<String, _>(refused));
    let answer = remote.next(IoResult::Child(outcome));
    assert!(matches!(answer, Err(MergeError::Incomplete { .. })));
}
