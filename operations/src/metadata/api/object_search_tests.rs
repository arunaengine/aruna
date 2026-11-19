//! Drives the object search through signers, discovery, fanout, skipped buckets and signature.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::object::search::{ObjectInventoryHit, ObjectKeyMatch, SearchNodeHit};
use aruna_core::UserId;
use aruna_core::structs::identity::auth::AuthContext;
use ulid::Ulid;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn secret(seed: u8) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[seed; 32])
}

fn node(seed: u8) -> NodeId {
    secret(seed).public()
}

fn request(mode: ObjectQueryMode, cursor: Option<String>) -> SearchQueryRequest {
    SearchQueryRequest {
        auth: AuthContext {
            user_id: UserId::nil(realm()),
            realm_id: realm(),
            path_restrictions: None,
            session: None,
        },
        bearer_token: None,
        query: "reads".to_string(),
        key_match: ObjectKeyMatch::Substring,
        bucket: None,
        limit: 1,
        cursor,
        mode,
        target_nodes: None,
    }
}

fn started(request: SearchQueryRequest) -> ObjectSearchOperation {
    let mut search = search_objects(realm(), node(1), request, false);
    search.stamp(1_000);
    search
}

fn page(keys: &[&str], next: Option<&str>) -> SearchNodePage {
    let hit = |key: &&str| SearchNodeHit {
        hit: ObjectInventoryHit {
            node_id: node(1),
            group_id: Ulid::nil(),
            bucket: "data".to_string(),
            key: key.to_string(),
            content_w3id: None,
            checksum: None,
            size: None,
            updated_at: None,
            metadata: Default::default(),
        },
        cursor_key: key.as_bytes().to_vec(),
    };
    SearchNodePage {
        hits: keys.iter().map(hit).collect(),
        next_start_after: next.map(|key| key.as_bytes().to_vec()),
        observed_at: UNIX_EPOCH,
        incomplete_ranges: 0,
        remote_ranges: 0,
        index_progress: Vec::new(),
        partial_ranges: Vec::new(),
        skipped_buckets: Vec::new(),
        changed: Some(Vec::new()),
        layout: None,
    }
}

fn fanout(parts: Vec<SearchNodePage>) -> IoResult {
    let parts = parts.into_iter().map(|part| (node(1), part)).collect();
    let answer: Result<FanoutParts<SearchNodePage>, MetadataApiError> =
        Ok((parts, MetadataFanoutStats::default()));
    IoResult::Child(ChildOutcome::new(answer))
}

#[test]
fn last_page_ends() {
    let mut search = started(request(ObjectQueryMode::Local, None));
    assert!(matches!(search.first(), Ok(Next::Call(IoCall::Child(_)))));
    let Ok(Next::Done(execution)) = search.next(fanout(vec![page(&["a"], None)])) else {
        panic!("expected the last page");
    };
    assert_eq!(execution.hits.len(), 1);
    assert_eq!(execution.next_cursor, None);
    assert_eq!(execution.as_of, UNIX_EPOCH + Duration::from_millis(1_000));
}

#[test]
fn open_page_signs_cursor() {
    let mut search = started(request(ObjectQueryMode::Local, None));
    search.first().unwrap();
    let Ok(Next::Call(IoCall::Effect(call))) = search.next(fanout(vec![page(&["a"], Some("a"))]))
    else {
        panic!("expected the cursor signature");
    };
    let Effect::Net(NetEffect::CursorSign(bytes)) = *call else {
        panic!("expected a cursor signature");
    };
    assert!(aruna_core::cursor::names_signer(&bytes, node(1).as_bytes()));
    let signed = NetEvent::CursorSigned(Some(secret(1).sign(&bytes)));
    let Ok(Next::Done(execution)) = search.next(IoResult::Event(Box::new(Event::Net(signed))))
    else {
        panic!("expected a page with its cursor");
    };
    let next = execution.next_cursor.expect("a cursor");
    let fingerprint = search.plan.as_ref().unwrap().fingerprint;
    assert!(ObjectCursor::decode(&next, fingerprint, &[node(1)]).is_ok());
}

#[test]
fn cursor_reads_config() {
    let raw = "cursor".to_string();
    let mut search = started(request(ObjectQueryMode::DistributedBestEffort, Some(raw)));
    assert!(matches!(search.first(), Ok(Next::Call(IoCall::Child(_)))));
    let missing = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
    let refused = search.next(IoResult::Child(ChildOutcome::new(missing)));
    assert!(matches!(refused, Err(MetadataApiError::ServiceUnavailable)));
}

#[test]
fn late_discovery_local() {
    let mut search = started(request(ObjectQueryMode::DistributedBestEffort, None));
    let Ok(Next::Call(IoCall::Group(group))) = search.first() else {
        panic!("expected the bounded discovery");
    };
    assert_eq!(
        group.form,
        GroupForm::All {
            deadline: Some(DISTRIBUTED_QUERY_DEADLINE)
        }
    );
    let late = IoResult::Group(GroupEvent::Finished {
        results: vec![GroupResult::Unfinished],
    });
    assert!(matches!(
        search.next(late),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let ObjectStage::Fanout(partitions) = &search.stage else {
        panic!("expected the fanout");
    };
    assert!(partitions.discovery_failed);
}

#[test]
fn strict_discovery_refused() {
    let mut search = started(request(ObjectQueryMode::DistributedStrict, None));
    search.first().unwrap();
    let late = IoResult::Group(GroupEvent::Finished {
        results: vec![GroupResult::Unfinished],
    });
    assert!(matches!(
        search.next(late),
        Err(MetadataApiError::ServiceUnavailable)
    ));
}

#[test]
fn skipped_buckets_searched() {
    let mut search = started(request(ObjectQueryMode::Local, None));
    search.first().unwrap();
    let skipped = SearchNodePage {
        incomplete_ranges: 1,
        skipped_buckets: vec!["sharded".to_string()],
        ..page(&[], None)
    };
    let Ok(Next::Call(IoCall::Group(group))) = search.next(fanout(vec![skipped])) else {
        panic!("expected the skipped bucket search");
    };
    assert_eq!(group.members.len(), 1);
    let (found, _) = assemble_object_execution(
        (search.plan.as_ref().unwrap(), node(1)),
        fresh_partitions(
            search.plan.as_ref().unwrap(),
            node(1),
            (vec![node(1)], false),
            UNIX_EPOCH,
        )
        .unwrap(),
        vec![(node(1), page(&["b"], None))],
        MetadataFanoutStats::default(),
    )
    .unwrap();
    let answer: Result<ObjectExecution, MetadataApiError> = Ok(found);
    let results = vec![GroupResult::Done(Box::new(Event::Child(
        ChildOutcome::new(answer),
    )))];
    let finished = IoResult::Group(GroupEvent::Finished { results });
    let Ok(Next::Done(execution)) = search.next(finished) else {
        panic!("expected the merged page");
    };
    let keys: Vec<_> = execution.hits.iter().map(|hit| hit.key.as_str()).collect();
    assert_eq!(keys, vec!["b"]);
    assert_eq!(execution.incomplete_ranges, 0);
    assert!(execution.complete);
}

#[test]
fn failed_skip_incomplete() {
    let mut search = started(request(ObjectQueryMode::Local, None));
    search.first().unwrap();
    let skipped = SearchNodePage {
        incomplete_ranges: 1,
        skipped_buckets: vec!["sharded".to_string()],
        ..page(&[], None)
    };
    search.next(fanout(vec![skipped])).unwrap();
    let answer: Result<ObjectExecution, MetadataApiError> =
        Err(MetadataApiError::ServiceUnavailable);
    let results = vec![GroupResult::Done(Box::new(Event::Child(
        ChildOutcome::new(answer),
    )))];
    let Ok(Next::Done(execution)) = search.next(IoResult::Group(GroupEvent::Finished { results }))
    else {
        panic!("expected an incomplete page");
    };
    assert_eq!(execution.incomplete_ranges, 1);
    assert!(!execution.complete);
}
