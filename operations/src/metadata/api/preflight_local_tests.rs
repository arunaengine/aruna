//! Drives one node's preflight through its backlinks, records, checks and freshness answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use super::super::preflight_types::{MetadataIndexState, MetadataResolvedTarget};
use super::*;
use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;

const LOCATION: &str = "s3://genes/dir/a";

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[2; 32]).public()
}

fn auth() -> AuthContext {
    AuthContext {
        user_id: UserId::nil(realm()),
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    }
}

fn location_target() -> MetadataResolvedTarget {
    MetadataResolvedTarget {
        content_w3id: LOCATION.to_string(),
        content_hash: [0; 32],
        queried_iris: vec![LOCATION.to_string()],
        targeted_versions: Vec::new(),
        removed_locations: Vec::new(),
        remove_resolvable_locations: false,
    }
}

fn preflight(auth: Option<AuthContext>, limit: usize) -> LocalPreflightOperation {
    let request = ReferenceNodeRequest {
        targets: vec![location_target()],
        limit,
    };
    references_preflight_local((realm(), node()), auth, request, None)
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn record(document_id: Ulid) -> MetadataRegistryRecord {
    MetadataRegistryRecord {
        realm_id: realm(),
        group_id: Ulid::from_parts(3, 3),
        document_id,
        document_path: "datasets/d".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: false,
        permission_path: String::new(),
        placement: aruna_core::structs::placement::record::PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: Ulid::from_parts(4, 4),
        last_event_id: Ulid::from_parts(5, 5),
    }
}

fn page(document_id: Ulid) -> BacklinkPage {
    let posting = IriIndexRecord {
        document_id,
        document_cursor: Ulid::from_parts(5, 5),
        predicate_iri: "urn:p".to_string(),
        object_iri: LOCATION.to_string(),
        subject_iris: Vec::new(),
    };
    BacklinkPage {
        documents: BTreeMap::from([(document_id, vec![posting])]),
        next: None,
    }
}

/// Answers the freshness, pending, queued and dead reads of a node with nothing unsettled.
fn settled(preflight: &mut LocalPreflightOperation, statuses: usize) -> ReferenceNodeExecution {
    let Ok(Next::Call(IoCall::Read(rows))) = preflight.next(child_free()) else {
        panic!("expected the status reads");
    };
    assert_eq!(rows.len(), statuses);
    let values = IoResult::Values(vec![None; statuses]);
    assert!(matches!(
        preflight.next(values),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let pending: Result<PendingRecords, MetadataApiError> = Ok(HashMap::new());
    assert!(matches!(
        preflight.next(child(pending)),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    let empty = || IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    assert!(matches!(
        preflight.next(empty()),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    match preflight.next(empty()) {
        Ok(Next::Done(execution)) => execution,
        _ => panic!("expected the node's answer"),
    }
}

/// The backlinks answer of an empty page, which ends the target's walk.
fn child_free() -> IoResult {
    let answer: Result<Backlinks, BacklinkError> = Ok((BacklinkPage::default(), PREFLIGHT_BUDGET));
    child(answer)
}

#[test]
fn anonymous_refused() {
    let mut preflight = preflight(None, 5);
    assert!(matches!(
        preflight.first(),
        Err(MetadataApiError::Unauthorized)
    ));
}

#[test]
fn zero_limit_refused() {
    let mut preflight = preflight(Some(auth()), 0);
    assert!(matches!(
        preflight.first(),
        Err(MetadataApiError::BadRequest)
    ));
}

#[test]
fn location_walks_backlinks() {
    let mut preflight = preflight(Some(auth()), 5);
    assert!(matches!(
        preflight.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let execution = settled(&mut preflight, 0);
    assert!(execution.visible_references.is_empty());
    assert_eq!(execution.freshness.index_state, MetadataIndexState::Current);
    assert!(execution.path_style_available);
    let [target] = &execution.targets[..] else {
        panic!("expected one target");
    };
    assert!(!target.hidden_references_exist && !target.resolvable_location_found);
}

#[test]
fn unreadable_document_hidden() {
    let mut preflight = preflight(Some(auth()), 5);
    preflight.first().unwrap();
    let document_id = Ulid::from_parts(6, 6);
    let backlinks: Result<Backlinks, BacklinkError> = Ok((page(document_id), PREFLIGHT_BUDGET));
    let Ok(Next::Call(IoCall::Group(group))) = preflight.next(child(backlinks)) else {
        panic!("expected the record lookups");
    };
    assert_eq!(group.members.len(), 1);
    let found: Result<_, LiveRecordsError> = Ok(Some(record(document_id)));
    let records = IoResult::Group(aruna_core::events::GroupEvent::Finished {
        results: vec![aruna_core::events::GroupResult::Done(Box::new(
            aruna_core::events::Event::Child(ChildOutcome::new(found)),
        ))],
    });
    assert!(matches!(
        preflight.next(records),
        Ok(Next::Call(IoCall::Group(_)))
    ));
    let denied: Result<(), MetadataApiError> = Err(MetadataApiError::NotFound);
    let checks = IoResult::Group(aruna_core::events::GroupEvent::Finished {
        results: vec![aruna_core::events::GroupResult::Done(Box::new(
            aruna_core::events::Event::Child(ChildOutcome::new(denied)),
        ))],
    });
    let Ok(Next::Call(IoCall::Read(rows))) = preflight.next(checks) else {
        panic!("expected the status reads");
    };
    assert_eq!(rows.len(), 1);
    let values = IoResult::Values(vec![None]);
    preflight.next(values).unwrap();
    let pending: Result<PendingRecords, MetadataApiError> = Ok(HashMap::new());
    preflight.next(child(pending)).unwrap();
    let empty = || IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    preflight.next(empty()).unwrap();
    let Ok(Next::Done(execution)) = preflight.next(empty()) else {
        panic!("expected the node's answer");
    };
    assert!(execution.targets[0].hidden_references_exist);
    assert!(execution.visible_references.is_empty());
    // The document has no status yet, so its references may still be missing.
    assert_eq!(execution.freshness.index_state, MetadataIndexState::Pending);
}
