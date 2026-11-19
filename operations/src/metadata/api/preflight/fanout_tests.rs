//! Drives the preflight coordinator through target resolution, the fanout and its assembly.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::super::MetadataFanoutStats;
use super::super::super::preflight_types::{
    MetadataIndexState, MetadataNodeFreshness, ReferenceTarget,
};
use super::*;
use aruna_core::UserId;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[2; 32]).public()
}

fn request(realm_id: RealmId) -> ReferenceRequest {
    ReferenceRequest {
        auth: AuthContext {
            user_id: UserId::nil(realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        },
        bearer_token: None,
        target: ReferenceTarget::ContentW3ids {
            content_w3ids: Vec::new(),
            remove_resolvable_locations: false,
        },
        s3_endpoint: None,
        limit: None,
        cursor: None,
        mode: Some(ApiQueryMode::Local),
        target_nodes: None,
        allow_partial: false,
    }
}

fn started(request: ReferenceRequest) -> ReferencesPreflightOperation {
    let mut preflight = references_preflight(realm(), node(), request);
    preflight.stamp(1_000);
    preflight
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn resolved() -> ResolvedPreflightTargets {
    ResolvedPreflightTargets {
        targets: Vec::new(),
        complete: true,
    }
}

#[test]
fn foreign_caller_refused() {
    let mut preflight = started(request(RealmId::from_bytes([9; 32])));
    assert!(matches!(
        preflight.first(),
        Err(MetadataApiError::Forbidden)
    ));
}

#[test]
fn target_failure_ends() {
    let mut preflight = started(request(realm()));
    assert!(matches!(
        preflight.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let failed = preflight.next(child(Err::<ResolvedPreflightTargets, _>(
        MetadataApiError::BadRequest,
    )));
    assert!(matches!(failed, Err(MetadataApiError::BadRequest)));
}

#[test]
fn bad_cursor_refused() {
    let mut preflight = started(ReferenceRequest {
        cursor: Some("not-a-cursor".to_string()),
        ..request(realm())
    });
    preflight.first().unwrap();
    let refused = preflight.next(child(Ok::<_, MetadataApiError>(resolved())));
    assert!(matches!(refused, Err(MetadataApiError::InvalidCursor(_))));
}

#[test]
fn complete_page_ends() {
    let mut preflight = started(request(realm()));
    preflight.first().unwrap();
    let asked = preflight.next(child(Ok::<_, MetadataApiError>(resolved())));
    assert!(matches!(asked, Ok(Next::Call(IoCall::Child(_)))));
    let part = ReferenceNodeExecution {
        visible_references: Vec::new(),
        targets: Vec::new(),
        freshness: MetadataNodeFreshness {
            node_id: node(),
            index_state: MetadataIndexState::Current,
            oldest_status_updated: None,
        },
        path_style_available: true,
        saturated: false,
    };
    let stats = MetadataFanoutStats {
        nodes_queried: 1,
        ..MetadataFanoutStats::default()
    };
    let answer: Result<FanoutParts<ReferenceNodeExecution>, MetadataApiError> =
        Ok((vec![(node(), part)], stats));
    let Ok(Next::Done(execution)) = preflight.next(child(answer)) else {
        panic!("expected a complete page");
    };
    assert!(execution.complete);
    assert_eq!(execution.next_cursor, None);
}
