//! Drives the deletion guard through the cache mark, the lifecycle read and the effect.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::metadata::visible_scope::call_tests::answer as metadata;
use crate::tests::calls::{child, runs_child};
use aruna_core::errors::StorageError;

const GRAPH: &str = "urn:graph:guarded";

fn contains() -> MetadataEffect {
    MetadataEffect::ContainsGraph {
        graph_iri: GRAPH.to_string(),
    }
}

fn guard(effect: MetadataEffect) -> DeletionGuardOperation {
    DeletionGuardOperation::new(GRAPH.to_string(), effect)
}

// Walks a guard past an unmarked cache to the lifecycle read's answer.
fn read_lifecycle(guard: &mut DeletionGuardOperation, deleted: bool) -> Turn {
    let mark = graph_call(MetadataEffect::DeletionMark {
        graph_iri: GRAPH.to_string(),
    });
    assert_eq!(guard.first(), Ok(Next::Call(mark)));
    let unmarked = guard.next(metadata(MetadataEvent::DeletionMark { deleted: false }));
    assert!(runs_child(unmarked));
    guard.next(child(Ok::<_, GraphDeletedError>(deleted)))
}

#[test]
fn caches_pass_through() {
    let generation = MetadataEffect::CacheGeneration;
    assert_eq!(metadata_call(generation.clone()), graph_call(generation));
    assert!(matches!(metadata_call(contains()), IoCall::Child(_)));
}

#[test]
fn marked_graph_refused() {
    let policy = MetadataEffect::SetGraphPolicy {
        graph_iri: GRAPH.to_string(),
        policy: aruna_core::metadata::MetadataGraphPolicy {
            public: true,
            permission_paths: Vec::new(),
        },
    };
    let mut set = guard(policy);
    assert_eq!(
        set.hold_scope(),
        Some(HoldScope::GraphFence(fence_shard(GRAPH)))
    );
    set.first().unwrap();
    let refused = set.next(metadata(MetadataEvent::DeletionMark { deleted: true }));
    let Ok(Next::Done(MetadataEvent::Error { error, .. })) = refused else {
        panic!("expected the refusal, got {refused:?}");
    };
    assert!(matches!(error, MetadataError::InvalidInput(_)));
}

#[test]
fn tombstone_answers_absent() {
    let mut check = guard(contains());
    assert_eq!(check.hold_scope(), None);
    let store = graph_call(MetadataEffect::MarkDeletion {
        graph_iri: GRAPH.to_string(),
        deleted: true,
    });
    assert_eq!(read_lifecycle(&mut check, true), Ok(Next::Call(store)));
    let absent = MetadataEvent::ContainsGraphResult {
        graph_iri: GRAPH.to_string(),
        exists: false,
    };
    let marked = check.next(metadata(MetadataEvent::DeletionMarked));
    assert_eq!(marked, Ok(Next::Done(absent)));
}

#[test]
fn live_graph_runs() {
    let mut check = guard(contains());
    read_lifecycle(&mut check, false).unwrap();
    let run = check.next(metadata(MetadataEvent::DeletionMarked));
    assert_eq!(run, Ok(Next::Call(graph_call(contains()))));
    let present = MetadataEvent::ContainsGraphResult {
        graph_iri: GRAPH.to_string(),
        exists: true,
    };
    let done = check.next(metadata(present.clone()));
    assert_eq!(done, Ok(Next::Done(present)));
}

#[test]
fn lifecycle_failure_answers() {
    let mut check = guard(contains());
    check.first().unwrap();
    check
        .next(metadata(MetadataEvent::DeletionMark { deleted: false }))
        .unwrap();
    let failed = GraphDeletedError::Storage(StorageError::Timeout);
    let answered = check.next(child(Err::<bool, _>(failed)));
    let expected = MetadataEvent::Error {
        graph_iri: Some(GRAPH.to_string()),
        error: MetadataError::Storage(StorageError::Timeout),
    };
    assert_eq!(answered, Ok(Next::Done(expected)));
}
