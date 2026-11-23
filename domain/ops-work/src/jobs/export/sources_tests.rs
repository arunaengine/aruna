//! Finds export candidates through the lookup calls and answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::tests::{plain_entity, source_scope, stored_candidate};
use super::*;
use crate::jobs::import::job_call;
use crate::jobs::repository::test_support::child;

fn stops(cancelled: bool, shutdown: bool) -> IoResult {
    let stops = JobStops {
        cancelled,
        shutdown,
    };
    let event = aruna_core::jobs::JobEvent::Stops(stops);
    IoResult::Event(Box::new(aruna_core::events::Event::Job(event)))
}

fn lookup(aliases: AliasState) -> HashCandidatesOperation {
    HashCandidatesOperation::new(source_scope(), [1; 32], None, Vec::new(), aliases)
}

fn resolved_state() -> AliasState {
    let mut state = AliasState::default();
    state
        .resolved
        .insert([1; 32], (vec![stored_candidate()], true));
    state
}

// A cancel wins over a shutdown, and no stop is no failure.
#[test]
fn stops_become_failures() {
    let stop = |cancelled, shutdown| {
        stop_failure(JobStops {
            cancelled,
            shutdown,
        })
    };
    assert_eq!(stop(true, true), Some(ExportFailure::Cancelled));
    assert_eq!(stop(false, true), Some(ExportFailure::Interrupted));
    assert_eq!(stop(false, false), None);
}

// A resumed export counts the hash aliases its candidates already use.
#[test]
fn state_seeds_aliases() {
    let mut entity = plain_entity("a");
    let mut candidate = stored_candidate();
    candidate.report_source = ExportReportSource::Hash;
    entity.candidates = vec![candidate];
    let state = AliasState::of(&[entity]);
    assert_eq!((state.paths.len(), state.keys.len()), (1, 1));
    assert!(!state.over_limit());
}

// Only groups without a loaded policy are asked for.
#[test]
fn policies_load_once() {
    let mut entity = plain_entity("a");
    entity.candidates = vec![stored_candidate()];
    let state = AliasState::default();
    assert_eq!(state.candidate_groups(&[entity]).len(), 1);
    assert!(state.candidate_groups(&[]).is_empty());
}

// An unknown hash starts at its path index.
#[test]
fn unknown_reads_paths() {
    let mut lookup = lookup(AliasState::default());
    let call = IoCall::child(ResolvePathsOperation::new([1; 32]));
    assert_eq!(lookup.first(), Ok(Next::Call(call)));
    assert_eq!(lookup.step, HashStep::Paths);
}

// A known hash reuses its aliases and then asks for holders.
#[test]
fn known_asks_holders() {
    let mut lookup = lookup(resolved_state());
    assert!(matches!(lookup.first(), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(lookup.step, HashStep::Holders);
    assert!(lookup.denied);
    assert_eq!(lookup.candidates, vec![stored_candidate()]);
}

// Failed holder discovery is reported, and it does not fail the export.
#[test]
fn holder_failure_unavailable() {
    let mut lookup = lookup(resolved_state());
    lookup.first().unwrap();
    let failed = child(Err::<Vec<NodeId>, _>(GetHoldersError::Aborted));
    let Ok(Next::Done(found)) = lookup.next(failed) else {
        panic!("expected the finished lookup");
    };
    assert!(found.unavailable);
}

// Rules of a group this node cannot read grant nothing.
#[test]
fn unknown_group_empty() {
    let group_id = Ulid::from_parts(9, 9);
    let mut lookup = lookup(AliasState::default());
    lookup.step = HashStep::Rules(group_id);
    let missing = child(Err::<PermissionRules, _>(AuthorizationError::GroupNotFound));
    lookup.next(missing).unwrap();
    assert_eq!(
        lookup.aliases.rules.get(&group_id),
        Some(&PermissionRules::default())
    );
}

// A resolve checks the job's stops before each entity and counts each one.
#[test]
fn resolve_walks_entities() {
    let entity = plain_entity("a");
    let mut resolve =
        ResolveEntriesOperation::new(source_scope(), vec![entity.clone()], AliasState::default());
    assert_eq!(resolve.first(), Ok(Next::Call(job_call(JobEffect::Stops))));
    assert!(matches!(
        resolve.next(stops(false, false)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let end: EntityEnd = (entity.clone(), AliasState::default());
    let report = JobEffect::Report(JobReport {
        current: None,
        total: None,
        advance: 1,
        checkpoint: false,
    });
    assert_eq!(
        resolve.next(child(Ok::<_, ExportFailure>(end))),
        Ok(Next::Call(job_call(report)))
    );
}

// A cancelled job resolves nothing further.
#[test]
fn resolve_stops_cancelled() {
    let mut resolve = ResolveEntriesOperation::new(
        source_scope(),
        vec![plain_entity("a")],
        AliasState::default(),
    );
    resolve.first().unwrap();
    assert_eq!(
        resolve.next(stops(true, false)),
        Err(ExportFailure::Cancelled)
    );
}

// An entity without any way to find its bytes is reported as missing.
#[test]
fn sourceless_entity_missing() {
    let mut resolve =
        ResolveEntityOperation::new(source_scope(), plain_entity("a"), AliasState::default());
    let Ok(Next::Done((entity, _))) = resolve.first() else {
        panic!("expected the resolved entity");
    };
    assert_eq!(entity.omission, Some(ReasonCode::Missing));
}

// An entity that is already left out needs no lookup.
#[test]
fn omitted_entity_skipped() {
    let mut entity = plain_entity("a");
    entity.omission = Some(ReasonCode::External);
    let mut resolve =
        ResolveEntityOperation::new(source_scope(), entity.clone(), AliasState::default());
    assert_eq!(
        resolve.first(),
        Ok(Next::Done((entity, AliasState::default())))
    );
}

// A version and a hash that disagree leave the entity out.
#[test]
fn mismatch_unsupported() {
    let mut resolve =
        ResolveEntityOperation::new(source_scope(), plain_entity("a"), AliasState::default());
    resolve.hash = Some([1; 32]);
    resolve.exact_found(ResolveResult::Missing {
        hash: Some([2; 32]),
    });
    let Ok(Next::Done((entity, _))) = resolve.use_hash() else {
        panic!("expected the resolved entity");
    };
    assert_eq!(entity.omission, Some(ReasonCode::Unsupported));
}
