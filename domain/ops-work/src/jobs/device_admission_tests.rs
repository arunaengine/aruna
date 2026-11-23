//! Tests the backend choice of a device job and the recheck of its admission, as calls and
//! results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::IoCall;
use aruna_core::compute::ResourceEnvelope;
use aruna_core::compute::backend::{Suitability, WorkloadSuitability};
use aruna_core::compute::offer::BackendCaps;
use aruna_core::compute::runtimes::{SESSION_TAG, SESSION_TAG_NOTEBOOK};
use aruna_core::structs::execution::job::local::JobErrorKind;

use super::*;
use crate::jobs::workflow::fixtures::{child, claimed, compute, spec, token};

/// A local Docker backend named `name` with one slot, changed by `adjust`.
fn offer(name: &str, adjust: impl FnOnce(&mut BackendCaps)) -> ExecutorOffer {
    let mut caps = BackendCaps {
        file_staging: true,
        local_site: true,
        limits: ResourceEnvelope {
            max_concurrent: Some(1),
            ..ResourceEnvelope::default()
        },
        ..BackendCaps::default()
    };
    adjust(&mut caps);
    ExecutorOffer::new((name.to_string(), "docker".to_string()), [1; 32], caps)
}

fn admitted(offer: &ExecutorOffer) -> LocalAdmission {
    LocalAdmission {
        backend: offer.name().to_string(),
        config_generation: 1,
        config_digest: offer.config_digest(),
        home_digest: offer.home(),
        subject: None,
    }
}

// The first backend in name order that takes the run is chosen; one it passes over gives
// the refusal only when none takes it.
#[test]
fn picks_first_taker() {
    let short = offer("a", |caps| caps.max_walltime_ms = Some(1));
    let offers = [short.clone(), offer("b", |_| {})];
    assert_eq!(
        select_offer(&offers, &spec()).map(ExecutorOffer::name),
        Ok("b")
    );
    let refused = select_offer(&offers[..1], &spec()).map(ExecutorOffer::name);
    assert_eq!(refused, Err(Some(RejectionVerdict::WalltimeLimit)));
}

// Each rule planning applies refuses on its own: allow-list, envelope and suitability.
#[test]
fn refuses_by_rule() {
    let refuse_batch = WorkloadSuitability {
        batch: Suitability::Refused,
        ..WorkloadSuitability::of_kind(&ExecutorKind::Docker)
    };
    type Case = (fn(&mut BackendCaps), RejectionVerdict);
    let cases: [Case; 2] = [
        (
            |caps| caps.groups = vec![Ulid::from_bytes([9; 16])],
            RejectionVerdict::GroupNotAllowed,
        ),
        (
            |caps| caps.limits.max_ram_bytes = Some(1),
            RejectionVerdict::Resources,
        ),
    ];
    let mut wanted = spec();
    wanted.resources.ram_bytes = Some(2);
    for (adjust, verdict) in cases {
        assert_eq!(backend_refusal(&offer("a", adjust), &wanted), Some(verdict));
    }
    let unsuitable = offer("a", |caps| caps.suitability = Some(refuse_batch));
    assert_eq!(
        backend_refusal(&unsuitable, &spec()),
        Some(RejectionVerdict::Unsuitable)
    );
}

// A session needs a backend with the session channel; the same run without one starts there.
#[test]
fn session_needs_channel() {
    let mut session = spec();
    let notebook = (SESSION_TAG.to_string(), SESSION_TAG_NOTEBOOK.to_string());
    session.tags.extend([notebook]);
    let plain = offer("a", |_| {});
    assert_eq!(
        backend_refusal(&plain, &session),
        Some(RejectionVerdict::Session)
    );
    assert_eq!(backend_refusal(&plain, &spec()), None);
    let bridged = offer("a", |caps| caps.session = true);
    assert_eq!(backend_refusal(&bridged, &session), None);
}

// A named backend or a kind constraint passes over every other backend without a reason.
#[test]
fn narrows_by_name() {
    let offers = [offer("a", |_| {})];
    let mut named = spec();
    named.backend = Some("b".to_string());
    assert_eq!(select_offer(&offers, &named), Err(None));
    let mut constrained = spec();
    constrained.executor_constraint = Some("slurm".to_string());
    assert_eq!(select_offer(&offers, &constrained), Err(None));
}

// The admission carries the stored config generation, and no subject on a plain device.
#[test]
fn admission_reads_generation() {
    let chosen = offer("a", |_| {});
    let mut admission = BackendAdmission::new(chosen.clone());
    assert!(matches!(
        admission.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let record = BackendConfigRecord::seed(chosen.home(), chosen.config_digest());
    let read = admission.next(child(Ok::<_, BackendRecordError>(record)));
    assert!(matches!(read, Ok(Next::Call(IoCall::Child(_)))));
    let none = child(Ok::<Option<NodeSubjectRecord>, SubjectScanError>(None));
    assert_eq!(admission.next(none), Ok(Next::Done(admitted(&chosen))));
}

fn recheck(stored: LocalAdmission) -> RecheckAdmission {
    let mut job = claimed();
    job.local_admission = Some(stored);
    let mut recheck = RecheckAdmission::of_job(&job, token(), Arc::new(spec())).unwrap();
    assert_eq!(
        recheck.first(),
        Ok(Next::Call(compute_call(ComputeEffect::Offers)))
    );
    recheck
}

/// Answers `recheck` with `offers`, then with the current admission `current`.
fn rechecked(
    recheck: &mut RecheckAdmission,
    offers: Vec<ExecutorOffer>,
    current: LocalAdmission,
) -> RecheckNext {
    let read = recheck.next(compute(ComputeAnswer::Offers(offers)));
    assert!(matches!(read, Ok(Next::Call(IoCall::Child(_)))));
    recheck.next(child(Ok::<_, LocalExecutionError>(current)))
}

// Unchanged values start the job; changed values that still pass are stored under the claim.
#[test]
fn recheck_stores_change() {
    let chosen = offer("a", |_| {});
    let mut same = recheck(admitted(&chosen));
    assert_eq!(
        rechecked(&mut same, vec![chosen.clone()], admitted(&chosen)),
        Ok(Next::Done(()))
    );
    let mut moved = admitted(&chosen);
    moved.config_generation = 2;
    let mut changed = recheck(admitted(&chosen));
    let store = rechecked(&mut changed, vec![chosen.clone()], moved);
    assert!(matches!(store, Ok(Next::Call(IoCall::Child(_)))));
    let stored = changed.next(child(Ok::<_, JobMutationError>(claimed())));
    assert_eq!(stored, Ok(Next::Done(())));
}

// A changed backend that refuses the job, or a lost one, refuses the start as retryable.
#[test]
fn recheck_refuses_retryable() {
    let before = offer("a", |_| {});
    let after = offer("a", |caps| caps.max_walltime_ms = Some(1));
    let mut refused = recheck(admitted(&before));
    let error = rechecked(&mut refused, vec![after.clone()], admitted(&after)).unwrap_err();
    assert_eq!(error.kind, JobErrorKind::Retryable);
    let mut lost = recheck(admitted(&before));
    let error = lost
        .next(compute(ComputeAnswer::Offers(Vec::new())))
        .unwrap_err();
    assert_eq!(error.kind, JobErrorKind::Retryable);
}
