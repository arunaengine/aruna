//! Reads the request family one accepted job alias belongs to.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::FAMILY_ALIAS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{JobFamilyId, JobId};
use aruna_core::types::Effects;
use smallvec::smallvec;

use crate::jobs::JobRouteError;
use crate::jobs::records::keys::{MAX_ALIAS_FAMILIES, alias_family, alias_prefix};

/// The family of one alias, ordered by key, so two families claiming one alias
/// resolve identically on every replica. `None` names no family here.
#[derive(Debug, PartialEq)]
pub struct AliasFamilyOperation {
    job_id: JobId,
    started: bool,
    outcome: Option<Result<Option<JobFamilyId>, JobRouteError>>,
}

impl AliasFamilyOperation {
    pub fn new(job_id: JobId) -> Self {
        Self {
            job_id,
            started: false,
            outcome: None,
        }
    }

    fn finish(&mut self, outcome: Result<Option<JobFamilyId>, JobRouteError>) -> Effects {
        self.outcome = Some(outcome);
        smallvec![]
    }
}

impl Operation for AliasFamilyOperation {
    type Output = Option<JobFamilyId>;
    type Error = JobRouteError;

    fn start(&mut self) -> Effects {
        self.started = true;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: FAMILY_ALIAS_KEYSPACE.to_string(),
            prefix: Some(alias_prefix(self.job_id)),
            start: None,
            limit: MAX_ALIAS_FAMILIES,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let outcome = match event {
            _ if !self.started || self.outcome.is_some() => Err(JobRouteError::Unavailable(
                format!("unexpected storage event: {}", event.label()),
            )),
            Event::Storage(StorageEvent::IterResult { values, .. }) => {
                Ok(values.iter().filter_map(|(key, _)| alias_family(key)).min())
            }
            Event::Storage(StorageEvent::Error { error }) => {
                Err(JobRouteError::Unavailable(error.to_string()))
            }
            other => Err(JobRouteError::Unavailable(format!(
                "unexpected storage event: {}", other.label()
            ))),
        };
        self.finish(outcome)
    }

    fn is_complete(&self) -> bool {
        self.outcome.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(JobRouteError::Unavailable(
            "job alias read did not finish".to_string(),
        )))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::FAMILY_ALIAS_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::execution::job::{JobFamilyId, JobId, SubmissionId};
    use aruna_core::types::{Key, Value};

    use super::AliasFamilyOperation;
    use crate::jobs::JobRouteError;
    use crate::jobs::records::keys::{MAX_ALIAS_FAMILIES, alias_key, alias_prefix};

    fn job() -> JobId {
        JobId::from_bytes([4u8; 16])
    }

    fn family(seed: u8) -> JobFamilyId {
        JobFamilyId {
            submission_id: SubmissionId([seed; 32]),
            request_digest: [seed; 32],
        }
    }

    fn rows(families: &[JobFamilyId]) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: families
                .iter()
                .map(|family| (alias_key(job(), family), Value::from(&[0u8; 0][..])))
                .collect(),
            next_start_after: None,
        })
    }

    fn started() -> AliasFamilyOperation {
        let mut alias = AliasFamilyOperation::new(job());
        alias.start();
        alias
    }

    #[test]
    fn picks_lowest_family() {
        let mut alias = AliasFamilyOperation::new(job());
        let scan = Effect::Storage(StorageEffect::Iter {
            key_space: FAMILY_ALIAS_KEYSPACE.to_string(),
            prefix: Some(alias_prefix(job())),
            start: None,
            limit: MAX_ALIAS_FAMILIES,
            txn_id: None,
        });
        check_trace(
            &mut alias,
            &[scan],
            vec![(rows(&[family(9), family(2)]), Vec::new())],
        );
        assert_eq!(alias.finalize(), Ok(Some(family(2))));
    }

    #[test]
    fn names_no_family() {
        let mut alias = started();
        alias.step(rows(&[]));
        assert_eq!(alias.finalize(), Ok(None));
        // A row whose key names no family is not one.
        let mut alias = started();
        alias.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(Key::from(&[1u8][..]), Value::from(&[0u8; 0][..]))],
            next_start_after: None,
        }));
        assert_eq!(alias.finalize(), Ok(None));
    }

    #[test]
    fn reports_storage_error() {
        let mut alias = started();
        let error = StorageError::Timeout;
        alias.step(Event::Storage(StorageEvent::Error {
            error: error.clone(),
        }));
        assert_eq!(
            alias.finalize(),
            Err(JobRouteError::Unavailable(error.to_string()))
        );
    }

    #[test]
    fn rejects_unfit_events() {
        let fits = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
            )
        };
        let refusals = rejects_events(started, sample_events, fits);
        assert!(!refusals.is_empty());
        for (label, effects, error) in refusals {
            assert!(effects.is_empty(), "{label} emitted effects");
            assert!(
                matches!(error, JobRouteError::Unavailable(_)),
                "{label} gave {error:?}"
            );
        }
        let unstarted = || AliasFamilyOperation::new(job());
        assert_eq!(
            rejects_events(unstarted, sample_events, |_| false).len(),
            sample_events().len()
        );
        let mut done = started();
        done.step(rows(&[family(1)]));
        done.step(rows(&[family(1)]));
        assert!(matches!(
            done.finalize(),
            Err(JobRouteError::Unavailable(_))
        ));
    }

    #[test]
    fn aborts_without_cleanup() {
        check_abort(&mut AliasFamilyOperation::new(job()), &[]);
        check_abort(&mut started(), &[]);
    }

    #[test]
    fn rejects_early_finalize() {
        rejects_finalize(AliasFamilyOperation::new(job()));
        rejects_finalize(started());
    }
}
