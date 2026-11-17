//! Fetches one bounded page of a job family from its current holders.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, JobRecordEffect, JobRecordFrame, NetEffect};
use aruna_core::events::{Event, JobRecordEvent, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::types::Effects;
use smallvec::smallvec;
use tracing::debug;

use super::LifecycleError;

/// Reads one bounded page of a family from its current holders, with the holder
/// that answered, so the records keep their real relay in the audit.
pub(super) type FetchedFamily = (Option<NodeId>, Vec<JobRecordFrame>);

#[derive(Debug, PartialEq)]
pub(super) struct FetchFamilyOperation {
    effect: Option<JobRecordEffect>,
    sent: bool,
    outcome: Option<Result<FetchedFamily, LifecycleError>>,
}

impl FetchFamilyOperation {
    pub(super) fn new(effect: JobRecordEffect) -> Self {
        Self {
            effect: Some(effect),
            sent: false,
            outcome: None,
        }
    }
}

impl Operation for FetchFamilyOperation {
    type Output = FetchedFamily;
    type Error = LifecycleError;

    fn start(&mut self) -> Effects {
        match self.effect.take() {
            Some(effect) => {
                self.sent = true;
                smallvec![Effect::Net(NetEffect::JobRecord(Box::new(effect)))]
            }
            None => {
                self.outcome = Some(Err(LifecycleError::NotFinished));
                smallvec![]
            }
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        if !std::mem::take(&mut self.sent) {
            self.outcome = Some(Err(LifecycleError::UnexpectedEvent {
                state: "Settled".to_string(),
                expected: "no further event",
                got: event.label(),
            }));
            return smallvec![];
        }
        self.outcome = Some(match event {
            Event::Net(NetEvent::JobRecord(JobRecordEvent::Fetched {
                holder, records, ..
            })) => Ok((Some(holder), records.into_inner())),
            Event::Net(NetEvent::JobRecord(JobRecordEvent::Unavailable(message))) => {
                debug!(message, "No family holder answered the record fetch");
                Ok((None, Vec::new()))
            }
            other => Err(LifecycleError::UnexpectedEvent {
                state: "Fetch".to_string(),
                expected: "job record page",
                got: other.label(),
            }),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.outcome.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(LifecycleError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use std::time::Duration;

    use aruna_core::effects::{HolderList, JobRecordEffect, PageLimit};
    use aruna_core::events::{Event, JobRecordEvent, NetEvent};
    use aruna_core::operation::Operation;

    use super::FetchFamilyOperation;
    use crate::jobs::lifecycle::LifecycleError;
    use crate::tests::records::{Family, REALM, node};

    fn fetch() -> FetchFamilyOperation {
        let family = Family::new([3u8; 32]);
        FetchFamilyOperation::new(JobRecordEffect::Fetch {
            realm_id: REALM,
            placement: family.placement,
            holders: HolderList::new(vec![node(2)]).expect("one holder"),
            submission_id: family.submission_id,
            request_digest: Some(family.request_digest),
            cursor: None,
            limit: PageLimit::default(),
            deadline: Duration::from_secs(1),
        })
    }

    fn unavailable() -> Event {
        Event::Net(NetEvent::JobRecord(JobRecordEvent::Unavailable(
            "offline".to_string(),
        )))
    }

    // An answer before the request was sent, or after the fetch settled, is refused.
    #[test]
    fn rejects_unsent_answer() {
        let mut early = fetch();
        assert!(early.step(unavailable()).is_empty());
        assert!(matches!(
            early.finalize(),
            Err(LifecycleError::UnexpectedEvent { .. })
        ));

        let mut settled = fetch();
        settled.start();
        settled.step(unavailable());
        assert!(settled.is_complete());
        settled.step(unavailable());
        assert!(matches!(
            settled.finalize(),
            Err(LifecycleError::UnexpectedEvent { .. })
        ));
    }
}
