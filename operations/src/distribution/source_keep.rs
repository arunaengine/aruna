//! Keeps the source bytes of an external reference on a copy-on-use or cache-on-use member
//! after a read served it from its source.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The whole object is read again, bound to its ETag or checked against its digests, and
// published like any kept copy; else nothing is kept.

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::distribution::on_use::{
    CACHE_CAPACITY_BYTES, FillClaim, PullPlan, claim_key,
};
use aruna_core::time::unix_timestamp_millis;
use tracing::warn;
use ulid::Ulid;

use super::keep::{ClaimFillOperation, Claimed, KeepError, KeptCopy};
use super::pull::{KeepCopyOperation, ReleaseClaimOperation};
use super::source_plan::{
    KeepPlanOperation, Plan, PlanError, SourceKeep, SpoolError, SpoolSourceOperation, SpooledCopy,
};

/// How keeping a reference goes on after its plan.
#[derive(Debug, PartialEq)]
pub enum KeepStart {
    /// Nothing more to do; `true` once the copy is kept before the read is served.
    Settled(bool),
    /// The copy is kept in the background when a fill slot is free.
    Background(KeepSourceOperation),
}

/// Keeps the reference's bytes per the member's strategy and range behaviour: before the read
/// is served when it fills first, else in the background.
#[derive(Debug, PartialEq)]
pub struct KeepFirstOperation {
    keep: SourceKeep,
    filling: bool,
}

impl KeepFirstOperation {
    pub fn new(keep: SourceKeep) -> Self {
        Self {
            keep,
            filling: false,
        }
    }
}

impl Operation for KeepFirstOperation {
    type Output = KeepStart;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<KeepStart>, IoError> {
        let plan = KeepPlanOperation::new(self.keep.clone());
        Ok(Next::Call(IoCall::child(plan)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<KeepStart>, IoError> {
        if self.filling {
            let kept = result.child::<bool, IoError>()??;
            return Ok(Next::Done(KeepStart::Settled(kept)));
        }
        let plan = match result.child::<Option<Plan>, PlanError>()? {
            Ok(Some(plan)) => plan,
            Ok(None) => return Ok(Next::Done(KeepStart::Settled(false))),
            Err(error) => {
                let version_id = self.keep.version_id;
                warn!(%error, %version_id, "A reference was not kept");
                return Ok(Next::Done(KeepStart::Settled(false)));
            }
        };
        let first = self.keep.ranged && plan.pull == PullPlan::FillFirst(plan.kind);
        let keep = KeepSourceOperation::new(self.keep.clone(), plan);
        if first {
            self.filling = true;
            return Ok(Next::Call(IoCall::child(keep)));
        }
        Ok(Next::Done(KeepStart::Background(keep)))
    }
}

/// What the pending call of keeping a reference answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceStep {
    Claim,
    Spool,
    Publish,
    Release,
}

/// Claims, reads, verifies and publishes one kept copy of a reference; `true` once kept.
#[derive(Debug, PartialEq)]
pub struct KeepSourceOperation {
    keep: SourceKeep,
    plan: Plan,
    claim: Vec<u8>,
    step: SourceStep,
}

impl KeepSourceOperation {
    fn new(keep: SourceKeep, plan: Plan) -> Self {
        let claim = claim_key(plan.kind, plan.set_id, keep.version_id, plan.identity);
        Self {
            keep,
            plan,
            claim,
            step: SourceStep::Claim,
        }
    }

    fn not_kept(&self, reason: &dyn std::fmt::Display) {
        let version_id = self.keep.version_id;
        warn!(%reason, %version_id, "A reference was not kept");
    }

    fn publish(&mut self, (location, blake3, size): SpooledCopy) -> Next<bool> {
        let copy = KeptCopy {
            kind: self.plan.kind,
            set_id: self.plan.set_id,
            version_id: self.keep.version_id,
            identity: self.plan.identity,
            blake3,
            size,
            location,
            claim_key: self.claim.clone(),
            capacity: CACHE_CAPACITY_BYTES,
            order: Ulid::generate(),
            member_id: self.plan.member_id,
        };
        self.step = SourceStep::Publish;
        Next::Call(IoCall::child(KeepCopyOperation::new(copy)))
    }
}

impl Operation for KeepSourceOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        let claimed = FillClaim {
            claim_id: Ulid::generate(),
            claimed_at_ms: unix_timestamp_millis(),
        };
        let claim = ClaimFillOperation::new(self.claim.clone(), claimed);
        Ok(Next::Call(IoCall::child(claim)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        match self.step {
            SourceStep::Claim => match result.child::<Claimed, KeepError>()? {
                Ok(Claimed::Claimed) => {
                    let spool = SpoolSourceOperation::new(self.keep.clone(), self.plan.clone());
                    self.step = SourceStep::Spool;
                    Ok(Next::Call(IoCall::child(spool)))
                }
                Ok(Claimed::Busy) => Ok(Next::Done(false)),
                Err(error) => {
                    self.not_kept(&error);
                    Ok(Next::Done(false))
                }
            },
            SourceStep::Spool => match result.child::<SpooledCopy, SpoolError>()? {
                Ok(copy) => Ok(self.publish(copy)),
                Err(error) => {
                    self.not_kept(&error);
                    self.step = SourceStep::Release;
                    let release = ReleaseClaimOperation::new(self.claim.clone());
                    Ok(Next::Call(IoCall::child(release)))
                }
            },
            SourceStep::Publish => match result.child::<bool, KeepError>()? {
                Ok(kept) => Ok(Next::Done(kept)),
                Err(error) => {
                    self.not_kept(&error);
                    Ok(Next::Done(false))
                }
            },
            SourceStep::Release => {
                result.child::<(), IoError>()??;
                Ok(Next::Done(false))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::realm::RealmId;

    use super::*;

    fn keep() -> SourceKeep {
        SourceKeep {
            bucket: "spire".to_string(),
            key: "x".to_string(),
            version_id: Ulid::from_bytes([1; 16]),
            group_id: Ulid::from_bytes([2; 16]),
            user_id: UserId::nil(RealmId::from_bytes([9; 32])),
            ranged: true,
        }
    }

    // A version with nothing to keep ends the work before any claim.
    #[test]
    fn no_plan_settles() {
        let mut first = KeepFirstOperation::new(keep());
        assert!(matches!(first.first(), Ok(Next::Call(IoCall::Child(_)))));
        let none = Ok::<Option<Plan>, PlanError>(None);
        let answer = IoResult::Child(ChildOutcome::new(none));
        assert_eq!(
            first.next(answer),
            Ok(Next::Done(KeepStart::Settled(false)))
        );
    }
}
