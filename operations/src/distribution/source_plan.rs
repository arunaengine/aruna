//! Decides what a keeping member keeps of one reference version a read served from its source:
//! the version, the bucket binding, the encryption settings and the set record decide it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, DISTRIBUTION_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::{BlobVersion, BlobVersionState, BucketInfo, VersionKey};
use aruna_core::structs::storage::distribution::on_use::{KeptKind, PullPlan};
use aruna_core::structs::storage::distribution::{DistributionBinding, DistributionView};
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::types::{Effects, GroupId, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use super::held::{HeldElsewhere, admitted};

/// One reference a read served from its source.
#[derive(Clone, Debug, PartialEq)]
pub struct SourceKeep {
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    /// The bucket's group, whose connector of the source root reads the bytes.
    pub group_id: GroupId,
    pub user_id: UserId,
    /// The read asked for a byte range, so the member's range behaviour applies.
    pub ranged: bool,
}

/// What a keeping member keeps of one reference version.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Plan {
    pub(super) kind: KeptKind,
    pub(super) pull: PullPlan,
    pub(super) set_id: Ulid,
    pub(super) member_id: Ulid,
    pub(super) identity: [u8; 32],
    pub(super) version: BlobVersion,
}

#[derive(Debug, Error, PartialEq)]
#[error("the keep plan could not be read: {0}")]
pub(super) struct PlanError(String);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlanState {
    Init,
    Version,
    Bucket,
    Encryption,
    Record,
    Finish,
    Error,
}

/// Reads the version and this member's strategy; outputs `None` when nothing is to be kept.
#[derive(Debug, PartialEq)]
pub(super) struct KeepPlanOperation {
    keep: SourceKeep,
    state: PlanState,
    version: Option<(BlobVersion, [u8; 32])>,
    binding: Option<DistributionBinding>,
    output: Option<Result<Option<Plan>, PlanError>>,
}

impl KeepPlanOperation {
    pub(super) fn new(keep: SourceKeep) -> Self {
        Self {
            keep,
            state: PlanState::Init,
            version: None,
            binding: None,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<Option<Plan>, PlanError>) -> Effects {
        self.state = match output {
            Ok(_) => PlanState::Finish,
            Err(_) => PlanState::Error,
        };
        self.output = Some(output);
        smallvec![]
    }

    fn read(&mut self, state: PlanState, key_space: &str, key: Vec<u8>) -> Effects {
        self.state = state;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    /// The next read, `Ok(None)` when nothing is kept, or the plan.
    fn decide(&mut self, value: Option<Value>) -> Result<Result<Effects, Option<Plan>>, String> {
        let fault = |error: &dyn std::fmt::Display| error.to_string();
        match self.state {
            PlanState::Version => {
                let Some(value) = value else {
                    return Ok(Err(None));
                };
                let version = BlobVersion::from_bytes(&value).map_err(|error| fault(&error))?;
                let BlobVersionState::Reference { source, .. } = &version.state else {
                    return Ok(Err(None));
                };
                // Governed versions are served only from registered copies, never kept ones.
                if !version.placement_policies.is_empty() {
                    return Ok(Err(None));
                }
                let identity = version
                    .identity
                    .filter(|_| HeldElsewhere::of(source, version.identity).is_none());
                let Some(identity) = identity else {
                    return Ok(Err(None));
                };
                self.version = Some((version, identity));
                let bucket = self.keep.bucket.as_bytes().to_vec();
                Ok(Ok(self.read(PlanState::Bucket, S3_BUCKET_KEYSPACE, bucket)))
            }
            PlanState::Bucket => {
                let binding = (value.as_deref())
                    .and_then(|bucket| BucketInfo::from_bytes(bucket).ok())
                    .and_then(|bucket| bucket.distribution);
                let Some(binding) = binding else {
                    return Ok(Err(None));
                };
                self.binding = Some(binding);
                let bucket = self.keep.bucket.as_bytes().to_vec();
                Ok(Ok(self.read(
                    PlanState::Encryption,
                    BUCKET_ENCRYPTION_KEYSPACE,
                    bucket,
                )))
            }
            PlanState::Encryption => {
                let encryption =
                    BucketEncryption::from_row(value.as_deref()).map_err(|error| fault(&error))?;
                let set_id = self.binding.as_ref().ok_or("no binding was read")?.set_id;
                if encryption.active_key().is_some() {
                    return Ok(Err(None));
                }
                let record = set_id.to_bytes().to_vec();
                Ok(Ok(self.read(
                    PlanState::Record,
                    DISTRIBUTION_KEYSPACE,
                    record,
                )))
            }
            PlanState::Record => {
                let Some(value) = value else {
                    return Ok(Err(None));
                };
                let view = DistributionView::from_bytes(&value).map_err(|error| fault(&error))?;
                let binding = self.binding.take().ok_or("no binding was read")?;
                let (version, identity) = self.version.take().ok_or("no version was read")?;
                Ok(Err(plan(&self.keep, &view, &binding, version, identity)))
            }
            _ => Err("no read was asked".to_string()),
        }
    }
}

/// The plan of a member that keeps what a read through it served, if its strategy keeps it.
fn plan(
    keep: &SourceKeep,
    view: &DistributionView,
    binding: &DistributionBinding,
    version: BlobVersion,
    identity: [u8; 32],
) -> Option<Plan> {
    let snapshot = view.snapshot();
    let member = snapshot.member(binding.member_id)?;
    let behaviour = snapshot.config.range_behaviour(member);
    // A passthrough range read keeps nothing, like a read through a holder.
    let (kind, pull) = match PullPlan::choose(member.strategy, behaviour, keep.ranged, true) {
        pull @ (PullPlan::FillFirst(kind) | PullPlan::ServeAndFill(kind)) => (kind, pull),
        PullPlan::Stream | PullPlan::SpoolFirst => return None,
    };
    let BlobVersionState::Reference {
        cached_metadata, ..
    } = &version.state
    else {
        return None;
    };
    if !admitted(kind, cached_metadata.content_length) {
        return None;
    }
    Some(Plan {
        kind,
        pull,
        set_id: binding.set_id,
        member_id: binding.member_id,
        identity,
        version,
    })
}

impl Operation for KeepPlanOperation {
    type Output = Option<Plan>;
    type Error = PlanError;

    fn start(&mut self) -> Effects {
        let keep = &self.keep;
        match VersionKey::new(&keep.bucket, &keep.key, keep.version_id).to_bytes() {
            Ok(key) => self.read(PlanState::Version, BLOB_VERSIONS_KEYSPACE, key),
            Err(error) => self.finish(Err(PlanError(error.to_string()))),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        let value = match event {
            Event::Storage(StorageEvent::ReadResult { value, .. })
                if !matches!(
                    self.state,
                    PlanState::Init | PlanState::Finish | PlanState::Error
                ) =>
            {
                value
            }
            Event::Storage(StorageEvent::Error { error }) => {
                return self.finish(Err(PlanError(error.to_string())));
            }
            event => {
                let reason = format!("unexpected {} while planning a keep", event.label());
                return self.finish(Err(PlanError(reason)));
            }
        };
        match self.decide(value) {
            Ok(Ok(effects)) => effects,
            Ok(Err(plan)) => self.finish(Ok(plan)),
            Err(reason) => self.finish(Err(PlanError(reason))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PlanState::Finish | PlanState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(PlanError("the keep plan did not finish".to_string())))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::identity::realm::RealmId;

    fn keep() -> SourceKeep {
        let realm_id = RealmId::from_bytes([1; 32]);
        SourceKeep {
            bucket: "spire".to_string(),
            key: "a".to_string(),
            version_id: Ulid::from_bytes([2; 16]),
            group_id: Ulid::from_bytes([3; 16]),
            user_id: UserId::local(Ulid::from_bytes([4; 16]), realm_id),
            ranged: false,
        }
    }

    fn version_read() -> Effect {
        let key = VersionKey::new("spire", "a", Ulid::from_bytes([2; 16]));
        Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.to_bytes().unwrap().into(),
            txn_id: None,
        })
    }

    fn missing() -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        })
    }

    // A version this node lacks keeps nothing.
    #[test]
    fn missing_version_ignored() {
        let mut operation = KeepPlanOperation::new(keep());
        check_trace(
            &mut operation,
            &[version_read()],
            vec![(missing(), Vec::new())],
        );
        assert_eq!(operation.finalize(), Ok(None));
    }

    // A failed read or an undecodable version fails the plan.
    #[test]
    fn failures_fail() {
        let mut operation = KeepPlanOperation::new(keep());
        operation.start();
        operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert!(operation.finalize().is_err());
        let mut operation = KeepPlanOperation::new(keep());
        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(vec![0xFF].into()),
        }));
        assert!(operation.finalize().is_err());
    }

    // Unstarted plans refuse every event, abort nothing and never finish early.
    #[test]
    fn rejects_unexpected() {
        let fresh = || KeepPlanOperation::new(keep());
        let all = sample_events().len();
        assert_eq!(rejects_events(fresh, sample_events, |_| false).len(), all);
        rejects_finalize(fresh());
        assert!(fresh().abort().is_empty());
    }
}
