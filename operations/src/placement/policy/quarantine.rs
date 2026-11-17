//! Lets an operator release the quarantined local copies of one version, or rescan them.
//! This node keeps draining until no quarantined copy is left.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::keyspaces::{MANAGED_COPY_KEYSPACE, NODE_SUBJECT_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, done, values};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::node_subject::{NODE_SUBJECT_KEY, NodeSubjectRecord};
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::structs::storage::blob::{ManagedCopyQuarantine, VersionKey};
use aruna_core::types::Key;
use thiserror::Error;
use tracing::info;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_core::managed_copy::{COPY_PAGE_LIMIT, ManagedCopyError, version_scope};

use super::subject::{
    SubjectScanConfig, SubjectScanError, SubjectScanMode, SubjectScanOperation, SubjectScanResult,
};

#[derive(Clone, Debug, PartialEq)]
pub struct ResolveQuarantineConfig {
    pub auth_context: AuthContext,
    pub realm_id: RealmId,
    /// The version whose local registrations the operator releases. `None`
    /// revalidates the inventory without dropping anything.
    pub release: Option<VersionKey>,
    pub now_ms: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QuarantineResolution {
    pub released: bool,
    pub scanned: usize,
    pub restored: usize,
    pub quarantined: usize,
    /// True once no quarantined copy remains, so this node admits and serves
    /// governed data again.
    pub cleared: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum QuarantineError {
    #[error(transparent)]
    Copy(#[from] ManagedCopyError),
    #[error(transparent)]
    Scan(#[from] SubjectScanError),
    #[error(transparent)]
    Conversion(#[from] aruna_core::errors::ConversionError),
    #[error("caller may not administer the realm configuration")]
    Unauthorized,
    #[error("unexpected event in state {0}")]
    InvalidEvent(&'static str),
    #[error(transparent)]
    Io(#[from] IoError),
}

#[derive(Debug, PartialEq)]
enum ResolveState {
    Authorize,
    ReadSubject,
    /// Pages through the local registrations of the released version.
    Release,
    /// Deletes one page of them.
    Delete,
    Rescan,
}

type ResolveTurn = Result<Next<QuarantineResolution>, QuarantineError>;

#[derive(Debug, PartialEq)]
pub struct ResolveQuarantineOperation {
    config: ResolveQuarantineConfig,
    state: ResolveState,
    subject: Option<NodeSubjectRecord>,
    removal: Option<PrefixPages>,
    released: bool,
}

impl ResolveQuarantineOperation {
    pub fn new(config: ResolveQuarantineConfig) -> Self {
        Self {
            config,
            state: ResolveState::Authorize,
            subject: None,
            removal: None,
            released: false,
        }
    }

    fn settle(resolution: QuarantineResolution) -> Next<QuarantineResolution> {
        info!(
            released = resolution.released,
            restored = resolution.restored,
            quarantined = resolution.quarantined,
            cleared = resolution.cleared,
            "Quarantine resolution finished"
        );
        Next::Done(resolution)
    }

    fn read_subject(&mut self) -> Next<QuarantineResolution> {
        self.state = ResolveState::ReadSubject;
        let row = (
            NODE_SUBJECT_KEYSPACE.to_string(),
            Key::from(NODE_SUBJECT_KEY.to_vec()),
        );
        Next::Call(IoCall::Read(vec![row]))
    }

    /// Drops every local registration of the released version, so the copy stops
    /// being inventory this node has to decide about. The bytes stay on the
    /// backend: releasing makes them unserveable, it never deletes them.
    fn release(&mut self, version: &VersionKey) -> ResolveTurn {
        let pages = PrefixPages::new(
            MANAGED_COPY_KEYSPACE,
            version_scope(version)?,
            COPY_PAGE_LIMIT,
        );
        let call = pages.call();
        self.removal = Some(pages);
        self.released = true;
        self.state = ResolveState::Release;
        Ok(Next::Call(call))
    }

    fn removal(&mut self) -> Result<&mut PrefixPages, QuarantineError> {
        self.removal
            .as_mut()
            .ok_or(QuarantineError::InvalidEvent("Release"))
    }

    fn rescan(&mut self) -> ResolveTurn {
        let subject = self
            .subject
            .as_ref()
            .ok_or(QuarantineError::InvalidEvent("ReadSubject"))?;
        let scan = SubjectScanOperation::new(SubjectScanConfig {
            realm_id: self.config.realm_id,
            observed: subject.subject.clone(),
            mode: SubjectScanMode::Revalidate(ManagedCopyQuarantine::PolicyViolation),
            now_ms: self.config.now_ms,
        });
        self.state = ResolveState::Rescan;
        Ok(Next::Call(IoCall::child(scan)))
    }
}

impl Operation for ResolveQuarantineOperation {
    type Output = QuarantineResolution;
    type Error = QuarantineError;

    fn first(&mut self) -> ResolveTurn {
        self.state = ResolveState::Authorize;
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: policy_admin_path(self.config.realm_id),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> ResolveTurn {
        match self.state {
            ResolveState::Authorize => match result.child::<bool, AuthorizationError>()? {
                Ok(true) => Ok(self.read_subject()),
                _ => Err(QuarantineError::Unauthorized),
            },
            ResolveState::ReadSubject => {
                // A node that never advertised a subject holds nothing governed,
                // so there is no quarantine to resolve and nothing is blocked.
                let [Some(value)] = values::<1>(result)? else {
                    return Ok(Self::settle(QuarantineResolution {
                        cleared: true,
                        ..QuarantineResolution::default()
                    }));
                };
                self.subject = Some(NodeSubjectRecord::from_bytes(value.as_ref())?);
                match self.config.release.clone() {
                    Some(version) => self.release(&version),
                    None => self.rescan(),
                }
            }
            ResolveState::Release => {
                let rows = self.removal()?.take(result)?;
                if rows.is_empty() {
                    return self.rescan();
                }
                self.state = ResolveState::Delete;
                let deletes = rows
                    .into_iter()
                    .map(|(key, _)| (MANAGED_COPY_KEYSPACE.to_string(), key))
                    .collect();
                Ok(Next::Call(IoCall::Delete(deletes)))
            }
            ResolveState::Delete => {
                done(result)?;
                // A full page resumes strictly after the last deleted key, so removal progresses.
                let removal = self.removal()?;
                if removal.done() {
                    return self.rescan();
                }
                let call = removal.call();
                self.state = ResolveState::Release;
                Ok(Next::Call(call))
            }
            ResolveState::Rescan => {
                let result = result.child::<SubjectScanResult, SubjectScanError>()??;
                Ok(Self::settle(QuarantineResolution {
                    released: self.released,
                    scanned: result.scanned,
                    restored: result.restored,
                    quarantined: result.quarantined,
                    cleared: result.quarantined == 0,
                }))
            }
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::id::NodeId;
    use aruna_core::structs::placement::policy::PlacementSubject;
    use aruna_core::structs::storage::blob::VersionKey;
    use std::collections::BTreeMap;
    use ulid::Ulid;

    const REALM: RealmId = RealmId([3u8; 32]);

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[5u8; 32]).public()
    }

    fn subject() -> PlacementSubject {
        PlacementSubject {
            node_id: node(),
            generation: 1,
            location: "eu-west".to_string(),
            labels: BTreeMap::new(),
            executor_kind: None,
            local_to_controller: true,
        }
    }

    fn blocked() -> NodeSubjectRecord {
        let mut record = NodeSubjectRecord::seed(subject()).expect("subject is valid");
        record.policy_draining = true;
        record.serving_blocked = true;
        record
    }

    fn version() -> VersionKey {
        VersionKey::new("bucket", "file.txt", Ulid::from_bytes([4u8; 16]))
    }

    fn operation(release: Option<VersionKey>) -> ResolveQuarantineOperation {
        ResolveQuarantineOperation::new(ResolveQuarantineConfig {
            auth_context: AuthContext {
                user_id: UserId::new(Ulid::from_bytes([2u8; 16]), REALM),
                realm_id: REALM,
                path_restrictions: None,
                session: None,
            },
            realm_id: REALM,
            release,
            now_ms: 1_000,
        })
    }

    fn allowed(allowed: bool) -> IoResult {
        IoResult::Child(ChildOutcome::new(Ok::<_, AuthorizationError>(allowed)))
    }

    fn stored(record: &NodeSubjectRecord) -> IoResult {
        IoResult::Values(vec![Some(
            record.to_bytes().expect("record encodes").into(),
        )])
    }

    fn page(keys: &[&[u8]]) -> IoResult {
        IoResult::Page {
            rows: keys
                .iter()
                .map(|key| (Key::from(key.to_vec()), Key::from(Vec::new())))
                .collect(),
            next: None,
        }
    }

    /// The answer of a finished revalidation walk.
    fn scanned(quarantined: usize) -> IoResult {
        let result = SubjectScanResult {
            quarantined,
            ..SubjectScanResult::default()
        };
        IoResult::Child(ChildOutcome::new(Ok::<_, SubjectScanError>(result)))
    }

    #[test]
    fn refuses_non_admin() {
        // Resolving a quarantine is a realm-admin decision, checked in the
        // operation and not only at the transport.
        let mut operation = operation(None);
        operation.first().unwrap();

        assert_eq!(
            operation.next(allowed(false)).err(),
            Some(QuarantineError::Unauthorized)
        );
    }

    #[test]
    fn clears_when_empty() {
        // The revalidation walk ends the block exactly when no quarantined copy
        // is left, which is what reopens governed admission.
        let mut operation = operation(None);
        operation.first().unwrap();
        operation.next(allowed(true)).unwrap();
        let scan = operation.next(stored(&blocked()));
        assert!(matches!(scan, Ok(Next::Call(IoCall::Child(_)))));

        let Ok(Next::Done(resolution)) = operation.next(scanned(0)) else {
            panic!("expected the resolution");
        };
        assert!(resolution.cleared);
        assert!(!resolution.released);
        assert_eq!(resolution.quarantined, 0);
    }

    #[test]
    fn release_drops_registrations() {
        // Releasing removes the local rows of one exact version before the walk,
        // so the copy becomes locally unavailable rather than serveable.
        let mut operation = operation(Some(version()));
        operation.first().unwrap();
        operation.next(allowed(true)).unwrap();
        let scan = operation.next(stored(&blocked()));
        assert!(matches!(
            scan,
            Ok(Next::Call(IoCall::Page { space, .. }))
                if space == aruna_core::keyspaces::MANAGED_COPY_KEYSPACE
        ));
        let deletes = operation.next(page(&[b"one", b"two"]));
        assert!(matches!(deletes, Ok(Next::Call(IoCall::Delete(rows))) if rows.len() == 2));
        let walk = operation.next(IoResult::Done);
        assert!(matches!(walk, Ok(Next::Call(IoCall::Child(_)))));

        let Ok(Next::Done(resolution)) = operation.next(scanned(0)) else {
            panic!("expected the resolution");
        };
        assert!(resolution.released);
        assert!(resolution.cleared);
    }

    #[test]
    fn subjectless_node_clears() {
        // A node that never advertised a subject holds nothing governed, so
        // there is nothing to resolve and nothing stays blocked.
        let mut operation = operation(None);
        operation.first().unwrap();
        operation.next(allowed(true)).unwrap();

        let Ok(Next::Done(resolution)) = operation.next(IoResult::Values(vec![None])) else {
            panic!("expected the resolution");
        };
        assert!(resolution.cleared);
        assert_eq!(resolution.scanned, 0);
    }
}
