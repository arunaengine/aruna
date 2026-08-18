//! Operator resolution of quarantined local copies.
//!
//! A subject scan that leaves any copy quarantined keeps this node draining:
//! it serves nothing governed and admits no new governed work until an operator
//! decides what happens to those copies. This is that decision. Releasing drops
//! the local registrations of one exact version, which makes it locally
//! unavailable rather than serveable, and the revalidation walk afterwards is
//! the same one the transition uses, so the block ends exactly when nothing
//! quarantined is left.

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::NODE_SUBJECT_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{
    AuthContext, ManagedCopyQuarantine, NODE_SUBJECT_KEY, NodeSubjectRecord, Permission, RealmId,
    VersionKey, policy_admin_path,
};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;
use tracing::info;

use crate::blob::managed_copy::{ManagedCopyError, ManagedCopyRemoval};
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};

use super::subject::{SubjectScanConfig, SubjectScanError, SubjectScanMode, SubjectScanOperation};

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
}

#[derive(Debug, PartialEq)]
enum ResolveState {
    Init,
    Authorize,
    ReadSubject,
    Release,
    Rescan,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct ResolveQuarantineOperation {
    config: ResolveQuarantineConfig,
    state: ResolveState,
    subject: Option<NodeSubjectRecord>,
    removal: Option<ManagedCopyRemoval>,
    scan: Option<SubjectScanOperation>,
    released: bool,
    output: Option<Result<QuarantineResolution, QuarantineError>>,
}

impl ResolveQuarantineOperation {
    pub fn new(config: ResolveQuarantineConfig) -> Self {
        Self {
            config,
            state: ResolveState::Init,
            subject: None,
            removal: None,
            scan: None,
            released: false,
            output: None,
        }
    }

    fn fail(&mut self, error: QuarantineError) -> Effects {
        self.state = ResolveState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn settle(&mut self, resolution: QuarantineResolution) -> Effects {
        info!(
            released = resolution.released,
            restored = resolution.restored,
            quarantined = resolution.quarantined,
            cleared = resolution.cleared,
            "Quarantine resolution finished"
        );
        self.state = ResolveState::Finish;
        self.output = Some(Ok(resolution));
        smallvec![]
    }

    fn read_subject(&mut self) -> Effects {
        self.state = ResolveState::ReadSubject;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: NODE_SUBJECT_KEYSPACE.to_string(),
            key: Key::from(NODE_SUBJECT_KEY.to_vec()),
            txn_id: None,
        })]
    }

    /// Drops every local registration of the released version, so the copy stops
    /// being inventory this node has to decide about.
    fn release(&mut self, version: &VersionKey) -> Effects {
        let mut removal = match ManagedCopyRemoval::for_version(version) {
            Ok(removal) => removal,
            Err(error) => return self.fail(error.into()),
        };
        let effects = removal.start(None);
        self.removal = Some(removal);
        self.released = true;
        self.state = ResolveState::Release;
        effects
    }

    fn rescan(&mut self) -> Effects {
        let Some(subject) = self.subject.as_ref() else {
            return self.fail(QuarantineError::InvalidEvent("ReadSubject"));
        };
        let mut scan = SubjectScanOperation::new(SubjectScanConfig {
            realm_id: self.config.realm_id,
            observed: subject.subject.clone(),
            mode: SubjectScanMode::Revalidate(ManagedCopyQuarantine::PolicyViolation),
            now_ms: self.config.now_ms,
        });
        let effects = scan.start();
        let complete = scan.is_complete();
        self.scan = Some(scan);
        self.state = ResolveState::Rescan;
        match complete {
            true => self.finish_scan(),
            false => effects,
        }
    }

    fn finish_scan(&mut self) -> Effects {
        let Some(scan) = self.scan.take() else {
            return self.fail(QuarantineError::InvalidEvent("Rescan"));
        };
        match scan.finalize() {
            Ok(result) => self.settle(QuarantineResolution {
                released: self.released,
                scanned: result.scanned,
                restored: result.restored,
                quarantined: result.quarantined,
                cleared: result.quarantined == 0,
            }),
            Err(error) => self.fail(error.into()),
        }
    }
}

impl Operation for ResolveQuarantineOperation {
    type Output = QuarantineResolution;
    type Error = QuarantineError;

    fn start(&mut self) -> Effects {
        self.state = ResolveState::Authorize;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: policy_admin_path(self.config.realm_id),
                required_permission: Permission::WRITE,
            }),
            |result| Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: result
            }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ResolveState::Authorize => match event {
                Event::SubOperation(SubOperationEvent::AuthorizationResult {
                    allowed: Ok(true),
                }) => self.read_subject(),
                Event::SubOperation(SubOperationEvent::AuthorizationResult { .. }) => {
                    self.fail(QuarantineError::Unauthorized)
                }
                _ => self.fail(QuarantineError::InvalidEvent("Authorize")),
            },
            ResolveState::ReadSubject => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(QuarantineError::InvalidEvent("ReadSubject"));
                };
                // A node that never advertised a subject holds nothing governed,
                // so there is no quarantine to resolve and nothing is blocked.
                let Some(value) = value else {
                    return self.settle(QuarantineResolution {
                        cleared: true,
                        ..QuarantineResolution::default()
                    });
                };
                match NodeSubjectRecord::from_bytes(value.as_ref()) {
                    Ok(record) => {
                        self.subject = Some(record);
                        match self.config.release.clone() {
                            Some(version) => self.release(&version),
                            None => self.rescan(),
                        }
                    }
                    Err(error) => self.fail(error.into()),
                }
            }
            ResolveState::Release => {
                let Some(removal) = self.removal.as_mut() else {
                    return self.fail(QuarantineError::InvalidEvent("Release"));
                };
                match removal.step(event, None) {
                    Ok(Some(effects)) => effects,
                    Ok(None) => {
                        self.removal = None;
                        self.rescan()
                    }
                    Err(error) => self.fail(error.into()),
                }
            }
            ResolveState::Rescan => {
                let Some(scan) = self.scan.as_mut() else {
                    return self.fail(QuarantineError::InvalidEvent("Rescan"));
                };
                let effects = scan.step(event);
                match scan.is_complete() {
                    true => self.finish_scan(),
                    false => effects,
                }
            }
            ResolveState::Init | ResolveState::Finish | ResolveState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ResolveState::Finish | ResolveState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(QuarantineError::InvalidEvent("Finish")))
    }

    fn abort(&mut self) -> Effects {
        match self.scan.as_mut() {
            Some(scan) => scan.abort(),
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{PlacementSubject, VersionKey};
    use aruna_core::types::NodeId;
    use aruna_core::types::UserId;
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
            },
            realm_id: REALM,
            release,
            now_ms: 1_000,
        })
    }

    fn allowed(allowed: bool) -> Event {
        Event::SubOperation(SubOperationEvent::AuthorizationResult {
            allowed: Ok(allowed),
        })
    }

    fn stored(record: &NodeSubjectRecord) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(record.to_bytes().expect("record encodes").into()),
        })
    }

    fn empty_page() -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        })
    }

    #[test]
    fn refuses_non_admin() {
        // Resolving a quarantine is a realm-admin decision, checked in the
        // operation and not only at the transport.
        let mut operation = operation(None);
        operation.start();

        operation.step(allowed(false));

        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Err(QuarantineError::Unauthorized));
    }

    #[test]
    fn clears_when_nothing_remains() {
        // The revalidation walk ends the block exactly when no quarantined copy
        // is left, which is what reopens governed admission.
        let mut operation = operation(None);
        operation.start();
        operation.step(allowed(true));
        operation.step(stored(&blocked()));
        // The embedded scan reads the record itself, then walks the inventory.
        operation.step(stored(&blocked()));
        operation.step(empty_page());
        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        }));

        assert!(operation.is_complete());
        let resolution = operation.finalize().expect("resolution completes");
        assert!(resolution.cleared);
        assert!(!resolution.released);
        assert_eq!(resolution.quarantined, 0);
    }

    #[test]
    fn release_drops_registrations() {
        // Releasing removes the local rows of one exact version before the walk,
        // so the copy becomes locally unavailable rather than serveable.
        let mut operation = operation(Some(version()));
        operation.start();
        operation.step(allowed(true));
        let effects = operation.step(stored(&blocked()));

        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::Iter { key_space, .. }))
                if key_space == aruna_core::keyspaces::MANAGED_COPY_KEYSPACE
        ));
        operation.step(empty_page());
        operation.step(stored(&blocked()));
        operation.step(empty_page());
        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        }));

        let resolution = operation.finalize().expect("resolution completes");
        assert!(resolution.released);
        assert!(resolution.cleared);
    }

    #[test]
    fn subjectless_node_clears() {
        // A node that never advertised a subject holds nothing governed, so
        // there is nothing to resolve and nothing stays blocked.
        let mut operation = operation(None);
        operation.start();
        operation.step(allowed(true));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        }));

        let resolution = operation.finalize().expect("resolution completes");
        assert!(resolution.cleared);
        assert_eq!(resolution.scanned, 0);
    }
}
