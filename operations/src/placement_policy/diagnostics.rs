//! Responder-local policy diagnostics: what this node enforces, which of its
//! own registered copies are not serveable, and how much of the durable policy
//! cache it holds.
//!
//! Every number here is an observation of this node's own rows. It says nothing
//! about another partition, and cache coverage is never policy truth: an evicted
//! entry only costs a refetch.

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    MANAGED_COPY_KEYSPACE, NODE_SUBJECT_KEYSPACE, PLACEMENT_POLICY_CACHE_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{
    AuthContext, ManagedCopyRecord, ManagedCopyState, NODE_SUBJECT_KEY, NodeSubjectRecord,
    Permission, PlacementPolicyRef, PlacementSubject, VersionKey, policy_admin_path,
};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};

use super::cache::{MAX_CACHE_ENTRIES, PolicyCacheEntry};

/// Upper bound on the copies one diagnostics page inspects.
pub const DIAGNOSTICS_PAGE_LIMIT: usize = 256;

#[derive(Clone, Debug, PartialEq)]
pub struct DiagnosticsInput {
    pub auth_context: AuthContext,
    pub start_after: Option<Key>,
    pub limit: usize,
}

/// One local registration that cannot serve until it is re-evaluated.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopyViolation {
    pub version: VersionKey,
    pub state: ManagedCopyState,
    pub policies: Vec<PlacementPolicyRef>,
}

/// How much of the durable cache this node currently holds. Diagnostics only:
/// eviction changes no decision.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheCoverage {
    pub entries: usize,
    pub verified: usize,
    pub unavailable: usize,
    pub bytes: usize,
    /// True when the scan hit its bound before the cache ended.
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DiagnosticsReport {
    /// Absent means this node advertises no subject, so it serves nothing
    /// governed.
    pub subject: Option<PlacementSubject>,
    pub policy_draining: bool,
    pub serving_blocked: bool,
    pub observed: usize,
    pub registered: usize,
    pub quarantined: usize,
    pub unresolved_departed: usize,
    pub violations: Vec<CopyViolation>,
    pub cache: CacheCoverage,
    pub cursor: Option<Key>,
    /// True only when the bounded local iterator was exhausted in this pass.
    pub complete: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum DiagnosticsError {
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("caller may not administer the realm configuration")]
    Unauthorized,
    #[error("unexpected event during the diagnostics scan")]
    InvalidEvent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiagnosticsState {
    Init,
    Authorize,
    ReadSubject,
    ScanCopies,
    ScanCache,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct PolicyDiagnosticsOperation {
    input: DiagnosticsInput,
    state: DiagnosticsState,
    report: DiagnosticsReport,
    output: Option<Result<DiagnosticsReport, DiagnosticsError>>,
}

impl PolicyDiagnosticsOperation {
    pub fn new(input: DiagnosticsInput) -> Self {
        Self {
            input,
            state: DiagnosticsState::Init,
            report: DiagnosticsReport {
                subject: None,
                policy_draining: false,
                serving_blocked: false,
                observed: 0,
                registered: 0,
                quarantined: 0,
                unresolved_departed: 0,
                violations: Vec::new(),
                cache: CacheCoverage::default(),
                cursor: None,
                complete: false,
            },
            output: None,
        }
    }

    fn fail(&mut self, error: DiagnosticsError) -> Effects {
        self.state = DiagnosticsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_subject(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(DiagnosticsError::InvalidEvent);
        };
        if let Some(value) = value {
            let record = match NodeSubjectRecord::from_bytes(value.as_ref()) {
                Ok(record) => record,
                Err(error) => return self.fail(error.into()),
            };
            self.report.policy_draining = record.policy_draining;
            self.report.serving_blocked = record.serving_blocked;
            self.report.subject = Some(record.subject);
        }
        self.state = DiagnosticsState::ScanCopies;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: MANAGED_COPY_KEYSPACE.to_string(),
            prefix: None,
            start: self.input.start_after.clone().map(IterStart::After),
            limit: self.input.limit.clamp(1, DIAGNOSTICS_PAGE_LIMIT),
            txn_id: None,
        })]
    }

    fn handle_copies(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.fail(DiagnosticsError::InvalidEvent);
        };
        self.report.cursor = next_start_after;
        self.report.complete = self.report.cursor.is_none();
        for (_, value) in values {
            let record = match ManagedCopyRecord::from_bytes(value.as_ref()) {
                Ok(record) => record,
                Err(error) => return self.fail(error.into()),
            };
            self.report.observed += 1;
            match record.state {
                ManagedCopyState::Registered => {
                    self.report.registered += 1;
                    continue;
                }
                ManagedCopyState::Quarantined(_) => self.report.quarantined += 1,
                ManagedCopyState::UnresolvedDeparted => self.report.unresolved_departed += 1,
            }
            self.report.violations.push(CopyViolation {
                version: record.version,
                state: record.state,
                policies: record.policies,
            });
        }
        self.state = DiagnosticsState::ScanCache;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: PLACEMENT_POLICY_CACHE_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: MAX_CACHE_ENTRIES + 1,
            txn_id: None,
        })]
    }

    fn handle_cache(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.fail(DiagnosticsError::InvalidEvent);
        };
        let mut coverage = CacheCoverage {
            truncated: next_start_after.is_some(),
            ..CacheCoverage::default()
        };
        for (_, value) in values {
            coverage.entries += 1;
            coverage.bytes += value.len();
            match PolicyCacheEntry::from_bytes(value.as_ref()) {
                Ok(PolicyCacheEntry::Verified { .. }) => coverage.verified += 1,
                Ok(PolicyCacheEntry::Unavailable { .. }) => coverage.unavailable += 1,
                // An unreadable row answers nothing and is replaced by the next
                // resolve; it is counted but never trusted.
                Err(_) => {}
            }
        }
        self.report.cache = coverage;
        self.output = Some(Ok(self.report.clone()));
        self.state = DiagnosticsState::Finish;
        smallvec![]
    }
}

impl Operation for PolicyDiagnosticsOperation {
    type Output = DiagnosticsReport;
    type Error = DiagnosticsError;

    fn start(&mut self) -> Effects {
        self.state = DiagnosticsState::Authorize;
        let auth_config = CheckPermissionsConfig {
            auth_context: self.input.auth_context.clone(),
            path: policy_admin_path(self.input.auth_context.realm_id),
            required_permission: Permission::READ,
        };
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(auth_config),
            |result| Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: result
            }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state {
            DiagnosticsState::Init => self.start(),
            DiagnosticsState::Authorize => {
                let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event
                else {
                    return self.fail(DiagnosticsError::InvalidEvent);
                };
                match allowed {
                    Ok(true) => {
                        self.state = DiagnosticsState::ReadSubject;
                        smallvec![Effect::Storage(StorageEffect::Read {
                            key_space: NODE_SUBJECT_KEYSPACE.to_string(),
                            key: Key::from(NODE_SUBJECT_KEY.to_vec()),
                            txn_id: None,
                        })]
                    }
                    Ok(false) => self.fail(DiagnosticsError::Unauthorized),
                    Err(error) => {
                        warn!(error = %error, "Policy diagnostics authorization check failed");
                        self.fail(DiagnosticsError::Unauthorized)
                    }
                }
            }
            DiagnosticsState::ReadSubject => self.handle_subject(event),
            DiagnosticsState::ScanCopies => self.handle_copies(event),
            DiagnosticsState::ScanCache => self.handle_cache(event),
            DiagnosticsState::Finish | DiagnosticsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DiagnosticsState::Finish | DiagnosticsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(DiagnosticsError::InvalidEvent))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(error, DiagnosticsError::Unauthorized)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DiagnosticsError, DiagnosticsInput, DiagnosticsReport, PolicyDiagnosticsOperation,
    };
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        AuthContext, BackendLocation, BackendRef, ManagedCopyQuarantine, ManagedCopyRecord,
        ManagedCopyState, NodeSubjectRecord, RealmId, VersionKey,
    };
    use aruna_core::types::{Key, NodeId, UserId};
    use std::collections::HashMap;
    use std::time::UNIX_EPOCH;
    use ulid::Ulid;

    use crate::placement_policy::cache::PolicyCacheEntry;
    use crate::placement_policy::fixtures::{signed_document, subject};

    fn realm_id() -> RealmId {
        RealmId::from_bytes([1u8; 32])
    }

    fn node_id() -> NodeId {
        iroh::SecretKey::from_bytes(&[3u8; 32]).public()
    }

    fn policy() -> aruna_core::structs::VerifiedPolicy {
        let policy = aruna_core::structs::PlacementPolicy::new(
            Ulid::from_bytes([5u8; 16]),
            "residency".to_string(),
            vec![aruna_core::structs::PlacementSelector {
                node_id: Some(node_id()),
                location: None,
                labels: Vec::new(),
                executor_kind: None,
            }],
        )
        .expect("policy is valid");
        aruna_core::structs::VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    fn copy(state: ManagedCopyState) -> ManagedCopyRecord {
        ManagedCopyRecord::new(
            VersionKey::new("bucket", "object", Ulid::from_bytes([9u8; 16])),
            node_id(),
            BackendLocation {
                backend: BackendRef::node_default(),
                storage_class: None,
                root: "/data".to_string(),
                storage_bucket: "aruna".to_string(),
                backend_path: "objects/one".to_string(),
                ulid: Ulid::from_bytes([5u8; 16]),
                compressed: false,
                encrypted: false,
                created_by: Default::default(),
                created_at: UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 3,
                hashes: HashMap::new(),
            },
            Vec::new(),
            7,
            state,
        )
        .expect("record builds")
    }

    fn operation() -> PolicyDiagnosticsOperation {
        PolicyDiagnosticsOperation::new(DiagnosticsInput {
            auth_context: AuthContext {
                user_id: UserId::nil(realm_id()),
                realm_id: realm_id(),
                path_restrictions: None,
                session: None,
            },
            start_after: None,
            limit: 16,
        })
    }

    fn authorized(allowed: bool) -> Event {
        Event::SubOperation(SubOperationEvent::AuthorizationResult {
            allowed: Ok(allowed),
        })
    }

    fn iter(values: Vec<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: values
                .into_iter()
                .map(|value| (Key::from(Vec::new()), Key::from(value)))
                .collect(),
            next_start_after: None,
        })
    }

    fn report(copies: Vec<ManagedCopyState>) -> DiagnosticsReport {
        let mut operation = operation();
        operation.start();
        operation.step(authorized(true));
        let record = NodeSubjectRecord::seed(subject(node_id(), "eu-west")).expect("subject valid");
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Key::from(Vec::new()),
            value: Some(record.to_bytes().expect("record encodes").into()),
        }));
        operation.step(iter(
            copies
                .into_iter()
                .map(|state| copy(state).to_bytes().expect("record encodes"))
                .collect(),
        ));
        let entry = PolicyCacheEntry::verified(&signed_document(realm_id(), &policy(), 9), 10);
        operation.step(iter(vec![
            entry.to_bytes().expect("entry encodes"),
            PolicyCacheEntry::unavailable(10)
                .to_bytes()
                .expect("entry encodes"),
        ]));
        operation.finalize().expect("report returned")
    }

    #[test]
    fn reports_violations() {
        // A serveable copy is not a violation; every other state is listed.
        let report = report(vec![
            ManagedCopyState::Registered,
            ManagedCopyState::Quarantined(ManagedCopyQuarantine::PolicyViolation),
            ManagedCopyState::UnresolvedDeparted,
        ]);

        assert_eq!(report.observed, 3);
        assert_eq!(report.registered, 1);
        assert_eq!(report.quarantined, 1);
        assert_eq!(report.unresolved_departed, 1);
        assert_eq!(report.violations.len(), 2);
        assert!(report.complete);
        assert_eq!(report.subject.map(|subject| subject.generation), Some(1));
    }

    #[test]
    fn counts_cache_entries() {
        // Cache coverage is a diagnostic, so a negative hint is counted apart
        // from a verified document.
        let report = report(Vec::new());

        assert_eq!(report.cache.entries, 2);
        assert_eq!(report.cache.verified, 1);
        assert_eq!(report.cache.unavailable, 1);
        assert!(report.cache.bytes > 0);
        assert!(!report.cache.truncated);
    }

    #[test]
    fn denies_non_admin() {
        let mut operation = operation();
        operation.start();
        let effects = operation.step(authorized(false));

        assert!(effects.is_empty(), "nothing is read for a denied caller");
        assert_eq!(operation.finalize(), Err(DiagnosticsError::Unauthorized));
    }
}
