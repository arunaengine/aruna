//! Keeps the local inventory of the version copies this node may serve.
//! Every registration and removal joins the transaction that makes the copy visible.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::effects::{Effect, IterStart, StorageEffect};
use crate::errors::ConversionError;
use crate::events::{Event, StorageEvent};
use crate::id::NodeId;
use crate::keyspaces::{MANAGED_COPY_KEYSPACE, NODE_SUBJECT_KEYSPACE};
use crate::structs::placement::node_subject::{NODE_SUBJECT_KEY, NodeSubjectRecord};
use crate::structs::placement::policy::{PlacementPolicyError, PlacementPolicyRef};
use crate::structs::storage::blob::{
    BackendLocation, CopyOrigin, ManagedCopyKey, ManagedCopyRecord, ManagedCopyState, VersionKey,
};
use crate::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;

/// Bounded page size for every local managed-copy scan.
pub const COPY_PAGE_LIMIT: usize = 256;

#[derive(Debug, Error, PartialEq)]
pub enum ManagedCopyError {
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    #[error("a staging or partial copy must not be registered")]
    UnstableCopy,
    #[error("no local registration for this version copy")]
    Unregistered,
    #[error("the local copy is not serveable")]
    NotServeable(ManagedCopyState),
    #[error("the registration does not describe the requested copy")]
    Mismatched,
    #[error("this node advertises no placement subject")]
    NoSubject,
    #[error("this node has not revalidated its copies under the current subject")]
    ServingBlocked,
    #[error("unexpected event during managed-copy removal")]
    InvalidEvent,
}

/// One local registration. `subject_generation` is the one the gate admitted
/// under, so the row names the subject that allowed the copy, and `origin`
/// records why the copy is here.
pub struct CopyRegistration<'a> {
    pub version: VersionKey,
    pub node_id: NodeId,
    pub location: &'a BackendLocation,
    pub policies: &'a [PlacementPolicyRef],
    pub origin: CopyOrigin,
    pub subject_generation: u64,
    pub registered_at_ms: u64,
}

/// Registers one local copy. A staging or partial location is refused, so a
/// half-written blob stays unregistered and therefore unservable.
pub fn register_effect(
    registration: CopyRegistration<'_>,
    txn_id: Option<TxnId>,
) -> Result<Effect, ManagedCopyError> {
    let (key_space, key, value) = register_entry(registration)?;
    Ok(Effect::Storage(StorageEffect::Write {
        key_space,
        key,
        value,
        txn_id,
    }))
}

/// The same registration as a batch entry, for a transaction that commits the
/// copy together with the version it belongs to.
pub fn register_entry(
    registration: CopyRegistration<'_>,
) -> Result<(String, Key, Value), ManagedCopyError> {
    if registration.location.staging || registration.location.partial {
        return Err(ManagedCopyError::UnstableCopy);
    }
    let record = ManagedCopyRecord::new(
        registration.version,
        registration.node_id,
        registration.location.clone(),
        registration.policies.to_vec(),
        registration.registered_at_ms,
        ManagedCopyState::Registered,
    )?
    .stored_under(registration.subject_generation)
    .from_origin(registration.origin);
    Ok((
        MANAGED_COPY_KEYSPACE.to_string(),
        record.key().to_bytes()?.into(),
        record.to_bytes()?.into(),
    ))
}

pub fn read_effect(
    key: &ManagedCopyKey,
    txn_id: Option<TxnId>,
) -> Result<Effect, ManagedCopyError> {
    Ok(Effect::Storage(StorageEffect::Read {
        key_space: MANAGED_COPY_KEYSPACE.to_string(),
        key: key.to_bytes()?.into(),
        txn_id,
    }))
}

/// Rewrites an existing registration under a new state, for rejoin,
/// subject-transition and verification quarantine.
pub fn transition_effect(
    record: &ManagedCopyRecord,
    state: ManagedCopyState,
    txn_id: Option<TxnId>,
) -> Result<Effect, ManagedCopyError> {
    let mut record = record.clone();
    record.state = state;
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: MANAGED_COPY_KEYSPACE.to_string(),
        key: record.key().to_bytes()?.into(),
        value: record.to_bytes()?.into(),
        txn_id,
    }))
}

/// A governed version is unavailable without a serveable local registration.
/// An ungoverned version has no refs and never reaches this gate.
pub fn check_serveable(value: Option<&[u8]>) -> Result<ManagedCopyRecord, ManagedCopyError> {
    let Some(value) = value else {
        return Err(ManagedCopyError::Unregistered);
    };
    let record = ManagedCopyRecord::from_bytes(value)?;
    if record.state.is_serveable() {
        Ok(record)
    } else {
        Err(ManagedCopyError::NotServeable(record.state))
    }
}

/// The copy a caller asked about. Fields the caller cannot know are `None`.
pub struct CopyRequest<'a> {
    pub key: &'a ManagedCopyKey,
    pub node_id: Option<NodeId>,
    pub blake3: Option<[u8; 32]>,
    pub refs: &'a [PlacementPolicyRef],
    /// Subject generation this node advertises now. A governed row stored under
    /// an older one describes a subject that no longer exists here.
    pub subject_generation: Option<u64>,
}

/// A registration is evidence only for the exact copy it was read for. A row
/// naming another version, backend, node, digest or ref set says nothing about
/// the requested one and must never make it serveable.
pub fn validate_registration(
    value: Option<&[u8]>,
    request: &CopyRequest<'_>,
) -> Result<ManagedCopyRecord, ManagedCopyError> {
    let record = check_serveable(value)?;
    matched(record, request)
}

/// The registration of exactly this copy whatever its state, for a caller that
/// reports a copy the node holds but does not serve instead of hiding it.
/// Serving never uses this: it must fail closed on a non-serveable row.
pub fn registration_for(
    value: Option<&[u8]>,
    request: &CopyRequest<'_>,
) -> Result<ManagedCopyRecord, ManagedCopyError> {
    let Some(value) = value else {
        return Err(ManagedCopyError::Unregistered);
    };
    matched(ManagedCopyRecord::from_bytes(value)?, request)
}

fn matched(
    record: ManagedCopyRecord,
    request: &CopyRequest<'_>,
) -> Result<ManagedCopyRecord, ManagedCopyError> {
    let refs = PlacementPolicyRef::canonical_set(request.refs)?;
    let matches = &record.key() == request.key
        && request
            .node_id
            .is_none_or(|node_id| record.node_id == node_id)
        && request
            .blake3
            .is_none_or(|hash| record.location.get_blake3() == Some(hash.as_slice()))
        && !record.location.staging
        && !record.location.partial
        && (refs.is_empty()
            || request
                .subject_generation
                .is_none_or(|generation| record.subject_generation == generation))
        && record.policies == refs;
    match matches {
        true => Ok(record),
        false => Err(ManagedCopyError::Mismatched),
    }
}

/// Reads the local subject row alongside a registration, so one round trip
/// answers both "is this copy registered" and "may this node serve at all".
pub fn serve_reads(
    key: &ManagedCopyKey,
    txn_id: Option<TxnId>,
) -> Result<Effect, ManagedCopyError> {
    Ok(Effect::Storage(StorageEffect::BatchRead {
        reads: vec![
            (MANAGED_COPY_KEYSPACE.to_string(), key.to_bytes()?.into()),
            (
                NODE_SUBJECT_KEYSPACE.to_string(),
                Key::from(NODE_SUBJECT_KEY.to_vec()),
            ),
        ],
        txn_id,
    }))
}

/// Splits the `serve_reads` answer into its two raw values, leaving the verdict
/// to the caller: a reporting read describes a copy it may not serve.
pub fn split_reads(
    values: Vec<(Key, Option<Value>)>,
) -> Result<(Option<Value>, Option<Value>), ManagedCopyError> {
    let mut values = values.into_iter();
    let (_, copy) = values.next().ok_or(ManagedCopyError::InvalidEvent)?;
    let (_, subject) = values.next().ok_or(ManagedCopyError::InvalidEvent)?;
    Ok((copy, subject))
}

/// Splits the `serve_reads` answer. A missing subject row is not an admission:
/// a node that never advertised a subject serves nothing governed.
pub fn split_serve_reads(
    values: Vec<(Key, Option<Value>)>,
) -> Result<(Option<Value>, NodeSubjectRecord), ManagedCopyError> {
    let (copy, subject) = split_reads(values)?;
    let subject = subject.ok_or(ManagedCopyError::NoSubject)?;
    let record = NodeSubjectRecord::from_bytes(subject.as_ref())?;
    if record.serving_blocked {
        return Err(ManagedCopyError::ServingBlocked);
    }
    Ok((copy, record))
}

/// Scan prefix covering every local backend holding one logical version.
pub fn version_scope(version: &VersionKey) -> Result<Key, ManagedCopyError> {
    Ok(ManagedCopyKey::version_prefix(version)?.into())
}

/// Paginated scan. `scope` of `None` walks every copy this node registered,
/// which is what departure, subject transition and verification enumerate.
pub fn scan_effect(
    scope: Option<Key>,
    start_after: Option<Key>,
    limit: usize,
    txn_id: Option<TxnId>,
) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: MANAGED_COPY_KEYSPACE.to_string(),
        prefix: scope,
        start: start_after.map(IterStart::After),
        limit,
        txn_id,
    })
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ManagedCopyPage {
    pub entries: Vec<(ManagedCopyKey, ManagedCopyRecord)>,
    /// Cursor for the next `scan_effect`; `None` once the scope is exhausted.
    pub cursor: Option<Key>,
}

impl ManagedCopyPage {
    pub fn decode(event: Event) -> Result<Self, ManagedCopyError> {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return Err(ManagedCopyError::InvalidEvent);
        };
        let mut entries = Vec::with_capacity(values.len());
        for (key, value) in values {
            entries.push((
                ManagedCopyKey::from_bytes(key.as_ref())?,
                ManagedCopyRecord::from_bytes(value.as_ref())?,
            ));
        }
        Ok(Self {
            entries,
            cursor: next_start_after,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RemovalPhase {
    Scanning,
    Deleting,
}

/// Removes every local registration of one logical version inside the caller's
/// transaction, so the copies stop being serveable exactly when the version does.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedCopyRemoval {
    scope: Key,
    cursor: Option<Key>,
    phase: RemovalPhase,
}

impl ManagedCopyRemoval {
    pub fn for_version(version: &VersionKey) -> Result<Self, ManagedCopyError> {
        Ok(Self {
            scope: version_scope(version)?,
            cursor: None,
            phase: RemovalPhase::Scanning,
        })
    }

    pub fn start(&mut self, txn_id: Option<TxnId>) -> Effects {
        self.phase = RemovalPhase::Scanning;
        smallvec![scan_effect(
            Some(self.scope.clone()),
            self.cursor.clone(),
            COPY_PAGE_LIMIT,
            txn_id,
        )]
    }

    /// `Ok(None)` once every registration in scope is deleted.
    pub fn step(
        &mut self,
        event: Event,
        txn_id: Option<TxnId>,
    ) -> Result<Option<Effects>, ManagedCopyError> {
        match self.phase {
            RemovalPhase::Scanning => {
                let Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) = event
                else {
                    return Err(ManagedCopyError::InvalidEvent);
                };
                if values.is_empty() {
                    return Ok(None);
                }
                self.cursor = next_start_after;
                self.phase = RemovalPhase::Deleting;
                let deletes = values
                    .into_iter()
                    .map(|(key, _)| (MANAGED_COPY_KEYSPACE.to_string(), key))
                    .collect();
                Ok(Some(smallvec![Effect::Storage(
                    StorageEffect::BatchDelete { deletes, txn_id }
                )]))
            }
            RemovalPhase::Deleting => {
                let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
                    return Err(ManagedCopyError::InvalidEvent);
                };
                // A cursor means the page was full: the scan resumes strictly
                // after the last deleted key, so removal always makes progress.
                match self.cursor.is_some() {
                    true => Ok(Some(self.start(txn_id))),
                    false => Ok(None),
                }
            }
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{
        CopyRequest, ManagedCopyError, ManagedCopyPage, ManagedCopyRemoval, check_serveable,
        register_effect, scan_effect, split_serve_reads, transition_effect, validate_registration,
    };
    use crate::effects::{Effect, IterStart, StorageEffect};
    use crate::events::{Event, StorageEvent};
    use crate::id::NodeId;
    use crate::keyspaces::MANAGED_COPY_KEYSPACE;
    use crate::structs::placement::node_subject::NodeSubjectRecord;
    use crate::structs::placement::policy::{PlacementPolicyRef, PlacementSubject};
    use crate::structs::storage::blob::CopyOrigin;
    use crate::structs::storage::blob::{
        BackendLocation, BackendRef, ManagedCopyKey, ManagedCopyQuarantine, ManagedCopyRecord,
        ManagedCopyState, VersionKey,
    };
    use crate::structs::storage::format::StoredFormat;
    use std::collections::HashMap;
    use std::time::UNIX_EPOCH;
    use ulid::Ulid;

    fn node_id() -> NodeId {
        iroh::SecretKey::from_bytes(&[9u8; 32]).public()
    }

    fn version() -> VersionKey {
        VersionKey::new("bucket", "path/file.txt", Ulid::from_bytes([4u8; 16]))
    }

    fn location(staging: bool, partial: bool) -> BackendLocation {
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "aruna".to_string(),
            backend_path: "objects/one".to_string(),
            ulid: Ulid::from_bytes([5u8; 16]),
            format: StoredFormat::default(),
            created_by: Default::default(),
            created_at: UNIX_EPOCH,
            staging,
            partial,
            blob_size: 3,
            hashes: HashMap::new(),
        }
    }

    fn registration(
        location: &BackendLocation,
        origin: CopyOrigin,
        registered_at_ms: u64,
    ) -> super::CopyRegistration<'_> {
        super::CopyRegistration {
            version: version(),
            node_id: node_id(),
            location,
            policies: &[],
            origin,
            subject_generation: 1,
            registered_at_ms,
        }
    }

    fn record(state: ManagedCopyState) -> ManagedCopyRecord {
        ManagedCopyRecord::new(
            version(),
            node_id(),
            location(false, false),
            Vec::new(),
            7,
            state,
        )
        .expect("record builds")
    }

    #[test]
    fn refuses_unstable_copies() {
        // A staging or partial write must never become a serveable registration.
        for location in [location(true, false), location(false, true)] {
            assert_eq!(
                register_effect(registration(&location, CopyOrigin::Write, 7), None),
                Err(ManagedCopyError::UnstableCopy)
            );
        }
    }

    #[test]
    fn joins_caller_txn() {
        // The registration also stores the provenance a reader later reports.
        let txn_id = Ulid::from_bytes([8u8; 16]);
        let relationship_id = Ulid::from_bytes([3u8; 16]);
        let effect = register_effect(
            registration(
                &location(false, false),
                CopyOrigin::Sync { relationship_id },
                7,
            ),
            Some(txn_id),
        )
        .expect("effect builds");

        let Effect::Storage(StorageEffect::Write {
            key_space,
            value,
            txn_id: effect_txn,
            ..
        }) = effect
        else {
            panic!("expected a storage write");
        };
        assert_eq!(key_space, MANAGED_COPY_KEYSPACE);
        assert_eq!(effect_txn, Some(txn_id));
        let stored = ManagedCopyRecord::from_bytes(value.as_ref()).expect("record decodes");
        assert_eq!(stored.state, ManagedCopyState::Registered);
        assert_eq!(stored.node_id, node_id());
        assert_eq!(stored.origin, CopyOrigin::Sync { relationship_id });
    }

    #[test]
    fn reports_unserveable_row() {
        // A reporting caller sees a quarantined registration that serving must
        // refuse, so a held copy can be listed instead of hidden.
        let quarantined = record(ManagedCopyState::Quarantined(
            ManagedCopyQuarantine::PolicyViolation,
        ));
        let bytes = quarantined.to_bytes().expect("record encodes");
        let key = quarantined.key();
        let request = CopyRequest {
            key: &key,
            node_id: Some(node_id()),
            blake3: None,
            refs: &[],
            subject_generation: None,
        };

        assert_eq!(
            super::registration_for(Some(&bytes), &request),
            Ok(quarantined.clone())
        );
        assert_eq!(
            validate_registration(Some(&bytes), &request),
            Err(ManagedCopyError::NotServeable(quarantined.state))
        );
        assert_eq!(
            super::registration_for(None, &request),
            Err(ManagedCopyError::Unregistered)
        );
    }

    #[test]
    fn rejects_missing_row() {
        assert_eq!(
            check_serveable(None),
            Err(ManagedCopyError::Unregistered),
            "an absent registration must fail closed"
        );
    }

    #[test]
    fn gate_rejects_quarantine() {
        for state in [
            ManagedCopyState::Quarantined(ManagedCopyQuarantine::Rejoin),
            ManagedCopyState::Quarantined(ManagedCopyQuarantine::SubjectTransition),
            ManagedCopyState::Quarantined(ManagedCopyQuarantine::PolicyViolation),
            ManagedCopyState::UnresolvedDeparted,
        ] {
            let bytes = record(state).to_bytes().expect("record encodes");
            assert_eq!(
                check_serveable(Some(&bytes)),
                Err(ManagedCopyError::NotServeable(state))
            );
        }
        let registered = record(ManagedCopyState::Registered);
        let bytes = registered.to_bytes().expect("record encodes");
        assert_eq!(check_serveable(Some(&bytes)), Ok(registered));
    }

    #[test]
    fn registration_must_match() {
        // A row for another copy is evidence about that copy, never this one.
        let registered = record(ManagedCopyState::Registered);
        let bytes = registered.to_bytes().expect("record encodes");
        let key = registered.key();
        assert!(
            validate_registration(
                Some(&bytes),
                &CopyRequest {
                    key: &key,
                    node_id: Some(node_id()),
                    blake3: None,
                    refs: &[],
                    subject_generation: None,
                }
            )
            .is_ok()
        );

        let other = ManagedCopyKey::new(
            VersionKey::new("bucket", "path/file.txt", Ulid::from_bytes([9u8; 16])),
            BackendRef::node_default(),
        );
        for request in [
            CopyRequest {
                key: &other,
                node_id: Some(node_id()),
                blake3: None,
                refs: &[],
                subject_generation: None,
            },
            CopyRequest {
                key: &key,
                node_id: Some(iroh::SecretKey::from_bytes(&[1u8; 32]).public()),
                blake3: None,
                refs: &[],
                subject_generation: None,
            },
            CopyRequest {
                key: &key,
                node_id: Some(node_id()),
                blake3: Some([7u8; 32]),
                refs: &[],
                subject_generation: None,
            },
            CopyRequest {
                key: &key,
                node_id: Some(node_id()),
                blake3: None,
                refs: &[PlacementPolicyRef {
                    policy_id: Ulid::from_bytes([1u8; 16]),
                    digest: [2u8; 32],
                }],
                subject_generation: None,
            },
        ] {
            assert_eq!(
                validate_registration(Some(&bytes), &request),
                Err(ManagedCopyError::Mismatched)
            );
        }
    }

    #[test]
    fn rejects_blocked_serving() {
        // A rejoin blocks every governed serve until the inventory is revalidated,
        // even for rows that still read as registered.
        let registered = record(ManagedCopyState::Registered);
        let copy: crate::types::Value = registered.to_bytes().expect("record encodes").into();
        let mut subject = NodeSubjectRecord::seed(PlacementSubject {
            node_id: node_id(),
            generation: 1,
            location: "eu-west".to_string(),
            labels: Default::default(),
            executor_kind: None,
            local_to_controller: true,
        })
        .expect("subject is valid");
        subject.serving_blocked = true;

        let reads = |subject: &NodeSubjectRecord| {
            vec![
                (crate::types::Key::from(vec![1u8]), Some(copy.clone())),
                (
                    crate::types::Key::from(vec![2u8]),
                    Some(subject.to_bytes().expect("record encodes").into()),
                ),
            ]
        };
        assert_eq!(
            split_serve_reads(reads(&subject)),
            Err(ManagedCopyError::ServingBlocked)
        );
        assert!(split_serve_reads(reads(&subject.cleared())).is_ok());
        assert_eq!(
            split_serve_reads(vec![
                (crate::types::Key::from(vec![1u8]), Some(copy)),
                (crate::types::Key::from(vec![2u8]), None),
            ]),
            Err(ManagedCopyError::NoSubject)
        );
    }

    #[test]
    fn transition_keeps_key() {
        let registered = record(ManagedCopyState::Registered);
        let effect = transition_effect(
            &registered,
            ManagedCopyState::Quarantined(ManagedCopyQuarantine::SubjectTransition),
            None,
        )
        .expect("effect builds");

        let Effect::Storage(StorageEffect::Write { key, value, .. }) = effect else {
            panic!("expected a storage write");
        };
        assert_eq!(
            key.as_ref(),
            registered.key().to_bytes().expect("key encodes").as_slice()
        );
        let stored = ManagedCopyRecord::from_bytes(value.as_ref()).expect("record decodes");
        assert!(!stored.state.is_serveable());
    }

    #[test]
    fn scan_uses_cursor() {
        let cursor = crate::types::Key::from(vec![1u8, 2, 3]);
        let effect = scan_effect(None, Some(cursor.clone()), 32, None);

        let Effect::Storage(StorageEffect::Iter {
            prefix,
            start,
            limit,
            ..
        }) = effect
        else {
            panic!("expected a storage iteration");
        };
        assert!(prefix.is_none());
        assert_eq!(start, Some(IterStart::After(cursor)));
        assert_eq!(limit, 32);
    }

    #[test]
    fn page_decodes_entries() {
        let registered = record(ManagedCopyState::Registered);
        let page = ManagedCopyPage::decode(Event::Storage(StorageEvent::IterResult {
            values: vec![(
                registered.key().to_bytes().expect("key encodes").into(),
                registered.to_bytes().expect("record encodes").into(),
            )],
            next_start_after: None,
        }))
        .expect("page decodes");

        assert_eq!(page.entries, vec![(registered.key(), registered)]);
        assert!(page.cursor.is_none());
    }

    #[test]
    fn removal_deletes_backends() {
        let mut removal = ManagedCopyRemoval::for_version(&version()).expect("scope builds");
        let txn_id = Ulid::from_bytes([8u8; 16]);
        assert_eq!(removal.start(Some(txn_id)).len(), 1);

        let registered = record(ManagedCopyState::Registered);
        let key: crate::types::Key = registered.key().to_bytes().expect("key encodes").into();
        let effects = removal
            .step(
                Event::Storage(StorageEvent::IterResult {
                    values: vec![(key.clone(), registered.to_bytes().unwrap().into())],
                    next_start_after: None,
                }),
                Some(txn_id),
            )
            .expect("removal steps")
            .expect("delete effects");

        let [
            Effect::Storage(StorageEffect::BatchDelete {
                deletes,
                txn_id: id,
            }),
        ] = effects.as_slice()
        else {
            panic!("expected one batch delete");
        };
        assert_eq!(deletes, &vec![(MANAGED_COPY_KEYSPACE.to_string(), key)]);
        assert_eq!(*id, Some(txn_id));

        assert_eq!(
            removal
                .step(
                    Event::Storage(StorageEvent::BatchDeleteResult { entries: vec![] }),
                    Some(txn_id)
                )
                .expect("removal steps"),
            None
        );
    }

    #[test]
    fn removal_pages_forward() {
        // A full page carries a cursor: the next scan must resume strictly after it.
        let mut removal = ManagedCopyRemoval::for_version(&version()).expect("scope builds");
        let registered = record(ManagedCopyState::Registered);
        let key: crate::types::Key = registered.key().to_bytes().expect("key encodes").into();
        removal.start(None);
        removal
            .step(
                Event::Storage(StorageEvent::IterResult {
                    values: vec![(key.clone(), registered.to_bytes().unwrap().into())],
                    next_start_after: Some(key.clone()),
                }),
                None,
            )
            .expect("removal steps");

        let effects = removal
            .step(
                Event::Storage(StorageEvent::BatchDeleteResult { entries: vec![] }),
                None,
            )
            .expect("removal steps")
            .expect("another scan");

        let [Effect::Storage(StorageEffect::Iter { start, .. })] = effects.as_slice() else {
            panic!("expected one iteration");
        };
        assert_eq!(start, &Some(IterStart::After(key)));
    }

    #[test]
    fn removal_rejects_event() {
        let mut removal = ManagedCopyRemoval::for_version(&version()).expect("scope builds");
        removal.start(None);
        assert_eq!(
            removal.step(
                Event::Storage(StorageEvent::WriteResult {
                    key: vec![0u8].into()
                }),
                None
            ),
            Err(ManagedCopyError::InvalidEvent)
        );
    }

    #[test]
    fn caller_supplies_time() {
        // Registration must not read a clock inside the sans-I/O operation.
        let effect = register_effect(
            registration(&location(false, false), CopyOrigin::Write, 42),
            None,
        )
        .expect("effect builds");
        let Effect::Storage(StorageEffect::Write { value, .. }) = effect else {
            panic!("expected a storage write");
        };
        let stored = ManagedCopyRecord::from_bytes(value.as_ref()).expect("record decodes");
        assert_eq!(stored.registered_at_ms, 42);
    }
}
