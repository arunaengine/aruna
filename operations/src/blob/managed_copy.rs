//! Local inventory of the logical version copies this node exposes. Every
//! registration and removal joins the transaction that makes the copy visible,
//! so an interrupted write can never leave a serveable unregistered copy.

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{MANAGED_COPY_KEYSPACE, NODE_SUBJECT_KEYSPACE};
use aruna_core::structs::{
    BackendLocation, CopyOrigin, ManagedCopyKey, ManagedCopyRecord, ManagedCopyState,
    NODE_SUBJECT_KEY, NodeSubjectRecord, PlacementPolicyError, PlacementPolicyRef, VersionKey,
};
use aruna_core::types::{Effects, Key, NodeId, TxnId, Value};
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
mod tests {
    use super::{
        CopyRequest, ManagedCopyError, ManagedCopyPage, ManagedCopyRemoval, check_serveable,
        register_effect, scan_effect, split_serve_reads, transition_effect, validate_registration,
    };
    use aruna_core::effects::{Effect, IterStart, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::MANAGED_COPY_KEYSPACE;
    use aruna_core::structs::CopyOrigin;
    use aruna_core::structs::{
        BackendLocation, BackendRef, ManagedCopyKey, ManagedCopyQuarantine, ManagedCopyRecord,
        ManagedCopyState, NodeSubjectRecord, PlacementPolicyRef, PlacementSubject, VersionKey,
    };
    use aruna_core::types::NodeId;
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
            compressed: false,
            encrypted: false,
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
    fn blocked_node_serves_nothing() {
        // A rejoin blocks every governed serve until the inventory is revalidated,
        // even for rows that still read as registered.
        let registered = record(ManagedCopyState::Registered);
        let copy: aruna_core::types::Value = registered.to_bytes().expect("record encodes").into();
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
                (aruna_core::types::Key::from(vec![1u8]), Some(copy.clone())),
                (
                    aruna_core::types::Key::from(vec![2u8]),
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
                (aruna_core::types::Key::from(vec![1u8]), Some(copy)),
                (aruna_core::types::Key::from(vec![2u8]), None),
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
        let cursor = aruna_core::types::Key::from(vec![1u8, 2, 3]);
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
        let key: aruna_core::types::Key = registered.key().to_bytes().expect("key encodes").into();
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
        let key: aruna_core::types::Key = registered.key().to_bytes().expect("key encodes").into();
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

/// Exit gate for step 5: an interrupted, rolled back or deleted copy must never
/// be serveable, and a governed read must fail closed without its registration.
#[cfg(test)]
mod driver_tests {
    use super::{CopyRegistration, ManagedCopyError, register_effect, scan_effect, version_scope};
    use crate::driver::{DriverContext, drive};
    use crate::s3::delete_object::{DeleteObjectInput, DeleteObjectOperation};
    use crate::s3::get_object::{GetObjectError, GetObjectInput, GetObjectOperation};
    use crate::s3::head_object::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
    use crate::s3::put_object::{
        PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation,
    };
    use aruna_blob::blob::BlobHandler;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        BLOB_VERSIONS_KEYSPACE, MANAGED_COPY_KEYSPACE, NODE_SUBJECT_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::{
        Backend, BackendConfig, BackendRef, BlobVersion, ManagedCopyKey, ManagedCopyQuarantine,
        ManagedCopyRecord, ManagedCopyState, NODE_SUBJECT_KEY, NodeSubjectRecord,
        PlacementPolicyRef, PlacementSubject, RealmId, RoutingSnapshot, VersionKey,
    };
    use aruna_core::types::{GroupId, NodeId, UserId};
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use std::collections::{HashMap, VecDeque};
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    const BUCKET: &str = "mybucket";
    const OBJECT: &str = "governed.txt";
    const BODY: &[u8] = b"payload";

    async fn full_context() -> (TempDir, DriverContext) {
        let temp_handle = tempdir().expect("temp dir");
        let temp_root = temp_handle.path().to_str().expect("utf8 path");
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).expect("blob root");
        let storage_handle = storage::FjallStorage::open(temp_root).expect("storage opens");
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .expect("net handle");
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100_000),
                multipart_bucket: Some("multipart".to_string()),
                root: blob_root,
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .expect("blob handle");
        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        (temp_handle, context)
    }

    fn put_config(
        realm_id: RealmId,
        group_id: GroupId,
        node_id: NodeId,
        user_id: UserId,
        preassigned: Option<Ulid>,
        quota_ceiling: Option<u64>,
    ) -> PutObjectConfig {
        PutObjectConfig {
            user_id,
            group_id,
            realm_id,
            node_id,
            request: PutObjectInput {
                bucket: BUCKET.to_string(),
                key: OBJECT.to_string(),
                content_length: Some(BODY.len() as u64),
                body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(BODY))),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: preassigned,
            quota_ceiling,
            routing: RoutingSnapshot::single(group_id),
        }
    }

    fn version_key(version_id: Ulid) -> VersionKey {
        VersionKey::new(BUCKET, OBJECT, version_id)
    }

    async fn read_copy(context: &DriverContext, version_id: Ulid) -> Option<ManagedCopyRecord> {
        let key = ManagedCopyKey::new(version_key(version_id), BackendRef::node_default());
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: MANAGED_COPY_KEYSPACE.to_string(),
                key: key.to_bytes().expect("key encodes").into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage read result");
        };
        value.map(|value| ManagedCopyRecord::from_bytes(value.as_ref()).expect("record decodes"))
    }

    async fn count_copies(context: &DriverContext, version_id: Ulid) -> usize {
        let scope = version_scope(&version_key(version_id)).expect("scope builds");
        let Event::Storage(StorageEvent::IterResult { values, .. }) = context
            .storage_handle
            .send_storage_effect(match scan_effect(Some(scope), None, 64, None) {
                Effect::Storage(effect) => effect,
                other => panic!("expected a storage effect, got {other:?}"),
            })
            .await
        else {
            panic!("unexpected storage iteration result");
        };
        values.len()
    }

    async fn read_version(context: &DriverContext, version_id: Ulid) -> Option<BlobVersion> {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: version_key(version_id)
                    .to_bytes()
                    .expect("key encodes")
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage read result");
        };
        value.map(|value| BlobVersion::from_bytes(value.as_ref()).expect("version decodes"))
    }

    async fn write_copy(context: &DriverContext, record: &ManagedCopyRecord) {
        let Effect::Storage(effect) = register_effect(
            CopyRegistration {
                version: record.version.clone(),
                node_id: record.node_id,
                location: &record.location,
                policies: &record.policies,
                origin: record.origin,
                subject_generation: record.subject_generation,
                registered_at_ms: record.registered_at_ms,
            },
            None,
        )
        .expect("effect builds") else {
            panic!("expected a storage effect");
        };
        let _ = context.storage_handle.send_storage_effect(effect).await;
    }

    /// Runs a put until the version transaction would start, which is exactly a
    /// crash after the blob bytes landed and before anything was registered.
    async fn drive_until_txn(operation: &mut PutObjectOperation, context: &DriverContext) {
        let mut queue: VecDeque<Effect> = operation.start().into_iter().collect();
        while let Some(effect) = queue.pop_front() {
            let event = match effect {
                Effect::Storage(StorageEffect::StartTransaction { .. }) => return,
                Effect::Storage(effect) => context.storage_handle.send_storage_effect(effect).await,
                Effect::Blob(effect) => {
                    context
                        .blob_handle
                        .as_ref()
                        .expect("blob handle")
                        .send_blob_effect(effect)
                        .await
                }
                other => panic!("unexpected pre-transaction effect {other:?}"),
            };
            queue.extend(operation.step(event));
        }
    }

    struct Fixture {
        realm_id: RealmId,
        group_id: GroupId,
        node_id: NodeId,
        user_id: UserId,
    }

    fn fixture(context: &DriverContext) -> Fixture {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        Fixture {
            realm_id,
            group_id: Ulid::generate(),
            node_id: context.net_handle.as_ref().expect("net handle").node_id(),
            user_id: UserId::local(Ulid::generate(), realm_id),
        }
    }

    async fn put_object(context: &DriverContext, fixture: &Fixture) -> Ulid {
        drive(
            PutObjectOperation::new(put_config(
                fixture.realm_id,
                fixture.group_id,
                fixture.node_id,
                fixture.user_id,
                None,
                None,
            )),
            context,
        )
        .await
        .expect("put drives")
        .expect("put succeeds")
        .expect("put returns a result")
        .version_id
    }

    async fn head_object(context: &DriverContext) -> Result<(), HeadObjectError> {
        drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: BUCKET.to_string(),
                key: OBJECT.to_string(),
                version_id: None,
            }),
            context,
        )
        .await
        .map(|_| ())
    }

    async fn get_object(context: &DriverContext, fixture: &Fixture) -> Result<(), GetObjectError> {
        drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: BUCKET.to_string(),
                key: OBJECT.to_string(),
                version_id: None,
                range: None,
                group_id: fixture.group_id,
                user_identity: fixture.user_id,
                node_id: fixture.node_id,
            }),
            context,
        )
        .await
        .map(|_| ())
    }

    /// Advertises a subject for this node, without which nothing governed may
    /// be served at all.
    async fn seed_subject(context: &DriverContext, node_id: NodeId, blocked: bool) {
        let mut record = NodeSubjectRecord::seed(PlacementSubject {
            node_id,
            generation: 1,
            location: "eu-west".to_string(),
            labels: Default::default(),
            executor_kind: None,
            local_to_controller: true,
        })
        .expect("subject is valid");
        record.serving_blocked = blocked;
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: NODE_SUBJECT_KEYSPACE.to_string(),
                key: NODE_SUBJECT_KEY.to_vec().into(),
                value: record.to_bytes().expect("record encodes").into(),
                txn_id: None,
            })
            .await;
    }

    /// Puts refs on the stored version, the state step 6 mints directly.
    async fn govern_version(context: &DriverContext, version_id: Ulid) {
        let version = read_version(context, version_id)
            .await
            .expect("version exists")
            .with_policies(vec![PlacementPolicyRef {
                policy_id: Ulid::from_bytes([3u8; 16]),
                digest: [4u8; 32],
            }])
            .expect("refs stored");
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: version_key(version_id)
                    .to_bytes()
                    .expect("key encodes")
                    .into(),
                value: version.to_bytes().expect("version encodes").into(),
                txn_id: None,
            })
            .await;
        // A gated write stores the same refs on the registration, so the
        // fixture must too or the row describes a different copy.
        if let Some(mut record) = read_copy(context, version_id).await {
            record.policies = version.placement_policies.clone();
            record.subject_generation = 1;
            let _ = context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: MANAGED_COPY_KEYSPACE.to_string(),
                    key: record.key().to_bytes().expect("key encodes").into(),
                    value: record.to_bytes().expect("record encodes").into(),
                    txn_id: None,
                })
                .await;
        }
    }

    async fn delete_copy(context: &DriverContext, version_id: Ulid) {
        let key = ManagedCopyKey::new(version_key(version_id), BackendRef::node_default());
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Delete {
                key_space: MANAGED_COPY_KEYSPACE.to_string(),
                key: key.to_bytes().expect("key encodes").into(),
                txn_id: None,
            })
            .await;
    }

    #[tokio::test]
    async fn put_registers_copy() {
        let (_temp, context) = full_context().await;
        let fixture = fixture(&context);
        let version_id = put_object(&context, &fixture).await;

        let record = read_copy(&context, version_id).await.expect("copy row");
        assert_eq!(record.state, ManagedCopyState::Registered);
        assert_eq!(record.node_id, fixture.node_id);
        assert_eq!(record.version, version_key(version_id));
        assert!(!record.location.staging && !record.location.partial);
    }

    #[tokio::test]
    async fn crash_skips_registration() {
        // Bytes written before the version transaction never become serveable.
        let (_temp, context) = full_context().await;
        let fixture = fixture(&context);
        let version_id = Ulid::generate();
        let mut operation = PutObjectOperation::new(put_config(
            fixture.realm_id,
            fixture.group_id,
            fixture.node_id,
            fixture.user_id,
            Some(version_id),
            None,
        ));

        drive_until_txn(&mut operation, &context).await;
        drop(operation);

        assert!(read_version(&context, version_id).await.is_none());
        assert!(read_copy(&context, version_id).await.is_none());
        assert!(matches!(
            get_object(&context, &fixture).await,
            Err(GetObjectError::NoSuchKey)
        ));
    }

    #[tokio::test]
    async fn replay_reuses_registration() {
        // The retry after an interrupted write registers the copy exactly once.
        let (_temp, context) = full_context().await;
        let fixture = fixture(&context);
        let version_id = Ulid::generate();
        let mut interrupted = PutObjectOperation::new(put_config(
            fixture.realm_id,
            fixture.group_id,
            fixture.node_id,
            fixture.user_id,
            Some(version_id),
            None,
        ));
        drive_until_txn(&mut interrupted, &context).await;
        drop(interrupted);

        for _ in 0..2 {
            drive(
                PutObjectOperation::new(put_config(
                    fixture.realm_id,
                    fixture.group_id,
                    fixture.node_id,
                    fixture.user_id,
                    Some(version_id),
                    None,
                )),
                &context,
            )
            .await
            .expect("put drives")
            .expect("put succeeds")
            .expect("put returns a result");
        }

        assert_eq!(count_copies(&context, version_id).await, 1);
        assert!(
            read_copy(&context, version_id)
                .await
                .expect("copy row")
                .state
                .is_serveable()
        );
    }

    #[tokio::test]
    async fn rollback_removes_registration() {
        // The quota gate aborts the shared transaction, so neither the version
        // nor its registration may survive.
        let (_temp, context) = full_context().await;
        let fixture = fixture(&context);
        let version_id = Ulid::generate();

        let error = drive(
            PutObjectOperation::new(put_config(
                fixture.realm_id,
                fixture.group_id,
                fixture.node_id,
                fixture.user_id,
                Some(version_id),
                Some(1),
            )),
            &context,
        )
        .await;

        assert!(matches!(error, Err(PutObjectError::QuotaExceeded { .. })));
        assert!(read_version(&context, version_id).await.is_none());
        assert!(read_copy(&context, version_id).await.is_none());
    }

    #[tokio::test]
    async fn delete_removes_registration() {
        let (_temp, context) = full_context().await;
        let fixture = fixture(&context);
        let version_id = put_object(&context, &fixture).await;
        assert!(read_copy(&context, version_id).await.is_some());

        drive(
            DeleteObjectOperation::new(DeleteObjectInput {
                bucket: BUCKET.to_string(),
                key: OBJECT.to_string(),
                version_id: Some(version_id),
                group_id: fixture.group_id,
                realm_id: fixture.realm_id,
                node_id: fixture.node_id,
                deleted_by: fixture.user_id,
            }),
            &context,
        )
        .await
        .expect("delete drives")
        .expect("delete succeeds")
        .expect("delete returns a result");

        assert!(read_version(&context, version_id).await.is_none());
        assert_eq!(count_copies(&context, version_id).await, 0);
    }

    #[tokio::test]
    async fn read_needs_registration() {
        let (_temp, context) = full_context().await;
        let fixture = fixture(&context);
        let version_id = put_object(&context, &fixture).await;
        govern_version(&context, version_id).await;
        seed_subject(&context, fixture.node_id, false).await;
        let record = read_copy(&context, version_id).await.expect("copy row");
        delete_copy(&context, version_id).await;

        assert_eq!(
            get_object(&context, &fixture).await,
            Err(GetObjectError::ManagedCopyError(
                ManagedCopyError::Unregistered
            ))
        );

        write_copy(&context, &record).await;
        assert!(get_object(&context, &fixture).await.is_ok());
    }

    #[tokio::test]
    async fn quarantine_blocks_read() {
        let (_temp, context) = full_context().await;
        let fixture = fixture(&context);
        let version_id = put_object(&context, &fixture).await;
        govern_version(&context, version_id).await;
        seed_subject(&context, fixture.node_id, false).await;
        let mut record = read_copy(&context, version_id).await.expect("copy row");
        record.state = ManagedCopyState::Quarantined(ManagedCopyQuarantine::SubjectTransition);
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: MANAGED_COPY_KEYSPACE.to_string(),
                key: record.key().to_bytes().expect("key encodes").into(),
                value: record.to_bytes().expect("record encodes").into(),
                txn_id: None,
            })
            .await;

        assert_eq!(
            get_object(&context, &fixture).await,
            Err(GetObjectError::ManagedCopyError(
                ManagedCopyError::NotServeable(record.state)
            ))
        );
    }

    #[tokio::test]
    async fn head_needs_registration() {
        let (_temp, context) = full_context().await;
        let fixture = fixture(&context);
        let version_id = put_object(&context, &fixture).await;
        govern_version(&context, version_id).await;
        seed_subject(&context, fixture.node_id, false).await;
        delete_copy(&context, version_id).await;

        assert_eq!(
            head_object(&context).await,
            Err(HeadObjectError::ManagedCopyError(
                ManagedCopyError::Unregistered
            ))
        );
    }

    #[tokio::test]
    async fn read_needs_subject() {
        // A node that never advertised a subject serves nothing governed.
        let (_temp, context) = full_context().await;
        let fixture = fixture(&context);
        let version_id = put_object(&context, &fixture).await;
        govern_version(&context, version_id).await;

        assert_eq!(
            get_object(&context, &fixture).await,
            Err(GetObjectError::ManagedCopyError(
                ManagedCopyError::NoSubject
            ))
        );
    }

    #[tokio::test]
    async fn rejoin_blocks_read() {
        // A rejoining node serves nothing until its inventory is revalidated,
        // even though the registration itself still reads as registered.
        let (_temp, context) = full_context().await;
        let fixture = fixture(&context);
        let version_id = put_object(&context, &fixture).await;
        govern_version(&context, version_id).await;
        seed_subject(&context, fixture.node_id, true).await;

        assert_eq!(
            get_object(&context, &fixture).await,
            Err(GetObjectError::ManagedCopyError(
                ManagedCopyError::ServingBlocked
            ))
        );
        seed_subject(&context, fixture.node_id, false).await;
        assert!(get_object(&context, &fixture).await.is_ok());
    }

    #[tokio::test]
    async fn ungoverned_read_unchanged() {
        // An object with no refs must read exactly as before, registration or not.
        let (_temp, context) = full_context().await;
        let fixture = fixture(&context);
        let version_id = put_object(&context, &fixture).await;
        delete_copy(&context, version_id).await;

        assert!(get_object(&context, &fixture).await.is_ok());
        assert!(head_object(&context).await.is_ok());
    }
}
