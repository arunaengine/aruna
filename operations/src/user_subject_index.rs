use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{stale_subject_index_deletes, subject_index_writes};
use aruna_core::structs::{Actor, User};
use aruna_core::types::{Effects, TxnId, UserId};
use aruna_core::{USER_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE};
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::{BTreeSet, HashSet, VecDeque};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct ResolveUserSubjectConflictsInput {
    pub txn_id: TxnId,
    pub actor: Actor,
    pub document_user_id: UserId,
    pub previous_bytes: Option<Vec<u8>>,
    pub current_bytes: Vec<u8>,
}

#[derive(Debug, PartialEq)]
pub struct ResolveUserSubjectConflictsOperation {
    input: ResolveUserSubjectConflictsInput,
    state: ResolveUserSubjectConflictsState,
    current_user: Option<User>,
    previous_user: Option<User>,
    subject_queue: VecDeque<String>,
    conflict_user_queue: VecDeque<UserId>,
    conflict_user_ids: Vec<UserId>,
    conflict_users: Vec<StoredUser>,
    pending_deletes: Vec<(String, ByteView)>,
    pending_writes: Vec<(String, ByteView, ByteView)>,
    output: Option<Result<(), ResolveUserSubjectConflictsError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ResolveUserSubjectConflictsState {
    Init,
    ReadSubjectIndex { subject: String },
    ReadConflictingUser { user_id: UserId },
    WriteCanonicalUser,
    DeleteStaleEntries,
    WriteSubjectIndexes,
    Finish,
    Error,
}

#[derive(Debug, Clone, PartialEq)]
struct StoredUser {
    user: User,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
struct ConflictResolutionPlan {
    canonical_user: User,
    canonical_bytes: Vec<u8>,
    deletes: Vec<(String, ByteView)>,
    writes: Vec<(String, ByteView, ByteView)>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ResolveUserSubjectConflictsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("resolved user id does not match requested user id")]
    UserIdMismatch,
    #[error("resolver did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl ResolveUserSubjectConflictsOperation {
    pub fn new(input: ResolveUserSubjectConflictsInput) -> Self {
        Self {
            input,
            state: ResolveUserSubjectConflictsState::Init,
            current_user: None,
            previous_user: None,
            subject_queue: VecDeque::new(),
            conflict_user_queue: VecDeque::new(),
            conflict_user_ids: Vec::new(),
            conflict_users: Vec::new(),
            pending_deletes: Vec::new(),
            pending_writes: Vec::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: ResolveUserSubjectConflictsError) -> Effects {
        self.state = ResolveUserSubjectConflictsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }
        Ok(event)
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(ResolveUserSubjectConflictsError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn init(&mut self) -> Result<Effects, ResolveUserSubjectConflictsError> {
        let current_user = User::from_bytes(&self.input.current_bytes)?;
        if current_user.user_id != self.input.document_user_id {
            return Err(ResolveUserSubjectConflictsError::UserIdMismatch);
        }
        let previous_user = match self.input.previous_bytes.as_deref() {
            Some(bytes) if !bytes.is_empty() => Some(User::from_bytes(bytes)?),
            _ => None,
        };
        self.subject_queue = current_user
            .subject_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        self.current_user = Some(current_user);
        self.previous_user = previous_user;
        self.emit_next_read_or_resolution()
    }

    fn emit_next_read_or_resolution(
        &mut self,
    ) -> Result<Effects, ResolveUserSubjectConflictsError> {
        if let Some(subject) = self.subject_queue.pop_front() {
            self.state = ResolveUserSubjectConflictsState::ReadSubjectIndex {
                subject: subject.clone(),
            };
            return Ok(smallvec![Effect::Storage(StorageEffect::Read {
                key_space: USER_SUBJECT_INDEX_KEYSPACE.to_string(),
                key: ByteView::from(subject.into_bytes()),
                txn_id: Some(self.input.txn_id),
            })]);
        }

        if let Some(user_id) = self.conflict_user_queue.pop_front() {
            self.state = ResolveUserSubjectConflictsState::ReadConflictingUser { user_id };
            return Ok(smallvec![Effect::Storage(StorageEffect::Read {
                key_space: USER_KEYSPACE.to_string(),
                key: ByteView::from(user_id.to_bytes()),
                txn_id: Some(self.input.txn_id),
            })]);
        }

        self.emit_write_canonical_user()
    }

    fn handle_read_subject_index(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event("Event::Storage(StorageEvent::ReadResult)", got);
        };

        if let Some(value) = value {
            let indexed_user_id = match parse_index_user_id(&value) {
                Ok(user_id) => user_id,
                Err(error) => return self.fail(error.into()),
            };
            let Some(current_user) = self.current_user.as_ref() else {
                return self.fail(ResolveUserSubjectConflictsError::NotFinished);
            };
            if indexed_user_id != current_user.user_id
                && indexed_user_id.realm_id == current_user.user_id.realm_id
                && !self.conflict_user_ids.contains(&indexed_user_id)
            {
                self.conflict_user_ids.push(indexed_user_id);
                self.conflict_user_queue.push_back(indexed_user_id);
            }
        }

        match self.emit_next_read_or_resolution() {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn handle_read_conflicting_user(&mut self, event: Event, user_id: UserId) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event("Event::Storage(StorageEvent::ReadResult)", got);
        };

        if let Some(bytes) = value {
            let bytes = bytes.to_vec();
            let user = match User::from_bytes(&bytes) {
                Ok(user) => user,
                Err(error) => return self.fail(error.into()),
            };
            if user.user_id == user_id {
                self.conflict_users.push(StoredUser { user, bytes });
            }
        }

        match self.emit_next_read_or_resolution() {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn emit_write_canonical_user(&mut self) -> Result<Effects, ResolveUserSubjectConflictsError> {
        let plan = self.build_resolution()?;
        self.pending_deletes = plan.deletes;
        self.pending_writes = plan.writes;
        self.state = ResolveUserSubjectConflictsState::WriteCanonicalUser;
        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: USER_KEYSPACE.to_string(),
            key: ByteView::from(plan.canonical_user.user_id.to_bytes()),
            value: ByteView::from(plan.canonical_bytes),
            txn_id: Some(self.input.txn_id),
        })])
    }

    fn build_resolution(&self) -> Result<ConflictResolutionPlan, ResolveUserSubjectConflictsError> {
        let current_user = self
            .current_user
            .as_ref()
            .ok_or(ResolveUserSubjectConflictsError::NotFinished)?;
        let mut candidates = Vec::with_capacity(self.conflict_users.len() + 1);
        candidates.push(StoredUser {
            user: current_user.clone(),
            bytes: self.input.current_bytes.clone(),
        });
        candidates.extend(self.conflict_users.iter().cloned());

        let canonical = candidates
            .iter()
            .min_by_key(|candidate| candidate.user.user_id)
            .ok_or(ResolveUserSubjectConflictsError::NotFinished)?;
        let canonical_id = canonical.user.user_id;
        let mut canonical_user = canonical.user.clone();
        let mut subject_ids = BTreeSet::new();
        let mut alias_user_ids = canonical_user
            .alias_user_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut loser_user_ids = BTreeSet::new();

        for candidate in &candidates {
            subject_ids.extend(candidate.user.subject_ids.iter().cloned());
            alias_user_ids.extend(candidate.user.alias_user_ids.iter().copied());
            if candidate.user.user_id != canonical_id {
                loser_user_ids.insert(candidate.user.user_id);
            }
        }
        for loser_user_id in &loser_user_ids {
            alias_user_ids.insert(*loser_user_id);
        }
        alias_user_ids.remove(&canonical_id);

        canonical_user.subject_ids = subject_ids.iter().cloned().collect();
        canonical_user.alias_user_ids = alias_user_ids.into_iter().collect::<HashSet<_>>();

        let canonical_base = candidates
            .iter()
            .find(|candidate| candidate.user.user_id == canonical_id)
            .ok_or(ResolveUserSubjectConflictsError::NotFinished)?;
        let canonical_bytes =
            canonical_user.reconcile_bytes(Some(&canonical_base.bytes), &self.input.actor)?;

        let mut deletes = Vec::new();
        for loser_user_id in loser_user_ids {
            deletes.push((
                USER_KEYSPACE.to_string(),
                ByteView::from(loser_user_id.to_bytes()),
            ));
        }
        deletes.extend(stale_subject_index_deletes(
            self.previous_user.as_ref(),
            Some(&canonical_user),
        ));

        let writes = subject_index_writes(&canonical_user);

        Ok(ConflictResolutionPlan {
            canonical_user,
            canonical_bytes,
            deletes,
            writes,
        })
    }

    fn handle_write_canonical_user(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event("Event::Storage(StorageEvent::WriteResult)", got);
        };
        self.emit_delete_or_write_subject_indexes()
    }

    fn emit_delete_or_write_subject_indexes(&mut self) -> Effects {
        if !self.pending_deletes.is_empty() {
            self.state = ResolveUserSubjectConflictsState::DeleteStaleEntries;
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes: std::mem::take(&mut self.pending_deletes),
                txn_id: Some(self.input.txn_id),
            })];
        }
        self.emit_write_subject_indexes()
    }

    fn handle_delete_stale_entries(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.unexpected_event("Event::Storage(StorageEvent::BatchDeleteResult)", got);
        };
        self.emit_write_subject_indexes()
    }

    fn emit_write_subject_indexes(&mut self) -> Effects {
        if self.pending_writes.is_empty() {
            self.state = ResolveUserSubjectConflictsState::Finish;
            self.output = Some(Ok(()));
            return smallvec![];
        }

        self.state = ResolveUserSubjectConflictsState::WriteSubjectIndexes;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: std::mem::take(&mut self.pending_writes),
            txn_id: Some(self.input.txn_id),
        })]
    }

    fn handle_write_subject_indexes(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.unexpected_event("Event::Storage(StorageEvent::BatchWriteResult)", got);
        };
        self.state = ResolveUserSubjectConflictsState::Finish;
        self.output = Some(Ok(()));
        smallvec![]
    }
}

impl Operation for ResolveUserSubjectConflictsOperation {
    type Output = ();
    type Error = ResolveUserSubjectConflictsError;

    fn start(&mut self) -> Effects {
        match self.init() {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state.clone() {
            ResolveUserSubjectConflictsState::ReadSubjectIndex { .. } => {
                self.handle_read_subject_index(event)
            }
            ResolveUserSubjectConflictsState::ReadConflictingUser { user_id } => {
                self.handle_read_conflicting_user(event, user_id)
            }
            ResolveUserSubjectConflictsState::WriteCanonicalUser => {
                self.handle_write_canonical_user(event)
            }
            ResolveUserSubjectConflictsState::DeleteStaleEntries => {
                self.handle_delete_stale_entries(event)
            }
            ResolveUserSubjectConflictsState::WriteSubjectIndexes => {
                self.handle_write_subject_indexes(event)
            }
            ResolveUserSubjectConflictsState::Init
            | ResolveUserSubjectConflictsState::Finish
            | ResolveUserSubjectConflictsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ResolveUserSubjectConflictsState::Finish | ResolveUserSubjectConflictsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(ResolveUserSubjectConflictsError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

fn parse_index_user_id(value: &[u8]) -> Result<UserId, ConversionError> {
    UserId::from_storage_key(value)
}

pub fn rewrite_subject_index_effects(
    previous: Option<&User>,
    current: &User,
    txn_id: TxnId,
) -> Result<Effects, ConversionError> {
    let deletes = stale_subject_index_deletes(previous, Some(current));
    let writes = subject_index_writes(current);

    let mut effects = smallvec![];
    if !deletes.is_empty() {
        effects.push(Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(txn_id),
        }));
    }
    if !writes.is_empty() {
        effects.push(Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        }));
    }
    Ok(effects)
}

#[cfg(test)]
mod tests {
    use super::{ResolveUserSubjectConflictsInput, ResolveUserSubjectConflictsOperation};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{Actor, RealmId, User, oidc_subject_key};
    use aruna_core::{USER_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE, UserId};
    use byteview::ByteView;
    use std::collections::HashSet;
    use ulid::Ulid;

    fn node_id(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn actor(realm_id: RealmId) -> Actor {
        Actor {
            node_id: node_id(1),
            user_id: UserId::nil(realm_id),
            realm_id,
        }
    }

    fn user_bytes(user_id: UserId, subject_ids: Vec<String>, aliases: HashSet<UserId>) -> Vec<u8> {
        User {
            user_id,
            name: format!("user-{}", user_id.user_ulid),
            subject_ids,
            alias_user_ids: aliases,
            attributes: Default::default(),
        }
        .to_bytes(&actor(user_id.realm_id))
        .unwrap()
    }

    #[test]
    fn resolver_writes_user_and_subject_index_without_conflict() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id);
        let subject = oidc_subject_key("https://issuer.example", "subject-1").unwrap();
        let current_bytes = user_bytes(user_id, vec![subject.clone()], HashSet::new());
        let txn_id = Ulid::new();
        let mut operation =
            ResolveUserSubjectConflictsOperation::new(ResolveUserSubjectConflictsInput {
                txn_id,
                actor: actor(realm_id),
                document_user_id: user_id,
                previous_bytes: None,
                current_bytes,
            });

        let effects = operation.start();
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::Read { .. }))
        ));

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(subject.clone().into_bytes()),
            value: None,
        }));
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::Write { .. }))
        ));

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: ByteView::from(user_id.to_bytes()),
        }));
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchWrite { writes, txn_id: id }) => {
                assert_eq!(id, &Some(txn_id));
                assert_eq!(writes.len(), 1);
                let (key_space, key, value) = &writes[0];
                assert_eq!(key_space, USER_SUBJECT_INDEX_KEYSPACE);
                assert_eq!(key.as_ref(), subject.as_bytes());
                assert_eq!(value.as_ref(), user_id.to_storage_key().as_slice());
            }
            other => panic!("unexpected effect: {other:?}"),
        }

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: vec![(
                USER_SUBJECT_INDEX_KEYSPACE.to_string(),
                ByteView::from(subject.into_bytes()),
            )],
        }));
        assert!(effects.is_empty());
        assert!(operation.is_complete());
    }

    #[test]
    fn resolver_merges_conflicting_subject_into_lowest_user_id() {
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let winner_id = UserId::local(Ulid::from_bytes([1u8; 16]), realm_id);
        let loser_id = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id);
        let subject = oidc_subject_key("https://issuer.example", "subject-1").unwrap();
        let winner_bytes = user_bytes(winner_id, vec![subject.clone()], HashSet::new());
        let loser_bytes = user_bytes(loser_id, vec![subject.clone()], HashSet::new());
        let txn_id = Ulid::new();
        let mut operation =
            ResolveUserSubjectConflictsOperation::new(ResolveUserSubjectConflictsInput {
                txn_id,
                actor: actor(realm_id),
                document_user_id: loser_id,
                previous_bytes: None,
                current_bytes: loser_bytes,
            });

        operation.start();
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(subject.clone().into_bytes()),
            value: Some(ByteView::from(winner_id.to_storage_key())),
        }));
        assert!(
            matches!(effects.first(), Some(Effect::Storage(StorageEffect::Read { key, .. })) if key.as_ref() == winner_id.to_bytes().as_slice())
        );

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(winner_id.to_bytes()),
            value: Some(ByteView::from(winner_bytes)),
        }));
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::Write { key, value, .. }) => {
                assert_eq!(key.as_ref(), winner_id.to_bytes().as_slice());
                let resolved = User::from_bytes(value).unwrap();
                assert_eq!(resolved.user_id, winner_id);
                assert_eq!(resolved.subject_ids, vec![subject.clone()]);
                assert!(resolved.alias_user_ids.contains(&loser_id));
            }
            other => panic!("unexpected effect: {other:?}"),
        }

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: ByteView::from(winner_id.to_bytes()),
        }));
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchDelete {
                deletes,
                txn_id: id,
            }) => {
                assert_eq!(id, &Some(txn_id));
                assert_eq!(deletes.len(), 1);
                let (key_space, key) = &deletes[0];
                assert_eq!(key_space, USER_KEYSPACE);
                assert_eq!(key.as_ref(), loser_id.to_bytes().as_slice());
            }
            other => panic!("unexpected effect: {other:?}"),
        }

        let effects = operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: vec![(
                USER_KEYSPACE.to_string(),
                ByteView::from(loser_id.to_bytes()),
            )],
        }));
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchWrite { writes, .. }) => {
                assert_eq!(writes.len(), 1);
                let (_, key, value) = &writes[0];
                assert_eq!(key.as_ref(), subject.as_bytes());
                assert_eq!(value.as_ref(), winner_id.to_storage_key().as_slice());
            }
            other => panic!("unexpected effect: {other:?}"),
        }
    }
}
