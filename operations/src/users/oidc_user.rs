//! Resolves an OIDC login into a stored user and queues the user document for sync.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{
    DocumentChange, DocumentChangeKind, DocumentOutboxEvent, DocumentSyncRevision, DocumentTarget,
};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState};
use aruna_core::rows::{PrefixPages, done, values};
use aruna_core::storage_entries::{reducer_state_entry, subject_index_writes, sync_revision_entry};
use aruna_core::structs::identity::auth::{Actor, oidc_subject_key};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::user::User;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::time::unix_timestamp_millis as current_timestamp_ms;
use aruna_core::user::validation::EMAIL_HASH_ATTRIBUTE;
use aruna_core::{SUBJECT_CLAIMS_KEYSPACE, SUBJECT_INDEX_KEYSPACE, USER_KEYSPACE};
use byteview::ByteView;
use std::collections::BTreeSet;
use thiserror::Error;
use ulid::Ulid;

use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::placement::target_placement_ref;
#[derive(Clone, Debug, PartialEq)]
pub struct ResolveOidcInput {
    pub actor: Actor,
    pub issuer: String,
    pub subject_id: String,
    pub name: String,
    pub user_id: UserId,
    /// Hash of the email the issuer marked verified; only such an identity hears of a match.
    pub email_hash: Option<String>,
    /// The person chose a new account although an existing one may be theirs.
    pub create_new: bool,
}

/// Users read per page while looking for an account a new identity may belong to.
const MATCH_PAGE: usize = 256;

/// Resolves the login in one write transaction: a known subject returns its user, a new one
/// creates the user with its subject index and outbox rows.
#[derive(Debug, PartialEq)]
pub struct ResolveOidcOperation {
    input: ResolveOidcInput,
    realm_config: Option<RealmConfigDocument>,
    /// Bucket the user's rows publish onto, read inside the write transaction.
    fence: aruna_core::placement::fence::WriteFence,
    users: PrefixPages,
    stage: Stage,
    user: Option<User>,
    announce: bool,
}

#[derive(Debug, PartialEq)]
enum Stage {
    Subject,
    Existing,
    Scan,
    Write,
    Fence,
}

type ResolveTurn = Result<Next<User>, ResolveOidcError>;

#[derive(Debug, Error, PartialEq)]
pub enum ResolveOidcError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentError(#[from] AdminDocumentError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
    #[error("the user's bucket cut over to a new holder set; retry the registration")]
    PlacementFenced,
    #[error("registration did not finish")]
    NotFinished,
    #[error("an account with this email or name already exists")]
    MatchPending,
}

impl From<IoError> for ResolveOidcError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "ResolveOidc".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

impl ResolveOidcOperation {
    pub fn new(input: ResolveOidcInput) -> Self {
        Self {
            input,
            realm_config: None,
            fence: Default::default(),
            users: PrefixPages::new(USER_KEYSPACE, Vec::new(), MATCH_PAGE),
            stage: Stage::Subject,
            user: None,
            announce: false,
        }
    }

    fn subject_key(&self) -> Result<String, ResolveOidcError> {
        Ok(oidc_subject_key(
            &self.input.issuer,
            &self.input.subject_id,
        )?)
    }

    fn accept_subject(&mut self, result: IoResult) -> ResolveTurn {
        let [subject, realm_config] = values::<2>(result)?;
        if let Some(bytes) = realm_config {
            self.realm_config = Some(RealmConfigDocument::from_bytes(&bytes)?);
        }
        match subject {
            Some(user_id) => {
                self.stage = Stage::Existing;
                Ok(Next::Call(IoCall::Read(vec![(
                    USER_KEYSPACE.to_string(),
                    user_id,
                )])))
            }
            None if self.input.email_hash.is_some() && !self.input.create_new => {
                self.stage = Stage::Scan;
                Ok(Next::Call(self.users.call()))
            }
            None => self.create_user(),
        }
    }

    /// Looks for an active account the new identity may belong to before creating one.
    fn accept_users(&mut self, result: IoResult) -> ResolveTurn {
        let email_hash = self.input.email_hash.clone().unwrap_or_default();
        for (_, value) in self.users.take(result)? {
            if may_match(&User::from_bytes(&value)?, &self.input.name, &email_hash) {
                return Err(ResolveOidcError::MatchPending);
            }
        }
        if self.users.done() {
            return self.create_user();
        }
        Ok(Next::Call(self.users.call()))
    }

    fn create_user(&mut self) -> ResolveTurn {
        let subject_id = self.subject_key()?;
        let user = User {
            user_id: self.input.user_id,
            name: self.input.name.clone(),
            subject_ids: vec![subject_id.clone()],
            alias_user_ids: Default::default(),
            attributes: self
                .input
                .email_hash
                .iter()
                .map(|hash| (EMAIL_HASH_ATTRIBUTE.to_string(), hash.clone()))
                .collect(),
        };
        let document_target = DocumentTarget::User {
            user_id: self.input.user_id,
        };
        let admin_target = AdminDocumentTarget::User {
            user_id: self.input.user_id,
        };
        let placement = self
            .realm_config
            .as_ref()
            .map(|config| target_placement_ref(config, &document_target, Default::default()))
            .unwrap_or(PlacementRef::NIL);
        let realm_id = self.input.actor.realm_id;
        if let Some(config) = self.realm_config.as_ref() {
            self.fence.add(realm_id, config, [placement]);
        }
        let generation = self.fence.generation(&realm_id, &placement);
        let document_revision = initial_sync_change(&self.input.actor, placement);
        let mut reducer_state = AdminDocumentState::new(admin_target);
        let admin_events = seed_admin_events(&mut reducer_state, &self.input, subject_id)?;
        let mut writes = vec![
            (
                USER_KEYSPACE.to_string(),
                ByteView::from(self.input.user_id.to_bytes()),
                ByteView::from(user.to_bytes(&self.input.actor)?),
            ),
            sync_revision_entry(&document_target, &document_revision)?,
            reducer_state_entry(&reducer_state)?,
        ];
        for event in &admin_events {
            let record = new_identified_record(
                event.event_id,
                self.input.actor.node_id,
                document_target.clone(),
                Vec::new(),
                DocumentOutboxEvent::admin(event.clone()),
                placement,
                true,
            )
            .fenced_at(generation);
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
        writes.extend(subject_index_writes(&user));
        let claims = postcard::to_allocvec(&BTreeSet::from([user.user_id])).map_err(|error| {
            ResolveOidcError::ConversionError(ConversionError::FromStrError(error.to_string()))
        })?;
        writes.push((
            SUBJECT_CLAIMS_KEYSPACE.to_string(),
            ByteView::from(self.subject_key()?.into_bytes()),
            ByteView::from(claims),
        ));
        self.user = Some(user);
        self.announce = true;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// Takes the bucket's fence inside the transaction before committing, so a
    /// departing holder's close rejects or conflicts this write.
    fn fence_or_finish(&mut self) -> ResolveTurn {
        if self.fence.is_empty() {
            return self.finish();
        }
        self.stage = Stage::Fence;
        Ok(Next::Call(IoCall::Read(self.fence.reads())))
    }

    fn finish(&mut self) -> ResolveTurn {
        let user = self.user.take().ok_or(ResolveOidcError::NotFinished)?;
        if !self.announce {
            return Ok(Next::Done(user));
        }
        Ok(Next::Then {
            output: user,
            calls: vec![IoCall::Effect(Box::new(schedule_drain_effect()))],
        })
    }
}

impl Operation for ResolveOidcOperation {
    type Output = User;
    type Error = ResolveOidcError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> ResolveTurn {
        let subject_key = ByteView::from(self.subject_key()?.into_bytes());
        Ok(Next::Call(IoCall::Read(vec![
            (SUBJECT_INDEX_KEYSPACE.to_string(), subject_key),
            (
                REALM_CONFIG_KEYSPACE.to_string(),
                ByteView::from(*self.input.actor.realm_id.as_bytes()),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> ResolveTurn {
        match self.stage {
            Stage::Subject => self.accept_subject(result),
            Stage::Scan => self.accept_users(result),
            Stage::Existing => {
                let [value] = values::<1>(result)?;
                let value = value.ok_or_else(|| {
                    ConversionError::FromStrError("missing user value".to_string())
                })?;
                self.user = Some(User::from_bytes(&value)?);
                self.fence_or_finish()
            }
            Stage::Write => {
                done(result)?;
                self.fence_or_finish()
            }
            Stage::Fence => {
                let values = result.values()?;
                let keys = self.fence.reads().into_iter().map(|(_, key)| key);
                let rows: Vec<_> = keys.zip(values).collect();
                if !self.fence.admits(&rows) {
                    return Err(ResolveOidcError::PlacementFenced);
                }
                self.finish()
            }
        }
    }
}

/// Whether an active personal account may be the person behind a new identity: the same
/// verified email, or the same name where the directory shows it, so no private name is probed.
fn may_match(user: &User, name: &str, email_hash: &str) -> bool {
    if user.is_deactivated() || user.service_group().is_some() {
        return false;
    }
    user.public_name_is(&name.trim().to_lowercase())
        || user
            .attributes
            .get(EMAIL_HASH_ATTRIBUTE)
            .map(String::as_str)
            == Some(email_hash)
}

fn seed_admin_events(
    state: &mut AdminDocumentState,
    input: &ResolveOidcInput,
    subject_id: String,
) -> Result<Vec<AdminDocumentEvent>, AdminDocumentError> {
    let mut events = Vec::with_capacity(3);
    let email = input
        .email_hash
        .iter()
        .map(|hash| AdminDocumentOperation::UserAttributeSet {
            key: EMAIL_HASH_ATTRIBUTE.to_string(),
            value: hash.clone(),
        });
    for operation in [
        AdminDocumentOperation::UserNameSet {
            name: input.name.clone(),
        },
        AdminDocumentOperation::SubjectIdAdded { subject_id },
    ]
    .into_iter()
    .chain(email)
    {
        let event = state.apply_operation(&input.actor, operation)?;
        events.push(event);
    }

    Ok(events)
}

pub(crate) fn initial_sync_change(actor: &Actor, placement: PlacementRef) -> DocumentChange {
    let updated_at_ms = current_timestamp_ms();
    DocumentChange {
        base: None,
        current: DocumentSyncRevision {
            generation: updated_at_ms,
            event_id: Ulid::generate(),
            actor: actor.node_id,
            updated_at_ms,
        },
        kind: DocumentChangeKind::Upsert,
        placement,
    }
}

#[cfg(test)]
mod tests {
    use super::{ResolveOidcInput, ResolveOidcOperation};
    use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::document::{
        DocumentChange, DocumentChangeKind, DocumentOutboxEvent, DocumentOutboxRecord,
        DocumentTarget,
    };
    use aruna_core::operation::Operation;
    use aruna_core::reducer::AdminDocumentState;
    use aruna_core::storage_entries::{reducer_state_key, sync_revision_key};
    use aruna_core::structs::identity::auth::{Actor, oidc_subject_key};
    use aruna_core::structs::identity::user::User;
    use aruna_core::types::{Key, KeySpace, Value};
    use aruna_core::{SUBJECT_CLAIMS_KEYSPACE, SUBJECT_INDEX_KEYSPACE, UserId};
    use std::collections::BTreeSet;
    use ulid::Ulid;

    fn write_call(turn: Next<User>) -> Vec<(KeySpace, Key, Value)> {
        match turn {
            Next::Call(IoCall::Write(writes)) => writes,
            _ => panic!("expected one write"),
        }
    }

    /// Answers the subject index and realm config read with `subject` and no config.
    fn answer_subject(operation: &mut ResolveOidcOperation, subject: Option<UserId>) -> Next<User> {
        assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
        let subject = subject.map(|user_id| user_id.to_storage_key().into());
        operation
            .next(IoResult::Values(vec![subject, None]))
            .unwrap()
    }

    #[test]
    fn private_name_unmatched() {
        // A chosen display name must not probe for a private name; a verified email still matches.
        let realm_id = aruna_core::structs::identity::realm::RealmId([5u8; 32]);
        let mut user = User {
            user_id: UserId::local(Ulid::from_bytes([6u8; 16]), realm_id),
            name: "Anna Smith".to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: [(
                aruna_core::user::validation::EMAIL_HASH_ATTRIBUTE.to_string(),
                "hash".to_string(),
            )]
            .into(),
        };
        assert!(super::may_match(&user, " anna smith ", "other"));
        user.attributes.insert(
            format!("{}name", aruna_core::user::profile::VISIBILITY_PREFIX),
            "private".to_string(),
        );
        assert!(!super::may_match(&user, "Anna Smith", "other"));
        assert!(super::may_match(&user, "Someone", "hash"));
    }

    #[tokio::test]
    async fn creates_user_index() {
        let realm_id = aruna_core::structs::identity::realm::RealmId([3u8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let mut operation = ResolveOidcOperation::new(ResolveOidcInput {
            actor: actor.clone(),
            issuer: "https://issuer.example".to_string(),
            subject_id: "subject-1".to_string(),
            name: "alice".to_string(),
            user_id,
            email_hash: None,
            create_new: false,
        });
        let expected_user = User {
            user_id,
            name: "alice".to_string(),
            subject_ids: vec![oidc_subject_key("https://issuer.example", "subject-1").unwrap()],
            alias_user_ids: Default::default(),
            attributes: Default::default(),
        };

        let writes = write_call(answer_subject(&mut operation, None));
        let subject_id = oidc_subject_key("https://issuer.example", "subject-1").unwrap();
        let admin_target = AdminDocumentTarget::User { user_id };
        let document_target = DocumentTarget::User { user_id };
        assert_eq!(writes.len(), 7);
        let user_write = writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == aruna_core::USER_KEYSPACE)
            .expect("user write is included");
        let stored_user = User::from_bytes(user_write.2.as_ref()).unwrap();
        assert_eq!(stored_user, expected_user);
        assert!(
            writes
                .iter()
                .any(|(keyspace, _, _)| keyspace == aruna_core::SYNC_REVISION_KEYSPACE)
        );
        let reducer_state_write = writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == aruna_core::DOCUMENT_STATE_KEYSPACE)
            .expect("reducer state write is included");
        assert_eq!(reducer_state_write.1, reducer_state_key(&admin_target));
        let reducer_state: AdminDocumentState =
            postcard::from_bytes(reducer_state_write.2.as_ref()).unwrap();
        assert_eq!(reducer_state.target, admin_target);
        assert_eq!(
            reducer_state.materialized_user_name(),
            Some("alice".to_string())
        );
        assert_eq!(reducer_state.materialized_subject_ids().len(), 1);
        assert!(
            reducer_state
                .materialized_subject_ids()
                .contains(&subject_id)
        );
        assert_eq!(reducer_state.applied_event_ids.len(), 2);
        assert_eq!(reducer_state.clock.sequence_for(&actor.node_id), 2);

        let outbox_records: Vec<DocumentOutboxRecord> = writes
            .iter()
            .filter(|(keyspace, _, _)| keyspace == aruna_core::SYNC_OUTBOX_KEYSPACE)
            .map(|(_, _, value)| postcard::from_bytes(value).unwrap())
            .collect();
        assert_eq!(outbox_records.len(), 2);
        let mut saw_name = false;
        let mut saw_subject = false;
        for record in outbox_records {
            assert_eq!(record.target, document_target);
            assert_eq!(record.node_id, actor.node_id);
            assert!(record.peers.is_empty());
            let DocumentOutboxEvent::AdminOperation { event, .. } = record.event else {
                panic!("unexpected outbox event");
            };
            assert_eq!(record.outbox_id, event.event_id);
            assert_eq!(event.target, admin_target);
            assert_eq!(event.origin_node_id, actor.node_id);
            match event.op {
                AdminDocumentOperation::UserNameSet { name } => {
                    assert_eq!(name, "alice");
                    assert_eq!(event.origin_seq, 1);
                    saw_name = true;
                }
                AdminDocumentOperation::SubjectIdAdded {
                    subject_id: event_subject,
                } => {
                    assert_eq!(event_subject, subject_id);
                    assert_eq!(event.origin_seq, 2);
                    saw_subject = true;
                }
                other => panic!("unexpected admin operation: {other:?}"),
            }
        }
        assert!(saw_name);
        assert!(saw_subject);
        assert!(writes.iter().any(|(keyspace, _, value)| {
            keyspace == SUBJECT_INDEX_KEYSPACE
                && value.as_ref() == user_id.to_storage_key().as_slice()
        }));
        let claims = writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == SUBJECT_CLAIMS_KEYSPACE)
            .map(|(_, _, value)| value)
            .expect("subject claims write exists");
        assert_eq!(
            postcard::from_bytes::<BTreeSet<UserId>>(claims).expect("claims decode"),
            BTreeSet::from([user_id])
        );

        // Without a realm config there is no fence: the user comes out with the drain wake.
        let Ok(Next::Then { output, calls }) = operation.next(IoResult::Done) else {
            panic!("expected the new user with the drain wake");
        };
        assert!(matches!(calls.as_slice(), [IoCall::Effect(_)]));
        assert_eq!(output, expected_user);
    }

    #[tokio::test]
    async fn writes_sync_sidecar() {
        let realm_id = aruna_core::structs::identity::realm::RealmId([7u8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let mut operation = ResolveOidcOperation::new(ResolveOidcInput {
            actor: actor.clone(),
            issuer: "https://issuer.example".to_string(),
            subject_id: "subject-3".to_string(),
            name: "carol".to_string(),
            user_id,
            email_hash: None,
            create_new: false,
        });
        let document_target = DocumentTarget::User { user_id };

        let writes = write_call(answer_subject(&mut operation, None));
        assert_eq!(writes.len(), 7);
        assert!(
            writes
                .iter()
                .any(|(keyspace, _, _)| keyspace == aruna_core::USER_KEYSPACE)
        );

        let (revision_key, revision): (_, DocumentChange) = writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == aruna_core::SYNC_REVISION_KEYSPACE)
            .map(|(_, key, value)| {
                (
                    key,
                    postcard::from_bytes(value).expect("revision sidecar decodes"),
                )
            })
            .expect("revision sidecar write exists");
        assert_eq!(revision_key, &sync_revision_key(&document_target));
        assert_eq!(revision.base, None);
        assert_eq!(revision.current.actor, actor.node_id);
        assert!(revision.current.generation > 0);
        assert!(revision.current.updated_at_ms > 0);
        assert_eq!(revision.kind, DocumentChangeKind::Upsert);
    }

    #[tokio::test]
    async fn returns_existing_user() {
        let realm_id = aruna_core::structs::identity::realm::RealmId([5u8; 32]);
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let existing_user = User {
            user_id,
            name: "bob".to_string(),
            subject_ids: vec![oidc_subject_key("https://issuer.example", "subject-2").unwrap()],
            alias_user_ids: Default::default(),
            attributes: Default::default(),
        };
        let mut operation = ResolveOidcOperation::new(ResolveOidcInput {
            actor: Actor {
                node_id: iroh::SecretKey::from_bytes(&[6u8; 32]).public(),
                user_id: UserId::nil(realm_id),
                realm_id,
            },
            issuer: "https://issuer.example".to_string(),
            subject_id: "subject-2".to_string(),
            name: "ignored".to_string(),
            user_id: UserId::local(Ulid::generate(), realm_id),
            email_hash: None,
            create_new: false,
        });

        let turn = answer_subject(&mut operation, Some(user_id));
        assert!(matches!(turn, Next::Call(IoCall::Read(_))));
        let value = existing_user.to_bytes(&operation.input.actor).unwrap();
        // A known subject commits without new rows, so no drain wake follows.
        assert_eq!(
            operation.next(IoResult::Values(vec![Some(value.into())])),
            Ok(Next::Done(existing_user))
        );
    }

    #[test]
    fn scan_finds_match() {
        // A new identity whose verified email an active account holds is refused, not created.
        let realm_id = aruna_core::structs::identity::realm::RealmId([9u8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        let existing = User {
            user_id: UserId::local(Ulid::generate(), realm_id),
            name: "Dana".to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: [(
                aruna_core::user::validation::EMAIL_HASH_ATTRIBUTE.to_string(),
                "hash".to_string(),
            )]
            .into(),
        };
        let mut operation = ResolveOidcOperation::new(ResolveOidcInput {
            actor: actor.clone(),
            issuer: "https://issuer.example".to_string(),
            subject_id: "subject-4".to_string(),
            name: "Someone".to_string(),
            user_id: UserId::local(Ulid::generate(), realm_id),
            email_hash: Some("hash".to_string()),
            create_new: false,
        });
        let turn = answer_subject(&mut operation, None);
        assert!(matches!(turn, Next::Call(IoCall::Page { .. })));
        let row = (
            existing.user_id.to_storage_key().into(),
            existing.to_bytes(&actor).unwrap().into(),
        );
        assert_eq!(
            operation.next(IoResult::Page {
                rows: vec![row],
                next: None,
            }),
            Err(super::ResolveOidcError::MatchPending)
        );
    }

    #[tokio::test]
    async fn subject_skips_scan() {
        // A known subject signs in to its own account, though its email and name match it.
        let realm_id = aruna_core::structs::identity::realm::RealmId([5u8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[6u8; 32]).public(),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        let subject = oidc_subject_key("https://issuer.example", "subject-3").unwrap();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let existing_user = User {
            user_id,
            name: "Aruna Admin".to_string(),
            subject_ids: vec![subject.clone()],
            alias_user_ids: Default::default(),
            attributes: [(
                aruna_core::user::validation::EMAIL_HASH_ATTRIBUTE.to_string(),
                "hash".to_string(),
            )]
            .into(),
        };
        let mut operation = ResolveOidcOperation::new(ResolveOidcInput {
            actor: actor.clone(),
            issuer: "https://issuer.example".to_string(),
            subject_id: "subject-3".to_string(),
            name: "Aruna Admin".to_string(),
            user_id: UserId::local(Ulid::generate(), realm_id),
            email_hash: Some("hash".to_string()),
            create_new: false,
        });

        let turn = answer_subject(&mut operation, Some(user_id));
        assert!(matches!(turn, Next::Call(IoCall::Read(_))));
        let value = existing_user.to_bytes(&actor).unwrap();
        assert_eq!(
            operation.next(IoResult::Values(vec![Some(value.into())])),
            Ok(Next::Done(existing_user))
        );
    }
}
