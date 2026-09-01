use super::index::{decode_index, owner_key};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{USER_SESSION_KEYSPACE, USER_SESSION_OWNER_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::UserSession;
use aruna_core::types::{Effects, TxnId, UserId};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
enum ListSessionState {
    Init,
    StartTransaction,
    ReadOwnerIndex,
    ReadSessions,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListSessionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("session owner index is inconsistent")]
    IndexInconsistent,
    #[error("session list did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

#[derive(Debug, PartialEq)]
pub struct ListSessionOperation {
    user_id: UserId,
    session_ids: Vec<String>,
    sessions: Vec<UserSession>,
    txn_id: Option<TxnId>,
    state: ListSessionState,
    output: Option<Result<Vec<UserSession>, ListSessionError>>,
}

impl ListSessionOperation {
    pub fn new(user_id: UserId) -> Self {
        Self {
            user_id,
            session_ids: Vec::new(),
            sessions: Vec::new(),
            txn_id: None,
            state: ListSessionState::Init,
            output: None,
        }
    }

    fn emit_error(&mut self, error: ListSessionError) -> Effects {
        let cleanup = self.abort();
        self.state = ListSessionState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected(&mut self, expected: &'static str, event: Event) -> Effects {
        self.emit_error(ListSessionError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got: format!("{event:?}"),
        })
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected("transaction started", event);
        };
        self.txn_id = Some(txn_id);
        self.state = ListSessionState::ReadOwnerIndex;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USER_SESSION_OWNER_KEYSPACE.to_string(),
            key: owner_key(self.user_id),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_index(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected("owner index read", event);
        };
        let index = match decode_index(value.as_ref()) {
            Ok(index) => index,
            Err(error) => return self.emit_error(error.into()),
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(StorageError::TransactionNotFound.into());
        };
        if index.is_empty() {
            self.output = Some(Ok(Vec::new()));
            self.state = ListSessionState::CommitTransaction;
            return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
        }
        self.session_ids = index.into_iter().collect();
        self.state = ListSessionState::ReadSessions;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: self
                .session_ids
                .iter()
                .map(|sid| (USER_SESSION_KEYSPACE.to_string(), sid.as_bytes().into()))
                .collect(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_sessions(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected("session records read", event);
        };
        if values.len() != self.session_ids.len() {
            return self.emit_error(ListSessionError::IndexInconsistent);
        }
        for (key, value) in values {
            let Some(value) = value else {
                return self.emit_error(ListSessionError::IndexInconsistent);
            };
            let session = match UserSession::from_bytes(value.as_ref()) {
                Ok(session) => session,
                Err(error) => return self.emit_error(error.into()),
            };
            if session.user_id != self.user_id
                || session.sid.as_bytes() != key.as_ref()
                || !self.session_ids.contains(&session.sid)
            {
                return self.emit_error(ListSessionError::IndexInconsistent);
            }
            self.sessions.push(session);
        }
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(StorageError::TransactionNotFound.into());
        };
        self.sessions
            .sort_by_key(|session| std::cmp::Reverse(session.created_at));
        self.output = Some(Ok(std::mem::take(&mut self.sessions)));
        self.state = ListSessionState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected("transaction committed", event);
        };
        self.txn_id = None;
        self.state = ListSessionState::Finish;
        smallvec![]
    }
}

impl Operation for ListSessionOperation {
    type Output = Vec<UserSession>;
    type Error = ListSessionError;

    fn start(&mut self) -> Effects {
        self.state = ListSessionState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.emit_error(error.into());
        }
        match self.state {
            ListSessionState::Init => self.start(),
            ListSessionState::StartTransaction => self.handle_started(event),
            ListSessionState::ReadOwnerIndex => self.handle_index(event),
            ListSessionState::ReadSessions => self.handle_sessions(event),
            ListSessionState::CommitTransaction => self.handle_committed(event),
            ListSessionState::Finish | ListSessionState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListSessionState::Finish | ListSessionState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ListSessionError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{RealmId, SessionKind};
    use byteview::ByteView;
    use std::collections::BTreeSet;
    use ulid::Ulid;

    #[test]
    fn lists_revoked_session() {
        let realm_id = RealmId::from_bytes([3; 32]);
        let user_id = UserId::local(Ulid::from_bytes([4; 16]), realm_id);
        let sid = Ulid::from_bytes([5; 16]).to_string();
        let txn_id = Ulid::from_bytes([6; 16]);
        let record = UserSession {
            sid: sid.clone(),
            user_id,
            kind: SessionKind::Assistant,
            label: None,
            created_at: 10,
            expires_at: 20,
            token_hash: "a".repeat(64),
            revoked: true,
        };
        let mut operation = ListSessionOperation::new(user_id);
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: owner_key(user_id),
            value: Some(ByteView::from(
                postcard::to_allocvec(&BTreeSet::from([sid.clone()])).unwrap(),
            )),
        }));
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(
                sid.as_bytes().into(),
                Some(ByteView::from(record.to_bytes().unwrap())),
            )],
        }));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));

        assert!(operation.finalize().unwrap()[0].revoked);
    }
}
