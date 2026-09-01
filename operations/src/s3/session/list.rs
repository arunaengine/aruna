use super::{S3SessionError, decode_index};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{S3_SESSION_KEYSPACE, S3_SESSION_OWNER_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::S3Session;
use aruna_core::types::{Effects, UserId};
use smallvec::smallvec;
use ulid::Ulid;

const OWNER_SCAN_BATCH: usize = 128;

#[derive(Clone, Debug, Eq, PartialEq)]
enum ListSessionsState {
    Init,
    StartTransaction,
    ScanOwners,
    ReadSessions,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct ListS3SessionsOperation {
    user_identity: UserId,
    access_keys: Vec<String>,
    sessions: Vec<S3Session>,
    txn_id: Option<Ulid>,
    state: ListSessionsState,
    output: Result<Vec<S3Session>, S3SessionError>,
}

impl ListS3SessionsOperation {
    pub fn new(user_identity: UserId) -> Self {
        Self {
            user_identity,
            access_keys: Vec::new(),
            sessions: Vec::new(),
            txn_id: None,
            state: ListSessionsState::Init,
            output: Err(S3SessionError::NotFinished),
        }
    }

    fn fail(&mut self, error: S3SessionError) -> Effects {
        self.state = ListSessionsState::Error;
        self.output = Err(error);
        self.abort()
    }

    fn start_list(&mut self) -> Effects {
        if !matches!(self.state, ListSessionsState::Init) {
            return self.fail(S3SessionError::Failed);
        }
        self.state = ListSessionsState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected(event, "StorageEvent::TransactionStarted");
        };
        self.txn_id = Some(txn_id);
        self.scan_owners(None)
    }

    fn scan_owners(&mut self, start: Option<IterStart>) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(S3SessionError::Failed);
        };
        self.state = ListSessionsState::ScanOwners;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_SESSION_OWNER_KEYSPACE.to_string(),
            prefix: Some(self.user_identity.to_storage_key().into()),
            start,
            limit: OWNER_SCAN_BATCH,
            txn_id: Some(txn_id),
        })]
    }

    fn owners_scanned(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.unexpected(event, "StorageEvent::IterResult");
        };
        for (_, value) in values {
            match decode_index(Some(&value)) {
                Ok(index) => self.access_keys.extend(index),
                Err(error) => return self.fail(error),
            }
        }
        match next_start_after {
            Some(key) => self.scan_owners(Some(IterStart::After(key))),
            None => self.read_sessions(),
        }
    }

    fn read_sessions(&mut self) -> Effects {
        if self.access_keys.is_empty() {
            return self.commit();
        }
        let Some(txn_id) = self.txn_id else {
            return self.fail(S3SessionError::Failed);
        };
        self.state = ListSessionsState::ReadSessions;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: self
                .access_keys
                .iter()
                .map(|access_key| (
                    S3_SESSION_KEYSPACE.to_string(),
                    access_key.as_bytes().into()
                ))
                .collect(),
            txn_id: Some(txn_id),
        })]
    }

    fn sessions_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected(event, "StorageEvent::BatchReadResult");
        };
        if values.len() != self.access_keys.len() {
            return self.fail(S3SessionError::IndexInconsistent);
        }
        for (key, value) in values {
            let Some(value) = value else {
                return self.fail(S3SessionError::IndexInconsistent);
            };
            let session = match S3Session::from_bytes(value.as_ref()) {
                Ok(session) => session,
                Err(error) => return self.fail(error.into()),
            };
            if session.access_key.as_bytes() != key.as_ref()
                || session.user_identity != self.user_identity
            {
                return self.fail(S3SessionError::IndexInconsistent);
            }
            self.sessions.push(session);
        }
        self.commit()
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(S3SessionError::Failed);
        };
        self.state = ListSessionsState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected(event, "StorageEvent::TransactionCommitted");
        };
        self.txn_id = None;
        self.output = Ok(std::mem::take(&mut self.sessions));
        self.state = ListSessionsState::Finish;
        smallvec![]
    }

    fn unexpected(&mut self, received: Event, expected: &'static str) -> Effects {
        self.fail(S3SessionError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            received,
        })
    }
}

impl Operation for ListS3SessionsOperation {
    type Output = Vec<S3Session>;
    type Error = S3SessionError;

    fn start(&mut self) -> Effects {
        self.start_list()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.fail(error.clone().into());
        }
        match self.state {
            ListSessionsState::StartTransaction => self.transaction_started(event),
            ListSessionsState::ScanOwners => self.owners_scanned(event),
            ListSessionsState::ReadSessions => self.sessions_read(event),
            ListSessionsState::CommitTransaction => self.transaction_committed(event),
            ListSessionsState::Init | ListSessionsState::Finish | ListSessionsState::Error => {
                self.unexpected(event, "valid session operation event")
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListSessionsState::Finish | ListSessionsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }

    fn expected_error(error: &Self::Error) -> bool {
        error.expected()
    }
}
