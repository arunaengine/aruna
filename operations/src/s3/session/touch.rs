use super::S3SessionError;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_SESSION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::S3Session;
use aruna_core::types::Effects;
use smallvec::smallvec;
use std::time::SystemTime;
use ulid::Ulid;

const TOUCH_ATTEMPTS: u8 = 4;

#[derive(Debug, PartialEq)]
pub struct TouchS3SessionConfig {
    pub access_key: String,
    pub token_hash: String,
    pub now: SystemTime,
    pub issued_by: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TouchSessionState {
    Init,
    StartTransaction,
    ReadSession,
    WriteSession,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct TouchS3SessionOperation {
    config: TouchS3SessionConfig,
    pending: Option<S3Session>,
    txn_id: Option<Ulid>,
    attempts: u8,
    state: TouchSessionState,
    output: Result<S3Session, S3SessionError>,
}

impl TouchS3SessionOperation {
    pub fn new(config: TouchS3SessionConfig) -> Self {
        Self {
            config,
            pending: None,
            txn_id: None,
            attempts: 0,
            state: TouchSessionState::Init,
            output: Err(S3SessionError::NotFinished),
        }
    }

    fn fail(&mut self, error: S3SessionError) -> Effects {
        self.state = TouchSessionState::Error;
        self.output = Err(error);
        self.abort()
    }

    fn start_touch(&mut self) -> Effects {
        if !matches!(self.state, TouchSessionState::Init) {
            return self.fail(S3SessionError::Failed);
        }
        if !S3Session::valid_access_key(&self.config.access_key) {
            return self.fail(S3SessionError::InvalidAccessKey);
        }
        self.state = TouchSessionState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected(event, "StorageEvent::TransactionStarted");
        };
        self.txn_id = Some(txn_id);
        self.state = TouchSessionState::ReadSession;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_SESSION_KEYSPACE.to_string(),
            key: self.config.access_key.as_bytes().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn session_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event, "StorageEvent::ReadResult");
        };
        let Some(value) = value else {
            return self.fail(S3SessionError::NotFound);
        };
        let mut session = match S3Session::from_bytes(value.as_ref()) {
            Ok(session) => session,
            Err(error) => return self.fail(error.into()),
        };
        if session.access_key != self.config.access_key {
            return self.fail(S3SessionError::IndexInconsistent);
        }
        if session.issued_by != self.config.issued_by {
            return self.fail(S3SessionError::WrongIssuer);
        }
        if !session.token_matches(&self.config.token_hash) {
            return self.fail(S3SessionError::InvalidToken);
        }
        if session.is_expired(self.config.now) {
            return self.fail(S3SessionError::Expired);
        }
        session.last_used_at = Some(self.config.now);
        let bytes = match session.to_bytes() {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error.into()),
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(S3SessionError::Failed);
        };
        self.pending = Some(session);
        self.state = TouchSessionState::WriteSession;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: S3_SESSION_KEYSPACE.to_string(),
            key: self.config.access_key.as_bytes().into(),
            value: bytes.into(),
            txn_id: Some(txn_id),
        })]
    }

    fn session_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected(event, "StorageEvent::WriteResult");
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(S3SessionError::Failed);
        };
        self.state = TouchSessionState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected(event, "StorageEvent::TransactionCommitted");
        };
        self.txn_id = None;
        let Some(session) = self.pending.take() else {
            return self.fail(S3SessionError::Failed);
        };
        self.output = Ok(session);
        self.state = TouchSessionState::Finish;
        smallvec![]
    }

    fn unexpected(&mut self, received: Event, expected: &'static str) -> Effects {
        self.fail(S3SessionError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            received,
        })
    }

    fn retry_touch(&mut self) -> Effects {
        self.txn_id = None;
        self.pending = None;
        self.attempts += 1;
        self.state = TouchSessionState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }
}

impl Operation for TouchS3SessionOperation {
    type Output = S3Session;
    type Error = S3SessionError;

    fn start(&mut self) -> Effects {
        self.start_touch()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            if self.state == TouchSessionState::CommitTransaction
                && matches!(error, StorageError::TransactionConflict)
                && self.attempts + 1 < TOUCH_ATTEMPTS
            {
                return self.retry_touch();
            }
            return self.fail(error.clone().into());
        }
        match self.state {
            TouchSessionState::StartTransaction => self.transaction_started(event),
            TouchSessionState::ReadSession => self.session_read(event),
            TouchSessionState::WriteSession => self.session_written(event),
            TouchSessionState::CommitTransaction => self.transaction_committed(event),
            TouchSessionState::Init | TouchSessionState::Finish | TouchSessionState::Error => {
                self.unexpected(event, "valid session operation event")
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            TouchSessionState::Finish | TouchSessionState::Error
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_touch_conflict() {
        let mut operation = TouchS3SessionOperation::new(TouchS3SessionConfig {
            access_key: S3Session::build_access_key("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            token_hash: "token".to_string(),
            now: SystemTime::UNIX_EPOCH,
            issued_by: [1; 32],
        });
        operation.state = TouchSessionState::CommitTransaction;
        operation.txn_id = Some(Ulid::from_bytes([2; 16]));

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        ));
        assert_eq!(operation.state, TouchSessionState::StartTransaction);
        assert_eq!(operation.attempts, 1);
        assert!(!operation.is_complete());
    }
}
