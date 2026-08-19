use super::S3SessionError;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_SESSION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::S3Session;
use aruna_core::types::Effects;
use smallvec::smallvec;

#[derive(Clone, Debug, Eq, PartialEq)]
enum GetSessionState {
    Init,
    Read,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct GetS3SessionOperation {
    access_key: String,
    state: GetSessionState,
    output: Result<Option<S3Session>, S3SessionError>,
}

impl GetS3SessionOperation {
    pub fn new(access_key: String) -> Self {
        Self {
            access_key,
            state: GetSessionState::Init,
            output: Err(S3SessionError::NotFinished),
        }
    }

    fn fail(&mut self, error: S3SessionError) -> Effects {
        self.state = GetSessionState::Error;
        self.output = Err(error);
        smallvec![]
    }

    fn start_read(&mut self) -> Effects {
        if !matches!(self.state, GetSessionState::Init) {
            return self.fail(S3SessionError::Failed);
        }
        if !S3Session::valid_access_key(&self.access_key) {
            return self.fail(S3SessionError::InvalidAccessKey);
        }
        self.state = GetSessionState::Read;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_SESSION_KEYSPACE.to_string(),
            key: self.access_key.as_bytes().into(),
            txn_id: None,
        })]
    }

    fn finish_read(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                let session = match value {
                    Some(value) => match S3Session::from_bytes(value.as_ref()) {
                        Ok(session) if session.access_key == self.access_key => Some(session),
                        Ok(_) => return self.fail(S3SessionError::IndexInconsistent),
                        Err(error) => return self.fail(error.into()),
                    },
                    None => None,
                };
                self.state = GetSessionState::Finish;
                self.output = Ok(session);
                smallvec![]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            received => self.fail(S3SessionError::UnexpectedEvent {
                state: format!("{:?}", self.state),
                expected: "StorageEvent::ReadResult",
                received,
            }),
        }
    }
}

impl Operation for GetS3SessionOperation {
    type Output = Option<S3Session>;
    type Error = S3SessionError;

    fn start(&mut self) -> Effects {
        self.start_read()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GetSessionState::Read => self.finish_read(event),
            GetSessionState::Init | GetSessionState::Finish | GetSessionState::Error => {
                self.fail(S3SessionError::UnexpectedEvent {
                    state: format!("{:?}", self.state),
                    expected: "StorageEvent::ReadResult",
                    received: event,
                })
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetSessionState::Finish | GetSessionState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }

    fn expected_error(error: &Self::Error) -> bool {
        error.expected()
    }
}
