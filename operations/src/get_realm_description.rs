use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::REALM_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{Realm, RealmId};
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Debug, PartialEq)]
pub struct GetRealmDescriptionOperation {
    realm_id: RealmId,
    state: GetRealmDescriptionState,
    output: Option<Result<Option<String>, GetRealmDescriptionError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum GetRealmDescriptionState {
    Init,
    ReadRealm,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetRealmDescriptionError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
    #[error("get realm description did not finish")]
    NotFinished,
}

impl GetRealmDescriptionOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            realm_id,
            state: GetRealmDescriptionState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: GetRealmDescriptionError) -> Effects {
        self.state = GetRealmDescriptionState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_realm_read(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                let result = value
                    .map(|bytes| {
                        Realm::from_bytes(&bytes)
                            .map(|realm| realm.description)
                            .map_err(Into::into)
                    })
                    .transpose();
                self.state = GetRealmDescriptionState::Finish;
                self.output = Some(result);
                smallvec![]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            other => self.fail(GetRealmDescriptionError::UnexpectedEvent {
                state: format!("{:?}", self.state),
                expected: "storage read result",
                got: format!("{other:?}"),
            }),
        }
    }
}

impl Operation for GetRealmDescriptionOperation {
    type Output = Option<String>;
    type Error = GetRealmDescriptionError;

    fn start(&mut self) -> Effects {
        self.state = GetRealmDescriptionState::ReadRealm;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: REALM_KEYSPACE.to_string(),
            key: self.realm_id.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GetRealmDescriptionState::ReadRealm => self.handle_realm_read(event),
            GetRealmDescriptionState::Init
            | GetRealmDescriptionState::Finish
            | GetRealmDescriptionState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            GetRealmDescriptionState::Finish | GetRealmDescriptionState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(GetRealmDescriptionError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
