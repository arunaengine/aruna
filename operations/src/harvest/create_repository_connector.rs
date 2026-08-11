use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    RepositoryConnector, RepositoryConnectorKind, RepositoryConnectorSecret,
};
use aruna_core::types::{Effects, GroupId, UserId};
use smallvec::smallvec;
use thiserror::Error;

use crate::endpoint;
use crate::harvest::repository::connector_writes;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateConnectorInput {
    pub group_id: GroupId,
    pub created_by: UserId,
    pub name: String,
    pub kind: RepositoryConnectorKind,
    pub endpoint: String,
    pub public_config: HashMap<String, String>,
    pub secret_config: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateConnectorResult {
    pub connector: RepositoryConnector,
    pub has_secret_config: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum State {
    Init,
    WriteRecords,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateConnectorError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("connector name must not be empty")]
    EmptyName,
    #[error("connector endpoint must not be empty")]
    EmptyEndpoint,
    #[error("endpoint `{0}` must be spelled as the http client parses it")]
    AmbiguousEndpoint(String),
    #[error("CreateRepositoryConnector failed")]
    Failed,
    #[error("state [{state:?}] invalid: expected [{expected}] - received [{received:?}]")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct CreateConnectorOperation {
    input: CreateConnectorInput,
    state: State,
    connector: Option<RepositoryConnector>,
    has_secret: bool,
    output: Option<Result<CreateConnectorResult, CreateConnectorError>>,
}

impl CreateConnectorOperation {
    pub fn new(input: CreateConnectorInput) -> Self {
        Self {
            input,
            state: State::Init,
            connector: None,
            has_secret: false,
            output: None,
        }
    }

    fn emit_error(&mut self, error: CreateConnectorError) -> Effects {
        self.state = State::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        if self.input.name.trim().is_empty() {
            return self.emit_error(CreateConnectorError::EmptyName);
        }
        if self.input.endpoint.trim().is_empty() {
            return self.emit_error(CreateConnectorError::EmptyEndpoint);
        }
        // Every harvest fetch is built from this string, so a spelling the http
        // client reads as another host fails here rather than at first use.
        if !endpoint::is_canonical(&self.input.endpoint) {
            return self.emit_error(CreateConnectorError::AmbiguousEndpoint(
                self.input.endpoint.clone(),
            ));
        }

        let now = SystemTime::now();
        let connector = RepositoryConnector::new(
            ulid::Ulid::generate(),
            self.input.group_id,
            self.input.name.clone(),
            self.input.kind,
            self.input.endpoint.clone(),
            self.input.public_config.clone(),
            now,
            now,
            self.input.created_by,
        );
        let secret = RepositoryConnectorSecret::new(
            connector.connector_id,
            self.input.secret_config.clone(),
            now,
        );

        let writes = match connector_writes(&connector, secret.as_ref()) {
            Ok(writes) => writes,
            Err(error) => return self.emit_error(error.into()),
        };

        self.has_secret = secret.is_some();
        self.connector = Some(connector);
        self.state = State::WriteRecords;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.emit_error(CreateConnectorError::InvalidStateEvent {
                state: format!("{:?}", self.state),
                expected: "Event::Storage(StorageEvent::BatchWriteResult)",
                received: event,
            });
        };
        let Some(connector) = self.connector.clone() else {
            return self.emit_error(CreateConnectorError::Failed);
        };

        self.state = State::Finish;
        self.output = Some(Ok(CreateConnectorResult {
            connector,
            has_secret_config: self.has_secret,
        }));
        smallvec![]
    }
}

impl Operation for CreateConnectorOperation {
    type Output = CreateConnectorResult;
    type Error = CreateConnectorError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            State::Init => self.handle_init(),
            State::WriteRecords => self.handle_written(event),
            State::Finish => smallvec![],
            State::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, State::Finish | State::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == State::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(CreateConnectorError::Failed);
        }
        self.output.ok_or(CreateConnectorError::Failed)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::harvest::repository::{parse_connector_read, read_connector_effect};
    use aruna_core::handle::Handle;
    use aruna_storage::storage;
    use tempfile::tempdir;

    fn input() -> CreateConnectorInput {
        CreateConnectorInput {
            group_id: ulid::Ulid::generate(),
            created_by: Default::default(),
            name: "zenodo".to_string(),
            kind: RepositoryConnectorKind::OaiPmh,
            endpoint: "https://zenodo.org/oai2d".to_string(),
            public_config: HashMap::new(),
            secret_config: HashMap::from([("token".to_string(), "t".to_string())]),
        }
    }

    // an empty name errors before any write
    #[test]
    fn empty_name_errors() {
        let mut op = CreateConnectorOperation::new(CreateConnectorInput {
            name: "  ".to_string(),
            ..input()
        });
        assert!(op.start().is_empty());
        assert!(op.is_complete());
        assert_eq!(op.finalize().unwrap_err(), CreateConnectorError::EmptyName);
    }

    #[test]
    fn respelled_endpoint_errors() {
        // Each of these parses into a link-local or loopback address, and the
        // last two are read as a different host than an operator reads back.
        for endpoint in [
            "https://2852039166",
            "https://0xa9fea9fe",
            "https://169.254.169.254.",
            "https://good.example\\@169.254.169.254",
            "https://Zenodo.ORG/oai2d",
            "zenodo.org/oai2d",
        ] {
            let mut op = CreateConnectorOperation::new(CreateConnectorInput {
                endpoint: endpoint.to_string(),
                ..input()
            });
            assert!(op.start().is_empty(), "{endpoint}");
            assert_eq!(
                op.finalize().unwrap_err(),
                CreateConnectorError::AmbiguousEndpoint(endpoint.to_string())
            );
        }
    }

    #[test]
    fn canonical_endpoint_writes() {
        for endpoint in [
            "https://zenodo.org/oai2d",
            "https://oai.example.org:9000/oai",
            "http://127.0.0.1/oai",
        ] {
            let mut op = CreateConnectorOperation::new(CreateConnectorInput {
                endpoint: endpoint.to_string(),
                ..input()
            });
            assert_eq!(op.start().len(), 1, "{endpoint}");
        }
    }

    // a wrong event in the write state errors
    #[test]
    fn wrong_event_errors() {
        let mut op = CreateConnectorOperation::new(input());
        let effects = op.start();
        assert_eq!(effects.len(), 1);
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: byteview::ByteView::from(Vec::new()),
            value: None,
        }));
        assert!(matches!(
            op.finalize().unwrap_err(),
            CreateConnectorError::InvalidStateEvent { .. }
        ));
    }

    #[tokio::test]
    async fn persists_connector_record() {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = drive(CreateConnectorOperation::new(input()), &context)
            .await
            .unwrap();

        let stored = context
            .storage_handle
            .send_effect(read_connector_effect(
                result.connector.group_id,
                result.connector.connector_id,
                None,
            ))
            .await;
        assert_eq!(
            parse_connector_read(stored).unwrap().unwrap(),
            result.connector
        );
        assert!(result.has_secret_config);
    }
}
