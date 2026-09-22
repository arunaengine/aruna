//! Registers a repository connector for a group after screening its endpoint and config.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::harvest::{
    RepositoryConnector, RepositoryConnectorKind, RepositoryConnectorSecret,
};
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use thiserror::Error;

use crate::endpoint_screening;
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
    GroupWrite(#[from] aruna_core::structs::identity::group_delete::GroupWriteError),
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
    #[error("repository endpoint must use https outside loopback")]
    InsecureEndpoint,
    #[error("repository connector does not accept public key `{0}`")]
    UnknownPublicKey(String),
    #[error("repository connector does not accept secret key `{0}`")]
    UnknownSecretKey(String),
    #[error("repository connector value for `{0}` must not be empty")]
    EmptyValue(String),
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
        if let Err(error) = validate_connector(
            &self.input.name,
            self.input.kind,
            &self.input.endpoint,
            &self.input.public_config,
            &self.input.secret_config,
        ) {
            return self.emit_error(error);
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
        smallvec![crate::groups::fence::write_group_records(
            self.input.group_id,
            writes
        )]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        if !matches!(
            &event,
            Event::SubOperation(aruna_core::events::SubOperationEvent::GroupWritten { .. })
        ) {
            return self.emit_error(CreateConnectorError::InvalidStateEvent {
                state: "WriteRecords".into(),
                expected: "group write result",
                received: event,
            });
        }
        if let Err(error) = crate::groups::fence::group_write_result(event) {
            return self.emit_error(error.into());
        }
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

/// Checks a connector definition before any write; update applies the same rules.
pub fn validate_connector(
    name: &str,
    kind: RepositoryConnectorKind,
    endpoint: &str,
    public_config: &HashMap<String, String>,
    secret_config: &HashMap<String, String>,
) -> Result<(), CreateConnectorError> {
    if name.trim().is_empty() {
        return Err(CreateConnectorError::EmptyName);
    }
    if endpoint.trim().is_empty() {
        return Err(CreateConnectorError::EmptyEndpoint);
    }
    // Every fetch is built from this string, so a spelling the http client
    // reads as another host fails here rather than at first use.
    if !endpoint_screening::is_canonical(endpoint) {
        return Err(CreateConnectorError::AmbiguousEndpoint(
            endpoint.to_string(),
        ));
    }
    match kind {
        RepositoryConnectorKind::OaiPmh => Ok(()),
        RepositoryConnectorKind::Invenio => {
            validate_invenio(endpoint, public_config, secret_config)
        }
    }
}

fn validate_invenio(
    endpoint: &str,
    public_config: &HashMap<String, String>,
    secret_config: &HashMap<String, String>,
) -> Result<(), CreateConnectorError> {
    let url = url::Url::parse(endpoint)
        .map_err(|_| CreateConnectorError::AmbiguousEndpoint(endpoint.to_string()))?;
    let loopback = match url.host() {
        Some(url::Host::Domain(host)) => host == "localhost",
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    };
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        return Err(CreateConnectorError::InsecureEndpoint);
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(CreateConnectorError::AmbiguousEndpoint(
            endpoint.to_string(),
        ));
    }
    for (key, value) in public_config {
        if key != INVENIO_COMMUNITY {
            return Err(CreateConnectorError::UnknownPublicKey(key.clone()));
        }
        if value.trim().is_empty() {
            return Err(CreateConnectorError::EmptyValue(key.clone()));
        }
    }
    for (key, value) in secret_config {
        if key != INVENIO_TOKEN {
            return Err(CreateConnectorError::UnknownSecretKey(key.clone()));
        }
        if value.trim().is_empty() {
            return Err(CreateConnectorError::EmptyValue(key.clone()));
        }
    }
    Ok(())
}

/// Public config key naming the Invenio community new records are submitted to.
pub const INVENIO_COMMUNITY: &str = "community";
/// Secret config key of the optional read token an Invenio connector keeps.
pub const INVENIO_TOKEN: &str = "token";

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
    use aruna_core::events::StorageEvent;
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

    #[test]
    fn invenio_rules_enforced() {
        let invenio = |endpoint: &str, public: &[(&str, &str)], secret: &[(&str, &str)]| {
            let map = |pairs: &[(&str, &str)]| {
                pairs
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect::<HashMap<_, _>>()
            };
            validate_connector(
                "repo",
                RepositoryConnectorKind::Invenio,
                endpoint,
                &map(public),
                &map(secret),
            )
        };
        assert!(
            invenio(
                "https://zenodo.org/api/",
                &[("community", "c")],
                &[("token", "t")]
            )
            .is_ok()
        );
        assert!(invenio("http://127.0.0.1:5000/api/", &[], &[]).is_ok());
        assert!(invenio("http://localhost/api", &[], &[]).is_ok());
        assert_eq!(
            invenio("http://zenodo.org/api/", &[], &[]),
            Err(CreateConnectorError::InsecureEndpoint)
        );
        assert_eq!(
            invenio("https://zenodo.org/api/", &[("root", "/")], &[]),
            Err(CreateConnectorError::UnknownPublicKey("root".into()))
        );
        assert_eq!(
            invenio("https://zenodo.org/api/", &[], &[("password", "p")]),
            Err(CreateConnectorError::UnknownSecretKey("password".into()))
        );
        assert_eq!(
            invenio("https://zenodo.org/api/", &[], &[("token", " ")]),
            Err(CreateConnectorError::EmptyValue("token".into()))
        );
        assert!(matches!(
            invenio("https://zenodo.org/api/?q=1", &[], &[]),
            Err(CreateConnectorError::AmbiguousEndpoint(_))
        ));
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
