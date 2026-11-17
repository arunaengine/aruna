//! Registers a repository connector for a group after screening its endpoint and config.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::calls::{IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::operation::Operation;
use aruna_core::rows;
use aruna_core::structs::execution::harvest::{
    RepositoryConnector, RepositoryConnectorKind, RepositoryConnectorSecret,
};
use aruna_core::types::GroupId;
use thiserror::Error;

use crate::endpoint_screening;
use crate::harvest::repository::connector_writes;

#[derive(Clone, PartialEq, Eq)]
pub struct CreateConnectorInput {
    pub group_id: GroupId,
    pub created_by: UserId,
    pub name: String,
    pub kind: RepositoryConnectorKind,
    pub endpoint: String,
    pub public_config: HashMap<String, String>,
    pub secret_config: HashMap<String, String>,
}

/// Shows only the secret keys; the values are live credentials.
impl std::fmt::Debug for CreateConnectorInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateConnectorInput")
            .field("group_id", &self.group_id)
            .field("created_by", &self.created_by)
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("endpoint", &self.endpoint)
            .field("public_config", &self.public_config)
            .field(
                "secret_keys",
                &self.secret_config.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateConnectorResult {
    pub connector: RepositoryConnector,
    pub has_secret_config: bool,
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
    #[error("endpoint must be spelled as the http client parses it")]
    AmbiguousEndpoint,
    #[error("connector endpoint must not contain credentials")]
    EndpointCredentials,
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
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for CreateConnectorError {
    fn from(error: IoError) -> Self {
        error.into_storage().map_or_else(Self::Io, Self::Storage)
    }
}

#[derive(Debug, PartialEq)]
pub struct CreateConnectorOperation {
    input: CreateConnectorInput,
    result: Option<CreateConnectorResult>,
}

impl CreateConnectorOperation {
    pub fn new(input: CreateConnectorInput) -> Self {
        Self {
            input,
            result: None,
        }
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
    // The endpoint is stored and listed in the clear, so it must not hold a password.
    if url::Url::parse(endpoint)
        .is_ok_and(|url| !url.username().is_empty() || url.password().is_some())
    {
        return Err(CreateConnectorError::EndpointCredentials);
    }
    // Every fetch is built from this string, so a spelling the http client
    // reads as another host fails here rather than at first use.
    if !endpoint_screening::is_canonical(endpoint) {
        return Err(CreateConnectorError::AmbiguousEndpoint);
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
    let url = url::Url::parse(endpoint).map_err(|_| CreateConnectorError::AmbiguousEndpoint)?;
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
        return Err(CreateConnectorError::AmbiguousEndpoint);
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

    fn first(&mut self) -> Result<Next<Self::Output>, CreateConnectorError> {
        validate_connector(
            &self.input.name,
            self.input.kind,
            &self.input.endpoint,
            &self.input.public_config,
            &self.input.secret_config,
        )?;
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
        let writes = connector_writes(&connector, secret.as_ref())?;
        self.result = Some(CreateConnectorResult {
            connector,
            has_secret_config: secret.is_some(),
        });
        let write = crate::groups::fence::write_group_records(self.input.group_id, writes);
        Ok(Next::Call(write.into()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CreateConnectorError> {
        crate::groups::fence::group_write_result(rows::event(result)?)?;
        let result = self.result.take().ok_or(CreateConnectorError::Failed)?;
        Ok(Next::Done(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::harvest::repository::{parse_connector_read, read_connector_effect};
    use aruna_core::calls::IoCall;
    use aruna_core::events::{Event, StorageEvent};
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
        assert_eq!(op.first(), Err(CreateConnectorError::EmptyName));
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
            assert_eq!(
                op.first(),
                Err(CreateConnectorError::AmbiguousEndpoint),
                "{endpoint}"
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
            let next = op.first();
            assert!(
                matches!(next, Ok(Next::Call(IoCall::Child(_)))),
                "{endpoint}"
            );
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
            Err(CreateConnectorError::AmbiguousEndpoint)
        ));
    }

    #[test]
    fn endpoint_credentials_refused() {
        for kind in [
            RepositoryConnectorKind::OaiPmh,
            RepositoryConnectorKind::Invenio,
        ] {
            for (endpoint, expected) in [
                (
                    "https://harvester:canary-346-connector@oai.example.org/oai",
                    CreateConnectorError::EndpointCredentials,
                ),
                (
                    "https://canary-346-connector@oai.example.org/oai",
                    CreateConnectorError::EndpointCredentials,
                ),
                // An address that does not parse must not come back in the error either.
                (
                    "https://harvester:canary-346-connector@oai.example.org:bad/oai",
                    CreateConnectorError::AmbiguousEndpoint,
                ),
            ] {
                let error =
                    validate_connector("repo", kind, endpoint, &HashMap::new(), &HashMap::new())
                        .unwrap_err();
                assert_eq!(error, expected);
                assert!(!format!("{error} {error:?}").contains("canary"));
            }
        }
    }

    // a wrong event in the write state errors
    #[test]
    fn wrong_event_errors() {
        let mut op = CreateConnectorOperation::new(input());
        op.first().unwrap();
        let read = op.next(IoResult::Event(Box::new(Event::Storage(
            StorageEvent::ReadResult {
                key: byteview::ByteView::from(Vec::new()),
                value: None,
            },
        ))));
        assert!(matches!(read, Err(CreateConnectorError::GroupWrite(_))));
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
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
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
