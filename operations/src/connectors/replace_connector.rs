//! Replaces the config of a source connector unless an object version still uses it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::SOURCE_INDEX_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows;
use aruna_core::structs::execution::source_connector::{
    SourceConnector, SourceConnectorKind, SourceConnectorSecret,
};
use aruna_core::types::GroupId;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::reference_prune::schedule_prune;
use crate::connectors::reference_scan::{ReferenceScan, ScanStep};
use crate::connectors::repository::{
    StorageReadError, delete_secret_effect, parse_secret_read, read_secret_effect,
    source_connector_key, write_secret_effect,
};
use crate::connectors::validation::{ValidationError, validate_connector_input};
use crate::storage_read::decode_row;

#[derive(Clone, PartialEq, Eq)]
pub struct ReplaceSourceInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
    pub name: String,
    pub kind: SourceConnectorKind,
    pub public_config: HashMap<String, String>,
    pub secret_config: HashMap<String, String>,
}

/// Shows only the secret keys; the values are live credentials.
impl std::fmt::Debug for ReplaceSourceInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplaceSourceInput")
            .field("group_id", &self.group_id)
            .field("connector_id", &self.connector_id)
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("public_config", &self.public_config)
            .field(
                "secret_keys",
                &self.secret_config.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplaceSourceResult {
    pub connector: SourceConnector,
    pub has_secret_config: bool,
}

/// The call a replacement waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Current,
    Secret,
    Scan(ReferenceScan),
    Records,
    SecretWritten,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReplaceSourceError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    ValidationError(#[from] ValidationError),
    #[error("Connector not found")]
    NotFound,
    #[error("Connector credentials are referenced by object versions")]
    ReferencedObjectVersion,
    #[error("Connector references are still being checked; retry later")]
    ReferencesPending,
    #[error("ReplaceSourceConnector failed")]
    ReplaceConnectorFailed,
}

impl From<IoError> for ReplaceSourceError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

impl From<StorageReadError> for ReplaceSourceError {
    fn from(value: StorageReadError) -> Self {
        match value {
            StorageReadError::Storage(error) => Self::StorageError(error),
            StorageReadError::Conversion(error) => Self::ConversionError(error),
        }
    }
}

/// Replaces a connector in one transaction. Its output is `None` while the reference check is
/// pending: the stale page and the pruning queue row commit, and the caller retries later.
#[derive(Debug, PartialEq)]
pub struct ReplaceSourceOperation {
    input: ReplaceSourceInput,
    stage: Stage,
    replacement: Option<SourceConnector>,
    replacement_secret: Option<SourceConnectorSecret>,
}

type Replaced = Result<Next<Option<ReplaceSourceResult>>, ReplaceSourceError>;

impl ReplaceSourceOperation {
    pub fn new(input: ReplaceSourceInput) -> Self {
        Self {
            input,
            stage: Stage::Current,
            replacement: None,
            replacement_secret: None,
        }
    }

    fn prepare_replacement(&mut self, existing: SourceConnector) {
        let now = SystemTime::now();
        self.replacement = Some(
            SourceConnector::new(
                existing.connector_id,
                existing.group_id,
                self.input.name.clone(),
                self.input.kind,
                self.input.public_config.clone(),
                existing.created_at,
                now,
                existing.created_by,
            )
            .with_namespace(&self.input.secret_config),
        );
        self.replacement_secret = SourceConnectorSecret::new(
            existing.connector_id,
            self.input.secret_config.clone(),
            now,
        );
    }

    fn write_records(&mut self) -> Replaced {
        let replacement =
            (self.replacement.as_ref()).ok_or(ReplaceSourceError::ReplaceConnectorFailed)?;
        let row = (
            SOURCE_INDEX_KEYSPACE.to_string(),
            source_connector_key(replacement.group_id, replacement.connector_id),
            replacement.to_bytes()?.into(),
        );
        self.stage = Stage::Records;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn scanned(&mut self, step: ScanStep) -> Replaced {
        match step {
            ScanStep::Referenced => Err(ReplaceSourceError::ReferencedObjectVersion),
            ScanStep::Next(call) => Ok(Next::Call(call)),
            ScanStep::Deferred => Ok(Next::Then {
                output: None,
                calls: vec![schedule_prune().into()],
            }),
            ScanStep::Complete => self.write_records(),
        }
    }

    /// A replacement with secrets writes them; one without removes the old record.
    fn secret_written(&mut self, event: Event) -> Replaced {
        let written = matches!(
            (&self.replacement_secret, &event),
            (Some(_), Event::Storage(StorageEvent::WriteResult { .. }))
                | (None, Event::Storage(StorageEvent::DeleteResult { .. }))
        );
        if !written {
            let expected = "StorageEvent::WriteResult or DeleteResult";
            return Err(IoError::Unexpected {
                expected,
                got: event.label(),
            }
            .into());
        }
        let connector =
            (self.replacement.take()).ok_or(ReplaceSourceError::ReplaceConnectorFailed)?;
        Ok(Next::Done(Some(ReplaceSourceResult {
            connector,
            has_secret_config: self.replacement_secret.is_some(),
        })))
    }
}

fn secret_config_changed(
    current: Option<&SourceConnectorSecret>,
    replacement: Option<&SourceConnectorSecret>,
) -> bool {
    current.map(|secret| &secret.secret_config) != replacement.map(|secret| &secret.secret_config)
}

impl Operation for ReplaceSourceOperation {
    type Output = Option<ReplaceSourceResult>;
    type Error = ReplaceSourceError;

    fn first(&mut self) -> Replaced {
        validate_connector_input(
            &self.input.name,
            self.input.kind,
            &self.input.public_config,
            &self.input.secret_config,
        )?;
        Ok(Next::Call(rows::read_row(
            SOURCE_INDEX_KEYSPACE,
            source_connector_key(self.input.group_id, self.input.connector_id),
        )))
    }

    fn next(&mut self, result: IoResult) -> Replaced {
        let connector_id = self.input.connector_id;
        match &mut self.stage {
            Stage::Current => {
                let existing = decode_row(rows::value(result)?, SourceConnector::from_bytes)?;
                self.prepare_replacement(existing.ok_or(ReplaceSourceError::NotFound)?);
                self.stage = Stage::Secret;
                Ok(Next::Call(read_secret_effect(connector_id, None).into()))
            }
            Stage::Secret => {
                let current = parse_secret_read(rows::event(result)?, connector_id)?;
                if !secret_config_changed(current.as_ref(), self.replacement_secret.as_ref()) {
                    return self.write_records();
                }
                let mut scan = ReferenceScan::new(connector_id);
                let call = scan.start();
                self.stage = Stage::Scan(scan);
                Ok(Next::Call(call))
            }
            Stage::Scan(scan) => {
                let step = scan.step(result)?;
                self.scanned(step)
            }
            Stage::Records => {
                rows::done(result)?;
                let effect = match self.replacement_secret.as_ref() {
                    Some(secret) => write_secret_effect(secret, None)?,
                    None => delete_secret_effect(connector_id, None),
                };
                self.stage = Stage::SecretWritten;
                Ok(Next::Call(effect.into()))
            }
            Stage::SecretWritten => self.secret_written(rows::event(result)?),
        }
    }

    fn transaction(&self) -> aruna_core::operation::TransactionMode {
        aruna_core::operation::TransactionMode::Write
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::create_connector::{SourceConnectorInput, SourceConnectorOperation};
    use crate::connectors::descriptor::build_source_binding;
    use crate::connectors::resolver::{ResolveBindingInput, ResolveBindingOperation};
    use crate::driver::drive;
    use crate::tests::connectors::{
        create_connector, defers_pending, reference_rows, source_metadata, vault_context,
        write_reference,
    };
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::structs::execution::source_access::ResolvedSourceAccess;
    use aruna_core::structs::execution::staging::StagingStrategy;
    use aruna_core::structs::storage::blob::BlobVersion;

    fn connector_secret(connector_id: Ulid, user: &str) -> SourceConnectorSecret {
        SourceConnectorSecret::new(
            connector_id,
            HashMap::from([
                ("username".to_string(), user.to_string()),
                ("password".to_string(), "secret".to_string()),
            ]),
            SystemTime::UNIX_EPOCH,
        )
        .unwrap()
    }

    fn replacement_connector(group_id: Ulid, connector_id: Ulid) -> SourceConnector {
        SourceConnector::new(
            connector_id,
            group_id,
            "new".to_string(),
            SourceConnectorKind::Webdav,
            HashMap::from([(
                "endpoint".to_string(),
                "https://dav.example.org".to_string(),
            )]),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            Default::default(),
        )
    }

    fn reference_blob_version(connector_id: Ulid) -> BlobVersion {
        let connector = replacement_connector(Ulid::generate(), connector_id);
        let source = build_source_binding(
            StagingStrategy::Reference,
            &connector,
            &source_metadata(),
            "run-1/data.txt".to_string(),
            None,
            Some(connector_id),
        );

        BlobVersion::reference(
            source,
            source_metadata(),
            SystemTime::UNIX_EPOCH,
            Default::default(),
            SystemTime::UNIX_EPOCH,
        )
    }

    fn event(event: Event) -> IoResult {
        IoResult::Event(Box::new(event))
    }

    /// An operation past its connector read that meets the stored `alice` secret while it
    /// replaces it with `bob`.
    fn reading_secret(connector_id: Ulid) -> (ReplaceSourceOperation, IoResult) {
        let group_id = Ulid::from_bytes([1u8; 16]);
        let replacement = replacement_connector(group_id, connector_id);
        let mut operation = ReplaceSourceOperation::new(ReplaceSourceInput {
            group_id,
            connector_id,
            name: "new".to_string(),
            kind: SourceConnectorKind::Webdav,
            public_config: replacement.public_config.clone(),
            secret_config: connector_secret(connector_id, "bob").secret_config,
        });
        assert_eq!(
            operation.transaction(),
            aruna_core::operation::TransactionMode::Write
        );
        operation.first().unwrap();
        let stored = IoResult::Values(vec![Some(replacement.to_bytes().unwrap().into())]);
        let read = operation.next(stored);
        assert!(matches!(read, Ok(Next::Call(IoCall::Effect(_)))));
        let secret = event(Event::Storage(StorageEvent::VaultResult {
            entry: SourceConnectorSecret::vault_entry(connector_id),
            secret: Some(connector_secret(connector_id, "alice").to_secret().unwrap()),
        }));
        (operation, secret)
    }

    /// An operation inside its transaction with the reference scan started.
    fn scanning(connector_id: Ulid) -> ReplaceSourceOperation {
        let (mut operation, secret) = reading_secret(connector_id);
        let scan = operation.next(secret);
        assert!(matches!(scan, Ok(Next::Call(IoCall::Page { .. }))));
        operation
    }

    fn version_read(version: Option<BlobVersion>) -> IoResult {
        IoResult::Values(vec![
            version.map(|version| version.to_bytes().unwrap().into()),
        ])
    }

    #[test]
    fn scan_then_write() {
        let connector_id = Ulid::from_bytes([2u8; 16]);
        let mut operation = scanning(connector_id);
        let empty = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert!(matches!(
            operation.next(empty),
            Ok(Next::Call(IoCall::Write(rows))) if rows.len() == 1
        ));
        let next = operation.next(IoResult::Done);
        let Ok(Next::Call(IoCall::Effect(secret))) = &next else {
            panic!("expected the secret write");
        };
        assert!(matches!(
            **secret,
            Effect::Storage(StorageEffect::VaultWrite { entry, txn_id: None, .. })
                if entry.id == connector_id
        ));
        let rendered = format!("{next:?} {operation:?}");
        assert!(!rendered.contains("bob"), "{rendered}");
        let written = event(Event::Storage(StorageEvent::WriteResult {
            key: vec![].into(),
        }));
        let Ok(Next::Done(Some(result))) = operation.next(written) else {
            panic!("expected the replaced connector");
        };
        assert!(result.has_secret_config);
    }

    #[test]
    fn reference_refuses_replace() {
        let connector_id = Ulid::from_bytes([2u8; 16]);
        let mut operation = scanning(connector_id);
        let reads = operation.next(reference_rows(connector_id, None));
        assert!(matches!(reads, Ok(Next::Call(IoCall::Read(_)))));
        let live = version_read(Some(reference_blob_version(connector_id)));
        assert_eq!(
            operation.next(live),
            Err(ReplaceSourceError::ReferencedObjectVersion)
        );
    }

    #[test]
    fn pending_defers_check() {
        let connector_id = Ulid::from_bytes([2u8; 16]);
        defers_pending(scanning(connector_id), connector_id);
    }

    #[test]
    fn scan_failure_fails() {
        let mut operation = scanning(Ulid::from_bytes([2u8; 16]));
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(ReplaceSourceError::StorageError(StorageError::ReadError(_)))
        ));
    }

    #[tokio::test]
    async fn removes_secret_config() {
        let (_tempdir, context) = vault_context();
        let created = drive(
            SourceConnectorOperation::new(SourceConnectorInput {
                group_id: ulid::Ulid::generate(),
                created_by: Default::default(),
                name: "old".to_string(),
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                secret_config: HashMap::from([("token".to_string(), "secret".to_string())]),
            }),
            &context,
        )
        .await
        .unwrap();

        let replaced = drive(
            ReplaceSourceOperation::new(ReplaceSourceInput {
                group_id: created.connector.group_id,
                connector_id: created.connector.connector_id,
                name: "new".to_string(),
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org/v2".to_string(),
                )]),
                secret_config: HashMap::new(),
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(replaced.connector.name, "new");
        assert!(!replaced.has_secret_config);
    }

    #[tokio::test]
    async fn secret_change_rejected() {
        let (_tempdir, context) = vault_context();
        let connector = create_connector(&context).await;
        let source = build_source_binding(
            StagingStrategy::Reference,
            &connector,
            &source_metadata(),
            "run-1/data.txt".to_string(),
            None,
            Some(connector.connector_id),
        );
        write_reference(&context, source).await;

        let result = drive(
            ReplaceSourceOperation::new(ReplaceSourceInput {
                group_id: connector.group_id,
                connector_id: connector.connector_id,
                name: "new".to_string(),
                kind: SourceConnectorKind::Webdav,
                public_config: connector.public_config.clone(),
                secret_config: HashMap::from([
                    ("username".to_string(), "bob".to_string()),
                    ("password".to_string(), "secret".to_string()),
                ]),
            }),
            &context,
        )
        .await;

        assert_eq!(result, Err(ReplaceSourceError::ReferencedObjectVersion));
    }

    #[tokio::test]
    async fn public_change_allowed() {
        let (_tempdir, context) = vault_context();
        let connector = create_connector(&context).await;
        let source = build_source_binding(
            StagingStrategy::Reference,
            &connector,
            &source_metadata(),
            "run-1/data.txt".to_string(),
            None,
            Some(connector.connector_id),
        );
        write_reference(&context, source.clone()).await;

        let replaced = drive(
            ReplaceSourceOperation::new(ReplaceSourceInput {
                group_id: connector.group_id,
                connector_id: connector.connector_id,
                name: "new".to_string(),
                kind: SourceConnectorKind::Webdav,
                public_config: HashMap::from([
                    (
                        "endpoint".to_string(),
                        "https://dav.example.org/v2".to_string(),
                    ),
                    ("root".to_string(), "/datasets-v2".to_string()),
                ]),
                secret_config: HashMap::from([
                    ("username".to_string(), "alice".to_string()),
                    ("password".to_string(), "secret".to_string()),
                ]),
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(replaced.connector.name, "new");

        let access = drive(
            ResolveBindingOperation::new(ResolveBindingInput {
                source,
                group_id: None,
            }),
            &context,
        )
        .await
        .unwrap();
        let ResolvedSourceAccess::OpenDal { config, .. } = access;
        assert_eq!(
            config.get("endpoint").map(String::as_str),
            Some("https://dav.example.org")
        );
        assert_eq!(config.get("root").map(String::as_str), Some("/datasets"));
        assert_eq!(config.get("username").map(String::as_str), Some("alice"));
    }
}
