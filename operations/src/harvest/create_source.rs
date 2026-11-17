//! Creates or reuses a harvest source on a connector with its selector, target prefix and schedule.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{CONNECTOR_INDEX_KEYSPACE, HARVEST_SOURCE_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows;
use aruna_core::structs::execution::harvest::{
    HarvestSelector, HarvestSource, RepositoryConnector, RepositoryConnectorKind,
};
use aruna_core::structs::identity::group_delete::GroupWriteError;
use aruna_core::types::{GroupId, Key, Value};
use thiserror::Error;
use ulid::Ulid;

use crate::groups::fence::{group_write_result, write_group_new};
use crate::harvest::oai_pmh::request::{normalize_metadata_prefix, normalize_set};
use crate::harvest::repository::{connector_key, source_key};
use crate::harvest::target_path::{normalize_target_prefix, prefix_is_blank};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSourceInput {
    pub group_id: GroupId,
    pub created_by: UserId,
    pub connector_id: Ulid,
    pub namespace: String,
    pub target_prefix: String,
    pub selector: HarvestSelector,
    pub schedule_interval_ms: Option<u64>,
}

/// The call a source creation waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Read,
    Write,
    /// A concurrent first submission created the source; its row is read again.
    Reread,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateSourceError {
    #[error(transparent)]
    GroupWrite(#[from] GroupWriteError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("harvest namespace must not be empty")]
    EmptyNamespace,
    #[error("harvest target prefix must not be empty")]
    EmptyTargetPrefix,
    #[error("harvest target prefix [{0}] leaves no room for a record identifier")]
    LongTargetPrefix(String),
    #[error("unsupported harvest metadata prefix [{0}]: only [oai_dc] is mapped")]
    UnsupportedMetadataPrefix(String),
    #[error("repository connector not found")]
    ConnectorNotFound,
    #[error("harvest requires an OAI-PMH repository connector")]
    UnsupportedConnector,
    #[error("harvest source [{0}] already exists with another target prefix or selector")]
    SourceConflict(Ulid),
    #[error("CreateHarvestSource failed")]
    Failed,
}

impl From<IoError> for CreateSourceError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

#[derive(Debug, PartialEq)]
pub struct CreateSourceOperation {
    input: CreateSourceInput,
    stage: Stage,
    source: Option<HarvestSource>,
}

/// One source per connector and namespace, so a repeated submission keeps its cursor.
pub fn source_id(connector_id: Ulid, namespace: &str) -> Ulid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"aruna-harvest-source");
    hasher.update(&connector_id.to_bytes());
    hasher.update(namespace.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Ulid::from_bytes(bytes)
}

impl CreateSourceOperation {
    pub fn new(input: CreateSourceInput) -> Self {
        Self {
            input,
            stage: Stage::Read,
            source: None,
        }
    }

    fn source_row(&self) -> (String, Key) {
        let source_id = source_id(self.input.connector_id, &self.input.namespace);
        (
            HARVEST_SOURCE_KEYSPACE.to_string(),
            source_key(self.input.group_id, source_id),
        )
    }

    /// Validate and canonicalize the input, so only values the request builder
    /// and the path encoder can use are ever stored.
    fn normalize(&mut self) -> Result<(), CreateSourceError> {
        if self.input.namespace.trim().is_empty() {
            return Err(CreateSourceError::EmptyNamespace);
        }
        let Some(target_prefix) = normalize_target_prefix(&self.input.target_prefix) else {
            return Err(if prefix_is_blank(&self.input.target_prefix) {
                CreateSourceError::EmptyTargetPrefix
            } else {
                CreateSourceError::LongTargetPrefix(self.input.target_prefix.clone())
            });
        };
        // The parser and the RO-Crate mapper are `oai_dc`-typed; any other
        // prefix would silently import identifier-only records.
        let metadata_prefix =
            normalize_metadata_prefix(self.input.selector.metadata_prefix.as_deref())
                .map_err(CreateSourceError::UnsupportedMetadataPrefix)?;
        self.input.target_prefix = target_prefix;
        self.input.selector.metadata_prefix = Some(metadata_prefix);
        self.input.selector.set = normalize_set(self.input.selector.set.as_deref());
        Ok(())
    }

    /// Returns a stored source of the same definition, or creates it unless it was just taken.
    fn decide(
        &mut self,
        existing: Option<Value>,
        reread: bool,
    ) -> Result<Next<HarvestSource>, CreateSourceError> {
        let existing = existing.map(|bytes| HarvestSource::from_bytes(&bytes));
        if let Some(existing) = existing.transpose()? {
            if existing.connector_id != self.input.connector_id
                || existing.target_prefix != self.input.target_prefix
                || existing.selector != self.input.selector
            {
                return Err(CreateSourceError::SourceConflict(existing.source_id));
            }
            return Ok(Next::Done(existing));
        }
        if reread {
            return Err(CreateSourceError::Failed);
        }
        let source = HarvestSource::new(
            source_id(self.input.connector_id, &self.input.namespace),
            self.input.group_id,
            self.input.connector_id,
            self.input.namespace.clone(),
            self.input.target_prefix.clone(),
            self.input.selector.clone(),
            self.input.schedule_interval_ms,
            SystemTime::now(),
            self.input.created_by,
        );
        let (keyspace, key) = self.source_row();
        let row = (keyspace.clone(), key.clone(), source.to_bytes()?.into());
        let write = write_group_new(self.input.group_id, vec![row], None, (keyspace, key));
        self.source = Some(source);
        self.stage = Stage::Write;
        Ok(Next::Call(write.into()))
    }
}

impl Operation for CreateSourceOperation {
    type Output = HarvestSource;
    type Error = CreateSourceError;

    fn first(&mut self) -> Result<Next<Self::Output>, CreateSourceError> {
        self.normalize()?;
        let connector = (
            CONNECTOR_INDEX_KEYSPACE.to_string(),
            connector_key(self.input.group_id, self.input.connector_id),
        );
        Ok(Next::Call(IoCall::Read(vec![connector, self.source_row()])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CreateSourceError> {
        match self.stage {
            Stage::Read => {
                let [connector, existing] = rows::values::<2>(result)?;
                let connector = connector.ok_or(CreateSourceError::ConnectorNotFound)?;
                if RepositoryConnector::from_bytes(&connector)?.kind
                    != RepositoryConnectorKind::OaiPmh
                {
                    return Err(CreateSourceError::UnsupportedConnector);
                }
                self.decide(existing, false)
            }
            Stage::Write => match group_write_result(rows::event(result)?) {
                Ok(()) => (self.source.take().map(Next::Done)).ok_or(CreateSourceError::Failed),
                Err(GroupWriteError::Taken) => {
                    self.stage = Stage::Reread;
                    Ok(Next::Call(IoCall::Read(vec![self.source_row()])))
                }
                Err(error) => Err(error.into()),
            },
            Stage::Reread => self.decide(rows::value(result)?, true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::harvest::create_connector::{CreateConnectorInput, CreateConnectorOperation};
    use crate::harvest::repository::{parse_source_read, read_source_effect};
    use crate::harvest::target_path::{DIGEST_SEGMENT_BYTES, HARVEST_PATH_BYTES};
    use aruna_core::events::{Event, SubOperationEvent};
    use aruna_core::handle::Handle;
    use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
    use aruna_storage::storage;
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn input(group: GroupId, connector: Ulid) -> CreateSourceInput {
        CreateSourceInput {
            group_id: group,
            created_by: Default::default(),
            connector_id: connector,
            namespace: "zenodo".to_string(),
            target_prefix: "imported/zenodo".to_string(),
            selector: HarvestSelector::default(),
            schedule_interval_ms: Some(3_600_000),
        }
    }

    // an empty namespace errors before any read
    #[test]
    fn empty_namespace_errors() {
        let mut op = CreateSourceOperation::new(CreateSourceInput {
            namespace: " ".to_string(),
            ..input(ulid::Ulid::generate(), ulid::Ulid::generate())
        });
        assert_eq!(op.first(), Err(CreateSourceError::EmptyNamespace));
    }

    #[test]
    fn foreign_prefix_rejected() {
        for prefix in ["datacite", "marc21", "oai_datacite"] {
            let mut op = CreateSourceOperation::new(CreateSourceInput {
                selector: HarvestSelector {
                    set: None,
                    metadata_prefix: Some(prefix.to_string()),
                },
                ..input(ulid::Ulid::generate(), ulid::Ulid::generate())
            });
            assert_eq!(
                op.first(),
                Err(CreateSourceError::UnsupportedMetadataPrefix(
                    prefix.to_string()
                ))
            );
        }
    }

    /// Omitted, empty, whitespace, padded and exact prefixes are one source
    /// configuration, stored in exactly one form.
    #[test]
    fn omitted_prefix_agrees() {
        for prefix in [
            None,
            Some("oai_dc".to_string()),
            Some(String::new()),
            Some("   ".to_string()),
            Some(" oai_dc ".to_string()),
        ] {
            let mut op = CreateSourceOperation::new(CreateSourceInput {
                selector: HarvestSelector {
                    set: Some("  ".to_string()),
                    metadata_prefix: prefix,
                },
                ..input(ulid::Ulid::generate(), ulid::Ulid::generate())
            });
            assert!(matches!(op.first(), Ok(Next::Call(_))));
            assert_eq!(op.input.selector.metadata_prefix.as_deref(), Some("oai_dc"));
            assert_eq!(op.input.selector.set, None);
        }
    }

    #[test]
    fn prefix_stored_canonical() {
        let mut op = CreateSourceOperation::new(CreateSourceInput {
            target_prefix: "  /imported/zenodo/  ".to_string(),
            ..input(ulid::Ulid::generate(), ulid::Ulid::generate())
        });
        assert!(matches!(op.first(), Ok(Next::Call(_))));
        assert_eq!(op.input.target_prefix, "imported/zenodo");
    }

    /// A prefix one byte past the path budget makes every record fail later, so
    /// it is refused at creation; the longest usable prefix is still accepted.
    #[test]
    fn prefix_budget_enforced() {
        let longest = HARVEST_PATH_BYTES - DIGEST_SEGMENT_BYTES - 1;
        let mut accepted = CreateSourceOperation::new(CreateSourceInput {
            target_prefix: "p".repeat(longest),
            ..input(ulid::Ulid::generate(), ulid::Ulid::generate())
        });
        assert!(matches!(accepted.first(), Ok(Next::Call(_))));

        let too_long = "p".repeat(longest + 1);
        let mut refused = CreateSourceOperation::new(CreateSourceInput {
            target_prefix: too_long.clone(),
            ..input(ulid::Ulid::generate(), ulid::Ulid::generate())
        });
        assert_eq!(
            refused.first(),
            Err(CreateSourceError::LongTargetPrefix(too_long))
        );
    }

    #[test]
    fn blank_prefix_rejected() {
        for prefix in ["", " ", "/", "  //  "] {
            let mut op = CreateSourceOperation::new(CreateSourceInput {
                target_prefix: prefix.to_string(),
                ..input(ulid::Ulid::generate(), ulid::Ulid::generate())
            });
            assert_eq!(op.first(), Err(CreateSourceError::EmptyTargetPrefix));
        }
    }

    // a missing connector errors as not found
    #[test]
    fn missing_connector_errors() {
        let mut op =
            CreateSourceOperation::new(input(ulid::Ulid::generate(), ulid::Ulid::generate()));
        op.first().unwrap();
        assert_eq!(
            op.next(IoResult::Values(vec![None, None])),
            Err(CreateSourceError::ConnectorNotFound)
        );
    }

    // the source persists once the connector exists
    #[tokio::test]
    async fn persists_after_connector() {
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

        let connector = drive(
            CreateConnectorOperation::new(CreateConnectorInput {
                group_id: ulid::Ulid::generate(),
                created_by: Default::default(),
                name: "zenodo".to_string(),
                kind: RepositoryConnectorKind::OaiPmh,
                endpoint: "https://zenodo.org/oai2d".to_string(),
                public_config: HashMap::new(),
                secret_config: HashMap::new(),
            }),
            &context,
        )
        .await
        .unwrap()
        .connector;

        let source = drive(
            CreateSourceOperation::new(CreateSourceInput {
                target_prefix: " /imported/zenodo/ ".to_string(),
                selector: HarvestSelector {
                    set: Some(" ".to_string()),
                    metadata_prefix: Some(" oai_dc ".to_string()),
                },
                ..input(connector.group_id, connector.connector_id)
            }),
            &context,
        )
        .await
        .unwrap();

        let stored = context
            .storage_handle
            .send_effect(read_source_effect(source.group_id, source.source_id, None))
            .await;
        let stored = parse_source_read(stored).unwrap().unwrap();
        assert_eq!(stored, source);
        assert_eq!(stored.target_prefix, "imported/zenodo");
        assert_eq!(stored.selector.metadata_prefix.as_deref(), Some("oai_dc"));
        assert_eq!(stored.selector.set, None);

        // The same definition returns the stored source with its cursor.
        let again = drive(
            CreateSourceOperation::new(CreateSourceInput {
                target_prefix: "imported/zenodo".to_string(),
                ..input(connector.group_id, connector.connector_id)
            }),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(again, source);

        let changed = drive(
            CreateSourceOperation::new(CreateSourceInput {
                target_prefix: "imported/other".to_string(),
                ..input(connector.group_id, connector.connector_id)
            }),
            &context,
        )
        .await
        .unwrap_err();
        assert_eq!(changed, CreateSourceError::SourceConflict(source.source_id));
    }

    #[test]
    fn invenio_connector_refused() {
        let group = ulid::Ulid::generate();
        let connector = RepositoryConnector::new(
            ulid::Ulid::generate(),
            group,
            "zenodo".to_string(),
            RepositoryConnectorKind::Invenio,
            "https://zenodo.org/api/".to_string(),
            HashMap::new(),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            Default::default(),
        );
        let mut op = CreateSourceOperation::new(input(group, connector.connector_id));
        op.first().unwrap();
        let stored = Some(connector.to_bytes().unwrap().into());
        assert_eq!(
            op.next(IoResult::Values(vec![stored, None])),
            Err(CreateSourceError::UnsupportedConnector)
        );
    }

    #[test]
    fn taken_source_reread() {
        // A concurrent first submission wins the create; this one rereads and compares.
        let group = ulid::Ulid::generate();
        let connector = RepositoryConnector::new(
            ulid::Ulid::generate(),
            group,
            "zenodo".to_string(),
            RepositoryConnectorKind::OaiPmh,
            "https://zenodo.org/oai2d".to_string(),
            HashMap::new(),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            Default::default(),
        );
        let taken = || {
            IoResult::Event(Box::new(Event::SubOperation(
                SubOperationEvent::GroupWritten {
                    result: Err(GroupWriteError::Taken),
                },
            )))
        };
        for (prefix, reused) in [("imported/zenodo", true), ("imported/other", false)] {
            let mut op = CreateSourceOperation::new(input(group, connector.connector_id));
            op.first().unwrap();
            let stored = Some(connector.to_bytes().unwrap().into());
            let next = op.next(IoResult::Values(vec![stored, None]));
            assert!(
                matches!(next, Ok(Next::Call(IoCall::Child(_)))),
                "the create is conditional"
            );
            let mut winner = op.source.clone().unwrap();
            winner.target_prefix = prefix.to_string();
            let next = op.next(taken());
            assert!(
                matches!(next, Ok(Next::Call(IoCall::Read(_)))),
                "the source is read again"
            );
            let reread = op.next(IoResult::Values(vec![Some(
                winner.to_bytes().unwrap().into(),
            )]));
            match reused {
                true => assert_eq!(reread, Ok(Next::Done(winner))),
                false => assert_eq!(
                    reread,
                    Err(CreateSourceError::SourceConflict(winner.source_id))
                ),
            }
        }
    }

    #[test]
    fn source_id_stable() {
        let connector = ulid::Ulid::from_bytes([7; 16]);
        assert_eq!(
            source_id(connector, "zenodo"),
            source_id(connector, "zenodo")
        );
        assert_ne!(
            source_id(connector, "zenodo"),
            source_id(connector, "dryad")
        );
        let other = ulid::Ulid::from_bytes([8; 16]);
        assert_ne!(source_id(connector, "zenodo"), source_id(other, "zenodo"));
    }
}
