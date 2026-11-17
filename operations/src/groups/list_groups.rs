//! Lists stored groups with a limit and an offset.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::GROUP_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::group::Group;
use aruna_core::types::Key;
use thiserror::Error;

/// Reads one page of groups from the start of the keyspace and skips `offset` of them.
#[derive(Debug, PartialEq)]
pub struct ListGroupOperation {
    limit: usize,
    offset: usize,
}

impl Default for ListGroupOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl ListGroupOperation {
    const DEFAULT_LIMIT: usize = 10_000;

    pub fn new() -> Self {
        Self::with_pagination(Self::DEFAULT_LIMIT, 0)
    }

    pub fn with_pagination(limit: usize, offset: usize) -> Self {
        ListGroupOperation { limit, offset }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum ListGroupError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Creating Group did not finish")]
    NotFinished,
    #[error("Unexpected event: expected {expected}, got {got}")]
    UnexpectedEvent {
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for ListGroupError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent { expected, got },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl Operation for ListGroupOperation {
    type Output = Vec<Group>;
    type Error = ListGroupError;

    fn first(&mut self) -> Result<Next<Vec<Group>>, ListGroupError> {
        Ok(Next::Call(IoCall::Page {
            space: GROUP_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: self.offset.saturating_add(self.limit).max(1),
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<Group>>, ListGroupError> {
        let IoResult::Page { rows, .. } = result else {
            return Err(ListGroupError::UnexpectedEvent {
                expected: "IoResult::Page",
                got: result.label(),
            });
        };
        let groups = (rows.into_iter().skip(self.offset).take(self.limit))
            .map(|(_, value)| Group::from_bytes(&value))
            .collect::<Result<_, _>>()?;
        Ok(Next::Done(groups))
    }
}

#[cfg(test)]
mod test {
    use crate::driver::{DriverContext, drive};
    use crate::groups::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::groups::list_groups::ListGroupOperation;
    use aruna_core::UserId;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_list_group() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let task_handle = TaskHandle::new();

        let context = DriverContext {
            storage_handle,
            blob_handle: None,
            net_handle: Some(net_handle.clone()),
            metadata_handle: None,
            graph_handle: None,
            task_handle: Some(task_handle),
            compute_handle: None,
            outbound_handle: None,
        };

        let mut groups = Vec::new();
        for i in 0..100 {
            let group_config = CreateGroupConfig {
                actor: Actor {
                    user_id: UserId::local(
                        Ulid::generate(),
                        aruna_core::structs::identity::realm::RealmId([0u8; 32]),
                    ),
                    realm_id: aruna_core::structs::identity::realm::RealmId([0u8; 32]),
                    node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
                },
                display_name: format!("Test group {i}"),
                owner_cap: None,
            };
            let group_operation = CreateGroupOperation::new(group_config.clone());
            let create_result = drive(group_operation, &context).await.unwrap();
            groups.push(create_result.0);
        }

        let list_operation = ListGroupOperation::new();
        let mut list_result = drive(list_operation, &context).await.unwrap();
        list_result.sort_by_key(|a| a.group_id);
        groups.sort_by_key(|a| a.group_id);

        assert_eq!(list_result, groups);

        net_handle.shutdown().await;
    }
}
