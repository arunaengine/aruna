//! Deletes a repository connector and its secret in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{
    CONNECTOR_INDEX_KEYSPACE, CONNECTOR_SECRET_KEYSPACE, LINK_CONNECTOR_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows;
use aruna_core::structs::execution::harvest::RepositoryConnector;
use aruna_core::types::GroupId;
use byteview::ByteView;
use ulid::Ulid;

use crate::harvest::repository::{connector_key, connector_secret_key};
use crate::harvest::update_connector::UpdateConnectorError;

/// The call a delete waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Record,
    Links,
    Delete,
}

/// Removes a connector and its secret together unless a link uses it; a second delete is not found.
#[derive(Debug, PartialEq)]
pub struct DeleteRepositoryOperation {
    group_id: GroupId,
    connector_id: Ulid,
    stage: Stage,
}

impl DeleteRepositoryOperation {
    pub fn new(group_id: GroupId, connector_id: Ulid) -> Self {
        Self {
            group_id,
            connector_id,
            stage: Stage::Record,
        }
    }
}

impl Operation for DeleteRepositoryOperation {
    type Output = ();
    type Error = UpdateConnectorError;

    fn first(&mut self) -> Result<Next<()>, UpdateConnectorError> {
        Ok(Next::Call(rows::read_row(
            CONNECTOR_INDEX_KEYSPACE,
            connector_key(self.group_id, self.connector_id),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, UpdateConnectorError> {
        match self.stage {
            Stage::Record => {
                let record = rows::value(result)?.ok_or(UpdateConnectorError::NotFound)?;
                RepositoryConnector::from_bytes(&record)
                    .map_err(|_| UpdateConnectorError::Unexpected)?;
                self.stage = Stage::Links;
                Ok(Next::Call(IoCall::Page {
                    space: LINK_CONNECTOR_KEYSPACE.to_string(),
                    prefix: ByteView::from(self.connector_id.to_bytes().to_vec()),
                    after: None,
                    limit: 1,
                }))
            }
            Stage::Links => {
                if !rows::page(result)?.0.is_empty() {
                    return Err(UpdateConnectorError::InUse);
                }
                self.stage = Stage::Delete;
                Ok(Next::Call(IoCall::Delete(vec![
                    (
                        CONNECTOR_INDEX_KEYSPACE.to_string(),
                        connector_key(self.group_id, self.connector_id),
                    ),
                    (
                        CONNECTOR_SECRET_KEYSPACE.to_string(),
                        connector_secret_key(self.connector_id),
                    ),
                ])))
            }
            Stage::Delete => {
                rows::done(result)?;
                Ok(Next::Done(()))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use crate::harvest::read_connector::ListRepositoryOperation;
    use crate::harvest::update_connector::tests::{context, create};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};

    #[tokio::test]
    async fn delete_scoped_group() {
        let (_dir, context) = context();
        let group_id = Ulid::generate();
        let connector = create(&context, group_id).await;
        let other = drive(
            DeleteRepositoryOperation::new(Ulid::generate(), connector.connector_id),
            &context,
        )
        .await;
        assert_eq!(other.unwrap_err(), UpdateConnectorError::NotFound);

        drive(
            DeleteRepositoryOperation::new(group_id, connector.connector_id),
            &context,
        )
        .await
        .unwrap();
        let listed = drive(ListRepositoryOperation::new(group_id), &context)
            .await
            .unwrap();
        assert!(listed.is_empty());
        let again = drive(
            DeleteRepositoryOperation::new(group_id, connector.connector_id),
            &context,
        )
        .await;
        assert_eq!(again.unwrap_err(), UpdateConnectorError::NotFound);
    }

    #[tokio::test]
    async fn keeps_linked_connector() {
        let (_dir, context) = context();
        let group_id = Ulid::generate();
        let connector = create(&context, group_id).await.connector_id;
        let key = aruna_core::repository::connector_link_key(connector, Ulid::generate());
        let written = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: LINK_CONNECTOR_KEYSPACE.to_string(),
                key: ByteView::from(key.clone()),
                value: ByteView::from(Ulid::generate().to_bytes().to_vec()),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            written,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        let refused = drive(
            DeleteRepositoryOperation::new(group_id, connector),
            &context,
        )
        .await;
        assert_eq!(refused.unwrap_err(), UpdateConnectorError::InUse);
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Delete {
                key_space: LINK_CONNECTOR_KEYSPACE.to_string(),
                key: ByteView::from(key),
                txn_id: None,
            })
            .await;
        drive(
            DeleteRepositoryOperation::new(group_id, connector),
            &context,
        )
        .await
        .unwrap();
    }
}
