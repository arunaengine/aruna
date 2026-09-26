//! Deletes a repository connector and its secret in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    CONNECTOR_INDEX_KEYSPACE, CONNECTOR_SECRET_KEYSPACE, LINK_CONNECTOR_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, GroupId, TxnId};
use byteview::ByteView;
use smallvec::smallvec;
use ulid::Ulid;

use crate::harvest::repository::{
    connector_key, connector_secret_key, parse_connector_read, read_connector_effect,
};
use crate::harvest::update_connector::UpdateConnectorError;

#[derive(Debug, PartialEq)]
enum State {
    Init,
    Start,
    ReadRecord,
    ReadLinks,
    Delete,
    Commit,
    Aborting,
    Finish,
}

/// Removes a connector and its secret together unless a link uses it; a second delete is not found.
#[derive(Debug, PartialEq)]
pub struct DeleteRepositoryOperation {
    group_id: GroupId,
    connector_id: Ulid,
    state: State,
    txn_id: Option<TxnId>,
    output: Option<Result<(), UpdateConnectorError>>,
}

impl DeleteRepositoryOperation {
    pub fn new(group_id: GroupId, connector_id: Ulid) -> Self {
        Self {
            group_id,
            connector_id,
            state: State::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, error: UpdateConnectorError) -> Effects {
        self.output = Some(Err(error));
        self.abort()
    }
}

impl Operation for DeleteRepositoryOperation {
    type Output = ();
    type Error = UpdateConnectorError;

    fn start(&mut self) -> Effects {
        self.state = State::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.state == State::Aborting {
            self.state = State::Finish;
            return smallvec![];
        }
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match (&self.state, event) {
            (State::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = State::ReadRecord;
                smallvec![read_connector_effect(
                    self.group_id,
                    self.connector_id,
                    Some(txn_id)
                )]
            }
            (State::ReadRecord, event) => match parse_connector_read(event) {
                Ok(Some(_)) => {
                    self.state = State::ReadLinks;
                    smallvec![Effect::Storage(StorageEffect::Iter {
                        key_space: LINK_CONNECTOR_KEYSPACE.to_string(),
                        prefix: Some(ByteView::from(self.connector_id.to_bytes().to_vec())),
                        start: None,
                        limit: 1,
                        txn_id: self.txn_id,
                    })]
                }
                Ok(None) => self.fail(UpdateConnectorError::NotFound),
                Err(_) => self.fail(UpdateConnectorError::Unexpected),
            },
            (State::ReadLinks, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                if !values.is_empty() {
                    return self.fail(UpdateConnectorError::InUse);
                }
                self.state = State::Delete;
                smallvec![Effect::Storage(StorageEffect::BatchDelete {
                    deletes: vec![
                        (
                            CONNECTOR_INDEX_KEYSPACE.to_string(),
                            connector_key(self.group_id, self.connector_id),
                        ),
                        (
                            CONNECTOR_SECRET_KEYSPACE.to_string(),
                            connector_secret_key(self.connector_id),
                        ),
                    ],
                    txn_id: self.txn_id,
                })]
            }
            (State::Delete, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                match self.txn_id {
                    Some(txn_id) => {
                        self.state = State::Commit;
                        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                    }
                    None => self.fail(UpdateConnectorError::Unexpected),
                }
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.state = State::Finish;
                self.output = Some(Ok(()));
                smallvec![]
            }
            _ => self.fail(UpdateConnectorError::Unexpected),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == State::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(UpdateConnectorError::Unexpected))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) if self.state != State::Commit => {
                self.state = State::Aborting;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => {
                self.state = State::Finish;
                smallvec![]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use crate::harvest::read_connector::ListRepositoryOperation;
    use crate::harvest::update_connector::tests::{context, create};

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
