//! Reports what blocks a group deletion on each realm node, without changing any state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::SystemTime;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::group_delete::{GroupDeleteAction, GroupDeletionError};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::types::{Effects, GroupId, TxnId};
use smallvec::smallvec;

use super::coordinator::{authorize, load_group, request_nodes, visible};
use super::resources::EmptyCheck;
use crate::document_repository::GetConfigOperation;
use crate::driver::{DriverContext, drive};
use crate::forward::authorize::is_sync_eligible;

#[derive(Debug, PartialEq)]
enum State {
    Init,
    Start,
    Scan,
    Close,
    Finish,
}

/// Runs the deletion emptiness scan in a read transaction.
#[derive(Debug, PartialEq)]
pub(super) struct CheckOperation {
    scan: EmptyCheck,
    txn_id: Option<TxnId>,
    state: State,
    result: Option<Result<(), GroupDeletionError>>,
}

impl CheckOperation {
    pub fn new(group_id: GroupId, now: SystemTime) -> Self {
        Self {
            scan: EmptyCheck::new(group_id, now),
            txn_id: None,
            state: State::Init,
            result: None,
        }
    }

    fn fail(&mut self, error: GroupDeletionError) -> Effects {
        self.result = Some(Err(error));
        self.state = State::Finish;
        self.abort()
    }

    fn close(&mut self, txn_id: TxnId, result: Result<(), GroupDeletionError>) -> Effects {
        self.result = Some(result);
        self.state = State::Close;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }
}

impl Operation for CheckOperation {
    type Output = ();
    type Error = GroupDeletionError;

    fn start(&mut self) -> Effects {
        self.state = State::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, self.txn_id, event) {
            (State::Start, None, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = State::Scan;
                self.scan.start(txn_id)
            }
            (State::Scan, Some(txn_id), event) => match self.scan.step(event, txn_id) {
                Ok(Some(effects)) => effects,
                Ok(None) => self.close(txn_id, Ok(())),
                Err(error @ GroupDeletionError::NotEmpty { .. }) => self.close(txn_id, Err(error)),
                Err(error) => self.fail(error),
            },
            (
                State::Close,
                Some(txn_id),
                Event::Storage(StorageEvent::TransactionCommitted { txn_id: done }),
            ) if done == txn_id => {
                self.txn_id = None;
                self.state = State::Finish;
                smallvec![]
            }
            (_, _, Event::Storage(StorageEvent::Error { error })) => self.fail(error.into()),
            _ => self.fail(GroupDeletionError::Unavailable(
                "unexpected deletion check event".into(),
            )),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == State::Finish
    }

    fn finalize(self) -> Result<(), GroupDeletionError> {
        match (self.state, self.result) {
            (State::Finish, Some(result)) => result,
            _ => Err(GroupDeletionError::Unavailable(
                "deletion check did not finish".into(),
            )),
        }
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map(|txn_id| smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })])
            .unwrap_or_default()
    }
}

/// Scans this node and keeps only the bucket names the caller may read; `hidden` counts the
/// others. `more` stays the scan's own flag: more blocking buckets than it named.
pub(super) async fn scan(
    context: &DriverContext,
    auth: &AuthContext,
    group_id: GroupId,
    node: NodeId,
) -> Result<(), GroupDeletionError> {
    match drive(CheckOperation::new(group_id, SystemTime::now()), context).await {
        Err(GroupDeletionError::NotEmpty {
            kinds,
            buckets,
            more,
            hidden,
        }) => {
            let count = buckets.len();
            let names = visible(context, auth, group_id, node, buckets).await;
            Err(GroupDeletionError::NotEmpty {
                kinds,
                more,
                hidden: hidden + (count - names.len()) as u64,
                buckets: names,
            })
        }
        result => result,
    }
}

/// Answers an infrastructure peer's check. Like Prepare, a node that holds the group
/// authorizes the caller again, and a device without the group row still scans.
pub(super) async fn check_node(
    context: &Arc<DriverContext>,
    config: &RealmConfigDocument,
    auth: &AuthContext,
    group_id: GroupId,
    node: NodeId,
) -> Result<(), GroupDeletionError> {
    let (group, record) = load_group(context, group_id).await?;
    if group.is_some() {
        authorize(context, auth, group_id, record.as_ref(), true).await?;
    } else if is_sync_eligible(config, node) {
        return Err(GroupDeletionError::Unavailable(
            "group has not synchronized to this node".into(),
        ));
    }
    scan(context, auth, group_id, node).await
}

/// Asks every realm node what blocks the deletion. A node error is kept in its entry.
pub async fn check_group(
    context: &Arc<DriverContext>,
    auth: AuthContext,
    group_id: GroupId,
) -> Result<Vec<(NodeId, Result<(), GroupDeletionError>)>, GroupDeletionError> {
    let net = context
        .net_handle
        .as_ref()
        .ok_or_else(|| GroupDeletionError::Unavailable("network handle is missing".into()))?;
    if auth.realm_id != *net.realm_id() {
        return Err(GroupDeletionError::Unauthorized);
    }
    let config = drive(GetConfigOperation::new(auth.realm_id), context)
        .await
        .ok()
        .ok_or_else(|| GroupDeletionError::Unavailable("realm configuration is missing".into()))?;
    let (group, record) = load_group(context, group_id).await?;
    authorize(context, &auth, group_id, record.as_ref(), group.is_some()).await?;
    if group.is_none() {
        return Err(GroupDeletionError::NotFound);
    }
    let local = net.node_id();
    // Peers answer a check only from infrastructure nodes, as they do for Prepare.
    if !is_sync_eligible(&config, local) {
        return Err(GroupDeletionError::Unavailable(
            "run the deletion check on a server node".into(),
        ));
    }
    let nodes = config
        .nodes
        .iter()
        .map(|node| node.node_id.parse::<NodeId>())
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|error| GroupDeletionError::Unavailable(error.to_string()))?;
    let mut replies = Vec::new();
    if nodes.contains(&local) {
        replies.push((local, scan(context, &auth, group_id, local).await));
    }
    let remote = nodes
        .into_iter()
        .filter(|node| *node != local)
        .collect::<Vec<_>>();
    let check = || GroupDeleteAction::Check {
        group_id,
        caller: Box::new(auth.clone()),
    };
    let answers = request_nodes(context, &remote, check).await;
    replies.extend(
        answers
            .into_iter()
            .map(|(node, reply)| (node, reply.map(|_| ()))),
    );
    replies.sort_by_key(|(node, _)| *node);
    Ok(replies)
}
