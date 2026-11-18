//! Reports what blocks a group deletion on each realm node, without changing any state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::time::SystemTime;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::group_delete::{
    GroupDeleteAction, GroupDeleteProof, GroupDeletionError,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::GroupId;

use super::access::{AuthorizeDeleteOperation, LoadGroupOperation, LoadedGroup, VisibleOperation};
use super::coordinator::{clock_time, config_call, finished, member_nodes, read_config};
use super::peer::{NodeReplies, RequestNodesOperation};
use super::resources::EmptyCheck;
use crate::realm::peer_trust::is_sync_eligible;

/// Runs the deletion emptiness scan in one read snapshot.
#[derive(Debug, PartialEq)]
pub(super) struct CheckOperation {
    scan: EmptyCheck,
}

impl CheckOperation {
    pub fn new(group_id: GroupId, now: SystemTime) -> Self {
        Self {
            scan: EmptyCheck::new(group_id, now),
        }
    }
}

impl Operation for CheckOperation {
    type Output = ();
    type Error = GroupDeletionError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }

    fn first(&mut self) -> Result<Next<()>, GroupDeletionError> {
        Ok(Next::Call(self.scan.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GroupDeletionError> {
        Ok(match self.scan.take(result)? {
            Some(call) => Next::Call(call),
            None => Next::Done(()),
        })
    }
}

/// A refusal waiting for the bucket names the caller may read.
#[derive(Debug, PartialEq)]
struct Refusal {
    kinds: Vec<String>,
    scanned: usize,
    more: bool,
    hidden: u64,
}

/// Scans this node and keeps only the bucket names the caller may read; `hidden` counts the
/// others. `more` stays the scan's own flag: more blocking buckets than it named.
#[derive(Debug, PartialEq)]
pub(super) struct ScanOperation {
    auth: AuthContext,
    group_id: GroupId,
    node: NodeId,
    now_ms: u64,
    refusal: Option<Refusal>,
}

impl ScanOperation {
    pub(super) fn new(auth: &AuthContext, group_id: GroupId, node: NodeId) -> Self {
        Self {
            auth: auth.clone(),
            group_id,
            node,
            now_ms: 0,
            refusal: None,
        }
    }
}

impl Operation for ScanOperation {
    type Output = ();
    type Error = GroupDeletionError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<()>, GroupDeletionError> {
        let check = CheckOperation::new(self.group_id, clock_time(self.now_ms));
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GroupDeletionError> {
        if let Some(refusal) = self.refusal.take() {
            let names = result.child::<Vec<String>, GroupDeletionError>()??;
            return Err(GroupDeletionError::NotEmpty {
                kinds: refusal.kinds,
                more: refusal.more,
                hidden: refusal.hidden + (refusal.scanned - names.len()) as u64,
                buckets: names,
            });
        }
        match result.child::<(), GroupDeletionError>()? {
            Err(GroupDeletionError::NotEmpty {
                kinds,
                buckets,
                more,
                hidden,
            }) => {
                self.refusal = Some(Refusal {
                    kinds,
                    scanned: buckets.len(),
                    more,
                    hidden,
                });
                let visible = VisibleOperation::new(&self.auth, self.group_id, self.node, buckets);
                Ok(Next::Call(IoCall::child(visible)))
            }
            other => other.map(Next::Done),
        }
    }
}

#[derive(Debug, PartialEq)]
enum NodeStage {
    Load,
    Authorize,
    Scan,
}

/// Answers an infrastructure peer's check. Like Prepare, a node that holds the group
/// authorizes the caller again, and a device without the group row still scans.
#[derive(Debug, PartialEq)]
pub(super) struct CheckNodeOperation {
    config: RealmConfigDocument,
    auth: AuthContext,
    group_id: GroupId,
    node: NodeId,
    stage: NodeStage,
}

impl CheckNodeOperation {
    pub(super) fn new(
        config: &RealmConfigDocument,
        auth: &AuthContext,
        group_id: GroupId,
        node: NodeId,
    ) -> Self {
        Self {
            config: config.clone(),
            auth: auth.clone(),
            group_id,
            node,
            stage: NodeStage::Load,
        }
    }

    fn scan(&mut self) -> Result<Next<()>, GroupDeletionError> {
        self.stage = NodeStage::Scan;
        let scan = ScanOperation::new(&self.auth, self.group_id, self.node);
        Ok(Next::Call(IoCall::child(scan)))
    }
}

impl Operation for CheckNodeOperation {
    type Output = ();
    type Error = GroupDeletionError;

    fn first(&mut self) -> Result<Next<()>, GroupDeletionError> {
        let load = LoadGroupOperation::new(self.group_id);
        Ok(Next::Call(IoCall::child(load)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GroupDeletionError> {
        match self.stage {
            NodeStage::Load => {
                let (group, record) = result.child::<LoadedGroup, GroupDeletionError>()??;
                if group.is_some() {
                    self.stage = NodeStage::Authorize;
                    let check = AuthorizeDeleteOperation::new(
                        &self.auth,
                        self.group_id,
                        record.as_ref(),
                        true,
                    );
                    return Ok(Next::Call(IoCall::child(check)));
                }
                if is_sync_eligible(&self.config, self.node) {
                    return Err(GroupDeletionError::Unavailable(
                        "group has not synchronized to this node".into(),
                    ));
                }
                self.scan()
            }
            NodeStage::Authorize => {
                finished(result)?;
                self.scan()
            }
            NodeStage::Scan => {
                finished(result)?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// What blocks a deletion on each node; a node error stays in that node's entry.
pub type GroupBlockers = Vec<(NodeId, Result<(), GroupDeletionError>)>;

#[derive(Debug, PartialEq)]
enum GroupStage {
    Config,
    Load,
    Authorize { exists: bool },
    Scan,
    Ask,
}

type GroupTurn = Result<Next<GroupBlockers>, GroupDeletionError>;

/// Asks every realm node what blocks the deletion.
#[derive(Debug, PartialEq)]
pub struct CheckGroupOperation {
    local: Option<(NodeId, RealmId)>,
    auth: AuthContext,
    group_id: GroupId,
    stage: GroupStage,
    config: Option<RealmConfigDocument>,
    remote: Vec<NodeId>,
    replies: GroupBlockers,
}

impl CheckGroupOperation {
    pub fn new(local: Option<(NodeId, RealmId)>, auth: AuthContext, group_id: GroupId) -> Self {
        Self {
            local,
            auth,
            group_id,
            stage: GroupStage::Config,
            config: None,
            remote: Vec::new(),
            replies: Vec::new(),
        }
    }

    fn missing(&self) -> GroupDeletionError {
        GroupDeletionError::Unavailable("network handle is missing".into())
    }

    /// Peers answer a check only from infrastructure nodes, as they do for Prepare.
    fn scan_here(&mut self) -> GroupTurn {
        let (local, _) = self.local.ok_or_else(|| self.missing())?;
        let config = self.config.take().ok_or_else(|| self.missing())?;
        if !is_sync_eligible(&config, local) {
            return Err(GroupDeletionError::Unavailable(
                "run the deletion check on a server node".into(),
            ));
        }
        let nodes: BTreeSet<NodeId> = member_nodes(&config)?;
        self.remote = nodes.iter().copied().filter(|node| *node != local).collect();
        if !nodes.contains(&local) {
            return self.ask();
        }
        self.stage = GroupStage::Scan;
        let scan = ScanOperation::new(&self.auth, self.group_id, local);
        Ok(Next::Call(IoCall::child(scan)))
    }

    fn ask(&mut self) -> GroupTurn {
        self.stage = GroupStage::Ask;
        let action = GroupDeleteAction::Check {
            group_id: self.group_id,
            caller: Box::new(self.auth.clone()),
        };
        let nodes = std::mem::take(&mut self.remote);
        let ask = RequestNodesOperation::new(self.local, nodes, action);
        Ok(Next::Call(IoCall::child(ask)))
    }
}

impl Operation for CheckGroupOperation {
    type Output = GroupBlockers;
    type Error = GroupDeletionError;

    fn first(&mut self) -> GroupTurn {
        let (_, realm_id) = self.local.ok_or_else(|| self.missing())?;
        if self.auth.realm_id != realm_id {
            return Err(GroupDeletionError::Unauthorized);
        }
        Ok(Next::Call(config_call(realm_id)))
    }

    fn next(&mut self, result: IoResult) -> GroupTurn {
        match self.stage {
            GroupStage::Config => {
                self.config = Some(read_config(result)?);
                self.stage = GroupStage::Load;
                let load = LoadGroupOperation::new(self.group_id);
                Ok(Next::Call(IoCall::child(load)))
            }
            GroupStage::Load => {
                let (group, record) = result.child::<LoadedGroup, GroupDeletionError>()??;
                let exists = group.is_some();
                self.stage = GroupStage::Authorize { exists };
                let check = AuthorizeDeleteOperation::new(
                    &self.auth,
                    self.group_id,
                    record.as_ref(),
                    exists,
                );
                Ok(Next::Call(IoCall::child(check)))
            }
            GroupStage::Authorize { exists } => {
                finished(result)?;
                match exists {
                    true => self.scan_here(),
                    false => Err(GroupDeletionError::NotFound),
                }
            }
            GroupStage::Scan => {
                let (local, _) = self.local.ok_or_else(|| self.missing())?;
                let scanned = result.child::<(), GroupDeletionError>()?;
                self.replies.push((local, scanned));
                self.ask()
            }
            GroupStage::Ask => {
                let answers = result.child::<NodeReplies, GroupDeletionError>()??;
                let answers = answers.into_iter();
                let answers = answers.map(|(node, reply)| (node, reply.map(drop_proof)));
                self.replies.extend(answers);
                self.replies.sort_by_key(|(node, _)| *node);
                Ok(Next::Done(std::mem::take(&mut self.replies)))
            }
        }
    }
}

fn drop_proof(_: Option<GroupDeleteProof>) {}
