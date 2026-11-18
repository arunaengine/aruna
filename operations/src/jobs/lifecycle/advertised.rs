//! Reads the node advertisements of a realm's members.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::node_state::{NodeInfoError, ReadInfoOperation};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::node_info::NodeInfoDocument;
use tracing::warn;

/// The documents read, and whether a read failed.
pub(super) type Advertised = (BTreeMap<NodeId, NodeInfoDocument>, bool);

/// Node-info documents of every sync-eligible realm member that has one. An unread
/// advertisement may hold the only legal target, so a caller must not conclude from a partial
/// read.
#[derive(Debug, PartialEq)]
pub(super) struct AdvertisementsOperation {
    /// Members still to read, the next one last.
    members: Vec<NodeId>,
    asked: Option<NodeId>,
    documents: BTreeMap<NodeId, NodeInfoDocument>,
    unread: bool,
}

type AdvertisedNext = Result<Next<Advertised>, IoError>;

impl AdvertisementsOperation {
    pub(super) fn new(config: &RealmConfigDocument) -> Self {
        let members = config.sync_eligible_nodes();
        let unread = members.is_err();
        let mut members: Vec<NodeId> = members.into_iter().flatten().collect();
        members.reverse();
        Self {
            members,
            asked: None,
            documents: BTreeMap::new(),
            unread,
        }
    }

    fn read_next(&mut self) -> AdvertisedNext {
        let Some(node) = self.members.pop() else {
            let documents = std::mem::take(&mut self.documents);
            return Ok(Next::Done((documents, self.unread)));
        };
        self.asked = Some(node);
        let read = ReadInfoOperation::new(vec![node]);
        Ok(Next::Call(IoCall::child(read)))
    }
}

impl Operation for AdvertisementsOperation {
    type Output = Advertised;
    type Error = IoError;

    fn first(&mut self) -> AdvertisedNext {
        self.read_next()
    }

    fn next(&mut self, result: IoResult) -> AdvertisedNext {
        let read = result.child::<BTreeMap<NodeId, NodeInfoDocument>, NodeInfoError>()?;
        let node = self.asked.take().ok_or(IoError::Unfinished)?;
        match read.map(|mut documents| documents.remove(&node)) {
            Ok(Some(document)) => {
                self.documents.insert(node, document);
            }
            Ok(None) => {}
            Err(error) => {
                warn!(node = %node, error = %error, "Node info advertisement stayed unread");
                self.unread = true;
            }
        }
        self.read_next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{Family, node};
    use aruna_core::calls::ChildOutcome;

    // Every member is asked once, and a failed read marks the round as incomplete.
    #[test]
    fn unread_member_noted() {
        let config = Family::new([4; 32]).config;
        let members = config.sync_eligible_nodes().expect("members").len();
        let mut read = AdvertisementsOperation::new(&config);
        let mut turn = read.first();
        let mut asked = 0;
        let done = loop {
            match turn.expect("the read continues") {
                Next::Call(IoCall::Child(_)) => asked += 1,
                Next::Done(done) => break done,
                other => panic!("unexpected turn {other:?}"),
            }
            let failed = NodeInfoError::Decode("unreadable".to_string());
            let answer = Err::<BTreeMap<NodeId, NodeInfoDocument>, _>(failed);
            turn = read.next(IoResult::Child(ChildOutcome::new(answer)));
        };
        assert_eq!(asked, members);
        assert!(done.0.is_empty() && done.1);
    }

    // A member without a document is simply absent from a complete read.
    #[test]
    fn missing_document_complete() {
        let mut read = AdvertisementsOperation {
            members: Vec::new(),
            asked: Some(node(1)),
            documents: BTreeMap::new(),
            unread: false,
        };
        let empty = Ok::<_, NodeInfoError>(BTreeMap::<NodeId, NodeInfoDocument>::new());
        let turn = read.next(IoResult::Child(ChildOutcome::new(empty)));
        assert_eq!(turn, Ok(Next::Done((BTreeMap::new(), false))));
    }
}
