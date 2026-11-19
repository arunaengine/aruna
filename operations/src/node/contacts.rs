//! Reads when this node last saw each of a set of peers, as the realm info shows devices.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::operation::Operation;

/// The last contact of each peer this node saw, in unix milliseconds; a peer never seen, or a
/// node without a network, has none.
#[derive(Debug, PartialEq)]
pub struct PeerContactsOperation {
    peers: Vec<NodeId>,
}

impl PeerContactsOperation {
    pub fn new(peers: Vec<NodeId>) -> Self {
        Self { peers }
    }
}

impl Operation for PeerContactsOperation {
    type Output = BTreeMap<NodeId, u64>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<BTreeMap<NodeId, u64>>, IoError> {
        let peers = std::mem::take(&mut self.peers);
        let read = Effect::Net(NetEffect::PeerContacts { peers });
        Ok(Next::Call(IoCall::Effect(Box::new(read))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<BTreeMap<NodeId, u64>>, IoError> {
        match result.event()? {
            Event::Net(NetEvent::PeerContacts(seen)) => Ok(Next::Done(seen)),
            other => Err(IoError::Unexpected {
                expected: "the peer contacts",
                got: other.label(),
            }),
        }
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use crate::tests::calls::node;

    #[test]
    fn contacts_read_once() {
        let mut contacts = PeerContactsOperation::new(vec![node(1), node(2)]);
        let peers = vec![node(1), node(2)];
        let read = Effect::Net(NetEffect::PeerContacts { peers });
        assert_eq!(
            contacts.first(),
            Ok(Next::Call(IoCall::Effect(Box::new(read))))
        );
        let seen = BTreeMap::from([(node(1), 7)]);
        let answer = IoResult::Event(Box::new(Event::Net(NetEvent::PeerContacts(seen.clone()))));
        assert_eq!(contacts.next(answer), Ok(Next::Done(seen)));
    }

    #[test]
    fn other_answer_fails() {
        let mut contacts = PeerContactsOperation::new(Vec::new());
        contacts.first().unwrap();
        let other = IoResult::Event(Box::new(Event::Net(NetEvent::Recorded)));
        assert!(contacts.next(other).is_err());
    }
}
