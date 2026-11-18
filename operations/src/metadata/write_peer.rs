//! Checks the token a peer forwarded for a caller.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect, WritePeerRequest};
use aruna_core::events::{Event, NetEvent, PeerAuthRefusal};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;

/// The caller a forwarded token stands for, accepted only from a peer of that caller's realm.
#[derive(Debug, PartialEq)]
pub struct WritePeerOperation {
    request: WritePeerRequest,
}

impl WritePeerOperation {
    pub fn new(peer: NodeId, token: AuthToken) -> Self {
        let request = WritePeerRequest { peer, token };
        Self { request }
    }
}

impl Operation for WritePeerOperation {
    type Output = AuthContext;
    type Error = PeerAuthRefusal;

    fn first(&mut self) -> Result<Next<AuthContext>, PeerAuthRefusal> {
        let request = Box::new(self.request.clone());
        let effect = Effect::Net(NetEffect::WritePeerAuth(request));
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AuthContext>, PeerAuthRefusal> {
        let expected = "Event::Net(NetEvent::WritePeerAuth)";
        let IoResult::Event(event) = result else {
            let got = result.label();
            return Err(IoError::Unexpected { expected, got }.into());
        };
        let Event::Net(NetEvent::WritePeerAuth(answer)) = *event else {
            let got = "another event";
            return Err(IoError::Unexpected { expected, got }.into());
        };
        Ok(Next::Done(*answer?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{REALM, node};

    fn operation() -> WritePeerOperation {
        WritePeerOperation::new(node(1), AuthToken::Internal(AuthContext::anonymous(REALM)))
    }

    fn answered(answer: Result<Box<AuthContext>, PeerAuthRefusal>) -> IoResult {
        IoResult::Event(Box::new(Event::Net(NetEvent::WritePeerAuth(answer))))
    }

    // The token and the forwarding peer go to the adapter together, and its caller is the result.
    #[test]
    fn asks_for_caller() {
        let mut operation = operation();
        let request = Box::new(operation.request.clone());
        let effect = Effect::Net(NetEffect::WritePeerAuth(request));
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Effect(Box::new(effect))))
        );
        let caller = AuthContext::anonymous(REALM);
        assert_eq!(
            operation.next(answered(Ok(Box::new(caller.clone())))),
            Ok(Next::Done(caller))
        );
    }

    // A refusal stays a refusal, and any other answer leaves the caller unknown.
    #[test]
    fn keeps_the_refusal() {
        let refused = operation().next(answered(Err(PeerAuthRefusal::Unauthorized)));
        assert_eq!(refused, Err(PeerAuthRefusal::Unauthorized));
        let other = IoResult::Event(Box::new(Event::Net(NetEvent::Recorded)));
        assert!(matches!(
            operation().next(other),
            Err(PeerAuthRefusal::Unavailable(_))
        ));
    }
}
