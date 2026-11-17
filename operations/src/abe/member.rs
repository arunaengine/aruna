//! Opens member key requests in a group's encrypted buckets after a role grant.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{KeyAction, KeyError, KeyOperation, KeyResult};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::keyspaces::GROUP_ENCRYPTED_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::types::GroupId;
use aruna_core::{NodeId, UserId};
use ulid::Ulid;

/// Runs one member key operation per encrypted bucket and member, best effort.
///
/// A group without encrypted buckets costs one index scan and nothing else.
#[derive(Debug, PartialEq)]
pub struct MemberKeysOperation {
    auth: AuthContext,
    node: NodeId,
    group_id: GroupId,
    members: Vec<UserId>,
    now: u64,
    scanned: bool,
    pending: Vec<(String, UserId)>,
    opened: Vec<Ulid>,
}
impl MemberKeysOperation {
    pub fn new(
        auth: AuthContext,
        node: NodeId,
        group_id: GroupId,
        members: Vec<UserId>,
        now: u64,
    ) -> Self {
        Self {
            auth,
            node,
            group_id,
            members,
            now,
            scanned: false,
            pending: Vec::new(),
            opened: Vec::new(),
        }
    }
    /// The key operation of the next bucket and member, or the opened requests.
    fn request(&mut self) -> Next<Vec<Ulid>> {
        let Some((bucket, member)) = self.pending.pop() else {
            return Next::Done(std::mem::take(&mut self.opened));
        };
        let action = KeyAction::Member(member);
        let operation = KeyOperation::new(bucket, self.auth.clone(), self.node, action, self.now);
        let sub = boxed_suboperation(operation, |result| {
            let request_ids = match result {
                Ok(KeyResult::Opened(ids)) => ids,
                Ok(_) | Err(KeyError::Missing) => Vec::new(),
                Err(error) => {
                    tracing::warn!(event = "abe.member_keys.failed", error = %error);
                    Vec::new()
                }
            };
            Event::SubOperation(SubOperationEvent::KeyRequestsOpened { request_ids })
        });
        Next::Call(IoCall::Child(sub))
    }
}
impl Operation for MemberKeysOperation {
    type Output = Vec<Ulid>;
    type Error = KeyError;
    fn first(&mut self) -> Result<Next<Vec<Ulid>>, KeyError> {
        self.members.retain(|member| !member.is_nil());
        if self.members.is_empty() {
            return Ok(self.request());
        }
        Ok(Next::Call(IoCall::Page {
            space: GROUP_ENCRYPTED_KEYSPACE.to_string(),
            prefix: self.group_id.to_bytes().to_vec().into(),
            after: None,
            limit: usize::MAX,
        }))
    }
    fn next(&mut self, result: IoResult) -> Result<Next<Vec<Ulid>>, KeyError> {
        match result {
            IoResult::Page { rows, .. } if !self.scanned => {
                self.scanned = true;
                for (key, _) in rows {
                    let Some(Ok(bucket)) = key.get(16..).map(std::str::from_utf8) else {
                        continue;
                    };
                    for member in &self.members {
                        self.pending.push((bucket.to_string(), *member));
                    }
                }
                Ok(self.request())
            }
            IoResult::Event(event) if self.scanned => match *event {
                Event::SubOperation(SubOperationEvent::KeyRequestsOpened { request_ids }) => {
                    self.opened.extend(request_ids);
                    Ok(self.request())
                }
                _ => Err(KeyError::Storage),
            },
            _ => Err(KeyError::Storage),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;

    #[test]
    fn opens_foreign_members() {
        // Approving a federated member opens its key requests; the nil user is still dropped.
        let realm_id = RealmId([1; 32]);
        let auth = AuthContext {
            user_id: UserId::new(Ulid::from_bytes([2; 16]), realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let foreign = UserId::new(Ulid::from_bytes([3; 16]), RealmId([9; 32]));
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let group_id = Ulid::from_bytes([4; 16]);
        let members = vec![UserId::nil(realm_id), foreign];
        let mut operation = MemberKeysOperation::new(auth, node, group_id, members, 1);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        assert_eq!(operation.members, vec![foreign]);
        let key = [&group_id.to_bytes()[..], b"bucket"].concat();
        let page = IoResult::Page {
            rows: vec![(key.into(), Vec::new().into())],
            next: None,
        };
        assert!(matches!(
            operation.next(page),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let opened = Event::SubOperation(SubOperationEvent::KeyRequestsOpened {
            request_ids: vec![Ulid::from_bytes([5; 16])],
        });
        assert_eq!(
            operation.next(IoResult::Event(Box::new(opened))),
            Ok(Next::Done(vec![Ulid::from_bytes([5; 16])]))
        );
    }
}
