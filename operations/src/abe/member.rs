//! Opens member key requests in a group's encrypted buckets after a role grant.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{KeyAction, KeyError, KeyOperation, KeyResult};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::GROUP_ENCRYPTED_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::types::{GroupId, Key};
use aruna_core::user::id::UserId;
use std::collections::VecDeque;
use ulid::Ulid;

/// Encrypted buckets of the group read per page.
const BUCKET_PAGE: usize = 256;

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
    /// A bucket page was asked for; `None` before the first page.
    paging: Option<bool>,
    /// The buckets of the current page, and the next member of the front one.
    buckets: VecDeque<String>,
    member: usize,
    next_page: Option<Key>,
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
            paging: None,
            buckets: VecDeque::new(),
            member: 0,
            next_page: None,
            opened: Vec::new(),
        }
    }

    fn page(&mut self, after: Option<Key>) -> Next<Vec<Ulid>> {
        self.paging = Some(true);
        Next::Call(IoCall::Page {
            space: GROUP_ENCRYPTED_KEYSPACE.to_string(),
            prefix: self.group_id.to_bytes().to_vec().into(),
            after,
            limit: BUCKET_PAGE,
        })
    }

    /// The key operation of the next bucket and member, the next bucket page, or the opened
    /// requests; pairs are made one at a time.
    fn request(&mut self) -> Next<Vec<Ulid>> {
        let (bucket, member) = loop {
            let Some(bucket) = self.buckets.front() else {
                if let Some(after) = self.next_page.take() {
                    return self.page(Some(after));
                }
                return Next::Done(std::mem::take(&mut self.opened));
            };
            if let Some(member) = self.members.get(self.member) {
                self.member += 1;
                break (bucket.clone(), *member);
            }
            self.buckets.pop_front();
            self.member = 0;
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
        Ok(self.page(None))
    }
    fn next(&mut self, result: IoResult) -> Result<Next<Vec<Ulid>>, KeyError> {
        match result {
            IoResult::Page { rows, next } if self.paging == Some(true) => {
                self.paging = Some(false);
                self.next_page = next;
                for (key, _) in rows {
                    if let Some(Ok(bucket)) = key.get(16..).map(std::str::from_utf8) {
                        self.buckets.push_back(bucket.to_string());
                    }
                }
                Ok(self.request())
            }
            IoResult::Event(event) if self.paging == Some(false) => match *event {
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
        let after = Key::from(key.clone());
        let page = IoResult::Page {
            rows: vec![(key.into(), Vec::new().into())],
            next: Some(after.clone()),
        };
        assert!(matches!(
            operation.next(page),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let opened = Event::SubOperation(SubOperationEvent::KeyRequestsOpened {
            request_ids: Vec::new(),
        });
        // The next bucket page follows once the page's buckets have their requests.
        let Ok(Next::Call(IoCall::Page { after: asked, limit, .. })) =
            operation.next(IoResult::Event(Box::new(opened)))
        else {
            panic!("expected the next bucket page");
        };
        assert_eq!((asked, limit), (Some(after), BUCKET_PAGE));
        let last = [&group_id.to_bytes()[..], b"other"].concat();
        let page = IoResult::Page {
            rows: vec![(last.into(), Vec::new().into())],
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
