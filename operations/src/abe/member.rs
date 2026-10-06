//! Opens member key requests in a group's encrypted buckets after a role grant.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{KeyAction, KeyError, KeyOperation, KeyResult};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::GROUP_ENCRYPTED_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::types::{Effects, GroupId};
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
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
    output: Option<Result<Vec<Ulid>, KeyError>>,
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
            output: None,
        }
    }
    fn next(&mut self) -> Effects {
        let Some((bucket, member)) = self.pending.pop() else {
            self.output = Some(Ok(std::mem::take(&mut self.opened)));
            return smallvec![];
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
        smallvec![Effect::SubOperation(sub)]
    }
}
impl Operation for MemberKeysOperation {
    type Output = Vec<Ulid>;
    type Error = KeyError;
    fn start(&mut self) -> Effects {
        self.members.retain(|member| !member.is_nil());
        if self.members.is_empty() {
            return self.next();
        }
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: GROUP_ENCRYPTED_KEYSPACE.to_string(),
            prefix: Some(self.group_id.to_bytes().to_vec().into()),
            start: None,
            limit: usize::MAX,
            txn_id: None
        })]
    }
    fn step(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::IterResult { values, .. }) if !self.scanned => {
                self.scanned = true;
                for (key, _) in values {
                    let Some(Ok(bucket)) = key.get(16..).map(std::str::from_utf8) else {
                        continue;
                    };
                    for member in &self.members {
                        self.pending.push((bucket.to_string(), *member));
                    }
                }
                self.next()
            }
            Event::SubOperation(SubOperationEvent::KeyRequestsOpened { request_ids })
                if self.scanned =>
            {
                self.opened.extend(request_ids);
                self.next()
            }
            _ => {
                self.output = Some(Err(KeyError::Storage));
                smallvec![]
            }
        }
    }
    fn is_complete(&self) -> bool {
        self.output.is_some()
    }
    fn finalize(self) -> Result<Vec<Ulid>, KeyError> {
        self.output.unwrap_or(Err(KeyError::Storage))
    }
    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
