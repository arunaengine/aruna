//! Searches stored groups by display name and answers a page of matches.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::GROUP_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::group::Group;
use aruna_core::types::{GroupId, Key};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct SearchGroupsInput {
    pub query: String,
    pub limit: usize,
    pub start_after: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchGroupsMatch {
    pub group_id: GroupId,
    pub display_name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchGroupsOutput {
    pub groups: Vec<SearchGroupsMatch>,
    pub next_start_after: Option<String>,
}

/// Scans groups page by page until one more match than the limit or the end of the keyspace.
#[derive(Debug, PartialEq)]
pub struct SearchGroupsOperation {
    input: SearchGroupsInput,
    matches: Vec<SearchGroupsMatch>,
}

#[derive(Debug, Error, PartialEq)]
pub enum SearchGroupsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
    #[error("search groups did not finish")]
    NotFinished,
}

impl From<IoError> for SearchGroupsError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "SearchGroups".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl SearchGroupsOperation {
    pub fn new(mut input: SearchGroupsInput) -> Self {
        input.limit = input.limit.max(1);
        Self {
            input,
            matches: Vec::new(),
        }
    }

    fn page(&self, after: Option<Key>) -> IoCall {
        IoCall::Page {
            space: GROUP_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: self.input.limit.saturating_add(1).max(64),
        }
    }

    fn finish(&mut self, next_start_after: Option<String>) -> Next<SearchGroupsOutput> {
        Next::Done(SearchGroupsOutput {
            groups: std::mem::take(&mut self.matches),
            next_start_after,
        })
    }
}

impl Operation for SearchGroupsOperation {
    type Output = SearchGroupsOutput;
    type Error = SearchGroupsError;

    fn first(&mut self) -> Result<Next<SearchGroupsOutput>, SearchGroupsError> {
        let after = (self.input.start_after.as_deref())
            .map(|group_id| GroupId::from_string(group_id).map_err(ConversionError::from))
            .transpose()?
            .map(|group_id| Key::from(group_id.to_bytes().to_vec()));
        Ok(Next::Call(self.page(after)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SearchGroupsOutput>, SearchGroupsError> {
        let IoResult::Page { rows, next } = result else {
            return Err(SearchGroupsError::UnexpectedEvent {
                state: "SearchGroups".to_string(),
                expected: "IoResult::Page",
                got: result.label(),
            });
        };
        let query = self.input.query.to_lowercase();
        for (_, value) in rows {
            let group = Group::from_bytes(&value)?;
            if !group.display_name.to_lowercase().contains(&query) {
                continue;
            }
            self.matches.push(SearchGroupsMatch {
                group_id: group.group_id,
                display_name: group.display_name,
            });
        }
        if self.matches.len() > self.input.limit {
            self.matches.truncate(self.input.limit);
            let next_start_after = self.matches.last().map(|group| group.group_id.to_string());
            return Ok(self.finish(next_start_after));
        }
        match next {
            Some(after) => Ok(Next::Call(self.page(Some(after)))),
            None => Ok(self.finish(None)),
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{SearchGroupsInput, SearchGroupsMatch, SearchGroupsOperation, SearchGroupsOutput};
    use aruna_core::UserId;
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::operation::Operation;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::group::Group;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::types::{Key, Value};
    use std::collections::HashSet;
    use ulid::Ulid;

    fn realm() -> RealmId {
        RealmId::from_bytes([2u8; 32])
    }

    fn actor() -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([9u8; 16]), realm()),
            realm_id: realm(),
        }
    }

    fn group(seed: u8, name: &str) -> Group {
        let owner = UserId::local(Ulid::from_bytes([seed; 16]), realm());
        Group {
            display_name: name.to_string(),
            group_id: Ulid::from_bytes([seed; 16]),
            realm_id: realm(),
            roles: HashSet::new(),
            owner,
        }
    }

    fn entry(group: &Group) -> (Key, Value) {
        (
            group.group_id.to_bytes().to_vec().into(),
            group.to_bytes(&actor()).unwrap().into(),
        )
    }

    fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
        IoResult::Page { rows, next }
    }

    fn matched(group: &Group) -> SearchGroupsMatch {
        SearchGroupsMatch {
            group_id: group.group_id,
            display_name: group.display_name.clone(),
        }
    }

    fn input(query: &str, limit: usize) -> SearchGroupsInput {
        SearchGroupsInput {
            query: query.to_string(),
            limit,
            start_after: None,
        }
    }

    #[test]
    fn matches_display_name() {
        // Case-insensitive substring over display_name.
        let alice = group(2, "Alice Lab");
        let bob = group(3, "bob");
        let mut operation = SearchGroupsOperation::new(input("ali", 10));

        let scan = IoCall::Page {
            space: aruna_core::keyspaces::GROUP_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: 64,
        };
        assert_eq!(operation.first(), Ok(Next::Call(scan)));

        let page = page(vec![entry(&alice), entry(&bob)], None);
        let output = SearchGroupsOutput {
            groups: vec![matched(&alice)],
            next_start_after: None,
        };
        assert_eq!(operation.next(page), Ok(Next::Done(output)));
    }

    #[test]
    fn cursor_over_limit() {
        let alice = group(2, "match-alice");
        let bob = group(3, "match-bob");
        let mut operation = SearchGroupsOperation::new(input("match", 1));
        operation.first().unwrap();

        let page = page(vec![entry(&alice), entry(&bob)], None);
        let output = SearchGroupsOutput {
            groups: vec![matched(&alice)],
            next_start_after: Some(alice.group_id.to_string()),
        };
        assert_eq!(operation.next(page), Ok(Next::Done(output)));
    }

    #[test]
    fn continues_scan() {
        // A non-matching page entry forces another storage scan.
        let alice = group(2, "match-alice");
        let skipped = group(3, "other");
        let bob = group(4, "match-bob");
        let mut operation = SearchGroupsOperation::new(input("match", 2));
        operation.first().unwrap();

        let continuation_key: Key = skipped.group_id.to_bytes().to_vec().into();
        let first = page(
            vec![entry(&alice), entry(&skipped)],
            Some(continuation_key.clone()),
        );
        assert!(matches!(
            operation.next(first),
            Ok(Next::Call(IoCall::Page { after: Some(key), .. })) if key == continuation_key
        ));

        let output = SearchGroupsOutput {
            groups: vec![matched(&alice), matched(&bob)],
            next_start_after: None,
        };
        assert_eq!(
            operation.next(page(vec![entry(&bob)], None)),
            Ok(Next::Done(output))
        );
    }
}
