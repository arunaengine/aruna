use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::GROUP_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::Group;
use aruna_core::types::{Effects, GroupId, Key, Value};
use smallvec::smallvec;
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

#[derive(Debug, PartialEq)]
pub struct SearchGroupsOperation {
    input: SearchGroupsInput,
    state: SearchGroupsState,
    matches: Vec<SearchGroupsMatch>,
    next_storage_start_after: Option<Key>,
    output: Option<Result<SearchGroupsOutput, SearchGroupsError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum SearchGroupsState {
    Init,
    SearchGroups,
    Finish,
    Error,
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
        got: String,
    },
    #[error("search groups did not finish")]
    NotFinished,
}

impl SearchGroupsOperation {
    pub fn new(mut input: SearchGroupsInput) -> Self {
        input.limit = input.limit.max(1);
        Self {
            input,
            state: SearchGroupsState::Init,
            matches: Vec::new(),
            next_storage_start_after: None,
            output: None,
        }
    }

    fn fail(&mut self, error: SearchGroupsError) -> Effects {
        self.state = SearchGroupsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }
        Ok(event)
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(SearchGroupsError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn start_after_key(&self) -> Result<Option<Key>, SearchGroupsError> {
        match (&self.next_storage_start_after, &self.input.start_after) {
            (Some(key), _) => Ok(Some(key.clone())),
            (None, Some(group_id)) => {
                let group_id = GroupId::from_string(group_id).map_err(ConversionError::from)?;
                Ok(Some(group_id.to_bytes().to_vec().into()))
            }
            (None, None) => Ok(None),
        }
    }

    fn emit_search_groups(&mut self) -> Result<Effects, SearchGroupsError> {
        self.state = SearchGroupsState::SearchGroups;
        Ok(smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: GROUP_KEYSPACE.to_string(),
            prefix: None,
            start: self.start_after_key()?.map(IterStart::After),
            limit: self.input.limit.saturating_add(1).max(64),
            txn_id: None,
        })])
    }

    fn handle_search_groups(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.unexpected_event("Event::Storage(StorageEvent::IterResult)", got);
        };

        match self.collect_matches(values, next_start_after) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn matches_query(group: &Group, query: &str) -> bool {
        group.display_name.to_lowercase().contains(query)
    }

    fn collect_matches(
        &mut self,
        values: Vec<(Key, Value)>,
        next_start_after: Option<Key>,
    ) -> Result<Effects, SearchGroupsError> {
        let query = self.input.query.to_lowercase();
        for (_, value) in values {
            let group = Group::from_bytes(&value)?;
            if !Self::matches_query(&group, &query) {
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
            return Ok(self.emit_finish(next_start_after));
        }

        if let Some(next_start_after) = next_start_after {
            self.next_storage_start_after = Some(next_start_after);
            return self.emit_search_groups();
        }

        Ok(self.emit_finish(None))
    }

    fn emit_finish(&mut self, next_start_after: Option<String>) -> Effects {
        self.state = SearchGroupsState::Finish;
        self.output = Some(Ok(SearchGroupsOutput {
            groups: std::mem::take(&mut self.matches),
            next_start_after,
        }));
        smallvec![]
    }
}

impl Operation for SearchGroupsOperation {
    type Output = SearchGroupsOutput;
    type Error = SearchGroupsError;

    fn start(&mut self) -> Effects {
        match self.emit_search_groups() {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state {
            SearchGroupsState::SearchGroups => self.handle_search_groups(event),
            SearchGroupsState::Init | SearchGroupsState::Finish | SearchGroupsState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            SearchGroupsState::Finish | SearchGroupsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(SearchGroupsError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{SearchGroupsInput, SearchGroupsMatch, SearchGroupsOperation, SearchGroupsOutput};
    use aruna_core::effects::{Effect, IterStart, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{Actor, Group, RealmId};
    use aruna_core::types::{Key, UserId, Value};
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

        let effects = operation.start();
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::Iter {
                key_space,
                prefix,
                start,
                limit,
                txn_id,
            }) => {
                assert_eq!(key_space, aruna_core::keyspaces::GROUP_KEYSPACE);
                assert_eq!(prefix, &None);
                assert_eq!(start, &None);
                assert_eq!(*limit, 64);
                assert_eq!(txn_id, &None);
            }
            other => panic!("unexpected effect: {other:?}"),
        }

        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![entry(&alice), entry(&bob)],
            next_start_after: None,
        }));

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize().unwrap(),
            SearchGroupsOutput {
                groups: vec![matched(&alice)],
                next_start_after: None,
            }
        );
    }

    #[test]
    fn returns_cursor_over_limit() {
        let alice = group(2, "match-alice");
        let bob = group(3, "match-bob");
        let mut operation = SearchGroupsOperation::new(input("match", 1));
        operation.start();

        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![entry(&alice), entry(&bob)],
            next_start_after: None,
        }));

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize().unwrap(),
            SearchGroupsOutput {
                groups: vec![matched(&alice)],
                next_start_after: Some(alice.group_id.to_string()),
            }
        );
    }

    #[test]
    fn continues_scan() {
        // A non-matching page entry forces another storage scan.
        let alice = group(2, "match-alice");
        let skipped = group(3, "other");
        let bob = group(4, "match-bob");
        let mut operation = SearchGroupsOperation::new(input("match", 2));
        operation.start();

        let continuation_key: Key = skipped.group_id.to_bytes().to_vec().into();
        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![entry(&alice), entry(&skipped)],
            next_start_after: Some(continuation_key.clone()),
        }));
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::Iter {
                start: Some(IterStart::After(key)),
                ..
            }) => {
                assert_eq!(key, &continuation_key);
            }
            other => panic!("unexpected effect: {other:?}"),
        }
        assert!(!operation.is_complete());

        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![entry(&bob)],
            next_start_after: None,
        }));

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize().unwrap(),
            SearchGroupsOutput {
                groups: vec![matched(&alice), matched(&bob)],
                next_start_after: None,
            }
        );
    }
}
