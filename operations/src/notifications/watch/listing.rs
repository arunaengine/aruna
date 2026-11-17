//! Lists stored watch subscriptions of one owner, of one realm, or one bounded realm page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::WATCH_SUBSCRIPTIONS_KEYSPACE;
use aruna_core::operation::EffectOperation;
use aruna_core::structs::execution::notification_watch::{
    WatchSubscription, watch_subscription_prefix,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;

use crate::notifications::protocol::SUBSCRIPTION_SCAN_CAP;
use crate::notifications::watch::records::{WatchSubscriptionError, decode_stored_subscription};

/// Single owner-prefix scan bound. Watches are hard-capped per user, so one page
/// always covers a subscription set with a wide safety margin.
const WATCH_LIST_LIMIT: usize = 256;
pub(crate) const SUBSCRIPTION_PAGE_LIMIT: usize = 256;

/// Which subscriptions to list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchScope {
    /// Every watch of one owner, in one page.
    Owner(UserId),
    /// Every watch this node holds for a realm, at most the scan cap.
    Realm(RealmId),
    /// One bounded realm page after `start`, with its continuation.
    Page {
        realm_id: RealmId,
        start: Option<Vec<u8>>,
        limit: usize,
    },
}

/// Listed subscriptions; `next` continues a realm page.
#[derive(Debug, Default, PartialEq)]
pub struct WatchPage {
    pub subscriptions: Vec<WatchSubscription>,
    pub next: Option<Vec<u8>>,
}

#[derive(Debug, Default, PartialEq)]
enum ListState {
    #[default]
    Init,
    Reading,
    Done,
    Failed(WatchSubscriptionError),
}

/// Reads stored watch subscriptions and checks each row against its key and scope; a row of
/// another owner or realm, or a scan beyond its cap, is an error.
#[derive(Debug, PartialEq)]
pub struct ListWatchesOperation {
    scope: WatchScope,
    page: WatchPage,
    state: ListState,
}

impl ListWatchesOperation {
    pub fn new(scope: WatchScope) -> Self {
        Self {
            scope,
            page: WatchPage::default(),
            state: ListState::Init,
        }
    }

    fn read(&mut self, start: Option<Key>) -> Effects {
        let (prefix, limit) = match &self.scope {
            WatchScope::Owner(owner) => (watch_subscription_prefix(*owner), WATCH_LIST_LIMIT),
            WatchScope::Realm(realm_id) => {
                let remaining = SUBSCRIPTION_SCAN_CAP - self.page.subscriptions.len();
                let limit = remaining.saturating_add(1).min(SUBSCRIPTION_PAGE_LIMIT);
                (UserId::storage_prefix(*realm_id), limit)
            }
            WatchScope::Page {
                realm_id, limit, ..
            } => (UserId::storage_prefix(*realm_id), limit.saturating_add(1)),
        };
        self.state = ListState::Reading;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: WATCH_SUBSCRIPTIONS_KEYSPACE.to_string(),
            prefix: Some(prefix),
            start: start.map(IterStart::After),
            limit,
            txn_id: None,
        })]
    }

    fn fail(&mut self, error: WatchSubscriptionError) -> Effects {
        self.state = ListState::Failed(error);
        smallvec![]
    }

    /// Decodes rows into the page, checking that each belongs to the scope.
    fn take(&mut self, rows: Vec<(Key, Value)>) -> Result<(), WatchSubscriptionError> {
        for (key, value) in rows {
            let subscription = decode_stored_subscription(&key, &value)?;
            let foreign = match &self.scope {
                WatchScope::Owner(owner) => (subscription.owner != *owner)
                    .then_some("stored watch subscription belongs to a different owner"),
                WatchScope::Realm(realm_id) | WatchScope::Page { realm_id, .. } => {
                    (subscription.owner.realm_id != *realm_id)
                        .then_some("stored watch subscription belongs to a different realm")
                }
            };
            if let Some(reason) = foreign {
                return Err(WatchSubscriptionError::Storage(reason.to_string()));
            }
            self.page.subscriptions.push(subscription);
        }
        Ok(())
    }

    fn page_read(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        let taken = match self.scope.clone() {
            WatchScope::Owner(_) => self.take(values).map(|()| None),
            WatchScope::Realm(_) => {
                let remaining = SUBSCRIPTION_SCAN_CAP - self.page.subscriptions.len();
                if values.len() > remaining {
                    let reason = "notification watch subscription scan cap reached";
                    return self.fail(WatchSubscriptionError::Storage(reason.to_string()));
                }
                self.take(values).map(|()| next)
            }
            WatchScope::Page { limit, .. } => {
                let has_more = values.len() > limit;
                let page: Vec<_> = values.into_iter().take(limit).collect();
                let last = page.last().map(|(key, _)| key.to_vec());
                self.page.next = has_more.then_some(last).flatten();
                self.take(page).map(|()| None)
            }
        };
        match taken {
            Ok(Some(next)) => self.read(Some(next)),
            Ok(None) => {
                self.state = ListState::Done;
                smallvec![]
            }
            Err(error) => self.fail(error),
        }
    }
}

impl EffectOperation for ListWatchesOperation {
    type Output = WatchPage;
    type Error = WatchSubscriptionError;

    fn start(&mut self) -> Effects {
        match &self.scope {
            WatchScope::Page { limit, .. } if *limit == 0 || *limit > SUBSCRIPTION_SCAN_CAP => {
                let reason = "notification watch subscription page cap exceeded";
                self.fail(WatchSubscriptionError::Storage(reason.to_string()))
            }
            WatchScope::Page { start, .. } => {
                let start = start.clone().map(Key::from);
                self.read(start)
            }
            _ => self.read(None),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        match (std::mem::take(&mut self.state), event) {
            (
                ListState::Reading,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.page_read(values, next_start_after),
            (_, Event::Storage(StorageEvent::Error { error })) => {
                self.fail(WatchSubscriptionError::Storage(error.to_string()))
            }
            (_, other) => self.fail(WatchSubscriptionError::Storage(format!(
                "unexpected storage event: {other:?}"
            ))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ListState::Done | ListState::Failed(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            ListState::Done => Ok(self.page),
            ListState::Failed(error) => Err(error),
            _ => Err(WatchSubscriptionError::Storage(
                "the watch listing did not finish".to_string(),
            )),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use crate::tests::notifications::user;
    use aruna_core::operation_check::{
        check_abort, rejects_events, rejects_finalize, sample_events,
    };

    fn iter(prefix: Key, start: Option<Key>, limit: usize) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: WATCH_SUBSCRIPTIONS_KEYSPACE.to_string(),
            prefix: Some(prefix),
            start: start.map(IterStart::After),
            limit,
            txn_id: None,
        })
    }

    #[test]
    fn owner_scan_trace() {
        let owner = user(1, 2);
        let mut list = ListWatchesOperation::new(WatchScope::Owner(owner));
        assert_eq!(
            list.start().as_slice(),
            [iter(
                watch_subscription_prefix(owner),
                None,
                WATCH_LIST_LIMIT
            )]
        );
        let done = list.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: Some(Key::from(vec![1])),
        }));
        assert!(done.is_empty(), "an owner list reads one page");
        assert_eq!(list.finalize(), Ok(WatchPage::default()));
    }

    #[test]
    fn realm_scan_pages() {
        let realm_id = user(1, 2).realm_id;
        let prefix = UserId::storage_prefix(realm_id);
        let mut list = ListWatchesOperation::new(WatchScope::Realm(realm_id));
        list.start();
        let next = list.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: Some(Key::from(vec![1])),
        }));
        assert_eq!(
            next.as_slice(),
            [iter(
                prefix,
                Some(Key::from(vec![1])),
                SUBSCRIPTION_PAGE_LIMIT
            )]
        );
    }

    #[test]
    fn bad_rows_fail() {
        let mut list = ListWatchesOperation::new(WatchScope::Owner(user(1, 2)));
        list.start();
        list.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(Key::from(vec![1]), Value::from(vec![2]))],
            next_start_after: None,
        }));
        assert!(list.finalize().is_err());
        let mut page = ListWatchesOperation::new(WatchScope::Page {
            realm_id: user(1, 2).realm_id,
            start: None,
            limit: 0,
        });
        assert!(page.start().is_empty());
        assert!(page.finalize().is_err());
    }

    #[test]
    fn unexpected_events_fail() {
        let make = || {
            let mut list = ListWatchesOperation::new(WatchScope::Owner(user(1, 2)));
            list.start();
            list
        };
        let expected =
            |event: &Event| matches!(event, Event::Storage(StorageEvent::IterResult { .. }));
        assert!(!rejects_events(make, sample_events, expected).is_empty());
    }

    #[test]
    fn abort_and_finalize() {
        let mut list = ListWatchesOperation::new(WatchScope::Realm(user(1, 2).realm_id));
        list.start();
        check_abort(&mut list, &[]);
        rejects_finalize(list);
    }
}
