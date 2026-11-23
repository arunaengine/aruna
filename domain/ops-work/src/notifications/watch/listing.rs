//! Lists stored watch subscriptions of one owner, of one realm, or one bounded realm page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::WATCH_SUBSCRIPTIONS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification_watch::{
    WatchSubscription, watch_subscription_prefix,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::{Key, Value};
use aruna_core::user::id::UserId;
use tracing::warn;

use crate::notifications::protocol::SUBSCRIPTION_SCAN_CAP;
use crate::notifications::unexpected;
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

/// Reads stored watch subscriptions and checks each row against its key and scope; a scan beyond
/// its cap is an error. A malformed row fails the owner's own listing, while a delivery scan of a
/// realm skips it, so one broken row never costs the healthy watches on its page their events.
#[derive(Debug, PartialEq)]
pub struct ListWatchesOperation {
    scope: WatchScope,
    page: WatchPage,
}

impl ListWatchesOperation {
    pub fn new(scope: WatchScope) -> Self {
        Self {
            scope,
            page: WatchPage::default(),
        }
    }

    fn read(&self, after: Option<Key>) -> IoCall {
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
        IoCall::Page {
            space: WATCH_SUBSCRIPTIONS_KEYSPACE.to_string(),
            prefix,
            after,
            limit,
        }
    }

    /// Decodes rows into the page, checking that each belongs to the scope.
    fn take(&mut self, rows: Vec<(Key, Value)>) -> Result<(), WatchSubscriptionError> {
        for (key, value) in rows {
            match self.checked(&key, &value) {
                Ok(subscription) => self.page.subscriptions.push(subscription),
                Err(WatchSubscriptionError::Malformed(reason))
                    if !matches!(self.scope, WatchScope::Owner(_)) =>
                {
                    warn!(key = ?key.to_vec(), %reason, "Skipping a malformed watch subscription");
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn checked(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Result<WatchSubscription, WatchSubscriptionError> {
        let subscription = decode_stored_subscription(key, value)?;
        let foreign = match &self.scope {
            WatchScope::Owner(owner) => (subscription.owner != *owner)
                .then_some("stored watch subscription belongs to a different owner"),
            WatchScope::Realm(realm_id) | WatchScope::Page { realm_id, .. } => {
                (subscription.owner.realm_id != *realm_id)
                    .then_some("stored watch subscription belongs to a different realm")
            }
        };
        match foreign {
            Some(reason) => Err(WatchSubscriptionError::Malformed(reason.to_string())),
            None => Ok(subscription),
        }
    }

    fn page_read(
        &mut self,
        values: Vec<(Key, Value)>,
        next: Option<Key>,
    ) -> Result<Next<WatchPage>, WatchSubscriptionError> {
        let next = match self.scope.clone() {
            WatchScope::Owner(_) => self.take(values).map(|()| None),
            WatchScope::Realm(_) => {
                let remaining = SUBSCRIPTION_SCAN_CAP - self.page.subscriptions.len();
                if values.len() > remaining {
                    let reason = "notification watch subscription scan cap reached";
                    return Err(WatchSubscriptionError::Storage(reason.to_string()));
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
        }?;
        match next {
            Some(next) => Ok(Next::Call(self.read(Some(next)))),
            None => Ok(Next::Done(std::mem::take(&mut self.page))),
        }
    }
}

impl Operation for ListWatchesOperation {
    type Output = WatchPage;
    type Error = WatchSubscriptionError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        match &self.scope {
            WatchScope::Page { limit, .. } if *limit == 0 || *limit > SUBSCRIPTION_SCAN_CAP => {
                let reason = "notification watch subscription page cap exceeded";
                Err(WatchSubscriptionError::Storage(reason.to_string()))
            }
            WatchScope::Page { start, .. } => {
                let start = start.clone().map(Key::from);
                Ok(Next::Call(self.read(start)))
            }
            _ => Ok(Next::Call(self.read(None))),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        match result {
            IoResult::Page { rows, next } => self.page_read(rows, next),
            other => Err(unexpected("IoResult::Page", &other).into()),
        }
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use crate::tests::notifications::user;

    fn page(prefix: Key, after: Option<Key>, limit: usize) -> Next<WatchPage> {
        Next::Call(IoCall::Page {
            space: WATCH_SUBSCRIPTIONS_KEYSPACE.to_string(),
            prefix,
            after,
            limit,
        })
    }

    fn empty(next: Option<Key>) -> IoResult {
        IoResult::Page {
            rows: Vec::new(),
            next,
        }
    }

    #[test]
    fn owner_scan_calls() {
        let owner = user(1, 2);
        let mut list = ListWatchesOperation::new(WatchScope::Owner(owner));
        assert_eq!(
            list.first(),
            Ok(page(
                watch_subscription_prefix(owner),
                None,
                WATCH_LIST_LIMIT
            ))
        );
        let done = list.next(empty(Some(Key::from(vec![1]))));
        assert_eq!(done, Ok(Next::Done(WatchPage::default())), "one page");
    }

    #[test]
    fn realm_scan_pages() {
        let realm_id = user(1, 2).realm_id;
        let prefix = UserId::storage_prefix(realm_id);
        let mut list = ListWatchesOperation::new(WatchScope::Realm(realm_id));
        list.first().unwrap();
        assert_eq!(
            list.next(empty(Some(Key::from(vec![1])))),
            Ok(page(
                prefix,
                Some(Key::from(vec![1])),
                SUBSCRIPTION_PAGE_LIMIT
            ))
        );
    }

    #[test]
    fn bad_rows_fail() {
        let mut list = ListWatchesOperation::new(WatchScope::Owner(user(1, 2)));
        list.first().unwrap();
        let rows = vec![(Key::from(vec![1]), Value::from(vec![2]))];
        assert!(list.next(IoResult::Page { rows, next: None }).is_err());
        assert!(list.next(IoResult::Done).is_err());
        let mut page = ListWatchesOperation::new(WatchScope::Page {
            realm_id: user(1, 2).realm_id,
            start: None,
            limit: 0,
        });
        assert!(page.first().is_err());
    }

    // The realm scan this node delivers its own events with skips a malformed row and keeps the
    // healthy watch on its page, so the event settles; the owner's own listing still reports it.
    #[test]
    fn realm_scan_skips() {
        use aruna_core::structs::execution::notification_watch::{
            WatchEventKind, WatchEventMask, watch_subscription_key,
        };
        let mask = WatchEventMask::from_kinds([WatchEventKind::DataUploaded]);
        let owner = user(4, 3);
        let id = ulid::Ulid::generate();
        let mut healthy = WatchSubscription::new(id, owner, "data/".into(), mask, 1);
        healthy.watch_id = ulid::Ulid::from_bytes([7; 16]);
        let broken = watch_subscription_key(owner, ulid::Ulid::from_bytes([6; 16]));
        let kept = watch_subscription_key(owner, healthy.watch_id);
        let rows = vec![
            (broken, Value::from(vec![0xff])),
            (kept, Value::from(healthy.to_bytes().unwrap())),
        ];
        let mut realm = ListWatchesOperation::new(WatchScope::Realm(owner.realm_id));
        realm.first().unwrap();
        let listed = realm.next(IoResult::Page {
            rows: rows.clone(),
            next: None,
        });
        let page = WatchPage {
            subscriptions: vec![healthy],
            next: None,
        };
        assert_eq!(listed, Ok(Next::Done(page)));

        let mut own = ListWatchesOperation::new(WatchScope::Owner(owner));
        own.first().unwrap();
        let listed = own.next(IoResult::Page { rows, next: None });
        assert!(matches!(listed, Err(WatchSubscriptionError::Malformed(_))));
    }
}
