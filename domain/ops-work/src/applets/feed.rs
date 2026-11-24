//! Pages the notices after an edge's position and waits a little for new ones when none are
//! there, so a stateless edge learns of a change within a second.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::applet::notices::{Notice, notice_prefix};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::APPLETS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::types::Key;

use super::AppletError;

/// How long the feed sleeps between two reads while it waits.
const POLL: Duration = Duration::from_secs(1);

/// The notices after a position, and the row to continue after.
#[derive(Clone, Debug, PartialEq)]
pub struct NoticePage {
    pub notices: Vec<Notice>,
    /// The last row read; `None` when nothing came after the position.
    pub last: Option<Key>,
}

/// Reads up to `limit` notices after `after`, waiting up to `wait` for the first one.
#[derive(Debug, PartialEq)]
pub struct FeedOperation {
    after: Key,
    limit: usize,
    wait: Duration,
    waited: Duration,
}

impl FeedOperation {
    pub fn new(after: Key, limit: usize, wait: Duration) -> Self {
        Self {
            after,
            limit: limit.max(1),
            wait,
            waited: Duration::ZERO,
        }
    }

    fn page(&self) -> IoCall {
        IoCall::Page {
            space: APPLETS_KEYSPACE.to_string(),
            prefix: notice_prefix(),
            after: Some(self.after.clone()),
            limit: self.limit,
        }
    }
}

impl Operation for FeedOperation {
    type Output = NoticePage;
    type Error = AppletError;

    fn first(&mut self) -> Result<Next<NoticePage>, AppletError> {
        Ok(Next::Call(self.page()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<NoticePage>, AppletError> {
        if result == IoResult::Woke {
            self.waited += POLL;
            return Ok(Next::Call(self.page()));
        }
        let (rows, _) = result.page()?;
        if rows.is_empty() && self.waited < self.wait {
            return Ok(Next::Call(IoCall::Delay(POLL)));
        }
        let last = rows.last().map(|(key, _)| key.clone());
        let mut notices = Vec::with_capacity(rows.len());
        for (_, value) in rows {
            notices.push(Notice::from_bytes(&value)?);
        }
        Ok(Next::Done(NoticePage { notices, last }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::applet::notices::notice_key;
    use byteview::ByteView;
    use ulid::Ulid;

    // An empty page waits and reads again; rows end the feed with the last row as position.
    #[test]
    fn waits_then_answers() {
        let mut feed = FeedOperation::new(notice_prefix(), 10, Duration::from_secs(2));
        assert!(matches!(feed.first(), Ok(Next::Call(IoCall::Page { .. }))));
        let empty = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert!(matches!(feed.next(empty), Ok(Next::Call(IoCall::Delay(_)))));
        assert!(matches!(
            feed.next(IoResult::Woke),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        let notice = Notice {
            applet_id: Ulid::from_bytes([1; 16]),
            hosts: vec!["bakta.apps.example.org".into()],
        };
        let key = notice_key(5, notice.applet_id, 2);
        let rows = vec![(key.clone(), ByteView::from(notice.to_bytes().unwrap()))];
        let Ok(Next::Done(page)) = feed.next(IoResult::Page { rows, next: None }) else {
            panic!("the feed answers its rows");
        };
        assert_eq!((page.notices, page.last), (vec![notice], Some(key)));
    }
}
