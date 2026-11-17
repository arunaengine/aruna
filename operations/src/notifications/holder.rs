//! Reads a recipient's inbox on its holder as the recipient sees it: a watch notice shows only
//! while its watch and the resource it names are still authorized.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationRecord,
};
use thiserror::Error;
use ulid::Ulid;

use crate::notifications::list::{
    LIST_MAX_LIMIT, ListNotificationsError, ListNotificationsInput, ListNotificationsOperation,
    ListNotificationsOutput, SCAN_MAX_ROWS, UNREAD_COUNT_CAP,
};
use crate::notifications::mark_read::{
    MARK_MAX_IDS, MarkReadError, MarkReadInput, MarkReadOperation, MarkReadOutput,
};
use crate::notifications::watch::check::{WatchCheckError, WatchCheckOperation};
use crate::notifications::watch::paths::WatchAuthorization;

#[derive(Debug, Error, PartialEq)]
pub enum HolderReadError {
    #[error("mark read id count exceeds cap")]
    TooManyIds,
    /// A watch notice could not be checked now.
    #[error("{0}")]
    Unavailable(String),
    #[error(transparent)]
    List(#[from] ListNotificationsError),
    #[error(transparent)]
    Mark(#[from] MarkReadError),
    #[error(transparent)]
    Check(#[from] WatchCheckError),
    #[error(transparent)]
    Io(#[from] IoError),
}

/// What the recipient asks of its inbox.
#[derive(Clone, Debug, PartialEq)]
pub enum HolderRead {
    /// At most `limit` visible records after `cursor`, newest first.
    List {
        cursor: Option<Vec<u8>>,
        limit: usize,
    },
    /// The visible unread records, counted up to a cap.
    Unread,
    /// Marks the visible unread records it names, by id or up to a time, read at `now_ms`.
    Mark {
        ids: Vec<Ulid>,
        up_to_ms: Option<u64>,
        now_ms: u64,
    },
}

#[derive(Debug, PartialEq)]
pub enum HolderAnswer {
    Listed {
        records: Vec<NotificationRecord>,
        next_cursor: Option<Vec<u8>>,
    },
    Unread {
        count: u32,
        capped: bool,
    },
    Marked(u32),
}

/// Whether a record shows without a check, or the watch check that decides it.
fn visibility(recipient: UserId, record: &NotificationRecord) -> Result<bool, WatchCheckOperation> {
    let watched = matches!(
        record.kind,
        NotificationKind::MetadataCreated { .. }
            | NotificationKind::DataUploaded { .. }
            | NotificationKind::SyncCompleted { .. }
            | NotificationKind::SyncFailed { .. }
    );
    if !watched {
        return Ok(record.watch_authorization.is_none());
    }
    let Some(binding) = record.watch_authorization.as_ref() else {
        return Ok(false);
    };
    if record.class != NotificationClass::Transient || record.recipient != recipient {
        return Ok(false);
    }
    Err(WatchCheckOperation::notification(
        recipient,
        &record.kind,
        binding,
    ))
}

#[derive(Debug, Default, PartialEq)]
enum HolderState {
    #[default]
    Page,
    Check,
    Mark,
}

/// Pages the inbox and checks each watch notice before it counts. For a list, hidden records
/// do not use up the page, so a revoked watch cannot hide older notices behind a page of stale
/// ones; marking lists full pages and marks the visible records it names in chunks.
#[derive(Debug, PartialEq)]
pub struct HolderReadOperation {
    recipient: UserId,
    read: HolderRead,
    state: HolderState,
    /// The page cursor of the next inbox page.
    cursor: Option<Vec<u8>>,
    next_cursor: Option<Vec<u8>>,
    /// The records of the current page still to check, and the visible ones so far.
    pending: VecDeque<NotificationRecord>,
    current: Option<NotificationRecord>,
    visible: Vec<NotificationRecord>,
    examined: usize,
    count: usize,
    marks: VecDeque<Vec<Ulid>>,
    marked: u32,
}

type Turn = Result<Next<HolderAnswer>, HolderReadError>;

impl HolderReadOperation {
    pub fn new(recipient: UserId, read: HolderRead) -> Self {
        let cursor = match &read {
            HolderRead::List { cursor, .. } => cursor.clone(),
            HolderRead::Unread | HolderRead::Mark { .. } => None,
        };
        Self {
            recipient,
            read,
            state: HolderState::Page,
            cursor,
            next_cursor: None,
            pending: VecDeque::new(),
            current: None,
            visible: Vec::new(),
            examined: 0,
            count: 0,
            marks: VecDeque::new(),
            marked: 0,
        }
    }

    /// The visible records one list or mark batch fills.
    fn batch_limit(&self) -> usize {
        match &self.read {
            HolderRead::List { limit, .. } => (*limit).clamp(1, LIST_MAX_LIMIT),
            HolderRead::Unread | HolderRead::Mark { .. } => LIST_MAX_LIMIT,
        }
    }

    fn page(&mut self) -> Turn {
        let limit = match self.read {
            HolderRead::Unread => UNREAD_COUNT_CAP.min(SCAN_MAX_ROWS - self.examined),
            _ => self.batch_limit() - self.visible.len(),
        };
        self.state = HolderState::Page;
        let list = ListNotificationsOperation::new(ListNotificationsInput {
            recipient: self.recipient,
            cursor: self.cursor.take(),
            limit,
        });
        Ok(Next::Call(IoCall::child(list)))
    }

    fn paged(&mut self, output: ListNotificationsOutput) -> Turn {
        self.examined += output.records.len();
        self.next_cursor = output.next_cursor;
        self.pending = output.records.into();
        self.check_next()
    }

    /// Checks the next record of the page, or ends the page once none is left.
    fn check_next(&mut self) -> Turn {
        while let Some(record) = self.pending.pop_front() {
            if matches!(self.read, HolderRead::Unread) && record.read_at_ms.is_some() {
                continue;
            }
            match visibility(self.recipient, &record) {
                Ok(true) => {
                    if let Some(done) = self.shown(record) {
                        return done;
                    }
                }
                Ok(false) => {}
                Err(check) => {
                    self.current = Some(record);
                    self.state = HolderState::Check;
                    return Ok(Next::Call(IoCall::child(check)));
                }
            }
        }
        self.page_done()
    }

    /// Counts a visible record; an unread count may end here at its cap.
    fn shown(&mut self, record: NotificationRecord) -> Option<Turn> {
        if !matches!(self.read, HolderRead::Unread) {
            self.visible.push(record);
            return None;
        }
        if self.count == UNREAD_COUNT_CAP {
            return Some(Ok(Next::Done(HolderAnswer::Unread {
                count: self.count as u32,
                capped: true,
            })));
        }
        self.count += 1;
        None
    }

    fn checked(&mut self, check: Result<WatchAuthorization, WatchCheckError>) -> Turn {
        let record = self.current.take().ok_or(IoError::Unfinished)?;
        match check? {
            WatchAuthorization::Authorized => {
                if let Some(done) = self.shown(record) {
                    return done;
                }
            }
            WatchAuthorization::Denied(_) => {}
            WatchAuthorization::Unavailable(error) => {
                return Err(HolderReadError::Unavailable(error));
            }
        }
        self.check_next()
    }

    fn page_done(&mut self) -> Turn {
        let next = self.next_cursor.take();
        match &self.read {
            HolderRead::Unread => {
                let capped = self.examined >= SCAN_MAX_ROWS && next.is_some();
                match next {
                    Some(next) if !capped => {
                        self.cursor = Some(next);
                        self.page()
                    }
                    _ => Ok(Next::Done(HolderAnswer::Unread {
                        count: self.count as u32,
                        capped,
                    })),
                }
            }
            HolderRead::List { .. } | HolderRead::Mark { .. } => {
                if self.visible.len() < self.batch_limit() && next.is_some() {
                    self.cursor = next;
                    return self.page();
                }
                if !matches!(self.read, HolderRead::Mark { .. }) {
                    let records = std::mem::take(&mut self.visible);
                    return Ok(Next::Done(HolderAnswer::Listed {
                        records,
                        next_cursor: next,
                    }));
                }
                self.next_cursor = next;
                self.plan_marks()
            }
        }
    }

    /// Marks the visible unread records of a full batch the request names, in chunks.
    fn plan_marks(&mut self) -> Turn {
        let HolderRead::Mark { ids, up_to_ms, .. } = &self.read else {
            return Err(IoError::Unfinished.into());
        };
        let named: Vec<Ulid> = std::mem::take(&mut self.visible)
            .into_iter()
            .filter(|record| {
                record.read_at_ms.is_none()
                    && (ids.binary_search(&record.notification_id).is_ok()
                        || up_to_ms.is_some_and(|limit| record.created_at_ms <= limit))
            })
            .map(|record| record.notification_id)
            .collect();
        self.marks = named.chunks(MARK_MAX_IDS).map(<[Ulid]>::to_vec).collect();
        self.mark_next()
    }

    fn mark_next(&mut self) -> Turn {
        let HolderRead::Mark { now_ms, .. } = self.read else {
            return Err(IoError::Unfinished.into());
        };
        if let Some(chunk) = self.marks.pop_front() {
            self.state = HolderState::Mark;
            let mark = MarkReadOperation::new(MarkReadInput {
                recipient: self.recipient,
                ids: chunk,
                up_to_ms: None,
                now_ms,
            });
            return Ok(Next::Call(IoCall::child(mark)));
        }
        match self.next_cursor.take() {
            Some(next) => {
                self.cursor = Some(next);
                self.page()
            }
            None => Ok(Next::Done(HolderAnswer::Marked(self.marked))),
        }
    }
}

impl Operation for HolderReadOperation {
    type Output = HolderAnswer;
    type Error = HolderReadError;

    fn first(&mut self) -> Turn {
        if let HolderRead::Mark { ids, up_to_ms, .. } = &mut self.read {
            if ids.len() > MARK_MAX_IDS {
                return Err(HolderReadError::TooManyIds);
            }
            if ids.is_empty() && up_to_ms.is_none() {
                return Ok(Next::Done(HolderAnswer::Marked(0)));
            }
            ids.sort_unstable();
            ids.dedup();
        }
        self.page()
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.state {
            HolderState::Page => {
                let output =
                    result.child::<ListNotificationsOutput, ListNotificationsError>()??;
                self.paged(output)
            }
            HolderState::Check => {
                self.checked(result.child::<WatchAuthorization, WatchCheckError>()?)
            }
            HolderState::Mark => {
                let output = result.child::<MarkReadOutput, MarkReadError>()??;
                self.marked += output.marked as u32;
                self.mark_next()
            }
        }
    }
}

#[cfg(test)]
#[path = "holder_tests.rs"]
mod tests;
