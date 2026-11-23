//! Operations that list, create, rename and tombstone the chat heads of one user.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    CHAT_CAP, ChatStoreError, TotalsRead, TotalsStep, decode_head, decode_heads, head_key,
    heads_page, read_head, totals_entry, turns_page,
};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{CHAT_HEAD_KEYSPACE, CHAT_TURN_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::assistant_chat::{
    AssistantChatHead, AssistantChatTotals, MAX_ASSISTANT_CHATS,
};
use aruna_core::user::id::UserId;

/// Lists one user's live chat heads, newest change first.
#[derive(Debug, PartialEq)]
pub struct ListChatOperation {
    user_id: UserId,
}

impl ListChatOperation {
    pub fn new(user_id: UserId) -> Self {
        Self { user_id }
    }
}

impl Operation for ListChatOperation {
    type Output = Vec<AssistantChatHead>;
    type Error = ChatStoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, ChatStoreError> {
        Ok(Next::Call(heads_page(self.user_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ChatStoreError> {
        let (rows, _) = result.page()?;
        let mut heads = decode_heads(rows)?;
        heads.retain(AssistantChatHead::is_live);
        heads.sort_by_key(|head| std::cmp::Reverse(head.updated_at));
        Ok(Next::Done(heads))
    }
}

/// What a head write waits for.
#[derive(Debug, PartialEq)]
enum WriteHeadStage {
    Head,
    Totals(TotalsRead),
    Written(AssistantChatHead),
}

/// Creates or renames one chat. A rename is refused when the caller read an
/// older revision than the node holds, so a second browser cannot silently
/// drop what the first one saved.
#[derive(Debug, PartialEq)]
pub struct WriteChatOperation {
    user_id: UserId,
    chat_id: String,
    title: String,
    subject: Option<String>,
    expected_revision: Option<u64>,
    now: u64,
    stage: WriteHeadStage,
}

impl WriteChatOperation {
    pub fn new(
        user_id: UserId,
        chat_id: String,
        title: String,
        subject: Option<String>,
        expected_revision: Option<u64>,
        now: u64,
    ) -> Self {
        Self {
            user_id,
            chat_id,
            title,
            subject,
            expected_revision,
            now,
            stage: WriteHeadStage::Head,
        }
    }

    fn rename(
        &mut self,
        mut head: AssistantChatHead,
    ) -> Result<Next<AssistantChatHead>, ChatStoreError> {
        if !head.is_live() {
            return Err(ChatStoreError::Deleted);
        }
        // No expectation writes over whatever is there; an expectation must match.
        if self
            .expected_revision
            .is_some_and(|expected| expected != head.revision)
        {
            return Err(ChatStoreError::Stale);
        }
        head.title = std::mem::take(&mut self.title);
        head.subject = self.subject.take();
        head.revision = head.revision.saturating_add(1);
        head.updated_at = self.now;
        self.write(head, None)
    }

    fn create(
        &mut self,
        totals: AssistantChatTotals,
    ) -> Result<Next<AssistantChatHead>, ChatStoreError> {
        if totals.chats as usize >= MAX_ASSISTANT_CHATS {
            return Err(ChatStoreError::TooLarge(CHAT_CAP));
        }
        let head = AssistantChatHead {
            user_id: self.user_id,
            chat_id: self.chat_id.clone(),
            title: std::mem::take(&mut self.title),
            subject: self.subject.take(),
            created_at: self.now,
            updated_at: self.now,
            first_seq: 0,
            next_seq: 0,
            bytes: 0,
            revision: 1,
            deleted_at: None,
        };
        let counted = AssistantChatTotals {
            chats: totals.chats.saturating_add(1),
            ..totals
        };
        self.write(head, Some(counted))
    }

    /// Writes the head, with the user's totals when the write changes them.
    fn write(
        &mut self,
        head: AssistantChatHead,
        totals: Option<AssistantChatTotals>,
    ) -> Result<Next<AssistantChatHead>, ChatStoreError> {
        let mut rows = vec![(
            CHAT_HEAD_KEYSPACE.to_string(),
            head_key(self.user_id, &self.chat_id),
            head.to_bytes()?.into(),
        )];
        if let Some(totals) = totals {
            rows.push(totals_entry(self.user_id, totals)?);
        }
        self.stage = WriteHeadStage::Written(head);
        Ok(Next::Call(IoCall::Write(rows)))
    }
}

impl Operation for WriteChatOperation {
    type Output = AssistantChatHead;
    type Error = ChatStoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, ChatStoreError> {
        Ok(Next::Call(read_head(self.user_id, &self.chat_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ChatStoreError> {
        match std::mem::replace(&mut self.stage, WriteHeadStage::Head) {
            WriteHeadStage::Head => match decode_head(result.value()?)? {
                Some(head) => self.rename(head),
                None => {
                    let totals = TotalsRead::new(self.user_id);
                    let call = totals.call();
                    self.stage = WriteHeadStage::Totals(totals);
                    Ok(Next::Call(call))
                }
            },
            WriteHeadStage::Totals(mut totals) => match totals.next(result)? {
                TotalsStep::Call(call) => {
                    self.stage = WriteHeadStage::Totals(totals);
                    Ok(Next::Call(call))
                }
                TotalsStep::Done(counted) => self.create(counted),
            },
            WriteHeadStage::Written(head) => {
                result.done()?;
                Ok(Next::Done(head))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// What a chat delete waits for.
#[derive(Debug, PartialEq)]
enum DeleteChatStage {
    Head,
    Turns(AssistantChatHead),
    Deleted(AssistantChatHead),
    Totals(AssistantChatHead, TotalsRead),
    Written,
}

/// Drops the turns of one chat and leaves its head as a tombstone. Deleting an
/// unknown or already deleted chat finishes without a write.
#[derive(Debug, PartialEq)]
pub struct DeleteChatOperation {
    user_id: UserId,
    chat_id: String,
    now: u64,
    stage: DeleteChatStage,
}

impl DeleteChatOperation {
    pub fn new(user_id: UserId, chat_id: String, now: u64) -> Self {
        Self {
            user_id,
            chat_id,
            now,
            stage: DeleteChatStage::Head,
        }
    }

    fn count(&mut self, head: AssistantChatHead) -> Result<Next<()>, ChatStoreError> {
        let totals = TotalsRead::new(self.user_id);
        let call = totals.call();
        self.stage = DeleteChatStage::Totals(head, totals);
        Ok(Next::Call(call))
    }

    /// Leaves the head as a tombstone and takes its chat and bytes off the user's totals.
    fn tombstone(
        &mut self,
        mut head: AssistantChatHead,
        totals: AssistantChatTotals,
    ) -> Result<Next<()>, ChatStoreError> {
        let left = AssistantChatTotals {
            chats: totals.chats.saturating_sub(1),
            bytes: totals.bytes.saturating_sub(head.bytes),
        };
        head.deleted_at = Some(self.now);
        head.bytes = 0;
        head.first_seq = head.next_seq;
        head.updated_at = self.now;
        head.revision = head.revision.saturating_add(1);
        let row = (
            CHAT_HEAD_KEYSPACE.to_string(),
            head_key(self.user_id, &self.chat_id),
            head.to_bytes()?.into(),
        );
        let rows = vec![row, totals_entry(self.user_id, left)?];
        self.stage = DeleteChatStage::Written;
        Ok(Next::Call(IoCall::Write(rows)))
    }
}

impl Operation for DeleteChatOperation {
    type Output = ();
    type Error = ChatStoreError;

    fn first(&mut self) -> Result<Next<()>, ChatStoreError> {
        Ok(Next::Call(read_head(self.user_id, &self.chat_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ChatStoreError> {
        match std::mem::replace(&mut self.stage, DeleteChatStage::Written) {
            DeleteChatStage::Head => match decode_head(result.value()?)? {
                Some(head) if head.is_live() => {
                    let turns = turns_page(self.user_id, &self.chat_id, head.first_seq, usize::MAX);
                    self.stage = DeleteChatStage::Turns(head);
                    Ok(Next::Call(turns))
                }
                _ => Ok(Next::Done(())),
            },
            DeleteChatStage::Turns(head) => {
                let (rows, _) = result.page()?;
                if rows.is_empty() {
                    return self.count(head);
                }
                let deletes = rows
                    .into_iter()
                    .map(|(key, _)| (CHAT_TURN_KEYSPACE.to_string(), key))
                    .collect();
                self.stage = DeleteChatStage::Deleted(head);
                Ok(Next::Call(IoCall::Delete(deletes)))
            }
            DeleteChatStage::Deleted(head) => {
                result.done()?;
                self.count(head)
            }
            DeleteChatStage::Totals(head, mut totals) => match totals.next(result)? {
                TotalsStep::Call(call) => {
                    self.stage = DeleteChatStage::Totals(head, totals);
                    Ok(Next::Call(call))
                }
                TotalsStep::Done(counted) => self.tombstone(head, counted),
            },
            DeleteChatStage::Written => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}
