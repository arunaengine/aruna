//! Operations that list, create, rename and tombstone the chat heads of one user.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    CHAT_CAP, ChatStoreError, decode_head, decode_heads, head_key, heads_page, read_head,
    turns_page,
};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{CHAT_HEAD_KEYSPACE, CHAT_TURN_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{AssistantChatHead, MAX_ASSISTANT_CHATS};

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
    Count,
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

    fn rename(&mut self, mut head: AssistantChatHead) -> Result<Next<AssistantChatHead>, ChatStoreError> {
        if !head.is_live() {
            return Err(ChatStoreError::Deleted);
        }
        // No expectation writes over whatever is there; an expectation must match.
        if self.expected_revision.is_some_and(|expected| expected != head.revision) {
            return Err(ChatStoreError::Stale);
        }
        head.title = std::mem::take(&mut self.title);
        head.subject = self.subject.take();
        head.revision = head.revision.saturating_add(1);
        head.updated_at = self.now;
        self.write(head)
    }

    fn create(&mut self, rows: usize) -> Result<Next<AssistantChatHead>, ChatStoreError> {
        if rows >= MAX_ASSISTANT_CHATS {
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
        self.write(head)
    }

    fn write(&mut self, head: AssistantChatHead) -> Result<Next<AssistantChatHead>, ChatStoreError> {
        let row = (
            CHAT_HEAD_KEYSPACE.to_string(),
            head_key(self.user_id, &self.chat_id),
            head.to_bytes()?.into(),
        );
        self.stage = WriteHeadStage::Written(head);
        Ok(Next::Call(IoCall::Write(vec![row])))
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
                    self.stage = WriteHeadStage::Count;
                    Ok(Next::Call(heads_page(self.user_id)))
                }
            },
            WriteHeadStage::Count => {
                let (rows, _) = result.page()?;
                let live = decode_heads(rows)?.iter().filter(|head| head.is_live()).count();
                self.create(live)
            }
            WriteHeadStage::Written(head) => {
                result.done()?;
                Ok(Next::Done(head))
            }
        }
    }

    fn transaction(&self) -> bool {
        true
    }
}

/// What a chat delete waits for.
#[derive(Debug, PartialEq)]
enum DeleteChatStage {
    Head,
    Turns(AssistantChatHead),
    Deleted(AssistantChatHead),
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

    fn tombstone(&mut self, mut head: AssistantChatHead) -> Result<Next<()>, ChatStoreError> {
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
        self.stage = DeleteChatStage::Written;
        Ok(Next::Call(IoCall::Write(vec![row])))
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
                    return self.tombstone(head);
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
                self.tombstone(head)
            }
            DeleteChatStage::Written => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }

    fn transaction(&self) -> bool {
        true
    }
}
