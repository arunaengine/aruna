//! Reads chat turns and appends or rewrites the tail turn within the turn and byte caps.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    BUDGET_CAP, ChatStoreError, TURN_CAP, decode_heads, head_key, heads_page, live_head,
    read_head, turn_key, turns_page,
};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{CHAT_HEAD_KEYSPACE, CHAT_TURN_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    AssistantChatHead, AssistantChatTurn, MAX_ASSISTANT_BYTES, MAX_ASSISTANT_TURNS, MAX_TURN_BYTES,
};
use aruna_core::types::{Key, Value};

/// Reads the live turns of one chat in seq order, optionally only those after
/// a seq the caller already holds.
#[derive(Debug, PartialEq)]
pub struct ReadChatOperation {
    user_id: UserId,
    chat_id: String,
    after: Option<u32>,
    /// Whether the head was read and the turns are next.
    turns: bool,
}

impl ReadChatOperation {
    pub fn new(user_id: UserId, chat_id: String, after: Option<u32>) -> Self {
        Self {
            user_id,
            chat_id,
            after,
            turns: false,
        }
    }
}

impl Operation for ReadChatOperation {
    type Output = Vec<AssistantChatTurn>;
    type Error = ChatStoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, ChatStoreError> {
        Ok(Next::Call(read_head(self.user_id, &self.chat_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ChatStoreError> {
        if self.turns {
            let (rows, _) = result.page()?;
            let turns = rows
                .into_iter()
                .map(|(_, value)| AssistantChatTurn::from_bytes(value.as_ref()))
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(Next::Done(turns));
        }
        let head = live_head(result.value()?)?;
        let from = self.after.map_or(head.first_seq, |after| {
            after.saturating_add(1).max(head.first_seq)
        });
        self.turns = true;
        Ok(Next::Call(turns_page(self.user_id, &self.chat_id, from, usize::MAX)))
    }
}

/// What a turn write waits for.
#[derive(Debug, PartialEq)]
enum WriteTurnStage {
    Head,
    Turns(AssistantChatHead),
    Sum(AssistantChatHead),
    Trimmed(AssistantChatHead),
    Written(AssistantChatHead),
}

/// Appends one turn to a chat or rewrites its tail turn. The chat keeps its
/// newest `MAX_ASSISTANT_TURNS` turns, and all chats of the user stay
/// within `MAX_ASSISTANT_BYTES` together.
#[derive(Debug, PartialEq)]
pub struct WriteTurnOperation {
    user_id: UserId,
    chat_id: String,
    seq: u32,
    payload: String,
    expected_revision: Option<u64>,
    now: u64,
    /// Payload bytes of the turns this write replaces or drops.
    freed: u64,
    /// Turn keys dropped to keep the chat within its turn cap.
    trim: Vec<Key>,
    stage: WriteTurnStage,
}

impl WriteTurnOperation {
    pub fn new(
        user_id: UserId,
        chat_id: String,
        seq: u32,
        payload: String,
        expected_revision: Option<u64>,
        now: u64,
    ) -> Self {
        Self {
            user_id,
            chat_id,
            seq,
            payload,
            expected_revision,
            now,
            freed: 0,
            trim: Vec::new(),
            stage: WriteTurnStage::Head,
        }
    }

    fn head(&mut self, head: AssistantChatHead) -> Result<Next<AssistantChatHead>, ChatStoreError> {
        // A revision from an older read means another browser wrote in between.
        if self.expected_revision.is_some_and(|expected| expected != head.revision) {
            return Err(ChatStoreError::StaleTurn {
                next_seq: head.next_seq,
            });
        }
        let read = if self.seq == head.next_seq {
            let over = head
                .next_seq
                .saturating_sub(head.first_seq)
                .saturating_add(1)
                .saturating_sub(MAX_ASSISTANT_TURNS);
            if over == 0 {
                self.stage = WriteTurnStage::Sum(head);
                return Ok(Next::Call(heads_page(self.user_id)));
            }
            turns_page(self.user_id, &self.chat_id, head.first_seq, over as usize)
        } else if head.next_seq.checked_sub(1) == Some(self.seq) {
            turns_page(self.user_id, &self.chat_id, self.seq, 1)
        } else {
            return Err(ChatStoreError::StaleTurn {
                next_seq: head.next_seq,
            });
        };
        self.stage = WriteTurnStage::Turns(head);
        Ok(Next::Call(read))
    }

    fn turns(
        &mut self,
        head: AssistantChatHead,
        rows: Vec<(Key, Value)>,
    ) -> Result<Next<AssistantChatHead>, ChatStoreError> {
        let mut keys = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            let turn = AssistantChatTurn::from_bytes(value.as_ref())?;
            self.freed = self.freed.saturating_add(turn.payload.len() as u64);
            keys.push(key);
        }
        if self.seq == head.next_seq {
            self.trim = keys;
        }
        self.stage = WriteTurnStage::Sum(head);
        Ok(Next::Call(heads_page(self.user_id)))
    }

    fn sum(
        &mut self,
        head: AssistantChatHead,
        rows: Vec<(Key, Value)>,
    ) -> Result<Next<AssistantChatHead>, ChatStoreError> {
        let held: u64 = decode_heads(rows)?
            .iter()
            .filter(|head| head.is_live())
            .map(|head| head.bytes)
            .sum();
        let total = held
            .saturating_sub(self.freed)
            .saturating_add(self.payload.len() as u64);
        if total > MAX_ASSISTANT_BYTES {
            return Err(ChatStoreError::TooLarge(BUDGET_CAP));
        }
        if self.trim.is_empty() {
            return self.write_all(head);
        }
        let deletes = (self.trim.iter())
            .map(|key| (CHAT_TURN_KEYSPACE.to_string(), key.clone()))
            .collect();
        self.stage = WriteTurnStage::Trimmed(head);
        Ok(Next::Call(IoCall::Delete(deletes)))
    }

    fn write_all(
        &mut self,
        mut head: AssistantChatHead,
    ) -> Result<Next<AssistantChatHead>, ChatStoreError> {
        let turn = AssistantChatTurn {
            seq: self.seq,
            payload: std::mem::take(&mut self.payload),
            updated_at: self.now,
        };
        let trimmed = u32::try_from(self.trim.len()).unwrap_or(u32::MAX);
        head.next_seq = head.next_seq.max(self.seq.saturating_add(1));
        head.first_seq = head.first_seq.saturating_add(trimmed);
        head.bytes = head
            .bytes
            .saturating_sub(self.freed)
            .saturating_add(turn.payload.len() as u64);
        head.updated_at = self.now;
        head.revision = head.revision.saturating_add(1);
        let writes = vec![
            (
                CHAT_TURN_KEYSPACE.to_string(),
                turn_key(self.user_id, &self.chat_id, self.seq),
                turn.to_bytes()?.into(),
            ),
            (
                CHAT_HEAD_KEYSPACE.to_string(),
                head_key(self.user_id, &self.chat_id),
                head.to_bytes()?.into(),
            ),
        ];
        self.stage = WriteTurnStage::Written(head);
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for WriteTurnOperation {
    type Output = AssistantChatHead;
    type Error = ChatStoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, ChatStoreError> {
        if self.payload.len() > MAX_TURN_BYTES {
            return Err(ChatStoreError::TooLarge(TURN_CAP));
        }
        Ok(Next::Call(read_head(self.user_id, &self.chat_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ChatStoreError> {
        match std::mem::replace(&mut self.stage, WriteTurnStage::Head) {
            WriteTurnStage::Head => {
                let head = live_head(result.value()?)?;
                self.head(head)
            }
            WriteTurnStage::Turns(head) => self.turns(head, result.page()?.0),
            WriteTurnStage::Sum(head) => self.sum(head, result.page()?.0),
            WriteTurnStage::Trimmed(head) => {
                result.done()?;
                self.write_all(head)
            }
            WriteTurnStage::Written(head) => {
                result.done()?;
                Ok(Next::Done(head))
            }
        }
    }

    fn transaction(&self) -> bool {
        true
    }
}
