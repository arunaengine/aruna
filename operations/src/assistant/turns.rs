use super::{
    BUDGET_CAP, ChatStoreError, TURN_CAP, abort_effects, commit, decode_heads, head_key,
    iter_heads, iter_turns, live_head, read_head, turn_key, unexpected,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{ASSISTANT_CHAT_HEAD_KEYSPACE, ASSISTANT_CHAT_TURN_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    AssistantChatHead, AssistantChatTurn, MAX_ASSISTANT_CHAT_BYTES, MAX_ASSISTANT_CHAT_TURNS,
    MAX_ASSISTANT_TURN_BYTES,
};
use aruna_core::types::{Effects, Key, TxnId, UserId};
use smallvec::smallvec;

#[derive(Clone, Debug, PartialEq, Eq)]
enum ReadTurnsState {
    Init,
    ReadHead,
    IterTurns,
    Finish,
    Error,
}

/// Reads the live turns of one chat in seq order, optionally only those after
/// a seq the caller already holds.
#[derive(Debug, PartialEq)]
pub struct ReadChatTurnsOperation {
    user_id: UserId,
    chat_id: String,
    after: Option<u32>,
    state: ReadTurnsState,
    output: Option<Result<Vec<AssistantChatTurn>, ChatStoreError>>,
}

impl ReadChatTurnsOperation {
    pub fn new(user_id: UserId, chat_id: String, after: Option<u32>) -> Self {
        Self {
            user_id,
            chat_id,
            after,
            state: ReadTurnsState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ChatStoreError) -> Effects {
        self.state = ReadTurnsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for ReadChatTurnsOperation {
    type Output = Vec<AssistantChatTurn>;
    type Error = ChatStoreError;

    fn start(&mut self) -> Effects {
        self.state = ReadTurnsState::ReadHead;
        smallvec![read_head(self.user_id, &self.chat_id, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state {
            ReadTurnsState::Init => self.start(),
            ReadTurnsState::ReadHead => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(unexpected(&self.state, "chat head read", &event));
                };
                let head = match live_head(value) {
                    Ok(head) => head,
                    Err(error) => return self.fail(error),
                };
                let from = self.after.map_or(head.first_seq, |after| {
                    after.saturating_add(1).max(head.first_seq)
                });
                self.state = ReadTurnsState::IterTurns;
                smallvec![iter_turns(
                    self.user_id,
                    &self.chat_id,
                    from,
                    usize::MAX,
                    None
                )]
            }
            ReadTurnsState::IterTurns => {
                let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
                    return self.fail(unexpected(&self.state, "chat turn iteration", &event));
                };
                let turns = values
                    .into_iter()
                    .map(|(_, value)| AssistantChatTurn::from_bytes(value.as_ref()))
                    .collect::<Result<Vec<_>, _>>();
                self.state = ReadTurnsState::Finish;
                self.output = Some(turns.map_err(Into::into));
                smallvec![]
            }
            ReadTurnsState::Finish | ReadTurnsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReadTurnsState::Finish | ReadTurnsState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ChatStoreError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum WriteTurnState {
    Init,
    StartTransaction,
    ReadHead,
    ReadTurns { head: AssistantChatHead },
    SumBytes { head: AssistantChatHead },
    TrimTurns { head: AssistantChatHead },
    WriteAll,
    CommitTransaction,
    Finish,
    Error,
}

/// Appends one turn to a chat or rewrites its tail turn. The chat keeps its
/// newest `MAX_ASSISTANT_CHAT_TURNS` turns, and all chats of the user stay
/// within `MAX_ASSISTANT_CHAT_BYTES` together.
#[derive(Debug, PartialEq)]
pub struct WriteChatTurnOperation {
    user_id: UserId,
    chat_id: String,
    seq: u32,
    payload: String,
    expected_revision: Option<u64>,
    now: u64,
    txn_id: Option<TxnId>,
    /// Payload bytes of the turns this write replaces or drops.
    freed: u64,
    /// Turn keys dropped to keep the chat within its turn cap.
    trim: Vec<Key>,
    state: WriteTurnState,
    output: Option<Result<AssistantChatHead, ChatStoreError>>,
}

impl WriteChatTurnOperation {
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
            txn_id: None,
            freed: 0,
            trim: Vec::new(),
            state: WriteTurnState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ChatStoreError) -> Effects {
        let cleanup = self.abort();
        self.state = WriteTurnState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.fail(unexpected(&self.state, "transaction started", &event));
        };
        self.txn_id = Some(txn_id);
        self.state = WriteTurnState::ReadHead;
        smallvec![read_head(self.user_id, &self.chat_id, Some(txn_id))]
    }

    fn handle_head(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(unexpected(&self.state, "chat head read", &event));
        };
        let head = match live_head(value) {
            Ok(head) => head,
            Err(error) => return self.fail(error),
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        // A revision from an older read means another browser wrote in between.
        if let Some(expected) = self.expected_revision
            && expected != head.revision
        {
            return self.fail(ChatStoreError::StaleTurn {
                next_seq: head.next_seq,
            });
        }
        let read = if self.seq == head.next_seq {
            let over = head
                .next_seq
                .saturating_sub(head.first_seq)
                .saturating_add(1)
                .saturating_sub(MAX_ASSISTANT_CHAT_TURNS);
            if over == 0 {
                self.state = WriteTurnState::SumBytes { head };
                return smallvec![iter_heads(self.user_id, Some(txn_id))];
            }
            iter_turns(
                self.user_id,
                &self.chat_id,
                head.first_seq,
                over as usize,
                Some(txn_id),
            )
        } else if head.next_seq.checked_sub(1) == Some(self.seq) {
            iter_turns(self.user_id, &self.chat_id, self.seq, 1, Some(txn_id))
        } else {
            return self.fail(ChatStoreError::StaleTurn {
                next_seq: head.next_seq,
            });
        };
        self.state = WriteTurnState::ReadTurns { head };
        smallvec![read]
    }

    fn handle_turns(&mut self, head: AssistantChatHead, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.fail(unexpected(&self.state, "chat turn iteration", &event));
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        let mut keys = Vec::with_capacity(values.len());
        for (key, value) in values {
            match AssistantChatTurn::from_bytes(value.as_ref()) {
                Ok(turn) => self.freed = self.freed.saturating_add(turn.payload.len() as u64),
                Err(error) => return self.fail(error.into()),
            }
            keys.push(key);
        }
        if self.seq == head.next_seq {
            self.trim = keys;
        }
        self.state = WriteTurnState::SumBytes { head };
        smallvec![iter_heads(self.user_id, Some(txn_id))]
    }

    fn handle_sum(&mut self, head: AssistantChatHead, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.fail(unexpected(&self.state, "chat head iteration", &event));
        };
        let held: u64 = match decode_heads(values) {
            Ok(heads) => heads
                .iter()
                .filter(|head| head.is_live())
                .map(|head| head.bytes)
                .sum(),
            Err(error) => return self.fail(error),
        };
        let total = held
            .saturating_sub(self.freed)
            .saturating_add(self.payload.len() as u64);
        if total > MAX_ASSISTANT_CHAT_BYTES {
            return self.fail(ChatStoreError::TooLarge(BUDGET_CAP));
        }
        if self.trim.is_empty() {
            return self.write_all(head);
        }
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        let deletes = self
            .trim
            .iter()
            .map(|key| (ASSISTANT_CHAT_TURN_KEYSPACE.to_string(), key.clone()))
            .collect();
        self.state = WriteTurnState::TrimTurns { head };
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(txn_id),
        })]
    }

    fn handle_trimmed(&mut self, head: AssistantChatHead, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.fail(unexpected(&self.state, "chat turn batch delete", &event));
        };
        self.write_all(head)
    }

    fn write_all(&mut self, mut head: AssistantChatHead) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
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
        let encoded = turn
            .to_bytes()
            .and_then(|turn| head.to_bytes().map(|head| (turn, head)));
        let (turn_bytes, head_bytes) = match encoded {
            Ok(encoded) => encoded,
            Err(error) => return self.fail(error.into()),
        };
        self.state = WriteTurnState::WriteAll;
        self.output = Some(Ok(head));
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (
                    ASSISTANT_CHAT_TURN_KEYSPACE.to_string(),
                    turn_key(self.user_id, &self.chat_id, self.seq),
                    turn_bytes.into(),
                ),
                (
                    ASSISTANT_CHAT_HEAD_KEYSPACE.to_string(),
                    head_key(self.user_id, &self.chat_id),
                    head_bytes.into(),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.fail(unexpected(&self.state, "chat batch write", &event));
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        self.state = WriteTurnState::CommitTransaction;
        commit(txn_id)
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.fail(unexpected(&self.state, "transaction committed", &event));
        };
        self.txn_id = None;
        self.state = WriteTurnState::Finish;
        smallvec![]
    }
}

impl Operation for WriteChatTurnOperation {
    type Output = AssistantChatHead;
    type Error = ChatStoreError;

    fn start(&mut self) -> Effects {
        if self.payload.len() > MAX_ASSISTANT_TURN_BYTES {
            return self.fail(ChatStoreError::TooLarge(TURN_CAP));
        }
        self.state = WriteTurnState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state.clone() {
            WriteTurnState::Init => self.start(),
            WriteTurnState::StartTransaction => self.handle_started(event),
            WriteTurnState::ReadHead => self.handle_head(event),
            WriteTurnState::ReadTurns { head } => self.handle_turns(head, event),
            WriteTurnState::SumBytes { head } => self.handle_sum(head, event),
            WriteTurnState::TrimTurns { head } => self.handle_trimmed(head, event),
            WriteTurnState::WriteAll => self.handle_written(event),
            WriteTurnState::CommitTransaction => self.handle_committed(event),
            WriteTurnState::Finish | WriteTurnState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, WriteTurnState::Finish | WriteTurnState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ChatStoreError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        abort_effects(&mut self.txn_id)
    }
}
