use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{ASSISTANT_CHAT_HEAD_KEYSPACE, ASSISTANT_CHAT_TURN_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    AssistantChatHead, AssistantChatTurn, MAX_ASSISTANT_CHAT_BYTES, MAX_ASSISTANT_CHAT_TURNS,
    MAX_ASSISTANT_CHATS, MAX_ASSISTANT_TURN_BYTES,
};
use aruna_core::types::{Effects, Key, TxnId, UserId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

pub const CHAT_CAP: &str = "a user may keep at most 20 chats";
pub const TURN_CAP: &str = "a turn payload may hold at most 64 KiB";
pub const BUDGET_CAP: &str = "the chats of a user may hold at most 8 MiB together";

fn head_prefix(user_id: UserId) -> Vec<u8> {
    let mut key = user_id.to_storage_key();
    key.push(0);
    key
}

fn head_key(user_id: UserId, chat_id: &str) -> Key {
    let mut key = head_prefix(user_id);
    key.extend_from_slice(chat_id.as_bytes());
    ByteView::from(key)
}

fn turn_prefix(user_id: UserId, chat_id: &str) -> Vec<u8> {
    let mut key = head_prefix(user_id);
    key.extend_from_slice(chat_id.as_bytes());
    key.push(0);
    key
}

fn turn_key(user_id: UserId, chat_id: &str, seq: u32) -> Key {
    let mut key = turn_prefix(user_id, chat_id);
    key.extend_from_slice(&seq.to_be_bytes());
    ByteView::from(key)
}

fn read_head(user_id: UserId, chat_id: &str, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: ASSISTANT_CHAT_HEAD_KEYSPACE.to_string(),
        key: head_key(user_id, chat_id),
        txn_id,
    })
}

fn iter_heads(user_id: UserId, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: ASSISTANT_CHAT_HEAD_KEYSPACE.to_string(),
        prefix: Some(ByteView::from(head_prefix(user_id))),
        start: None,
        limit: usize::MAX,
        txn_id,
    })
}

fn iter_turns(
    user_id: UserId,
    chat_id: &str,
    seq: u32,
    limit: usize,
    txn_id: Option<TxnId>,
) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: ASSISTANT_CHAT_TURN_KEYSPACE.to_string(),
        prefix: Some(ByteView::from(turn_prefix(user_id, chat_id))),
        start: Some(IterStart::At(turn_key(user_id, chat_id, seq))),
        limit,
        txn_id,
    })
}

fn decode_head(value: Option<Value>) -> Result<Option<AssistantChatHead>, ChatStoreError> {
    Ok(value
        .map(|bytes| AssistantChatHead::from_bytes(bytes.as_ref()))
        .transpose()?)
}

/// The stored head of a chat that still takes reads and writes.
fn live_head(value: Option<Value>) -> Result<AssistantChatHead, ChatStoreError> {
    let head = decode_head(value)?.ok_or(ChatStoreError::NotFound)?;
    if head.is_live() {
        Ok(head)
    } else {
        Err(ChatStoreError::Deleted)
    }
}

fn decode_heads(values: Vec<(Key, Value)>) -> Result<Vec<AssistantChatHead>, ChatStoreError> {
    Ok(values
        .into_iter()
        .map(|(_, value)| AssistantChatHead::from_bytes(value.as_ref()))
        .collect::<Result<Vec<_>, _>>()?)
}

fn commit(txn_id: TxnId) -> Effects {
    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
}

fn abort_effects(txn_id: &mut Option<TxnId>) -> Effects {
    txn_id
        .take()
        .map_or_else(smallvec::SmallVec::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
}

#[derive(Debug, Error, PartialEq)]
pub enum ChatStoreError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("chat not found")]
    NotFound,
    #[error("chat was deleted")]
    Deleted,
    #[error("{0}")]
    TooLarge(&'static str),
    #[error("chat changed in another browser")]
    Stale,
    #[error("the next turn seq is {next_seq}")]
    StaleTurn { next_seq: u32 },
    #[error("chat operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

fn unexpected(
    state: &impl std::fmt::Debug,
    expected: &'static str,
    event: &Event,
) -> ChatStoreError {
    ChatStoreError::UnexpectedEvent {
        state: format!("{state:?}"),
        expected,
        got: format!("{event:?}"),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ListHeadsState {
    Init,
    Iter,
    Finish,
    Error,
}

/// Lists one user's live chat heads, newest change first.
#[derive(Debug, PartialEq)]
pub struct ListChatHeadsOperation {
    user_id: UserId,
    state: ListHeadsState,
    output: Option<Result<Vec<AssistantChatHead>, ChatStoreError>>,
}

impl ListChatHeadsOperation {
    pub fn new(user_id: UserId) -> Self {
        Self {
            user_id,
            state: ListHeadsState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ChatStoreError) -> Effects {
        self.state = ListHeadsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for ListChatHeadsOperation {
    type Output = Vec<AssistantChatHead>;
    type Error = ChatStoreError;

    fn start(&mut self) -> Effects {
        self.state = ListHeadsState::Iter;
        smallvec![iter_heads(self.user_id, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state {
            ListHeadsState::Init => self.start(),
            ListHeadsState::Iter => {
                let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
                    return self.fail(unexpected(&self.state, "chat head iteration", &event));
                };
                let mut heads = match decode_heads(values) {
                    Ok(heads) => heads,
                    Err(error) => return self.fail(error),
                };
                heads.retain(AssistantChatHead::is_live);
                heads.sort_by_key(|head| std::cmp::Reverse(head.updated_at));
                self.state = ListHeadsState::Finish;
                self.output = Some(Ok(heads));
                smallvec![]
            }
            ListHeadsState::Finish | ListHeadsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ListHeadsState::Finish | ListHeadsState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ChatStoreError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

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
enum WriteHeadState {
    Init,
    StartTransaction,
    ReadHead,
    CountHeads,
    WriteHead,
    CommitTransaction,
    Finish,
    Error,
}

/// Creates or renames one chat. A rename is refused when the caller read an
/// older revision than the node holds, so a second browser cannot silently
/// drop what the first one saved.
#[derive(Debug, PartialEq)]
pub struct WriteChatHeadOperation {
    user_id: UserId,
    chat_id: String,
    title: String,
    subject: Option<String>,
    expected_revision: Option<u64>,
    now: u64,
    txn_id: Option<TxnId>,
    state: WriteHeadState,
    output: Option<Result<AssistantChatHead, ChatStoreError>>,
}

impl WriteChatHeadOperation {
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
            txn_id: None,
            state: WriteHeadState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ChatStoreError) -> Effects {
        let cleanup = self.abort();
        self.state = WriteHeadState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.fail(unexpected(&self.state, "transaction started", &event));
        };
        self.txn_id = Some(txn_id);
        self.state = WriteHeadState::ReadHead;
        smallvec![read_head(self.user_id, &self.chat_id, Some(txn_id))]
    }

    fn handle_current(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(unexpected(&self.state, "chat head read", &event));
        };
        let current = match decode_head(value) {
            Ok(current) => current,
            Err(error) => return self.fail(error),
        };
        let Some(mut head) = current else {
            let Some(txn_id) = self.txn_id else {
                return self.fail(StorageError::TransactionNotFound.into());
            };
            self.state = WriteHeadState::CountHeads;
            return smallvec![iter_heads(self.user_id, Some(txn_id))];
        };
        if !head.is_live() {
            return self.fail(ChatStoreError::Deleted);
        }
        // No expectation writes over whatever is there; an expectation must match.
        if let Some(expected) = self.expected_revision
            && expected != head.revision
        {
            return self.fail(ChatStoreError::Stale);
        }
        head.title = std::mem::take(&mut self.title);
        head.subject = self.subject.take();
        head.revision = head.revision.saturating_add(1);
        head.updated_at = self.now;
        self.write(head)
    }

    fn handle_count(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.fail(unexpected(&self.state, "chat head iteration", &event));
        };
        let live = match decode_heads(values) {
            Ok(heads) => heads.iter().filter(|head| head.is_live()).count(),
            Err(error) => return self.fail(error),
        };
        if live >= MAX_ASSISTANT_CHATS {
            return self.fail(ChatStoreError::TooLarge(CHAT_CAP));
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

    fn write(&mut self, head: AssistantChatHead) -> Effects {
        let bytes = match head.to_bytes() {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error.into()),
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        self.state = WriteHeadState::WriteHead;
        self.output = Some(Ok(head));
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: ASSISTANT_CHAT_HEAD_KEYSPACE.to_string(),
            key: head_key(self.user_id, &self.chat_id),
            value: bytes.into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.fail(unexpected(&self.state, "chat head write", &event));
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        self.state = WriteHeadState::CommitTransaction;
        commit(txn_id)
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.fail(unexpected(&self.state, "transaction committed", &event));
        };
        self.txn_id = None;
        self.state = WriteHeadState::Finish;
        smallvec![]
    }
}

impl Operation for WriteChatHeadOperation {
    type Output = AssistantChatHead;
    type Error = ChatStoreError;

    fn start(&mut self) -> Effects {
        self.state = WriteHeadState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state {
            WriteHeadState::Init => self.start(),
            WriteHeadState::StartTransaction => self.handle_started(event),
            WriteHeadState::ReadHead => self.handle_current(event),
            WriteHeadState::CountHeads => self.handle_count(event),
            WriteHeadState::WriteHead => self.handle_written(event),
            WriteHeadState::CommitTransaction => self.handle_committed(event),
            WriteHeadState::Finish | WriteHeadState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, WriteHeadState::Finish | WriteHeadState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ChatStoreError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        abort_effects(&mut self.txn_id)
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum DeleteChatState {
    Init,
    StartTransaction,
    ReadHead,
    IterTurns { head: AssistantChatHead },
    DeleteTurns { head: AssistantChatHead },
    WriteHead,
    CommitTransaction,
    Finish,
    Error,
}

/// Drops the turns of one chat and leaves its head as a tombstone. Deleting an
/// unknown or already deleted chat finishes without a write.
#[derive(Debug, PartialEq)]
pub struct DeleteChatOperation {
    user_id: UserId,
    chat_id: String,
    now: u64,
    txn_id: Option<TxnId>,
    state: DeleteChatState,
    output: Option<Result<(), ChatStoreError>>,
}

impl DeleteChatOperation {
    pub fn new(user_id: UserId, chat_id: String, now: u64) -> Self {
        Self {
            user_id,
            chat_id,
            now,
            txn_id: None,
            state: DeleteChatState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ChatStoreError) -> Effects {
        let cleanup = self.abort();
        self.state = DeleteChatState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.fail(unexpected(&self.state, "transaction started", &event));
        };
        self.txn_id = Some(txn_id);
        self.state = DeleteChatState::ReadHead;
        smallvec![read_head(self.user_id, &self.chat_id, Some(txn_id))]
    }

    fn handle_head(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(unexpected(&self.state, "chat head read", &event));
        };
        let head = match decode_head(value) {
            Ok(Some(head)) if head.is_live() => head,
            Ok(_) => {
                self.state = DeleteChatState::Finish;
                self.output = Some(Ok(()));
                return self.abort();
            }
            Err(error) => return self.fail(error),
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        let read = iter_turns(
            self.user_id,
            &self.chat_id,
            head.first_seq,
            usize::MAX,
            Some(txn_id),
        );
        self.state = DeleteChatState::IterTurns { head };
        smallvec![read]
    }

    fn handle_turns(&mut self, head: AssistantChatHead, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.fail(unexpected(&self.state, "chat turn iteration", &event));
        };
        if values.is_empty() {
            return self.tombstone(head);
        }
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        let deletes = values
            .into_iter()
            .map(|(key, _)| (ASSISTANT_CHAT_TURN_KEYSPACE.to_string(), key))
            .collect();
        self.state = DeleteChatState::DeleteTurns { head };
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(txn_id),
        })]
    }

    fn handle_deleted(&mut self, head: AssistantChatHead, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.fail(unexpected(&self.state, "chat turn batch delete", &event));
        };
        self.tombstone(head)
    }

    fn tombstone(&mut self, mut head: AssistantChatHead) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        head.deleted_at = Some(self.now);
        head.bytes = 0;
        head.first_seq = head.next_seq;
        head.updated_at = self.now;
        head.revision = head.revision.saturating_add(1);
        let bytes = match head.to_bytes() {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error.into()),
        };
        self.state = DeleteChatState::WriteHead;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: ASSISTANT_CHAT_HEAD_KEYSPACE.to_string(),
            key: head_key(self.user_id, &self.chat_id),
            value: bytes.into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.fail(unexpected(&self.state, "chat head write", &event));
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        self.state = DeleteChatState::CommitTransaction;
        commit(txn_id)
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.fail(unexpected(&self.state, "transaction committed", &event));
        };
        self.txn_id = None;
        self.state = DeleteChatState::Finish;
        self.output = Some(Ok(()));
        smallvec![]
    }
}

impl Operation for DeleteChatOperation {
    type Output = ();
    type Error = ChatStoreError;

    fn start(&mut self) -> Effects {
        self.state = DeleteChatState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state.clone() {
            DeleteChatState::Init => self.start(),
            DeleteChatState::StartTransaction => self.handle_started(event),
            DeleteChatState::ReadHead => self.handle_head(event),
            DeleteChatState::IterTurns { head } => self.handle_turns(head, event),
            DeleteChatState::DeleteTurns { head } => self.handle_deleted(head, event),
            DeleteChatState::WriteHead => self.handle_written(event),
            DeleteChatState::CommitTransaction => self.handle_committed(event),
            DeleteChatState::Finish | DeleteChatState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, DeleteChatState::Finish | DeleteChatState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ChatStoreError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        abort_effects(&mut self.txn_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::RealmId;
    use ulid::Ulid;

    const CHAT: &str = "c-1";

    fn user() -> UserId {
        UserId::local(Ulid::from_bytes([7; 16]), RealmId::from_bytes([3; 32]))
    }

    fn txn() -> Ulid {
        Ulid::from_bytes([6; 16])
    }

    fn head(chat_id: &str, next_seq: u32, bytes: u64) -> AssistantChatHead {
        AssistantChatHead {
            user_id: user(),
            chat_id: chat_id.to_string(),
            title: "Run".to_string(),
            subject: None,
            created_at: 10,
            updated_at: 10,
            first_seq: 0,
            next_seq,
            bytes,
            revision: 3,
            deleted_at: None,
        }
    }

    fn turn(seq: u32, payload: &str) -> AssistantChatTurn {
        AssistantChatTurn {
            seq,
            payload: payload.to_string(),
            updated_at: 10,
        }
    }

    fn started() -> Event {
        Event::Storage(StorageEvent::TransactionStarted { txn_id: txn() })
    }

    fn committed() -> Event {
        Event::Storage(StorageEvent::TransactionCommitted { txn_id: txn() })
    }

    fn head_read(head: Option<&AssistantChatHead>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: head_key(user(), CHAT),
            value: head.map(|head| head.to_bytes().unwrap().into()),
        })
    }

    fn heads_iter(heads: &[AssistantChatHead]) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: heads
                .iter()
                .map(|head| {
                    (
                        head_key(user(), &head.chat_id),
                        head.to_bytes().unwrap().into(),
                    )
                })
                .collect(),
            next_start_after: None,
        })
    }

    fn turns_iter(turns: &[AssistantChatTurn]) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: turns
                .iter()
                .map(|turn| {
                    (
                        turn_key(user(), CHAT, turn.seq),
                        turn.to_bytes().unwrap().into(),
                    )
                })
                .collect(),
            next_start_after: None,
        })
    }

    fn written() -> Event {
        Event::Storage(StorageEvent::WriteResult {
            key: head_key(user(), CHAT),
        })
    }

    fn batch_written() -> Event {
        Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        })
    }

    fn batch_deleted() -> Event {
        Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        })
    }

    fn read_op(after: Option<u32>) -> ReadChatTurnsOperation {
        ReadChatTurnsOperation::new(user(), CHAT.to_string(), after)
    }

    fn head_op(expected: Option<u64>) -> WriteChatHeadOperation {
        WriteChatHeadOperation::new(
            user(),
            CHAT.to_string(),
            "Run QC".to_string(),
            Some("subject".to_string()),
            expected,
            50,
        )
    }

    fn turn_op(seq: u32, payload: &str) -> WriteChatTurnOperation {
        WriteChatTurnOperation::new(user(), CHAT.to_string(), seq, payload.to_string(), None, 50)
    }

    fn turn_op_at(seq: u32, revision: u64) -> WriteChatTurnOperation {
        WriteChatTurnOperation::new(
            user(),
            CHAT.to_string(),
            seq,
            "x".to_string(),
            Some(revision),
            50,
        )
    }

    fn delete_op() -> DeleteChatOperation {
        DeleteChatOperation::new(user(), CHAT.to_string(), 70)
    }

    fn is_iter(effects: &Effects, key_space: &str) -> bool {
        matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::Iter { key_space: space, .. })) if space == key_space
        )
    }

    fn is_write(effects: &Effects) -> bool {
        matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::Write { .. }))
        )
    }

    fn is_batch_write(effects: &Effects) -> bool {
        matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::BatchWrite { .. }))
        )
    }

    /// The start key and limit of a turn iteration.
    fn iter_start(effects: &Effects) -> Option<(Key, usize)> {
        match effects.first() {
            Some(Effect::Storage(StorageEffect::Iter {
                start: Some(IterStart::At(key)),
                limit,
                ..
            })) => Some((key.clone(), *limit)),
            _ => None,
        }
    }

    fn deleted_keys(effects: &Effects) -> Vec<(String, Key)> {
        match effects.first() {
            Some(Effect::Storage(StorageEffect::BatchDelete { deletes, .. })) => deletes.clone(),
            _ => panic!("expected a batch delete, got {effects:?}"),
        }
    }

    #[test]
    fn lists_live_heads() {
        // Newest change first; a tombstone is left out.
        let mut old = head("a", 1, 5);
        old.updated_at = 5;
        let mut new = head("b", 1, 5);
        new.updated_at = 9;
        let mut gone = head("c", 1, 5);
        gone.deleted_at = Some(11);
        let mut operation = ListChatHeadsOperation::new(user());
        assert!(is_iter(&operation.start(), ASSISTANT_CHAT_HEAD_KEYSPACE));
        assert!(!operation.is_complete());
        operation.step(heads_iter(&[old.clone(), gone, new.clone()]));

        assert!(operation.is_complete());
        assert_eq!(operation.finalize().unwrap(), vec![new, old]);
    }

    #[test]
    fn reads_after_seq() {
        // `after` below first_seq starts at first_seq; above it starts one past `after`.
        let mut stored = head(CHAT, 130, 9);
        stored.first_seq = 10;
        let mut operation = read_op(Some(3));
        operation.start();
        let effects = operation.step(head_read(Some(&stored)));
        assert_eq!(
            iter_start(&effects),
            Some((turn_key(user(), CHAT, 10), usize::MAX))
        );
        operation.step(turns_iter(&[turn(10, "x"), turn(11, "y")]));
        assert_eq!(
            operation.finalize().unwrap(),
            vec![turn(10, "x"), turn(11, "y")]
        );

        let mut operation = read_op(Some(20));
        operation.start();
        let effects = operation.step(head_read(Some(&stored)));
        assert_eq!(
            iter_start(&effects),
            Some((turn_key(user(), CHAT, 21), usize::MAX))
        );

        let mut operation = read_op(None);
        operation.start();
        let effects = operation.step(head_read(Some(&stored)));
        assert_eq!(
            iter_start(&effects),
            Some((turn_key(user(), CHAT, 10), usize::MAX))
        );
    }

    #[test]
    fn refuses_missing_chat() {
        // Unknown is not found; a tombstone is deleted. A write also releases its transaction.
        let mut gone = head(CHAT, 2, 4);
        gone.deleted_at = Some(12);

        let mut read = read_op(None);
        read.start();
        assert!(read.step(head_read(None)).is_empty());
        assert_eq!(read.finalize().unwrap_err(), ChatStoreError::NotFound);

        let mut read = read_op(None);
        read.start();
        read.step(head_read(Some(&gone)));
        assert_eq!(read.finalize().unwrap_err(), ChatStoreError::Deleted);

        let mut write = turn_op(2, "x");
        write.start();
        write.step(started());
        assert_eq!(write.step(head_read(None)).len(), 1);
        assert_eq!(write.finalize().unwrap_err(), ChatStoreError::NotFound);

        let mut write = turn_op(2, "x");
        write.start();
        write.step(started());
        assert_eq!(write.step(head_read(Some(&gone))).len(), 1);
        assert_eq!(write.finalize().unwrap_err(), ChatStoreError::Deleted);

        let mut rename = head_op(None);
        rename.start();
        rename.step(started());
        assert_eq!(rename.step(head_read(Some(&gone))).len(), 1);
        assert_eq!(rename.finalize().unwrap_err(), ChatStoreError::Deleted);
    }

    #[test]
    fn creates_a_head() {
        let mut operation = head_op(None);
        assert_eq!(operation.start().len(), 1);
        operation.step(started());
        let effects = operation.step(head_read(None));
        assert!(is_iter(&effects, ASSISTANT_CHAT_HEAD_KEYSPACE));
        let effects = operation.step(heads_iter(&[head("other", 1, 1)]));
        assert!(is_write(&effects));
        operation.step(written());
        operation.step(committed());

        assert!(operation.is_complete());
        let saved = operation.finalize().unwrap();
        assert_eq!(saved.chat_id, CHAT);
        assert_eq!(saved.title, "Run QC");
        assert_eq!(saved.subject.as_deref(), Some("subject"));
        assert_eq!(
            (
                saved.revision,
                saved.first_seq,
                saved.next_seq,
                saved.bytes,
                saved.created_at
            ),
            (1, 0, 0, 0, 50)
        );
    }

    #[test]
    fn refuses_chat_cap() {
        // Tombstones do not count against the cap.
        let mut heads: Vec<_> = (0..MAX_ASSISTANT_CHATS)
            .map(|index| head(&format!("c{index}"), 1, 1))
            .collect();
        let mut operation = head_op(None);
        operation.start();
        operation.step(started());
        operation.step(head_read(None));
        let cleanup = operation.step(heads_iter(&heads));
        assert_eq!(cleanup.len(), 1);
        assert_eq!(
            operation.finalize().unwrap_err(),
            ChatStoreError::TooLarge(CHAT_CAP)
        );

        heads[0].deleted_at = Some(1);
        let mut operation = head_op(None);
        operation.start();
        operation.step(started());
        operation.step(head_read(None));
        assert!(is_write(&operation.step(heads_iter(&heads))));
    }

    #[test]
    fn renames_with_revision() {
        let mut operation = head_op(Some(3));
        operation.start();
        operation.step(started());
        assert!(is_write(
            &operation.step(head_read(Some(&head(CHAT, 4, 9))))
        ));
        operation.step(written());
        operation.step(committed());

        let saved = operation.finalize().unwrap();
        assert_eq!(saved.title, "Run QC");
        assert_eq!(
            (
                saved.revision,
                saved.updated_at,
                saved.created_at,
                saved.next_seq,
                saved.bytes
            ),
            (4, 50, 10, 4, 9)
        );
    }

    #[test]
    fn refuses_stale_head() {
        let mut operation = head_op(Some(2));
        operation.start();
        operation.step(started());
        assert_eq!(operation.step(head_read(Some(&head(CHAT, 4, 9)))).len(), 1);
        assert_eq!(operation.finalize().unwrap_err(), ChatStoreError::Stale);

        // No expectation overwrites.
        let mut operation = head_op(None);
        operation.start();
        operation.step(started());
        assert!(is_write(
            &operation.step(head_read(Some(&head(CHAT, 4, 9))))
        ));
    }

    #[test]
    fn appends_a_turn() {
        let mut operation = turn_op(4, "abcd");
        assert_eq!(operation.start().len(), 1);
        operation.step(started());
        let effects = operation.step(head_read(Some(&head(CHAT, 4, 9))));
        assert!(is_iter(&effects, ASSISTANT_CHAT_HEAD_KEYSPACE));
        let effects = operation.step(heads_iter(&[head(CHAT, 4, 9), head("other", 1, 100)]));
        let Some(Effect::Storage(StorageEffect::BatchWrite { writes, .. })) = effects.first()
        else {
            panic!("expected a batch write, got {effects:?}");
        };
        assert_eq!(writes[0].1, turn_key(user(), CHAT, 4));
        assert_eq!(writes[1].1, head_key(user(), CHAT));
        assert_eq!(
            AssistantChatTurn::from_bytes(writes[0].2.as_ref()).unwrap(),
            AssistantChatTurn {
                seq: 4,
                payload: "abcd".to_string(),
                updated_at: 50
            }
        );
        operation.step(batch_written());
        operation.step(committed());

        let saved = operation.finalize().unwrap();
        assert_eq!(
            (
                saved.first_seq,
                saved.next_seq,
                saved.bytes,
                saved.revision,
                saved.updated_at
            ),
            (0, 5, 13, 4, 50)
        );
    }

    #[test]
    fn rewrites_the_tail() {
        // The old tail's bytes are released; next_seq stays.
        let mut operation = turn_op(3, "abcdef");
        operation.start();
        operation.step(started());
        let effects = operation.step(head_read(Some(&head(CHAT, 4, 9))));
        assert_eq!(iter_start(&effects), Some((turn_key(user(), CHAT, 3), 1)));
        let effects = operation.step(turns_iter(&[turn(3, "abcd")]));
        assert!(is_iter(&effects, ASSISTANT_CHAT_HEAD_KEYSPACE));
        assert!(is_batch_write(
            &operation.step(heads_iter(&[head(CHAT, 4, 9)]))
        ));
        operation.step(batch_written());
        operation.step(committed());

        let saved = operation.finalize().unwrap();
        assert_eq!(
            (saved.first_seq, saved.next_seq, saved.bytes, saved.revision),
            (0, 4, 11, 4)
        );
    }

    #[test]
    fn trims_old_turns() {
        // An append at the turn cap drops the oldest turn and advances first_seq.
        let mut full = head(CHAT, 130, 500);
        full.first_seq = 10;
        let mut operation = turn_op(130, "new");
        operation.start();
        operation.step(started());
        let effects = operation.step(head_read(Some(&full)));
        assert_eq!(iter_start(&effects), Some((turn_key(user(), CHAT, 10), 1)));
        let effects = operation.step(turns_iter(&[turn(10, "old!!")]));
        assert!(is_iter(&effects, ASSISTANT_CHAT_HEAD_KEYSPACE));
        let effects = operation.step(heads_iter(&[full.clone()]));
        assert_eq!(
            deleted_keys(&effects),
            vec![(
                ASSISTANT_CHAT_TURN_KEYSPACE.to_string(),
                turn_key(user(), CHAT, 10)
            )]
        );
        assert!(is_batch_write(&operation.step(batch_deleted())));
        operation.step(batch_written());
        operation.step(committed());

        let saved = operation.finalize().unwrap();
        assert_eq!(
            (saved.first_seq, saved.next_seq, saved.bytes),
            (11, 131, 498)
        );
    }

    #[test]
    fn refuses_wrong_seq() {
        // Anything but next_seq or the tail is stale, and the error names next_seq.
        for seq in [2, 5] {
            let mut operation = turn_op(seq, "x");
            operation.start();
            operation.step(started());
            assert_eq!(operation.step(head_read(Some(&head(CHAT, 4, 9)))).len(), 1);
            assert_eq!(
                operation.finalize().unwrap_err(),
                ChatStoreError::StaleTurn { next_seq: 4 }
            );
        }
    }

    #[test]
    fn refuses_stale_revision() {
        // An append and a tail rewrite from an older read both stop; a matching one goes on.
        for seq in [3, 4] {
            let mut operation = turn_op_at(seq, 2);
            operation.start();
            operation.step(started());
            assert_eq!(operation.step(head_read(Some(&head(CHAT, 4, 9)))).len(), 1);
            assert_eq!(
                operation.finalize().unwrap_err(),
                ChatStoreError::StaleTurn { next_seq: 4 }
            );
        }
        let mut operation = turn_op_at(4, 3);
        operation.start();
        operation.step(started());
        let effects = operation.step(head_read(Some(&head(CHAT, 4, 9))));
        assert!(is_iter(&effects, ASSISTANT_CHAT_HEAD_KEYSPACE));
    }

    #[test]
    fn refuses_large_turn() {
        let payload = "x".repeat(MAX_ASSISTANT_TURN_BYTES + 1);
        let mut operation =
            WriteChatTurnOperation::new(user(), CHAT.to_string(), 0, payload, None, 50);

        assert!(operation.start().is_empty());
        assert_eq!(
            operation.finalize().unwrap_err(),
            ChatStoreError::TooLarge(TURN_CAP)
        );
    }

    #[test]
    fn refuses_over_budget() {
        // The bytes a write releases count in the user's favour.
        let other = head("other", 1, MAX_ASSISTANT_CHAT_BYTES - 12);
        let mut operation = turn_op(4, "abcd");
        operation.start();
        operation.step(started());
        operation.step(head_read(Some(&head(CHAT, 4, 9))));
        let cleanup = operation.step(heads_iter(&[head(CHAT, 4, 9), other.clone()]));
        assert_eq!(cleanup.len(), 1);
        assert_eq!(
            operation.finalize().unwrap_err(),
            ChatStoreError::TooLarge(BUDGET_CAP)
        );

        let mut operation = turn_op(3, "abcd");
        operation.start();
        operation.step(started());
        operation.step(head_read(Some(&head(CHAT, 4, 9))));
        operation.step(turns_iter(&[turn(3, "abcd")]));
        assert!(is_batch_write(
            &operation.step(heads_iter(&[head(CHAT, 4, 9), other]))
        ));
    }

    #[test]
    fn deletes_once() {
        let mut operation = delete_op();
        assert_eq!(operation.start().len(), 1);
        operation.step(started());
        let effects = operation.step(head_read(Some(&head(CHAT, 2, 8))));
        assert_eq!(
            iter_start(&effects),
            Some((turn_key(user(), CHAT, 0), usize::MAX))
        );
        let effects = operation.step(turns_iter(&[turn(0, "a"), turn(1, "b")]));
        assert_eq!(deleted_keys(&effects).len(), 2);
        let effects = operation.step(batch_deleted());
        let Some(Effect::Storage(StorageEffect::Write { value, .. })) = effects.first() else {
            panic!("expected the tombstone write, got {effects:?}");
        };
        let tombstone = AssistantChatHead::from_bytes(value.as_ref()).unwrap();
        assert_eq!(tombstone.deleted_at, Some(70));
        assert_eq!(
            (
                tombstone.bytes,
                tombstone.first_seq,
                tombstone.next_seq,
                tombstone.revision
            ),
            (0, 2, 2, 4)
        );
        operation.step(written());
        operation.step(committed());

        assert!(operation.is_complete());
        assert_eq!(operation.finalize().unwrap(), ());
    }

    #[test]
    fn skips_missing_chat() {
        // Unknown or already deleted: the transaction is released and the delete succeeds.
        let mut operation = delete_op();
        operation.start();
        operation.step(started());
        let cleanup = operation.step(head_read(None));
        assert!(matches!(
            cleanup.first(),
            Some(Effect::Storage(StorageEffect::AbortTransaction { .. }))
        ));
        assert!(operation.is_complete());
        assert_eq!(operation.finalize().unwrap(), ());

        let mut gone = head(CHAT, 2, 8);
        gone.deleted_at = Some(1);
        let mut operation = delete_op();
        operation.start();
        operation.step(started());
        assert_eq!(operation.step(head_read(Some(&gone))).len(), 1);
        assert!(operation.finalize().is_ok());

        // No live turns: the tombstone is written without a batch delete.
        let mut operation = delete_op();
        operation.start();
        operation.step(started());
        operation.step(head_read(Some(&head(CHAT, 0, 0))));
        assert!(is_write(&operation.step(turns_iter(&[]))));
    }

    fn expect_unexpected<O: Operation<Error = ChatStoreError>>(
        mut operation: O,
        events: Vec<Event>,
        wrong: Event,
        cleanup: usize,
    ) {
        operation.start();
        for event in events {
            operation.step(event);
        }
        assert_eq!(operation.step(wrong).len(), cleanup);
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize().unwrap_err(),
            ChatStoreError::UnexpectedEvent { .. }
        ));
    }

    #[test]
    fn rejects_unexpected_events() {
        // Each state accepts one event kind; anything else fails and aborts the transaction.
        let stored = head(CHAT, 4, 9);
        let mut full = head(CHAT, 130, 500);
        full.first_seq = 10;
        let live = || head_read(Some(&stored));

        expect_unexpected(ListChatHeadsOperation::new(user()), vec![], started(), 0);
        expect_unexpected(read_op(None), vec![], started(), 0);
        expect_unexpected(read_op(None), vec![live()], head_read(None), 0);

        expect_unexpected(head_op(None), vec![], head_read(None), 0);
        expect_unexpected(head_op(None), vec![started()], started(), 1);
        expect_unexpected(
            head_op(None),
            vec![started(), head_read(None)],
            written(),
            1,
        );
        expect_unexpected(head_op(None), vec![started(), live()], head_read(None), 1);
        expect_unexpected(
            head_op(None),
            vec![started(), live(), written()],
            written(),
            1,
        );

        expect_unexpected(turn_op(4, "x"), vec![], head_read(None), 0);
        expect_unexpected(turn_op(4, "x"), vec![started()], started(), 1);
        expect_unexpected(turn_op(3, "x"), vec![started(), live()], written(), 1);
        expect_unexpected(turn_op(4, "x"), vec![started(), live()], written(), 1);
        expect_unexpected(
            turn_op(130, "x"),
            vec![
                started(),
                head_read(Some(&full)),
                turns_iter(&[turn(10, "old")]),
                heads_iter(&[full.clone()]),
            ],
            written(),
            1,
        );
        expect_unexpected(
            turn_op(4, "x"),
            vec![started(), live(), heads_iter(&[])],
            written(),
            1,
        );
        expect_unexpected(
            turn_op(4, "x"),
            vec![started(), live(), heads_iter(&[]), batch_written()],
            written(),
            1,
        );

        expect_unexpected(delete_op(), vec![], head_read(None), 0);
        expect_unexpected(delete_op(), vec![started()], started(), 1);
        expect_unexpected(delete_op(), vec![started(), live()], written(), 1);
        expect_unexpected(
            delete_op(),
            vec![started(), live(), turns_iter(&[turn(0, "a")])],
            written(),
            1,
        );
        expect_unexpected(
            delete_op(),
            vec![started(), live(), turns_iter(&[])],
            batch_deleted(),
            1,
        );
        expect_unexpected(
            delete_op(),
            vec![started(), live(), turns_iter(&[]), written()],
            written(),
            1,
        );
    }

    #[test]
    fn aborts_on_error() {
        let error = || {
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict,
            })
        };
        let mut write = turn_op(4, "x");
        write.start();
        write.step(started());
        assert_eq!(write.step(error()).len(), 1);
        assert!(write.is_complete());
        assert_eq!(
            write.finalize().unwrap_err(),
            ChatStoreError::Storage(StorageError::TransactionConflict)
        );

        let mut rename = head_op(None);
        rename.start();
        rename.step(started());
        assert_eq!(rename.step(error()).len(), 1);
        assert!(rename.finalize().is_err());

        let mut delete = delete_op();
        delete.start();
        delete.step(started());
        assert_eq!(delete.step(error()).len(), 1);
        assert!(delete.finalize().is_err());

        let mut list = ListChatHeadsOperation::new(user());
        list.start();
        assert!(list.step(error()).is_empty());
        assert!(list.finalize().is_err());

        let mut read = read_op(None);
        read.start();
        assert!(read.step(error()).is_empty());
        assert_eq!(
            read.finalize().unwrap_err(),
            ChatStoreError::Storage(StorageError::TransactionConflict)
        );
    }

    #[test]
    fn rejects_corrupt_records() {
        let corrupt = || {
            Event::Storage(StorageEvent::ReadResult {
                key: head_key(user(), CHAT),
                value: Some(vec![0xff; 3].into()),
            })
        };
        let mut read = read_op(None);
        read.start();
        read.step(corrupt());
        assert!(matches!(
            read.finalize().unwrap_err(),
            ChatStoreError::Conversion(_)
        ));

        let mut write = turn_op(0, "x");
        write.start();
        write.step(started());
        assert_eq!(write.step(corrupt()).len(), 1);
        assert!(matches!(
            write.finalize().unwrap_err(),
            ChatStoreError::Conversion(_)
        ));

        let mut list = ListChatHeadsOperation::new(user());
        list.start();
        list.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(head_key(user(), CHAT), vec![0xff; 3].into())],
            next_start_after: None,
        }));
        assert!(matches!(
            list.finalize().unwrap_err(),
            ChatStoreError::Conversion(_)
        ));
    }

    #[test]
    fn finalize_needs_completion() {
        let mut operation = turn_op(0, "x");
        assert!(!operation.is_complete());
        // A step before start behaves like start.
        assert_eq!(operation.step(head_read(None)).len(), 1);
        assert!(operation.abort().is_empty());
        assert_eq!(
            operation.finalize().unwrap_err(),
            ChatStoreError::NotFinished
        );
        assert_eq!(
            ListChatHeadsOperation::new(user()).finalize().unwrap_err(),
            ChatStoreError::NotFinished
        );
        assert_eq!(
            read_op(None).finalize().unwrap_err(),
            ChatStoreError::NotFinished
        );
        assert_eq!(
            head_op(None).finalize().unwrap_err(),
            ChatStoreError::NotFinished
        );
        assert_eq!(
            delete_op().finalize().unwrap_err(),
            ChatStoreError::NotFinished
        );
    }
}
