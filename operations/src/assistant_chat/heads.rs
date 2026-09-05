use super::{
    CHAT_CAP, ChatStoreError, abort_effects, commit, decode_head, decode_heads, head_key,
    iter_heads, iter_turns, read_head, unexpected,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{ASSISTANT_CHAT_HEAD_KEYSPACE, ASSISTANT_CHAT_TURN_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{AssistantChatHead, MAX_ASSISTANT_CHATS};
use aruna_core::types::{Effects, TxnId, UserId};
use smallvec::smallvec;

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
