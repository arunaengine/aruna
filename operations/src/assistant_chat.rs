use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::ASSISTANT_CHAT_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{AssistantChats, MAX_ASSISTANT_CHAT_BYTES};
use aruna_core::types::{Effects, Key, TxnId, UserId};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

fn chat_key(user_id: UserId) -> Key {
    ByteView::from(user_id.to_storage_key())
}

#[derive(Debug, Error, PartialEq)]
pub enum ChatStoreError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("stored chats are larger than the node accepts")]
    TooLarge,
    #[error("chats changed in another browser")]
    Stale,
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
enum ReadChatsState {
    Init,
    Read,
    Finish,
    Error,
}

/// Reads one user's stored chats; an unknown user simply has none.
#[derive(Debug, PartialEq)]
pub struct ReadChatsOperation {
    user_id: UserId,
    state: ReadChatsState,
    output: Option<Result<Option<AssistantChats>, ChatStoreError>>,
}

impl ReadChatsOperation {
    pub fn new(user_id: UserId) -> Self {
        Self {
            user_id,
            state: ReadChatsState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ChatStoreError) -> Effects {
        self.state = ReadChatsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for ReadChatsOperation {
    type Output = Option<AssistantChats>;
    type Error = ChatStoreError;

    fn start(&mut self) -> Effects {
        self.state = ReadChatsState::Read;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: ASSISTANT_CHAT_KEYSPACE.to_string(),
            key: chat_key(self.user_id),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state {
            ReadChatsState::Read => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(unexpected(&self.state, "chats read", &event));
                };
                let decoded = value
                    .as_ref()
                    .map(|bytes| AssistantChats::from_bytes(bytes.as_ref()))
                    .transpose();
                self.state = ReadChatsState::Finish;
                self.output = Some(decoded.map_err(Into::into));
                smallvec![]
            }
            ReadChatsState::Init | ReadChatsState::Finish | ReadChatsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReadChatsState::Finish | ReadChatsState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ChatStoreError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum WriteChatsState {
    Init,
    StartTransaction,
    ReadChats,
    WriteChats,
    CommitTransaction,
    Finish,
    Error,
}

/// Replaces one user's stored chats. The write is refused when the caller wrote
/// from an older revision than the node holds, so a second browser cannot
/// silently drop what the first one saved.
#[derive(Debug, PartialEq)]
pub struct WriteChatsOperation {
    user_id: UserId,
    payload: String,
    expected_revision: Option<u64>,
    now: u64,
    txn_id: Option<TxnId>,
    state: WriteChatsState,
    output: Option<Result<AssistantChats, ChatStoreError>>,
}

impl WriteChatsOperation {
    pub fn new(user_id: UserId, payload: String, expected_revision: Option<u64>, now: u64) -> Self {
        Self {
            user_id,
            payload,
            expected_revision,
            now,
            txn_id: None,
            state: WriteChatsState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ChatStoreError) -> Effects {
        let cleanup = self.abort();
        self.state = WriteChatsState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.fail(unexpected(&self.state, "transaction started", &event));
        };
        self.txn_id = Some(txn_id);
        self.state = WriteChatsState::ReadChats;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: ASSISTANT_CHAT_KEYSPACE.to_string(),
            key: chat_key(self.user_id),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_current(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(unexpected(&self.state, "chats read", &event));
        };
        let current = match value
            .as_ref()
            .map(|bytes| AssistantChats::from_bytes(bytes.as_ref()))
            .transpose()
        {
            Ok(current) => current,
            Err(error) => return self.fail(error.into()),
        };
        let held = current.as_ref().map(|chats| chats.revision);
        // No expectation writes over whatever is there; an expectation must match.
        if let Some(expected) = self.expected_revision
            && held.unwrap_or(0) != expected
        {
            return self.fail(ChatStoreError::Stale);
        }
        let next = AssistantChats {
            user_id: self.user_id,
            payload: std::mem::take(&mut self.payload),
            revision: held.unwrap_or(0).saturating_add(1),
            updated_at: self.now,
        };
        let bytes = match next.to_bytes() {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error.into()),
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        self.output = Some(Ok(next));
        self.state = WriteChatsState::WriteChats;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: ASSISTANT_CHAT_KEYSPACE.to_string(),
            key: chat_key(self.user_id),
            value: bytes.into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.fail(unexpected(&self.state, "chats write", &event));
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        self.state = WriteChatsState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.fail(unexpected(&self.state, "transaction committed", &event));
        };
        self.txn_id = None;
        self.state = WriteChatsState::Finish;
        smallvec![]
    }
}

impl Operation for WriteChatsOperation {
    type Output = AssistantChats;
    type Error = ChatStoreError;

    fn start(&mut self) -> Effects {
        if self.payload.len() > MAX_ASSISTANT_CHAT_BYTES {
            return self.fail(ChatStoreError::TooLarge);
        }
        self.state = WriteChatsState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state {
            WriteChatsState::Init => self.start(),
            WriteChatsState::StartTransaction => self.handle_started(event),
            WriteChatsState::ReadChats => self.handle_current(event),
            WriteChatsState::WriteChats => self.handle_written(event),
            WriteChatsState::CommitTransaction => self.handle_committed(event),
            WriteChatsState::Finish | WriteChatsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, WriteChatsState::Finish | WriteChatsState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ChatStoreError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::RealmId;
    use ulid::Ulid;

    fn user() -> UserId {
        UserId::local(Ulid::from_bytes([7; 16]), RealmId::from_bytes([3; 32]))
    }

    fn stored(revision: u64) -> AssistantChats {
        AssistantChats {
            user_id: user(),
            payload: "{\"chats\":[]}".to_string(),
            revision,
            updated_at: 10,
        }
    }

    fn read_result(chats: Option<&AssistantChats>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: chat_key(user()),
            value: chats.map(|chats| chats.to_bytes().unwrap().into()),
        })
    }

    #[test]
    fn reads_nothing_before_first_save() {
        let mut operation = ReadChatsOperation::new(user());
        operation.start();
        operation.step(read_result(None));

        assert_eq!(operation.finalize().unwrap(), None);
    }

    #[test]
    fn counts_every_accepted_save() {
        // The revision the caller reads back is what the next save must carry.
        let mut operation = WriteChatsOperation::new(user(), "{}".to_string(), Some(2), 30);
        let txn_id = Ulid::from_bytes([6; 16]);
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(read_result(Some(&stored(2))));
        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: chat_key(user()),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));

        let saved = operation.finalize().unwrap();
        assert_eq!(saved.revision, 3);
        assert_eq!(saved.updated_at, 30);
    }

    #[test]
    fn refuses_a_save_from_an_older_read() {
        let mut operation = WriteChatsOperation::new(user(), "{}".to_string(), Some(1), 30);
        let txn_id = Ulid::from_bytes([6; 16]);
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let cleanup = operation.step(read_result(Some(&stored(4))));

        assert_eq!(operation.finalize().unwrap_err(), ChatStoreError::Stale);
        assert_eq!(cleanup.len(), 1);
    }

    #[test]
    fn refuses_more_than_the_node_keeps() {
        let payload = "x".repeat(MAX_ASSISTANT_CHAT_BYTES + 1);
        let mut operation = WriteChatsOperation::new(user(), payload, None, 30);

        assert!(operation.start().is_empty());
        assert_eq!(operation.finalize().unwrap_err(), ChatStoreError::TooLarge);
    }
}
