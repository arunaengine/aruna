use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::{ASSISTANT_CHAT_HEAD_KEYSPACE, ASSISTANT_CHAT_TURN_KEYSPACE};
use aruna_core::structs::AssistantChatHead;
use aruna_core::types::{Effects, Key, TxnId, UserId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

mod heads;
#[cfg(test)]
mod tests;
mod turns;

pub use heads::{DeleteChatOperation, ListChatHeadsOperation, WriteChatHeadOperation};
pub use turns::{ReadChatTurnsOperation, WriteChatTurnOperation};

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
