//! Owns the assistant chat key layout, its shared storage helpers and the chat limits.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{CHAT_HEAD_KEYSPACE, CHAT_TURN_KEYSPACE};
use aruna_core::structs::AssistantChatHead;
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use thiserror::Error;

pub mod central;
mod heads;
pub mod provider;
#[cfg(test)]
mod tests;
mod turns;

pub use heads::{DeleteChatOperation, ListChatOperation, WriteChatOperation};
pub use turns::{ReadChatOperation, WriteTurnOperation};

pub const CHAT_CAP: &str = "a user may keep at most 20 chats";
pub const TURN_CAP: &str = "a turn payload may hold at most 256 KiB";
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

fn read_head(user_id: UserId, chat_id: &str) -> IoCall {
    IoCall::Read(vec![(
        CHAT_HEAD_KEYSPACE.to_string(),
        head_key(user_id, chat_id),
    )])
}

fn heads_page(user_id: UserId) -> IoCall {
    IoCall::Page {
        space: CHAT_HEAD_KEYSPACE.to_string(),
        prefix: ByteView::from(head_prefix(user_id)),
        after: None,
        limit: usize::MAX,
    }
}

/// At most `limit` turns of one chat from `seq` on.
fn turns_page(user_id: UserId, chat_id: &str, seq: u32, limit: usize) -> IoCall {
    IoCall::Page {
        space: CHAT_TURN_KEYSPACE.to_string(),
        prefix: ByteView::from(turn_prefix(user_id, chat_id)),
        after: (seq.checked_sub(1)).map(|before| turn_key(user_id, chat_id, before)),
        limit,
    }
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
}

impl From<IoError> for ChatStoreError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}
