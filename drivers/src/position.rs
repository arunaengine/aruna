//! Seals and opens search and listing positions with a key of this process, so a cursor names
//! no key and a restart of the node makes older sealed cursors invalid.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::LazyLock;

use aruna_core::cursor::{SEALED_POSITION, position_bytes, position_text};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// This process's cipher for sealed positions; a restart makes older sealed cursors invalid.
static POSITION_CIPHER: LazyLock<XChaCha20Poly1305> = LazyLock::new(|| {
    let key: [u8; 32] = rand::random();
    XChaCha20Poly1305::new((&key).into())
});

/// `position` encrypted and authenticated with this process's key; only it opens it again.
pub fn seal_position(position: &[u8]) -> Option<Vec<u8>> {
    let nonce: [u8; 24] = rand::random();
    let sealed = POSITION_CIPHER
        .encrypt(&XNonce::from(nonce), position)
        .ok()?;
    Some([&[SEALED_POSITION][..], &nonce, &sealed].concat())
}

/// The plain bytes of a position: a sealed one is opened, a plain one is passed on.
pub fn open_position(position: &[u8]) -> Option<Vec<u8>> {
    let Some(sealed) = position.strip_prefix(&[SEALED_POSITION]) else {
        return Some(position.to_vec());
    };
    let (nonce, sealed) = sealed.split_at_checked(24)?;
    let nonce: [u8; 24] = nonce.try_into().ok()?;
    POSITION_CIPHER.decrypt(&XNonce::from(nonce), sealed).ok()
}

/// A listing position sealed as cursor text, so it names no entry.
pub fn seal_cursor(value: &impl Serialize) -> Option<String> {
    let sealed = seal_position(&postcard::to_allocvec(value).ok()?)?;
    Some(position_text(&sealed))
}

/// The position a sealed cursor of this process holds; `None` for any other text.
pub fn open_cursor<T: DeserializeOwned>(text: &str) -> Option<T> {
    postcard::from_bytes(&open_position(&position_bytes(text)?)?).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sealed_cursor_opens() {
        // A cursor sealed here opens again; one sealed elsewhere keeps its shape but does not.
        let cursor = seal_cursor(&(7u32, "key".to_string())).unwrap();
        assert_eq!(open_cursor(&cursor), Some((7u32, "key".to_string())));
        let foreign = position_text(&[SEALED_POSITION; 64]);
        assert_eq!(open_cursor::<(u32, String)>(&foreign), None);
        assert_eq!(open_position(b"plain").unwrap(), b"plain");
    }
}
