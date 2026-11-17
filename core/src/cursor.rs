//! Encodes page cursors as URL-safe base64 text, optionally over postcard values.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::errors::ConversionError;

/// Why a cursor could not be encoded or decoded. Callers check versions, sizes and bindings.
#[derive(Debug, Error)]
pub enum CursorError {
    #[error(transparent)]
    Text(#[from] base64::DecodeError),
    #[error(transparent)]
    Value(#[from] postcard::Error),
}

impl From<CursorError> for ConversionError {
    fn from(error: CursorError) -> Self {
        match error {
            CursorError::Text(error) => Self::Base64DecodeError(error),
            CursorError::Value(error) => Self::PostcardError(error),
        }
    }
}

/// The cursor text for `bytes`.
pub fn encode(bytes: impl AsRef<[u8]>) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

/// The bytes a cursor text holds.
pub fn decode(text: impl AsRef<[u8]>) -> Result<Vec<u8>, CursorError> {
    Ok(URL_SAFE_NO_PAD.decode(text)?)
}

/// The cursor text for `value` in postcard form.
pub fn encode_value<T: Serialize + ?Sized>(value: &T) -> Result<String, CursorError> {
    Ok(encode(postcard::to_allocvec(value)?))
}

/// The value a cursor text holds in postcard form; trailing bytes are ignored.
pub fn decode_value<T: DeserializeOwned>(text: impl AsRef<[u8]>) -> Result<T, CursorError> {
    Ok(postcard::from_bytes(&decode(text)?)?)
}

#[cfg(test)]
mod tests {
    use super::{decode, decode_value, encode, encode_value};

    #[test]
    fn keeps_existing_text() {
        assert_eq!(encode(b"k\xff"), "a_8");
        assert_eq!(encode_value(&vec![1u8, 2]).unwrap(), "AgEC");
        assert_eq!(decode("a_8").unwrap(), b"k\xff");
        assert_eq!(decode_value::<Vec<u8>>("AgEC").unwrap(), vec![1, 2]);
    }

    #[test]
    fn rejects_bad_text() {
        assert!(decode("a+8=").is_err());
        assert!(decode_value::<Vec<u8>>("Ag").is_err());
    }
}
