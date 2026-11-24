//! Standard base64 with padding for the byte fields of messages, as a serde field codec.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Deserializer, Serializer};

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Writes bytes as base64 text.
pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&encode(bytes))
}

/// Reads base64 text back into bytes.
pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    let text = String::deserialize(deserializer)?;
    decode(&text).map_err(serde::de::Error::custom)
}

pub fn encode(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let group = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let value = u32::from_be_bytes([0, group[0], group[1], group[2]]);
        for position in 0..4 {
            if position <= chunk.len() {
                let index = (value >> (18 - 6 * position)) & 0x3f;
                text.push(char::from(ALPHABET[index as usize]));
            } else {
                text.push('=');
            }
        }
    }
    text
}

pub fn decode(text: &str) -> Result<Vec<u8>, String> {
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err("base64 text must come in groups of four characters".into());
    }
    let mut decoded = Vec::with_capacity(bytes.len() / 4 * 3);
    let groups = bytes.len() / 4;
    for (number, group) in bytes.chunks(4).enumerate() {
        let padding = group.iter().rev().take_while(|byte| **byte == b'=').count();
        if padding > 2 || (padding > 0 && number + 1 != groups) {
            return Err("base64 padding may only end the text".into());
        }
        let mut value = 0u32;
        for byte in &group[..4 - padding] {
            let index = ALPHABET
                .iter()
                .position(|letter| letter == byte)
                .ok_or_else(|| format!("{:?} is not a base64 character", char::from(*byte)))?;
            value = (value << 6) | index as u32;
        }
        value <<= 6 * padding as u32;
        let [_, first, second, third] = value.to_be_bytes();
        decoded.extend_from_slice(&[first, second, third][..3 - padding]);
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_lengths() {
        for length in 0..10 {
            let bytes: Vec<u8> = (0..length).map(|value| (value * 37) as u8).collect();
            assert_eq!(
                decode(&encode(&bytes)).as_deref(),
                Ok(&bytes[..]),
                "{length}"
            );
        }
        assert_eq!(encode(b"Man"), "TWFu");
        assert_eq!(encode(b"Ma"), "TWE=");
        assert_eq!(encode(b"M"), "TQ==");
    }

    #[test]
    fn rejects_malformed_text() {
        assert!(decode("TWF").is_err());
        assert!(decode("TW=u").is_err());
        assert!(decode("TQ==TWFu").is_err());
        assert!(decode("T*Fu").is_err());
    }
}
