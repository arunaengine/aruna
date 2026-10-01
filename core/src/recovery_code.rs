//! Formats and reads the 32-byte vault recovery code as Crockford base32 in groups of four.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use thiserror::Error;
use zeroize::Zeroizing;

pub const RECOVERY_BYTES: usize = 32;
const RECOVERY_CHARS: usize = 52;
const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
#[error("the recovery code is malformed")]
pub struct RecoveryCodeError;

/// The recovery code as the user keeps it.
pub fn format_recovery(bytes: &[u8; RECOVERY_BYTES]) -> String {
    let mut chars = Vec::with_capacity(RECOVERY_CHARS);
    let mut value: u32 = 0;
    let mut bits = 0;
    for byte in bytes {
        value = (value << 8) | u32::from(*byte);
        bits += 8;
        while bits >= 5 {
            chars.push(CROCKFORD[((value >> (bits - 5)) & 31) as usize]);
            bits -= 5;
        }
        value &= (1 << bits) - 1;
    }
    if bits > 0 {
        chars.push(CROCKFORD[((value << (5 - bits)) & 31) as usize]);
    }
    chars
        .chunks(4)
        .map(|group| String::from_utf8_lossy(group).into_owned())
        .collect::<Vec<_>>()
        .join("-")
}

/// Accepts any case and separators, and reads O as 0 and I or L as 1.
pub fn parse_recovery(code: &str) -> Result<Zeroizing<[u8; RECOVERY_BYTES]>, RecoveryCodeError> {
    let digits: Vec<u8> = code
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|char| match char.to_ascii_uppercase() {
            'O' => '0',
            'I' | 'L' => '1',
            other => other,
        })
        .map(|char| {
            CROCKFORD
                .iter()
                .position(|digit| char::from(*digit) == char)
                .map(|position| position as u8)
                .ok_or(RecoveryCodeError)
        })
        .collect::<Result<_, _>>()?;
    if digits.len() != RECOVERY_CHARS {
        return Err(RecoveryCodeError);
    }
    let mut bytes = Zeroizing::new([0u8; RECOVERY_BYTES]);
    let mut value: u32 = 0;
    let mut bits = 0;
    let mut at = 0;
    for digit in digits {
        value = (value << 5) | u32::from(digit);
        bits += 5;
        if bits >= 8 && at < RECOVERY_BYTES {
            bytes[at] = (value >> (bits - 8)) as u8;
            at += 1;
            bits -= 8;
        }
        value &= (1 << bits) - 1;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vector() -> (String, [u8; 32]) {
        let all: serde_json::Value =
            serde_json::from_str(include_str!("../tests/vectors/vault.json")).unwrap();
        let code = all["vault"]["recovery_code"].as_str().unwrap().to_string();
        let bytes = hex::decode(all["vault"]["recovery_bytes"].as_str().unwrap()).unwrap();
        (code, bytes.try_into().unwrap())
    }

    #[test]
    fn reads_recovery_codes() {
        let (code, bytes) = vector();
        assert_eq!(format_recovery(&bytes), code);
        // Typed by hand: lower case, spaces, and O, I or L instead of 0 and 1.
        let typed = code
            .to_lowercase()
            .replace('-', " ")
            .replace('0', "o")
            .replace('1', "l");
        assert_eq!(*parse_recovery(&typed).unwrap(), bytes);
        assert_eq!(parse_recovery(&code[..20]).err(), Some(RecoveryCodeError));
        assert_eq!(
            parse_recovery(&code.replace('P', "U")).err(),
            Some(RecoveryCodeError)
        );
    }
}
