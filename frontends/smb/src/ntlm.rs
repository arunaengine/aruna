//! NTLMv2 acceptor (MS-NLMP): builds the CHALLENGE message, verifies the AUTHENTICATE
//! proof, MIC and key exchange, and signs the SPNEGO mechListMIC with session security.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use hmac::{Hmac, Mac};
use md4::Md4;
use md5::{Digest, Md5};
use rc4::{Rc4, StreamCipher};
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use crate::wire::{utf16_bytes, utf16_text};

pub const SIGNATURE: &[u8; 8] = b"NTLMSSP\0";

pub const UNICODE: u32 = 0x0000_0001;
pub const REQUEST_TARGET: u32 = 0x0000_0004;
pub const SIGN: u32 = 0x0000_0010;
pub const SEAL: u32 = 0x0000_0020;
pub const NTLM: u32 = 0x0000_0200;
pub const ANONYMOUS: u32 = 0x0000_0800;
pub const ALWAYS_SIGN: u32 = 0x0000_8000;
pub const TARGET_SERVER: u32 = 0x0002_0000;
pub const EXTENDED_SECURITY: u32 = 0x0008_0000;
pub const TARGET_INFO: u32 = 0x0080_0000;
pub const VERSION: u32 = 0x0200_0000;
pub const NEGOTIATE_128: u32 = 0x2000_0000;
pub const KEY_EXCHANGE: u32 = 0x4000_0000;
pub const NEGOTIATE_56: u32 = 0x8000_0000;
/// Flags every accepted client must offer: Unicode, extended session security, 128-bit
/// keys and key exchange.
pub const REQUIRED: u32 = UNICODE | EXTENDED_SECURITY | NEGOTIATE_128 | KEY_EXCHANGE;
const OFFERED: u32 = UNICODE
    | REQUEST_TARGET
    | SIGN
    | SEAL
    | NTLM
    | ALWAYS_SIGN
    | EXTENDED_SECURITY
    | VERSION
    | NEGOTIATE_128
    | KEY_EXCHANGE
    | NEGOTIATE_56;

pub const AV_END: u16 = 0x0000;
pub const AV_COMPUTER: u16 = 0x0001;
pub const AV_DOMAIN: u16 = 0x0002;
pub const AV_DNS_COMPUTER: u16 = 0x0003;
pub const AV_DNS_DOMAIN: u16 = 0x0004;
pub const AV_FLAGS: u16 = 0x0006;
pub const AV_TIMESTAMP: u16 = 0x0007;
/// MsvAvFlags bit announcing a MIC in the AUTHENTICATE message.
pub const MIC_PRESENT: u32 = 0x0000_0002;
const MIC_OFFSET: usize = 72;
const MIC_END: usize = 88;
/// Fixed NTLMv2 client blob header before its AV pairs.
const BLOB_HEADER: usize = 28;
const SERVER_VERSION: [u8; 8] = [10, 0, 0x61, 0x4a, 0, 0, 0, 15];

const CLIENT_SIGNING: &[u8] = b"session key to client-to-server signing key magic constant\0";
const SERVER_SIGNING: &[u8] = b"session key to server-to-client signing key magic constant\0";
const CLIENT_SEALING: &[u8] = b"session key to client-to-server sealing key magic constant\0";
const SERVER_SEALING: &[u8] = b"session key to server-to-client sealing key magic constant\0";

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum NtlmError {
    #[error("malformed NTLM message: {0}")]
    Malformed(&'static str),
    #[error("required NTLM flags missing")]
    Flags,
    #[error("anonymous or guest logon refused")]
    Anonymous,
    #[error("NTLMv1 or LM responses are refused")]
    Version,
    #[error("authentication failed")]
    Failed,
    #[error("the AUTHENTICATE message carries no MIC")]
    MissingMic,
    #[error("invalid key length")]
    KeyLength,
}

/// What to do with an AUTHENTICATE message whose MsvAvFlags announce no MIC. The CHALLENGE
/// always carries MsvAvTimestamp, so MS-NLMP clients must send one; `AllowMissing` serves
/// older clients that do not.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MicPolicy {
    #[default]
    Require,
    AllowMissing,
}

/// Names the server reports in the CHALLENGE target information.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetNames {
    pub domain: String,
    pub computer: String,
}

/// Server state between CHALLENGE and AUTHENTICATE.
#[derive(Clone)]
pub struct Challenge {
    negotiate: Vec<u8>,
    message: Vec<u8>,
    server_challenge: [u8; 8],
}

impl std::fmt::Debug for Challenge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Challenge(***)")
    }
}

/// A decoded AUTHENTICATE message; nothing in it is trusted before `verify`.
#[derive(Clone, Debug)]
pub struct Authenticate {
    pub user: String,
    pub domain: String,
    pub workstation: String,
    pub flags: u32,
    nt_response: Vec<u8>,
    encrypted_key: Vec<u8>,
    raw: Vec<u8>,
}

/// The result of a verified AUTHENTICATE message.
pub struct Verified {
    pub session_key: Zeroizing<[u8; 16]>,
    pub mic_present: bool,
}

impl std::fmt::Debug for Verified {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Verified")
            .field("mic_present", &self.mic_present)
            .finish_non_exhaustive()
    }
}

fn read_u16(bytes: &[u8], at: usize) -> Result<u16, NtlmError> {
    let slice = bytes
        .get(at..at + 2)
        .ok_or(NtlmError::Malformed("truncated"))?;
    Ok(u16::from_le_bytes([slice[0], slice[1]]))
}

fn read_u32(bytes: &[u8], at: usize) -> Result<u32, NtlmError> {
    let slice = bytes
        .get(at..at + 4)
        .ok_or(NtlmError::Malformed("truncated"))?;
    Ok(u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

/// The payload slice named by a Len/MaxLen/Offset field at `at`, plus its offset.
fn field_at(bytes: &[u8], at: usize) -> Result<(&[u8], usize), NtlmError> {
    let length = read_u16(bytes, at)? as usize;
    let offset = read_u32(bytes, at + 4)? as usize;
    if length == 0 {
        return Ok((&[], offset));
    }
    let end = offset
        .checked_add(length)
        .ok_or(NtlmError::Malformed("field overflow"))?;
    let slice = bytes
        .get(offset..end)
        .ok_or(NtlmError::Malformed("field out of bounds"))?;
    Ok((slice, offset))
}

fn check_header(bytes: &[u8], kind: u32) -> Result<(), NtlmError> {
    if bytes.len() < 12 || &bytes[..8] != SIGNATURE {
        return Err(NtlmError::Malformed("signature"));
    }
    if read_u32(bytes, 8)? != kind {
        return Err(NtlmError::Malformed("message type"));
    }
    Ok(())
}

pub(crate) fn hmac_md5(key: &[u8], parts: &[&[u8]]) -> Result<[u8; 16], NtlmError> {
    let mut mac =
        <Hmac<Md5> as hmac::KeyInit>::new_from_slice(key).map_err(|_| NtlmError::KeyLength)?;
    for part in parts {
        mac.update(part);
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(&mac.finalize().into_bytes());
    Ok(out)
}

/// MD4 of the UTF-16LE secret, the NT one-way function.
pub(crate) fn nt_hash(secret: &str) -> Zeroizing<[u8; 16]> {
    let units = Zeroizing::new(utf16_bytes(secret));
    let mut out = Zeroizing::new([0u8; 16]);
    out.copy_from_slice(&Md4::digest(units.as_slice()));
    out
}

/// NTOWFv2: HMAC-MD5 of the NT hash over UTF-16LE `UPPER(user) || domain`.
pub(crate) fn response_key(
    hash: &[u8; 16],
    user: &str,
    domain: &str,
) -> Result<Zeroizing<[u8; 16]>, NtlmError> {
    let identity = utf16_bytes(&format!("{}{}", user.to_uppercase(), domain));
    Ok(Zeroizing::new(hmac_md5(hash, &[&identity])?))
}

pub(crate) fn rc4_apply(key: &[u8], data: &mut [u8]) -> Result<(), NtlmError> {
    let mut cipher =
        <Rc4 as rc4::KeyInit>::new_from_slice(key).map_err(|_| NtlmError::KeyLength)?;
    cipher.apply_keystream(data);
    Ok(())
}

/// Reads AV pairs up to MsvAvEOL, failing on a pair that runs past the buffer.
pub(crate) fn av_pairs(mut bytes: &[u8]) -> Result<Vec<(u16, &[u8])>, NtlmError> {
    let mut pairs = Vec::new();
    loop {
        let id = read_u16(bytes, 0)?;
        let length = read_u16(bytes, 2)? as usize;
        let value = bytes
            .get(4..4 + length)
            .ok_or(NtlmError::Malformed("AV pair out of bounds"))?;
        if id == AV_END {
            return Ok(pairs);
        }
        pairs.push((id, value));
        bytes = &bytes[4 + length..];
    }
}

fn push_pair(out: &mut Vec<u8>, id: u16, value: &[u8]) {
    out.extend_from_slice(&id.to_le_bytes());
    out.extend_from_slice(&(value.len() as u16).to_le_bytes());
    out.extend_from_slice(value);
}

impl Challenge {
    /// Answers a NEGOTIATE message. `timestamp` is the server FILETIME placed in
    /// MsvAvTimestamp, which obliges conforming clients to send a MIC.
    pub fn answer(
        negotiate: &[u8],
        server_challenge: [u8; 8],
        timestamp: u64,
        names: &TargetNames,
    ) -> Result<Self, NtlmError> {
        check_header(negotiate, 1)?;
        let client_flags = read_u32(negotiate, 12)?;
        if client_flags & REQUIRED != REQUIRED {
            return Err(NtlmError::Flags);
        }
        if client_flags & ANONYMOUS != 0 {
            return Err(NtlmError::Anonymous);
        }
        let flags = (client_flags & OFFERED) | TARGET_SERVER | TARGET_INFO | REQUEST_TARGET;
        let target_name = utf16_bytes(&names.domain);
        let mut info = Vec::new();
        push_pair(&mut info, AV_DOMAIN, &utf16_bytes(&names.domain));
        push_pair(&mut info, AV_COMPUTER, &utf16_bytes(&names.computer));
        push_pair(&mut info, AV_DNS_DOMAIN, &utf16_bytes(&names.domain));
        push_pair(&mut info, AV_DNS_COMPUTER, &utf16_bytes(&names.computer));
        push_pair(&mut info, AV_TIMESTAMP, &timestamp.to_le_bytes());
        push_pair(&mut info, AV_END, &[]);
        let name_offset = 56u32;
        let info_offset = name_offset + target_name.len() as u32;
        let mut message = Vec::with_capacity(56 + target_name.len() + info.len());
        message.extend_from_slice(SIGNATURE);
        message.extend_from_slice(&2u32.to_le_bytes());
        message.extend_from_slice(&(target_name.len() as u16).to_le_bytes());
        message.extend_from_slice(&(target_name.len() as u16).to_le_bytes());
        message.extend_from_slice(&name_offset.to_le_bytes());
        message.extend_from_slice(&flags.to_le_bytes());
        message.extend_from_slice(&server_challenge);
        message.extend_from_slice(&[0u8; 8]);
        message.extend_from_slice(&(info.len() as u16).to_le_bytes());
        message.extend_from_slice(&(info.len() as u16).to_le_bytes());
        message.extend_from_slice(&info_offset.to_le_bytes());
        message.extend_from_slice(&SERVER_VERSION);
        message.extend_from_slice(&target_name);
        message.extend_from_slice(&info);
        Ok(Self {
            negotiate: negotiate.to_vec(),
            message,
            server_challenge,
        })
    }

    pub fn message(&self) -> &[u8] {
        &self.message
    }

    /// Checks the NTLMv2 proof with `secret`, recovers the exported session key through
    /// key exchange and checks the MIC. The secret and derived hashes are dropped on return.
    pub fn verify(
        &self,
        auth: &Authenticate,
        secret: &str,
        policy: MicPolicy,
    ) -> Result<Verified, NtlmError> {
        let hash = nt_hash(secret);
        let key = response_key(&hash, &auth.user, &auth.domain)?;
        drop(hash);
        let (proof, blob) = auth.nt_response.split_at(16);
        let expected = Zeroizing::new(hmac_md5(key.as_slice(), &[&self.server_challenge, blob])?);
        if !bool::from(expected.as_slice().ct_eq(proof)) {
            return Err(NtlmError::Failed);
        }
        let base = Zeroizing::new(hmac_md5(key.as_slice(), &[proof])?);
        drop(key);
        if auth.encrypted_key.len() != 16 {
            return Err(NtlmError::Malformed("encrypted session key length"));
        }
        let mut session_key = Zeroizing::new([0u8; 16]);
        session_key.copy_from_slice(&auth.encrypted_key);
        rc4_apply(base.as_slice(), session_key.as_mut_slice())?;
        let mic_present = blob_flags(blob)? & MIC_PRESENT != 0;
        if mic_present {
            let mut zeroed = auth.raw.clone();
            zeroed[MIC_OFFSET..MIC_END].fill(0);
            let mic = hmac_md5(
                session_key.as_slice(),
                &[&self.negotiate, &self.message, &zeroed],
            )?;
            if !bool::from(mic.ct_eq(&auth.raw[MIC_OFFSET..MIC_END])) {
                return Err(NtlmError::Failed);
            }
        } else if policy == MicPolicy::Require {
            return Err(NtlmError::MissingMic);
        }
        Ok(Verified {
            session_key,
            mic_present,
        })
    }
}

/// The MsvAvFlags value of an NTLMv2 client blob, zero when absent.
fn blob_flags(blob: &[u8]) -> Result<u32, NtlmError> {
    let pairs = av_pairs(&blob[BLOB_HEADER..])?;
    for (id, value) in pairs {
        if id == AV_FLAGS {
            if value.len() != 4 {
                return Err(NtlmError::Malformed("MsvAvFlags length"));
            }
            return read_u32(value, 0);
        }
    }
    Ok(0)
}

/// Decodes an AUTHENTICATE message and refuses anonymous, guest and NTLMv1 logons.
pub fn parse_authenticate(bytes: &[u8]) -> Result<Authenticate, NtlmError> {
    check_header(bytes, 3)?;
    let flags = read_u32(bytes, 60)?;
    if flags & REQUIRED != REQUIRED {
        return Err(NtlmError::Flags);
    }
    if flags & ANONYMOUS != 0 {
        return Err(NtlmError::Anonymous);
    }
    let (nt_response, nt_offset) = field_at(bytes, 20)?;
    let (domain, domain_offset) = field_at(bytes, 28)?;
    let (user, user_offset) = field_at(bytes, 36)?;
    let (workstation, station_offset) = field_at(bytes, 44)?;
    let (encrypted_key, key_offset) = field_at(bytes, 52)?;
    let lowest = [
        (nt_response.len(), nt_offset),
        (domain.len(), domain_offset),
        (user.len(), user_offset),
        (workstation.len(), station_offset),
        (encrypted_key.len(), key_offset),
    ]
    .into_iter()
    .filter(|(length, _)| *length > 0)
    .map(|(_, offset)| offset)
    .min()
    .unwrap_or(bytes.len());
    if nt_response.len() == 24 || nt_response.len() < 16 + BLOB_HEADER + 4 {
        return Err(NtlmError::Version);
    }
    if nt_response[16] != 1 || nt_response[17] != 1 {
        return Err(NtlmError::Version);
    }
    let announced = blob_flags(&nt_response[16..])?;
    if announced & MIC_PRESENT != 0 && (bytes.len() < MIC_END || lowest < MIC_END) {
        return Err(NtlmError::Malformed("no room for the MIC"));
    }
    let user = utf16_text(user).ok_or(NtlmError::Malformed("user name"))?;
    if user.is_empty() || user.eq_ignore_ascii_case("guest") {
        return Err(NtlmError::Anonymous);
    }
    Ok(Authenticate {
        user,
        domain: utf16_text(domain).ok_or(NtlmError::Malformed("domain name"))?,
        workstation: utf16_text(workstation).ok_or(NtlmError::Malformed("workstation"))?,
        flags,
        nt_response: nt_response.to_vec(),
        encrypted_key: encrypted_key.to_vec(),
        raw: bytes.to_vec(),
    })
}

/// NTLM session security with extended session security and key exchange, used only for
/// the SPNEGO mechListMIC of each direction.
pub struct SessionSecurity {
    client_sign: Zeroizing<[u8; 16]>,
    server_sign: Zeroizing<[u8; 16]>,
    client_seal: Rc4,
    server_seal: Rc4,
}

impl std::fmt::Debug for SessionSecurity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SessionSecurity(***)")
    }
}

fn magic_key(session_key: &[u8; 16], magic: &[u8]) -> Zeroizing<[u8; 16]> {
    let mut hasher = Md5::new();
    hasher.update(session_key);
    hasher.update(magic);
    let mut out = Zeroizing::new([0u8; 16]);
    out.copy_from_slice(&hasher.finalize());
    out
}

fn seal_mac(
    sign: &[u8; 16],
    seal: &mut Rc4,
    sequence: u32,
    message: &[u8],
) -> Result<[u8; 16], NtlmError> {
    let digest = hmac_md5(sign, &[&sequence.to_le_bytes(), message])?;
    let mut checksum = [0u8; 8];
    checksum.copy_from_slice(&digest[..8]);
    seal.apply_keystream(&mut checksum);
    let mut out = [0u8; 16];
    out[..4].copy_from_slice(&1u32.to_le_bytes());
    out[4..12].copy_from_slice(&checksum);
    out[12..].copy_from_slice(&sequence.to_le_bytes());
    Ok(out)
}

impl SessionSecurity {
    pub fn new(session_key: &[u8; 16]) -> Result<Self, NtlmError> {
        let client_key = magic_key(session_key, CLIENT_SEALING);
        let server_key = magic_key(session_key, SERVER_SEALING);
        Ok(Self {
            client_sign: magic_key(session_key, CLIENT_SIGNING),
            server_sign: magic_key(session_key, SERVER_SIGNING),
            client_seal: <Rc4 as rc4::KeyInit>::new_from_slice(client_key.as_slice())
                .map_err(|_| NtlmError::KeyLength)?,
            server_seal: <Rc4 as rc4::KeyInit>::new_from_slice(server_key.as_slice())
                .map_err(|_| NtlmError::KeyLength)?,
        })
    }

    /// The signature a client produces over `message` with sequence number `sequence`.
    pub fn client_mic(&mut self, sequence: u32, message: &[u8]) -> Result<[u8; 16], NtlmError> {
        seal_mac(&self.client_sign, &mut self.client_seal, sequence, message)
    }

    pub fn server_mic(&mut self, sequence: u32, message: &[u8]) -> Result<[u8; 16], NtlmError> {
        seal_mac(&self.server_sign, &mut self.server_seal, sequence, message)
    }
}

#[cfg(test)]
#[path = "ntlm_tests.rs"]
mod tests;
