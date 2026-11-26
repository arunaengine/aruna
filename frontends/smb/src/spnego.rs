//! Minimal ASN.1 DER codec for SPNEGO (RFC 4178, MS-SPNG): the server NegTokenInit2 hint,
//! client NegTokenInit and NegTokenResp tokens, and raw NTLMSSP tokens without a wrapper.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

/// DER contents of the NTLMSSP mechanism OID 1.3.6.1.4.1.311.2.2.10.
pub const NTLMSSP_OID: &[u8] = &[0x2b, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x0a];
/// DER contents of the SPNEGO OID 1.3.6.1.5.5.2.
pub const SPNEGO_OID: &[u8] = &[0x2b, 0x06, 0x01, 0x05, 0x05, 0x02];
const HINT_NAME: &[u8] = b"not_defined_in_RFC4178@please_ignore";
const RAW_PREFIX: &[u8] = b"NTLMSSP\0";

pub const ACCEPT_COMPLETED: u8 = 0;
pub const ACCEPT_INCOMPLETE: u8 = 1;
pub const REJECT: u8 = 2;

const TAG_OCTETS: u8 = 0x04;
const TAG_OID: u8 = 0x06;
const TAG_ENUMERATED: u8 = 0x0a;
const TAG_GENERAL: u8 = 0x1b;
const TAG_SEQUENCE: u8 = 0x30;
const TAG_APPLICATION: u8 = 0x60;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SpnegoError {
    #[error("malformed SPNEGO token: {0}")]
    Malformed(&'static str),
}

/// A client NegTokenInit; `mechanism_list` keeps the exact DER MechTypeList for mechListMIC.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InitToken {
    pub mechanisms: Vec<Vec<u8>>,
    pub mechanism_list: Vec<u8>,
    pub token: Option<Vec<u8>>,
    pub list_mic: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResponseToken {
    pub state: Option<u8>,
    pub mechanism: Option<Vec<u8>>,
    pub token: Option<Vec<u8>>,
    pub list_mic: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientToken {
    Init(InitToken),
    Response(ResponseToken),
    /// An NTLMSSP message sent without SPNEGO, as Linux CIFS does with `sec=ntlmssp`.
    Raw(Vec<u8>),
}

/// One decoded TLV: its tag, its contents, its full encoding and the bytes after it.
struct Element<'a> {
    tag: u8,
    value: &'a [u8],
    whole: &'a [u8],
    rest: &'a [u8],
}

fn read_element(input: &[u8]) -> Result<Element<'_>, SpnegoError> {
    let tag = *input
        .first()
        .ok_or(SpnegoError::Malformed("empty element"))?;
    if tag & 0x1f == 0x1f {
        return Err(SpnegoError::Malformed("high tag number"));
    }
    let first = *input
        .get(1)
        .ok_or(SpnegoError::Malformed("missing length"))?;
    let (length, header) = if first < 0x80 {
        (first as usize, 2usize)
    } else {
        let count = (first & 0x7f) as usize;
        if count == 0 {
            return Err(SpnegoError::Malformed("indefinite length"));
        }
        if count > 4 {
            return Err(SpnegoError::Malformed("length too long"));
        }
        let bytes = input
            .get(2..2 + count)
            .ok_or(SpnegoError::Malformed("truncated length"))?;
        let length = bytes
            .iter()
            .fold(0usize, |total, byte| (total << 8) | *byte as usize);
        (length, 2 + count)
    };
    let end = header
        .checked_add(length)
        .ok_or(SpnegoError::Malformed("length overflow"))?;
    let value = input
        .get(header..end)
        .ok_or(SpnegoError::Malformed("truncated value"))?;
    Ok(Element {
        tag,
        value,
        whole: &input[..end],
        rest: &input[end..],
    })
}

fn expect(input: &[u8], tag: u8) -> Result<Element<'_>, SpnegoError> {
    let element = read_element(input)?;
    if element.tag != tag {
        return Err(SpnegoError::Malformed("unexpected tag"));
    }
    Ok(element)
}

fn encode_length(length: usize, out: &mut Vec<u8>) {
    if length < 0x80 {
        out.push(length as u8);
        return;
    }
    let bytes = (length as u32).to_be_bytes();
    let skip = bytes.iter().take_while(|byte| **byte == 0).count();
    out.push(0x80 | (4 - skip) as u8);
    out.extend_from_slice(&bytes[skip..]);
}

fn encode(tag: u8, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() + 6);
    out.push(tag);
    encode_length(value.len(), &mut out);
    out.extend_from_slice(value);
    out
}

/// Decodes the security buffer of a client SESSION_SETUP request.
pub fn parse_client(bytes: &[u8]) -> Result<ClientToken, SpnegoError> {
    if bytes.starts_with(RAW_PREFIX) {
        return Ok(ClientToken::Raw(bytes.to_vec()));
    }
    let outer = read_element(bytes)?;
    if !outer.rest.is_empty() {
        return Err(SpnegoError::Malformed("trailing bytes"));
    }
    match outer.tag {
        TAG_APPLICATION => {
            let oid = expect(outer.value, TAG_OID)?;
            if oid.value != SPNEGO_OID {
                return Err(SpnegoError::Malformed("not an SPNEGO token"));
            }
            let choice = expect(oid.rest, 0xa0)?;
            parse_init(choice.value).map(ClientToken::Init)
        }
        0xa0 => parse_init(outer.value).map(ClientToken::Init),
        0xa1 => parse_response(outer.value).map(ClientToken::Response),
        _ => Err(SpnegoError::Malformed("unknown negotiation token")),
    }
}

fn parse_init(content: &[u8]) -> Result<InitToken, SpnegoError> {
    let sequence = expect(content, TAG_SEQUENCE)?;
    let mut token = InitToken::default();
    let mut fields = sequence.value;
    let mut last = None;
    while !fields.is_empty() {
        let field = read_element(fields)?;
        if last.is_some_and(|previous| field.tag <= previous) {
            return Err(SpnegoError::Malformed("field order"));
        }
        last = Some(field.tag);
        match field.tag {
            0xa0 => {
                let list = expect(field.value, TAG_SEQUENCE)?;
                token.mechanism_list = list.whole.to_vec();
                let mut items = list.value;
                while !items.is_empty() {
                    let oid = expect(items, TAG_OID)?;
                    token.mechanisms.push(oid.value.to_vec());
                    items = oid.rest;
                }
            }
            0xa1 => {}
            0xa2 => token.token = Some(expect(field.value, TAG_OCTETS)?.value.to_vec()),
            0xa3 | 0xa4 => {
                let inner = read_element(field.value)?;
                if inner.tag == TAG_OCTETS {
                    token.list_mic = Some(inner.value.to_vec());
                }
            }
            _ => return Err(SpnegoError::Malformed("unknown NegTokenInit field")),
        }
        fields = field.rest;
    }
    if token.mechanisms.is_empty() {
        return Err(SpnegoError::Malformed("no mechanisms"));
    }
    Ok(token)
}

fn parse_response(content: &[u8]) -> Result<ResponseToken, SpnegoError> {
    let sequence = expect(content, TAG_SEQUENCE)?;
    let mut token = ResponseToken::default();
    let mut fields = sequence.value;
    let mut last = None;
    while !fields.is_empty() {
        let field = read_element(fields)?;
        if last.is_some_and(|previous| field.tag <= previous) {
            return Err(SpnegoError::Malformed("field order"));
        }
        last = Some(field.tag);
        match field.tag {
            0xa0 => {
                let state = expect(field.value, TAG_ENUMERATED)?;
                if state.value.len() != 1 {
                    return Err(SpnegoError::Malformed("negotiation state"));
                }
                token.state = Some(state.value[0]);
            }
            0xa1 => token.mechanism = Some(expect(field.value, TAG_OID)?.value.to_vec()),
            0xa2 => token.token = Some(expect(field.value, TAG_OCTETS)?.value.to_vec()),
            0xa3 => token.list_mic = Some(expect(field.value, TAG_OCTETS)?.value.to_vec()),
            _ => return Err(SpnegoError::Malformed("unknown NegTokenResp field")),
        }
        fields = field.rest;
    }
    Ok(token)
}

/// The DER MechTypeList that names only NTLMSSP.
pub fn ntlm_list() -> Vec<u8> {
    encode(TAG_SEQUENCE, &encode(TAG_OID, NTLMSSP_OID))
}

/// The NegTokenInit2 hint the server places in its NEGOTIATE response.
pub fn server_hint() -> Vec<u8> {
    let hints = encode(TAG_SEQUENCE, &encode(0xa0, &encode(TAG_GENERAL, HINT_NAME)));
    let mut fields = encode(0xa0, &ntlm_list());
    fields.extend_from_slice(&encode(0xa3, &hints));
    let init = encode(0xa0, &encode(TAG_SEQUENCE, &fields));
    let mut inner = encode(TAG_OID, SPNEGO_OID);
    inner.extend_from_slice(&init);
    encode(TAG_APPLICATION, &inner)
}

/// Encodes a NegTokenResp; `mechanism` adds the NTLMSSP supportedMech field.
pub fn encode_response(
    state: Option<u8>,
    mechanism: bool,
    token: Option<&[u8]>,
    list_mic: Option<&[u8]>,
) -> Vec<u8> {
    let mut fields = Vec::new();
    if let Some(state) = state {
        fields.extend_from_slice(&encode(0xa0, &encode(TAG_ENUMERATED, &[state])));
    }
    if mechanism {
        fields.extend_from_slice(&encode(0xa1, &encode(TAG_OID, NTLMSSP_OID)));
    }
    if let Some(token) = token {
        fields.extend_from_slice(&encode(0xa2, &encode(TAG_OCTETS, token)));
    }
    if let Some(list_mic) = list_mic {
        fields.extend_from_slice(&encode(0xa3, &encode(TAG_OCTETS, list_mic)));
    }
    encode(0xa1, &encode(TAG_SEQUENCE, &fields))
}

/// Encodes a client NegTokenInit wrapped in the GSS-API header, as clients send it.
#[cfg(test)]
pub(crate) fn encode_init(mechanisms: &[&[u8]], token: Option<&[u8]>) -> Vec<u8> {
    let mut list = Vec::new();
    for mechanism in mechanisms {
        list.extend_from_slice(&encode(TAG_OID, mechanism));
    }
    let mut fields = encode(0xa0, &encode(TAG_SEQUENCE, &list));
    if let Some(token) = token {
        fields.extend_from_slice(&encode(0xa2, &encode(TAG_OCTETS, token)));
    }
    let mut inner = encode(TAG_OID, SPNEGO_OID);
    inner.extend_from_slice(&encode(0xa0, &encode(TAG_SEQUENCE, &fields)));
    encode(TAG_APPLICATION, &inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hint_names_ntlmssp() {
        let hint = server_hint();
        let ClientToken::Init(init) = parse_client(&hint).expect("the hint parses") else {
            panic!("the hint is a NegTokenInit");
        };
        assert_eq!(init.mechanisms, vec![NTLMSSP_OID.to_vec()]);
        assert_eq!(init.mechanism_list, ntlm_list());
        assert_eq!(init.token, None);
    }

    #[test]
    fn init_keeps_list() {
        let kerberos: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x02];
        let bytes = encode_init(&[kerberos, NTLMSSP_OID], Some(&b"NTLMSSP\0token"[..]));
        let ClientToken::Init(init) = parse_client(&bytes).expect("init parses") else {
            panic!("expected init");
        };
        assert_eq!(init.mechanisms.len(), 2);
        assert_eq!(init.mechanisms[1], NTLMSSP_OID);
        assert_eq!(init.token.as_deref(), Some(&b"NTLMSSP\0token"[..]));
        let mut expected = encode(TAG_OID, kerberos);
        expected.extend_from_slice(&encode(TAG_OID, NTLMSSP_OID));
        assert_eq!(init.mechanism_list, encode(TAG_SEQUENCE, &expected));
    }

    #[test]
    fn response_round_trip() {
        let long = vec![7u8; 300];
        let bytes = encode_response(
            Some(ACCEPT_INCOMPLETE),
            true,
            Some(long.as_slice()),
            Some(&b"mic"[..]),
        );
        let ClientToken::Response(token) = parse_client(&bytes).expect("response parses") else {
            panic!("expected response");
        };
        assert_eq!(token.state, Some(ACCEPT_INCOMPLETE));
        assert_eq!(token.mechanism.as_deref(), Some(NTLMSSP_OID));
        assert_eq!(token.token, Some(long));
        assert_eq!(token.list_mic.as_deref(), Some(&b"mic"[..]));
    }

    #[test]
    fn raw_ntlmssp_passes() {
        let token = parse_client(b"NTLMSSP\0\x01\0\0\0").expect("raw token");
        assert!(matches!(token, ClientToken::Raw(_)));
    }

    #[test]
    fn rejects_malformed_lengths() {
        assert!(parse_client(&[0xa1, 0x80, 0x00, 0x00]).is_err());
        assert!(parse_client(&[0xa1, 0x85, 1, 1, 1, 1, 1]).is_err());
        assert!(parse_client(&[0xa1, 0x05, 0x30]).is_err());
        assert!(parse_client(&[]).is_err());
        let mut trailing = encode_response(Some(ACCEPT_COMPLETED), false, None, None);
        trailing.push(0);
        assert!(parse_client(&trailing).is_err());
    }

    #[test]
    fn rejects_field_disorder() {
        let mut fields = encode(0xa2, &encode(TAG_OCTETS, b"x"));
        fields.extend_from_slice(&encode(0xa0, &encode(TAG_ENUMERATED, &[0])));
        let bytes = encode(0xa1, &encode(TAG_SEQUENCE, &fields));
        assert!(parse_client(&bytes).is_err());
    }
}
