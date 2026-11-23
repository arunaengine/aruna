//! Defines the DHT wire messages and stored entries with their encoding and signature checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};

use crate::alpn::Alpn;
use crate::id::{DhtKeyId, NodeId};
use crate::structs::identity::realm::RealmId;
use crate::trace_context::DistributedTraceContext;

pub const DHT_ALPN: &[u8] = Alpn::Dht.as_bytes();

/// DHT RPC request messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DhtRequest {
    Ping,
    FindNode {
        /// Target is raw bytes - could be a node ID or key hash
        target: [u8; 32],
    },
    GetValue {
        key: DhtKeyId,
        realm_filter: Option<RealmId>,
    },
    PutValue {
        key: DhtKeyId,
        realm_id: RealmId,
        value: Vec<u8>,
        expires_at: u64,
        revision: u64,
        publisher: NodeId,
        /// Required Ed25519 signature over the complete record envelope.
        signature: Option<iroh::Signature>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DhtRequestEnvelope {
    trace_context: Option<DistributedTraceContext>,
    request: DhtRequest,
}

/// DHT RPC response messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DhtResponse {
    Pong,
    /// Response to FindNode - returns node IDs closest to target.
    /// Connection info is looked up via iroh discovery.
    Nodes {
        nodes: Vec<NodeId>,
    },
    Value {
        /// Retained signed values, including expired anti-rollback floors.
        entries: Vec<StoredValue>,
        /// Closer nodes if we don't have the value (or in addition to it)
        closer_nodes: Vec<NodeId>,
    },
    Stored,
    Error {
        code: ErrorCode,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    InvalidRequest,
    KeyNotFound,
    StorageFull,
    Internal,
    InvalidSignature,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredValue {
    pub publisher: NodeId,
    pub realm_id: RealmId,
    pub value: Vec<u8>,
    pub expires_at: u64,
    pub revision: u64,
    pub signature: iroh::Signature,
}

/// One retained DHT record row; `retain_until` keeps an expired record as an anti-rollback floor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredEntry {
    pub publisher: NodeId,
    pub realm_id: RealmId,
    pub value: Vec<u8>,
    pub expires_at: u64,
    pub revision: u64,
    pub signature: iroh::Signature,
    pub retain_until: u64,
}

pub fn signed_record_bytes(
    key: &DhtKeyId,
    publisher: &NodeId,
    realm_id: &RealmId,
    value: &[u8],
    expires_at: u64,
    revision: u64,
) -> Vec<u8> {
    const SIGNING_CONTEXT: &[u8] = b"aruna/dht/2/record";

    let mut signed_data =
        Vec::with_capacity(SIGNING_CONTEXT.len() + 32 + 32 + 32 + 8 + 8 + 8 + value.len());
    signed_data.extend_from_slice(SIGNING_CONTEXT);
    signed_data.extend_from_slice(key.as_bytes());
    signed_data.extend_from_slice(publisher.as_bytes());
    signed_data.extend_from_slice(realm_id.as_bytes());
    signed_data.extend_from_slice(&expires_at.to_le_bytes());
    signed_data.extend_from_slice(&revision.to_le_bytes());
    signed_data.extend_from_slice(&(value.len() as u64).to_le_bytes());
    signed_data.extend_from_slice(value);
    signed_data
}

pub fn verify_stored_value(key: &DhtKeyId, entry: &StoredValue) -> bool {
    verify_record(
        key,
        &entry.publisher,
        &entry.realm_id,
        &entry.value,
        entry.expires_at,
        entry.revision,
        &entry.signature,
    )
}

pub fn verify_record(
    key: &DhtKeyId,
    publisher: &NodeId,
    realm_id: &RealmId,
    value: &[u8],
    expires_at: u64,
    revision: u64,
    signature: &iroh::Signature,
) -> bool {
    let signed_data = signed_record_bytes(key, publisher, realm_id, value, expires_at, revision);
    publisher.verify(&signed_data, signature).is_ok()
}

pub fn encode_request(req: &DhtRequest) -> Result<Vec<u8>, postcard::Error> {
    encode_traced_request(req, None)
}

pub fn encode_traced_request(
    req: &DhtRequest,
    trace_context: Option<DistributedTraceContext>,
) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&DhtRequestEnvelope {
        trace_context,
        request: req.clone(),
    })
}

pub fn decode_request(bytes: &[u8]) -> Result<DhtRequest, postcard::Error> {
    decode_traced_request(bytes).map(|(_, request)| request)
}

pub fn decode_traced_request(
    bytes: &[u8],
) -> Result<(Option<DistributedTraceContext>, DhtRequest), postcard::Error> {
    decode_exact::<DhtRequestEnvelope>(bytes)
        .map(|envelope| (envelope.trace_context, envelope.request))
}

pub fn encode_response(resp: &DhtResponse) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(resp)
}

pub fn decode_response(bytes: &[u8]) -> Result<DhtResponse, postcard::Error> {
    decode_exact(bytes)
}

/// Decodes one postcard value and rejects trailing bytes.
pub fn decode_exact<'a, T>(bytes: &'a [u8]) -> Result<T, postcard::Error>
where
    T: Deserialize<'a>,
{
    let (value, remainder) = postcard::take_from_bytes(bytes)?;
    if remainder.is_empty() {
        Ok(value)
    } else {
        Err(postcard::Error::DeserializeBadEncoding)
    }
}
