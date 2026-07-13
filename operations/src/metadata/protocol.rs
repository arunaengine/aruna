use aruna_core::metadata::{MetadataQueryResults, MetadataSearchHit};
use aruna_core::structs::MetadataRegistryRecord;
use aruna_core::types::GroupId;
use aruna_net::streams::BiStream;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;
use tokio::io::AsyncWriteExt;
use ulid::Ulid;

use crate::create_metadata_document::CreateMetadataDocumentPayload;
use crate::update_metadata_document::UpdateMetadataDocumentMutation;

const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
pub const MAX_METADATA_BEARER_TOKEN_LEN: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataAuthToken {
    Bearer(MetadataBearerToken),
}

impl MetadataAuthToken {
    pub fn bearer(token: impl Into<String>) -> Result<Self, MetadataAuthTokenError> {
        MetadataBearerToken::new(token).map(Self::Bearer)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MetadataBearerToken(String);

impl MetadataBearerToken {
    pub fn new(token: impl Into<String>) -> Result<Self, MetadataAuthTokenError> {
        let token = token.into();
        if token.len() > MAX_METADATA_BEARER_TOKEN_LEN {
            return Err(MetadataAuthTokenError {
                length: token.len(),
            });
        }
        Ok(Self(token))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for MetadataBearerToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let token = String::deserialize(deserializer)?;
        Self::new(token).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataAuthTokenError {
    length: usize,
}

impl fmt::Display for MetadataAuthTokenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "metadata bearer token length {} exceeds maximum {}",
            self.length, MAX_METADATA_BEARER_TOKEN_LEN
        )
    }
}

impl std::error::Error for MetadataAuthTokenError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetadataTransportMessage {
    QueryGraphs {
        auth_token: Option<MetadataAuthToken>,
        graph_iris: Option<Vec<String>>,
        sparql: String,
    },
    QueryResults {
        results: MetadataQueryResults,
    },
    SearchGraphs {
        auth_token: Option<MetadataAuthToken>,
        graph_iris: Option<Vec<String>>,
        query: String,
        limit: usize,
    },
    SearchResults {
        hits: Vec<MetadataSearchHit>,
    },
    /// A metadata write that arrived at a node holding none of the document's
    /// bucket, forwarded to a holder. The payloads mirror the HTTP handlers'
    /// deconstructed request; `auth_token` carries the caller's authority so the
    /// holder re-runs the same permission checks the origin would have run.
    ForwardCreateDocument {
        auth_token: Option<MetadataAuthToken>,
        group_id: GroupId,
        document_id: Ulid,
        document_path: String,
        public: bool,
        payload: CreateMetadataDocumentPayload,
    },
    ForwardUpdateDocument {
        auth_token: Option<MetadataAuthToken>,
        document_id: Ulid,
        /// `None` leaves the holder's current visibility untouched: the origin's
        /// record copy may be stale, so only an explicit request value travels.
        public: Option<bool>,
        mutation: UpdateMetadataDocumentMutation,
    },
    ForwardDeleteDocument {
        auth_token: Option<MetadataAuthToken>,
        document_id: Ulid,
    },
    ForwardedRecord {
        record: Box<MetadataRegistryRecord>,
    },
    ForwardedDelete,
    Reject(String),
    /// A permanent update validation failure. Appended after `Reject` so the
    /// postcard discriminants of existing control messages remain stable.
    ForwardedUpdateInvalidInput {
        message: String,
    },
}

pub async fn write_message(
    stream: &mut BiStream,
    message: &MetadataTransportMessage,
) -> Result<(), String> {
    let bytes = encode_message(message)?;
    write_encoded_message(stream, &bytes).await
}

pub(crate) fn encode_message(message: &MetadataTransportMessage) -> Result<Vec<u8>, String> {
    let bytes = postcard::to_allocvec(message).map_err(|err| err.to_string())?;
    if bytes.len() > MAX_MESSAGE_SIZE {
        return Err("metadata message exceeds maximum size".to_string());
    }

    Ok(bytes)
}

pub(crate) async fn write_encoded_message(
    stream: &mut BiStream,
    bytes: &[u8],
) -> Result<(), String> {
    stream
        .0
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|err| err.to_string())?;
    stream
        .0
        .write_all(bytes)
        .await
        .map_err(|err| err.to_string())?;
    stream.0.flush().await.map_err(|err| err.to_string())?;
    Ok(())
}

pub async fn read_message(stream: &mut BiStream) -> Result<MetadataTransportMessage, String> {
    let mut len_buf = [0u8; 4];
    stream
        .1
        .read_exact(&mut len_buf)
        .await
        .map_err(|err| err.to_string())?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_MESSAGE_SIZE {
        return Err("metadata frame exceeds maximum size".to_string());
    }

    let mut bytes = vec![0u8; len];
    stream
        .1
        .read_exact(&mut bytes)
        .await
        .map_err(|err| err.to_string())?;
    postcard::from_bytes(&bytes).map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_messages_use_auth_token_fields() {
        assert_has_auth_token_field(MetadataTransportMessage::QueryGraphs {
            auth_token: Some(MetadataAuthToken::bearer("query-token").unwrap()),
            graph_iris: None,
            sparql: "ASK {}".to_string(),
        });
        assert_has_auth_token_field(MetadataTransportMessage::SearchGraphs {
            auth_token: Some(MetadataAuthToken::bearer("search-token").unwrap()),
            graph_iris: None,
            query: "dataset".to_string(),
            limit: 10,
        });
    }

    #[test]
    fn forwarded_writes_carry_authority() {
        // The holder applies a forwarded write under the caller's own token, so
        // every forward variant must carry one: a tokenless forward would be an
        // unauthenticated internal write path.
        assert_has_auth_token_field(MetadataTransportMessage::ForwardCreateDocument {
            auth_token: Some(MetadataAuthToken::bearer("create-token").unwrap()),
            group_id: Ulid::nil(),
            document_id: Ulid::nil(),
            document_path: "datasets/forwarded".to_string(),
            public: true,
            payload: CreateMetadataDocumentPayload::RoCrate {
                jsonld: "{}".to_string(),
            },
        });
        assert_has_auth_token_field(MetadataTransportMessage::ForwardUpdateDocument {
            auth_token: Some(MetadataAuthToken::bearer("update-token").unwrap()),
            document_id: Ulid::nil(),
            public: None,
            mutation: UpdateMetadataDocumentMutation::UpsertDataEntity {
                jsonld: "{}".to_string(),
            },
        });
        assert_has_auth_token_field(MetadataTransportMessage::ForwardDeleteDocument {
            auth_token: Some(MetadataAuthToken::bearer("delete-token").unwrap()),
            document_id: Ulid::nil(),
        });
    }

    #[test]
    fn forwarded_create_round_trips() {
        let message = MetadataTransportMessage::ForwardCreateDocument {
            auth_token: Some(MetadataAuthToken::bearer("create-token").unwrap()),
            group_id: Ulid::from_bytes([3u8; 16]),
            document_id: Ulid::from_bytes([4u8; 16]),
            document_path: "datasets/forwarded".to_string(),
            public: false,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: "Forwarded".to_string(),
                description: "Placed by a holder".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
        };
        let bytes = postcard::to_allocvec(&message).unwrap();

        assert_eq!(
            postcard::from_bytes::<MetadataTransportMessage>(&bytes).unwrap(),
            message
        );
    }

    #[test]
    fn legacy_reject_stable() {
        assert_eq!(
            postcard::to_allocvec(&MetadataTransportMessage::Reject(String::new())).unwrap(),
            vec![9, 0]
        );
    }

    #[test]
    fn oversized_bearer_tokens_are_rejected() {
        let oversized = "x".repeat(MAX_METADATA_BEARER_TOKEN_LEN + 1);

        assert!(MetadataAuthToken::bearer(oversized).is_err());
    }

    #[test]
    fn bearer_auth_token_round_trips_through_postcard() {
        let token = MetadataAuthToken::bearer("bearer-token").unwrap();
        let bytes = postcard::to_allocvec(&token).unwrap();

        let decoded = postcard::from_bytes::<MetadataAuthToken>(&bytes).unwrap();

        assert_eq!(decoded, token);
        let MetadataAuthToken::Bearer(bearer) = decoded;
        assert_eq!(bearer.as_str(), "bearer-token");
    }

    #[test]
    fn oversized_bearer_tokens_are_rejected_on_decode() {
        #[derive(Serialize)]
        enum RawAuthToken {
            Bearer(String),
        }

        #[derive(Serialize)]
        enum RawTransportMessage {
            QueryGraphs {
                auth_token: Option<RawAuthToken>,
                graph_iris: Option<Vec<String>>,
                sparql: String,
            },
        }

        let bytes = postcard::to_allocvec(&RawTransportMessage::QueryGraphs {
            auth_token: Some(RawAuthToken::Bearer(
                "x".repeat(MAX_METADATA_BEARER_TOKEN_LEN + 1),
            )),
            graph_iris: None,
            sparql: "ASK {}".to_string(),
        })
        .unwrap();

        assert!(postcard::from_bytes::<MetadataTransportMessage>(&bytes).is_err());
    }

    fn assert_has_auth_token_field(message: MetadataTransportMessage) {
        let value = serde_json::to_value(message).unwrap();
        let fields = value
            .as_object()
            .and_then(|variants| variants.values().next())
            .and_then(|variant| variant.as_object())
            .unwrap();

        assert!(fields.contains_key("auth_token"));
        assert!(!fields.contains_key("auth_context"));
    }
}
