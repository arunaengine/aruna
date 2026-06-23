use aruna_core::metadata::{MetadataQueryResults, MetadataSearchHit};
use aruna_net::streams::BiStream;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;
use tokio::io::AsyncWriteExt;

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
    Reject(String),
}

pub async fn write_message(
    stream: &mut BiStream,
    message: &MetadataTransportMessage,
) -> Result<(), String> {
    let bytes = postcard::to_allocvec(message).map_err(|err| err.to_string())?;
    if bytes.len() > MAX_MESSAGE_SIZE {
        return Err("metadata message exceeds maximum size".to_string());
    }

    stream
        .0
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|err| err.to_string())?;
    stream
        .0
        .write_all(&bytes)
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
    fn oversized_bearer_tokens_are_rejected() {
        let oversized = "x".repeat(MAX_METADATA_BEARER_TOKEN_LEN + 1);

        assert!(MetadataAuthToken::bearer(oversized).is_err());
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
