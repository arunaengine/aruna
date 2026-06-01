use aruna_core::metadata::{MetadataQueryResults, MetadataSearchHit};
use aruna_core::structs::AuthContext;
use aruna_net::streams::BiStream;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetadataTransportMessage {
    QueryGraphs {
        auth_context: Option<AuthContext>,
        graph_iris: Option<Vec<String>>,
        sparql: String,
    },
    QueryResults {
        results: MetadataQueryResults,
    },
    SearchGraphs {
        auth_context: Option<AuthContext>,
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
