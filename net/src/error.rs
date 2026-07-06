use iroh::endpoint::VarIntBoundsExceeded;
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum NetError {
    #[error("Bootstrap failed: {0}")]
    Bootstrap(String),

    #[error("Connection failed: {0}")]
    Connection(String),

    #[error("DHT error: {0}")]
    Dht(String),

    #[error("Stream error: {0}")]
    Stream(String),

    #[error("Timeout after {0:?}")]
    Timeout(Duration),

    /// The document sync topic has no local genesis yet and this publisher is not
    /// the document's origin, so it may not mint one. Retryable: the write waits
    /// for the origin's genesis to replicate in.
    #[error("Document sync topic {0} not ready")]
    TopicNotReady(String),

    #[error("I/O error: {0}")]
    Io(String),

    #[error("Invalid effect")]
    InvalidEffect,

    #[error("Channel closed")]
    ChannelClosed,
}

impl From<std::io::Error> for NetError {
    fn from(err: std::io::Error) -> Self {
        NetError::Io(err.to_string())
    }
}

impl From<VarIntBoundsExceeded> for NetError {
    fn from(err: VarIntBoundsExceeded) -> Self {
        NetError::Connection(format!("Setup error: {}", err))
    }
}

pub type Result<T> = std::result::Result<T, NetError>;
