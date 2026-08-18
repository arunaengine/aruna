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

    /// Inbound sync stream refused before any payload byte was read (unknown
    /// peer or exhausted admission budget). No local state can have changed, so
    /// callers must drop it without a full-topic reconcile.
    #[error("Admission rejected: {0}")]
    AdmissionRejected(String),

    #[error("Timeout after {0:?}")]
    Timeout(Duration),

    /// The document sync topic has no local genesis yet and this publisher is not
    /// the document's origin, so it may not mint one. Retryable: the write waits
    /// for the origin's genesis to replicate in.
    #[error("Document sync topic {0} not ready")]
    TopicNotReady(String),

    /// A replicated document names evidence that has not arrived yet. Retryable:
    /// the event stays unapplied and its topic cursor does not advance.
    #[error("Deferred until its dependency replicates: {0}")]
    Deferred(String),

    #[error("I/O error: {0}")]
    Io(String),

    #[error("Invalid effect")]
    InvalidEffect,

    #[error("Channel closed")]
    ChannelClosed,
}

impl NetError {
    /// Whether this rejection happened before any payload processing, so no
    /// partial local state can exist to reconcile.
    pub fn is_admission_rejection(&self) -> bool {
        matches!(self, NetError::AdmissionRejected(_))
    }
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
