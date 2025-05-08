use thiserror::Error;

#[derive(Debug, Error)]
pub enum RealmError {
    #[error("Failed to init Realm: {0}")]
    InitError(String),

    #[error("Network operation failed: {0}")]
    NetworkError(String),

    #[error("Kademlia operation failed: {0}")]
    KademliaError(String),

    #[error("Serialization/deserialization failed: {0}")]
    SerializationError(#[from] postcard::Error),

    #[error("Authentication failed: {0}")]
    AuthenticationError(String),

    #[error("Custom error: {0}")]
    CustomError(String),
}
