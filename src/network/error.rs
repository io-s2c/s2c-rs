use std::sync::Arc;
use thiserror::Error;
use tokio::io;

#[derive(Error, Debug, Clone)]
pub enum NetworkError {
    #[error("Io error: {kind}, msg: {msg}")]
    Io { kind: io::ErrorKind, msg: String },

    #[error("message size {size} too big. Max allowed {max}")]
    MessageTooLarge { max: u32, size: u32 },

    #[error("Invalid protobuf: {err}")]
    InvalidProtobuf { err: String },
}

#[derive(Error, Debug, Clone)]
pub enum ClientError {
    #[error("Client closed")]
    Closed,
    #[error("IO error: {err}")]
    Io {err: NetworkError},
    #[error("Timeout")]
    ConnectTimeout,
    #[error("Client not connected")]
    NotConnected,
    #[error("Request timed out")]
    Timeout,
}
