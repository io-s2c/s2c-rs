use std::sync::Arc;

use thiserror::Error;

use crate::network::{client::ClientState, server::ServerState};

#[derive(Error, Debug, Clone)]
pub enum NetworkError {
    #[error("Io error: {err}")]
    Io { err: Arc<std::io::Error> },

    #[error("message size {size} too big. Max allowed {max}")]
    MessageTooLarge { max: u32, size: u32 },

    #[error("Invalid protobuf: {err}")]
    InvalidProtobuf { err: String },
}

#[derive(Error, Debug, Clone)]
pub enum ClientError {
    #[error("Wrong state {client_state}")]
    WrongState { client_state: ClientState },
    #[error("Network error: {err}")]
    NetworkError { err: NetworkError },
    #[error("Timeout")]
    ConnectTimeout,
    #[error("Client not connected")]
    NotConnected,
    #[error("Request timed out")]
    Timeout,
}

#[derive(Error, Debug, Clone)]
pub enum ServerError {
    #[error("Wrong state {server_state}")]
    WrongState { server_state: ServerState },
    #[error("Network error: {err}")]
    NetworkError { err: NetworkError },
}
