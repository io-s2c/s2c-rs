use thiserror::Error;

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Io error: {err}")]
    Io { err: std::io::Error },

    #[error("message size {size} too big. Max allowed {max}")]
    MessageTooLarge { max: u32, size: u32 },

    #[error("Invalid protobuf: {err}")]
    InvalidProtobuf { err: String },
}

#[derive(Error, Debug, Clone)]
pub enum ClientError {
    #[error("Client stopped")]
    Closed,
    #[error("IO error: {0}")]
    Io(String),
    #[error("Timeout")]
    ConnectTimeout,
    #[error("Client not connected")]
    NotConnected,
    #[error("Request timed out")]
    Timeout,
}
