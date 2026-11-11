use object_store::Error;

use crate::network::error::{ClientError, NetworkError, ServerError};

pub enum S2cError {
    NetworkError(NetworkError),
    ServerError(ServerError),
    ClientError(ClientError),
    NonTransientObjectStoreError(object_store::Error),
    StaleEpoch,
    ConcurrentLeaders,
}

pub fn is_transient(error: &Error) -> bool {
    match error {
        Error::Generic { source, store } => {
            let err_msg = source.to_string().to_lowercase();
            err_msg.contains("slowdown")
                || err_msg.contains("503")
                || err_msg.contains("timeout")
                || err_msg.contains("requesttimeout")
                || err_msg.contains("internalerror")
                || err_msg.contains("serviceunavailable")
                || err_msg.contains("skewed")
        }
        _ => false,
    }
}
