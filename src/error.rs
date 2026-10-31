use object_store::Error;

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
