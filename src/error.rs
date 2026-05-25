use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum S2CError {

    #[error("application result unavailable")]
    ApplicationResultUnavailable,

    #[error("commit failure")]
    CommitError,

    #[error("concurrent state modification")]
    ConcurrentStateModification,

    #[error("request out of sequence; next expected: {next_seq_num}")]
    RequestOutOfSequence { next_seq_num: u64 },

    #[error("low-rank node; highest rank seen: {highest_rank}")]
    LowRankNode { highest_rank: i32 },

    #[error("operation not permitted: node is not the leader")]
    OperationNotPermitted,

    #[error("object corrupted: {0}")]
    ObjectCorrupted(String),

    #[error("non-transient S3 error: {0}")]
    NonTransientS3(String),

    #[error("log replayer broken: {0}")]
    ReplayerBroken(String),



    #[error("IO error: {0}")]
    Io(String)
}

impl From<std::io::Error> for S2CError {
    fn from(err: std::io::Error) -> Self {
        S2CError::Io(err.to_string())
    }
}

