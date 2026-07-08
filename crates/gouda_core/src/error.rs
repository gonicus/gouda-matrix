use thiserror::Error;
use tokio::task::JoinError;

#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("Request reader dropped")]
    ReaderDropped,
    #[error("Response writer dropped")]
    WriterDropped,
    #[error("Received invalid data on the input reader")]
    InvalidData,
    #[error("Join error: {0}")]
    JoinError(#[from] JoinError),
}

pub type RunnerResult<T> = std::result::Result<T, RunnerError>;
