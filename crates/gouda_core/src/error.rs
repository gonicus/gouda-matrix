use thiserror::Error;

#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("Request reader dropped")]
    ReaderDropped,
    #[error("Response writer dropped")]
    WriterDropped,
    #[error("Received invalid data on the input reader")]
    InvalidData,
    #[error("An inernal task panicked or was unexpectedly cancelled")]
    TaskPanic,
}

pub type RunnerResult<T> = std::result::Result<T, RunnerError>;
