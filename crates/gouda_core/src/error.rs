use thiserror::Error;

/// An error returned from the runner.
#[derive(Debug, Error)]
pub enum RunnerError {
    #[allow(missing_docs)]
    #[error("Request reader dropped")]
    ReaderDropped,

    #[allow(missing_docs)]
    #[error("Response writer dropped")]
    WriterDropped,

    #[allow(missing_docs)]
    #[error("Received invalid data on the input reader")]
    InvalidData,

    #[allow(missing_docs)]
    #[error("An internal task panicked or was unexpectedly cancelled")]
    TaskPanic,

    #[allow(missing_docs)]
    #[error("An internal channel was unexpectedly closed")]
    ChannelClosed,
}

/// A result returned from the runner.
pub type RunnerResult<T> = std::result::Result<T, RunnerError>;
