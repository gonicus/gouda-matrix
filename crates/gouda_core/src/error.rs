use thiserror::Error;

/// An error returned from the runner.
#[derive(Debug, Error)]
pub enum RunnerError {
    #[allow(missing_docs)]
    #[error("Channel for receiving requests closed")]
    RequestChannelClosed,

    #[allow(missing_docs)]
    #[error("Channel for sending responses closed")]
    ResponseChannelClosed,

    #[allow(missing_docs)]
    #[error("Received invalid data on the input reader")]
    InvalidData,

    #[allow(missing_docs)]
    #[error("An internal task panicked or was unexpectedly cancelled")]
    TaskPanicked,

    #[allow(missing_docs)]
    #[error("An internal channel was unexpectedly closed")]
    InternalChannelClosed,
}

/// A result returned from the runner.
pub type RunnerResult<T> = std::result::Result<T, RunnerError>;
