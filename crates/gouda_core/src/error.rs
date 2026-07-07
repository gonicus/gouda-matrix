use thiserror::Error;
use tokio::task::JoinError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Request reader dropped")]
    ReaderDropped,
    #[error("Response writer dropped")]
    WriterDropped,
    #[error("Join error: {0}")]
    JoinError(#[from] JoinError),
}
