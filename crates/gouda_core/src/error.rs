use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Request reader dropped")]
    ReaderDropped,
    #[error("Response writer dropped")]
    WriterDropped,
}
