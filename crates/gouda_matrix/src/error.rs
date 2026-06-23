use std::borrow::Cow;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("the requested feature is not implemented")]
    NotImplemented,

    #[error("client has not yet been initialized")]
    NotInitialized,

    #[error("client is already initialized")]
    AlreadyInitialized,

    #[error("client is currently not logged in")]
    NotLoggedIn,

    #[error("the client is already logged in")]
    AlreadyLoggedIn,

    #[error("network error")]
    Network,

    #[error("timeout error")]
    Timeout,

    #[error("authorization error")]
    Authorization,

    #[error("the given URL is invalid")]
    InvalidUrl,

    #[error("the given user ID is invalid")]
    InvalidUserId,

    #[error("the requested user was not found")]
    UserNotFound,

    #[error("the given room ID is invalid")]
    InvalidRoomId,

    #[error("the requested room was not found")]
    RoomNotFound,

    #[error("the given message ID is invalid")]
    InvalidMessageId,

    #[error("the requested message as not found")]
    MessageNotFound,

    #[error("the requested reaction was not found")]
    ReactionNotFound,

    #[error("the requested verification flow was not found")]
    VerificationFlowNotFound,

    #[error("the requested cross signing method is not supported")]
    UnsupportedCrossSigningMethod,

    #[error("internal error: {0}")]
    #[allow(clippy::enum_variant_names)]
    InternalError(Cow<'static, str>),

    #[error(transparent)]
    #[allow(clippy::enum_variant_names)]
    MediaError(#[from] crate::media::MediaError),

    #[error(transparent)]
    #[allow(clippy::enum_variant_names)]
    MemoryCacheError(#[from] crate::memory_cache::MemoryCacheError),

    #[error(transparent)]
    #[allow(clippy::enum_variant_names)]
    MatrixSdkError(#[from] matrix_sdk::Error),

    #[error(transparent)]
    #[allow(clippy::enum_variant_names)]
    MatrixSdkClientBuildError(Box<matrix_sdk::ClientBuildError>),

    #[error(transparent)]
    #[allow(clippy::enum_variant_names)]
    MatrixSdkHttpError(#[from] matrix_sdk::HttpError),

    #[error(transparent)]
    #[allow(clippy::enum_variant_names)]
    MatrixSdkEditError(#[from] matrix_sdk::room::edit::EditError),

    #[error(transparent)]
    #[allow(clippy::enum_variant_names)]
    MatrixSdkRecoveryError(#[from] matrix_sdk::encryption::recovery::RecoveryError),

    #[error(transparent)]
    #[allow(clippy::enum_variant_names)]
    MatrixSdkStoreError(#[from] matrix_sdk::StoreError),

    #[error(transparent)]
    #[allow(clippy::enum_variant_names)]
    MatrixSdkCryptoStoreError(#[from] matrix_sdk_crypto::CryptoStoreError),

    #[error(transparent)]
    #[allow(clippy::enum_variant_names)]
    MatrixSdkRefreshTokenError(#[from] matrix_sdk::RefreshTokenError),

    #[error(transparent)]
    #[allow(clippy::enum_variant_names)]
    MatrixSdkRequestVerificationError(
        #[from] matrix_sdk::encryption::identities::RequestVerificationError,
    ),
}

impl From<matrix_sdk::ClientBuildError> for Error {
    fn from(err: matrix_sdk::ClientBuildError) -> Self {
        Self::MatrixSdkClientBuildError(Box::new(err))
    }
}

impl Error {
    pub fn internal(msg: impl Into<Cow<'static, str>>) -> Self {
        Error::InternalError(msg.into())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for gouda_proto::chat::Error {
    fn from(value: Error) -> Self {
        todo!()
    }
}
