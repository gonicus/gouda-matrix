use std::borrow::Cow;

use gouda_proto::chat::Error as ChatError;
use thiserror::Error;

macro_rules! chat_err {
    ($ty:ident) => {
        ChatError {
            r#type: gouda_proto::chat::error::ErrorType::$ty.into(),
            error_string: None,
        }
    };
    ($ty:ident, $msg:expr) => {
        ChatError {
            r#type: gouda_proto::chat::error::ErrorType::$ty.into(),
            error_string: Some($msg.to_string()),
        }
    };
}

pub(crate) use chat_err;

pub type Result<T> = std::result::Result<T, Error>;

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

    #[error("the session has been logged out")]
    LoggedOut,

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

    #[error("the given thread ID is invalid")]
    InvalidThreadId,

    #[error("the requested thread was not found")]
    ThreadNotFound,

    #[error("the requested verification flow was not found")]
    VerificationFlowNotFound,

    #[error("the requested cross signing method is not supported")]
    UnsupportedCrossSigningMethod,

    #[error("no supported cross signing method was set")]
    NoCrossSigningMethod,

    #[error("generic conversion error")]
    #[allow(clippy::enum_variant_names)]
    ConversionError,

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

impl From<Error> for ChatError {
    fn from(value: Error) -> Self {
        match value {
            Error::NotImplemented => chat_err!(NotImplemented),
            Error::NotInitialized => chat_err!(NotInitialized),
            Error::AlreadyInitialized => chat_err!(AlreadyInitialized),
            Error::NotLoggedIn => chat_err!(Authorization, "Not logged in"),
            Error::AlreadyLoggedIn => chat_err!(AlreadyLoggedIn),
            Error::LoggedOut => chat_err!(Authorization, "Session has been logged out"),
            Error::Network => chat_err!(Network),
            Error::Timeout => chat_err!(Timeout),
            Error::Authorization => chat_err!(Authorization),
            Error::InvalidUrl => chat_err!(InvalidUrl),
            Error::InvalidUserId => chat_err!(InvalidUserId),
            Error::UserNotFound => chat_err!(UserNotFound),
            Error::InvalidRoomId => chat_err!(RoomNotFound),
            Error::RoomNotFound => chat_err!(RoomNotFound),
            Error::InvalidMessageId => chat_err!(InvalidMessageId),
            Error::MessageNotFound => chat_err!(MessageNotFound),
            Error::ReactionNotFound => chat_err!(ReactionNotFound),
            Error::InvalidThreadId => chat_err!(ThreadNotFound),
            Error::ThreadNotFound => chat_err!(ThreadNotFound),
            Error::VerificationFlowNotFound => chat_err!(VerificationFlowNotFound),
            Error::UnsupportedCrossSigningMethod => chat_err!(Unknown, value),
            Error::NoCrossSigningMethod => chat_err!(Unknown, value),
            Error::ConversionError => chat_err!(Unknown, "Internal conversion error"),
            Error::InternalError(err) => chat_err!(Unknown, err),
            Error::MediaError(err) => err.into(),
            Error::MemoryCacheError(err) => err.into(),
            Error::MatrixSdkError(err) => convert_matrix_sdk_error(err),
            Error::MatrixSdkClientBuildError(err) => convert_client_build_error(err),
            Error::MatrixSdkHttpError(err) => convert_http_error(err),
            Error::MatrixSdkEditError(err) => convert_edit_error(err),
            Error::MatrixSdkRecoveryError(err) => convert_recovery_error(err),
            Error::MatrixSdkStoreError(err) => convert_store_error(err),
            Error::MatrixSdkCryptoStoreError(err) => convert_crypto_store_error(err),
            Error::MatrixSdkRefreshTokenError(err) => convert_refresh_token_error(err),
            Error::MatrixSdkRequestVerificationError(err) => {
                convert_request_verification_error(err)
            }
        }
    }
}

fn convert_into_http_error(err: ruma_common::api::error::IntoHttpError) -> ChatError {
    use ruma_common::api::error::IntoHttpError;

    log::error!("Received IntoHttpError: {err:?}");

    match err {
        IntoHttpError::Authentication(_) => chat_err!(Authorization),
        _ => chat_err!(Network, err),
    }
}

fn convert_http_error(err: matrix_sdk::HttpError) -> ChatError {
    use matrix_sdk::HttpError;

    log::error!("Received HttpError: {err:?}");

    if let Some(err) = err.as_client_api_error() {
        return convert_client_api_error(err);
    }

    match err {
        HttpError::IntoHttp(err) => convert_into_http_error(err),
        _ => chat_err!(Network, err),
    }
}

fn convert_client_api_error(err: &ruma_common::api::error::Error) -> ChatError {
    use ruma_common::api::error::ErrorKind as Kind;

    log::error!("Received RumaClientError: {err:?}");

    let Some(error_kind) = err.error_kind() else {
        return chat_err!(Network, err);
    };

    match *error_kind {
        Kind::MissingToken | Kind::Unauthorized | Kind::UnknownToken { .. } => {
            chat_err!(Authorization)
        }
        Kind::Forbidden => chat_err!(NotAllowed),
        Kind::TooLarge => chat_err!(MessageSizeLimitExceeded),
        _ => chat_err!(Network, err),
    }
}

fn convert_matrix_sdk_media_error(err: matrix_sdk::media::MediaError) -> ChatError {
    use matrix_sdk::media::MediaError;

    match err {
        MediaError::MediaTooLargeToUpload { .. } => chat_err!(UploadSizeLimitExceeded),
        _ => chat_err!(Unknown, err),
    }
}

pub fn convert_matrix_sdk_error(err: matrix_sdk::Error) -> ChatError {
    log::error!("Received matrix sdk Error: {err:?}");

    match err {
        matrix_sdk::Error::Http(err) => convert_http_error(*err),
        matrix_sdk::Error::AuthenticationRequired => chat_err!(Authorization),
        matrix_sdk::Error::Url(err) => chat_err!(InvalidUrl, err),
        matrix_sdk::Error::Media(err) => convert_matrix_sdk_media_error(err),
        _ => chat_err!(Unknown, err),
    }
}

fn convert_client_build_error(err: Box<matrix_sdk::ClientBuildError>) -> ChatError {
    use matrix_sdk::ClientBuildError;

    log::error!("Received ClientBuildError: {err:?}");

    match *err {
        ClientBuildError::Http(err) => convert_http_error(err),
        ClientBuildError::AutoDiscovery(err) => chat_err!(Network, err),
        _ => chat_err!(Unknown, err),
    }
}

fn convert_crypto_store_error(err: matrix_sdk_crypto::CryptoStoreError) -> ChatError {
    log::error!("Received CryptoStoreError: {err:?}");
    chat_err!(Unknown, format!("CryptoStoreError: {err}"))
}

fn convert_request_verification_error(
    err: matrix_sdk::encryption::identities::RequestVerificationError,
) -> ChatError {
    use matrix_sdk::encryption::identities::RequestVerificationError;

    log::error!("Received RequestVerificationError: {err:?}");

    match err {
        RequestVerificationError::Sdk(err) => convert_matrix_sdk_error(err),
        err => chat_err!(Unknown, err),
    }
}

fn convert_secret_storage_error(
    err: matrix_sdk::encryption::secret_storage::SecretStorageError,
) -> ChatError {
    use matrix_sdk::encryption::secret_storage::SecretStorageError;

    log::error!("Received SecretStorageError: {err:?}");

    match err {
        SecretStorageError::SecretStorageKey(err) => chat_err!(InvalidRecoveryKey, err),
        err => chat_err!(Unknown, err),
    }
}

fn convert_recovery_error(err: matrix_sdk::encryption::recovery::RecoveryError) -> ChatError {
    use matrix_sdk::encryption::recovery::RecoveryError;

    log::error!("Received RecoveryError: {err:?}");

    match err {
        RecoveryError::BackupExistsOnServer => chat_err!(Unknown, err),
        RecoveryError::Sdk(err) => convert_matrix_sdk_error(err),
        RecoveryError::SecretStorage(err) => convert_secret_storage_error(err),
    }
}

fn convert_refresh_token_error(err: matrix_sdk::RefreshTokenError) -> ChatError {
    log::error!("Received RefreshTokenError: {err:?}");
    chat_err!(Authorization, err)
}

fn convert_store_error(err: matrix_sdk::StoreError) -> ChatError {
    log::error!("Received StoreError: {err:?}");
    chat_err!(Unknown, err)
}

fn convert_edit_error(err: matrix_sdk::room::edit::EditError) -> ChatError {
    log::error!("Received EditError: {err:?}");
    chat_err!(Unknown, err)
}
