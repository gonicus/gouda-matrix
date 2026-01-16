use matrix_sdk::encryption::identities::RequestVerificationError;
use matrix_sdk::encryption::recovery::RecoveryError;
use matrix_sdk::encryption::secret_storage::SecretStorageError;
use matrix_sdk::ruma::api::client::error::ErrorKind as RumaClientErrorKind;
use matrix_sdk::ruma::api::client::Error as RumaClientError;
use matrix_sdk::{ClientBuildError, HttpError, IdParseError, StoreError};
use matrix_sdk_crypto::CryptoStoreError;
use mrhc_proto::chat::error::ErrorType;
use mrhc_proto::chat::Error;

/// Creates a new chat error given an error type as well as an error message.
pub fn create_error_msg<M: std::fmt::Display>(ty: ErrorType, msg: M) -> Error {
    Error {
        r#type: ty.into(),
        error_string: Some(msg.to_string()),
    }
}

/// Creates a new chat error without a message.
pub fn create_error(ty: ErrorType) -> Error {
    Error {
        r#type: ty.into(),
        error_string: None,
    }
}

/// Creates a new unknown error.
pub fn create_unknown<M: std::fmt::Display>(msg: M) -> Error {
    create_error_msg(ErrorType::Unknown, msg)
}

/// Converts a `matrix_sdk::HttpError` to a new chat error.
pub fn convert_http_error(err: HttpError) -> Error {
    if let Some(err) = err.as_client_api_error() {
        convert_client_api_error(err)
    } else {
        create_error_msg(ErrorType::Network, err)
    }
}

/// Converts a `ruma::api::client::Error` to a new chat error.
pub fn convert_client_api_error(err: &RumaClientError) -> Error {
    let Some(error_kind) = err.error_kind() else {
        return create_error_msg(ErrorType::Network, err);
    };

    match *error_kind {
        RumaClientErrorKind::MissingToken
        | RumaClientErrorKind::Unauthorized
        | RumaClientErrorKind::UnknownToken { .. } => {
            create_error_msg(ErrorType::Authorization, "Authentication required")
        }
        _ => create_error_msg(ErrorType::Network, err),
    }
}

/// Converts a `matrix_sdk::Error` to a new chat error.
pub fn convert_matrix_sdk_error(err: matrix_sdk::Error) -> Error {
    log::error!("Received matrix sdk error: {err:?}");

    match err {
        matrix_sdk::Error::Http(err) => convert_http_error(*err),
        matrix_sdk::Error::AuthenticationRequired => {
            create_error_msg(ErrorType::Authorization, "Authentication required")
        }
        matrix_sdk::Error::Url(err) => create_error_msg(ErrorType::InvalidUrl, err),
        _ => create_error_msg(ErrorType::Unknown, err),
    }
}

/// Converts a `matrix_sdk_base::Error` to a new chat error.
// We may need this function again in the future.
#[allow(dead_code)]
pub fn convert_matrix_sdk_base_error(err: matrix_sdk_base::Error) -> Error {
    log::error!("Received matrix sdk base error: {err:?}");

    match err {
        matrix_sdk_base::Error::CryptoStore(err) => convert_crypto_store_error(err),
        matrix_sdk_base::Error::StateStore(err) => convert_store_error(err),
        _ => create_unknown(err),
    }
}

/// Converts a `ClientBuildError` to a new chat error.
pub fn convert_client_build_error(err: ClientBuildError) -> Error {
    log::error!("Received client build error: {err:?}");

    match err {
        ClientBuildError::Http(err) => convert_http_error(err),
        ClientBuildError::AutoDiscovery(err) => create_error_msg(ErrorType::Network, err),
        _ => create_error_msg(ErrorType::Unknown, err),
    }
}

/// Converts a `CryptoStoreError` to a new chat error.
pub fn convert_crypto_store_error(err: CryptoStoreError) -> Error {
    log::error!("Received crypto store error: {err:?}");
    create_unknown(format!("CryptoStoreError: {err}"))
}

/// Converts a `RequestVerificationError` to a new chat error.
pub fn convert_request_verification_error(err: RequestVerificationError) -> Error {
    log::error!("Received request verification error error: {err:?}");

    match err {
        RequestVerificationError::Sdk(err) => convert_matrix_sdk_error(err),
        err => create_unknown(err),
    }
}

/// Converts a `SecretStorageError` to a new chat error.
pub fn convert_secret_storage_error(err: SecretStorageError) -> Error {
    log::error!("Received secret storage error: {err:?}");

    match err {
        SecretStorageError::SecretStorageKey(err) => {
            create_error_msg(ErrorType::InvalidRecoveryKey, err)
        }
        err => create_unknown(err),
    }
}

/// Converts a `RecoveryError` to a new chat error.
pub fn convert_recovery_error(err: RecoveryError) -> Error {
    log::error!("Received recovery error: {err:?}");

    match err {
        RecoveryError::BackupExistsOnServer => create_unknown(err),
        RecoveryError::Sdk(err) => convert_matrix_sdk_error(err),
        RecoveryError::SecretStorage(err) => convert_secret_storage_error(err),
    }
}

/// Converts a `StoreError` to a new chat error.
pub fn convert_store_error(err: StoreError) -> Error {
    create_unknown(err)
}

/// Converts a `IdParseError` to a new chat error.
pub fn convert_id_parse_error(err: IdParseError) -> Error {
    create_error_msg(ErrorType::InvalidUserId, err.to_string())
}
