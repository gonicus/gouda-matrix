use gouda_proto::chat::error::ErrorType;
use gouda_proto::chat::Error;
use matrix_sdk::encryption::identities::RequestVerificationError;
use matrix_sdk::encryption::recovery::RecoveryError;
use matrix_sdk::encryption::secret_storage::SecretStorageError;
use matrix_sdk::room::edit::EditError;
use matrix_sdk::{ClientBuildError, HttpError, IdParseError, RefreshTokenError, StoreError};
use matrix_sdk_crypto::CryptoStoreError;
use ruma_common::api::error::{
    Error as RumaClientError, ErrorKind as RumaClientErrorKind, IntoHttpError,
};

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

/// Convert an IntoHttpError to a new chat error.
pub fn convert_into_http_error(err: IntoHttpError) -> Error {
    log::error!("Received IntoHttpError: {err:?}");

    match err {
        IntoHttpError::Authentication(_) => {
            create_error_msg(ErrorType::Authorization, "Authentication required")
        }
        _ => create_error_msg(ErrorType::Network, err.to_string()),
    }
}

/// Converts a `matrix_sdk::HttpError` to a new chat error.
pub fn convert_http_error(err: HttpError) -> Error {
    log::error!("Received HttpError: {err:?}");

    if let Some(err) = err.as_client_api_error() {
        return convert_client_api_error(err);
    }

    match err {
        HttpError::IntoHttp(err) => convert_into_http_error(err),
        _ => create_error_msg(ErrorType::Network, err.to_string()),
    }
}

/// Converts a `ruma::api::client::Error` to a new chat error.
pub fn convert_client_api_error(err: &RumaClientError) -> Error {
    log::error!("Received RumaClientError: {err:?}");

    let Some(error_kind) = err.error_kind() else {
        return create_error_msg(ErrorType::Network, err);
    };

    match *error_kind {
        RumaClientErrorKind::MissingToken
        | RumaClientErrorKind::Unauthorized
        | RumaClientErrorKind::UnknownToken { .. } => {
            create_error_msg(ErrorType::Authorization, "Authentication required")
        }
        RumaClientErrorKind::Forbidden => {
            create_error_msg(ErrorType::NotAllowed, "Insufficient permissions")
        }
        _ => create_error_msg(ErrorType::Network, err),
    }
}

/// Converts a `matrix_sdk::Error` to a new chat error.
pub fn convert_matrix_sdk_error(err: matrix_sdk::Error) -> Error {
    log::error!("Received matrix sdk Error: {err:?}");

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
    log::error!("Received matrix sdk base Error: {err:?}");

    match err {
        matrix_sdk_base::Error::CryptoStore(err) => convert_crypto_store_error(err),
        matrix_sdk_base::Error::StateStore(err) => convert_store_error(err),
        _ => create_unknown(err),
    }
}

/// Converts a `ClientBuildError` to a new chat error.
pub fn convert_client_build_error(err: ClientBuildError) -> Error {
    log::error!("Received ClientBuildError: {err:?}");

    match err {
        ClientBuildError::Http(err) => convert_http_error(err),
        ClientBuildError::AutoDiscovery(err) => create_error_msg(ErrorType::Network, err),
        _ => create_error_msg(ErrorType::Unknown, err),
    }
}

/// Converts a `CryptoStoreError` to a new chat error.
pub fn convert_crypto_store_error(err: CryptoStoreError) -> Error {
    log::error!("Received CryptoStoreError: {err:?}");
    create_unknown(format!("CryptoStoreError: {err}"))
}

/// Converts a `RequestVerificationError` to a new chat error.
pub fn convert_request_verification_error(err: RequestVerificationError) -> Error {
    log::error!("Received RequestVerificationError: {err:?}");

    match err {
        RequestVerificationError::Sdk(err) => convert_matrix_sdk_error(err),
        err => create_unknown(err),
    }
}

/// Converts a `SecretStorageError` to a new chat error.
pub fn convert_secret_storage_error(err: SecretStorageError) -> Error {
    log::error!("Received SecretStorageError: {err:?}");

    match err {
        SecretStorageError::SecretStorageKey(err) => {
            create_error_msg(ErrorType::InvalidRecoveryKey, err)
        }
        err => create_unknown(err),
    }
}

/// Converts a `RecoveryError` to a new chat error.
pub fn convert_recovery_error(err: RecoveryError) -> Error {
    log::error!("Received RecoveryError: {err:?}");

    match err {
        RecoveryError::BackupExistsOnServer => create_unknown(err),
        RecoveryError::Sdk(err) => convert_matrix_sdk_error(err),
        RecoveryError::SecretStorage(err) => convert_secret_storage_error(err),
    }
}

pub fn convert_refresh_token_error(err: RefreshTokenError) -> Error {
    log::error!("Received RefreshTokenError: {err:?}");
    create_error_msg(ErrorType::Authorization, err)
}

/// Converts a `StoreError` to a new chat error.
pub fn convert_store_error(err: StoreError) -> Error {
    log::error!("Received StoreError: {err:?}");
    create_unknown(err)
}

/// Converts a `IdParseError` to a new chat error.
pub fn convert_id_parse_error(err: IdParseError) -> Error {
    create_error_msg(ErrorType::InvalidUserId, err.to_string())
}

pub fn convert_edit_error(err: EditError) -> Error {
    log::error!("Received EditError: {err:?}");
    create_unknown(err)
}
