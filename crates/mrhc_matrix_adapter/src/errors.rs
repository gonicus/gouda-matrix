use matrix_sdk::ClientBuildError;

use mrhc_proto::chat::error::ErrorType;
use mrhc_proto::chat::Error;

/// Creates a new chat error given an error type as well as an error message.
pub fn create_error_msg<M: std::fmt::Display>(ty: ErrorType, msg: M) -> Error {
    Error {
        r#type: ty as i32,
        error_string: Some(msg.to_string()),
    }
}

/// Creates a new chat error without a message.
pub fn create_error(ty: ErrorType) -> Error {
    Error {
        r#type: ty as i32,
        error_string: None,
    }
}

pub fn create_unknown<M: std::fmt::Display>(msg: M) -> Error {
    create_error_msg(ErrorType::Unknown, msg)
}

/// Converts a `matrix_sdk::Error` to a new chat error.
pub fn convert_matrix_sdk_error(err: matrix_sdk::Error) -> Error {
    match err {
        matrix_sdk::Error::Http(err) => create_error_msg(ErrorType::Network, err),
        matrix_sdk::Error::AuthenticationRequired => {
            create_error_msg(ErrorType::Authorization, "Authentication required")
        }
        matrix_sdk::Error::Url(err) => create_error_msg(ErrorType::InvalidUrl, err),
        _ => create_error_msg(ErrorType::Unknown, err),
    }
}

/// Converts a `ClientBuildError` to a new chat error.
pub fn convert_client_build_error(err: ClientBuildError) -> Error {
    match err {
        ClientBuildError::Http(err) => create_error_msg(ErrorType::Network, err),
        ClientBuildError::AutoDiscovery(err) => create_error_msg(ErrorType::Network, err),
        _ => create_error_msg(ErrorType::Unknown, err),
    }
}
