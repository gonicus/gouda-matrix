mod async_app;
mod client;
mod executor;
mod input_processor;
mod output_processor;

#[cfg(test)]
pub mod test_utils;

pub use async_app::AsyncApp;
pub use client::Client;

use mrhc_proto::chat::error::ErrorType;
use mrhc_proto::chat::Error;

pub type Result<T> = std::result::Result<T, mrhc_proto::chat::Error>;

pub fn create_error(ty: ErrorType) -> Error {
    Error {
        r#type: ty as i32,
        error_string: None,
    }
}

pub fn create_error_msg(ty: ErrorType, msg: String) -> Error {
    Error {
        r#type: ty as i32,
        error_string: Some(msg),
    }
}
