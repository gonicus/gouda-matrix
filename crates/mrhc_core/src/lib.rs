mod async_app;
mod client;
mod executor;
mod input_processor;
mod output_processor;

#[cfg(any(test, feature = "test-util"))]
pub mod test_utils;

pub use async_app::AsyncApp;
pub use client::{Client, ClientContext};

pub type Result<T> = std::result::Result<T, mrhc_proto::chat::Error>;
