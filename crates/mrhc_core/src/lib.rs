mod async_app;
mod client;
mod executor;
mod input_processor;
mod output_processor;

#[cfg(test)]
pub mod test_utils;

pub use async_app::AsyncApp;
pub use client::Client;

pub type Result<T> = std::result::Result<T, mrhc_proto::chat::Error>;
