mod client;
mod executor;
mod input_processor;
mod output_processor;
mod runner;

#[cfg(any(test, feature = "test-util"))]
pub mod test_utils;

pub use client::{Client, ClientContext};
pub use runner::Runner;

pub type Result<T> = std::result::Result<T, mrhc_proto::chat::Error>;
