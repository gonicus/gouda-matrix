mod runner;
mod client;
mod executor;
mod input_processor;
mod output_processor;

#[cfg(any(test, feature = "test-util"))]
pub mod test_utils;

pub use runner::Runner;
pub use client::{Client, ClientContext};

pub type Result<T> = std::result::Result<T, mrhc_proto::chat::Error>;
