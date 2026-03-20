mod client;
mod context;
mod executor;
mod input_processor;
mod multipart_response;
mod output_processor;
mod runner;

#[cfg(any(test, feature = "test-util"))]
pub mod test_utils;

pub use client::Client;
pub use context::ClientContext;
pub use multipart_response::MultipartResponse;
pub use output_processor::OutputTask;
pub use runner::Runner;

pub type Result<T> = std::result::Result<T, mrhc_proto::chat::Error>;
