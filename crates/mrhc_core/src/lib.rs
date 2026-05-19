mod client;
mod context;
mod executor;
mod input;
mod multipart_response;
mod output;
mod runner;

#[cfg(any(test, feature = "test-util"))]
pub mod test_utils;

pub use client::Client;
pub use context::ClientContext;
pub use executor::ExecutorTask;
pub use multipart_response::MultipartResponse;
pub use output::OutputTask;
pub use runner::Runner;

pub type Result<T> = std::result::Result<T, mrhc_proto::chat::Error>;
