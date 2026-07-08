//! Implements an async abstraction for the gouda-api and provides
//! a [`Client`] trait for chat implementation.
#![warn(missing_docs)]

mod client;
mod context;
mod error;
mod executor;
mod input;
mod multipart_response;
mod output;
mod runner;

#[cfg(any(test, feature = "test-util"))]
pub mod test_utils;

pub use client::Client;
pub use context::RequestContext;
pub use executor::ExecutorTask;
pub use multipart_response::MultipartResponse;
pub use output::OutputTask;
pub use runner::Runner;

/// A chat result.
pub type Result<T> = std::result::Result<T, gouda_proto::chat::Error>;
/// A runner result.
pub use error::{RunnerError, RunnerResult};
