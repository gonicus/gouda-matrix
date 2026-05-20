//! This module contains utilities used for testing.
//! The module can only be imported if the importing module is
//! annotated with #[cfg(test)]

mod client_mock;
mod writer_mock;

pub use client_mock::ClientMock;
pub use writer_mock::WriterMock;
