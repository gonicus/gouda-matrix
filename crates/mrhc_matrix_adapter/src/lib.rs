#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod client;

pub use client::MatrixClient;
