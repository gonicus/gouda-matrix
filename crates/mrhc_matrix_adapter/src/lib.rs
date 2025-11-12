#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod client;
mod crypto;
mod errors;
mod events;
mod rooms;
mod session;
mod utils;

pub use client::MatrixClient;
