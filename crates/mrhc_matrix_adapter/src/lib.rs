#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod client;
mod errors;
mod events;
mod login;
mod rooms;
mod utils;

pub use client::MatrixClient;
