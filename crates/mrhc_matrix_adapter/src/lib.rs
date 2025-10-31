#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod client;
mod events;
mod login;
mod rooms;

pub use client::MatrixClient;
