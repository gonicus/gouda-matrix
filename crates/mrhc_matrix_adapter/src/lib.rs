#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod client;
mod crypto;
mod errors;
mod events;
mod rooms;
mod sas;
mod session;
mod utils;
mod verification;

pub use client::MatrixClient;
