#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod client;
mod crypto;
mod errors;
mod events;
mod macros;
mod media;
mod rooms;
mod sas;
mod session;
#[cfg(test)]
mod test_utils;
mod utils;
mod verification;

pub use client::MatrixClient;
