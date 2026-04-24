#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod client;
mod crypto;
mod errors;
mod events;
mod macros;
mod media;
mod memory_cache;
mod messages;
mod proto_cache;
mod rooms;
mod sas;
mod session;
#[cfg(test)]
mod test_utils;
mod user;
mod utils;
mod verification;

pub use client::MatrixClient;
