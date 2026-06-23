#![recursion_limit = "256"]

mod client;
mod crypto;
mod error;
mod errors;
mod events;
mod macros;
mod media;
mod memory_cache;
mod messages;
mod notifications;
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
