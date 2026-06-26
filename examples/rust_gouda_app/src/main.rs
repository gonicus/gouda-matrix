mod actions;
mod app;
mod communication;
mod config;
mod context;
mod input;
mod messages;
mod ui;

use clap::Parser;
use interprocess::local_socket::prelude::*;
use interprocess::local_socket::{GenericFilePath, Listener, ListenerOptions, RecvHalf, SendHalf};

use crate::app::App;
use crate::config::Config;

const fn config_default_path() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/config.json")
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(help = "Path to the socket for sending requests")]
    pub request_socket: String,

    #[arg(help = "Path to the socket for receiving responses")]
    pub response_socket: String,

    #[arg(long, default_value = config_default_path(), help="Path to the config file")]
    pub config: String,
}

fn main() {
    let args = Args::parse();
    let native_options = eframe::NativeOptions::default();

    let cfg = Config::read_from_file(&args.config);
    let (recv, send) = setup_conn();

    eframe::run_native(
        "Rust GOuda App",
        native_options,
        Box::new(|cc| Ok(Box::new(App::new(cc, cfg, recv, send)))),
    )
    .expect("Error setting up graphics context");
}

fn start_server(socket: &str) -> Listener {
    println!("Starting server at: '{socket}'");

    let socket_name = socket
        .to_fs_name::<GenericFilePath>()
        .expect("Invalid socket name: '{socket_name}'");

    let opts = ListenerOptions::new().name(socket_name);

    match opts.create_sync() {
        Ok(listener) => listener,
        Err(err) => panic!("Error starting server '{socket}': {err}"),
    }
}

fn setup_conn() -> (RecvHalf, SendHalf) {
    let request_socket = std::env::args()
        .nth(1)
        .expect("No request socket specified");

    let response_socket = std::env::args()
        .nth(2)
        .expect("No response socket specified");

    println!("Request socket: '{request_socket}'");
    println!("Response socket: '{response_socket}'");

    let request_server = start_server(&request_socket);
    let response_server = start_server(&response_socket);

    println!("Waiting for connection at: '{request_socket}'");

    let (_, send) = request_server
        .accept()
        .expect("Error waiting for connection on request server")
        .split();

    println!("Waiting for connection at: '{response_socket}'");

    let (recv, _) = response_server
        .accept()
        .expect("Error waiting for connection on response server")
        .split();

    (recv, send)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        Config::read_from_file(config_default_path());
    }
}
