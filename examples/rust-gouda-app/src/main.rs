mod actions;
mod communication;
mod config;
mod input;
mod ui;

use interprocess::local_socket::prelude::*;
use interprocess::local_socket::{GenericFilePath, Listener, ListenerOptions, RecvHalf, SendHalf};

use crate::communication::OutputWindow;
use crate::config::Config;
use crate::input::InputWindow;

fn main() {
    let native_options = eframe::NativeOptions::default();

    let (recv, send) = setup_conn();

    eframe::run_native(
        "Rust Matrix Client",
        native_options,
        Box::new(|cc| Ok(Box::new(App::new(cc, recv, send)))),
    )
    .expect("Error setting up graphics context");
}

struct App {
    input_window: InputWindow,
    output_window: OutputWindow,
}

impl App {
    fn new(_cc: &eframe::CreationContext<'_>, recv: RecvHalf, send: SendHalf) -> Self {
        let config = Config::read_from_file("config.json");

        let (output_window, output_sender) = OutputWindow::new(recv);
        let input_window = InputWindow::new(config, send, output_sender);

        Self {
            input_window,
            output_window,
        }
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.input_window.update(ctx);
        self.output_window.update(ctx);

        ctx.request_repaint();
    }
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
