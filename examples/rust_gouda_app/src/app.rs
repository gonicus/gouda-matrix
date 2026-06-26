use std::io::{Read, Write};
use std::sync;
use std::sync::mpsc::{Receiver, Sender};

use gouda_proto::chat::{RequestContainer, ResponseContainer};
use interprocess::local_socket::{RecvHalf, SendHalf};
use prost::Message;

use crate::communication::CommunicationWindow;
use crate::config::Config;
use crate::context::Context;
use crate::input::InputWindow;

pub struct App {
    context: Context,
    input_window: InputWindow,
    output_window: CommunicationWindow,
}

impl App {
    pub fn new(
        _cc: &eframe::CreationContext<'_>,
        config: Config,
        recv: RecvHalf,
        send: SendHalf,
    ) -> Self {
        let (request_sender, request_receiver) = sync::mpsc::channel();
        let (response_sender, response_receiver) = sync::mpsc::channel();

        let context = Context::new(config, request_sender, response_receiver);

        run_response_reader(recv, response_sender);
        run_request_writer(send, request_receiver);

        let output_window = CommunicationWindow::new();
        let input_window = InputWindow::new(&context);

        Self {
            context,
            input_window,
            output_window,
        }
    }
}

impl eframe::App for App {
    fn update(&mut self, egui_ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.context.exec_io();

        self.input_window.show(egui_ctx, &mut self.context);
        self.output_window.update(egui_ctx, &self.context);

        egui_ctx.request_repaint();
    }
}

fn run_response_reader(mut reader: RecvHalf, sender: Sender<ResponseContainer>) {
    std::thread::spawn(move || {
        loop {
            let size = read_size(&mut reader);
            let response = read_response(&mut reader, size);
            sender.send(response).unwrap();
        }
    });
}

fn read_size(reader: &mut RecvHalf) -> u64 {
    let mut buf = [0; 8];
    reader.read_exact(&mut buf).expect("Error reading size");
    u64::from_le_bytes(buf)
}

fn read_response(reader: &mut RecvHalf, len: u64) -> ResponseContainer {
    let mut buf = vec![0; len as usize];

    reader
        .read_exact(&mut buf)
        .expect("error reading buffer of size {len}");

    ResponseContainer::decode(&mut std::io::Cursor::new(&buf as &[u8]))
        .expect("error decoding ResponseContainer")
}

fn run_request_writer(mut writer: SendHalf, recv: Receiver<RequestContainer>) {
    std::thread::spawn(move || {
        loop {
            let request = recv.recv().unwrap();
            send_request(&mut writer, request);
        }
    });
}

fn send_request(sender: &mut SendHalf, request: RequestContainer) {
    let mut encoded = request.encode_to_vec();
    let mut data = encoded.len().to_le_bytes().to_vec();

    data.append(&mut encoded);

    sender
        .write_all(&data)
        .expect("Error writing RequestContainer to sender");
}
