use std::io::Read;
use std::sync::mpsc::{self, Receiver, Sender};

use interprocess::local_socket::RecvHalf;
use mrhc_proto::chat::{RequestContainer, ResponseContainer, response_container};
use prost::Message;

const LOG_SPACING: f32 = 10.0;
const REQUEST_COLOR: egui::Color32 = egui::Color32::LIGHT_BLUE;
const RESPONSE_COLOR: egui::Color32 = egui::Color32::GREEN;
const ERROR_COLOR: egui::Color32 = egui::Color32::RED;

pub enum OutputLog {
    Request(RequestContainer),
    Response(ResponseContainer),
}

pub struct OutputWindow {
    receiver: Receiver<OutputLog>,
    logs: Vec<OutputLog>,
}

impl OutputWindow {
    pub fn new(mut recv_half: RecvHalf) -> (Self, Sender<OutputLog>) {
        let (tx, rx) = mpsc::channel();
        let tx2 = tx.clone();

        std::thread::spawn(move || {
            loop {
                let size = read_size(&mut recv_half);
                let request = read_request(&mut recv_half, size);
                tx2.send(OutputLog::Response(request))
                    .expect("Error sending request to UI");
            }
        });

        (
            Self {
                receiver: rx,
                logs: Vec::new(),
            },
            tx,
        )
    }

    pub fn update(&mut self, ctx: &egui::Context) {
        egui::Window::new("Communication")
            .resizable(true)
            .default_size(egui::Vec2::new(500.0, 500.0))
            .default_pos(egui::Pos2::new(700.0, 20.0))
            .show(ctx, |ui| {
                self.check_for_actions();

                egui::containers::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .stick_to_bottom(true)
                    .show(ui, |ui| {
                        self.display_logs(ui);
                    });
            });
    }

    fn check_for_actions(&mut self) {
        match self.receiver.try_recv() {
            Ok(action) => self.logs.push(action),
            Err(err) => {
                if matches!(err, mpsc::TryRecvError::Disconnected) {
                    panic!("Response receiver disconnected");
                }
            }
        }
    }

    fn display_logs(&self, ui: &mut egui::Ui) {
        for log in &self.logs {
            match log {
                OutputLog::Request(re) => self.display_request(ui, re),
                OutputLog::Response(re) => self.display_response(ui, re),
            }

            ui.add_space(LOG_SPACING);
        }
    }

    fn display_request(&self, ui: &mut egui::Ui, request: &RequestContainer) {
        let str = format!("{request:#?}");
        ui.colored_label(REQUEST_COLOR, str);
    }

    fn display_response(&self, ui: &mut egui::Ui, response: &ResponseContainer) {
        let str = format!("{response:#?}");
        let mut color = RESPONSE_COLOR;

        if let Some(content) = &response.content
            && matches!(content, response_container::Content::Error(_)) {
                color = ERROR_COLOR;
            }

        ui.colored_label(color, str);
    }
}

fn read_size(reader: &mut RecvHalf) -> u64 {
    let mut buf = [0; 8];
    reader.read_exact(&mut buf).expect("Error reading size");

    u64::from_le_bytes(buf)
}

fn read_request(reader: &mut RecvHalf, len: u64) -> ResponseContainer {
    let mut buf = vec![0; len as usize];
    reader
        .read_exact(&mut buf)
        .expect("error reading buffer of size {len}");

    ResponseContainer::decode(&mut std::io::Cursor::new(&buf as &[u8]))
        .expect("error decoding RequestContainer")
}
