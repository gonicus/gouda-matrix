use std::collections::VecDeque;
use std::io::Read;
use std::sync::mpsc::{self, Receiver, Sender};

use gouda_proto::chat::{RequestContainer, ResponseContainer, response_container};
use interprocess::local_socket::RecvHalf;
use prost::Message;

const MAX_LOGS: usize = 1000;
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
    logs: VecDeque<OutputLog>,

    hide_user_change_events: bool,
    hide_room_change_events: bool,
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

        let obj = Self {
            receiver: rx,
            logs: VecDeque::new(),
            hide_user_change_events: true,
            hide_room_change_events: true,
        };

        (obj, tx)
    }

    pub fn update(&mut self, ctx: &egui::Context) {
        egui::Window::new("Communication")
            .resizable(true)
            .default_size(egui::Vec2::new(500.0, 500.0))
            .default_pos(egui::Pos2::new(700.0, 20.0))
            .show(ctx, |ui| {
                self.check_for_actions();

                ui.checkbox(&mut self.hide_room_change_events, "Hide RoomChangeEvents");
                ui.checkbox(&mut self.hide_user_change_events, "Hide UserChangeEvents");

                ui.separator();

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
            Ok(log) => self.add_log(log),
            Err(err) => {
                if matches!(err, mpsc::TryRecvError::Disconnected) {
                    panic!("Response receiver disconnected");
                }
            }
        }
    }

    fn add_log(&mut self, log: OutputLog) {
        if self.logs.len() >= MAX_LOGS {
            self.logs.pop_front();
        }

        self.logs.push_back(log);
    }

    fn display_logs(&self, ui: &mut egui::Ui) {
        for log in &self.logs {
            let rendered = match log {
                OutputLog::Request(re) => self.display_request(ui, re),
                OutputLog::Response(re) => self.display_response(ui, re),
            };

            if rendered {
                ui.add_space(LOG_SPACING);
            }
        }
    }

    fn display_request(&self, ui: &mut egui::Ui, request: &RequestContainer) -> bool {
        let str = format!("{request:#?}");
        ui.colored_label(REQUEST_COLOR, str);
        true
    }

    fn display_response(&self, ui: &mut egui::Ui, response: &ResponseContainer) -> bool {
        if self.is_ignored(response) {
            return false;
        }

        let str = format!("{response:#?}");
        let mut color = RESPONSE_COLOR;

        if let Some(content) = &response.content
            && matches!(content, response_container::Content::Error(_))
        {
            color = ERROR_COLOR;
        }

        ui.colored_label(color, str);

        true
    }

    fn is_ignored(&self, response: &ResponseContainer) -> bool {
        use response_container::Content;

        if self.hide_room_change_events
            && matches!(response.content, Some(Content::RoomChangeEvent(_)))
        {
            return true;
        }

        if self.hide_user_change_events
            && matches!(response.content, Some(Content::UserChangeEvent(_)))
        {
            return true;
        }

        false
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
