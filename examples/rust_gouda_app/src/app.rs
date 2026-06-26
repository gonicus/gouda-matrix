use interprocess::local_socket::{RecvHalf, SendHalf};

use crate::communication::OutputWindow;
use crate::config::Config;
use crate::input::InputWindow;

pub struct App {
    input_window: InputWindow,
    output_window: OutputWindow,
}

impl App {
    pub fn new(
        _cc: &eframe::CreationContext<'_>,
        cfg: Config,
        recv: RecvHalf,
        send: SendHalf,
    ) -> Self {
        let (output_window, output_sender) = OutputWindow::new(recv);
        let input_window = InputWindow::new(cfg, send, output_sender);

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
