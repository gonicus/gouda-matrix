use std::collections::VecDeque;

use gouda_proto::chat::{RequestContainer, ResponseContainer, response_container};

use crate::context::Context;

const MAX_LOGS: usize = 1000;
const LOG_SPACING: f32 = 10.0;
const REQUEST_COLOR: egui::Color32 = egui::Color32::LIGHT_BLUE;
const RESPONSE_COLOR: egui::Color32 = egui::Color32::GREEN;
const ERROR_COLOR: egui::Color32 = egui::Color32::RED;

pub enum CommunicationLog {
    Request(RequestContainer),
    Response(ResponseContainer),
}

pub struct CommunicationWindow {
    logs: VecDeque<CommunicationLog>,
    hide_user_change_events: bool,
    hide_room_change_events: bool,
}

impl CommunicationWindow {
    pub fn new() -> Self {
        Self {
            logs: VecDeque::new(),
            hide_user_change_events: true,
            hide_room_change_events: true,
        }
    }

    pub fn show(&mut self, ui: &mut egui::Ui, ctx: &Context) {
        self.check_for_io(ctx);

        egui::Window::new("Communication")
            .resizable(true)
            .default_size(egui::Vec2::new(500.0, 500.0))
            .default_pos(egui::Pos2::new(700.0, 20.0))
            .show(ui, |ui| {
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

    fn check_for_io(&mut self, ctx: &Context) {
        for response in ctx.received_responses() {
            self.add_log(CommunicationLog::Response(response.clone()));
        }

        for request in ctx.send_requests() {
            self.add_log(CommunicationLog::Request(request.clone()));
        }
    }

    fn add_log(&mut self, log: CommunicationLog) {
        if self.logs.len() >= MAX_LOGS {
            self.logs.pop_front();
        }

        self.logs.push_back(log);
    }

    fn display_logs(&self, ui: &mut egui::Ui) {
        for log in &self.logs {
            let rendered = match log {
                CommunicationLog::Request(re) => self.display_request(ui, re),
                CommunicationLog::Response(re) => self.display_response(ui, re),
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
