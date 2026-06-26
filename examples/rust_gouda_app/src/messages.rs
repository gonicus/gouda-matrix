use std::collections::HashMap;

use gouda_proto::chat::response_container::Content as ResponseContent;
use gouda_proto::chat::{Message, MessageChangeEvent, MessageRemoveEvent, message};

use crate::context::Context;

#[derive(Debug, Default)]
struct Room {
    messages: HashMap<String, Message>,
}

pub struct MessagesWindow {
    rooms: HashMap<String, Room>,
    selected_room: Option<String>,
}

impl MessagesWindow {
    pub fn new() -> Self {
        Self {
            rooms: HashMap::new(),
            selected_room: None,
        }
    }

    pub fn show(&mut self, egui_ctx: &egui::Context, context: &Context) {
        self.collect_responses(context);

        egui::Window::new("Messages")
            .resizable(true)
            .default_size(egui::Vec2::new(500.0, 500.0))
            .show(egui_ctx, |ui| {
                self.ui(ui);
            });
    }

    fn get_or_create_room(&mut self, room_id: impl Into<String>) -> &mut Room {
        self.rooms.entry(room_id.into()).or_default()
    }

    fn get_selected_room(&self) -> Option<&Room> {
        self.rooms.get(self.selected_room.as_ref()?)
    }

    fn collect_responses(&mut self, context: &Context) {
        for response in context.received_responses() {
            let Some(content) = &response.content else {
                eprintln!("Receiver response with empty content: {response:?}");
                continue;
            };

            self.collect_response_content(&content);
        }
    }

    fn collect_response_content(&mut self, content: &ResponseContent) {
        match content {
            ResponseContent::MessageReceivedEvent(message) => self.collect_message(message.clone()),
            ResponseContent::MessageRemoveEvent(event) => self.remove_message(event.clone()),
            ResponseContent::MessageChangeEvent(event) => self.change_message(event.clone()),
            _ => (),
        }
    }

    fn collect_message(&mut self, message: Message) {
        let room = self.get_or_create_room(&message.room_id);
        room.messages.insert(message.message_id.clone(), message);
    }

    fn remove_message(&mut self, event: MessageRemoveEvent) {
        let room = self.get_or_create_room(&event.room_id);
        room.messages.remove(&event.message_id);
    }

    fn change_message(&mut self, event: MessageChangeEvent) {
        let room = self.get_or_create_room(&event.room_id);
        let Some(message) = room.messages.get_mut(&event.message_id) else {
            eprint!("Received a message change event of a message we don't know");
            return;
        };
        event.update_into_message(message);
    }

    fn ui(&mut self, ui: &mut egui::Ui) {
        self.ui_room_selection(ui);
        self.ui_messages(ui);
    }

    fn ui_room_selection(&mut self, ui: &mut egui::Ui) {
        egui::ComboBox::from_label("Room")
            .selected_text(format!("{:?}", self.selected_room))
            .show_ui(ui, |ui| {
                for (room_id, _) in &self.rooms {
                    let selected = Some(room_id) == self.selected_room.as_ref();
                    if ui.selectable_label(selected, room_id).clicked() {
                        self.selected_room = Some(room_id.clone());
                    }
                }
            });
    }

    fn ui_messages(&self, ui: &mut egui::Ui) {
        egui::containers::ScrollArea::vertical()
            .auto_shrink([false, false])
            .stick_to_bottom(true)
            .show(ui, |ui| {
                let Some(room) = self.get_selected_room() else {
                    ui.label("No room selected");
                    return;
                };

                let mut messages: Vec<&Message> = room.messages.values().collect();
                messages.sort_by_key(|f| f.timestamp);

                for message in messages {
                    self.ui_message(ui, message);
                }
            });
    }

    fn ui_message(&self, ui: &mut egui::Ui, message: &Message) {
        if message.is_encrypted {
            self.ui_encrypted_message(ui, message);
        } else {
            self.ui_decryped_message(ui, message);
        }
    }

    fn ui_decryped_message(&self, ui: &mut egui::Ui, message: &Message) {
        ui.horizontal(|ui| {
            ui.spacing_mut().item_spacing.x = 0.2;
            ui.label("🔑");
            let re = ui.label(format!("  {}", &message.message_id));

            re.on_hover_ui(|ui| {
                ui.label(format!("{message:?}"));

                if let Some(message::Content::Text(c)) = &message.content {
                    ui.label(format!("REDACTED_CONTENT:\n{}", c.content));
                }
            });
        });
    }

    fn ui_encrypted_message(&self, ui: &mut egui::Ui, message: &Message) {
        ui.colored_label(egui::Color32::YELLOW, format!("🔐 {}", message.message_id));
    }
}
