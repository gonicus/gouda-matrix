use std::collections::HashMap;

use gouda_proto::chat::response_container::Content as ResponseContent;
use gouda_proto::chat::{
    Message, MessageChangeEvent, MessageRemoveEvent, ResponseContainer, message,
};

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

    pub fn show(&mut self, ui: &mut egui::Ui, ctx: &mut Context) {
        self.collect_responses(ctx);

        egui::Window::new("Messages")
            .resizable(true)
            .default_size(egui::Vec2::new(500.0, 500.0))
            .show(ui, |ui| {
                self.ui(ui);
            });
    }

    fn get_or_create_room(&mut self, room_id: impl Into<String>) -> &mut Room {
        self.rooms.entry(room_id.into()).or_default()
    }

    fn get_selected_room(&self) -> Option<&Room> {
        self.rooms.get(self.selected_room.as_ref()?)
    }

    fn collect_responses(&mut self, context: &mut Context) {
        let responses: Vec<ResponseContainer> = context.received_responses().to_vec();

        for response in responses {
            let Some(content) = &response.content else {
                context.display_warning(format!(
                    "Received response with empty content: {response:?}"
                ));

                continue;
            };

            self.collect_response_content(context, content);
        }
    }

    fn collect_response_content(&mut self, ctx: &mut Context, content: &ResponseContent) {
        match content {
            ResponseContent::MessageReceivedEvent(message) => self.collect_message(message.clone()),
            ResponseContent::MessageRemoveEvent(event) => self.remove_message(ctx, event.clone()),
            ResponseContent::MessageChangeEvent(event) => self.change_message(ctx, event.clone()),
            _ => (),
        }
    }

    fn collect_message(&mut self, message: Message) {
        let room = self.get_or_create_room(&message.room_id);
        room.messages.insert(message.message_id.clone(), message);
    }

    fn remove_message(&mut self, ctx: &mut Context, event: MessageRemoveEvent) {
        let Some(room) = self.rooms.get_mut(&event.room_id) else {
            return;
        };

        if room.messages.remove(&event.message_id).is_none() {
            ctx.display_warning(format!(
                "Received MessageRemoveEvent for a message we don't know: {}",
                event.message_id
            ));
        }
    }

    fn change_message(&mut self, ctx: &mut Context, event: MessageChangeEvent) {
        let Some(room) = self.rooms.get_mut(&event.room_id) else {
            return;
        };

        let Some(message) = room.messages.get_mut(&event.message_id) else {
            ctx.display_warning(format!(
                "Received MessageChangeEvent for a message we don't know: {}",
                event.message_id
            ));

            return;
        };

        event.update_into_message(message);
    }

    fn ui(&mut self, ui: &mut egui::Ui) {
        self.ui_room_selection(ui);

        let Some(room) = self.get_selected_room() else {
            ui.label("No room selected");
            return;
        };

        self.ui_room(ui, room);
    }

    fn ui_room_selection(&mut self, ui: &mut egui::Ui) {
        if self.selected_room.is_none()
            && let Some(room) = self.rooms.keys().next()
        {
            self.selected_room = Some(room.clone());
        }

        egui::ComboBox::from_label("Room")
            .selected_text(format!("{:?}", self.selected_room))
            .show_ui(ui, |ui| {
                for room_id in self.rooms.keys() {
                    let selected = Some(room_id) == self.selected_room.as_ref();

                    if ui.selectable_label(selected, room_id).clicked() {
                        self.selected_room = Some(room_id.clone());
                    }
                }
            });
    }

    fn ui_room(&self, ui: &mut egui::Ui, room: &Room) {
        let num_messages = room.messages.len();
        let num_encrypted = room.messages.values().filter(|p| p.is_encrypted).count();

        ui.label(format!("Messages: {num_messages}"));
        ui.label(format!("Encrypted: {num_encrypted} / {num_messages}"));

        egui::containers::ScrollArea::vertical()
            .auto_shrink([false, false])
            .stick_to_bottom(true)
            .show(ui, |ui| {
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
            self.ui_decrypted_message(ui, message);
        }
    }

    fn ui_decrypted_message(&self, ui: &mut egui::Ui, message: &Message) {
        ui.horizontal(|ui| {
            ui.spacing_mut().item_spacing.x = 0.2;
            ui.label("🔑");
            let re = ui.label(format!("  {}", message.message_id));

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
