use egui::Widget;
use gouda_proto::chat::*;

macro_rules! input_attribute {
    ($self:ident, $ui:ident, $attr:ident) => {
        if $self.$attr.is_multiline() {
            $ui.label(concat!(stringify!($attr), ":"));
            $self.$attr.update($ui);
        } else {
            $ui.horizontal(|ui| {
                $self.$attr.update(ui);
                ui.label(stringify!($attr));
            });
        }
    };
}

pub trait InputUi {
    fn update(&mut self, ui: &mut egui::Ui);
    fn is_multiline(&self) -> bool {
        false
    }
}

impl InputUi for String {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui.text_edit_singleline(self);
    }
}

impl InputUi for bool {
    fn update(&mut self, ui: &mut egui::Ui) {
        egui::Checkbox::without_text(self).ui(ui);
    }
}

impl InputUi for i32 {
    fn update(&mut self, ui: &mut egui::Ui) {
        egui::DragValue::new(self).ui(ui);
    }
}

impl InputUi for u32 {
    fn update(&mut self, ui: &mut egui::Ui) {
        egui::DragValue::new(self).ui(ui);
    }
}

impl<T> InputUi for Option<T>
where
    T: InputUi + Default,
{
    fn update(&mut self, ui: &mut egui::Ui) {
        let mut checked = self.is_some();
        if egui::Checkbox::without_text(&mut checked).ui(ui).clicked() {
            if checked {
                *self = Some(T::default());
            } else {
                *self = None;
            }
        }

        ui.add_enabled_ui(checked, |ui| {
            if let Some(val) = self {
                val.update(ui);
            } else {
                T::default().update(ui);
            }
        });
    }
}

impl<T> InputUi for Vec<T>
where
    T: InputUi + Default,
{
    fn update(&mut self, ui: &mut egui::Ui) {
        const BUTTON_SIZE: egui::Vec2 = egui::Vec2::new(20.0, 20.0);

        ui.vertical(|ui| {
            let mut remove = Vec::new();

            for (i, item) in self.iter_mut().enumerate() {
                ui.horizontal(|ui| {
                    if ui.add_sized(BUTTON_SIZE, egui::Button::new("-")).clicked() {
                        remove.push(i);
                    }
                    item.update(ui);
                });
            }

            for i in remove {
                self.remove(i);
            }

            ui.horizontal(|ui| {
                if ui.add_sized(BUTTON_SIZE, egui::Button::new("+")).clicked() {
                    self.push(T::default());
                }
            });
        });
    }

    fn is_multiline(&self) -> bool {
        true
    }
}

impl InputUi for InitializationRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, backend_url);
        input_attribute!(self, ui, data_root_path);
        input_attribute!(self, ui, persistent_storage_secret);
        input_attribute!(self, ui, encryption_secret);
        input_attribute!(self, ui, device_display_name);
    }
}

impl InputUi for LoginUsernamePasswordRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, username);
        input_attribute!(self, ui, password);
    }
}

impl InputUi for LoginSsoRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, identity_provider);
    }
}

impl InputUi for RecoveryKeyVerificationRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, recovery_key);
    }
}

impl InputUi for CrossSigningStartRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, verification_flow_id);
        input_attribute!(self, ui, supported_methods);
    }
}

impl InputUi for CrossSigningMethodSelectedRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, verification_flow_id);
        input_attribute!(self, ui, selected_method);
    }
}

impl InputUi for CrossSigningConfirmRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, verification_flow_id);
    }
}

impl InputUi for VerificationAbortRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, verification_flow_id);
    }
}

impl InputUi for GlobalSettingsRequest {
    fn update(&mut self, _ui: &mut egui::Ui) {}
}

impl InputUi for UserRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, user_id);
    }
}

impl InputUi for UserSearchRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, query);
        input_attribute!(self, ui, limit);
    }
}

impl InputUi for UserStatus {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, state);
        input_attribute!(self, ui, status_message);
    }
}

impl InputUi for PublicRoomListRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, limit);
        input_attribute!(self, ui, since);
        input_attribute!(self, ui, generic_search_term);
    }
}

impl InputUi for InvitationRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, room_id);
        input_attribute!(self, ui, invitees);
        input_attribute!(self, ui, invitation_text);
    }
}

impl InputUi for InvitedReply {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, room_id);
        input_attribute!(self, ui, accepted);
    }
}

impl InputUi for RoomListRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, include_joined);
        input_attribute!(self, ui, include_unjoined);
    }
}

impl InputUi for RoomCreateGroupRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, display_name);
        input_attribute!(self, ui, invitees);
        input_attribute!(self, ui, join_rule);
        input_attribute!(self, ui, avatar_path);
    }
}

impl InputUi for RoomCreateDirectRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, display_name);
        input_attribute!(self, ui, invitee);
        input_attribute!(self, ui, avatar_path);
    }
}

impl InputUi for RoomChangeRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, room_id);
        input_attribute!(self, ui, display_name);
        input_attribute!(self, ui, join_rule);
        input_attribute!(self, ui, is_favorite);
        input_attribute!(self, ui, avatar_path);
    }
}

impl InputUi for RoomLeaveRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, room_id);
    }
}

impl InputUi for RoomJoinRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, room_id);
    }
}

impl InputUi for RoomKnockRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, room_id);
        input_attribute!(self, ui, message);
    }
}

impl InputUi for RoomMessagesRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, room_id);
        input_attribute!(self, ui, order);
        input_attribute!(self, ui, from_message_id);
        input_attribute!(self, ui, limit);
    }
}

impl InputUi for RoomMarkAsReadRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, room_id);
    }
}

impl InputUi for RoomTypingRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, room_id);
    }
}

impl InputUi for message_send_request::Content {
    fn update(&mut self, ui: &mut egui::Ui) {
        let text = match self {
            Self::Text(_) => "Text",
            Self::File(_) => "File",
        };

        ui.vertical(|ui| {
            egui::ComboBox::from_id_salt("message_send_request_content")
                .selected_text(text)
                .show_ui(ui, |ui| {
                    ui.selectable_value(self, Self::Text(MessageContentText::default()), "Text");
                    ui.selectable_value(self, Self::File(MessageContentFile::default()), "File");
                });

            match self {
                Self::Text(content) => content.update(ui),
                Self::File(content) => content.update(ui),
            }
        });
    }

    fn is_multiline(&self) -> bool {
        true
    }
}

impl InputUi for message_change_event::Content {
    fn update(&mut self, ui: &mut egui::Ui) {
        let text = match self {
            Self::Text(_) => "Text",
            Self::File(_) => "File",
            Self::MembershipChange(_) => "MembershipChange",
        };

        ui.vertical(|ui| {
            egui::ComboBox::from_id_salt("message_change_event_content")
                .selected_text(text)
                .show_ui(ui, |ui| {
                    ui.selectable_value(self, Self::Text(MessageContentText::default()), "Text");
                    ui.selectable_value(self, Self::File(MessageContentFile::default()), "File");
                });

            match self {
                Self::Text(content) => content.update(ui),
                Self::File(content) => content.update(ui),
                Self::MembershipChange(content) => content.update(ui),
            }
        });
    }

    fn is_multiline(&self) -> bool {
        true
    }
}

impl InputUi for message_change_request::Content {
    fn update(&mut self, ui: &mut egui::Ui) {
        let text = match self {
            Self::Text(_) => "Text",
            Self::File(_) => "File",
        };

        ui.vertical(|ui| {
            egui::ComboBox::from_id_salt("message_change_request_content")
                .selected_text(text)
                .show_ui(ui, |ui| {
                    ui.selectable_value(self, Self::Text(MessageContentText::default()), "Text");
                    ui.selectable_value(self, Self::File(MessageContentFile::default()), "File");
                });

            match self {
                Self::Text(content) => content.update(ui),
                Self::File(content) => content.update(ui),
            }
        });
    }

    fn is_multiline(&self) -> bool {
        true
    }
}

impl InputUi for MessageContentText {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, content);
    }
}

impl InputUi for MessageContentFile {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, file_path);
        input_attribute!(self, ui, file_name);
    }
}

impl InputUi for MessageContentMembershipChange {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, change);
    }
}

impl InputUi for MessageSendRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, room_id);
        input_attribute!(self, ui, related_message_id);
        input_attribute!(self, ui, mentioned_user_ids);
        input_attribute!(self, ui, content);
    }
}

impl InputUi for MessageRemoveRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, room_id);
        input_attribute!(self, ui, message_id);
    }
}

impl InputUi for MessageChangeRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, room_id);
        input_attribute!(self, ui, message_id);
        input_attribute!(self, ui, has_mentioned_user_ids_changed);
        input_attribute!(self, ui, mentioned_user_ids);
        input_attribute!(self, ui, content);
    }
}

impl InputUi for Reaction {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, room_id);
        input_attribute!(self, ui, message_id);
        input_attribute!(self, ui, reaction);
        input_attribute!(self, ui, user_id);
    }
}

impl InputUi for MessageRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        input_attribute!(self, ui, room_id);
        input_attribute!(self, ui, message_id);
    }
}
