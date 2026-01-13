use egui::Widget;
use mrhc_proto::chat::*;

macro_rules! ui_attribute {
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

pub trait UiAttribute {
    fn update(&mut self, ui: &mut egui::Ui);
    fn is_multiline(&self) -> bool {
        false
    }
}

impl UiAttribute for String {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui.text_edit_singleline(self);
    }
}

impl UiAttribute for bool {
    fn update(&mut self, ui: &mut egui::Ui) {
        egui::Checkbox::without_text(self).ui(ui);
    }
}

impl UiAttribute for i32 {
    fn update(&mut self, ui: &mut egui::Ui) {
        egui::DragValue::new(self).ui(ui);
    }
}

impl UiAttribute for u32 {
    fn update(&mut self, ui: &mut egui::Ui) {
        egui::DragValue::new(self).ui(ui);
    }
}

impl<T> UiAttribute for Option<T>
where
    T: UiAttribute + Default,
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

impl<T> UiAttribute for Vec<T>
where
    T: UiAttribute + Default,
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

impl UiAttribute for InitializationRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui_attribute!(self, ui, backend_url);
        ui_attribute!(self, ui, data_root_path);
        ui_attribute!(self, ui, persistent_storage_secret);
        ui_attribute!(self, ui, encryption_secret);
        ui_attribute!(self, ui, device_display_name);
    }
}

impl UiAttribute for SsoLoginRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui_attribute!(self, ui, identity_provider);
    }
}

impl UiAttribute for RoomListRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui_attribute!(self, ui, include_joined);
        ui_attribute!(self, ui, include_unjoined);
    }
}

impl UiAttribute for SendMessageRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui_attribute!(self, ui, room_id);
        ui_attribute!(self, ui, mime_type);
        ui_attribute!(self, ui, content);
        ui_attribute!(self, ui, related_message_id);
    }
}

impl UiAttribute for UserSearchRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui_attribute!(self, ui, query);
        ui_attribute!(self, ui, limit);
    }
}

impl UiAttribute for VerificationAbortRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui_attribute!(self, ui, verification_flow_id);
    }
}

impl UiAttribute for RecoveryKeyVerificationRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui_attribute!(self, ui, recovery_key);
    }
}

impl UiAttribute for CrossSigningStartRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui_attribute!(self, ui, verification_flow_id);
        ui_attribute!(self, ui, supported_methods);
    }
}

impl UiAttribute for CrossSigningMethodSelectedRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui_attribute!(self, ui, verification_flow_id);
        ui_attribute!(self, ui, selected_method);
    }
}

impl UiAttribute for CrossSigningAcceptRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui_attribute!(self, ui, verification_flow_id);
    }
}

impl UiAttribute for CreateDirectRoomRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui_attribute!(self, ui, display_name);
        ui_attribute!(self, ui, invitee);
    }
}

impl UiAttribute for CreateGroupRoomRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui_attribute!(self, ui, display_name);
        ui_attribute!(self, ui, invitees);
        ui_attribute!(self, ui, is_public);
    }
}

impl UiAttribute for MarkAsReadRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui_attribute!(self, ui, room_id);
    }
}

impl UiAttribute for InvitationRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui_attribute!(self, ui, room_id);
        ui_attribute!(self, ui, invitees);
        ui_attribute!(self, ui, invitation_text);
    }
}

impl UiAttribute for ChangeRoomRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui_attribute!(self, ui, room_id);
        ui_attribute!(self, ui, display_name);
        ui_attribute!(self, ui, is_public);
    }
}

impl UiAttribute for LeaveRoomRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui_attribute!(self, ui, room_id);
    }
}

impl UiAttribute for JoinRoomRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui_attribute!(self, ui, room_id);
    }
}

impl UiAttribute for PublicRoomListRequest {
    fn update(&mut self, ui: &mut egui::Ui) {
        ui_attribute!(self, ui, limit);
        ui_attribute!(self, ui, since);
        ui_attribute!(self, ui, generic_search_term);
    }
}
