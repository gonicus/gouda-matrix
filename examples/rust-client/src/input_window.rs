use std::sync::mpsc::Sender;

use interprocess::local_socket::SendHalf;

use crate::actions::Action;
use crate::config::Config;
use crate::output_window::OutputLog;
use crate::ui_attribute::UiAttribute;

macro_rules! ui_action {
    ($self:ident, $ui:ident, $name:ident) => {
        $ui.selectable_value(&mut $self.selected_action, Action::$name, stringify!($name));
    };
    ($self:ident, $ui:ident, $name:ident, $request_content:expr) => {
        $ui.selectable_value(
            &mut $self.selected_action,
            Action::$name(Box::new($request_content)),
            stringify!($name),
        );
    };
}

pub struct InputWindow {
    config: Config,
    sender: SendHalf,
    output_sender: Sender<OutputLog>,
    selected_action: Action,
    tag: u64,
}

impl InputWindow {
    pub fn new(config: Config, send_half: SendHalf, output_sender: Sender<OutputLog>) -> Self {
        let default_action = Action::Initialize(Box::new(config.initialize.clone()));

        Self {
            config,
            sender: send_half,
            output_sender,
            selected_action: default_action,
            tag: 0,
        }
    }

    pub fn update(&mut self, ctx: &egui::Context) {
        egui::Window::new("Request")
            .resizable(true)
            .min_size(egui::Vec2::new(500.0, 500.0))
            .show(ctx, |ui| {
                self.update_selection(ui);

                ui.add_space(10.0);

                egui::ScrollArea::vertical().show(ui, |ui| {
                    self.selected_action.update(ui);
                });

                ui.add_space(10.0);

                if ui.button("Submit").clicked() {
                    self.tag += 1;

                    let re = self.selected_action.run(&mut self.sender, self.tag);

                    self.output_sender
                        .send(OutputLog::Request(re))
                        .expect("Error sending request to output window");
                }
            });
    }

    fn update_selection(&mut self, ui: &mut egui::Ui) {
        egui::ComboBox::from_label("Action")
            .selected_text(format!("{}", self.selected_action))
            .show_ui(ui, |ui| {
                ui_action!(self, ui, Initialize, self.config.initialize.clone());
                ui_action!(self, ui, LoginFlows);
                ui_action!(self, ui, IdentityProviders);
                ui_action!(self, ui, LoginSso, self.config.login_sso.clone());
                ui_action!(self, ui, RoomList, self.config.room_list);
                ui_action!(self, ui, UserList);
                ui_action!(self, ui, UserSearch, self.config.user_search.clone());
                ui_action!(self, ui, SendMessage, self.config.send_message.clone());
                ui_action!(
                    self,
                    ui,
                    AbortVerification,
                    self.config.abort_verification.clone()
                );
                ui_action!(
                    self,
                    ui,
                    RecoveryKeyVerification,
                    self.config.recovery_key_verification.clone()
                );
                ui_action!(
                    self,
                    ui,
                    CrossSigningStart,
                    self.config.cross_signing_start.clone()
                );
                ui_action!(
                    self,
                    ui,
                    CrossSigningSelectMethod,
                    self.config.cross_signing_select_method.clone()
                );
                ui_action!(
                    self,
                    ui,
                    CrossSigningAccept,
                    self.config.cross_signing_accept.clone()
                );
                ui_action!(
                    self,
                    ui,
                    CreateDirectRoom,
                    self.config.create_direct_room.clone()
                );
                ui_action!(
                    self,
                    ui,
                    CreateGroupRoom,
                    self.config.create_group_room.clone()
                );
                ui_action!(self, ui, MarkAsRead, self.config.mark_as_read.clone());
                ui_action!(self, ui, Invite, self.config.invite.clone());
                ui_action!(self, ui, ChangeRoom, self.config.change_room.clone());
                ui_action!(self, ui, LeaveRoom, self.config.leave_room.clone());
                ui_action!(self, ui, JoinRoom, self.config.join_room.clone());
                ui_action!(
                    self,
                    ui,
                    PublicRoomList,
                    self.config.public_room_list.clone()
                );
                ui_action!(self, ui, KnockRoom, self.config.knock_room.clone());
                ui_action!(
                    self,
                    ui,
                    CreateReaction,
                    self.config.create_reaction.clone()
                );
                ui_action!(self, ui, ChangeMessage, self.config.change_message.clone());
                ui_action!(self, ui, RemoveMessage, self.config.remove_message.clone());
            });
    }
}
