use crate::actions::Action;
use crate::context::Context;
use crate::ui::InputUi;

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
    selected_action: Action,
    tag: u64,
}

impl InputWindow {
    pub fn new(context: &Context) -> Self {
        let default_action = Action::Initialize(Box::new(context.config().initialize.clone()));

        Self {
            selected_action: default_action,
            tag: 0,
        }
    }

    pub fn show(&mut self, ui: &mut egui::Ui, context: &mut Context) {
        egui::Window::new("Request")
            .resizable(true)
            .min_size(egui::Vec2::new(500.0, 500.0))
            .show(ui, |ui| {
                self.update_selection(context, ui);

                ui.add_space(10.0);

                egui::ScrollArea::vertical().show(ui, |ui| {
                    self.selected_action.update(ui);
                });

                ui.add_space(10.0);

                if ui.button("Submit").clicked() {
                    self.tag += 1;

                    let response = self.selected_action.to_container(self.tag);

                    context.queue_request(response);
                }
            });
    }

    fn update_selection(&mut self, ctx: &mut Context, ui: &mut egui::Ui) {
        egui::ComboBox::from_label("Action")
            .selected_text(format!("{}", self.selected_action))
            .show_ui(ui, |ui| {
                ui_action!(self, ui, Initialize, ctx.config().initialize.clone());
                ui_action!(self, ui, LoginFlows);
                ui_action!(self, ui, IdentityProviders);
                ui_action!(
                    self,
                    ui,
                    LoginUsernamePassword,
                    ctx.config().login_username_password.clone()
                );
                ui_action!(self, ui, LoginSso, ctx.config().login_sso.clone());
                ui_action!(
                    self,
                    ui,
                    RecoveryKeyVerification,
                    ctx.config().recovery_key_verification.clone()
                );
                ui_action!(
                    self,
                    ui,
                    CrossSigningStart,
                    ctx.config().cross_signing_start.clone()
                );
                ui_action!(
                    self,
                    ui,
                    CrossSigningSelectMethod,
                    ctx.config().cross_signing_select_method.clone()
                );
                ui_action!(
                    self,
                    ui,
                    CrossSigningConfirm,
                    ctx.config().cross_signing_confirm.clone()
                );
                ui_action!(
                    self,
                    ui,
                    AbortVerification,
                    ctx.config().abort_verification.clone()
                );
                ui_action!(
                    self,
                    ui,
                    GetGlobalSettings,
                    ctx.config().get_global_settings
                );
                ui_action!(self, ui, GetUser, ctx.config().get_user.clone());
                ui_action!(self, ui, UserSearch, ctx.config().user_search.clone());
                ui_action!(
                    self,
                    ui,
                    SetUserStatus,
                    ctx.config().set_user_status.clone()
                );
                ui_action!(
                    self,
                    ui,
                    PublicRoomList,
                    ctx.config().public_room_list.clone()
                );
                ui_action!(self, ui, Invite, ctx.config().invite.clone());
                ui_action!(
                    self,
                    ui,
                    InvitationReply,
                    ctx.config().invitation_reply.clone()
                );
                ui_action!(self, ui, RoomList, ctx.config().room_list);
                ui_action!(
                    self,
                    ui,
                    CreateGroupRoom,
                    ctx.config().create_group_room.clone()
                );
                ui_action!(
                    self,
                    ui,
                    CreateDirectRoom,
                    ctx.config().create_direct_room.clone()
                );
                ui_action!(self, ui, ChangeRoom, ctx.config().change_room.clone());
                ui_action!(self, ui, LeaveRoom, ctx.config().leave_room.clone());
                ui_action!(self, ui, JoinRoom, ctx.config().join_room.clone());
                ui_action!(self, ui, KnockRoom, ctx.config().knock_room.clone());
                ui_action!(self, ui, RoomMessages, ctx.config().room_messages.clone());
                ui_action!(self, ui, MarkAsRead, ctx.config().mark_as_read.clone());
                ui_action!(
                    self,
                    ui,
                    ActivateTypingNotice,
                    ctx.config().activate_typing_notice.clone()
                );
                ui_action!(self, ui, SendMessage, ctx.config().send_message.clone());
                ui_action!(self, ui, RemoveMessage, ctx.config().remove_message.clone());
                ui_action!(self, ui, ChangeMessage, ctx.config().change_message.clone());
                ui_action!(
                    self,
                    ui,
                    CreateReaction,
                    ctx.config().create_reaction.clone()
                );
                ui_action!(
                    self,
                    ui,
                    RemoveReaction,
                    ctx.config().remove_reaction.clone()
                );
                ui_action!(self, ui, GetMessage, ctx.config().get_message.clone());
            });
    }
}
