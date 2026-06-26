use gouda_proto::chat::request_container::Content as RequestContent;
use gouda_proto::chat::*;
use strum_macros::{Display, EnumString};

use crate::ui::InputUi;

macro_rules! impl_to_container {
    ($method:ident, $request_name:ident) => {
        fn $method(tag: u64) -> RequestContainer {
            let request = RequestContainer {
                tag,
                content: Some(RequestContent::$request_name($request_name::default())),
            };

            request
        }
    };
    ($method:ident, $request_name:ident, $payload_name:ident) => {
        fn $method(tag: u64, request: $payload_name) -> RequestContainer {
            let request = RequestContainer {
                tag,
                content: Some(RequestContent::$request_name(request)),
            };

            request
        }
    };
}

#[derive(Clone, Debug, EnumString, Display, PartialEq)]
#[strum(serialize_all = "kebab-case")]
pub enum Action {
    Initialize(Box<InitializationRequest>),
    LoginFlows,
    IdentityProviders,
    LoginUsernamePassword(Box<LoginUsernamePasswordRequest>),
    LoginSso(Box<LoginSsoRequest>),
    RecoveryKeyVerification(Box<RecoveryKeyVerificationRequest>),
    CrossSigningStart(Box<CrossSigningStartRequest>),
    CrossSigningSelectMethod(Box<CrossSigningMethodSelectedRequest>),
    CrossSigningConfirm(Box<CrossSigningConfirmRequest>),
    AbortVerification(Box<VerificationAbortRequest>),
    GetGlobalSettings(Box<GlobalSettingsRequest>),
    GetUser(Box<UserRequest>),
    UserSearch(Box<UserSearchRequest>),
    SetUserStatus(Box<UserStatus>),
    PublicRoomList(Box<PublicRoomListRequest>),
    Invite(Box<InvitationRequest>),
    InvitationReply(Box<InvitedReply>),
    RoomList(Box<RoomListRequest>),
    CreateGroupRoom(Box<RoomCreateGroupRequest>),
    CreateDirectRoom(Box<RoomCreateDirectRequest>),
    ChangeRoom(Box<RoomChangeRequest>),
    LeaveRoom(Box<RoomLeaveRequest>),
    JoinRoom(Box<RoomJoinRequest>),
    KnockRoom(Box<RoomKnockRequest>),
    RoomMessages(Box<RoomMessagesRequest>),
    MarkAsRead(Box<RoomMarkAsReadRequest>),
    ActivateTypingNotice(Box<RoomTypingRequest>),
    SendMessage(Box<MessageSendRequest>),
    RemoveMessage(Box<MessageRemoveRequest>),
    ChangeMessage(Box<MessageChangeRequest>),
    CreateReaction(Box<Reaction>),
    RemoveReaction(Box<Reaction>),
    GetMessage(Box<MessageRequest>),
}

impl Action {
    pub fn to_container(&self, tag: u64) -> RequestContainer {
        match self.clone() {
            Self::Initialize(request) => run_initialize(tag, *request),
            Self::LoginFlows => run_login_flows(tag),
            Self::IdentityProviders => run_identity_providers(tag),
            Self::LoginUsernamePassword(request) => run_login_username_password(tag, *request),
            Self::LoginSso(request) => run_login_sso(tag, *request),
            Self::RecoveryKeyVerification(request) => run_recovery_key_verification(tag, *request),
            Self::CrossSigningStart(request) => run_cross_signing_start(tag, *request),
            Self::CrossSigningSelectMethod(request) => {
                run_cross_signing_select_method(tag, *request)
            }
            Self::CrossSigningConfirm(request) => run_cross_signing_confirm(tag, *request),
            Self::AbortVerification(request) => run_abort_verification(tag, *request),
            Self::GetGlobalSettings(request) => run_get_global_settings(tag, *request),
            Self::GetUser(request) => run_get_user(tag, *request),
            Self::UserSearch(request) => run_user_search(tag, *request),
            Self::SetUserStatus(request) => run_set_status(tag, *request),
            Self::PublicRoomList(request) => run_public_room_list(tag, *request),
            Self::Invite(request) => run_invite(tag, *request),
            Self::InvitationReply(request) => run_invitation_reply(tag, *request),
            Self::RoomList(request) => run_room_list(tag, *request),
            Self::CreateGroupRoom(request) => run_create_group_room(tag, *request),
            Self::CreateDirectRoom(request) => run_create_direct_room(tag, *request),
            Self::ChangeRoom(request) => run_change_room(tag, *request),
            Self::LeaveRoom(request) => run_leave_room(tag, *request),
            Self::JoinRoom(request) => run_join_room(tag, *request),
            Self::KnockRoom(request) => run_knock_room(tag, *request),
            Self::RoomMessages(request) => run_room_messages(tag, *request),
            Self::MarkAsRead(request) => run_mark_as_read(tag, *request),
            Self::ActivateTypingNotice(request) => run_activate_typing_notice(tag, *request),
            Self::SendMessage(request) => run_send_message(tag, *request),
            Self::RemoveMessage(request) => run_remove_message(tag, *request),
            Self::ChangeMessage(request) => run_change_message(tag, *request),
            Self::CreateReaction(request) => run_create_reaction(tag, *request),
            Self::RemoveReaction(request) => run_remove_reaction(tag, *request),
            Self::GetMessage(request) => run_get_message(tag, *request),
        }
    }
}

impl InputUi for Action {
    fn update(&mut self, ui: &mut egui::Ui) {
        match self {
            Self::Initialize(request) => request.update(ui),
            Self::LoginFlows => (),
            Self::IdentityProviders => (),
            Self::LoginUsernamePassword(request) => request.update(ui),
            Self::LoginSso(request) => request.update(ui),
            Self::RecoveryKeyVerification(request) => request.update(ui),
            Self::CrossSigningStart(request) => request.update(ui),
            Self::CrossSigningSelectMethod(request) => request.update(ui),
            Self::CrossSigningConfirm(request) => request.update(ui),
            Self::AbortVerification(request) => request.update(ui),
            Self::GetGlobalSettings(request) => request.update(ui),
            Self::GetUser(request) => request.update(ui),
            Self::UserSearch(request) => request.update(ui),
            Self::SetUserStatus(request) => request.update(ui),
            Self::PublicRoomList(request) => request.update(ui),
            Self::Invite(request) => request.update(ui),
            Self::InvitationReply(request) => request.update(ui),
            Self::RoomList(request) => request.update(ui),
            Self::CreateGroupRoom(request) => request.update(ui),
            Self::CreateDirectRoom(request) => request.update(ui),
            Self::ChangeRoom(request) => request.update(ui),
            Self::LeaveRoom(request) => request.update(ui),
            Self::JoinRoom(request) => request.update(ui),
            Self::KnockRoom(request) => request.update(ui),
            Self::RoomMessages(request) => request.update(ui),
            Self::MarkAsRead(request) => request.update(ui),
            Self::ActivateTypingNotice(request) => request.update(ui),
            Self::SendMessage(request) => request.update(ui),
            Self::RemoveMessage(request) => request.update(ui),
            Self::ChangeMessage(request) => request.update(ui),
            Self::CreateReaction(request) => request.update(ui),
            Self::RemoveReaction(request) => request.update(ui),
            Self::GetMessage(request) => request.update(ui),
        }
    }
}

impl_to_container!(run_initialize, InitializationRequest, InitializationRequest);
impl_to_container!(run_login_flows, LoginFlowsRequest);
impl_to_container!(run_identity_providers, IdentityProvidersRequest);
impl_to_container!(
    run_login_username_password,
    LoginUsernamePasswordRequest,
    LoginUsernamePasswordRequest
);
impl_to_container!(run_login_sso, LoginSsoRequest, LoginSsoRequest);
impl_to_container!(
    run_recovery_key_verification,
    RecoveryKeyVerificationRequest,
    RecoveryKeyVerificationRequest
);
impl_to_container!(
    run_cross_signing_start,
    CrossSigningStartRequest,
    CrossSigningStartRequest
);
impl_to_container!(
    run_cross_signing_select_method,
    CrossSigningMethodSelectedRequest,
    CrossSigningMethodSelectedRequest
);
impl_to_container!(
    run_cross_signing_confirm,
    CrossSigningConfirmRequest,
    CrossSigningConfirmRequest
);
impl_to_container!(
    run_abort_verification,
    VerificationAbortRequest,
    VerificationAbortRequest
);
impl_to_container!(
    run_get_global_settings,
    GlobalSettingsRequest,
    GlobalSettingsRequest
);
impl_to_container!(run_get_user, UserRequest, UserRequest);
impl_to_container!(run_user_search, UserSearchRequest, UserSearchRequest);
impl_to_container!(run_set_status, UserStatusSetOwnRequest, UserStatus);
impl_to_container!(
    run_public_room_list,
    PublicRoomListRequest,
    PublicRoomListRequest
);
impl_to_container!(run_invite, InvitationRequest, InvitationRequest);
impl_to_container!(run_invitation_reply, InvitedReply, InvitedReply);
impl_to_container!(run_room_list, RoomListRequest, RoomListRequest);
impl_to_container!(
    run_create_group_room,
    RoomCreateGroupRequest,
    RoomCreateGroupRequest
);
impl_to_container!(
    run_create_direct_room,
    RoomCreateDirectRequest,
    RoomCreateDirectRequest
);
impl_to_container!(run_change_room, RoomChangeRequest, RoomChangeRequest);
impl_to_container!(run_leave_room, RoomLeaveRequest, RoomLeaveRequest);
impl_to_container!(run_join_room, RoomJoinRequest, RoomJoinRequest);

impl_to_container!(run_knock_room, RoomKnockRequest, RoomKnockRequest);
impl_to_container!(run_room_messages, RoomMessagesRequest, RoomMessagesRequest);
impl_to_container!(
    run_mark_as_read,
    RoomMarkAsReadRequest,
    RoomMarkAsReadRequest
);
impl_to_container!(
    run_activate_typing_notice,
    RoomTypingRequest,
    RoomTypingRequest
);
impl_to_container!(run_send_message, MessageSendRequest, MessageSendRequest);
impl_to_container!(
    run_remove_message,
    MessageRemoveRequest,
    MessageRemoveRequest
);
impl_to_container!(
    run_change_message,
    MessageChangeRequest,
    MessageChangeRequest
);
impl_to_container!(run_create_reaction, CreateReactionRequest, Reaction);
impl_to_container!(run_remove_reaction, RemoveReactionRequest, Reaction);
impl_to_container!(run_get_message, MessageRequest, MessageRequest);
