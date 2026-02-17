use std::io::Write;

use interprocess::local_socket::SendHalf;
use mrhc_proto::chat::request_container::Content as RequestContent;
use mrhc_proto::chat::*;
use prost::Message;
use strum_macros::{Display, EnumString};

use crate::ui_attribute::UiAttribute;

macro_rules! impl_run {
    ($method:ident, $request_name:ident) => {
        fn $method(tag: u64, sender: &mut SendHalf) -> RequestContainer {
            let request = RequestContainer {
                tag,
                content: Some(RequestContent::$request_name($request_name::default())),
            };

            send_request(sender, request.clone());

            request
        }
    };
    ($method:ident, $request_name:ident, $payload_name:ident) => {
        fn $method(tag: u64, sender: &mut SendHalf, request: $payload_name) -> RequestContainer {
            let request = RequestContainer {
                tag,
                content: Some(RequestContent::$request_name(request)),
            };

            send_request(sender, request.clone());

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
    LoginSso(Box<LoginSsoRequest>),
    RecoveryKeyVerification(Box<RecoveryKeyVerificationRequest>),
    CrossSigningStart(Box<CrossSigningStartRequest>),
    CrossSigningSelectMethod(Box<CrossSigningMethodSelectedRequest>),
    CrossSigningConfirm(Box<CrossSigningConfirmRequest>),
    AbortVerification(Box<VerificationAbortRequest>),
    UserList,
    UserSearch(Box<UserSearchRequest>),
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
    MarkAsRead(Box<RoomMarkAsReadRequest>),
    SendMessage(Box<MessageSendRequest>),
    RoomMessages(Box<RoomMessagesRequest>),
    RemoveMessage(Box<MessageRemoveRequest>),
    ChangeMessage(Box<MessageChangeRequest>),
    CreateReaction(Box<Reaction>),
}

impl Action {
    pub fn run(&self, sender: &mut SendHalf, tag: u64) -> RequestContainer {
        match self.clone() {
            Self::Initialize(request) => run_initialize(tag, sender, *request),
            Self::LoginFlows => run_login_flows(tag, sender),
            Self::IdentityProviders => run_identity_providers(tag, sender),
            Self::LoginSso(request) => run_login_sso(tag, sender, *request),
            Self::RecoveryKeyVerification(request) => {
                run_recovery_key_verification(tag, sender, *request)
            }
            Self::CrossSigningStart(request) => run_cross_signing_start(tag, sender, *request),
            Self::CrossSigningSelectMethod(request) => {
                run_cross_signing_select_method(tag, sender, *request)
            }
            Self::CrossSigningConfirm(request) => run_cross_signing_confirm(tag, sender, *request),
            Self::AbortVerification(request) => run_abort_verification(tag, sender, *request),
            Self::UserList => run_user_list(tag, sender),
            Self::UserSearch(request) => run_user_search(tag, sender, *request),
            Self::PublicRoomList(request) => run_public_room_list(tag, sender, *request),
            Self::Invite(request) => run_invite(tag, sender, *request),
            Self::InvitationReply(request) => run_invitation_reply(tag, sender, *request),
            Self::RoomList(request) => run_room_list(tag, sender, *request),
            Self::CreateGroupRoom(request) => run_create_group_room(tag, sender, *request),
            Self::CreateDirectRoom(request) => run_create_direct_room(tag, sender, *request),
            Self::ChangeRoom(request) => run_change_room(tag, sender, *request),
            Self::LeaveRoom(request) => run_leave_room(tag, sender, *request),
            Self::JoinRoom(request) => run_join_room(tag, sender, *request),
            Self::KnockRoom(request) => run_knock_room(tag, sender, *request),
            Self::MarkAsRead(request) => run_mark_as_read(tag, sender, *request),
            Self::SendMessage(request) => run_send_message(tag, sender, *request),
            Self::RemoveMessage(request) => run_remove_message(tag, sender, *request),
            Self::RoomMessages(request) => run_room_messages(tag, sender, *request),
            Self::ChangeMessage(request) => run_change_message(tag, sender, *request),
            Self::CreateReaction(request) => run_create_reaction(tag, sender, *request),
        }
    }
}

impl UiAttribute for Action {
    fn update(&mut self, ui: &mut egui::Ui) {
        match self {
            Self::Initialize(request) => request.update(ui),
            Self::LoginFlows => (),
            Self::IdentityProviders => (),
            Self::LoginSso(request) => request.update(ui),
            Self::RecoveryKeyVerification(request) => request.update(ui),
            Self::CrossSigningStart(request) => request.update(ui),
            Self::CrossSigningSelectMethod(request) => request.update(ui),
            Self::CrossSigningConfirm(request) => request.update(ui),
            Self::AbortVerification(request) => request.update(ui),
            Self::UserList => (),
            Self::UserSearch(request) => request.update(ui),
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
            Self::MarkAsRead(request) => request.update(ui),
            Self::SendMessage(request) => request.update(ui),
            Self::RoomMessages(request) => request.update(ui),
            Self::RemoveMessage(request) => request.update(ui),
            Self::ChangeMessage(request) => request.update(ui),
            Self::CreateReaction(request) => request.update(ui),
        }
    }
}

fn send_request(sender: &mut SendHalf, request: RequestContainer) {
    let mut encoded = request.encode_to_vec();
    let mut data = encoded.len().to_le_bytes().to_vec();

    data.append(&mut encoded);

    sender
        .write_all(&data)
        .expect("Error writing request container to sender");
}

impl_run!(run_initialize, InitializationRequest, InitializationRequest);
impl_run!(run_login_flows, LoginFlowsRequest);
impl_run!(run_identity_providers, IdentityProvidersRequest);
impl_run!(run_login_sso, LoginSsoRequest, LoginSsoRequest);
impl_run!(
    run_recovery_key_verification,
    RecoveryKeyVerificationRequest,
    RecoveryKeyVerificationRequest
);
impl_run!(
    run_cross_signing_start,
    CrossSigningStartRequest,
    CrossSigningStartRequest
);
impl_run!(
    run_cross_signing_select_method,
    CrossSigningMethodSelectedRequest,
    CrossSigningMethodSelectedRequest
);
impl_run!(
    run_cross_signing_confirm,
    CrossSigningConfirmRequest,
    CrossSigningConfirmRequest
);
impl_run!(
    run_abort_verification,
    VerificationAbortRequest,
    VerificationAbortRequest
);
impl_run!(run_user_list, UserListRequest);
impl_run!(run_user_search, UserSearchRequest, UserSearchRequest);
impl_run!(
    run_public_room_list,
    PublicRoomListRequest,
    PublicRoomListRequest
);
impl_run!(run_invite, InvitationRequest, InvitationRequest);
impl_run!(run_invitation_reply, InvitedReply, InvitedReply);
impl_run!(run_room_list, RoomListRequest, RoomListRequest);
impl_run!(run_room_messages, RoomMessagesRequest, RoomMessagesRequest);
impl_run!(
    run_create_group_room,
    RoomCreateGroupRequest,
    RoomCreateGroupRequest
);
impl_run!(
    run_create_direct_room,
    RoomCreateDirectRequest,
    RoomCreateDirectRequest
);
impl_run!(run_change_room, RoomChangeRequest, RoomChangeRequest);
impl_run!(run_leave_room, RoomLeaveRequest, RoomLeaveRequest);
impl_run!(run_join_room, RoomJoinRequest, RoomJoinRequest);

impl_run!(run_knock_room, RoomKnockRequest, RoomKnockRequest);
impl_run!(
    run_mark_as_read,
    RoomMarkAsReadRequest,
    RoomMarkAsReadRequest
);
impl_run!(run_send_message, MessageSendRequest, MessageSendRequest);
impl_run!(
    run_remove_message,
    MessageRemoveRequest,
    MessageRemoveRequest
);
impl_run!(
    run_change_message,
    MessageChangeRequest,
    MessageChangeRequest
);
impl_run!(run_create_reaction, CreateReactionRequest, Reaction);
